//! verdigris-ingest — records → Parquet → object store, plus the manifest write.
//!
//! The rolling *policy* lives in core (`Batcher`); this crate does the encoding
//! and the actual `put`s through the `object_store` seam, so it runs unchanged
//! against local fs, in-memory, or S3/MinIO.

pub mod encode;
pub mod generate;
pub mod otlp;
pub mod schema;
pub mod wire;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use object_store::path::Path as ObjPath;
// 0.13 moved put/get/delete into the ObjectStoreExt convenience trait.
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use verdigris_core::batch::{BatchPolicy, Batcher, LogRecord};
use verdigris_core::config::RoutingConfig;
use verdigris_core::manifest::{DataFile, Manifest, Predicate, TrigramIndex};
use verdigris_core::model::Tier;

pub use encode::FileStats;

/// Bounded retries for an optimistic manifest commit before giving up under
/// sustained contention. Generous: real contention resolves in 1–2 rounds.
const MAX_COMMIT_RETRIES: usize = 16;

/// Outcome of an optimistic (compare-and-swap) manifest commit.
#[derive(Debug, PartialEq, Eq)]
enum CommitOutcome {
    /// The manifest was written; our version was current.
    Committed,
    /// Another writer committed first — our base version is stale; reload & retry.
    Conflict,
}

fn tier_dir(tier: Tier) -> &'static str {
    match tier {
        Tier::Hot => "hot",
        Tier::Warm => "warm",
        Tier::Cold => "cold",
    }
}

/// FNV-1a 64-bit hex of a byte slice. Used to name data files by content so two
/// writers never collide on an object path (the counter-based scheme did): same
/// content → same name (idempotent), different content → different name. No RNG,
/// so it stays deterministic for simulation.
fn content_hash(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// Writes a single logical table's data + manifest to an object store.
pub struct Ingestor {
    store: Arc<dyn ObjectStore>,
    table: String,
}

impl Ingestor {
    pub fn new(store: Arc<dyn ObjectStore>, table: impl Into<String>) -> Self {
        Self {
            store,
            table: table.into(),
        }
    }

    fn data_path(&self, tier: Tier, hash: &str) -> String {
        format!("{}/{}/part-{hash}.parquet", self.table, tier_dir(tier))
    }

    fn manifest_path(&self) -> ObjPath {
        ObjPath::from(format!("{}/_metadata/manifest.json", self.table))
    }

    fn trigrams_path(&self) -> ObjPath {
        ObjPath::from(format!("{}/_metadata/trigrams.json", self.table))
    }

    /// Load the trigram sidecar, or an empty one if it does not exist yet.
    ///
    /// Only queries carrying a text predicate need this; everything else reads
    /// the manifest alone and never pays for it. A missing or unreadable sidecar
    /// is not an error — it yields an empty index, which means "no text pruning",
    /// the conservative direction.
    pub async fn load_trigrams(&self) -> anyhow::Result<TrigramIndex> {
        match self.store.get(&self.trigrams_path()).await {
            Ok(res) => {
                let bytes = res.bytes().await.context("reading trigram sidecar")?;
                Ok(serde_json::from_slice(&bytes).unwrap_or_default())
            }
            Err(object_store::Error::NotFound { .. }) => Ok(TrigramIndex::default()),
            Err(e) => Err(e).context("loading trigram sidecar"),
        }
    }

    /// Load the manifest with trigram sets attached. Use this only for queries
    /// with a `MessageContains` predicate; [`Self::load_manifest`] is the cheap
    /// path everything else should take.
    pub async fn load_manifest_with_trigrams(&self) -> anyhow::Result<Manifest> {
        let mut m = self.load_manifest().await?;
        m.hydrate_trigrams(&self.load_trigrams().await?);
        Ok(m)
    }

    /// Load the manifest, fetching the trigram sidecar only if `preds` can
    /// actually use it.
    ///
    /// One decision point rather than a judgement call at every query site —
    /// the same reasoning that makes `select_files` the single pruning choke
    /// point. Forgetting to hydrate is not a correctness bug (unhydrated
    /// trigrams simply never prune) but it is a silent performance cliff, and
    /// silent is the part worth designing out.
    pub async fn load_manifest_for(&self, preds: &[Predicate]) -> anyhow::Result<Manifest> {
        let needs_text = preds
            .iter()
            .any(|p| matches!(p, Predicate::MessageContains(_)));
        if needs_text {
            self.load_manifest_with_trigrams().await
        } else {
            self.load_manifest().await
        }
    }

    /// Merge `add` into the sidecar and write it back.
    ///
    /// Deliberately a plain put with no compare-and-swap. Entries are keyed by
    /// file path and data files are immutable, so two writers can only ever add
    /// disjoint or identical entries — there is no value to lose. The cost of a
    /// lost race is a missing entry, which means "does not prune", which is safe.
    /// Writing this *before* the manifest commit keeps that true: a sidecar can
    /// run ahead of the manifest (harmless orphans) but should not lag it.
    async fn merge_trigrams(&self, add: &TrigramIndex) -> anyhow::Result<()> {
        if add.is_empty() {
            return Ok(());
        }
        let mut idx = self.load_trigrams().await?;
        for (path, set) in &add.by_path {
            idx.insert(path.clone(), set.clone());
        }
        for (path, sets) in &add.row_groups_by_path {
            idx.insert_row_groups(path.clone(), sets.clone());
        }
        let bytes = serde_json::to_vec(&idx).context("serializing trigram sidecar")?;
        self.store
            .put(&self.trigrams_path(), bytes.into())
            .await
            .context("writing trigram sidecar")?;
        Ok(())
    }

    /// Drop sidecar entries whose files are no longer in `manifest`.
    ///
    /// Only safe to call after the manifest naming those files is committed —
    /// see the call site in `compact_bounded`.
    async fn prune_trigrams(&self, manifest: &Manifest) -> anyhow::Result<()> {
        let mut idx = self.load_trigrams().await?;
        // Both maps, not just `len()` (which counts file-level entries only) — a
        // sidecar carrying row-group entries for retired paths would otherwise
        // never be rewritten and would grow without bound.
        let before = (idx.by_path.len(), idx.row_groups_by_path.len());
        idx.retain_paths(manifest);
        if (idx.by_path.len(), idx.row_groups_by_path.len()) == before {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&idx).context("serializing trigram sidecar")?;
        self.store
            .put(&self.trigrams_path(), bytes.into())
            .await
            .context("writing trigram sidecar")?;
        Ok(())
    }

    /// Load the table manifest, or an empty one if the table is new.
    pub async fn load_manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.load_manifest_versioned().await?.0)
    }

    /// Load the manifest together with its object version (ETag/version id) so a
    /// later [`Self::commit_manifest`] can compare-and-swap against it. A new table
    /// yields `(empty, None)`.
    async fn load_manifest_versioned(&self) -> anyhow::Result<(Manifest, Option<UpdateVersion>)> {
        match self.store.get(&self.manifest_path()).await {
            Ok(res) => {
                let version = Some(UpdateVersion {
                    e_tag: res.meta.e_tag.clone(),
                    version: res.meta.version.clone(),
                });
                let bytes = res.bytes().await.context("reading manifest")?;
                let m = serde_json::from_slice(&bytes).context("parsing manifest")?;
                Ok((m, version))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((Manifest::new(&self.table), None)),
            Err(e) => Err(e).context("loading manifest"),
        }
    }

    /// Optimistic (compare-and-swap) manifest commit. `base` is the version the
    /// manifest was loaded at: `None` means "create — must not already exist";
    /// `Some(v)` means "update only if still at `v`". A lost race returns
    /// [`CommitOutcome::Conflict`] (reload & retry) instead of silently clobbering
    /// a concurrent writer. Backends without conditional-put support fall back to a
    /// plain put (last-write-wins), which is safe under the single-writer role.
    async fn commit_manifest(
        &self,
        manifest: &Manifest,
        base: Option<UpdateVersion>,
    ) -> anyhow::Result<CommitOutcome> {
        let bytes = serde_json::to_vec_pretty(manifest).context("serializing manifest")?;
        let mode = match base {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.manifest_path(), bytes.clone().into(), opts)
            .await
        {
            Ok(_) => Ok(CommitOutcome::Committed),
            // Another writer got there first (Update precondition failed, or Create
            // found an existing object).
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => Ok(CommitOutcome::Conflict),
            // Backend can't do conditional puts (some local stores): fall back to a
            // plain put. Correct under the single-writer deployment model.
            Err(object_store::Error::NotImplemented { .. }) => {
                self.store
                    .put(&self.manifest_path(), bytes.into())
                    .await
                    .context("writing manifest (no-CAS fallback)")?;
                Ok(CommitOutcome::Committed)
            }
            Err(e) => Err(e).context("committing manifest"),
        }
    }

    /// Append already-written data files to the manifest under optimistic
    /// concurrency: load → merge (skipping any path already present, so a retry or
    /// a content-hash duplicate can't double-count) → commit; retry on conflict.
    async fn append_files(&self, files: &[DataFile]) -> anyhow::Result<()> {
        // Sidecar first: it may run ahead of the manifest (an entry for a file
        // not yet listed is simply never looked up), but must not lag it, or a
        // text query could miss pruning it was entitled to.
        let mut incoming = TrigramIndex::default();
        for f in files {
            if let Some(t) = &f.message_trigrams {
                incoming.insert(f.path.clone(), t.clone());
            }
        }
        self.merge_trigrams(&incoming).await?;

        for _ in 0..MAX_COMMIT_RETRIES {
            let (mut manifest, base) = self.load_manifest_versioned().await?;
            let existing: HashSet<String> = manifest.files.iter().map(|f| f.path.clone()).collect();
            let mut added = false;
            for f in files {
                if !existing.contains(&f.path) {
                    manifest.add(f.clone());
                    added = true;
                }
            }
            if !added {
                return Ok(()); // everything already committed (idempotent)
            }
            if self.commit_manifest(&manifest, base).await? == CommitOutcome::Committed {
                return Ok(());
            }
            // Conflict: another writer committed; loop reloads the fresh version.
        }
        anyhow::bail!("manifest commit failed after {MAX_COMMIT_RETRIES} retries under contention")
    }

    /// Encode one batch to Parquet and write it under a content-addressed name;
    /// returns its manifest entry. The name derives from the bytes, so concurrent
    /// writers never collide on a path.
    async fn write_file(&self, records: &[LogRecord], tier: Tier) -> anyhow::Result<DataFile> {
        let (bytes, stats) = encode::encode_parquet(records)?;
        let path = self.data_path(tier, &content_hash(&bytes));
        let len = bytes.len() as u64;
        self.store
            .put(&ObjPath::from(path.clone()), bytes.into())
            .await
            .with_context(|| format!("writing {path}"))?;
        Ok(DataFile {
            path,
            bytes: len,
            rows: stats.rows,
            min_ts: stats.min_ts,
            max_ts: stats.max_ts,
            tier,
            services: stats.services,
            levels: stats.levels,
            message_trigrams: Some(stats.message_trigrams),
            row_groups: stats.row_groups,
            row_group_trigrams: Some(stats.row_group_trigrams),
        })
    }

    /// Ingest a stream of records: route each by severity (`routing`) to a
    /// hot/warm/cold tier prefix, batch per tier by `policy`, roll Parquet files,
    /// and commit them to the manifest. Returns the files written this call.
    ///
    /// Per-tier batchers and a fixed tier iteration order keep this deterministic
    /// (no HashMap ordering) — simulation-stable.
    ///
    /// Concurrency: data files are content-addressed (collision-free across
    /// writers) and written *before* the manifest is touched; the manifest append
    /// is then an optimistic compare-and-swap that retries on conflict. So multiple
    /// writers to one table no longer race — a lost commit is retried, never
    /// silently dropped. (Full Apache Iceberg commits — snapshots, partition specs
    /// — remain future work; this is optimistic concurrency on the JSON catalog.)
    pub async fn ingest(
        &self,
        records: impl IntoIterator<Item = LogRecord>,
        routing: &RoutingConfig,
        policy: BatchPolicy,
    ) -> anyhow::Result<Vec<DataFile>> {
        let mut batchers = [
            Batcher::new(policy),
            Batcher::new(policy),
            Batcher::new(policy),
        ];
        let mut written = Vec::new();

        for record in records {
            let tier = routing.tier_for(record.level);
            let i = tier.index();
            if batchers[i].push(record) {
                let batch = batchers[i].take();
                written.push(self.write_file(&batch, tier).await?);
            }
        }
        // Flush leftover batches in fixed hot→cold order.
        for tier in Tier::ALL {
            let i = tier.index();
            if !batchers[i].is_empty() {
                let batch = batchers[i].take();
                written.push(self.write_file(&batch, tier).await?);
            }
        }

        if !written.is_empty() {
            self.append_files(&written).await?;
        }
        Ok(written)
    }

    /// Compaction: merge each tier's many small Parquet files into fewer files of
    /// ~`target_bytes`, then update the manifest and delete the old objects.
    ///
    /// This is the difference between a toy and a usable system: streaming ingest
    /// produces millions of tiny files that wreck scan speed and waste the Glacier
    /// per-object tax. Bins are formed in manifest order and tiers iterated in a
    /// fixed order, so the operation is deterministic. The manifest is rewritten
    /// to point at the new files *before* old objects are deleted (crash-safer).
    pub async fn compact(&self, target_bytes: u64) -> anyhow::Result<Vec<CompactionReport>> {
        Ok(self.compact_bounded(target_bytes, usize::MAX).await?.0)
    }

    /// One bounded compaction pass: merges at most ~`max_merge_files` source files
    /// (across tiers), commits under CAS, and returns `(reports, more)` where
    /// `more` is true if mergeable files were left for a later pass because the
    /// budget was reached. This bounds how long a single pass holds the ingest
    /// lock; the scheduler runs repeated passes (releasing the lock between them)
    /// to drain a large backlog. `max_merge_files == usize::MAX` reproduces a full
    /// single-pass compaction (what the CLI `compact` does).
    pub async fn compact_bounded(
        &self,
        target_bytes: u64,
        max_merge_files: usize,
    ) -> anyhow::Result<(Vec<CompactionReport>, bool)> {
        // Whole-operation optimistic retry: snapshot the manifest, rewrite the
        // layout, and commit under compare-and-swap. If a concurrent writer
        // committed in between, our freshly written compacted files become harmless
        // orphans (the manifest is the source of truth) and we redo against the new
        // state. Compaction is a periodic maintenance op, so conflicts are rare.
        for _ in 0..MAX_COMMIT_RETRIES {
            let (mut manifest, base) = self.load_manifest_versioned().await?;
            let generation = manifest.compaction_gen;

            let mut new_files: Vec<DataFile> = Vec::new();
            let mut to_delete: Vec<String> = Vec::new();
            let mut reports: Vec<CompactionReport> = Vec::new();
            // Per-pass merge budget: once reached, remaining mergeable bins are
            // deferred (kept as-is) and `more` signals the scheduler to run again.
            let mut merged_this_pass = 0usize;
            let mut more = false;

            for tier in Tier::ALL {
                let tier_files: Vec<DataFile> = manifest
                    .files
                    .iter()
                    .filter(|f| f.tier == tier)
                    .cloned()
                    .collect();
                if tier_files.is_empty() {
                    continue;
                }
                let before = tier_files.len();
                let mut after = 0usize;
                let mut merged = 0usize;
                let mut seq = 0usize;

                for bin in bin_by_bytes(&tier_files, target_bytes) {
                    if bin.len() <= 1 {
                        // Already big enough on its own — leave it untouched.
                        new_files.push(bin[0].clone());
                        after += 1;
                        continue;
                    }
                    // Budget reached: defer this (and every subsequent) mergeable
                    // bin to a later pass so one pass can't hold the lock too long.
                    // Always merge at least one bin so every pass makes progress.
                    if merged_this_pass > 0 && merged_this_pass + bin.len() > max_merge_files {
                        more = true;
                        for f in &bin {
                            new_files.push(f.clone());
                        }
                        after += bin.len();
                        continue;
                    }
                    // Stream the bin through one Parquet writer: fetch → decode →
                    // encode, a batch at a time, folding the manifest stats as we
                    // go. Peak memory is one batch plus the encoder's in-progress
                    // row group, NOT the bin's whole decompressed size — see
                    // `MergeWriter` (issue #13).
                    let mut w = encode::MergeWriter::new(schema::log_schema())?;
                    for f in &bin {
                        let bytes = self
                            .store
                            .get(&ObjPath::from(f.path.clone()))
                            .await
                            .with_context(|| format!("reading {} for compaction", f.path))?
                            .bytes()
                            .await?;
                        encode::stream_parquet_into(bytes, &mut w)?;
                    }
                    let in_progress = w.in_progress_size();
                    let (data, stats) = w.finish()?;
                    let path = format!(
                        "{}/{}/c{generation}-{seq:05}.parquet",
                        self.table,
                        tier.as_str()
                    );
                    seq += 1;
                    let len = data.len() as u64;
                    self.store
                        .put(&ObjPath::from(path.clone()), data.into())
                        .await
                        .with_context(|| format!("writing compacted {path}"))?;

                    tracing::debug!(
                        tier = tier.as_str(),
                        files = bin.len(),
                        out_bytes = len,
                        rows = stats.rows,
                        writer_in_progress_bytes = in_progress,
                        "compacted bin"
                    );

                    // Stats are recomputed from the merged rows by `MergeWriter`
                    // (authoritative even if some input files predate value stats),
                    // so the compacted file still prunes by service/level/trigram.
                    new_files.push(DataFile {
                        path,
                        bytes: len,
                        rows: stats.rows,
                        min_ts: stats.min_ts,
                        max_ts: stats.max_ts,
                        tier,
                        services: stats.services,
                        levels: stats.levels,
                        message_trigrams: Some(stats.message_trigrams),
                        row_groups: stats.row_groups,
                        row_group_trigrams: Some(stats.row_group_trigrams),
                    });
                    for f in &bin {
                        to_delete.push(f.path.clone());
                    }
                    after += 1;
                    merged += bin.len();
                    merged_this_pass += bin.len();
                }

                reports.push(CompactionReport {
                    tier,
                    files_before: before,
                    files_after: after,
                    files_merged: merged,
                });
            }

            // Sidecar for the newly written files, before the manifest points at
            // them. Retired paths are left in place: they are cleaned up on the
            // next successful commit, and an orphan entry is never consulted.
            let mut incoming = TrigramIndex::default();
            for f in &new_files {
                if let Some(t) = &f.message_trigrams {
                    incoming.insert(f.path.clone(), t.clone());
                }
                if let Some(rgs) = &f.row_group_trigrams {
                    incoming.insert_row_groups(f.path.clone(), rgs.clone());
                }
            }
            self.merge_trigrams(&incoming).await?;

            // Point the manifest at the new layout and commit under CAS.
            manifest.files = new_files;
            manifest.compaction_gen = generation + 1;
            if self.commit_manifest(&manifest, base).await? == CommitOutcome::Committed {
                // Committed: now clean up the old objects (orphans are harmless).
                for path in to_delete {
                    let _ = self.store.delete(&ObjPath::from(path)).await;
                }
                // Drop sidecar entries for the paths just retired, so it does not
                // grow without bound as compaction rewrites files.
                //
                // Strictly AFTER the commit: pruning first could delete an entry
                // the still-live manifest references, and a lost entry means a
                // text query stops pruning that file. Best-effort — a failure
                // leaves orphans, which are never consulted.
                let _ = self.prune_trigrams(&manifest).await;
                return Ok((reports, more));
            }
            // Conflict: reload and redo against the fresh manifest.
        }
        anyhow::bail!(
            "compaction commit failed after {MAX_COMMIT_RETRIES} retries under contention"
        )
    }
}

/// Per-tier outcome of a compaction run.
#[derive(Debug, Clone)]
pub struct CompactionReport {
    pub tier: Tier,
    pub files_before: usize,
    pub files_after: usize,
    pub files_merged: usize,
}

/// How many files a [`Ingestor::compact`] pass would merge *right now*, per tier,
/// for `target_bytes` — i.e. files that fall into a multi-file bin (a lone file
/// already at/over target is left untouched, so it isn't pending). Pure and
/// manifest-only: no data reads, no `DataFile` clones, so the auto-compaction
/// scheduler and `/v1/metrics` can call it on every tick cheaply.
///
/// This mirrors the binning in [`bin_by_bytes`] exactly (same greedy accumulation,
/// same trailing-remainder bin); the `pending_matches_compact` test pins the two
/// together so this count can never drift from what compaction actually merges.
pub fn pending_compaction(manifest: &Manifest, target_bytes: u64) -> Vec<(Tier, usize)> {
    let mut out = Vec::new();
    for tier in Tier::ALL {
        let mut pending = 0usize;
        let mut acc = 0u64;
        let mut bin_len = 0usize;
        for f in manifest.files.iter().filter(|f| f.tier == tier) {
            acc += f.bytes;
            bin_len += 1;
            if acc >= target_bytes {
                if bin_len > 1 {
                    pending += bin_len; // this closed bin is a real merge
                }
                acc = 0;
                bin_len = 0;
            }
        }
        if bin_len > 1 {
            pending += bin_len; // trailing remainder bin
        }
        if pending > 0 {
            out.push((tier, pending));
        }
    }
    out
}

/// Total files pending a merge across all tiers.
pub fn pending_compaction_total(manifest: &Manifest, target_bytes: u64) -> usize {
    pending_compaction(manifest, target_bytes)
        .iter()
        .map(|(_, n)| n)
        .sum()
}

/// Greedily group files (in order) into bins whose total size reaches
/// `target_bytes`. A trailing remainder forms its own bin.
fn bin_by_bytes(files: &[DataFile], target_bytes: u64) -> Vec<Vec<DataFile>> {
    let mut bins = Vec::new();
    let mut cur: Vec<DataFile> = Vec::new();
    let mut acc = 0u64;
    for f in files {
        cur.push(f.clone());
        acc += f.bytes;
        if acc >= target_bytes {
            bins.push(std::mem::take(&mut cur));
            acc = 0;
        }
    }
    if !cur.is_empty() {
        bins.push(cur);
    }
    bins
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn ingest_routes_by_severity_and_writes_manifest() {
        use std::collections::HashSet;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();

        let records = generate::generate(1000, 1, 0);
        let policy = BatchPolicy {
            max_rows: 250,
            max_bytes: usize::MAX,
        };
        ing.ingest(records, &routing, policy).await.unwrap();

        let manifest = ing.load_manifest().await.unwrap();
        assert_eq!(manifest.total_rows(), 1000);
        assert!(manifest.total_bytes() > 0);

        // Generated data has ERROR/WARN/INFO/DEBUG, so it routes to >1 tier,
        // and the file paths reflect the tier prefix.
        let tiers: HashSet<_> = manifest.files.iter().map(|f| f.tier).collect();
        assert!(tiers.len() >= 2, "expected multiple tiers, got {tiers:?}");
        assert!(manifest.files.iter().any(|f| f.path.contains("/hot/")));
        assert!(manifest
            .files
            .iter()
            .all(|f| f.path.contains(&format!("/{}/", f.tier.as_str()))));

        // A second ingest appends, continuing each tier's sequence.
        let more = generate::generate(100, 2, 10_000_000);
        ing.ingest(more, &routing, policy).await.unwrap();
        let manifest = ing.load_manifest().await.unwrap();
        assert_eq!(manifest.total_rows(), 1100);
    }

    #[tokio::test]
    async fn compaction_merges_small_files_without_data_loss() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 100,
            max_bytes: usize::MAX,
        };

        // Many small ingests -> many small files (the streaming reality).
        for i in 0u64..10 {
            let recs = generate::generate(100, i, (i as i64) * 1_000_000);
            ing.ingest(recs, &routing, policy).await.unwrap();
        }
        let before = ing.load_manifest().await.unwrap();
        let rows_before = before.total_rows();
        assert!(before.files.len() > 3, "expected many small files");
        let span = |m: &Manifest| {
            (
                m.files.iter().map(|f| f.min_ts).min().unwrap(),
                m.files.iter().map(|f| f.max_ts).max().unwrap(),
            )
        };
        let span_before = span(&before);

        // Large target -> each tier collapses toward a single file.
        let reports = ing.compact(10 * 1024 * 1024).await.unwrap();
        assert!(!reports.is_empty());

        let after = ing.load_manifest().await.unwrap();
        assert!(
            after.files.len() < before.files.len(),
            "expected fewer files after compaction ({} -> {})",
            before.files.len(),
            after.files.len()
        );
        assert_eq!(after.total_rows(), rows_before, "no rows lost");
        assert_eq!(after.compaction_gen, 1);
        // The streaming merge recomputes min/max ts from the rows it writes rather
        // than inheriting them from the inputs' manifest entries; the time span it
        // reports must still cover exactly what went in, or the planner would prune
        // away real data.
        assert_eq!(
            span(&after),
            span_before,
            "time span changed across compaction"
        );
        // Compacted files use the new naming scheme.
        assert!(after.files.iter().any(|f| f.path.contains("/c0-")));
    }

    #[test]
    fn pending_compaction_counts_only_multi_file_bins() {
        // Hot: three 100-byte files, target 250 -> bin [a,b] closes at 200<250? no,
        // 100+100=200 < 250, +c=300 >=250 -> one bin of 3 -> all 3 pending.
        let mut m = Manifest::new("logs");
        for p in ["logs/hot/a", "logs/hot/b", "logs/hot/c"] {
            m.add(sample_file(p));
        }
        assert_eq!(pending_compaction_total(&m, 250), 3);

        // One file already over target -> its own bin, nothing pending.
        let mut m2 = Manifest::new("logs");
        let mut big = sample_file("logs/hot/big");
        big.bytes = 1000;
        m2.add(big);
        assert_eq!(pending_compaction_total(&m2, 250), 0);
    }

    #[tokio::test]
    async fn pending_matches_compact() {
        // The invariant: pending_compaction_total (a manifest-only estimate) must
        // equal the files compact() actually merges (sum of files_merged). If the
        // binning ever drifts, this fails.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 100,
            max_bytes: usize::MAX,
        };
        for i in 0u64..10 {
            let recs = generate::generate(100, i, (i as i64) * 1_000_000);
            ing.ingest(recs, &routing, policy).await.unwrap();
        }
        let target = 10 * 1024 * 1024;
        let m = ing.load_manifest().await.unwrap();
        let predicted = pending_compaction_total(&m, target);

        let reports = ing.compact(target).await.unwrap();
        let merged: usize = reports.iter().map(|r| r.files_merged).sum();
        assert_eq!(
            predicted, merged,
            "pending estimate must equal files actually merged"
        );
    }

    #[tokio::test]
    async fn bounded_compaction_drains_and_matches_unbounded() {
        // Invariant: draining a backlog with small bounded passes ends in the same
        // file layout as one unbounded pass — just spread over multiple commits so
        // no single pass holds the ingest lock for long.
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 100,
            max_bytes: usize::MAX,
        };
        let target = 10 * 1024 * 1024;

        let a = Ingestor::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>, "logs");
        let b = Ingestor::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>, "logs");
        // Identical data into both (generator is deterministic).
        for i in 0u64..20 {
            a.ingest(
                generate::generate(100, i, (i as i64) * 1_000_000),
                &routing,
                policy,
            )
            .await
            .unwrap();
            b.ingest(
                generate::generate(100, i, (i as i64) * 1_000_000),
                &routing,
                policy,
            )
            .await
            .unwrap();
        }
        let before = a.load_manifest().await.unwrap().files.len();
        assert!(before > 6, "expected a backlog, got {before}");

        // A: one unbounded pass leaves nothing pending.
        let (_ra, more_a) = a.compact_bounded(target, usize::MAX).await.unwrap();
        assert!(!more_a);

        // B: tiny budget -> many bounded passes until drained.
        let mut passes = 0usize;
        loop {
            let (_rb, more) = b.compact_bounded(target, 4).await.unwrap();
            passes += 1;
            assert!(passes < 1000, "must terminate");
            if !more {
                break;
            }
        }
        assert!(
            passes > 1,
            "small budget should need multiple passes, took {passes}"
        );

        let fa = a.load_manifest().await.unwrap();
        let fb = b.load_manifest().await.unwrap();
        assert_eq!(fa.total_rows(), fb.total_rows(), "no rows lost either way");
        assert_eq!(
            fa.files.len(),
            fb.files.len(),
            "bounded drains to the same layout"
        );
        assert_eq!(pending_compaction_total(&fb, target), 0, "fully drained");
    }

    fn sample_file(path: &str) -> DataFile {
        DataFile {
            path: path.into(),
            bytes: 100,
            rows: 10,
            min_ts: 0,
            max_ts: 100,
            tier: Tier::Hot,
            services: vec![],
            levels: vec![],
            message_trigrams: None,
            row_groups: 0,
            row_group_trigrams: None,
        }
    }

    #[tokio::test]
    async fn ingested_files_carry_value_stats_that_prune_scans() {
        use verdigris_core::batch::LogRecord;
        use verdigris_core::estimate::select_files;
        use verdigris_core::manifest::Predicate;
        use verdigris_core::model::{Level, Tier};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 1000,
            max_bytes: usize::MAX,
        };

        let rec = |svc: &str| LogRecord {
            ts_millis: 1,
            level: Level::Error, // all ERROR → same (hot) tier, so only service separates files
            service: svc.into(),
            status: Some(500),
            message: "boom".into(),
            trace_id: None,
            attrs: Default::default(),
        };
        // Two separate ingests → two separate hot files, one per service.
        ing.ingest(vec![rec("auth"), rec("auth")], &routing, policy)
            .await
            .unwrap();
        ing.ingest(vec![rec("billing")], &routing, policy)
            .await
            .unwrap();

        let m = ing.load_manifest().await.unwrap();
        assert_eq!(m.files.len(), 2, "expected one file per ingest");
        assert!(m.files.iter().all(|f| f.tier == Tier::Hot));
        // Stats were recorded from the real rows.
        assert!(m
            .files
            .iter()
            .any(|f| f.services == vec!["auth".to_string()]));
        assert!(m
            .files
            .iter()
            .any(|f| f.services == vec!["billing".to_string()]));
        assert!(m
            .files
            .iter()
            .all(|f| f.levels == vec!["ERROR".to_string()]));

        // `service:auth` prunes the billing file at plan time; `service:search`
        // (present in neither) prunes both.
        let auth = select_files(&m, &[Tier::Hot], None, &[Predicate::service("auth")]);
        assert_eq!(auth.len(), 1);
        assert_eq!(auth[0].services, vec!["auth".to_string()]);
        let none = select_files(&m, &[Tier::Hot], None, &[Predicate::service("search")]);
        assert!(none.is_empty(), "a value in no file scans nothing");
        // No predicate → both files.
        assert_eq!(select_files(&m, &[Tier::Hot], None, &[]).len(), 2);
    }

    #[tokio::test]
    async fn compaction_recomputes_value_stats_from_merged_rows() {
        use verdigris_core::batch::LogRecord;
        use verdigris_core::estimate::select_files;
        use verdigris_core::manifest::Predicate;
        use verdigris_core::model::{Level, Tier};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 1,
            max_bytes: usize::MAX,
        };

        let rec = |svc: &str| LogRecord {
            ts_millis: 1,
            level: Level::Error,
            service: svc.into(),
            status: Some(500),
            message: "x".into(),
            trace_id: None,
            attrs: Default::default(),
        };
        // Many tiny single-row hot files across two services.
        for svc in ["auth", "auth", "billing", "auth"] {
            ing.ingest(vec![rec(svc)], &routing, policy).await.unwrap();
        }
        ing.compact(10 * 1024 * 1024).await.unwrap();

        let m = ing.load_manifest().await.unwrap();
        // Merged into one file whose stats are the UNION recomputed from the rows.
        let hot: Vec<_> = m.files.iter().filter(|f| f.tier == Tier::Hot).collect();
        assert_eq!(hot.len(), 1, "one compacted hot file");
        assert_eq!(
            hot[0].services,
            vec!["auth".to_string(), "billing".to_string()]
        );
        // Both services still prune correctly against the compacted file.
        assert_eq!(
            select_files(&m, &[Tier::Hot], None, &[Predicate::service("auth")]).len(),
            1
        );
        assert_eq!(
            select_files(&m, &[Tier::Hot], None, &[Predicate::service("billing")]).len(),
            1
        );
        assert!(select_files(&m, &[Tier::Hot], None, &[Predicate::service("gateway")]).is_empty());
    }

    #[tokio::test]
    async fn free_text_prunes_files_and_survives_compaction() {
        use verdigris_core::batch::LogRecord;
        use verdigris_core::estimate::select_files;
        use verdigris_core::manifest::Predicate;
        use verdigris_core::model::{Level, Tier};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 1000,
            max_bytes: usize::MAX,
        };

        let rec = |msg: &str| LogRecord {
            ts_millis: 1,
            level: Level::Error,
            service: "auth".into(),
            status: Some(500),
            message: msg.into(),
            trace_id: None,
            attrs: Default::default(),
        };
        // Two separate hot files with distinct message vocabularies.
        ing.ingest(
            vec![rec("connection timeout to db-primary")],
            &routing,
            policy,
        )
        .await
        .unwrap();
        ing.ingest(
            vec![rec("NullPointerException at AuthFilter.java:42")],
            &routing,
            policy,
        )
        .await
        .unwrap();

        // The lean path: a manifest load carries no trigram payload at all.
        // This is the point of the sidecar — a non-text query never pays for it.
        let lean = ing.load_manifest().await.unwrap();
        assert_eq!(lean.files.len(), 2);
        assert!(
            lean.files.iter().all(|f| f.message_trigrams.is_none()),
            "load_manifest must not carry trigrams; they belong in the sidecar"
        );
        // ...and without them, a text predicate prunes nothing. Conservative, not
        // wrong: every file survives, so no real match can be dropped.
        assert_eq!(
            select_files(
                &lean,
                &[Tier::Hot],
                None,
                &[Predicate::message_contains("timeout")]
            )
            .len(),
            2,
            "unhydrated trigrams must not prune"
        );

        // The text path: hydrate from the sidecar, then prune as before.
        let m = ing.load_manifest_with_trigrams().await.unwrap();
        assert_eq!(m.files.len(), 2);
        assert!(m.files.iter().all(|f| f.message_trigrams.is_some()));

        // A term unique to one file scans only that file — including an in-word
        // substring, matching `ILIKE '%…%'` semantics ("ointer" ⊂ "NullPointer").
        assert_eq!(
            select_files(
                &m,
                &[Tier::Hot],
                None,
                &[Predicate::message_contains("timeout")]
            )
            .len(),
            1
        );
        assert_eq!(
            select_files(
                &m,
                &[Tier::Hot],
                None,
                &[Predicate::message_contains("ointer")]
            )
            .len(),
            1
        );
        // A term in no file scans nothing; a short term can't prune anything.
        assert!(select_files(
            &m,
            &[Tier::Hot],
            None,
            &[Predicate::message_contains("kubelet")]
        )
        .is_empty());
        assert_eq!(
            select_files(&m, &[Tier::Hot], None, &[Predicate::message_contains("db")]).len(),
            2
        );

        // Compaction merges both files and recomputes the trigram set from the
        // merged rows — both vocabularies still match, absent terms still prune.
        ing.compact(10 * 1024 * 1024).await.unwrap();
        let m = ing.load_manifest_with_trigrams().await.unwrap();
        assert_eq!(m.files.len(), 1, "one compacted hot file");
        for term in ["timeout", "ointer"] {
            assert_eq!(
                select_files(&m, &[Tier::Hot], None, &[Predicate::message_contains(term)]).len(),
                1,
                "term {term:?} must survive compaction"
            );
        }
        assert!(select_files(
            &m,
            &[Tier::Hot],
            None,
            &[Predicate::message_contains("kubelet")]
        )
        .is_empty());
        // The recorded stat round-trips the sidecar JSON (base64 bitmap).
        let t = m.files[0].message_trigrams.as_ref().unwrap();
        assert_eq!(t.contains_term("db-primary"), Some(true));

        // Compaction retires the pre-merge paths. The sidecar keeps only live
        // entries, so it does not grow without bound as files are rewritten.
        let idx = ing.load_trigrams().await.unwrap();
        assert_eq!(
            idx.len(),
            1,
            "sidecar should hold exactly the live compacted file"
        );
    }

    /// `load_manifest_for` is the one place that decides whether to pay for the
    /// sidecar, so it gets its own test: text predicates hydrate, everything
    /// else stays lean.
    #[tokio::test]
    async fn load_manifest_for_fetches_trigrams_only_when_text_can_use_them() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let routing = RoutingConfig::default();
        let policy = BatchPolicy::default();
        ing.ingest(
            vec![LogRecord {
                ts_millis: 1,
                level: verdigris_core::model::Level::Error,
                service: "auth".into(),
                status: Some(500),
                message: "connection timeout to db-primary".into(),
                trace_id: None,
                attrs: Default::default(),
            }],
            &routing,
            policy,
        )
        .await
        .unwrap();

        // No predicates at all, and non-text predicates: stay lean.
        for preds in [vec![], vec![Predicate::service("auth")]] {
            let m = ing.load_manifest_for(&preds).await.unwrap();
            assert!(
                m.files.iter().all(|f| f.message_trigrams.is_none()),
                "non-text query must not fetch the trigram sidecar"
            );
        }

        // A text predicate: hydrate.
        let m = ing
            .load_manifest_for(&[Predicate::message_contains("timeout")])
            .await
            .unwrap();
        assert!(
            m.files.iter().all(|f| f.message_trigrams.is_some()),
            "text query must hydrate trigrams"
        );

        // Mixed: one text predicate is enough.
        let m = ing
            .load_manifest_for(&[
                Predicate::service("auth"),
                Predicate::message_contains("timeout"),
            ])
            .await
            .unwrap();
        assert!(m.files.iter().all(|f| f.message_trigrams.is_some()));
    }

    #[tokio::test]
    async fn optimistic_commit_rejects_a_stale_writer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");

        // Create the manifest (no base version yet).
        let mut m0 = Manifest::new("logs");
        m0.add(sample_file("logs/hot/part-a.parquet"));
        assert_eq!(
            ing.commit_manifest(&m0, None).await.unwrap(),
            CommitOutcome::Committed
        );

        // Two writers load the SAME current version.
        let (mut w1, v1) = ing.load_manifest_versioned().await.unwrap();
        let (mut w2, v2) = ing.load_manifest_versioned().await.unwrap();
        assert!(v1.is_some(), "an existing manifest must carry a version");

        // Writer 1 commits first — succeeds and bumps the stored version.
        w1.add(sample_file("logs/hot/part-b.parquet"));
        assert_eq!(
            ing.commit_manifest(&w1, v1).await.unwrap(),
            CommitOutcome::Committed
        );

        // Writer 2's base version is now stale — rejected, not silently clobbering.
        w2.add(sample_file("logs/hot/part-c.parquet"));
        assert_eq!(
            ing.commit_manifest(&w2, v2).await.unwrap(),
            CommitOutcome::Conflict
        );

        // The winner survived; the loser did not overwrite it.
        let m = ing.load_manifest().await.unwrap();
        let paths: HashSet<_> = m.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains("logs/hot/part-b.parquet"));
        assert!(!paths.contains("logs/hot/part-c.parquet"));
    }

    #[tokio::test]
    async fn append_dedupes_duplicate_paths() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ing = Ingestor::new(store, "logs");
        let f = sample_file("logs/hot/part-x.parquet");
        ing.append_files(std::slice::from_ref(&f)).await.unwrap();
        // Re-appending the same path (retry / content-hash duplicate) must not
        // double-count.
        ing.append_files(std::slice::from_ref(&f)).await.unwrap();
        let m = ing.load_manifest().await.unwrap();
        assert_eq!(m.files.iter().filter(|x| x.path == f.path).count(), 1);
    }

    #[tokio::test]
    async fn concurrent_ingests_preserve_all_rows() {
        // Two writers sharing one table+store, committing concurrently. With the
        // old blind put they would collide on file paths and clobber each other's
        // manifest; with content-addressed files + optimistic commits, every row
        // survives.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routing = RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 40,
            max_bytes: usize::MAX,
        };
        let a = Ingestor::new(store.clone(), "logs");
        let b = Ingestor::new(store.clone(), "logs");
        // Different seeds+offsets → different content → different file hashes.
        let ra = generate::generate(200, 1, 0);
        let rb = generate::generate(200, 2, 9_000_000);
        let (xa, xb) = tokio::join!(
            a.ingest(ra, &routing, policy),
            b.ingest(rb, &routing, policy)
        );
        xa.unwrap();
        xb.unwrap();
        let m = a.load_manifest().await.unwrap();
        assert_eq!(
            m.total_rows(),
            400,
            "both writers' rows must survive the concurrent commit"
        );
    }
}
