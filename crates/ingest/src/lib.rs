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
use verdigris_core::manifest::{DataFile, Manifest};
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
        for _ in 0..MAX_COMMIT_RETRIES {
            let (mut manifest, base) = self.load_manifest_versioned().await?;
            let existing: HashSet<String> =
                manifest.files.iter().map(|f| f.path.clone()).collect();
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

            for tier in Tier::ALL {
                let tier_files: Vec<DataFile> =
                    manifest.files.iter().filter(|f| f.tier == tier).cloned().collect();
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
                    // Read every file in the bin and re-encode as one Parquet file.
                    let mut batches = Vec::new();
                    for f in &bin {
                        let bytes = self
                            .store
                            .get(&ObjPath::from(f.path.clone()))
                            .await
                            .with_context(|| format!("reading {} for compaction", f.path))?
                            .bytes()
                            .await?;
                        batches.extend(encode::read_parquet_bytes(bytes)?);
                    }
                    let data = encode::encode_record_batches(schema::log_schema(), &batches)?;
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

                    new_files.push(DataFile {
                        path,
                        bytes: len,
                        rows: bin.iter().map(|f| f.rows).sum(),
                        min_ts: bin.iter().map(|f| f.min_ts).min().unwrap_or(0),
                        max_ts: bin.iter().map(|f| f.max_ts).max().unwrap_or(0),
                        tier,
                    });
                    for f in &bin {
                        to_delete.push(f.path.clone());
                    }
                    after += 1;
                    merged += bin.len();
                }

                reports.push(CompactionReport {
                    tier,
                    files_before: before,
                    files_after: after,
                    files_merged: merged,
                });
            }

            // Point the manifest at the new layout and commit under CAS.
            manifest.files = new_files;
            manifest.compaction_gen = generation + 1;
            if self.commit_manifest(&manifest, base).await? == CommitOutcome::Committed {
                // Committed: now clean up the old objects (orphans are harmless).
                for path in to_delete {
                    let _ = self.store.delete(&ObjPath::from(path)).await;
                }
                return Ok(reports);
            }
            // Conflict: reload and redo against the fresh manifest.
        }
        anyhow::bail!("compaction commit failed after {MAX_COMMIT_RETRIES} retries under contention")
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
        // Compacted files use the new naming scheme.
        assert!(after.files.iter().any(|f| f.path.contains("/c0-")));
    }

    fn sample_file(path: &str) -> DataFile {
        DataFile {
            path: path.into(),
            bytes: 100,
            rows: 10,
            min_ts: 0,
            max_ts: 100,
            tier: Tier::Hot,
        }
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
        ing.append_files(&[f.clone()]).await.unwrap();
        // Re-appending the same path (retry / content-hash duplicate) must not
        // double-count.
        ing.append_files(&[f.clone()]).await.unwrap();
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
