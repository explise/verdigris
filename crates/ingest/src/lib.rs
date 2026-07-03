//! verdigris-ingest — records → Parquet → object store, plus the manifest write.
//!
//! The rolling *policy* lives in core (`Batcher`); this crate does the encoding
//! and the actual `put`s through the `object_store` seam, so it runs unchanged
//! against local fs, in-memory, or S3/MinIO.

pub mod encode;
pub mod generate;
pub mod schema;

use std::sync::Arc;

use anyhow::Context;
use object_store::path::Path as ObjPath;
// 0.13 moved put/get/delete into the ObjectStoreExt convenience trait.
use object_store::{ObjectStore, ObjectStoreExt};
use verdigris_core::batch::{BatchPolicy, Batcher, LogRecord};
use verdigris_core::config::RoutingConfig;
use verdigris_core::manifest::{DataFile, Manifest};
use verdigris_core::model::Tier;

pub use encode::FileStats;

fn tier_dir(tier: Tier) -> &'static str {
    match tier {
        Tier::Hot => "hot",
        Tier::Warm => "warm",
        Tier::Cold => "cold",
    }
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

    fn data_path(&self, tier: Tier, seq: usize) -> String {
        format!("{}/{}/part-{seq:08}.parquet", self.table, tier_dir(tier))
    }

    fn manifest_path(&self) -> ObjPath {
        ObjPath::from(format!("{}/_metadata/manifest.json", self.table))
    }

    /// Load the table manifest, or an empty one if the table is new.
    pub async fn load_manifest(&self) -> anyhow::Result<Manifest> {
        match self.store.get(&self.manifest_path()).await {
            Ok(res) => {
                let bytes = res.bytes().await.context("reading manifest")?;
                serde_json::from_slice(&bytes).context("parsing manifest")
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Manifest::new(&self.table)),
            Err(e) => Err(e).context("loading manifest"),
        }
    }

    async fn store_manifest(&self, manifest: &Manifest) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(manifest).context("serializing manifest")?;
        self.store
            .put(&self.manifest_path(), bytes.into())
            .await
            .context("writing manifest")?;
        Ok(())
    }

    /// Encode one batch to Parquet and write it; returns its manifest entry.
    async fn write_file(
        &self,
        records: &[LogRecord],
        tier: Tier,
        seq: usize,
    ) -> anyhow::Result<DataFile> {
        let (bytes, stats) = encode::encode_parquet(records)?;
        let path = self.data_path(tier, seq);
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
    /// and update the manifest at the end. Returns the files written this call.
    ///
    /// Per-tier batchers and a fixed tier iteration order keep this
    /// deterministic (no HashMap ordering) — simulation-stable.
    ///
    /// NOTE: single-writer model — concurrent ingestors would race on the
    /// manifest. Real Iceberg commits fix that; out of scope for now.
    pub async fn ingest(
        &self,
        records: impl IntoIterator<Item = LogRecord>,
        routing: &RoutingConfig,
        policy: BatchPolicy,
    ) -> anyhow::Result<Vec<DataFile>> {
        let mut manifest = self.load_manifest().await?;

        // Per-tier file sequence numbers continue from what's already stored.
        let mut seqs = [0usize; 3];
        for f in &manifest.files {
            seqs[f.tier.index()] += 1;
        }
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
                let df = self.write_file(&batch, tier, seqs[i]).await?;
                seqs[i] += 1;
                manifest.add(df.clone());
                written.push(df);
            }
        }
        // Flush leftover batches in fixed hot→cold order.
        for tier in Tier::ALL {
            let i = tier.index();
            if !batchers[i].is_empty() {
                let batch = batchers[i].take();
                let df = self.write_file(&batch, tier, seqs[i]).await?;
                seqs[i] += 1;
                manifest.add(df.clone());
                written.push(df);
            }
        }

        self.store_manifest(&manifest).await?;
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
        let mut manifest = self.load_manifest().await?;
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

        // Point the manifest at the new layout, then clean up old objects.
        manifest.files = new_files;
        manifest.compaction_gen = generation + 1;
        self.store_manifest(&manifest).await?;
        for path in to_delete {
            // Best-effort: orphaned objects are harmless (manifest is the truth).
            let _ = self.store.delete(&ObjPath::from(path)).await;
        }

        Ok(reports)
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
}
