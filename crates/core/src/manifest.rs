//! The table manifest — our catalog contract.
//!
//! NOTE: this is a deliberate *stand-in* for Apache Iceberg metadata. It carries
//! the essentials a planner/cost-estimator needs (file list + per-file stats) in
//! a simple JSON shape, so build steps 1–2 ship without dragging in a full
//! Iceberg implementation. It is replaced by real Iceberg later (ADR-002, TBD).
//!
//! It lives in core because both the ingest path (which *writes* it) and the
//! planner / DST harness (which *reads* and *fabricates* it at scale) share it.
//! Fabricating a trillion-file manifest here — with no bytes behind it — is
//! exactly the "metadata-scale without data-scale" mechanism from ADR-001.

use crate::model::Tier;
use serde::{Deserialize, Serialize};

/// One Parquet data file and the stats needed to plan/skip/price it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFile {
    /// Object-store key, relative to the store root.
    pub path: String,
    pub bytes: u64,
    pub rows: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub tier: Tier,
}

/// The set of files making up a table. (A single flat snapshot for now; real
/// Iceberg snapshots/partitions come with the Iceberg swap.)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub table: String,
    pub files: Vec<DataFile>,
    /// Monotonic counter bumped each compaction run, used to name compacted
    /// files uniquely so a run never collides with prior output.
    #[serde(default)]
    pub compaction_gen: u64,
}

impl Manifest {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            files: Vec::new(),
            compaction_gen: 0,
        }
    }

    pub fn add(&mut self, file: DataFile) {
        self.files.push(file);
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }

    pub fn total_rows(&self) -> u64 {
        self.files.iter().map(|f| f.rows).sum()
    }

    /// Files overlapping a `[from, to]` timestamp range — coarse min/max pruning,
    /// the cheapest planner win.
    pub fn files_in_range(&self, from: i64, to: i64) -> impl Iterator<Item = &DataFile> {
        self.files
            .iter()
            .filter(move |f| f.max_ts >= from && f.min_ts <= to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_and_range_pruning() {
        let mut m = Manifest::new("logs");
        m.add(DataFile {
            path: "logs/hot/part-0.parquet".into(),
            bytes: 1000,
            rows: 10,
            min_ts: 0,
            max_ts: 100,
            tier: Tier::Hot,
        });
        m.add(DataFile {
            path: "logs/hot/part-1.parquet".into(),
            bytes: 2000,
            rows: 20,
            min_ts: 200,
            max_ts: 300,
            tier: Tier::Hot,
        });
        assert_eq!(m.total_bytes(), 3000);
        assert_eq!(m.total_rows(), 30);
        // Only the first file overlaps [50, 150].
        let hit: Vec<_> = m.files_in_range(50, 150).collect();
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].path, "logs/hot/part-0.parquet");
    }
}
