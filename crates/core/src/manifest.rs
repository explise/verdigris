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

/// A stat-carrying string column whose per-file distinct values are recorded in
/// the manifest so a query can skip files that can't contain the wanted value —
/// plan-time pruning before any Parquet is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatColumn {
    Service,
    Level,
}

/// An equality predicate on a stat column (`column = value`), used to skip files
/// at plan time. Deliberately equality-only: ranges/negations can't safely prove
/// a file is value-free, so they never prune.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate {
    pub column: StatColumn,
    pub value: String,
}

impl Predicate {
    pub fn service(value: impl Into<String>) -> Self {
        Self { column: StatColumn::Service, value: value.into() }
    }
    pub fn level(value: impl Into<String>) -> Self {
        Self { column: StatColumn::Level, value: value.into() }
    }
}

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
    /// Distinct `service` values in this file (sorted, deduped). **Empty means
    /// "not recorded"** (a legacy file predating value stats) — an empty set never
    /// prunes, so pruning can only ever drop files provably free of a value.
    #[serde(default)]
    pub services: Vec<String>,
    /// Distinct `level` values in this file (sorted, deduped). Empty = not recorded.
    #[serde(default)]
    pub levels: Vec<String>,
}

impl DataFile {
    fn stat_values(&self, column: StatColumn) -> &[String] {
        match column {
            StatColumn::Service => &self.services,
            StatColumn::Level => &self.levels,
        }
    }

    /// Could this file contain a row satisfying every predicate in `preds`?
    ///
    /// A file is skippable for a predicate only when it has recorded values for
    /// that column **and** the wanted value is absent from them. A column with no
    /// recorded values (legacy/unknown) never prunes. So a `false` here is a proof
    /// the file holds no matching row — pruning never drops a real match.
    pub fn may_match(&self, preds: &[Predicate]) -> bool {
        preds.iter().all(|p| {
            let vals = self.stat_values(p.column);
            vals.is_empty() || vals.iter().any(|v| v == &p.value)
        })
    }
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
            services: vec![],
            levels: vec![],
        });
        m.add(DataFile {
            path: "logs/hot/part-1.parquet".into(),
            bytes: 2000,
            rows: 20,
            min_ts: 200,
            max_ts: 300,
            tier: Tier::Hot,
            services: vec![],
            levels: vec![],
        });
        assert_eq!(m.total_bytes(), 3000);
        assert_eq!(m.total_rows(), 30);
        // Only the first file overlaps [50, 150].
        let hit: Vec<_> = m.files_in_range(50, 150).collect();
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].path, "logs/hot/part-0.parquet");
    }

    #[test]
    fn may_match_skips_only_on_recorded_absence() {
        let f = DataFile {
            path: "logs/hot/part.parquet".into(),
            bytes: 1,
            rows: 1,
            min_ts: 0,
            max_ts: 1,
            tier: Tier::Hot,
            services: vec!["auth".into(), "billing".into()],
            levels: vec!["ERROR".into()],
        };
        // Present value → keep.
        assert!(f.may_match(&[Predicate::service("auth")]));
        assert!(f.may_match(&[Predicate::level("ERROR")]));
        // Recorded but absent → provably skippable.
        assert!(!f.may_match(&[Predicate::service("search")]));
        assert!(!f.may_match(&[Predicate::level("DEBUG")]));
        // All predicates must hold (AND): one miss skips the file.
        assert!(!f.may_match(&[Predicate::service("auth"), Predicate::level("WARN")]));
        assert!(f.may_match(&[Predicate::service("auth"), Predicate::level("ERROR")]));
        // Legacy file (no recorded values) is never pruned — correctness over speed.
        let legacy = DataFile { services: vec![], levels: vec![], ..f.clone() };
        assert!(legacy.may_match(&[Predicate::service("anything")]));
        assert!(legacy.may_match(&[Predicate::level("DEBUG")]));
    }
}
