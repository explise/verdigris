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
use crate::text::TrigramSet;
use serde::{Deserialize, Serialize};

/// A stat-carrying string column whose per-file distinct values are recorded in
/// the manifest so a query can skip files that can't contain the wanted value —
/// plan-time pruning before any Parquet is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatColumn {
    Service,
    Level,
}

/// A file-prunable predicate, used to skip files at plan time. Only predicates
/// that can *prove* a file match-free qualify: equality on a stat column, and
/// free-text substring via the trigram set. Ranges/negations never prune.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// `column = value` — checked against the per-file distinct-value stats.
    Equals { column: StatColumn, value: String },
    /// `message ILIKE '%term%'` (the DSL's free-text term) — checked against the
    /// per-file trigram set, which prunes only when some trigram of `term` was
    /// provably never written to the file.
    MessageContains(String),
}

impl Predicate {
    pub fn service(value: impl Into<String>) -> Self {
        Self::Equals {
            column: StatColumn::Service,
            value: value.into(),
        }
    }
    pub fn level(value: impl Into<String>) -> Self {
        Self::Equals {
            column: StatColumn::Level,
            value: value.into(),
        }
    }
    pub fn message_contains(term: impl Into<String>) -> Self {
        Self::MessageContains(term.into())
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
    /// Character-trigram presence set over this file's `message` column (~6.3 KB
    /// bitmap, base64 in JSON), letting a free-text search skip the file when a
    /// trigram of the term is provably absent. `None` = not recorded (legacy
    /// file, or a corrupt stat) — never prunes. See [`crate::text`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_trigrams: Option<TrigramSet>,
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
    /// A file is skippable for a predicate only when it has recorded stats for
    /// that predicate **and** those stats prove the value absent. Missing stats
    /// (legacy/unknown/corrupt) never prune, and neither does a term too short to
    /// judge. So a `false` here is a proof the file holds no matching row —
    /// pruning never drops a real match.
    pub fn may_match(&self, preds: &[Predicate]) -> bool {
        preds.iter().all(|p| match p {
            Predicate::Equals { column, value } => {
                let vals = self.stat_values(*column);
                vals.is_empty() || vals.iter().any(|v| v == value)
            }
            Predicate::MessageContains(term) => match &self.message_trigrams {
                None => true,
                Some(t) => t.contains_term(term).unwrap_or(true),
            },
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
            message_trigrams: None,
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
            message_trigrams: None,
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
            message_trigrams: None,
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
        let legacy = DataFile {
            services: vec![],
            levels: vec![],
            ..f.clone()
        };
        assert!(legacy.may_match(&[Predicate::service("anything")]));
        assert!(legacy.may_match(&[Predicate::level("DEBUG")]));
    }

    #[test]
    fn message_predicate_prunes_only_on_provable_trigram_absence() {
        let mut trigrams = crate::text::TrigramSet::new();
        trigrams.insert_text("connection timeout to db-primary");
        let f = DataFile {
            path: "logs/hot/part.parquet".into(),
            bytes: 1,
            rows: 1,
            min_ts: 0,
            max_ts: 1,
            tier: Tier::Hot,
            services: vec![],
            levels: vec![],
            message_trigrams: Some(trigrams),
        };
        // Present term (and an in-word substring, ILIKE semantics) → keep.
        assert!(f.may_match(&[Predicate::message_contains("timeout")]));
        assert!(f.may_match(&[Predicate::message_contains("nnect")]));
        // Provably absent term → skip.
        assert!(!f.may_match(&[Predicate::message_contains("kubelet")]));
        // Too short to judge → keep (never prune on a guess).
        assert!(f.may_match(&[Predicate::message_contains("db")]));
        // Combines with equality predicates under AND.
        assert!(!f.may_match(&[
            Predicate::message_contains("timeout"),
            Predicate::message_contains("kubelet"),
        ]));
        // No recorded trigrams (legacy file) → never pruned by free text.
        let legacy = DataFile {
            message_trigrams: None,
            ..f.clone()
        };
        assert!(legacy.may_match(&[Predicate::message_contains("kubelet")]));
    }
}
