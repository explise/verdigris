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
use std::collections::BTreeMap;

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
    /// Row groups in this Parquet file. `0` = not recorded (a file predating the
    /// row-group index), which disables row-group pruning for it.
    ///
    /// Recorded here, in the manifest, specifically because the row-group trigram
    /// sets live in the *sidecar*: two objects, written at different times by
    /// different code paths. Cross-checking one against the other is what makes
    /// [`DataFile::row_groups_to_scan`]'s length guard mean something rather than
    /// compare the index against itself.
    #[serde(default)]
    pub row_groups: u64,
    /// Character-trigram presence set over this file's `message` column, letting
    /// a free-text search skip the file when a trigram of the term is provably
    /// absent. `None` = not loaded or not recorded — **never prunes**. See
    /// [`crate::text`].
    ///
    /// Not serialized with the manifest. Trigrams are only needed by queries
    /// carrying a `MessageContains` predicate, and they are the bulk of a
    /// manifest entry, so they live in a sidecar
    /// (`{table}/_metadata/trigrams.json`) that is fetched only when a text
    /// predicate is present — see [`TrigramIndex`]. A plain
    /// `WHERE service=… AND ts>…` no longer pays for them.
    ///
    /// The default is `None`, which is why this is safe: a caller that forgets
    /// to hydrate simply does not prune on text. Slower, never wrong.
    #[serde(skip)]
    pub message_trigrams: Option<TrigramSet>,
    /// The same trigram idea one level down: one set per **row group**, in row-group
    /// order. Lets a surviving file be scanned partially — file-level pruning stops
    /// paying exactly when compaction makes files big, which is when grep hurts most.
    ///
    /// `None` = not loaded or not recorded, and an entry whose length disagrees with
    /// the file's actual row-group count is treated the same way (see
    /// [`DataFile::row_groups_to_scan`]). Sidecar-backed and `#[serde(skip)]` for the
    /// same reasons as [`DataFile::message_trigrams`].
    #[serde(skip)]
    pub row_group_trigrams: Option<Vec<TrigramSet>>,
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

    /// Which of this file's row groups can hold a row satisfying `preds`?
    ///
    /// Returns `Some(mask)` — `mask[i] == false` proving row group `i` matchless —
    /// or `None` meaning "no usable index, scan the whole file". The mask is only
    /// ever an input to a skip decision, so the two ways of saying "don't know"
    /// (`None` here, `true` in the mask) both degrade to scanning.
    ///
    /// `actual_row_groups` is the count read from the Parquet footer, and it must
    /// match the recorded index exactly. A mismatch means the index describes a
    /// *different* file than the one about to be read — a path collision, a
    /// half-written sidecar, a writer change — and applying it would skip row groups
    /// by index into the wrong file, which is the one failure mode that loses real
    /// matches rather than merely costing a scan. So a mismatch reads as "not
    /// recorded", never as a partial license.
    ///
    /// Only `MessageContains` narrows the mask. Equality predicates are already
    /// handled inside Parquet by the bloom filters on `service`/`level`/`trace_id`,
    /// which prune row groups better than a per-row-group value list would.
    pub fn row_groups_to_scan(
        &self,
        preds: &[Predicate],
        actual_row_groups: usize,
    ) -> Option<Vec<bool>> {
        let rgs = self.row_group_trigrams.as_ref()?;
        if rgs.len() != actual_row_groups {
            return None;
        }
        let terms: Vec<&str> = preds
            .iter()
            .filter_map(|p| match p {
                Predicate::MessageContains(t) => Some(t.as_str()),
                Predicate::Equals { .. } => None,
            })
            .collect();
        if terms.is_empty() {
            return None;
        }
        Some(
            rgs.iter()
                .map(|set| terms.iter().all(|t| set.contains_term(t).unwrap_or(true)))
                .collect(),
        )
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

/// The trigram sidecar: `file path -> trigram set`, stored separately from the
/// manifest and fetched only by queries with a text predicate.
///
/// **Why keying by path makes this safe.** Data files are immutable — compaction
/// writes new paths rather than rewriting one — so a path's trigram set is
/// correct forever once written. A sidecar that has fallen behind the manifest
/// can therefore only be *missing* entries, never hold wrong ones, and a missing
/// entry yields `None`, which never prunes. That removes the need for an atomic
/// two-object commit: the stale case degrades to "scans more files than
/// necessary", not "drops a real match".
///
/// The failure direction is the whole design. Anything that made a stale sidecar
/// able to *contradict* the manifest — reusing a path, or keying by index —
/// would turn a benign staleness into silent data loss at query time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TrigramIndex {
    /// Keyed by [`DataFile::path`].
    #[serde(default)]
    pub by_path: BTreeMap<String, TrigramSet>,
    /// Per-row-group sets, same keying, in row-group order.
    ///
    /// A separate map rather than a richer value type in `by_path`, so a sidecar
    /// written before row-group indexes existed still deserializes: the field is
    /// simply absent and every file reads as "row groups not recorded". The
    /// duplicated path keys cost a little JSON and buy a format change that cannot
    /// break a reader in either direction.
    #[serde(default)]
    pub row_groups_by_path: BTreeMap<String, Vec<TrigramSet>>,
}

impl TrigramIndex {
    pub fn is_empty(&self) -> bool {
        self.by_path.is_empty() && self.row_groups_by_path.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_path.len()
    }

    pub fn insert(&mut self, path: impl Into<String>, set: TrigramSet) {
        self.by_path.insert(path.into(), set);
    }

    pub fn insert_row_groups(&mut self, path: impl Into<String>, sets: Vec<TrigramSet>) {
        self.row_groups_by_path.insert(path.into(), sets);
    }

    /// Drop entries for files no longer in `manifest`, so the sidecar does not
    /// grow forever as compaction retires paths. Only ever called by the writer
    /// that also commits the manifest.
    pub fn retain_paths(&mut self, manifest: &Manifest) {
        let live: std::collections::BTreeSet<&str> =
            manifest.files.iter().map(|f| f.path.as_str()).collect();
        self.by_path.retain(|p, _| live.contains(p.as_str()));
        self.row_groups_by_path
            .retain(|p, _| live.contains(p.as_str()));
    }
}

impl Manifest {
    /// Attach trigram sets from the sidecar, in place.
    ///
    /// Call this before pruning a query that carries a `MessageContains`
    /// predicate. Files with no sidecar entry keep `None` and stay unprunable on
    /// text, which is the conservative direction.
    pub fn hydrate_trigrams(&mut self, index: &TrigramIndex) {
        for f in &mut self.files {
            f.message_trigrams = index.by_path.get(&f.path).cloned();
            f.row_group_trigrams = index.row_groups_by_path.get(&f.path).cloned();
        }
    }

    /// Collect the trigram sets currently attached, for writing to the sidecar.
    pub fn extract_trigrams(&self) -> TrigramIndex {
        let mut idx = TrigramIndex::default();
        for f in &self.files {
            if let Some(t) = &f.message_trigrams {
                idx.insert(f.path.clone(), t.clone());
            }
            if let Some(rgs) = &f.row_group_trigrams {
                idx.insert_row_groups(f.path.clone(), rgs.clone());
            }
        }
        idx
    }
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
            row_groups: 0,
            row_group_trigrams: None,
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
            row_groups: 0,
            row_group_trigrams: None,
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
            row_groups: 0,
            row_group_trigrams: None,
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
            row_groups: 0,
            row_group_trigrams: None,
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
