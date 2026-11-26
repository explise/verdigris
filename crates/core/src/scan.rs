//! The single scan plan shared by the cost estimator and query execution.
//!
//! Before this module, the estimate endpoint and `/v1/query` each assembled
//! their own inputs: they parsed the query separately, normalized tiers
//! separately, and could disagree (an omitted tier list priced *nothing* in the
//! estimator while the executor scanned every tier). This module makes the
//! pruning inputs and the selected file set one value, built once:
//!
//! 1. compile the DSL (or pass raw SQL through),
//! 2. extract the prunable predicates and time window,
//! 3. normalize the tier selection,
//! 4. select the files those rules admit, and carry the SQL that will run.
//!
//! The estimator prices [`ScanPlan::files`]; the executor registers exactly
//! those files. A quote therefore covers the files the query will read.

use crate::estimate::select_files;
use crate::manifest::{DataFile, Manifest, Predicate};
use crate::model::Tier;
use crate::search;

/// Default row limit for a search-DSL query, shared by the estimate and the
/// execution path so both compile the *same* SQL.
pub const DEFAULT_QUERY_LIMIT: usize = 200;

/// Everything needed to price and execute one query over one manifest snapshot.
#[derive(Debug, Clone)]
pub struct ScanPlan<'a> {
    /// The SQL that will actually execute (raw SQL passed through, or the DSL
    /// compiled by [`search::to_sql`]).
    pub sql: String,
    /// Tiers the scan is scoped to; empty input normalizes to all tiers.
    pub tiers: Vec<Tier>,
    /// Time window implied by a `| last …` command, if any.
    pub window: Option<(i64, i64)>,
    /// File-prunable predicates extracted from the query.
    pub preds: Vec<Predicate>,
    /// Files that survive tier + window + predicate pruning. This is the one
    /// set both the estimate and the execution consume.
    pub files: Vec<&'a DataFile>,
    /// Files in the manifest snapshot, for "touched / total" reporting.
    pub files_total: usize,
}

impl ScanPlan<'_> {
    /// Bytes the plan will read. Charged at file granularity, which is what a
    /// cold-tier retrieval bills and what the estimator quotes.
    pub fn scanned_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }
}

/// Normalize a tier selection: an empty slice means "all tiers" (the API's
/// documented default), and duplicates/unknown orderings collapse to the
/// canonical hot→cold order so the plan is deterministic.
pub fn normalize_tiers(tiers: &[Tier]) -> Vec<Tier> {
    if tiers.is_empty() {
        return Tier::ALL.to_vec();
    }
    Tier::ALL
        .into_iter()
        .filter(|t| tiers.contains(t))
        .collect()
}

/// Build the single scan plan for `input` against `manifest`.
///
/// `table` is used only when compiling the search DSL; raw SQL passes through
/// untouched. `now_millis` is supplied by the shell so the `last 1h` window is
/// deterministic and testable.
pub fn plan_scan<'a>(
    manifest: &'a Manifest,
    input: &str,
    table: &str,
    tiers: &[Tier],
    now_millis: i64,
    limit: usize,
) -> Result<ScanPlan<'a>, String> {
    let preds = search::stat_predicates(input);
    let window = search::time_window(input, now_millis);
    let sql = if search::looks_like_sql(input) {
        input.to_string()
    } else {
        search::to_sql(input, table, now_millis, limit)?
    };
    let tiers = normalize_tiers(tiers);
    let files = select_files(manifest, &tiers, window, &preds);
    Ok(ScanPlan {
        sql,
        tiers,
        window,
        preds,
        files,
        files_total: manifest.files.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::DataFile;

    fn file(path: &str, tier: Tier, bytes: u64, min_ts: i64, max_ts: i64) -> DataFile {
        DataFile {
            path: path.into(),
            bytes,
            rows: 1,
            min_ts,
            max_ts,
            tier,
            services: vec![],
            levels: vec![],
            message_trigrams: None,
            row_groups: 0,
            row_group_trigrams: None,
        }
    }

    fn manifest() -> Manifest {
        let mut m = Manifest::new("logs");
        m.add(file("logs/hot/a.parquet", Tier::Hot, 1_000, 0, 100));
        m.add(file("logs/hot/b.parquet", Tier::Hot, 2_000, 900, 1_000));
        m.add(file("logs/cold/c.parquet", Tier::Cold, 5_000, 0, 1_000));
        m
    }

    #[test]
    fn omitted_tiers_mean_all_tiers_in_both_paths() {
        // This is the quote-vs-scan bug the plan exists to prevent: an empty
        // tier list must not price zero while execution scans everything.
        let m = manifest();
        let plan = plan_scan(&m, "", "logs", &[], 1_000, DEFAULT_QUERY_LIMIT).unwrap();
        assert_eq!(plan.tiers, Tier::ALL.to_vec());
        assert_eq!(plan.files.len(), 3);
        assert_eq!(plan.scanned_bytes(), 8_000);
    }

    #[test]
    fn tier_window_and_predicates_are_one_selection() {
        let m = manifest();
        let plan = plan_scan(
            &m,
            "service:auth | last 1s",
            "logs",
            &[Tier::Hot, Tier::Hot],
            1_000,
            DEFAULT_QUERY_LIMIT,
        )
        .unwrap();
        // Window is [0,1000] here, so both hot files survive; duplicate tier
        // entries collapse, and the cold file is never included.
        assert_eq!(plan.tiers, vec![Tier::Hot]);
        assert_eq!(plan.window, Some((0, 1_000)));
        assert_eq!(plan.files.len(), 2);
        assert!(plan.sql.starts_with("SELECT ts, level, service"));
        assert!(plan.sql.contains("service = 'auth'"));
        assert!(plan.sql.contains("LIMIT 200"));
    }

    #[test]
    fn raw_sql_passes_through_and_selects_all_tiers() {
        let m = manifest();
        let sql = "SELECT count(*) FROM logs";
        let plan = plan_scan(&m, sql, "logs", &[], 0, 50).unwrap();
        assert_eq!(plan.sql, sql);
        assert!(plan.preds.is_empty());
        assert_eq!(plan.files.len(), 3);
    }

    #[test]
    fn malformed_dsl_is_an_error_not_an_empty_plan() {
        let m = manifest();
        assert!(plan_scan(&m, "status>=abc", "logs", &[], 0, DEFAULT_QUERY_LIMIT).is_err());
    }

    #[test]
    fn normalize_tiers_is_canonical_and_defaults_to_all() {
        assert_eq!(normalize_tiers(&[]), Tier::ALL.to_vec());
        assert_eq!(
            normalize_tiers(&[Tier::Cold, Tier::Hot]),
            vec![Tier::Hot, Tier::Cold]
        );
        assert_eq!(normalize_tiers(&[Tier::Warm]), vec![Tier::Warm]);
    }
}
