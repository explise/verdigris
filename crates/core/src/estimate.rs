//! The real, query-aware scan estimator.
//!
//! Predicts what a query will scan *before running it*, purely from manifest
//! metadata (no data read). Files are pruned by the selected tiers and by the
//! query's time window (each file's `min_ts`/`max_ts`). The dollar figure is
//! exact (bytes × published retrieval rates — you pay for whole files you
//! retrieve); the time figure is modeled from the provisioned throughput dial
//! and is only as good as its calibration.
//!
//! Pure and sans-I/O: the shell supplies `now` (for the window) and throughput.

use crate::cost::{self, RetrievalMode};
use crate::manifest::{DataFile, Manifest, Predicate};
use crate::model::Tier;

#[derive(Debug, Clone, PartialEq)]
pub struct TierEstimate {
    pub tier: Tier,
    pub bytes: u64,
    pub gib: f64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryEstimate {
    pub scan_bytes: u64,
    pub scan_gib: f64,
    pub cost_usd: f64,
    /// Restore latency to make the coldest touched tier scannable (ms).
    pub restore_ms: u64,
    /// Modeled scan time at the provisioned throughput (ms).
    pub scan_ms: u64,
    /// True when a touched tier needs a Glacier restore before it can be read.
    pub cold_restore: bool,
    pub files_touched: usize,
    pub files_total: usize,
    pub per_tier: Vec<TierEstimate>,
}

fn overlaps(f: &DataFile, window: Option<(i64, i64)>) -> bool {
    match window {
        Some((from, to)) => f.max_ts >= from && f.min_ts <= to,
        None => true,
    }
}

/// The files a query over `tiers` (optionally bounded to `window`, and to the
/// equality `preds` on stat columns) would touch — the single source of truth
/// shared by the cost *estimate* and the *executed scan*. Registering exactly this
/// set at query time means the quoted cost and the query it gates always read the
/// same files (no "estimated hot, scanned cold" surprise bills).
///
/// Pruning is layered and all metadata-only: tier → time window (`min_ts`/`max_ts`)
/// → per-file value stats (`service`/`level`). A file survives only if it passes
/// every layer; `preds` can only ever remove a file proven free of the value
/// (`DataFile::may_match`), so pruning never drops a real match.
pub fn select_files<'a>(
    manifest: &'a Manifest,
    tiers: &[Tier],
    window: Option<(i64, i64)>,
    preds: &[Predicate],
) -> Vec<&'a DataFile> {
    manifest
        .files
        .iter()
        .filter(|f| tiers.contains(&f.tier) && overlaps(f, window) && f.may_match(preds))
        .collect()
}

/// Estimate the scan for a query touching `tiers`, optionally bounded to `window`
/// and to the equality `preds` on stat columns (`service`/`level`).
/// `throughput_bytes_per_sec` is the provisioned query throughput (cores × rate).
pub fn estimate_scan(
    manifest: &Manifest,
    tiers: &[Tier],
    window: Option<(i64, i64)>,
    preds: &[Predicate],
    throughput_bytes_per_sec: f64,
    retrieval: RetrievalMode,
) -> QueryEstimate {
    // Bytes of touched files per tier (index 0/1/2 = hot/warm/cold).
    let mut per = [0u64; 3];
    let mut files_touched = 0;
    for f in select_files(manifest, tiers, window, preds) {
        per[f.tier.index()] += f.bytes;
        files_touched += 1;
    }

    let mut per_tier = Vec::new();
    let mut scan_bytes = 0u64;
    let mut cost_usd = 0.0;
    let mut restore_ms = 0u64;
    let mut cold_restore = false;
    for tier in Tier::ALL {
        let bytes = per[tier.index()];
        if bytes == 0 {
            continue;
        }
        scan_bytes += bytes;
        let gib = bytes as f64 / cost::GIB;
        let class = tier.default_class();
        let c = gib * cost::retrieval_usd_per_gib(class, retrieval);
        cost_usd += c;
        let r = cost::restore_latency_ms(class, retrieval);
        restore_ms = restore_ms.max(r);
        if r > 0 {
            cold_restore = true;
        }
        per_tier.push(TierEstimate {
            tier,
            bytes,
            gib,
            cost_usd: c,
        });
    }

    let scan_ms = if throughput_bytes_per_sec > 0.0 {
        (scan_bytes as f64 / throughput_bytes_per_sec * 1000.0) as u64
    } else {
        0
    };

    QueryEstimate {
        scan_bytes,
        scan_gib: scan_bytes as f64 / cost::GIB,
        cost_usd,
        restore_ms,
        scan_ms,
        cold_restore,
        files_touched,
        files_total: manifest.files.len(),
        per_tier,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        }
    }

    fn manifest() -> Manifest {
        let mut m = Manifest::new("logs");
        // hot files at t=0..100 and t=900..1000; a cold file at t=0..1000.
        m.add(file("logs/hot/a", Tier::Hot, 1000, 0, 100));
        m.add(file("logs/hot/b", Tier::Hot, 2000, 900, 1000));
        m.add(file("logs/cold/c", Tier::Cold, 5000, 0, 1000));
        m
    }

    #[test]
    fn time_window_prunes_files() {
        let m = manifest();
        // Window [800,1000], hot only: only the second hot file overlaps.
        let e = estimate_scan(&m, &[Tier::Hot], Some((800, 1000)), &[], 1e9, RetrievalMode::Standard);
        assert_eq!(e.scan_bytes, 2000);
        assert_eq!(e.files_touched, 1);
        assert_eq!(e.cost_usd, 0.0); // hot retrieval is free
        assert!(!e.cold_restore);
    }

    #[test]
    fn cold_tier_costs_and_needs_restore() {
        let m = manifest();
        let e = estimate_scan(&m, &[Tier::Cold], None, &[], 1e9, RetrievalMode::Standard);
        assert_eq!(e.scan_bytes, 5000);
        assert!(e.cost_usd > 0.0); // glacier flexible standard ~ $0.01/GB
        assert!(e.cold_restore);
        assert!(e.restore_ms > 0);
    }

    #[test]
    fn no_window_sums_selected_tiers() {
        let m = manifest();
        let e = estimate_scan(
            &m,
            &[Tier::Hot, Tier::Cold],
            None,
            &[],
            1e9,
            RetrievalMode::Standard,
        );
        assert_eq!(e.scan_bytes, 1000 + 2000 + 5000);
        assert_eq!(e.files_touched, 3);
    }

    fn trigrams_of(text: &str) -> Option<crate::text::TrigramSet> {
        let mut t = crate::text::TrigramSet::new();
        t.insert_text(text);
        Some(t)
    }

    #[test]
    fn value_predicate_prunes_files_it_proves_empty() {
        use crate::manifest::Predicate;
        let mut m = Manifest::new("logs");
        // Two hot files with recorded service stats; one covers auth, one billing.
        m.add(DataFile {
            path: "logs/hot/auth".into(), bytes: 1000, rows: 1, min_ts: 0, max_ts: 10,
            tier: Tier::Hot, services: vec!["auth".into()], levels: vec!["ERROR".into()],
            message_trigrams: trigrams_of("connection timeout to db-primary"),
        });
        m.add(DataFile {
            path: "logs/hot/billing".into(), bytes: 3000, rows: 1, min_ts: 0, max_ts: 10,
            tier: Tier::Hot, services: vec!["billing".into()], levels: vec!["INFO".into()],
            message_trigrams: trigrams_of("invoice generated"),
        });
        // A legacy file with no stats must always be scanned (no false prune).
        m.add(DataFile {
            path: "logs/hot/legacy".into(), bytes: 500, rows: 1, min_ts: 0, max_ts: 10,
            tier: Tier::Hot, services: vec![], levels: vec![],
            message_trigrams: None,
        });

        // service:auth → the billing file is proven empty and skipped; auth + legacy stay.
        let e = estimate_scan(
            &m, &[Tier::Hot], None, &[Predicate::service("auth")], 1e9, RetrievalMode::Standard,
        );
        assert_eq!(e.scan_bytes, 1000 + 500, "billing file pruned, legacy kept");
        assert_eq!(e.files_touched, 2);

        // Free text `timeout` → only the auth file's trigrams admit it; the
        // billing file is proven match-free, the stat-less legacy file stays.
        let e = estimate_scan(
            &m, &[Tier::Hot], None, &[Predicate::message_contains("timeout")],
            1e9, RetrievalMode::Standard,
        );
        assert_eq!(e.scan_bytes, 1000 + 500, "free text pruned billing, kept legacy");
        // A substring *inside* a word must not prune the file containing it.
        let e = estimate_scan(
            &m, &[Tier::Hot], None, &[Predicate::message_contains("imeou")],
            1e9, RetrievalMode::Standard,
        );
        assert_eq!(e.scan_bytes, 1000 + 500, "in-word substring still matches auth file");
        // A term too short to judge (< 3 chars) never prunes anything.
        let e = estimate_scan(
            &m, &[Tier::Hot], None, &[Predicate::message_contains("db")],
            1e9, RetrievalMode::Standard,
        );
        assert_eq!(e.files_touched, 3, "short terms cannot prove absence");

        // No predicate → nothing pruned by value.
        let all = estimate_scan(&m, &[Tier::Hot], None, &[], 1e9, RetrievalMode::Standard);
        assert_eq!(all.files_touched, 3);
    }

    // The executed scan (which registers `select_files`) and the cost estimate
    // MUST touch the same file set — this is the M4.1 "no surprise bills"
    // guarantee. Exercise both tier and window filtering.
    #[test]
    fn select_files_matches_what_the_estimate_prices() {
        let m = manifest();
        for (tiers, window) in [
            (vec![Tier::Hot], None),
            (vec![Tier::Hot], Some((800, 1000))),
            (vec![Tier::Cold], None),
            (vec![Tier::Hot, Tier::Cold], None),
            (vec![Tier::Warm], None), // no warm files → empty
        ] {
            let selected = select_files(&m, &tiers, window, &[]);
            let est = estimate_scan(&m, &tiers, window, &[], 1e9, RetrievalMode::Standard);
            // Same count and same bytes the estimate charged for.
            assert_eq!(selected.len(), est.files_touched, "count parity {tiers:?} {window:?}");
            let bytes: u64 = selected.iter().map(|f| f.bytes).sum();
            assert_eq!(bytes, est.scan_bytes, "bytes parity {tiers:?} {window:?}");
        }
        // Hot-only never includes the cold file (the core of the M4.1 bug).
        let hot = select_files(&m, &[Tier::Hot], None, &[]);
        assert!(hot.iter().all(|f| f.tier == Tier::Hot));
        assert_eq!(hot.len(), 2);
    }
}
