//! Deterministic Simulation Tests (ADR-001) for the object-store seam.
//!
//! These are the first real DST scenarios: the production control-plane logic —
//! the cost estimator, lifecycle tiering, and the Glacier restore state machine —
//! runs against [`SimObjectStore`] in *logical* time. A "months-long" tiering run
//! or an "8-hour" Glacier thaw completes in microseconds of wall-clock because the
//! `SimClock` only advances when code sleeps; no real bytes move and no real time
//! passes. Everything here is seeded and reproducible.
//!
//! What each test pins down:
//! - `glacier_restore_workflow_runs_in_logical_time` — the restore state machine:
//!   a cold object is unreadable until restore is requested *and* the modeled thaw
//!   time has elapsed in sim time.
//! - `estimator_matches_what_the_store_bills` — the load-bearing invariant of
//!   ADR-001: the pre-query cost estimate equals what the store actually meters,
//!   because both compute from `verdigris_core::cost`.
//! - `tiering_fast_forwards_months_in_milliseconds` — lifecycle demotions across
//!   simulated months, ending with a cold object that now needs a restore.
//! - `fabricated_catalog_prices_a_trillion_rows_without_bytes` — metadata-scale
//!   planning with no data behind it.

use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

use verdigris_core::clock::{Clock, Millis, SimClock};
use verdigris_core::cost::{self, RetrievalMode};
use verdigris_core::estimate::estimate_scan;
use verdigris_core::manifest::{DataFile, Manifest};
use verdigris_core::model::{StorageClass, Tier};
use verdigris_storage::SimObjectStore;

const MIB: u64 = 1024 * 1024;
const DAY_MS: Millis = 24 * 60 * 60 * 1000;

fn sim() -> (Arc<SimClock>, SimObjectStore) {
    let clock = Arc::new(SimClock::new(0));
    (clock.clone(), SimObjectStore::new(clock))
}

/// A cold object is locked until a restore is requested and the modeled thaw time
/// has elapsed — and that thaw "takes hours" without the test waiting at all.
#[tokio::test]
async fn glacier_restore_workflow_runs_in_logical_time() {
    let (clock, store) = sim();
    let p = Path::from("logs/cold/2026-01-01.parquet");
    store
        .put(&p, PutPayload::from_static(b"a cold archived log line"))
        .await
        .unwrap();
    store.set_class(&p, StorageClass::GlacierFlexible);

    // Cold and un-restored: not queryable in place.
    assert!(store.get(&p).await.is_err());

    // Bulk restore — the cheapest, slowest mode (~8h in the model).
    let ready_at = store.request_restore(&p, RetrievalMode::Bulk);
    let expected_thaw = cost::restore_latency_ms(StorageClass::GlacierFlexible, RetrievalMode::Bulk);
    assert!(expected_thaw >= 8 * 60 * 60 * 1000, "bulk thaw is hours");
    assert_eq!(ready_at, clock.now_millis() + expected_thaw, "ready at now + thaw");

    // Halfway through the thaw it is still locked.
    clock.advance(expected_thaw / 2);
    assert!(store.get(&p).await.is_err(), "still thawing at the halfway mark");

    // Once the modeled thaw has fully elapsed in *sim* time, it reads.
    clock.advance(expected_thaw / 2);
    let got = store.get(&p).await.unwrap().bytes().await.unwrap();
    assert_eq!(&*got, b"a cold archived log line");

    // The simulated clock jumped ~8 hours; the test itself took microseconds.
    assert!(clock.now_millis() >= expected_thaw);
}

/// ADR-001's load-bearing invariant: the estimate shown to the user before a scan
/// equals what the store actually bills, across all tiers, because both read the
/// same `verdigris_core::cost` model. If these ever diverge, the sim lies about
/// production cost.
#[tokio::test]
async fn estimator_matches_what_the_store_bills() {
    let (clock, store) = sim();
    let retrieval = RetrievalMode::Standard;

    // One file per tier, sizes chosen so each tier contributes real bytes.
    let files = [
        ("logs/hot/h.parquet", Tier::Hot, 8 * MIB),
        ("logs/warm/w.parquet", Tier::Warm, 16 * MIB),
        ("logs/cold/c.parquet", Tier::Cold, 64 * MIB),
    ];

    let mut manifest = Manifest::new("logs");
    for (path, tier, bytes) in files {
        manifest.add(DataFile {
            path: path.into(),
            bytes,
            rows: bytes / 256,
            min_ts: 0,
            max_ts: 1_000,
            tier,
        });
        // Materialize the bytes and tag the object's storage class to match.
        let payload = PutPayload::from(vec![0u8; bytes as usize]);
        let p = Path::from(path);
        store.put(&p, payload).await.unwrap();
        store.set_class(&p, tier.default_class());
    }

    // What we'd tell the user before running the query.
    let estimate = estimate_scan(
        &manifest,
        &[Tier::Hot, Tier::Warm, Tier::Cold],
        None,
        1e9,
        retrieval,
    );
    assert_eq!(estimate.files_touched, 3);
    assert!(estimate.cost_usd > 0.0);
    assert!(estimate.cold_restore, "the cold file forces a restore");

    // Now actually scan every touched file through the store. Cold tiers must be
    // restored first; the store meters retrieval dollars as it serves bytes.
    for (path, tier, _) in files {
        let p = Path::from(path);
        if cost::restore_latency_ms(tier.default_class(), retrieval) > 0 {
            let ready = store.request_restore(&p, retrieval);
            clock.advance(ready.saturating_sub(clock.now_millis()));
        }
        let _ = store.get(&p).await.unwrap().bytes().await.unwrap();
    }

    // Predicted == billed.
    let billed = store.metered_retrieval_usd();
    assert!(
        (billed - estimate.cost_usd).abs() < 1e-9,
        "estimator said ${:.10} but store billed ${:.10}",
        estimate.cost_usd,
        billed
    );
    assert_eq!(store.metered_get_bytes(), (8 + 16 + 64) * MIB);
}

/// Fast-forward a single object through the default lifecycle (hot→warm at 3d,
/// warm→cold at 30d) across simulated months. The whole arc runs in microseconds,
/// and the object that started hot-and-instant ends cold-and-frozen.
#[tokio::test]
async fn tiering_fast_forwards_months_in_milliseconds() {
    use verdigris_core::config::LifecycleConfig;
    let lc = LifecycleConfig::default();
    let (clock, store) = sim();

    let p = Path::from("logs/app.parquet");
    store.put(&p, PutPayload::from_static(b"log")).await.unwrap();

    // Day 0: hot, queried in place.
    assert_eq!(store.class_of(&p), StorageClass::Standard);
    assert!(store.is_readable(&p));

    // Apply the age-based transition the lifecycle policy encodes.
    let demote = |age_days: u32| {
        if age_days >= lc.warm_to_cold_days {
            Some(Tier::Cold.default_class())
        } else if age_days >= lc.hot_to_warm_days {
            Some(Tier::Warm.default_class())
        } else {
            None
        }
    };

    // ~Day 5: demoted to warm (Glacier Instant) — still readable in place.
    clock.advance(5 * DAY_MS);
    store.set_class(&p, demote(5).unwrap());
    assert_eq!(store.class_of(&p), StorageClass::GlacierInstant);
    assert!(store.is_readable(&p));

    // ~Day 45: demoted to cold (Glacier Flexible) — now needs a restore.
    clock.advance(40 * DAY_MS);
    store.set_class(&p, demote(45).unwrap());
    assert_eq!(store.class_of(&p), StorageClass::GlacierFlexible);
    assert!(!store.is_readable(&p), "cold object must be restored to read");
    assert!(store.get(&p).await.is_err());

    // We simulated 45 days of lifecycle in a test that ran in microseconds.
    assert!(clock.now_millis() >= 45 * DAY_MS);
}

/// The "trillion rows in seconds" trick: the catalog *declares* an enormous table,
/// the planner/estimator works over the declared file count, and no bytes exist.
#[tokio::test]
async fn fabricated_catalog_prices_a_trillion_rows_without_bytes() {
    // Declare 1,000,000 cold files of 256 MiB each — ~256 TB, no bytes behind it.
    let mut manifest = Manifest::new("huge");
    let file_count = 1_000_000u64;
    let per_file = 256 * MIB;
    for i in 0..file_count {
        manifest.add(DataFile {
            path: format!("logs/cold/part-{i:08}.parquet"),
            bytes: per_file,
            rows: 4_000_000,
            min_ts: 0,
            max_ts: 1_000_000,
            tier: Tier::Cold,
        });
    }
    assert_eq!(manifest.files.len() as u64, file_count);
    assert_eq!(manifest.total_rows(), file_count * 4_000_000); // 4 trillion rows

    let estimate = estimate_scan(&manifest, &[Tier::Cold], None, 1e9, RetrievalMode::Standard);
    assert_eq!(estimate.files_touched as u64, file_count);
    assert!(estimate.cold_restore);
    // Sanity: cost == declared GiB × the cold-standard retrieval rate.
    let gib = (file_count * per_file) as f64 / cost::GIB;
    let expected = gib * cost::retrieval_usd_per_gib(StorageClass::GlacierFlexible, RetrievalMode::Standard);
    assert!((estimate.cost_usd - expected).abs() < 1e-6);
}
