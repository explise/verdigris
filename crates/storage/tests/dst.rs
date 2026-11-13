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
//!
//! # Seeds and faults
//!
//! This is a seed *sweep*, not a fixed-seed suite. `DST_SEED_FIRST` and
//! `DST_SEED_COUNT` choose the seeds each scenario runs and `DST_FAULT_PPM` the
//! per-operation injected-fault rate, so the same binary serves three scales:
//!
//! | Lane | Command | Seeds |
//! |---|---|---|
//! | dev / `cargo test` | plain run | 1 (`DST_SEED_FIRST=0`) |
//! | per-PR batch | `DST_SEED_COUNT=256` | 256 |
//! | nightly | `DST_SEED_COUNT=100000` | 100k in a rotating window |
//!
//! Sweeping seeds with faults off would test nothing — with no faults the seed
//! never changes the run — so every swept scenario injects faults and retries the
//! operations that may fail through [`Dst::retry`]. Each seed runs twice and the
//! two runs must agree: a seed that passes once and fails on an identical re-run
//! is the one result this harness must never produce.
//!
//! `fabricated_catalog_prices_a_trillion_rows_without_bytes` is deliberately not
//! swept: it builds a million-file manifest for a metadata-only estimate, so there
//! is no store operation for a fault to reach, and its ~0.5 s runtime would
//! dominate a batch of seeds that each finish in microseconds.

use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use object_store::path::Path;
use object_store::{GetResult, ObjectStoreExt, PutPayload};

use verdigris_core::clock::{Clock, Millis, SimClock};
use verdigris_core::cost::{self, RetrievalMode};
use verdigris_core::estimate::estimate_scan;
use verdigris_core::manifest::{DataFile, Manifest};
use verdigris_core::model::{StorageClass, Tier};
use verdigris_storage::SimObjectStore;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * 1024;
const DAY_MS: Millis = 24 * 60 * 60 * 1000;

/// Default injected-fault rate per fallible store operation. 5% explores long
/// fault sequences without turning every operation into a failure.
const DEFAULT_FAULT_PPM: u32 = 50_000;

/// Attempts before a retried operation is reported as a hard failure. At 5% a
/// run of 8 consecutive faults on one operation is a ~4e-11 event.
const MAX_ATTEMPTS: u32 = 8;

/// A batch at least this large must show faults, or the sweep is a green lane
/// that tests nothing (the `with_fault_rate_ppm(0)` trap). The per-PR batch is
/// 256; at 5% the thinnest scenario (tiering, one retried operation per run)
/// still lands ~10 faulted seeds, so zero is not a plausible clean result.
/// Smaller ad-hoc runs make no assertion — a dev's one-seed run may well be
/// fault-free.
const FAULT_ASSERT_MIN_SEEDS: u64 = 256;

/// Which seeds and fault rate this run uses. Env-driven so one binary is the
/// dev lane, the per-PR batch, and the nightly sweep.
struct Sweep {
    first: u64,
    count: u64,
    fault_ppm: u32,
}

impl Sweep {
    fn from_env() -> Self {
        fn num<T>(name: &str, default: T) -> T
        where
            T: std::str::FromStr,
            T::Err: std::fmt::Display,
        {
            match std::env::var(name) {
                Ok(v) => v
                    .parse()
                    .unwrap_or_else(|e| panic!("{name}={v:?} is not a valid number: {e}")),
                Err(_) => default,
            }
        }
        Self {
            first: num("DST_SEED_FIRST", 0),
            count: num("DST_SEED_COUNT", 1),
            fault_ppm: num("DST_FAULT_PPM", DEFAULT_FAULT_PPM),
        }
    }

    fn seeds(&self) -> std::ops::Range<u64> {
        self.first..self.first.saturating_add(self.count)
    }
}

/// One scenario run: a fresh sim store, its clock, and the retry count. The
/// `Cell` is sound here because a run never spans threads — the whole point of
/// the sim store is that a scenario is a single-threaded sequence.
struct Dst {
    clock: Arc<SimClock>,
    store: SimObjectStore,
    retries: Cell<u64>,
}

impl Dst {
    fn new(seed: u64, fault_ppm: u32) -> Self {
        let clock = Arc::new(SimClock::new(0));
        let store = SimObjectStore::new(clock.clone())
            .with_seed(seed)
            .with_fault_rate_ppm(fault_ppm);
        Self {
            clock,
            store,
            retries: Cell::new(0),
        }
    }

    /// Run `op` until it succeeds, counting the injected faults it absorbed.
    ///
    /// Only for calls the scenario expects to *succeed*. A call whose failure is
    /// itself the assertion — a cold object refusing a GET — must go to
    /// `self.store` directly, because retrying would swallow the result under
    /// test (and, worse, a fault could make the refusal pass for the wrong
    /// reason).
    async fn retry<T, F, Fut>(&self, mut op: F) -> T
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let mut last = None;
        for _ in 0..MAX_ATTEMPTS {
            match op().await {
                Ok(v) => return v,
                Err(e) => {
                    self.retries.set(self.retries.get() + 1);
                    last = Some(e);
                }
            }
        }
        panic!(
            "{MAX_ATTEMPTS} consecutive faults on one operation: {}",
            last.expect("the loop only exits after a failure")
        );
    }

    async fn put(&self, path: &Path, payload: PutPayload) {
        self.retry(|| self.store.put(path, payload.clone())).await;
    }

    async fn get(&self, path: &Path) -> GetResult {
        self.retry(|| self.store.get(path)).await
    }

    async fn get_bytes(&self, path: &Path) -> bytes::Bytes {
        self.get(path)
            .await
            .bytes()
            .await
            .expect("in-memory object materializes")
    }
}

/// The scenarios the sweep drives. [`Scenario::name`] is what a failure report
/// prints, so it matches the `#[tokio::test]` that drives the scenario.
#[derive(Clone, Copy)]
enum Scenario {
    GlacierRestore,
    EstimatorVsBilled,
    Tiering,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::GlacierRestore => "glacier_restore_workflow_runs_in_logical_time",
            Scenario::EstimatorVsBilled => "estimator_matches_what_the_store_bills",
            Scenario::Tiering => "tiering_fast_forwards_months_in_milliseconds",
        }
    }

    async fn run(self, dst: &Dst) {
        match self {
            Scenario::GlacierRestore => glacier_restore(dst).await,
            Scenario::EstimatorVsBilled => estimator_vs_billed(dst).await,
            Scenario::Tiering => tiering(dst).await,
        }
    }
}

/// The observable outcome of one run. Two runs of the same seed must agree on
/// every field; exact equality *is* the reproducibility assertion.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    retries: u64,
    end_millis: Millis,
    metered_bytes: u64,
    /// Compared as bits: float equality here is exactness, not an epsilon.
    metered_usd_bits: u64,
}

/// Run one scenario over the configured seed range, twice per seed.
async fn sweep(scenario: Scenario) {
    let cfg = Sweep::from_env();
    let mut faults = 0u64;
    let mut faulted_seeds = 0u64;

    for seed in cfg.seeds() {
        let first = match run_once(scenario, seed, cfg.fault_ppm).await {
            Ok(o) => o,
            Err(msg) => fail(scenario, seed, &cfg, format!("run failed: {msg}")),
        };
        // Same seed, fresh store: identical instructions must produce an
        // identical run, or the reported repro command is a lie.
        let again = match run_once(scenario, seed, cfg.fault_ppm).await {
            Ok(o) => o,
            Err(msg) => fail(
                scenario,
                seed,
                &cfg,
                format!("passed, then failed on an identical re-run: {msg}"),
            ),
        };
        if first != again {
            fail(
                scenario,
                seed,
                &cfg,
                format!("identical re-runs diverged: {first:?} then {again:?}"),
            );
        }
        faults += first.retries + again.retries;
        if first.retries > 0 {
            faulted_seeds += 1;
        }
    }

    // A sweep at zero faults replays one script under many seed names — exactly
    // the vacuous lane this harness exists to replace, and one env var away. At
    // batch scale it is refused rather than reported as a meaningless green.
    if cfg.count >= FAULT_ASSERT_MIN_SEEDS {
        assert!(
            cfg.fault_ppm > 0,
            "{} seeds at 0 ppm tests nothing — faults are what makes a seed \
             matter; set DST_FAULT_PPM",
            cfg.count
        );
        assert!(
            faults > 0,
            "{} seeds at {} ppm injected no faults — the sweep is not exercising \
             fault paths and a green result means nothing",
            cfg.count,
            cfg.fault_ppm
        );
        // If every seed faulted (or none did) the seed is not reaching the
        // store's RNG and each "seed" is replaying one stream.
        assert!(
            faulted_seeds < cfg.count,
            "all {} seeds injected the identical fault history — the seed does \
             not appear to drive fault placement",
            cfg.count
        );
    }

    println!(
        "dst {}: seeds {}..{}, {} faults across {} seeds ({} ppm)",
        scenario.name(),
        cfg.first,
        cfg.first.saturating_add(cfg.count),
        faults,
        faulted_seeds,
        cfg.fault_ppm
    );
}

async fn run_once(scenario: Scenario, seed: u64, fault_ppm: u32) -> Result<Outcome, String> {
    let dst = Dst::new(seed, fault_ppm);
    let run = AssertUnwindSafe(scenario.run(&dst)).catch_unwind();
    match run.await {
        Ok(()) => Ok(Outcome {
            retries: dst.retries.get(),
            end_millis: dst.clock.now_millis(),
            metered_bytes: dst.store.metered_get_bytes(),
            metered_usd_bits: dst.store.metered_retrieval_usd().to_bits(),
        }),
        Err(payload) => Err(panic_message(payload)),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

/// A scenario failure, made actionable: the seed, the scenario, the assertion
/// that broke, and a command that reruns exactly that seed.
fn fail(scenario: Scenario, seed: u64, cfg: &Sweep, detail: impl std::fmt::Display) -> ! {
    panic!(
        "\n\
         DST failure: scenario `{}`, seed {}\n\
         {}\n\
         reproduce with:\n  \
         DST_SEED_FIRST={} DST_SEED_COUNT=1 DST_FAULT_PPM={} \
         cargo test -p verdigris-storage --test dst\n",
        scenario.name(),
        seed,
        detail,
        seed,
        cfg.fault_ppm,
    )
}

/// A cold object is locked until a restore is requested and the modeled thaw time
/// has elapsed — and that thaw "takes hours" without the test waiting at all.
#[tokio::test]
async fn glacier_restore_workflow_runs_in_logical_time() {
    sweep(Scenario::GlacierRestore).await;
}

async fn glacier_restore(dst: &Dst) {
    let p = Path::from("logs/cold/2026-01-01.parquet");
    dst.put(&p, PutPayload::from_static(b"a cold archived log line"))
        .await;
    dst.store.set_class(&p, StorageClass::GlacierFlexible);

    // Cold and un-restored: not queryable in place. The infallible `is_readable`
    // assertion is the real one under fault injection — a faulted GET is an
    // error too, so asserting only on the GET would pass for the wrong reason.
    assert!(!dst.store.is_readable(&p));
    assert!(dst.store.get(&p).await.is_err());

    // Bulk restore — the cheapest, slowest mode (~8h in the model).
    let ready_at = dst.store.request_restore(&p, RetrievalMode::Bulk);
    let expected_thaw =
        cost::restore_latency_ms(StorageClass::GlacierFlexible, RetrievalMode::Bulk);
    assert!(expected_thaw >= 8 * 60 * 60 * 1000, "bulk thaw is hours");
    assert_eq!(
        ready_at,
        dst.clock.now_millis() + expected_thaw,
        "ready at now + thaw"
    );

    // Halfway through the thaw it is still locked.
    dst.clock.advance(expected_thaw / 2);
    assert!(
        !dst.store.is_readable(&p),
        "still thawing at the halfway mark"
    );
    assert!(dst.store.get(&p).await.is_err());

    // Once the modeled thaw has fully elapsed in *sim* time, it reads.
    dst.clock.advance(expected_thaw / 2);
    let got = dst.get_bytes(&p).await;
    assert_eq!(&*got, b"a cold archived log line");

    // The simulated clock jumped ~8 hours; the test itself took microseconds.
    assert!(dst.clock.now_millis() >= expected_thaw);
}

/// ADR-001's load-bearing invariant: the estimate shown to the user before a scan
/// equals what the store actually bills, across all tiers, because both read the
/// same `verdigris_core::cost` model. If these ever diverge, the sim lies about
/// production cost.
///
/// This one is swept: a retried GET meters only the retrieval that succeeded, so
/// transport faults must not move the billed total away from the estimate.
#[tokio::test]
async fn estimator_matches_what_the_store_bills() {
    sweep(Scenario::EstimatorVsBilled).await;
}

async fn estimator_vs_billed(dst: &Dst) {
    let retrieval = RetrievalMode::Standard;

    // One file per tier, sizes chosen so each tier contributes real bytes. The
    // sizes are KiB, not MiB: the sweep replays this thousands of times, and the
    // invariant below is about the *rate*, not the volume.
    let files = [
        ("logs/hot/h.parquet", Tier::Hot, 8 * KIB),
        ("logs/warm/w.parquet", Tier::Warm, 16 * KIB),
        ("logs/cold/c.parquet", Tier::Cold, 64 * KIB),
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
            services: vec![],
            levels: vec![],
            message_trigrams: None,
            row_groups: 0,
            row_group_trigrams: None,
        });
        // Materialize the bytes and tag the object's storage class to match.
        let payload = PutPayload::from(vec![0u8; bytes as usize]);
        let p = Path::from(path);
        dst.put(&p, payload).await;
        dst.store.set_class(&p, tier.default_class());
    }

    // What we'd tell the user before running the query.
    let estimate = estimate_scan(
        &manifest,
        &[Tier::Hot, Tier::Warm, Tier::Cold],
        None,
        &[],
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
            let ready = dst.store.request_restore(&p, retrieval);
            dst.clock
                .advance(ready.saturating_sub(dst.clock.now_millis()));
        }
        let _ = dst.get_bytes(&p).await;
    }

    // Predicted == billed.
    let billed = dst.store.metered_retrieval_usd();
    assert!(
        (billed - estimate.cost_usd).abs() < 1e-9,
        "estimator said ${:.10} but store billed ${:.10}",
        estimate.cost_usd,
        billed
    );
    assert_eq!(dst.store.metered_get_bytes(), (8 + 16 + 64) * KIB);
}

/// Fast-forward a single object through the default lifecycle (hot→warm at 3d,
/// warm→cold at 30d) across simulated months. The whole arc runs in microseconds,
/// and the object that started hot-and-instant ends cold-and-frozen.
#[tokio::test]
async fn tiering_fast_forwards_months_in_milliseconds() {
    sweep(Scenario::Tiering).await;
}

async fn tiering(dst: &Dst) {
    use verdigris_core::config::LifecycleConfig;
    let lc = LifecycleConfig::default();

    let p = Path::from("logs/app.parquet");
    dst.put(&p, PutPayload::from_static(b"log")).await;

    // Day 0: hot, queried in place.
    assert_eq!(dst.store.class_of(&p), StorageClass::Standard);
    assert!(dst.store.is_readable(&p));

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
    dst.clock.advance(5 * DAY_MS);
    dst.store.set_class(&p, demote(5).unwrap());
    assert_eq!(dst.store.class_of(&p), StorageClass::GlacierInstant);
    assert!(dst.store.is_readable(&p));

    // ~Day 45: demoted to cold (Glacier Flexible) — now needs a restore.
    dst.clock.advance(40 * DAY_MS);
    dst.store.set_class(&p, demote(45).unwrap());
    assert_eq!(dst.store.class_of(&p), StorageClass::GlacierFlexible);
    assert!(
        !dst.store.is_readable(&p),
        "cold object must be restored to read"
    );
    assert!(dst.store.get(&p).await.is_err());

    // We simulated 45 days of lifecycle in a test that ran in microseconds.
    assert!(dst.clock.now_millis() >= 45 * DAY_MS);
}

/// The "trillion rows in seconds" trick: the catalog *declares* an enormous table,
/// the planner/estimator works over the declared file count, and no bytes exist.
///
/// Not swept: it performs no store I/O (so a fault has nothing to inject into)
/// and building the million-file manifest costs ~0.5 s — enough to dominate a
/// batch whose other scenarios each run in microseconds.
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
            services: vec![],
            levels: vec![],
            message_trigrams: None,
            row_groups: 0,
            row_group_trigrams: None,
        });
    }
    assert_eq!(manifest.files.len() as u64, file_count);
    assert_eq!(manifest.total_rows(), file_count * 4_000_000); // 4 trillion rows

    let estimate = estimate_scan(
        &manifest,
        &[Tier::Cold],
        None,
        &[],
        1e9,
        RetrievalMode::Standard,
    );
    assert_eq!(estimate.files_touched as u64, file_count);
    assert!(estimate.cold_restore);
    // Sanity: cost == declared GiB × the cold-standard retrieval rate.
    let gib = (file_count * per_file) as f64 / cost::GIB;
    let expected =
        gib * cost::retrieval_usd_per_gib(StorageClass::GlacierFlexible, RetrievalMode::Standard);
    assert!((estimate.cost_usd - expected).abs() < 1e-6);
}
