# ADR-001: Deterministic Simulation Testing (DST) as a core architecture constraint

**Status:** Accepted
**Date:** 2026-06-27
**Goal:** Test Verdigris at trillion-row / petabyte scale *without running at scale*,
by simulating the system deterministically in a single thread in logical time.

---

## Decision

Verdigris is architected so its **control plane is a deterministic, sans-I/O core**
whose every source of nondeterminism (clock, storage, randomness, task scheduling)
is injected through a swappable seam. In production the seams are backed by real
implementations; in tests they are backed by a deterministic simulator
([madsim](https://github.com/madsim-rs/madsim)) plus our own in-process fakes.

Prior art: RisingWave, a distributed SQL DB, runs its whole suite this way under
madsim (full round ~2 min vs ~8 min e2e). FoundationDB, TigerBeetle, and Antithesis
are the canonical examples of the methodology.

## Engine decision: DataFusion, not DuckDB

**We use Apache DataFusion as the real query engine, not DuckDB.** Rationale:

- We do **not** hand-roll a SQL kernel (parser, optimizer, vectorized executor,
  spill, join/sort/aggregate). That's person-years of commodity work and is not
  what Verdigris differentiates on. We embed a mature engine and own only the parts
  that differentiate: the Iceberg catalog, tier-aware planning, the cost gate,
  compaction, the ingest→Parquet writer, and text-search acceleration.
- Of the embeddable engines, **DataFusion is pure Rust and builds on the same
  `object_store` crate and tokio we already standardize on** — both interceptable by
  madsim. DuckDB is native C++ behind FFI (own threads, own I/O, own clock) and is
  fundamentally opaque to the simulator. Choosing DataFusion means real query
  execution can plausibly run *inside* the deterministic simulation against
  `SimObjectStore`, instead of only behind a non-executable stub.
- DataFusion is built to be embedded/extended (bring-your-own catalog +
  `TableProvider`), which is exactly how we wire Iceberg + tiering in.

**Unverified, must prove (not assume):** DataFusion still spawns its own tokio tasks
and a CPU thread pool, so *fully* deterministic execution under madsim likely needs
configuration — single `target_partitions`, no separate rayon pool, all I/O through
the simulated `object_store`. This is the load-bearing experiment before we commit to
running real execution in-sim. Until proven, treat in-sim DataFusion as a goal, not a
guarantee.

DuckDB is not used. If we ever want it as an alternative single-node engine, it would
sit behind the same `ScanExecutor` seam — but it can never participate in DST.

### We still don't simulate execution at trillion scale

Even with a Rust engine, you never *actually* execute a trillion rows in a test — that
work is rate-bound and covered by real small-scale **calibration** runs, then
extrapolated. DataFusion-in-sim buys deterministic *correctness* testing at small/medium
scale; the `ModeledExecutor` still carries trillion-scale timing. So we keep the seam
between orchestrator and executor regardless:

```
  ┌──────────── deterministic core — madsim simulates ALL of this ─────────────┐
  │  ingest batching · compaction scheduler · tiering state machine · Glacier   │
  │  restore workflow · Iceberg catalog/planner · cost estimator · scan fan-out │
  └─────────────────────────────────┬───────────────────────────────────────────┘
                                     │ trait ScanExecutor
            ┌─────────────────────────┴───────────────────────────┐
  prod impl │ DataFusionExecutor — real Rust engine, real bytes    │
   sim impl │ ModeledExecutor    — synthetic stats + modeled       │
            │ latency from the calibration model; NO execution     │
            │ (DataFusion-in-sim is a goal for correctness tests)  │
            └───────────────────────────────────────────────────────┘
```

### How a "trillion-scale" test runs in seconds of wall-clock

1. A **fabricated Iceberg catalog** *declares* the table at scale (e.g. 20M files /
   a trillion rows) — file counts, row counts, byte sizes, min/max stats. No bytes
   exist.
2. The **`SimObjectStore`** answers metadata/list reads instantly (with modeled
   latency added to the sim clock).
3. The **planner** plans over the real declared file count — this is where
   file-count-driven bugs surface (planner OOM, manifest-prune blowup).
4. The **`ModeledExecutor`** returns per-file scan latency from the calibration
   model instead of running the real engine.
5. **Simulated time** advances by the modeled durations — not real time — so the
   whole "trillion-row query" completes in seconds.

What this tests: orchestration, scheduling, tiering decisions, restore workflows,
cost-estimate accuracy, and the *timing model* at trillion scale. What it does **not**
test: absolute GB/s (that comes from calibration). **DST + calibration, never DST
instead of calibration** — the sim is only as truthful as the latency model fed to
`ModeledExecutor`.

## The four seams (build these into v1; they cannot be retrofitted)

| Seam | Prod impl | Sim impl |
|---|---|---|
| `ObjectStore` (use `object_store` crate's trait) | `AmazonS3` | `SimObjectStore` — in-memory, modeled latency off the sim clock, injected faults, **storage-class + Glacier-restore semantics** |
| `Clock` | real time | madsim logical time |
| `ScanExecutor` | `DataFusionExecutor` (feature-gated) | `ModeledExecutor` |
| `Rng` | OS entropy | seeded; one seed reproduces an entire run |

**Note on storage:** we write our *own* `SimObjectStore` rather than depending on
`madsim-aws-sdk-s3`, because the sim store's latency/cost model and the **cost
estimator** are the same model and must share code, not diverge. (`madsim-aws-sdk-s3`
stays a known fallback if we ever call `aws-sdk-s3` directly.)

## Discipline rules (enforced in review / lint)

- **Sans-I/O control plane.** Core logic is pure `(state, event) -> (state, effects)`.
  No direct I/O, time, threads, or randomness in the core — the shell executes effects
  through the seams. This is what makes a trillion events a trillion cheap function calls.
- **No `SystemTime::now()` / `Instant::now()`** outside the `Clock` impl.
- **No raw thread spawning / no `std::time::sleep`** — all concurrency goes through the
  runtime so the simulator controls scheduling.
- **All randomness through the injected `Rng`.** No `rand::thread_rng()` in the core.
- **Crate-swap pattern:** real vs sim runtime selected by a cfg flag (RisingWave's
  pattern), so identical code runs in both modes with no `#[cfg]` sprinkled through logic.

## What DST buys us, mapped to the build order

- **Compaction (step 4):** simulate millions of tiny files arriving and prove the
  scheduler keeps file count bounded over simulated weeks — in seconds.
- **Tiering (step 3):** fast-forward simulated months of lifecycle transitions; assert
  no hot/cold thrash, correct prefix routing.
- **Cost estimator (step 5):** the sim store *is* the estimator's model; every sim run
  cross-checks predicted vs (modeled) actual scan size and dollars.
- **Glacier restore UX:** drive the restore state machine through bulk/standard/expedited
  timings and fault injection without a single real (slow, costly) retrieval.

## Open questions to resolve before scaffolding

- Confirm `object_store`'s trait is object-safe / async-trait-shaped enough to back both
  impls cleanly (very likely yes).
- Decide whether the core uses madsim's runtime directly or a thin `Runtime` trait over
  both tokio and madsim (RisingWave uses the crate-swap; lean that way).
- Calibration harness: how `ModeledExecutor`'s per-file latency model is fit from real
  DataFusion-on-S3 runs, and how often it's re-fit.
