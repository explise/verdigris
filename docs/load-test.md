# Ingest load test & calibration

How to reproduce the ingest throughput measurement, and what it is for.

## Why this exists

Two reasons, and the second is the important one.

1. **Find the ingest bottleneck.** Not "can we hit N GB/s" — a fixed target tells
   you whether one number was hit; a ramp tells you which component is the ceiling
   and what to fix first. The deliverable is a throughput curve plus a named
   bottleneck.

2. **Calibrate the simulator.** ADR-001 (`docs/dst-architecture.md`) is emphatic:
   *DST + calibration, never DST instead of calibration — the sim is only as
   truthful as the latency model fed to it.* Today `ModeledExecutor::new(mibps_per_core,
   cores)` is fed `query.modeled_mibps_per_core = 250.0` and `query.cores = 4`, and
   those numbers are **guesses**. A green DST run proves orchestration, scheduling,
   and the timing *model*; it proves nothing about absolute GB/s. This harness is
   the other half.

This is ROADMAP **M1.3**'s third acceptance criterion and M5.3's "reproducible
benchmark harness".

## Running it

```sh
bench/run-loadtest.sh                        # default ramp: 50 MiB/s, +50, to saturation
bench/run-loadtest.sh --steps 6 --step-secs 10
KEEP_UP=1 bench/run-loadtest.sh              # leave the stack up to poke at
```

Everything after the script name is passed through to the generator
(`bench/loadgen --help` for the full set).

Outputs land in `bench/results/`:

| File | What |
|---|---|
| `latest.json` | The throughput curve — per-step offered/accepted rate, PUT/s, latency percentiles, error counts |
| `resources.csv` | Per-container CPU and memory, sampled every 2s |
| `machine.txt` | Host specs, docker version, git commit — so the run can be interpreted later |

## What the stack is

`bench/docker-compose.yml` brings up MinIO (pinned by tag) and exactly **one**
Verdigris ingest node. One is not a simplification: per ADR-0003 the ingest role is
the single manifest writer, so a second replica would not double throughput — it
would produce manifest CAS conflicts. That contention is measured by ramping load
against one node, not by adding nodes.

The node runs against **S3 (MinIO), not the local filesystem**, so the run
exercises the real object-store path — CAS manifest commits, PUT latency, retry —
rather than an `fs::write`. Config is `bench/config/loadtest.toml`, which differs
from `config/verdigris.toml` in two respects — storage, and the compaction check
cadence — and is otherwise left at production defaults. Compaction stays
**enabled**: disabling it would produce a prettier curve that lies, since
compaction competing with ingest for CPU and IO is production behaviour.

The cadence is the one value that had to move. `compaction.interval_secs` is how
often the scheduler *checks*, and it sleeps before its first pass, so a run
shorter than the interval never compacts at all. At the production 300s no run in
`bench/results/` ever did, while their config said compaction was on (#48). The
committed 30s puts a tick inside the shortest run the default ramp can produce —
two 20s steps plus a 5s settle. `run-loadtest.sh` prints the node's own counters
after the ramp and appends them to `machine.txt`; `runs=0` there means the curve
did not exercise compaction, whatever the config says.

### Reproducing the compaction on/off comparison

`bench/loss-matrix.sh` runs four 12s ingest arms — compaction × concurrency — and
compares what is queryable afterwards against what was acked. A 30s cadence would
never fire inside 12s, so the script sets the interval for the arm itself (4s) and
then *verifies* it: it reads `compaction.runsTotal` from `/v1/metrics` and refuses
to label an arm `compaction-*` unless a pass actually merged files. On the off
arms it checks the same counter is zero and that the node reports `enabled=false`,
which is also what proves the in-place config edit landed. An arm that fails the
check is kept as `loss-<arm>.UNVERIFIED.json` and the script exits nonzero.

That check is the point. "Compaction was enabled in the config" is not evidence a
pass ran, and two arms that never compact are the same experiment run twice.

### Load generator design

`bench/loadgen` is deliberately **outside the cargo workspace**
(`exclude = ["bench/loadgen"]` in the root `Cargo.toml`). It must read the real
clock to measure anything, and `crates/vdg/tests/seams.rs` fails the build on
wall-clock calls anywhere under `crates/`. Keeping it out preserves that gate
rather than growing an exemption for benchmark code, and keeps its HTTP/TLS
dependencies out of the workspace lockfile and CI.

Determinism: the corpus comes from `verdigris_ingest::generate` with a fixed seed
(default 42), serialized to NDJSON once up front. Per-request work is a refcount
bump on a `Bytes`, not a memcpy, so the numbers describe the server rather than
the generator's allocator.

## Findings (2026-07-19)

Full writeup with all tables:
[`bench/results/RESULTS.md`](https://github.com/explise/verdigris/blob/main/bench/results/RESULTS.md).
Read its **Corrections** section first: the CPU figures and several verdicts in the
first version came from runs that predate the load generator's dedup fix.

Ingest saturates at **~43 MiB/s (~205k records/s)** on a 16-core host, and the
limit is **serialization, not any hardware resource** — a peak of 3.35 of the
node's 8 cgroup cores (~42%) and 2.64 of MinIO's 4 (~66%), with zero 429s.

The bottleneck was the process-wide `ingest_lock`, held across
encode → compress → PUT → manifest-commit when only the manifest read-modify-write
needs mutual exclusion; `f5685ad` narrowed it to the commit. Throughput was flat
at ~10.3 req/s from concurrency 1 through 32 while p50 latency scaled linearly
(107 → 3023 ms): Little's Law for a one-at-a-time server.

Service time decomposes as **52.1 ms fixed + 12.95 ms/MiB** (r² > 0.998), i.e. a
**77 MiB/s single-core encode ceiling**. Narrowing the lock should give roughly
8 × that *if* per-core throughput survives 8-way parallelism — treat it as an
upper bound, not a forecast. Reaching 1 GB/s needs ~14 cores of encode and is
impossible at any core count until the lock is narrowed.

Durability is clean: zero loss in all four arms of the matrix
(`bench/loss-matrix.sh`). The committed arms never actually compacted — that is
#48, and the script now forces and verifies a pass before it will name an arm
`compaction-*`.

## Reading the results honestly

Three caveats that must travel with any number from this harness:

- **MinIO on localhost is not S3.** Latency is ~0.1ms where real S3 is ~20-50ms per
  PUT. This validates *logic and CPU cost* — encode, compress, commit, retry — not
  network-bound throughput. A bottleneck that is CPU-shaped here is real; one that
  is latency-shaped here would look completely different against real S3.
- **Everything shares one host.** MinIO, the node, and the generator compete for
  the same 16 cores. `resources.csv` exists to catch this: a ceiling found while
  MinIO is pinned at its CPU limit is an artifact, not a finding.
- **A number without its config is noise.** MiB/s-per-core depends on the worker
  thread count (`TOKIO_WORKER_THREADS`, pinned to 8 in compose, not left to the
  host's core count) and on the batch policy. `machine.txt` and the `config` block
  in `latest.json` record both, and `machine.txt` also records whether a compaction
  pass actually ran during the ramp.

## Knobs that matter

| Knob | Where | Default |
|---|---|---|
| `ingest.max_body_bytes` | `crates/core/src/config.rs` | 16 MiB → 413 |
| `ingest.max_inflight` | same | 32 → 429 |
| `ingest.max_batch_rows` | same | 100_000 |
| `ingest.max_batch_bytes` | same | 128 MiB |
| Parquet compression | `crates/ingest/src/encode.rs` | ZSTD level 3 |
| Bloom filters | same | trace_id, service, level, message |
| `compaction.trigger_pending_files` | `config.rs` | 16 |
| `compaction.interval_secs` | same | 300 (30 in `bench/config/loadtest.toml`) |
| `query.modeled_mibps_per_core` | same | 250.0 ← the guess being calibrated |

**`ROWS_PER_ROW_GROUP` is not a perf knob.** It is pinned at 128 Ki deliberately:
the trigram row-group index is positional (set *i* describes row group *i*), so a
byte-size-triggered cut would make boundaries depend on compression and the index
would skip into the wrong data. Changing it is a correctness change.

### Why `max_batch_rows` / `max_batch_bytes` had to become config

Before this harness they were hardcoded `BatchPolicy::default()` at both HTTP
handlers, and only the CLI could override them. That made a batch-size sweep
meaningless: `Ingestor::ingest` flushes leftover buffers at the end of *every*
call, so each POST writes at least one Parquet file per tier it touches regardless
of the thresholds. The **client's request size**, not server config, decided the
PUT rate — a 1 MiB vs 16 MiB post is the difference between ~1000 and ~60 PUTs/sec
at 1 GB/s. Any curve measured against the old code was really a measurement of the
load generator.

The defaults are unchanged; `Config::default()` still reproduces
`BatchPolicy::default()` exactly, pinned by a test.
