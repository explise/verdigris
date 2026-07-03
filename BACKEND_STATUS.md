# Verdigris — Backend & System Status

_Last updated: 2026-06-29_

S3-native, plug-and-play log storage + query engine in Rust. Logs are written as
compacted Parquet to the customer's own bucket and queried **in place** (no
rehydration). Product vision: `CLAUDE.md`. Testing architecture: `docs/dst-architecture.md`.
Frontend status lives in `STATUS.md` (UI) and `BACKEND_TODO.md` (API punch list).

**One-line state:** the full local loop works end to end — ingest → tier → compact →
query (SQL + search DSL) → cost-estimate → serve to a browser UI. Backend build steps
1–5 of 7 are done. **Nothing is committed** (still on the initial commit) and **nothing
is published** (all crates `publish = false`).

---

## Architecture at a glance

Cargo workspace, 5 crates. Default build is offline and dependency-light; the heavy bits
(DataFusion engine, HTTP server) are behind feature flags.

| Crate | Role |
|---|---|
| `crates/core` | **Sans-I/O control plane.** Pure logic + seam traits. No tokio/I/O. Modules: `batch`, `clock`, `config`, `cost`, `estimate`, `lifecycle`, `manifest`, `model`, `rng`, `search`. |
| `crates/storage` | `object_store` seam. `build(&StorageConfig)` → local fs / in-memory / S3-or-MinIO. |
| `crates/query` | `ScanExecutor` seam. `ModeledExecutor` (default) + DataFusion `engine` (feature `datafusion`). |
| `crates/ingest` | records → Arrow → Parquet → store; manifest writes; severity routing; compaction; synthetic generator. |
| `crates/vdg` | CLI/HTTP shell. Real `Clock`, config loading, all commands, `serve` (feature `serve`). |

**Feature flags:** `datafusion` (real query engine), `serve` (HTTP API + static frontend;
implies `datafusion`). Default build has neither — stays fast/offline.

**Version alignment (load-bearing):** pinned to DataFusion 54's deps —
`object_store 0.13`, `arrow`/`parquet 58` — so our `Arc<dyn ObjectStore>` and Parquet
bytes interop with the engine.

### Key decisions (ADRs)
- **ADR-001 (`docs/dst-architecture.md`): Deterministic Simulation Testing.** Control
  plane is sans-I/O; nondeterminism behind four seams (`Clock`, `Rng`, `ObjectStore`,
  `ScanExecutor`). Goal: test trillion-scale in simulation. **Seams exist; the madsim
  harness itself is NOT built yet.**
- **Engine = DataFusion, not DuckDB** — pure Rust + `object_store` + tokio, so it can
  (eventually) run inside the simulator. DuckDB's native C++ is opaque to madsim.
- **Manifest is a JSON stand-in for Apache Iceberg** — file list + per-file stats (bytes,
  rows, min/max ts, tier, compaction generation). Real Iceberg is a future ADR.

---

## What's done (build steps 1–5)

### 1. Ingest → Parquet → store ✅
- Arrow schema: `ts, level, service, status, message, trace_id, attrs_json`
  (known fields typed; `attrs_json` is the schema-evolution escape hatch).
- `Batcher` (sans-I/O rolling policy), zstd Parquet via `ArrowWriter`.
- `Manifest` (JSON catalog) with per-file stats; append + reload.
- Deterministic seeded synthetic generator; NDJSON file ingest.

### 2. Query in place ✅
- DataFusion reads Parquet straight from the object store (no rehydration).
- **Manifest-driven**: registers the exact files from the catalog (not directory scan).
- **Search DSL → SQL translator** (`core::search`): `service:auth status>=500 | last 1h`
  compiles to SQL; raw SQL passes through. `status` is a first-class column.

### 2.5. Serve layer ✅
- `vdg serve` (axum) hosts the static `frontend/` **and** the `/v1/*` API.
- `frontend/api.js` wired to the backend (`USE_MOCKS = false`).
- `--follow` live-ingest mode keeps `last 1h` queries populated.

### 3. Tiering ✅
- **Severity-based write-time routing** (`RoutingConfig`): ERROR→hot, WARN/INFO→warm,
  DEBUG→cold (configurable). Files land in `<table>/<tier>/`. Deterministic.
- **Lifecycle policy** (`core::lifecycle`): generates AWS `PutBucketLifecycleConfiguration`
  JSON (GLACIER_IR → GLACIER → expire). `vdg lifecycle` prints it.

### 4. Compaction ✅
- `Ingestor::compact(target_bytes)`: merges each tier's small files into ~target-sized
  files, rewrites manifest, deletes old objects (manifest-first). Deterministic,
  idempotent. Verified 21→3 files, no data loss.

### 5. Cost estimator (query-aware) ✅
- `core::estimate`: prunes manifest by selected tiers + the query's **time window**;
  returns `scanBytes/tier`, `costUsd` (exact), `restoreMs`, `scanMs` (modeled).
- `/v1/query/estimate` takes `{tiers, sql}`; the cold-scan confirm gate shows the **real**
  scan/cost/restore. Verified pruning: 21→20→11→7 files as window shrinks none→2h→1h→30m.

### BACKEND_TODO API fixes ✅
Resolved #2 (events = total match), #3 (invalid SQL → 400), #4 (real metrics series),
#5 (cost fields), #6 (pipeline fields), #9 (~60 histogram buckets). Per-item status and
intentional non-changes are in `BACKEND_TODO.md`.

**Tests:** 24 green (core 17, ingest 5, query 1, storage 1). Default build clean.

---

## CLI & API reference

**Commands** (`vdg <cmd>`): `config`, `check`, `ingest` (`--generate`/`--from`/`--follow`),
`manifest`, `lifecycle`, `compact`, `sql` (needs `datafusion`), `serve` (needs `serve`),
`query` (modeled cost).

**HTTP** (`vdg serve`): `POST /v1/query`, `POST /v1/query/estimate`, `GET /v1/metrics`,
`/v1/alerts`, `/v1/storage/tiers`, `/v1/cost`, `/v1/pipelines`, `/v1/settings`. Static
frontend at `/`.

### Local quickstart
```bash
# terminal 1 — server (first build pulls DataFusion, ~1.5 min)
cargo run -p vdg --features serve -- serve --table logs

# terminal 2 — keep logs flowing so `last 1h` stays populated
cargo run -- ingest --table logs --follow

# open http://localhost:8080
```
Storage backend is config-driven (`config/verdigris.toml`): local fs (default, offline),
in-memory, or S3/MinIO — no recompile to switch.

---

## What's left to do

### Remaining build steps
- **Step 6 — Helm / packaging.** _Mostly done._ Multi-stage `Dockerfile` (builds
  `vdg --features serve`, slim non-root runtime) + a Helm chart at
  `deploy/helm/verdigris` — one `helm install` brings up serve+UI. Two paths: a
  zero-config local-fs demo (auto-seeded via initContainer) and EKS+S3 (IRSA auth,
  stateless replicas, seed hook Job). Lints clean; renders validated for both.
  The Vector DaemonSet is now **functional** (opt-in): it tails pod logs and ships
  NDJSON to the real `POST /v1/ingest` endpoint (see below); `sinkEndpoint` defaults
  to the in-cluster serve Service. See `deploy/README.md`.
- **Step 7 — Frontend.** `frontend/` (vanilla) is wired and served. `web/` (Vite + SolidJS
  + TS + uPlot — the production rebuild, see `STATUS.md`) is mock-only and not yet served
  by `vdg`.

### Cross-cutting hard parts (the "real product" gap)
- **Real Apache Iceberg** to replace the JSON manifest (snapshots, partitions,
  concurrent-commit safety). _ADR-002, TBD._
- **The DST harness itself.** Seams exist but madsim isn't wired: need a `SimObjectStore`
  (in-memory + modeled latency + Glacier-restore semantics, sharing `core::cost`), a
  `SimClock` driving madsim time, and a fabricated-catalog generator to plan trillion-file
  tables. Plus a **calibration harness** that fits `ModeledExecutor`/`scanMs` throughput
  from real DataFusion-on-S3 runs.
- **DataFusion-in-sim** — prove single-partition deterministic execution under madsim
  (the open ADR-001 question).
- **Real ingestion sources** — _HTTP done._ `POST /v1/ingest` accepts NDJSON / a JSON
  object / a JSON array (shared `verdigris_ingest::wire::JsonLog` format, case-insensitive
  `level`), routes by severity, writes Parquet, updates the manifest; writes are
  serialized per-process by a mutex. The Vector DaemonSet ships to it. **Still TODO:** a
  native OTLP receiver, and cross-replica ingest safety (needs Iceberg commits — the
  per-process mutex doesn't cover multiple serve pods writing one S3 table).
- **Fast text search** over columnar Parquet — bloom filters / inverted index for
  "grep this stack trace".
- **Schema evolution** beyond the `attrs_json` blob.
- **Auth / multi-tenancy** — none yet. (Note: `web/` already routes `/:org/:env/:page` and
  its transport expects tenancy path segments; the backend serves flat `/v1/...` — these
  need reconciling.)

### Smaller follow-ups
- **Apply lifecycle to S3** — `vdg lifecycle` only *prints* the policy (`object_store` has
  no lifecycle API; needs an `aws-sdk-s3` call or the CLI).
- **Tier-filtered scans** — the query scans all tiers regardless of UI tier pills; only
  the *estimate* is tier-aware.
- **Estimate fidelity (L2)** — only the time window prunes today; add `service:`/`level:`
  predicate pruning via per-file column stats (level→tier is a freebie from routing).
- **`scanBytes` is upper-bound by file** — coarse min/max pruning; wide (compacted) files
  can't be time-pruned. Finer pruning needs row-group/column stats.
- **Streaming/multipart Parquet writes** (today buffered; OK because `BatchPolicy` bounds size).
- **`/v1/tail` SSE/WebSocket** — live tail is client-side mock only.
- **Pricing rates** — confirm warm (Glacier IR) ≈ $0.03/GB and cold (Glacier Flexible std)
  ≈ $0.01/GB per `CLAUDE.md`; client mock `TIER_ECON` currently has them swapped (live
  estimate is backend-computed and already correct).
- **Placeholders to make real**: `/v1/alerts` (no alert engine), `/v1/pipelines` (no
  introspection), `p99` in `/v1/metrics` (modeled — no latency field), `expensiveQueries`
  in `/v1/cost` (no query-history tracking).
- **Single-writer ingest** — concurrent ingestors race on the manifest (fixed by Iceberg commits).

---

## Repo housekeeping
- **Uncommitted:** everything is on the initial commit (`0a0426f`). A local checkpoint
  commit is overdue. Nothing pushed/published (per instruction).
- **Two frontends:** `frontend/` (vanilla, served) and `web/` (Vite + SolidJS + TS).
- **Scratch tables in `./data/`** (gitignored): `demo`, `real`, `logs`, `comp`, `tiered`,
  `estdemo`. `demo`/`real` predate the `status`-column schema change and are stale.
- **Crate name `vdg`** not verified on crates.io (moot until we publish).
