# Verdigris Architecture

This document describes how Verdigris is put together: the crates, the four seams, the
write and read paths, tiering, compaction, the cost model, and the split-role deployment
topology. For the *why* behind specific decisions, see [`adr/`](adr/). For the testing
philosophy that shapes all of it, see [`dst-architecture.md`](dst-architecture.md).

## Design goals

1. **The customer's bucket is the source of truth.** Verdigris never becomes a data toll
   booth on its own users; data stays in the customer's account and is queried in place.
2. **Storage cheap, compute provisioned.** Bytes in S3 are the storage cost; query speed
   is a separately provisioned dial. Severity decides *placement*, never *price*.
3. **Make cost legible.** A scan's size and dollar cost are estimated before it runs, and
   the estimate provably covers the same files the scan reads.
4. **One binary, one chart.** A single `vdg` binary behind one Helm chart runs ingest,
   query, UI, and tiering.
5. **Deterministic by construction.** Nondeterminism lives only behind seams; the control
   plane is sans-I/O so scale is testable in simulation, not just in production.

## Crate map

Verdigris is a Cargo workspace of five crates. The default build is offline and
dependency-light; DataFusion and the HTTP server are behind feature flags.

| Crate | Role |
|---|---|
| `crates/core` | **Sans-I/O control plane.** Pure logic + seam traits. No tokio/I/O. Modules: `batch`, `clock`, `config`, `cost`, `estimate`, `lifecycle`, `manifest`, `model`, `rng`, `search`. |
| `crates/storage` | The `ObjectStore` seam. `build(&StorageConfig)` → local fs / in-memory / S3-or-MinIO, plus `SimObjectStore` for tests. |
| `crates/query` | The `ScanExecutor` seam. `ModeledExecutor` (default) + a DataFusion `engine` (feature `datafusion`). |
| `crates/ingest` | The write path: records → Arrow → Parquet → store; manifest writes; severity routing; compaction; the OTLP mapping; a synthetic generator. |
| `crates/vdg` | The CLI + HTTP shell: config loading, all commands, and `serve` (feature `serve`). All real I/O lives here — including, today, direct wall-clock reads: the shell is not yet wired through the `Clock` seam ([#31](https://github.com/explise/verdigris/issues/31)). |

## The four seams

Every component is written against these seams from v1; they cannot be retrofitted.

```
                 ┌──────────────── crates/core (sans-I/O) ────────────────┐
                 │  (state, event) -> (state, effects)                     │
                 │  batching · compaction scheduling · tiering · restore   │
                 │  workflow · catalog planning · cost estimation          │
                 └───────┬───────────┬────────────┬────────────┬──────────┘
                         │           │            │            │
                   ObjectStore     Clock      ScanExecutor     Rng
                         │           │            │            │
        prod   AmazonS3/local   real time   DataFusionExec   OS entropy
        sim    SimObjectStore   madsim      ModeledExec      seeded
```

- **`ObjectStore`** — the `object_store` crate's trait. Production is `AmazonS3` (or local
  fs / in-memory); the simulator is `SimObjectStore`, which layers modeled latency (off the
  sim clock), per-object storage class, Glacier-restore semantics, and fault injection on
  top of an in-memory store. Crucially, `SimObjectStore`'s latency/cost model and the
  user-facing **cost estimator share the same `core::cost` code** — so the simulation can
  never lie about what production bills.
- **`Clock`** — real wall-clock in production; madsim logical time in simulation. Nothing
  in `core` calls `SystemTime::now()`.
- **`ScanExecutor`** — the boundary between orchestration and query execution. Production is
  a DataFusion executor over real bytes; simulation is a `ModeledExecutor` returning
  synthetic stats + calibrated latency. DST tests orchestration/timing; separate
  **calibration** runs supply absolute GB/s. Never one without the other.
- **`Rng`** — seeded in tests so a single seed reproduces an entire run.

## The write path (ingest)

```
records ──▶ Batcher ──▶ Arrow RecordBatch ──▶ Parquet (zstd) ──▶ ObjectStore
                                                     │
                                                     ▼
                                        Manifest (append file + stats)
```

1. **Sources.** Logs arrive via `POST /v1/ingest` (NDJSON, a single JSON object, or a JSON
   array — the shape Vector's HTTP sink emits) or `POST /v1/otlp/logs` (OTLP/HTTP JSON from
   an OpenTelemetry Collector). Both map to a common `LogRecord`. A synthetic generator and
   NDJSON-file ingest exist for local use.
2. **Schema.** The Arrow schema is `ts, level, service, status, message, trace_id,
   attrs_json`. Known fields are typed; `attrs_json` is the schema-evolution escape hatch
   for arbitrary structured fields.
3. **Batching.** A sans-I/O `Batcher` applies a rolling policy (size/row/time bounds) to
   decide when to flush a Parquet file, so files don't grow unbounded or stay tiny forever.
4. **Routing.** Each record is routed to a tier by severity (`RoutingConfig`: e.g.
   ERROR→hot, WARN/INFO→warm, DEBUG→cold). Files land under `<table>/<tier>/`. Routing is
   deterministic and is *placement only*.
5. **Catalog.** Each written file is appended to the **manifest** with per-file stats:
   byte size, row count, min/max timestamp, tier, and compaction generation.

Within a process, ingest writes are serialized by a mutex — the manifest is
read-modify-written and concurrent writes must not interleave. Across processes/replicas,
the single-writer role (below) provides that guarantee until Iceberg commits land.

## The catalog (manifest)

The catalog is a **JSON manifest** — a deliberate stand-in for Apache Iceberg (see
[ADR-0002](adr/0002-manifest-as-iceberg-standin.md)). It lists every file with the stats
above, which is enough to drive:

- **manifest-driven query registration** — the engine registers exactly the files in the
  catalog, not a directory scan;
- **time-range pruning** in both the planner and the cost estimator (via per-file min/max
  ts);
- **tier accounting** for the storage and cost pages;
- **compaction bookkeeping** (which files are small, which generation).

Real Iceberg (snapshots, partitions, concurrent-commit safety) is the future replacement.

## The read path (query in place)

```
SQL / search DSL ──▶ plan over manifest files ──▶ DataFusion ──▶ Parquet on S3
                                                       │
                                                       ▼
                                              rows + histogram + stats
```

1. **Query language.** SQL (via DataFusion) is the portable interface. A concise **search
   DSL** (`service:auth status>=500 | last 1h`) compiles to SQL in `core::search`; raw SQL
   passes through. `status` is a first-class column.
2. **Planning.** The query registers the exact files from the manifest and prunes by the
   query's time window using per-file min/max stats.
3. **Execution.** DataFusion reads Parquet **directly from object storage** — there is no
   rehydration step. On the hot tier this is interactive; on cold tiers it depends on
   Glacier retrieval (see the cost model).
4. **Response.** `/v1/query` returns `{ rows, stats, histogram }` in one envelope:
   a page of rows, a `~60`-bucket time histogram (total vs errors) tied to the table's time
   range, and stats (total matched events, scanned bytes, elapsed ms, engine, files).

## Tiering & lifecycle

Two mechanisms, working together:

- **Write-time routing** places a log into a hot/warm/cold prefix by severity (above).
- **S3 lifecycle policies** age data across storage classes over time
  (`hot → warm → cold → expire`). `core::lifecycle` generates the AWS
  `PutBucketLifecycleConfiguration` JSON; `vdg lifecycle --apply` pushes it to the bucket
  (the Helm chart runs this automatically as a post-install hook, so tiering happens with
  no manual step).

## Compaction

Streaming logs produce millions of tiny Parquet files, which destroy scan speed and waste
money on Glacier's per-object metadata tax. Compaction is **core, not optional**:
`Ingestor::compact(target_bytes)` merges each tier's small files into ~target-sized files
(100 MB–1 GB), rewrites the manifest, and deletes the old objects **manifest-first** (so a
crash can't orphan the catalog). It is deterministic and idempotent.

## The cost model & estimator

Glacier bills retrieval by scanned-GB, so one careless cold query can produce a four-figure
bill. The estimator (`core::estimate`) makes this legible **before** a scan:

- It prunes the manifest by the selected tiers and the query's time window, sums the bytes
  those files hold, and computes `costUsd = scanGB × per-tier retrieval rate` plus modeled
  restore/scan times.
- The web UI turns this into a pre-query confirm gate on cold-tier scans ("this will scan
  ~X GB from Glacier and cost ~$Y, continue?").
- Because the estimator shares `core::cost` with `SimObjectStore`, simulation tests can
  assert the estimate matches what the (modeled) store actually billed.

## Deployment topology (split roles)

`vdg serve --role {all,ingest,query}` selects the HTTP surface a node exposes, so a
production install can separate the single manifest **writer** from the stateless
**readers** (see [ADR-0003](adr/0003-ingest-query-role-split.md)):

```
                Vector DaemonSet / OTel Collector
                              │  POST /v1/ingest, /v1/otlp/logs
                              ▼
                 ┌─────────────────────────┐
                 │  ingest Deployment (×1)  │  --role ingest   ← the sole manifest writer
                 └────────────┬────────────┘
                              │  writes Parquet + manifest
                              ▼
                        S3 (your bucket)
                              ▲
                              │  reads in place
                 ┌────────────┴────────────┐
   users / UI ──▶│  query Deployment (×N)  │  --role query    ← stateless, scales freely
                 └─────────────────────────┘
```

- **`all`** — every endpoint on one node (the local demo / single-pod default).
- **`ingest`** — only the write endpoints; run exactly one as the manifest writer.
- **`query`** — read/UI endpoints + the static web UI; the write endpoints answer `405` so
  a misrouted writer gets a clear error. Scale `replicaCount` freely.

The Helm chart renders this automatically for the S3 backend and keeps a single `--role
all` pod for the local demo. Optional bearer-token auth gates the `/v1/*` surface; the
static UI and `/config.json` stay open so the UI can boot. See
[`deploy/README.md`](https://github.com/explise/verdigris/blob/main/deploy/README.md).

## HTTP API

The full endpoint reference (methods, request/response shapes, roles, auth) is in
[`API.md`](API.md).
