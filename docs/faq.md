# FAQ

Honest answers, grounded in what the system actually does today.

## Does my data ever leave my AWS account?

No. Logs are written as Parquet to **your own S3 bucket** and queried in place. Verdigris is
a single self-hostable binary you run on your own EKS cluster; there is no vendor cloud in the
path. This is the whole point — no ingestion margin charged on every GB that flows through
someone else's cloud. The customer's bucket is the source of truth.

## What's the query language?

**SQL** — portable, no proprietary DSL to learn. The engine is Apache DataFusion (pure Rust),
reading Parquet directly from object storage. There's also a concise search DSL for the common
case (`service:auth status>=500 | last 1h`) that compiles down to SQL; `POST /v1/query` accepts
either and auto-detects which. See [Querying](querying.md).

## What is "query in place" / "no rehydration"?

Queries read the Parquet files straight out of S3 — there is no "pull cold logs back into an
expensive index before you can search them" step. Cold logs are always live. You pay compute
only when you actually run a query, plus the underlying S3/Glacier retrieval cost for whatever
bytes the scan touches.

## How is this different from Datadog Flex Logs?

Two things a commercial incumbent structurally can't fix without breaking its own business
model, which is exactly the wedge:

1. **Data sovereignty** — data never leaves your account; no per-GB ingestion margin.
2. **No rehydration tax** — no cold-to-index rehydration step to pay for.

Verdigris *borrows* Flex Logs' best idea — decoupling storage (priced by bytes) from query
speed (a separately provisioned compute dial), and refusing to price around log severity — and
removes the two things that only exist because the vendor owns the storage.

## How is this different from Loki, Quickwit, OpenObserve, VictoriaLogs, …?

They're worth studying (we did). The recurring wedge across the newer S3-native tools is the
same as ours: data stays in the customer's own object storage, queried in place, no vendor
per-GB margin. Where Verdigris leans in specifically: SQL (not a proprietary DSL) as the query
language, severity used only for **tier placement and never for pricing**, and a first-class
pre-query **cost estimator** so Glacier-backed cold logs are safe to actually query. It's built
Rust-native around a set of I/O seams so it can be tested deterministically at scale (see
below).

## Is cold-tier querying really interactive?

Be realistic here: the cheapest tier is **not** 1-minute queryable by default. Cold logs live
in Glacier Flexible Retrieval, whose restore is minutes-to-hours (Standard ≈ 4 h, Expedited ≈
5 min, Bulk ≈ 8 h). Truly interactive cold queries mean either the warm tier (Glacier Instant,
pay per GET) or paying for Expedited retrieval. What Verdigris guarantees is that this
trade-off is **visible before you run the query**: the [cost estimator](cost.md#the-pre-query-cost-estimator)
tells you the scan size, dollar cost, and whether a restore is needed, and the UI puts a
confirm gate in front of cold scans. Hot-tier queries are interactive.

## What's the cost model, in one line?

Storage is priced by bytes in S3 (cheap); query speed is a compute dial you provision
separately; severity decides *placement* (which tier), never *price*. See
[Cost & tiering](cost.md).

## Do I have to route ERROR to hot and DEBUG to cold?

No — that's just the default. `[routing]` maps each severity to any tier, and the point is that
this choice only affects where a log physically lands (and thus its storage class), not what it
costs per byte in a punitive way. Relabeling `error` as `debug` can't game a pricing model that
doesn't price on severity.

## How do I secure the API?

An optional bearer-token gate on `/v1/*`, off by default. Enable `[auth]` (or set
`VERDIGRIS_API_TOKEN`); `/healthz` and `/config.json` stay open so probes and the UI can boot.
See [Configuration → Authentication](configuration.md#authentication-auth). One caveat: the
live-tail SSE stream is consumed by the browser's `EventSource`, which can't send an auth
header — front it with a query-param token or ingress auth.

## How does it scale writes safely?

Two ways, belt-and-suspenders. Operationally, the Helm chart runs a single `--role ingest`
writer pod and scales the stateless `--role query` tier separately, so query replicas never
write. At the data layer, data files are content-addressed and the manifest commits via
optimistic compare-and-swap with retry-on-conflict — so even concurrent writers to one table
can't silently lose or corrupt data. Full Apache Iceberg (partitions, snapshots, a catalog
service) is future work for scale and features, not correctness.

## Why DataFusion and not DuckDB?

DataFusion is pure Rust on `object_store` + tokio, so it can (eventually) run inside the
deterministic simulator; DuckDB's native C++ is opaque to it. Query *execution* sits behind a
`ScanExecutor` seam either way. See [Testing (DST)](dst-architecture.md).

## What's production-ready vs. early?

Honest state of the build:

**Working end to end (local and S3):** ingest (`/v1/ingest` NDJSON/JSON + native OTLP/JSON),
severity routing, tiering + lifecycle policy, small-file compaction, query in place (SQL +
search DSL), Arrow/JSON responses, live tail, the pre-query cost estimator with the cold-scan
confirm gate, concurrency-safe manifest commits, the Helm chart (local demo + EKS/S3 with
IRSA), and optional bearer auth.

**Early / partial / placeholder — don't rely on these as finished:**

- **Alerting** — no alert engine yet; `/v1/alerts` returns `[]`.
- **`p99` latency** in `/v1/metrics` is **modeled** (logs carry no latency field yet), not
  measured.
- **`expensiveQueries`** in `/v1/cost` is empty until query-history tracking exists.
- **Pipelines introspection** (`/v1/pipelines`) is derived/partial; there's no drop/filter
  pipeline stage yet.
- **Fast full-text search** (bloom filters / inverted index) over Parquet — future work;
  substring search is `ILIKE` today.
- **Query-time tier filtering** isn't wired — a query scans all tiers; only the *estimate* is
  tier-aware.
- **Real Apache Iceberg** (partitions/snapshots/catalog) — the manifest is a JSON stand-in;
  correctness is handled, scale features are future.
- **The DST simulation harness itself** — the four seams exist, but the madsim harness and
  calibration runs aren't built yet.

For the running punch list, see `STATUS.md` and `BACKEND_STATUS.md` in the repo.

## Is it published to crates.io? What's the binary called?

Not yet published (all crates are `publish = false`). The CLI binary is **`vdg`** (e.g.
`vdg query`, `vdg ingest`, `vdg serve`); the brand/product name everywhere users see it is
**Verdigris**.
