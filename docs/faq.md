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

## How is this different from a hosted log service?

Architecturally, two things follow from the data never leaving the operator's account:

1. **No ingestion middleman.** There is no vendor cloud between the emitter and the bucket,
   so there is nothing to meter per-GB in transit. Cost is the underlying S3 bill plus the
   compute you provision.
2. **No rehydration step.** Hosted platforms typically archive cold logs out of their index
   and re-index them on demand before they're searchable. Here the Parquet in the bucket *is*
   the queryable form, at every tier — a cold query pays Glacier retrieval, not a re-indexing
   pipeline.

The design keeps the storage/compute decoupling the category converged on (storage priced by
bytes, query speed a separately provisioned dial, severity never priced) and removes the
steps that only exist when someone else owns the storage.

## How is this different from other self-hosted, object-storage log tools?

The shared idea is a good one: data stays in your own object storage, queried in place, no
vendor per-GB margin. Where Verdigris leans in specifically: SQL (not a proprietary DSL) as
the query language, severity used only for **tier placement and never for pricing**, and a
first-class pre-query **cost estimator** so cold logs on cheap storage are safe to actually
query.

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

## What's working vs. early?

Honest state of the build. The rule: claims must match code — where something is modeled or
partial, it says so here and is tracked as an issue whose acceptance criteria are tests.

**Working end to end (local and S3):**

- Ingest: `/v1/ingest` (NDJSON/JSON) and native OTLP/HTTP **JSON**; severity → tier routing;
  backpressure (bounded in-flight, 429 shed).
- Storage: zstd Parquet with **bloom filters** on `trace_id`/`service`/`level`/`message`;
  content-addressed files; **compare-and-swap manifest commits** with retry-on-conflict.
- Compaction: bounded incremental passes, a background auto-trigger, and a **streaming merge**
  that never materializes a bin in memory.
- Query in place: SQL + the search DSL; file pruning by tier, time window, per-file
  service/level stats, and **trigram sets** (free-text, provably no false negatives);
  Arrow IPC or JSON on the wire; live tail over SSE.
- **Bounded query memory**: a spill-to-disk execution pool plus a streaming result-size
  ceiling — an oversized result is a typed 413, not an OOM.
- The **pre-query cost estimator** with the cold-scan confirm gate; the estimate and the
  executed scan read the same file set by construction.
- Real **alerting** (rules, scheduler, state transitions, webhook notify, CRUD), per-user
  **tokens + RBAC**, a query-history **audit** doc (`expensiveQueries` is real data),
  Prometheus `/metrics`, and the Helm chart (local demo and EKS/S3 with IRSA).

**Early / partial / modeled — tracked, not hidden:**

- The **DST harness is unfinished**: of the four seams, `ObjectStore` is fully real and `Rng`
  mostly; the shell still reads the wall clock directly and the production query path routes
  around the `ScanExecutor` trait. The sim is hand-driven, not madsim, and the modeled scan
  throughput is **uncalibrated**.
  ([#31](https://github.com/explise/verdigris/issues/31),
  [#11](https://github.com/explise/verdigris/issues/11))
- **Tier prefix ≠ storage class** yet: lifecycle transitions by age over the whole table, so a
  fresh cold-routed log sits in S3 Standard for its first days while the estimator prices it
  as Glacier. ([#20](https://github.com/explise/verdigris/issues/20))
- The JSON manifest is an explicit **Iceberg stand-in**: correct under CAS, but a single
  object rewritten per commit — it will not scale to very high commit rates or millions of
  files. ([#18](https://github.com/explise/verdigris/issues/18))
- **Single-writer ingest** (one `--role ingest` pod owns the manifest write); multi-writer HA
  is designed but not built. ([#19](https://github.com/explise/verdigris/issues/19))
- `p99` in `/v1/metrics` is **modeled**, not measured — logs carry no latency field yet; the
  service's own real latencies are on the Prometheus endpoint.
  ([#27](https://github.com/explise/verdigris/issues/27))
- `/v1/pipelines` is a shape-correct placeholder; `spendSeries` in `/v1/cost` is empty (no
  spend history store yet, [#33](https://github.com/explise/verdigris/issues/33)).
- Free-text pruning is **file-level only** — inside a surviving file, `ILIKE` scans rows.
  ([#23](https://github.com/explise/verdigris/issues/23))
- OTLP is **JSON-only**; default OTel exporters speak protobuf and need their encoding set
  to `http/json`.

The full capability-by-capability map, each defined by the tests that would prove it, is the
[parity scorecard](https://github.com/explise/verdigris/issues/34).

## Is it published? What's the binary called?

The source lives at [github.com/explise/verdigris](https://github.com/explise/verdigris).
Nothing is on crates.io and no container image is published yet — you build from source. The
CLI binary is **`vdg`** (e.g. `vdg query`, `vdg ingest`, `vdg serve`); the project name is
**Verdigris**.
