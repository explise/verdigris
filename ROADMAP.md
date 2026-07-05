# Verdigris — Product-Readiness Roadmap

> An honest gap analysis: what stands between the current codebase and something a
> customer would trust to hold their production logs. Grounded in the source as of
> this writing; every gap cites the file that proves it. Where a claim is inference
> rather than something I ran, it says so.

---

## Status snapshot

The **local, single-node loop is real and works end to end**: logs come in over
HTTP/OTLP, get routed by severity to hot/warm/cold prefixes, are written as zstd
Parquet with a JSON manifest, compacted, queried in place by DataFusion (no
rehydration), cost-estimated before a cold scan, and served to a browser UI — all
from one `vdg` binary, deployable by one `helm install`. Build steps 1–6 of the
7-step plan in `CLAUDE.md` are substantially done; step 7 (frontend) has a wired
prototype plus a production `web/` rebuild.

What it is **not yet**: a multi-tenant, durable, observable, published product. The
"hard 20%" that `CLAUDE.md` flagged — real Iceberg, fast text search, a finished DST
harness — is exactly where the gaps still are, plus the operational surface (auth
beyond a shared token, alerting, self-metrics, ingest durability) that any team
would demand before pointing production traffic at it. Nothing is published (every
crate is `publish = false`), and the deploy image is a local `repository: verdigris`
placeholder.

### Done — verified against source

- [x] **Ingest → Parquet → store** — `Ingestor::ingest` routes by severity, batches
  per tier, writes content-addressed zstd Parquet (`crates/ingest/src/lib.rs`,
  `encode.rs`, `schema.rs`).
- [x] **Native OTLP/HTTP JSON logs** — `crates/ingest/src/otlp.rs` + `POST /v1/otlp/logs`
  (`crates/vdg/src/serve.rs`). JSON only, no protobuf/gRPC.
- [x] **HTTP ingest** — `POST /v1/ingest` accepts NDJSON / object / array; bad lines
  skipped and counted (`serve.rs` `h_ingest`).
- [x] **Query in place (DataFusion + Arrow)** — `ListingTable` over the `object_store`
  seam, no rehydration; JSON **and** Arrow-IPC output with `Utf8View`→`Utf8` down-cast
  for broad decoder compatibility (`crates/query/src/engine.rs`).
- [x] **Search DSL → SQL** — `service:auth status>=500 | last 1h` compiles to SQL; raw
  SQL passes through (`crates/core/src/search.rs`).
- [x] **Tiering** — write-time severity routing + `core::lifecycle` policy generation;
  `vdg lifecycle --apply` really PUTs the S3 lifecycle config via `aws-sdk-s3`
  (`crates/vdg/src/lifecycle_apply.rs`, behind the `apply` feature).
- [x] **Compaction** — `Ingestor::compact` merges small files per tier, rewrites the
  manifest under CAS, deletes old objects manifest-first (`crates/ingest/src/lib.rs`).
- [x] **Query-aware cost estimator** — `core::estimate::estimate_scan` prunes by tier
  and time window, returns exact retrieval cost + modeled scan/restore time;
  `POST /v1/query/estimate` drives the cold-scan confirm gate (`estimate.rs`, `serve.rs`).
- [x] **Concurrency-safe commits** — content-addressed data files + optimistic
  compare-and-swap manifest commits with retry (`append_files`, `commit_manifest` in
  `crates/ingest/src/lib.rs`; ADR-0002/0003). Verified by
  `concurrent_ingests_preserve_all_rows`.
- [x] **Split-role serve** — `serve --role {all,ingest,query}` so one writer + N
  readers don't race (`serve.rs` `Role`).
- [x] **Bearer-token auth (single shared token)** — optional `[auth]` gate on `/v1/*`
  (`serve.rs` `require_bearer`).
- [x] **Live tail (SSE)** — `GET /v1/tail` polls the newest manifest file each second
  (`serve.rs` `h_tail`); `web/` consumes it via `EventSource`.
- [x] **Helm chart + Dockerfile** — role split, seed Job, lifecycle Job, functional
  Vector DaemonSet (`deploy/helm/verdigris/**`, `Dockerfile`).
- [x] **Production web UI** — Vite + SolidJS + uPlot, wired live via `/config.json`,
  Arrow round-trip decoded client-side (`web/src/lib/{api.ts,transport.ts}`).
- [x] **Grafana datasource** — Infinity-based (`deploy/grafana/datasource.yaml`).
- [x] **DST seams + a first hand-driven sim harness** — `SimObjectStore`
  (`crates/storage/src/sim.rs`) with modeled latency, per-object storage class, a
  Glacier-restore state machine, seeded fault injection and cost metering; `SimClock`;
  and **4 passing DST scenario tests** (`crates/storage/tests/dst.rs`) including a
  fabricated 4-trillion-row catalog priced with no bytes behind it. **This is further
  along than `BACKEND_STATUS.md` claims** ("madsim harness not built yet") — see M1.3
  for what's genuinely still missing.

### Test count (measured, not guessed)

- **42 tests pass** on the default offline build — `cargo test --workspace`:
  core 18, ingest 15, query 1, storage 4, `storage/tests/dst.rs` 4, vdg 0.
- **+4** in `vdg` under `cargo test -p vdg --features serve` (the HTTP/auth/OTLP tests
  aren't compiled in the default build) → **46 across the feature matrix.**
- All green; exit 0. I did not run a `--features datafusion`-only pass separately, so
  there may be a small number of engine-gated tests not counted above.

---

## Milestones

Ordered by what a customer needs before they'd trust their logs to it. Effort is
rough (S ≈ days, M ≈ 1–2 weeks, L ≈ weeks-plus). Priority is P0 (adoption blocker) →
P2 (polish).

### M1 — Correctness & scale core

**M1.1 — Real Apache Iceberg (or equivalent) instead of a single JSON manifest.**
*Problem:* the "catalog" is one `manifest.json` per table — a flat file list with
per-file stats, committed via optimistic CAS (`crates/core/src/manifest.rs`,
`crates/ingest/src/lib.rs`; ADR-0002 is explicit that this is an Iceberg *stand-in*).
Every read and every commit rewrites/reloads the whole file, so it doesn't scale to
millions of files and offers no snapshots, hidden partitioning, or time-travel.
*Why it matters:* at real log volumes the single manifest becomes the bottleneck and
the CAS retry loop degrades; partitions are what make large-table pruning cheap.
*Acceptance:*
- Table metadata is a manifest-list structure (or real Iceberg via `iceberg-rust`),
  not one monolithic JSON blob.
- Partition pruning on `ts` (and ideally `service`) happens at plan time from
  partition metadata, not by registering every file.
- Snapshot isolation: a reader sees a consistent snapshot while a writer commits.
- Existing JSON-manifest tables migrate or are read compatibly.
*Effort: L · Priority: P1 (scale unlock; can trail auth/search).*

**M1.2 — Fast text search over columnar Parquet.**
*Problem:* free-text search is `message ILIKE '%word%'` and attribute search is
`attrs_json LIKE '%"k":"v"%'` (`crates/core/src/search.rs` `translate_term`). That is
a full column scan for every "grep this stack trace" — precisely the workload
`CLAUDE.md` calls out columnar formats as bad at. No bloom filters, no inverted index.
*Why it matters:* incident response *is* grep-over-logs. Without indexed text search,
cold-tier and large hot-tier searches are slow and expensive, undercutting the "cold
logs are always live" pitch.
*Acceptance:*
- Parquet is written with bloom filters on `message`/`trace_id` (and attribute keys),
  used to skip row groups / files at scan time.
- A measurable win on a representative "find this rare string" query vs the current
  `ILIKE` scan (report before/after scanned-bytes).
- The estimator accounts for index-pruned scans so the cost gate stays truthful.
- (Stretch) an inverted-index option for the hot tier.
*Effort: L · Priority: P0 (headline UX for the actual job logs are used for).*

**M1.3 — Finish the DST harness (madsim, DataFusion-in-sim, calibration).**
*Problem:* the seams and a *hand-driven* sim exist (`SimObjectStore`, `SimClock`, 4
scenario tests advancing the clock manually in `crates/storage/tests/dst.rs`). What
`docs/dst-architecture.md` actually specifies is **not** yet wired: madsim scheduling
the whole control plane, deterministic single-partition **DataFusion under the
`ScanExecutor` seam** (the engine comment in `crates/query/src/engine.rs` calls this a
"later experiment"), and a **calibration harness** that fits `ModeledExecutor`
throughput from real DataFusion-on-S3 runs.
*Why it matters:* `CLAUDE.md` treats DST as a core constraint, not a test add-on; the
"trillion-row query in seconds" claim and truthful cost/latency modeling both depend
on it. Without calibration the modeled `scan_ms` is uncalibrated (`estimate.rs`
comment admits it's "only as good as its calibration").
*Acceptance:*
- madsim drives at least one multi-writer/concurrency scenario deterministically
  (seed reproduces the interleaving and the outcome).
- DataFusion runs single-partition-deterministic behind `ScanExecutor` in sim, OR the
  seam swaps in `ModeledExecutor` and the boundary is documented + tested.
- A calibration run emits absolute GB/s numbers that feed `ModeledExecutor`/`scan_ms`;
  the estimator's modeled scan time is validated against a real run.
- `BACKEND_STATUS.md`'s "not built yet" line is corrected to reflect reality.
*Effort: L · Priority: P1.*

### M2 — Multi-tenancy, auth & security

**M2.1 — Real authn/authz beyond one shared bearer token.**
*Problem:* auth is a single static token compared with `==` for every `/v1/*` request
(`serve.rs` `require_bearer`; token from `[auth].token` / `VERDIGRIS_API_TOKEN`).
There are no users, no per-user tokens, no roles. `EventSource` can't even send the
header, so SSE tail needs a query-param/ingress workaround (noted in `STATUS.md`).
*Why it matters:* no security team signs off on a single shared secret with no
rotation, no identity, and no revocation. This is a hard adoption blocker for anyone
past a solo demo.
*Acceptance:*
- OIDC/SSO login (or at minimum issued, revocable per-user API tokens).
- RBAC: at least read-only vs read-write vs admin, enforced server-side per route.
- Token rotation without downtime; SSE authenticated cleanly.
*Effort: L · Priority: P0.*

**M2.2 — Real tenant isolation (reconcile flat backend vs `/:org/:env` UI).**
*Problem:* the backend is single-tenant and flat — `/config.json` pins `mode:"onprem"`
with one org/env derived from the served table (`serve.rs` `h_config`), while the
`web/` app routes `/:org/:env/:page` and its transport carries tenancy segments
(`web/src/lib/transport.ts`, `STATUS.md`). The two don't meet; there's no enforced
per-tenant data boundary.
*Why it matters:* multi-team/multi-customer deployments need guaranteed isolation
(one tenant can't read another's bucket/prefix/table). Today that's structurally
absent.
*Acceptance:*
- A tenant maps to an isolated bucket/prefix (or table namespace), enforced on every
  read/write path, not just in UI routing.
- Auth identity resolves to a tenant; cross-tenant access is denied server-side.
- The `web/` cloud multi-org path is wired to a backend that honors it.
*Effort: L · Priority: P1 (P0 if selling multi-tenant/SaaS-style; single-tenant
on-prem can ship without it).*

**M2.3 — Audit log / query history.**
*Problem:* nothing records who queried what. `expensiveQueries` in `/v1/cost` is
hardcoded `[]` "until query-history tracking exists" (`serve.rs` `h_cost`).
*Why it matters:* compliance and cost-attribution both need it; it also unblocks the
expensive-queries UI and per-user cost visibility.
*Acceptance:*
- Every query is recorded (who, when, SQL, scanned bytes, cost).
- `/v1/cost` `expensiveQueries` is populated from real history.
- History is queryable/exportable for audit.
*Effort: M · Priority: P1.*

### M3 — Operations & durability

**M3.1 — Real alerting engine. ✅ DONE (2026-07-05).**
Shipped: `crates/core/src/alert.rs` (pure rule model + firing/OK state machine, unit-tested),
persisted as `alerts.json` in the object store; `vdg serve` runs a 15s scheduler evaluating
each rule's SQL via the query engine, fires a webhook on OK↔Firing transitions, and exposes
`GET/POST/DELETE /v1/alerts` (create validates the SQL + evaluates immediately). Two example
rules seeded. Wired end to end into BOTH the `frontend/` prototype and the `web/` production
Alerts pages (create form, delete, real state). Commits `4b0b132`, `d5aae36`.
*Follow-ups (deferred):* first-class time-window field (today the window is whatever `WHERE ts…`
you put in the SQL); Slack/PagerDuty channels (thin wrappers on the webhook); CAS persistence
for the alerts doc (today last-write-wins within the single writer).

**M3.2 — Self-observability (Prometheus `/metrics`, tracing, real latency).**
*Problem:* `/v1/metrics` returns *business* metrics computed from the log data, and
its `p99` is openly **modeled** (`800.0 + errorRate*18.0`) because "logs have no
latency field yet" (`serve.rs` `h_metrics`). There is no Prometheus `/metrics`
endpoint for the *service itself* and no real request-latency histogram. Only a
`/healthz` liveness probe exists.
*Why it matters:* operators can't run what they can't see — no SLOs, no dashboards on
the ingest/query service's own health, no real p99.
*Acceptance:*
- A Prometheus `/metrics` endpoint exposing request rate/latency/error counters and
  ingest/query internals.
- Real per-request latency replaces the modeled `p99`.
- Structured tracing (OpenTelemetry) spans across ingest and query paths.
*Effort: M · Priority: P1.*

**M3.3 — Ingest durability: WAL/queue, backpressure, rate limiting.**
*Problem:* ingest is best-effort in-memory: records are batched and buffered to
Parquet in-process, serialized only by a per-process `tokio::sync::Mutex`
(`serve.rs` `ingest_lock`, `AppState`); a crash before a batch rolls loses buffered
records, and there's no backpressure or rate limiting on `POST /v1/ingest`.
*Why it matters:* a log store that silently drops logs under load or on restart isn't
trustworthy for the incident you're logging *for*.
*Acceptance:*
- A durable buffer (WAL or durable queue) so an in-flight batch survives a crash.
- Backpressure: the endpoint sheds/queues rather than OOMing under a spike.
- Configurable per-source rate limiting.
*Effort: L · Priority: P0 (durability is the core promise of a log store).*

**M3.4 — Retention & orphan GC.**
*Problem:* S3 lifecycle expiry is generated, but the app never garbage-collects
orphaned data files. Compaction explicitly leaves failed-commit files as "harmless
orphans" and never sweeps them (`crates/ingest/src/lib.rs` `compact`); the SimStore
similarly leaves stale class metadata for deleted keys.
*Why it matters:* orphans accumulate as silent S3 cost — directly against the "make
cost legible / no surprise bills" principle.
*Acceptance:*
- A GC job reconciles bucket objects against the manifest and deletes unreferenced
  files (with a safety window).
- App-level retention enforcement independent of the S3 lifecycle rule.
- Orphan bytes reported so cost stays legible.
*Effort: M · Priority: P2.*

### M4 — Query & UX polish

**M4.1 — Tier-filtered scans (make the query honor the tier pills).**
*Problem:* the UI sends `{ sql, tiers }` (`web/src/lib/api.ts` `queryLogs`) but the
query handler's `QueryReq` deserializes **only `sql`** and registers *all* manifest
files regardless of tier (`serve.rs` `h_query`). So the *estimate* is tier-aware
(`h_estimate` reads `tiers`) but the *actual scan* ignores the pills — a user can be
quoted a hot-only cost and then scan cold anyway.
*Why it matters:* it breaks the cost-gate contract — the estimate and the executed
query must scan the same files, or the headline "no surprise bills" guarantee is
false.
*Acceptance:*
- `h_query` accepts `tiers` and restricts registered files to those tiers.
- Estimate and executed query provably touch the same file set (a test asserts it).
*Effort: S · Priority: P0 (correctness of the flagship feature).*

**M4.2 — Predicate/column-stats pruning beyond the time window.**
*Problem:* only the time window prunes, and only in the *estimate* — the actual query
registers every file and leans on DataFusion's row-group pruning
(`crates/query/src/engine.rs` registers all `files`; `estimate.rs` prunes by
`min_ts`/`max_ts` only). No `service:`/`level:` file-level pruning even though
level→tier is a freebie from routing (noted in `BACKEND_STATUS.md`). Coarse min/max
means wide compacted files can't be time-pruned.
*Why it matters:* less scanned = faster and cheaper; it's the whole compute dial.
*Acceptance:*
- Per-file column stats (service, level) used to skip files at plan time.
- Row-group/column-level pruning surfaced so compacted files still prune by time.
- Estimator and executor share the pruning logic.
*Effort: M · Priority: P1.*

**M4.3 — Saved queries & dashboard/alert persistence.**
*Problem:* there's no server-side persistence for anything a user creates — no saved
queries, no dashboards, no persisted alert rules (only the placeholder `/v1/alerts`).
*Why it matters:* teams live in saved views and shared dashboards; without persistence
the UI is a stateless viewer.
*Acceptance:*
- Saved queries and dashboards persisted per tenant/user and reloadable.
- Shareable within a tenant.
*Effort: M · Priority: P2.*

**M4.4 — Feed charts directly from Arrow columns.**
*Problem:* the Arrow round-trip is wired, but uPlot is fed materialized JS arrays
rather than Arrow columns directly (`STATUS.md` items 5/65; `web/src/lib/arrow.ts`).
*Why it matters:* the direct-column path is the real scale win for large result sets.
*Acceptance:*
- uPlot consumes `x[]`/`y[]` straight from Arrow columns, no full materialization.
- Measurable render/memory improvement on a large result set.
*Effort: S–M · Priority: P2.*

### M5 — Packaging & adoption

**M5.1 — Publish artifacts (image, chart, crate).**
*Problem:* every crate is `publish = false` (`crates/*/Cargo.toml`); the Helm image is
a bare local `repository: verdigris` with an empty tag (`deploy/helm/verdigris/values.yaml`);
the `vdg` crate name is unverified on crates.io (`CLAUDE.md`, `BACKEND_STATUS.md`).
Nothing is pushed anywhere.
*Why it matters:* "one `helm install`, done" isn't real until the image and chart are
pullable from a registry a customer can reach.
*Acceptance:*
- Container image published to a registry (GHCR/ECR) with versioned tags.
- Helm chart published to a chart repo; `values.yaml` points at the real image.
- `vdg` name confirmed/reserved on crates.io if publishing the CLI.
*Effort: M · Priority: P1.*

**M5.2 — Versioned releases + changelog.**
*Problem:* workspace is pinned at `0.0.1` (`Cargo.toml`); history is a handful of
commits with no tags, no `CHANGELOG.md`, no release process
(`git log`; `BACKEND_STATUS.md` "nothing is committed/published").
*Why it matters:* customers pin versions and read changelogs before upgrading.
*Acceptance:*
- Semver tags + GitHub releases with artifacts.
- A maintained `CHANGELOG.md`.
- A documented release/versioning process.
*Effort: S · Priority: P2.*

**M5.3 — Public benchmarks + product docs.**
*Problem:* no published ingest/query/cost benchmarks to back the "object-storage
prices, no rehydration tax" claims; docs exist (`docs/`, `README.md`) but are
architecture/status-oriented. (A parallel docs reorientation is already noted as in
progress — align with it rather than duplicate.)
*Why it matters:* the differentiators (data sovereignty, no rehydration tax) need
numbers, and adopters need task-oriented docs (deploy on EKS+S3, wire Vector, run a
cold query safely).
*Acceptance:*
- Reproducible benchmark harness + published results vs a named baseline.
- Getting-started + operations docs covering the real deploy path.
*Effort: M · Priority: P2.*

---

## Recommended sequencing (my honest take)

The next three, in order:

1. **M4.1 — tier-filtered scans (S, P0).** Small, and it fixes a *correctness* hole in
   the flagship feature: today the cost estimate and the executed query can scan
   different files because `h_query` ignores the `tiers` the UI sends. "Make cost
   legible / no surprise bills" is a stated core principle; this quietly violates it.
   Cheapest high-value fix on the board — do it first.

2. **M1.2 — fast text search (L, P0)** and **M2.1 — real auth (L, P0)**, in parallel.
   These are the two biggest *adoption* blockers. Text search is the job people
   actually use logs for (grep a stack trace); the current `ILIKE`-scan makes that
   slow and expensive and undercuts the "cold logs are always live" pitch. Real auth
   (a single shared static token is a non-starter for any security review) gates every
   serious deployment. Neither is small, but nothing past a demo happens without them.

3. **M3.3 — ingest durability (L, P0).** A log store that can drop logs on a crash or
   under load isn't trustworthy for the incident you're logging for. The in-process
   mutex + buffered-Parquet path needs a WAL/queue and backpressure before real
   production traffic.

**Iceberg (M1.1) is the scale unlock but can trail** — the optimistic-CAS JSON
manifest is *correct* (verified by `concurrent_ingests_preserve_all_rows`), just not
scalable to millions of files. It matters at volume, not for trust on day one, so
sequence it after the P0 adoption/correctness/durability work.

---

## Explicitly out of scope / non-goals (per `CLAUDE.md` principles)

- **A proprietary query DSL.** SQL is the interface on purpose ("SQL, not a
  proprietary query DSL. Portability is a feature"). The search DSL only compiles *to*
  SQL and must stay a convenience, never the sole interface.
- **Pricing or routing by log severity.** Severity decides *placement* only, never
  cost ("Never price by log level"). Any tiering/estimator work must preserve this.
- **Becoming a data toll booth.** The customer's own bucket stays the source of truth;
  no design should route customer bytes through a vendor account or add per-GB
  ingestion margin.
- **DuckDB / native-C++ engines in the hot path.** Ruled out by ADR-001 because native
  code is opaque to the simulator; DataFusion stays the engine. (DuckDB-Wasm in the
  *browser* for client-side re-aggregation is a separate, allowed phase-2 idea.)
- **Silent cold scans.** The pre-scan cost gate is a product feature, not an optional
  guardrail — any query path added must keep the estimate honest (see M4.1).
