# Verdigris

> The layer your infrastructure leaves behind.

**Before changing code, read [`AGENTS.md`](AGENTS.md)** — the tool-neutral contract
covering how to verify a change (`scripts/verify.sh`) and the DST discipline rules
that are easy to violate by accident. This file is the product brief: read it for
*what* Verdigris is and why. `AGENTS.md` is *how to work on it*.

Verdigris is a plug-and-play, S3-native log storage and query engine written in
Rust. It is a Datadog-replacement for teams who want their log data to stay in
**their own cloud account** — no per-GB ingestion margin, no rehydration toll,
no proprietary query language. Logs are written as compacted Parquet to the
customer's own S3 bucket and queried in place.

The name: *verdigris* is the green patina that forms on copper and bronze as it
sits exposed over time — the layer a metal accumulates simply by existing in the
world. That's what logs are: the layer your infrastructure accumulates as it
runs. (It's also a quiet Rust-language pun — verdigris is what oxidation matures
into.) The name evokes; the tagline states the function.

---

## What we're building

A single self-hostable binary that deploys on **EKS + S3** and gives you tiered,
queryable log storage at object-storage prices.

The core product insight, learned from studying Datadog Flex Logs: **do not
price or architect around log severity.** Severity is something the customer
controls and can trivially game (just relabel `error` as `debug`). Instead:

- **Storage** is priced by bytes in S3 — effectively free relative to SaaS log
  vendors. Severity only decides which S3 prefix / storage class a log lands in.
- **Query speed is a separately provisioned dial** (compute), decoupled from
  storage. Want fast? Provision more query compute. Want cheap? Less compute,
  slower queries from colder tiers. This decoupling of storage from compute is
  the single most important design idea — copy it directly from Flex Logs.

### The differentiators over Datadog Flex Logs

These are the two things the commercial incumbent structurally *can't* fix
without breaking its own business model, so they are our wedge:

1. **Data sovereignty** — data never leaves the customer's AWS account. No
   ingestion margin charged on every GB that flows through a vendor's cloud.
2. **No rehydration tax** — queries read Parquet straight out of S3 in place.
   There is no "pull cold logs back into an expensive index to search them"
   step. Cold logs are always live; you only pay compute when you actually
   query, plus the underlying S3/Glacier retrieval cost.

---

## Architecture

```
  pods on EKS
      │  (stdout/stderr)
      ▼
  Vector / Fluent Bit  (DaemonSet)          ← ingestion
      │
      ▼
  Verdigris ingest service                  ← batches → Parquet, writes Iceberg metadata
      │
      ▼
  S3 (customer's own bucket)                ← tiered via S3 lifecycle policies
      ├─ hot    : S3 Standard        (recent, interactive)
      ├─ warm   : Glacier Instant / Standard-IA
      └─ cold   : Glacier Flexible   (cheapest queryable; minutes-to-hours restore)
      ▲
      │  (query in place — no rehydration)
  Verdigris query engine  (DataFusion on Parquet/Iceberg; Trino later for scale)
      │
      ▼
  Query API  +  optional Grafana / SQL frontend
```

### Components

- **Ingest** — receives logs (native OTLP + Vector/Fluent Bit sinks), buffers,
  writes compacted Parquet with Iceberg table metadata to S3.
- **Compaction** — background job that merges the millions of tiny Parquet files
  streaming logs produce into 100MB–1GB files. **This is core, not optional** —
  it's the difference between a toy and something usable, and it solves two
  problems at once: query performance (tiny files destroy scan speed) and the
  Glacier 40KB-per-object metadata tax (millions of small objects waste money).
- **Tiering** — S3 lifecycle rules move data hot → warm → cold over time.
  Severity-based routing rules decide which prefix a log lands in at write time.
- **Query engine** — DataFusion (pure Rust) reading Parquet/Iceberg directly from
  S3. SQL as the query language (portable, no proprietary DSL to learn). Chosen
  over DuckDB because it's Rust + `object_store` + tokio, so it can participate in
  deterministic simulation; DuckDB's native C++ is opaque to the simulator (see
  `docs/dst-architecture.md`). Trino is a later option for distributed scale.
- **Cost estimator** — **critical UX, not a nice-to-have.** Before a query that
  scans cold storage, surface "this will scan ~X GB from Glacier and cost ~$Y,
  continue?" Glacier bills retrieval by scanned-GB, and one careless query over
  cold data can hand a user a four-figure bill. The estimator is part of what
  makes Glacier-backed logs *safe to actually use* — it's a real differentiator,
  not a guardrail bolted on.

---

## Pricing reference (AWS us-east-1, as of mid-2026 — verify before relying on these)

Storage per GB/month:
- S3 Standard: ~$0.023
- Glacier Instant Retrieval: ~$0.004  (storage) + ~$0.03/GB per GET
- Glacier Flexible Retrieval: ~$0.0036 (storage) + retrieval below
- Glacier Deep Archive: ~$0.00099 (storage)

Glacier Flexible retrieval modes:
- Bulk: 5–12 hours, free
- Standard: 3–5 hours, ~$0.01/GB
- Expedited: 1–5 minutes, ~$0.03/GB

Key gotcha to design around: the **cheap tier is not 1-minute queryable** by
default. True interactive cold queries mean either Glacier Instant (pay per GET)
or paying Expedited retrieval. The cost estimator must make this trade-off
visible.

---

## Naming & namespace conventions

- **Brand / product name (everywhere users see it):** `Verdigris`
- **Crate + CLI binary:** the bare `verdigris` crate name is already taken on
  crates.io (an unrelated, inactive CoAP browser demo). Publish under a short
  free name instead. Preferred binary: **`vdg`** (e.g. `vdg query ...`,
  `vdg ingest ...`). Alternative if the full word is wanted in the package
  name: `verdigris-rs` (the `-rs` suffix is an accepted Rust convention).
  *(Availability of `vdg` not yet verified — confirm on crates.io before
  publishing; crate names are permanent and first-come.)*
- **Brand color:** oxidized-copper green/teal (think Statue of Liberty). The
  category is all blues/purples/sterile dark mode — own the green.

Optional component sub-names (from the same metal-patina world, if useful):
`Sheen` (hot-path query layer), `Tarnish` (noisy-log filter/drop pipeline),
`Patina` (the cold tier).

---

## Build order

Ship the skeleton first, fake the hard 20% initially, then harden.

1. **Ingest → Parquet → S3.** Vector DaemonSet → ingest service → write Parquet
   + Iceberg metadata to a bucket. Get logs landing in S3 at all.
2. **Query in place.** DataFusion reads the Parquet/Iceberg from S3; expose a basic
   SQL query API. Prove "no rehydration" works end to end on the hot tier.
3. **Tiering.** S3 lifecycle policies + severity-based write-time routing. Hot /
   warm / cold prefixes.
4. **Compaction.** Background small-file merge job. (Do not ship to real users
   without this — see Architecture note.)
5. **Cost estimator.** Pre-query scan-size + dollar estimate, with a confirm gate
   on cold-tier scans.
6. **Helm chart.** Make it genuinely "plug and play on EKS" — one `helm install`.
7. **Frontend.** Grafana datasource or a minimal SQL UI.

Hard parts that separate a demo from a product (expect to spend real time here):
small-file compaction, schema evolution (log fields change constantly), fast
text search over columnar Parquet (good at `WHERE severity='error'`, bad at
"grep this stack trace" — consider Bloom filters / an inverted index), and the
Glacier restore UX.

---

## Prior art to study (don't reinvent; know the gaps)

S3-native / object-storage log tools already in this space — read these before
building, either to borrow patterns or to find the gap that justifies Verdigris:
Quickwit, OpenObserve, Grafana Loki, VictoriaLogs, SigNoz, Parseable,
groundcover. The recurring wedge across the newer ones is the same as ours:
data stays in the customer's own object storage, queried in place, no vendor
per-GB margin.

---

## Testing architecture — DST (see `docs/dst-architecture.md`)

We test at trillion-row / petabyte scale **without running at scale**, via
Deterministic Simulation Testing (madsim): the control plane runs single-threaded
in logical time, so a "trillion-row query" completes in seconds because no real
bytes move. This is a **core constraint, not a test-time add-on** — it dictates how
every component is written.

Query engine is **DataFusion** (pure Rust), chosen over DuckDB precisely because
native C++ is opaque to the simulator. Query *execution* still sits behind a
`ScanExecutor` seam (real DataFusion in prod, a modeled-latency stub in sim) and its
throughput is covered by separate
real-scale *calibration* runs. DST tests orchestration/scheduling/timing-model;
calibration supplies absolute GB/s. Never one without the other.

Four seams every component must respect from v1 (cannot be retrofitted):
`ObjectStore` (real S3 / `SimObjectStore`), `Clock`, `ScanExecutor`, `Rng`. The
control plane is **sans-I/O** — pure `(state, event) -> (state, effects)`; no direct
time, I/O, threads, or randomness in core logic. The `SimObjectStore` latency/cost
model and the **cost estimator** are the same model and share code.

## Frontend

**For current frontend status & roadmap, read [`web/STATUS.md`](web/STATUS.md) first** — it
covers both frontends, what's done, and what's left.

There are two: `frontend/` (the original vanilla no-build prototype, now wired to the
live backend) and `web/` (the production rebuild: Vite + SolidJS + uPlot, Arrow-ready,
multi-tenant, on-prem-capable). The prototype is the reference; `web/` is what ships.

The prototype is a dependency-free SPA (vanilla JS + CSS + inline SVG; open
`index.html`). **Its entire backend integration is one file: `frontend/api.js`** —
the return shapes there are the API contract the UI renders. `web/` has the same
single swap point in `web/src/lib/api.ts`, with the typed contract in
`web/src/lib/types.ts`. Backend gaps are tracked as GitHub issues (see epic #34).

If you are doing any frontend work, read **`frontend/AGENTS.md` first** — it is the
contract for that directory (architecture, file map, page-module contract, the
backend swap point, design-system conventions, how to add a page). Verify changes
with `node frontend/_verify.js`. Brand is oxidized-copper green; design tokens live
in `:root` in `frontend/styles.css` — never hardcode colors.

## Principles

- **SQL, not a proprietary query DSL.** Portability is a feature.
- **The customer's bucket is the source of truth.** We never become a data toll
  booth on our own users.
- **Storage cheap, compute provisioned.** Never price by log level.
- **Make cost legible.** No surprise bills — estimate before you scan.
- **One binary, `helm install`, done.** Plug-and-play is the whole promise.
- **Deterministic by construction.** Nondeterminism lives only behind seams; the
  control plane is sans-I/O so scale is testable in simulation, not just in prod.
