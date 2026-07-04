<div class="vdg-hero" markdown>

# Verdigris

<p class="tagline">The layer your infrastructure leaves behind.</p>

</div>

**Verdigris** is a plug-and-play, S3-native log storage and query engine written in Rust —
a self-hostable Datadog alternative for teams who want their log data to stay in **their
own cloud account**. Logs are written as compacted Parquet to the customer's own S3 bucket
and queried **in place**: no per-GB ingestion margin, no rehydration toll, no proprietary
query language.

!!! quote "Why the name"
    *Verdigris* is the green patina that forms on copper as it sits exposed over time — the
    layer a metal accumulates simply by existing in the world. That's what logs are: the
    layer your infrastructure accumulates as it runs. (It's also a quiet Rust pun —
    verdigris is what oxidation matures into.)

## Why Verdigris

Two things the commercial incumbents structurally *can't* fix without breaking their own
business model — and therefore the wedge:

1. **Data sovereignty.** Data never leaves your AWS account. No vendor cloud in the path
   charging an ingestion margin on every GB.
2. **No rehydration tax.** Queries read Parquet straight out of S3 in place — there is no
   "pull cold logs back into an expensive index to search them" step. Cold logs are always
   live; you pay compute only when you query, plus the underlying retrieval cost.

The core principle: **never price or architect around log severity.** Storage is priced by
bytes in S3; query speed is a *separately provisioned dial* (compute), decoupled from
storage. Severity only decides which prefix / storage class a log lands in — placement,
never price.

## Architecture at a glance

```
  pods on EKS
      │  (stdout/stderr, OTLP)
      ▼
  Vector / Fluent Bit (DaemonSet) · OTel Collector      ← ingestion
      │
      ▼
  Verdigris ingest  ── batches → Parquet, catalog metadata to S3
      │
      ▼
  S3 (your own bucket)                                  ← tiered via S3 lifecycle
      ├─ hot    : S3 Standard
      ├─ warm   : Glacier Instant / Standard-IA
      └─ cold   : Glacier Flexible
      ▲
      │  (query in place — no rehydration)
  Verdigris query engine  (Apache DataFusion on Parquet)
      │
      ▼
  Query API + Web UI + Grafana datasource
```

Storage tiers: <span class="tier hot">hot</span> <span class="tier warm">warm</span>
<span class="tier cold">cold</span> — decided at write time by severity, aged across S3
storage classes by lifecycle policy. See [Architecture](ARCHITECTURE.md) for the full
breakdown.

## Highlights

- **Ingest** — `POST /v1/ingest` (NDJSON/JSON) for Vector & Fluent Bit, plus a native
  **OTLP/HTTP** receiver for OpenTelemetry Collectors.
- **Query in place** — Apache DataFusion reads Parquet directly from object storage. **SQL**
  is the query language (plus a concise search DSL). Rows travel as **Apache Arrow** or JSON.
- **Cost estimator** — before a query scans cold storage, Verdigris surfaces "this will scan
  ~X GB from Glacier and cost ~$Y, continue?" — so Glacier-backed logs are safe to use.
- **Tiering & compaction** — severity-based routing + S3 lifecycle; a background job merges
  the millions of tiny Parquet files streaming logs produce.
- **Concurrency-safe commits** — content-addressed files + optimistic (compare-and-swap)
  manifest commits, so multiple writers never corrupt or lose data.
- **Deterministic by construction** — nondeterminism lives only behind four seams, so
  trillion-row scale is testable in [simulation](dst-architecture.md), not just production.
- **One `helm install`, done** — a single binary + Helm chart brings up ingest, query, UI,
  and tiering on EKS. Data lands in your bucket via IRSA — no static keys.

## Quickstart

Run the whole loop locally:

```bash
# 1. Start the server (first build pulls DataFusion, ~1.5 min).
cargo run -p vdg --features serve -- serve --table logs

# 2. In another terminal, keep synthetic logs flowing.
cargo run -- ingest --table logs --follow

# 3. Open the UI.
open http://localhost:8080
```

Deploy on EKS + S3 with one `helm install` — see [Deployment](deployment.md).

## Where to next

<div class="grid cards" markdown>

- :material-vector-arrange-below: **[Architecture](ARCHITECTURE.md)** — crates, the four
  seams, write/read paths, tiering, the cost model, split-role deployment.
- :material-api: **[HTTP API](API.md)** — every endpoint, request/response shapes, roles, auth.
- :material-kubernetes: **[Deployment](deployment.md)** — the container image and Helm chart.
- :material-flask-outline: **[Testing (DST)](dst-architecture.md)** — deterministic
  simulation at trillion-row scale without running at scale.
- :material-file-document-multiple-outline: **[Decision Records](adr/README.md)** — the
  architecturally significant choices and their trade-offs.

</div>

---

Licensed under Apache-2.0. Source is maintained privately; this site documents the public
architecture and interfaces.
