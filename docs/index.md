<div class="vdg-hero" markdown>

# Verdigris

<p class="tagline">The layer your infrastructure leaves behind.</p>

</div>

**Verdigris** is a plug-and-play, S3-native log storage and query engine written in Rust —
a self-hostable log platform for teams who want their log data to stay in **their
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
storage classes by lifecycle policy. See [Cost & tiering](cost.md) for the full
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
- **One `helm install`, done** — a single binary + Helm chart brings up ingest, query, UI,
  and tiering on EKS. Data lands in your bucket via IRSA — no static keys.

## Deploy it

Verdigris runs in **your** EKS cluster against **your** S3 bucket — one `helm install`:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1 \
  --set storage.backend=s3 \
  --set storage.s3.bucket=my-company-logs \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<acct>:role/verdigris-s3
```

Then point your existing **Vector / Fluent Bit / OpenTelemetry** pipeline at it — no app
changes — and your logs land as Parquet in your bucket, queried in place. Full walkthrough in
the [Quickstart](getting-started.md); every chart value in [Install on EKS](install.md).

## Where to next

<div class="grid cards" markdown>

- :material-rocket-launch-outline: **[Quickstart](getting-started.md)** — deploy to your EKS
  cluster, point your log pipeline at it, and query in place.
- :material-kubernetes: **[Install on EKS](install.md)** — production on your own S3
  bucket with one `helm install`; IRSA, no static keys.
- :material-upload-network: **[Sending logs](sending-logs.md)** — Vector, Fluent Bit, and
  native OTLP/HTTP.
- :material-database-search: **[Querying](querying.md)** — SQL, the search DSL, Arrow vs
  JSON, and the live tail.
- :material-cash-multiple: **[Cost & tiering](cost.md)** — storage priced by bytes,
  compute as a dial, and the cold-scan cost estimator.
- :material-cog-outline: **[Configuration](configuration.md)** — storage, tiers,
  severity routing, and API authentication.

</div>

---

Licensed under Apache-2.0. Source is maintained privately; this site documents the public
architecture and interfaces.
