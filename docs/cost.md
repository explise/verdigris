# Cost & tiering

The cost model *is* the product. Two ideas carried through the whole system:

1. **Storage is priced by bytes in S3** — effectively free relative to hosted log services.
   Severity only decides which prefix / storage class a log lands in. Placement, never price.
2. **Query speed is a separately provisioned dial (compute), decoupled from storage.** Want
   fast? Provision more query cores. Want cheap? Fewer cores, slower scans from colder tiers.

Because the data lives in **your** bucket and queries read it in place, there's no ingestion
margin on every GB and no rehydration tax to pull cold logs back into an index. You pay S3
storage, plus compute when you query, plus the underlying Glacier retrieval cost when a scan
touches cold data — and Verdigris shows you that last number *before* you run the query.

## The three tiers

Severity routes a log to a tier at write time; an S3 lifecycle policy then ages data across
storage classes. Defaults:

| Tier | Storage class | Default routing | Retrieval |
|---|---|---|---|
| <span class="tier hot">hot</span> | S3 Standard | ERROR | queried in place, no retrieval charge |
| <span class="tier warm">warm</span> | Glacier Instant Retrieval | WARN, INFO | pay per GET |
| <span class="tier cold">cold</span> | Glacier Flexible Retrieval | DEBUG | cheapest queryable; minutes-to-hours restore |

Routing is configurable (`[routing]` — see [Configuration](configuration.md#routing-routing)).
The severity → tier mapping decides *placement only*; it never changes what a byte costs.

## Pricing reference

!!! warning "Verify before relying"
    These are AWS us-east-1 approximations (mid-2026). Confirm current AWS pricing before
    making cost decisions. They are the exact numbers the cost model and the estimator use —
    the simulation and the estimator share one cost module on purpose, so a shown estimate is
    the same math production bills.

Storage, USD per GiB-month:

| Storage class | Storage |
|---|---|
| S3 Standard | ~$0.023 |
| Glacier Instant Retrieval | ~$0.004 |
| Glacier Flexible Retrieval | ~$0.0036 |
| Glacier Deep Archive | ~$0.00099 |

Retrieval, USD per GiB scanned:

| Class | Rate |
|---|---|
| S3 Standard | $0 (queried in place) |
| Glacier Instant | ~$0.03 per GET, any mode |
| Glacier Flexible — Bulk (5–12 h) | free |
| Glacier Flexible — Standard (3–5 h) | ~$0.01 |
| Glacier Flexible — Expedited (1–5 min) | ~$0.03 |

!!! note "The gotcha the estimator exists to surface"
    The cheapest tier is **not** 1-minute queryable by default. Truly interactive cold
    queries mean either Glacier Instant (pay per GET) or paying Expedited retrieval on
    Flexible. The cost estimator makes this trade-off visible so a careless scan over cold
    data can't hand you a surprise four-figure bill.

## The pre-query cost estimator

Before a query that scans cold storage, `POST /v1/query/estimate` surfaces "this will scan
~X GB and cost ~$Y, continue?" — and the UI puts a **confirm gate** in front of cold-tier
scans. This is what makes Glacier-backed logs safe to actually use.

Request:

```json
{ "tiers": ["hot","warm","cold"], "sql": "… | last 1h" }
```

`sql` is optional; when present, the estimate is pruned by the query's time window
(metadata-only — no bytes move). Response:

```json
{ "scanGB": 0.00008, "scanBytes": 87656, "costUsd": 0.0000008, "coldRestore": true,
  "restoreMs": 18000000, "scanMs": 3, "filesTouched": 1, "filesTotal": 3,
  "perTier": [ { "tier": "cold", "gb": 0.00008, "costUsd": 0.0000008 } ] }
```

- `costUsd` = scanned-GB × the per-tier retrieval rate (hot $0, warm ≈ $0.03/GB Glacier
  Instant, cold ≈ $0.01/GB Glacier Flexible **Standard** mode — what the estimate assumes).
- `coldRestore` / `restoreMs` flag when a scan needs a Glacier thaw and roughly how long
  (Standard mode ≈ 4 h; Expedited ≈ 5 min; Bulk ≈ 8 h).
- `scanMs` is the modeled scan time at the provisioned throughput
  (`query.cores × query.modeledMibpsPerCore`) — the compute dial.

!!! note "Estimate fidelity today"
    Pruning is by the query's time window plus coarse per-file min/max stats. Compacted
    (wide) files can't be time-pruned finely, so `scanBytes` is an upper bound per file.
    Finer predicate / row-group pruning is on the roadmap. The estimate is deliberately
    conservative — it won't *under*-quote a scan.

## Compaction

Streaming ingest produces millions of tiny Parquet files. That's a problem twice over: tiny
files wreck scan speed, and millions of small objects waste money against Glacier's
per-object metadata tax. Compaction merges each tier's small files into larger ones, rewrites
the manifest, and deletes the old objects (manifest-first, so it's crash-safer). It's
deterministic and idempotent.

Run it on demand:

```bash
vdg compact --table logs --target-mb 64
```

`/v1/storage/tiers` reports the real small-file / compacted counts and the compaction
generation, so the UI's Storage page reflects actual state.

## The lifecycle policy

Age-based transitions move data to colder, cheaper classes over time, then expire it.
Defaults: hot → warm after **3 days**, warm → cold after **30 days**, expire after **400
days** (all configurable via `[lifecycle]`). `vdg lifecycle` renders these into an AWS
`PutBucketLifecycleConfiguration` policy; on EKS the chart applies it automatically. See
[Install → S3 lifecycle policy](install.md#s3-lifecycle-policy-auto-applied).

## What the dashboards show

`GET /v1/cost` returns month-to-date / projected / last-month figures computed from the
manifest and the cost model, a per-tier breakdown, and an illustrative Verdigris-vs-SaaS
comparison. Honest caveat: `expensiveQueries` is empty until query-history tracking exists,
and the SaaS comparison figure is illustrative, not a benchmarked quote.
