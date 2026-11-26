---
description: Store logs as Parquet in your own S3 bucket, query them with SQL, and estimate scan costs before you run a query.
hide:
  - navigation
  - toc
  - footer
---

<div class="vdg-home" markdown>

<header class="vdg-intro" markdown>

# Verdigris documentation { #verdigris }

Store logs as Parquet in your own S3 bucket and query them in place with SQL.
Estimate what a scan will cost before you run it.

<div class="vdg-intro__actions" markdown>

[Quickstart :material-arrow-right:](getting-started.md){ .md-button .md-button--primary }
[How Verdigris works](ARCHITECTURE.md){ .vdg-text-link }

</div>
</header>

<div class="vdg-start" markdown>

<section class="vdg-start__guide" aria-labelledby="where-to-next" markdown>

## Get started { #where-to-next }

- :material-console: [Try it locally](getting-started.md#evaluate-locally)
  Run the demo without an AWS account.
- :material-kubernetes: [Install on EKS](install.md)
  Connect your S3 bucket and configure IAM.
- :material-upload-network: [Send logs](sending-logs.md)
  Set up Vector, Fluent Bit, or OpenTelemetry.

</section>

<section class="vdg-start__example" aria-labelledby="query-example" markdown>

## Query example

Find the ten services that logged the most errors.

```sql
SELECT service,
       count(*) AS errors
FROM logs
WHERE level = 'ERROR'
GROUP BY service
ORDER BY errors DESC
LIMIT 10;
```

[Read the query guide :material-arrow-right:](querying.md){ .vdg-text-link }

</section>

</div>

<nav class="vdg-reference" aria-labelledby="reference" markdown>

## Reference

- [HTTP API](API.md)
- [Configuration](configuration.md)
- [Cost & tiering](cost.md)

</nav>
</div>

<div class="vdg-home__notes" markdown>

## Design constraints

Two constraints shape every component. They are stated here as engineering
positions, because most of the architecture falls out of them:

1. **The operator's bucket is the source of truth.** Verdigris holds no data of
   its own — the Parquet files and the manifest that catalogs them live in the
   operator's account. Everything else (compaction, tiering, query, cost
   accounting) must work against object storage semantics: immutable objects,
   conditional puts, no filesystem.
2. **Storage and query speed are priced independently.** Storage cost is bytes
   in S3; query speed is provisioned compute. Log severity decides *placement*
   (which tier a record lands in) and never *price* — a pricing model keyed on
   severity can be gamed by relabeling, so the system refuses to build one.

A third constraint is about how the system is built rather than what it does:
**deterministic by construction**. The control plane is written sans-I/O, with
every source of nondeterminism (clock, storage, randomness, scan execution)
behind a seam, so large-scale behavior can be tested in simulation. See
[Testing (DST)](dst-architecture.md) — including its honest account of which
seams are finished and which are not.

## Architecture at a glance

Verdigris is written in Rust. A single `vdg` binary handles ingestion,
compaction, tiering, and serving SQL queries through Apache DataFusion.

![Verdigris architecture: pods on EKS send logs through Vector, Fluent Bit or an OpenTelemetry Collector into Verdigris ingest, which writes tiered Parquet to your own S3 bucket; the DataFusion query engine reads it in place and serves the API, web UI and Grafana.](assets/architecture.svg)

The write path: records arrive over HTTP (NDJSON or OTLP/JSON), are routed to a
tier by severity, batched, encoded as zstd Parquet with bloom filters on the
lookup columns, content-addressed, and committed to a JSON manifest under
optimistic compare-and-swap. The read path: the planner selects files from that
manifest (tier → time window → per-file value stats → trigram sets), and
DataFusion reads exactly the selected files — the same selection the cost
estimator priced, by construction, because both call the same function.

Storage tiers: <span class="tier hot">hot</span> <span class="tier warm">warm</span>
<span class="tier cold">cold</span> — assigned at write time by severity, aged
across S3 storage classes by lifecycle policy. Details in
[Cost & tiering](cost.md).

## The interesting problems

The parts of the codebase most worth reading, with where to read about them:

- **Deterministic simulation.** The control plane never touches the wall clock,
  I/O, or entropy directly, which is what lets a fabricated 4-trillion-row
  catalog be priced in a unit test and an 8-hour Glacier restore run in
  microseconds of logical time. [Testing (DST)](dst-architecture.md) covers the
  four seams — and which of them are real today versus still intentions.
- **A cost model the simulator and the user share.** The pre-query estimator and
  the simulation's billing meter call the same `core::cost` functions, so the
  number shown before a scan and the number the simulated store bills cannot
  drift apart. A test pins estimate == billed.
- **Free-text pruning with zero false negatives.** Per-file character-trigram
  presence sets over a 37-symbol alphabet — an exact 6,332-byte bitmap, not a
  probabilistic filter. A file missing any trigram of a search term provably
  contains no match; a property test brute-forces every substring of every
  recorded message to hold that line. [Architecture](ARCHITECTURE.md) has the
  details.
- **A catalog on object storage.** Data files are content-addressed (writers can
  never collide on a path) and the manifest commits by compare-and-swap against
  its ETag, with retry on conflict. [ADR-0002](adr/0002-manifest-as-iceberg-standin.md)
  is explicit about what this JSON stand-in does *not* give us and when real
  Iceberg is the answer.
- **Running small.** Compaction streams bins through the encoder instead of
  materializing them (a 7× peak-memory reduction, measured), and query execution
  is bounded twice — a spill-to-disk memory pool for operators, and a streaming
  per-batch ceiling on result size, because a non-aggregating `SELECT *` never
  asks the memory pool for anything.

## Status

Verdigris is **alpha**, built in the open as an engineering project — it is not
a commercial product and is not trying to become one. The rule the project
holds itself to: **claims must match code.** Where a capability is partial or
modeled, the docs and the [FAQ](faq.md#whats-working-vs-early) say so, and the
gaps are tracked as issues with their acceptance criteria written as tests. The
[parity scorecard](https://github.com/explise/verdigris/issues/34) is the
long-range map: what "credible at scale" means, capability by capability, each
defined by the tests that would prove it.

!!! quote "Why the name"
    Verdigris is the green patina that forms on copper over time. Logs are the
    layer your infrastructure accumulates as it runs.

</div>
