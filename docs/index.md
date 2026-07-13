<div class="vdg-hero" markdown>

# Verdigris

<p class="tagline">The layer your infrastructure leaves behind.</p>

</div>

**Verdigris** is an S3-native log storage and query engine written in Rust. Logs
are written as compacted Parquet to an S3 bucket the operator owns and queried
**in place** with Apache DataFusion — there is no separate index to build, and no
step that copies cold data back into a hot store before it can be searched. One
binary (`vdg`) does ingest, compaction, tiering, query, and serving.

!!! quote "Why the name"
    *Verdigris* is the green patina that forms on copper as it sits exposed over
    time — the layer a metal accumulates simply by existing in the world. That's
    what logs are: the layer your infrastructure accumulates as it runs. (It's
    also a quiet Rust pun — verdigris is what oxidation matures into.)

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

## Where to next

<div class="grid cards" markdown>

- :material-source-branch: **[Architecture](ARCHITECTURE.md)** — crates, seams,
  write/read paths, compaction, and the split-role topology.
- :material-test-tube: **[Testing (DST)](dst-architecture.md)** — the
  deterministic-simulation design, and its honest status.
- :material-file-document-outline: **[Decision records](adr/README.md)** — why
  DataFusion, why a JSON manifest, why split roles.
- :material-rocket-launch-outline: **[Quickstart](getting-started.md)** — run it
  locally or deploy to EKS and query in place.
- :material-database-search: **[Querying](querying.md)** — SQL, the search DSL,
  Arrow vs JSON, live tail.
- :material-cash-multiple: **[Cost & tiering](cost.md)** — the tier model, the
  pricing reference, and the pre-query estimator.

</div>

---

Apache-2.0. Source: [github.com/explise/verdigris](https://github.com/explise/verdigris).
