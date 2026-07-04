# ADR-0002: JSON manifest as an Apache Iceberg stand-in

**Status:** Accepted
**Date:** 2026-06-27

## Context

Querying Parquet in place requires a **table catalog** — a way to know which files make up
a logical table, plus enough per-file statistics to prune scans (time range, tier, size)
without opening every file. The natural end-state for this is **Apache Iceberg**: it gives
snapshots, hidden partitioning, schema evolution, and — critically — safe concurrent
commits via optimistic concurrency.

But Iceberg is a large dependency and a substantial integration. Adopting it before the
rest of the system exists would front-load the hardest piece and slow down proving the core
"ingest → tier → compact → query in place" loop. At the same time, the catalog format is
load-bearing: query registration, time pruning, tier accounting, and compaction all read
it, so it must exist from day one in *some* form.

## Decision

Use a **JSON manifest** as an explicit, temporary stand-in for Iceberg. The manifest is a
per-table file listing every Parquet object with its stats: byte size, row count, min/max
timestamp, tier, and compaction generation. It is append-on-write and reloaded on read.

This is enough to drive everything the current system needs:

- **manifest-driven query registration** (register exactly the catalog's files, never a
  directory scan),
- **time-range pruning** in both the planner and the cost estimator,
- **tier accounting** for storage/cost reporting,
- **compaction bookkeeping** (small-file detection, generation tracking, manifest-first
  deletes).

The manifest is treated as an *interface*, not a permanent format: components depend on the
catalog's capabilities, not on it being JSON.

## Consequences

**Easier now.** The full product loop works end to end without taking on Iceberg's
complexity. Compaction, tiering, and the cost estimator all have the metadata they need.

**Harder / owed later.** The JSON manifest has no concurrent-commit safety: two writers
read-modify-writing it will race and can lose data or corrupt the catalog. Today this is
mitigated by (a) a per-process mutex serializing writes and (b) a single designated writer
role at the deployment level (see [ADR-0003](0003-ingest-query-role-split.md)). Neither
gives *multi-writer* safety.

**Migration path.** Replacing the manifest with Iceberg is a future ADR (ADR-000x, TBD). It
should preserve the same catalog capabilities (file list + stats + pruning) so the query,
compaction, and estimator code paths change minimally.

## Update (2026-07-04): optimistic-concurrency commits

The "no concurrent-commit safety" gap above has been **closed for correctness** without
adopting full Iceberg, by adding optimistic concurrency directly to the JSON manifest:

- **Content-addressed data files.** Data files are now named by a hash of their bytes
  (`part-<hash>.parquet`) instead of a shared per-tier counter, so two writers can never
  collide on an object path. Deterministic (no RNG), so simulation stays reproducible.
- **Compare-and-swap manifest commits.** The manifest is written with a conditional put
  (`object_store` `PutMode::Create`/`Update` against the object's ETag/version). A writer
  that lost the race gets a `Conflict`, reloads the current manifest, re-applies its append,
  and retries — bounded, with per-path dedupe so a retry can't double-count. Backends
  without conditional-put support fall back to a plain put (safe under the single-writer
  role). Ingest and compaction both commit this way.

**What this does and doesn't buy.** It removes silent lost-update / path-collision
corruption, so multiple writers to one table are now *correct*, and the `ingest`/`query`
role split ([ADR-0003](0003-ingest-query-role-split.md)) becomes defense-in-depth and
resource isolation rather than the sole correctness mechanism. It does **not** provide
Iceberg's snapshots, hidden partitioning, schema evolution, or a catalog service — those
remain the future Iceberg ADR. The manifest is still a single JSON object rewritten per
commit, so very high commit rates will contend on it; Iceberg's manifest-list structure is
the scaling answer. This is the honest interim: correct concurrent commits on a simple
catalog, not the full table format.

**Testability.** Because the catalog is plain data, the DST harness can *fabricate* a
catalog that declares a trillion-row table (file counts, sizes, stats) with no bytes on
disk — which is how "trillion-scale" planner tests run in seconds. A real Iceberg
integration must keep this fabrication cheap.
