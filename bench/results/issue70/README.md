# Issue #70 — warm exact trace lookup

Target: **≤15 ms p50 / ≤20 ms p95** for `trace_id:<id>` on the 1M-row
Verdigris + MinIO fixture from #63, with the existing **4 CPU / 3 GiB vdg +
4 CPU / 3 GiB MinIO** budget.

## Verdict

Met in all three fresh deployments:

| Arm | Run | Trace p50 | Trace p95 | Search p50 | Aggregate p50 |
|---|---|---:|---:|---:|---:|
| Before (`sha256:6a6e2a0b…`) | 1 | 31.83 | 35.47 | 38.44 | 41.30 |
| Before | 2 | 31.39 | 35.41 | 36.64 | 39.02 |
| Before | 3 | 31.70 | 34.39 | 36.08 | 39.28 |
| **After (`sha256:8f1ed0c1…`)** | **1** | **14.01** | **17.11** | **20.59** | **21.13** |
| **After** | **2** | **14.80** | **16.86** | **22.65** | **21.40** |
| **After** | **3** | **13.89** | **15.77** | **19.01** | **22.13** |

Service-search and minute-aggregation medians also improved (they share the same
object-store client and metadata cache), so the acceptance condition "within
10% of baseline" is met in the safe direction.

## What changed

1. **One object-store client per process.** `AppState` now holds the store
   instead of rebuilding an S3 client on every request.
2. **Shared, bounded Parquet metadata cache.** A process-wide
   `DefaultFilesMetadataCache` (`query.metadata_cache_mib`, default 64 MiB) is
   passed into each per-query DataFusion runtime. It caches footer, row-group,
   bloom-filter and page-index metadata. Each query still gets its own execution
   memory pool.
3. **Schema cache keyed by immutable file identity.** The first selected file's
   path + size keys a cached `SchemaRef`; a warm query skips the listing/HEAD/
   footer schema inference entirely. Files are content-addressed, so the key
   cannot go stale in place.
4. **Audit persistence off the response path.** The audit doc is a CAS
   read-modify-write against the object store. Records now go to a bounded
   background queue; the in-memory history is still updated synchronously.
5. **Benchmark fixture covers the requested cases.** Rotating hit IDs spread
   across the corpus, a missing ID, a trace written to one row in each tier, a
   tier-scoped Verdigris lookup, raw per-request timings, image IDs, and the
   metadata-cache hit delta.

Warm trace phase split (after, p50): engine **12–13 ms**, manifest **<1 ms**,
planning **<1 ms**. The engine number includes Parquet scan and JSON encoding.

## Mechanism proof

- `crates/query/tests/engine.rs::shared_query_cache_reuses_metadata_without_changing_results`
  runs the same lookup cold and warm, asserts identical rows, and asserts the
  shared cache's hit count rises while the entry count stays fixed (no footer
  re-reads).
- `crates/query/tests/row_group_pruning.rs::trace_lookup_prunes_by_bloom_when_statistics_overlap`
  builds 30 row groups whose `trace_id` min/max all bracket the target (statistics
  cannot prune) and asserts `row_groups_pruned_bloom_filter=30 total → 1 matched`
  from DataFusion's own `EXPLAIN ANALYZE` metrics, plus the one matching row.
- Every after run reported `metadataHits` delta **2756** across the 110 trace
  requests (10 warmups + 100 measured) for that run.

## Reproduction

```sh
# before image (origin/main 296d865)
docker build -f bench/Dockerfile.vdg -t verdigris-bench:issue70-base .
SYSTEMS=vdg ROWS=1000000 ITERATIONS=100 WARMUP=10 \
  OUT=/tmp/issue70-base VDG_IMAGE=verdigris-bench:issue70-base \
  VDG_SKIP_BUILD=1 bench/compare/logs/run.sh

# after image (this branch)
docker build -f bench/Dockerfile.vdg -t verdigris-bench:issue70-after .
SYSTEMS=vdg ROWS=1000000 ITERATIONS=100 WARMUP=10 \
  OUT=/tmp/issue70-after VDG_IMAGE=verdigris-bench:issue70-after \
  VDG_SKIP_BUILD=1 bench/compare/logs/run.sh
```

Run each arm three times into separate `OUT` directories. Raw JSON, metadata,
machine info and console logs for the six runs are in `base-1..3/` and
`after-1..3/`.

Host: 16 vCPU / 15.6 GiB, kernel 7.0.0-38, Docker 29.9.0. Each run starts a
fresh MinIO volume and vdg container and waits for compaction to drain before
timing queries.
