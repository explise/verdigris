# Issue #83 · Trace query profiling

## Finding

Warm exact-trace hits spent **10.46 ms p50 in execution** and **4.42 ms p50
waiting for durable audit**. Logical and physical planning were 0.25 ms
and 0.79 ms p50 separately. A hit returned one row with 1.15 MiB median
logical Parquet read bytes. The median hit pruned nine row groups.

Warm misses pruned ten row groups, produced zero measured Parquet data
reads at the median, and spent just 0.34 ms p50 executing. Audit wait
dominated the miss at 5.09 ms p50. This is evidence to investigate the
remaining reads on hits before investing in reusable plans. The execution
measurement includes I/O, decode, operators and scheduling, so it does
not identify how much of execution is I/O wait alone.

**Next experiment.** Within #84/#85, compare the existing reader with a
bounded, request-equivalent cache of immutable Parquet ranges. Keep the
same rotating hit schedule, SQL, full result and audit policy. Record
object-read wait, bytes, execution p50/p95, total p50/p95 and cache memory.
Aim to remove at least 3 ms from hit execution p50 and verify total
latency against the 15/20 ms target on each deployment. If the cached
variant leaves execution time largely intact, profile decode and TopK
before considering a new index. Keep misses and cross-tier results in
the correctness and regression checks.

## Setup

The optimized Verdigris build uses the audit implementation from PR #75
(commit `e081175`); the profiler is `f259e1a` and the runner is `8184034`.
The corrected #81 stack is still pending, so this is a provisional
MinIO baseline, with no query admission gate. Verdicts about real AWS S3
latency require #11.

Each 1M-row deployment used a fresh MinIO volume, 4 CPU / 3 GiB per
service, 10 warmups and 100 samples per shape with diagnostics on and
another 100 with them off. On/off requests are paired with alternating
order. Ingest and compaction settle and the result checks pass before
warm measurements. After each Verdigris restart, only the named query
runs before the next restart. MinIO stays up and the OS cache is retained.
All three warm runs had 1,067 audit acknowledgements, zero failures,
zero timeouts and zero saturation. Latency uses median and nearest-rank
p95. Phase percentiles are calculated independently and do not add up
to a request percentile.

```sh
docker build -t verdigris-bench:issue83 .
for run in 1 2 3; do
  VDG_IMAGE=verdigris-bench:issue83 OUT="/tmp/vdg-83-ready-$run" \
    bench/compare/logs/run-profile.sh
done
VDG_IMAGE=verdigris-bench:issue83 OUT=/tmp/vdg-83-ready-20m-final \
  ROWS=20000000 BATCH_ROWS=50000 ITERATIONS=20 WARMUP=5 \
  bench/compare/logs/run-profile.sh
```

Raw samples, phase timings, read counters, machine/config metadata,
restart timings and the representative physical plan are in
[run 1](release-1.json.gz), [run 2](release-2.json.gz) and
[run 3](release-3.json.gz), and the [20M sizing run](release-20m.json.gz). Use `gzip -dc` to read the JSON. The plan for the
representative trace hit is a sort-preserving merge above a timestamp
TopK and a filtered Parquet scan across four file groups. Read counters
are at the ObjectStore boundary and logical range bytes exclude the
effects of provider coalescing and retries.

## Warm client latency

| Shape | 1M diagnostics off p50/p95 (ms) | 1M diagnostics on p50/p95 (ms) | 20M diagnostics off p50/p95 (ms) |
| --- | ---: | ---: | ---: |
| Trace hit | 18.02 / 21.29 | 18.07 / 20.48 | 16.71 / 19.48 |
| Trace miss | 8.18 / 9.57 | 8.27 / 9.55 | 7.78 / 8.35 |
| Cross-tier trace | 19.55 / 24.33 | 19.48 / 24.56 | 17.48 / 19.15 |
| Recent search | 25.14 / 28.27 | 25.28 / 28.88 | 83.28 / 91.36 |
| Minute aggregation | 28.01 / 32.11 | 28.30 / 32.14 | 247.10 / 260.46 |

Trace hit p50/p95 with diagnostics off by deployment was
18.32/21.61, 18.16/21.17 and 17.73/20.46 ms. Diagnostics moved pooled
trace hit p50 by about +0.05 ms; observed p95 varied across runs.

The 20M run used 50K-row ingest batches and 20 measured samples per mode
after five warmups. The 1M runs used 10K-row batches. Treat the 20M run as
a sizing check because both the compacted layout and sample count can
differ. The larger dataset still showed a clear search and aggregation
latency increase. The 20M run acknowledged 242 audit records with zero
failures, timeouts or saturation before the restart trials.

## Warm server phases

| Trace phase | 1M hit p50 / p95 ms | 1M miss p50 / p95 ms |
| --- | ---: | ---: |
| audit_prepare | 0.00 / 0.01 | 0.01 / 0.01 |
| audit_wait | 4.42 / 5.09 | 5.09 / 6.11 |
| collect_metrics | 0.02 / 0.03 | 0.02 / 0.03 |
| encode_response | 0.02 / 0.04 | 0.01 / 0.03 |
| encode_rows | 0.25 / 0.34 | 0.10 / 0.15 |
| execute | 10.46 / 12.79 | 0.34 / 0.49 |
| histogram | 0.01 / 0.03 | 0.00 / 0.00 |
| logical_plan | 0.25 / 0.40 | 0.22 / 0.34 |
| manifest | 0.81 / 1.05 | 0.80 / 1.11 |
| physical_plan | 0.79 / 1.16 | 0.72 / 1.17 |
| scan_plan | 0.01 / 0.02 | 0.01 / 0.02 |
| session_setup | 0.19 / 0.28 | 0.16 / 0.24 |
| table_setup | 0.01 / 0.02 | 0.01 / 0.01 |


## First request after each process restart

| Shape | Three separate 1M restart samples (ms) | 20M restart (ms) |
| --- | --- | ---: |
| Trace hit | 28.96, 32.34, 30.93 | 26.28 |
| Trace miss | 25.33, 16.82, 20.98 | 33.26 |
| Cross-tier trace | 40.61, 34.55, 40.05 | 46.42 |
| Recent search | 55.70, 42.13, 57.36 | 126.71 |
| Minute aggregation | 47.85, 36.40, 47.28 | 323.82 |
