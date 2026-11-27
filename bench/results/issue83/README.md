# Issue #83 · Trace-query profiling on the integrated stack

## Finding

Three fresh 1M-row MinIO deployments were measured on the corrected integrated
head `cfe5146` (image `sha256:861a0f09…`). With durable audit enabled and
diagnostics off, trace-hit latency was **18.7–19.7 ms p50 / 22.2–25.7 ms p95**.
The request phases were stable:

| Phase | 1M trace-hit p50 / p95 (three runs) |
|---|---:|
| manifest | 0.70–0.76 / 0.97–1.15 ms |
| scan plan | 0.012–0.014 / 0.023–0.026 ms |
| admission | 0.003 / 0.005–0.007 ms |
| execute | 11.0–11.9 / 13.8–16.1 ms |
| audit wait | 4.44–4.59 / 4.93–5.44 ms |
| encode rows | 0.25–0.27 / 0.45–0.50 ms |

The profiler records each ObjectStore read interval. The sum counts overlapping
requests multiple times. The interval union (the historical
`read_critical_wait_us` field) measures elapsed time with any read in flight.
Execution may make progress during those intervals, so their duration alone
cannot establish how much latency a cache could remove.

Trace-hit read-in-flight time was **5.25 / 5.42 / 5.03 ms p50** across the three
1M runs (p95 7.06–7.75 ms), and **5.33 ms p50 / 6.46 ms p95** at 20M. A 1M
trace hit reads a median 1.15 MiB of logical Parquet. A miss reads no data.

## Warm client latency

| Shape | 1M diagnostics off p50/p95 (ms, three runs) | 1M diagnostics on p50/p95 (ms, three runs) | 20M diagnostics off p50/p95 (ms) |
| --- | ---: | ---: | ---: |
| Trace hit | 19.22/24.02, 19.68/25.71, 18.71/22.19 | 19.23/24.73, 19.59/26.31, 18.66/21.29 | 18.43/20.72 |
| Trace miss | 8.31/9.98, 8.66/16.42, 8.47/9.88 | 8.63/10.17, 8.79/16.66, 8.71/10.29 | 8.20/9.07 |
| Cross-tier trace | 20.56/25.03, 19.46/21.97, 20.16/23.19 | 20.72/27.44, 19.80/21.94, 20.13/22.57 | 20.17/21.59 |
| Recent search | 27.71/31.04, 27.54/29.82, 26.46/28.40 | 28.03/30.23, 27.10/30.51, 26.84/29.36 | 124.32/129.82 |
| Minute aggregation | 30.52/34.54, 29.63/33.01, 28.29/31.79 | 30.77/34.61, 29.71/33.19, 28.09/30.60 | 247.48/258.75 |

All three runs acknowledged 1,067 audit records with zero failures, zero
timeouts and zero saturation. The 20M run acknowledged 242 with the same zero
failure/timeout/saturation counts.

## First request after each process restart

Each shape gets its own restart; MinIO and the OS cache stay warm.

| Shape | Three 1M restart samples (ms) | 20M restart (ms) |
| --- | ---: | ---: |
| Trace hit | 38.61, 29.17, 28.40 | 25.36 |
| Trace miss | 17.88, 20.53, 21.46 | 22.18 |
| Cross-tier trace | 42.18, 37.70, 44.69 | 48.49 |
| Recent search | 41.42, 39.97, 58.68 | 201.03 |
| Minute aggregation | 55.67, 44.83, 42.70 | 319.59 |

The exact-hit restart target (≤40 ms) is met in every run.

## Decision evidence

Keep #85 parked. The #84/#85 gate requires evidence that at least **3 ms p50**
of read wait can be removed from the critical path. The in-flight intervals are
insufficient to meet that gate. Even a uniform 3 ms reduction from the warm
client latencies in the table would leave all three diagnostics-off p50 values
above 15 ms. Prioritize the #29 mixed-load blocker; #86 is the next workload in
#82 after the integrated baseline and trace decision.

## Reproduction

```sh
docker build -f bench/Dockerfile.vdg -t verdigris-bench:issue83 .
for run in 1 2 3; do
  VDG_IMAGE=verdigris-bench:issue83 OUT=/tmp/vdg-83-$run \
    bench/compare/logs/run-profile.sh
done
VDG_IMAGE=verdigris-bench:issue83 OUT=/tmp/vdg-83-20m \
  ROWS=20000000 BATCH_ROWS=50000 ITERATIONS=20 WARMUP=5 \
  bench/compare/logs/run-profile.sh
```

Each deployment is a fresh MinIO volume at 4 CPU / 3 GiB for Verdigris and
4 CPU / 3 GiB for MinIO. Warm runs use 10 warmups and 100 samples per mode, with
diagnostics on/off alternating order. The 20M run is a sizing run with 20
samples per mode. Raw samples, phase timings, read counters, machine/config
metadata, restart files and the representative physical plan are in
[run 1](release-1.json.gz), [run 2](release-2.json.gz), [run 3](release-3.json.gz)
and [20M](release-20m.json.gz).

`read_wait_critical_us` is the union of request-local ObjectStore read intervals;
`read_wait_sum_us` is the unmerged sum and is diagnostic only. Read counters are
at the logical ObjectStore boundary and exclude provider coalescing/retries.
