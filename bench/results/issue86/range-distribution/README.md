# Issue #86 · Parquet range distribution

The paired run used one settled disk-backed MinIO fixture per size, two
CPU and 3 GiB per service, the frozen corpus timestamp, and one Verdigris
restart per arm. The arm order was control first at 1M and treatment first
at 20M. Each arm measured 100 warm requests per shape and diagnostic mode.
The comparison keeps the SQL and complete approved scan scope fixed. The
treatment distributes disjoint ranges from large immutable Parquet files
across the two existing scan partitions.
[GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45974432672)

| Workload | 1M control p95 | 1M treatment p95 | 20M control p95 | 20M treatment p95 |
| --- | ---: | ---: | ---: | ---: |
| Exact trace hit | 25.13 ms | 26.32 ms | 28.20 ms | 23.64 ms |
| Exact trace miss | 6.65 ms | 8.22 ms | 7.43 ms | 7.31 ms |
| Cross-tier trace | 39.33 ms | 31.87 ms | 36.14 ms | 41.16 ms |
| Recent search | 36.56 ms | 35.62 ms | 133.28 ms | 128.79 ms |
| Minute aggregation | 43.97 ms | 52.38 ms | 438.55 ms | 436.88 ms |

At 20M, recent-search p50 improved from 119.51 to 104.29 ms. Median peak
concurrent range reads rose from one to two, range count from 253 to 290,
and returned bytes from 5.35 to 7.21 MB. The treatment used two ranges
over the hot file in the physical plan. Recent search missed 25/75 ms p95
at both sizes. Cross-tier latency at 20M and aggregate latency at 1M
also moved in the wrong direction. Keep this policy experimental.

Both arms returned all 200 expected rows in order and matched their
identical estimate, approved file/byte scope, and histogram/activity
responses. There were no audit failures, timeouts, saturations, or OOMs.
The paired fixtures establish the direction for this run. Acceptance
requires three fresh runs per size.

[1M raw samples](one-million.json.gz) · [20M raw samples](twenty-million.json.gz)
