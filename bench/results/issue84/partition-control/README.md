# Issue #84 · Cross-tier trace partition control

The paired test kept Verdigris and disk-backed MinIO at 2 CPU and 3 GiB
each. Both arms shared the same settled fixture for each size and
restarted Verdigris before measuring. The arm order was two partitions
first at 1M and one partition first at 20M. Every arm has 100 warm
requests per workload and diagnostic mode.
[GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45976734182)

| Workload | 1M one partition p95 | 1M two partitions p95 | 20M one partition p95 | 20M two partitions p95 |
| --- | ---: | ---: | ---: | ---: |
| Exact trace hit | 20.42 ms | 18.22 ms | 24.02 ms | 21.58 ms |
| Exact trace miss | 4.53 ms | 4.34 ms | 4.41 ms | 4.91 ms |
| Cross-tier trace | 35.69 ms | 24.89 ms | 35.91 ms | 25.82 ms |
| Recent search | 24.26 ms | 27.72 ms | 111.37 ms | 108.95 ms |
| Minute aggregation | 47.92 ms | 34.36 ms | 541.60 ms | 381.17 ms |

The paired arms matched the scoped files, planned bytes, and complete
results for all requests. Both returned all three cross-tier records.
Warm trace misses read zero Parquet data bytes. All arms recorded zero
audit errors, saturation, timeouts, or OOM kills.

Cross-tier median read bytes and ranges stayed at 1.89 MB and 46 at 1M,
and 1.63 MB and 45 at 20M. Median peak range reads in flight rose from
one to two. Cross-tier execution p50 fell from 21.20 to 17.65 ms at 1M
and from 20.20 to 14.12 ms at 20M. Two partitions are already the default
for this resource budget. The 22/25 ms cross-tier targets remain open.
Keep the default parallelism, and track the remaining read and decode
work before choosing another trace optimization.

[1M raw samples](one-million.json.gz) · [20M raw samples](twenty-million.json.gz)
