# Issue #24 · First manifest-backed serving pilot

The experiment compares the production writer, explicit compaction and
serving reader on fresh raw and summary deployments at each size. Each
used the same frozen corpus and serial seed schedule, with Verdigris and
disk-backed MinIO at 2 CPU / 3 GiB each. All query outputs and five
independent restart shapes passed the exact result checks. The treatment
kept the same full raw quote and required no extra summary object GET.
[Workflow and correctness tests](https://github.com/explise/verdigris/actions/runs/45991634592)

| Corpus | Raw / summary aggregate p95 | Added active manifest bytes | Raw / summary ingest rows/s | Raw / summary compaction |
| --- | ---: | ---: | ---: | ---: |
| 1M | 45.81 / 5.53 ms | 13,325 (0.069%) | 182,982 / 181,159 | 1.78 / 1.86 s |
| 20M | 445.13 / 5.39 ms | 14,615 (0.004%) | 249,236 / 246,900 | 24.75 / 25.71 s |

Percentages use the active raw Parquet byte quote. These are foreground
seed rates and explicit compaction wall times. The short seed rate does
not establish the sustained ingest target. Each aggregate timing arm has
100 measured requests per profiling mode. The summary read no Parquet
data in profiled aggregate requests. The warm endpoint metrics ended at
1,068 acknowledged records in every arm with zero timeouts, failures or
saturation. Both aggregate latency and byte budgets passed in the first
pair at each size.

| Other p95 workload | 1M raw / summary (ms) | 20M raw / summary (ms) |
| --- | ---: | ---: |
| Exact trace hit | 21.15 / 24.71 | 25.62 / 22.01 |
| Exact trace miss | 6.33 / 6.89 | 6.58 / 6.71 |
| Cross-tier trace | 32.10 / 33.68 | 36.93 / 32.52 |
| Recent search | 31.48 / 32.75 | 124.20 / 126.74 |

Keep the feature off while the additional pairs verify overhead and
repeatability. The 1M trace regression and the 20M trace improvement
both come from one pair. The reply keeps the original raw quote for the
fallback, including tier and byte bounds.

[1M raw evidence](1000000.json.gz) · [20M raw evidence](20000000.json.gz)
