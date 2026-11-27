# Issue #83 · Four-CPU disk-backed 1M profiles

Three fresh 1M-row Verdigris deployments used 2 CPU and 3 GiB each for
the query node and disk-backed MinIO. Each shape has 100 warm samples per
diagnostic mode and an independent first query after restarting Verdigris.
The object store and OS retain their caches. The comparator used the same
budget and corpus timestamp on a separate run. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45970043896)

| Shape | Warm p95 off (ms) | Warm p95 on (ms) | First query (ms) |
| --- | ---: | ---: | ---: |
| Trace hit | 21.46–23.15 | 21.14–24.27 | 26.07–27.04 |
| Trace miss | 6.20–8.01 | 6.21–9.33 | 15.23–15.91 |
| Cross-tier trace | 33.13–33.83 | 31.47–36.88 | 37.01–39.58 |
| Recent search | 24.66–30.32 | 25.76–27.76 | 39.67–40.81 |
| Minute aggregation | 46.85–57.05 | 44.86–52.36 | 52.00–60.70 |

Profiles include both diagnostic modes, execution plans, cache and read
counters, and the individual requests. All three trials produced 1,068 audit
acknowledgements with zero failures, timeouts, or saturation. Every profiled
warm trace miss read zero Parquet data bytes.

Execution was the largest measured warm hit phase at 11.45–11.87 ms p50.
The union of in-flight object reads was 5.83–6.10 ms p50 and can overlap
decode and operators. The samples do not establish how much latency a cache
could remove, so keep #85 parked. The separate 20M sizing run measured
100.08 ms p95 for recent search against a 75 ms gate. Proceed with the #86
experiment using the same rows, ordering, and cost contract.

[Trial 1](trial-1.json.gz) · [Trial 2](trial-2.json.gz) · [Trial 3](trial-3.json.gz)
