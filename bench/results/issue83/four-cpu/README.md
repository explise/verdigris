# Issue #83 · Four-CPU disk-backed 20M sizing

This is one fresh 20M-row Verdigris deployment with 2 CPU / 3 GiB
each for the query node and disk-backed MinIO. The corpus timestamp
matches the 1M cohort, and the deterministic generator expands it to
20M rows. Each warm shape has 100 samples in each diagnostics mode.
Each first query ran after its own Verdigris restart, with cache state
retained at MinIO and the OS. See the raw source, host, config and
samples for reproducibility. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45963228656).

| Shape | Warm p50 ms | Warm p95 ms | First query ms |
| --- | ---: | ---: | ---: |
| trace_hit | 12.60 | 18.56 | 24.13 |
| trace_miss | 4.40 | 4.67 | 16.27 |
| trace_cross_tier | 18.62 | 28.93 | 35.87 |
| recent_search | 87.91 | 100.08 | 124.71 |
| minute_aggregation | 382.58 | 398.23 | 402.93 |

Warm latencies were measured with diagnostics off and durable audit in
the response path. There were 1,068 audit acknowledgements and zero
failures, timeouts or saturation events at the end of warm profiling.
Warm trace misses performed zero Parquet data reads. At this budget the
20M warm trace hit met 15/20 ms p50/p95 and the trace miss met 10 ms p95.
The cross-tier trace, recent search and raw aggregation missed their
25/75/220 ms p95 targets. The 20M deployment is a sizing run.

In the diagnostics-on paired control, SQL and search syntax returned
the same ordered 200 rows and matched their individual cost quotes.
Search syntax selected 1 of 7 files and reduced p95 from **103.85 ms**
to **87.02 ms** over 20 samples per form. Returned read bytes rose
from **5.33 MB to 7.19 MB**. This supports an experiment on file selection
and execution work, with the 75 ms p95 target still open.

[Raw measurements, config and physical plans](sizing.json.gz).
