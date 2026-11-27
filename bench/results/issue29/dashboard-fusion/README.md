# Issue #29 · Fused dashboard scan

The four-vCPU runner compared two existing dashboard scans with one
`GROUPING SETS` scan over the same disk-backed 20M-row fixture shape and
six-class open-loop workload. Both changes used two reserved permits
out of four and a 250 ms dashboard wait. The scheduled alert wait held at
50 ms. Both arms ran three 60-second trials. Incomplete dashboard payloads
counted as errors. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45969736322)

| Metric | Separate | Fused |
| --- | ---: | ---: |
| Complete refreshes | 8/90 | 10/90 |
| Refresh p95 by trial | 981–1,008 ms | 1,037–1,068 ms |
| Refresh execution p50 by trial | 738–750 ms | 779–814 ms |
| Trace success | 100% | 100% |
| Trace p95 by trial | 68–75 ms | 69–72 ms |
| Heavy goodput, mean | 7.82 req/s | 7.78 req/s |
| Scheduled alert evaluations | 0/36 | 0/36 |

Both arms passed the trace and heavy-progress gates, with zero OOM kills,
restarts, audit failures, audit timeouts, or saturation. The fused arm
recorded zero fallback warnings. Most refreshes were shed at admission in
both arms, and both execution and response latency rose for the small
number that completed. This is a no-go for promoting fusion. Check the
separate wait experiment before changing query execution again.

[Control raw results](fused-false.json.gz) · [Treatment raw results](fused-true.json.gz)
