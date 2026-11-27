# Issue #29 · Reserve one or two query permits

One release image ran both treatments on the same four-vCPU Ubuntu runner.
Each arm had a fresh disk-backed MinIO volume, a 20M-row corpus, and three
60-second trials with the identical six-class arrival schedule. Each corpus
timestamp was based on that arm's seed start, so the recent search window was
relative to the arm. Both arms kept four total query permits, durable audit,
and the same resource caps and timeouts. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45963659851)

| Metric | 1 reserved permit | 2 reserved permits |
| --- | ---: | ---: |
| Trace success across trials | 63.18–67.73% | 100% |
| Successful trace p95 | 1,060.91–1,061.20 ms | 70.05–73.10 ms |
| Trace admission p95 | 996.16–996.60 ms | 0.14 ms |
| Heavy goodput, mean | 8.41 req/s | 7.83 req/s |
| Recent search success | 1,626/1,980 | 1,980/1,980 |
| Scheduled alerts completed | 0/36 | 0/32 |
| Dashboard refresh HTTP 200 | 17/90 | 7/90 |

Both arms recorded zero OOM kills, container restarts, audit failures,
timeouts, or queue saturation. Heavy goodput fell 6.8% with two reserved
permits. That treatment passed the trace success and latency target with heavy
progress, and is a candidate for the next experiment. Dashboard progress
regressed and all scheduled alerts were shed in both treatments. Keep the
configuration on the experiment branch pending background-work verification.

The full evidence includes timings for every arrival, exact resource limits,
and teardown and audit counters. [One reserved permit](cheap-1.json.gz) ·
[Two reserved permits](cheap-2.json.gz)
