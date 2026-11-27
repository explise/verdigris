# Issue #29 · Scheduled alert admission wait

The same image and four-vCPU runner compared 50 ms and 1,000 ms waits for
scheduled alerts. Both treatments used two reserved permits and four total
permits. Interactive alert validation retained its 50 ms wait. Each treatment
ran the same six-class workload for three 60-second trials against a fresh
20M-row disk-backed MinIO fixture. The recent-search window was relative to
each fixture's seed start. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45967019331)

| Metric | 50 ms scheduled wait | 1,000 ms scheduled wait |
| --- | ---: | ---: |
| Scheduled evaluations completed | 0/36 | 33/33 |
| Scheduled evaluations shed | 36 | 0 |
| Trace success | 100% | 100% |
| Successful trace p95 | 69.71–72.79 ms | 67.23–70.91 ms |
| Heavy goodput, mean | 7.59 req/s | 7.61 req/s |
| Dashboard refresh HTTP 200 | 10/90 | 14/90 |

Both arms kept the measured trace and heavy-progress gates and recorded
zero OOM kills, restarts, audit failures, audit timeouts or saturation.
Scheduled evaluation progress recovered with the longer wait. The scheduler
sleeps between passes, so evaluation duration can change how many starts
land inside a fixed window. Report completed counts as well as percentages.
Dashboard refresh progress remains open. The runtime change stays on the
experiment branch pending combined validation.

[Control raw results](wait-50.json.gz) · [Treatment raw results](wait-1000.json.gz)
