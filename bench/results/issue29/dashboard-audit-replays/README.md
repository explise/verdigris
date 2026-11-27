# Issue #29 · Dashboard and audit replay

Two fresh paired 20M-row replays reversed cache-arm order. Each arm used
a disk-backed MinIO bucket, three independent 60-second steady trials,
one 60-second mutating trial, and an exact post-acknowledgement freshness
check that overlapped compaction. Verdigris had a 2 GiB limit, four total
query permits including two reserved for traces, and 60 seconds of
retired-object grace. Scheduled alerts kept a 1,000 ms admission wait.
[Workflow and source](https://github.com/explise/verdigris/actions/runs/45992227792)

| Mutating measurement | Trial 1 off / on | Trial 2 off / on |
| --- | ---: | ---: |
| Successful traces | 660 / 660 each | 660 / 660 each |
| Trace p95 (ms) | 80.27 / 76.50 | 64.27 / 68.26 |
| Heavy query goodput per second | 5.67 / 6.03 | 7.68 / 7.77 |
| Completed dashboards | 7 / 15 | 7 / 15 |
| Dashboard reads after acknowledgement | 3 / 9 | 4 / 9 |
| Audit timeouts, failures and saturation | 0 / 0 | 0 / 0 |

All four arms passed the steady, trace, heavy progress, health and
freshness gates. No slow audit batches were logged in the mutating
windows. The exact checks after compaction preserved the original
dashboard semantics. A previous replay had 17 audit acknowledgement
timeouts in the cache arm. The new runs did not reproduce them, so the
logs cannot assign a cause. Retain the current timeout and report any
recurrence separately.

The results support review of the opt-in profile in #91. The benchmark
includes additional timing instrumentation and is separate from the
deployment patch. Default settings and the real-S3 calibration gate
remain independent.

[Trial 1 raw, cache off first](trial-1.tar.gz) ·
[Trial 2 raw, cache on first](trial-2.tar.gz)
