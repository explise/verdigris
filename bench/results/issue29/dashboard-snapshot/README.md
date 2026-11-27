# Issue #29 · Reuse complete dashboard aggregates

The two arms each ran three 60-second fixed-rate queries on a fresh
20M-row disk-backed MinIO fixture, then one 60-second trial with
acknowledged writes. They used four query permits, two reserved for cheap
work, a 250 ms dashboard wait, and a 1,000 ms scheduled-alert wait. A
dashboard hit read the live manifest first and reused the service counts
and histogram only on the same immutable active snapshot. Audit,
admission and compaction counters came from the current response.
[GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45975203005)

| Metric | Control | Snapshot reuse |
| --- | ---: | ---: |
| Steady complete dashboards | 7/90 | 90/90 |
| Steady dashboard p95 across trials | 804–1,044 ms | 14.04–16.81 ms |
| Steady trace success | 100% | 100% |
| Steady trace p95 across trials | 68.20–72.73 ms | 66.18–68.45 ms |
| Steady heavy goodput | 7.76 req/s | 7.88 req/s |
| Steady scheduled rule evaluations | 33/33 | 33/33 |
| Mutating complete dashboards | 1/30 | 14/30 |
| Mutating incomplete dashboard responses | 0 | 1 |
| Mutating trace p95 | 71.52 ms | 73.04 ms |
| Mutating heavy goodput | 7.35 req/s | 7.15 req/s |
| Visible acknowledged write probes | 5/5 | 5/5 |

The steady arms passed the trace, heavy-progress and memory gates.
Both mutating arms completed all traces and nine scheduled evaluations.
Both arms had zero audit errors, audit timeouts, audit saturation, OOM
kills, or process restarts. The treatment recorded 92 aggregate reuses by
the steady metrics sample, including fixture warmup and metadata probes.
It recorded 11 additional reuses between the two metrics samples.

The performance job succeeded. The original freshness test accidentally
routed some records to cold storage, which is ineligible for reuse. A
separate corrected test passed after exercising acknowledged ingest,
compaction invalidation, and explicit cold and legacy exclusions.
[Corrected freshness test](https://github.com/explise/verdigris/actions/runs/45976083168)

The measured result supports an opt-in candidate for unchanged snapshots.
With writes, dashboard refreshes still shed under pressure. The
mutating fixture also used a zero-second retired-file grace period.
Record query errors in a follow-up before attributing the incomplete
response and confirm exact refreshed results while compaction runs.

[Control raw results](cache-false.json.gz) · [Treatment raw results](cache-true.json.gz)
