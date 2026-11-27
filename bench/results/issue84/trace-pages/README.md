# Issue #84 · Compacted Parquet page-size control

**Decision: keep the 20,000-row page limit.** A 4,096-row limit reduced
trace bytes, missed the cross-tier targets at both sizes, and slowed
search at 20M. This was one paired screen per size. Each pair used the
same serially seeded manifest before compaction, the same 64 MiB
compaction target, 128 Ki-row row groups, and two CPUs and 3 GiB each
for the node and disk-backed MinIO. The smaller page cap only changed
compaction output. The arms ran in opposite orders across sizes.
[Experiment source and workflow](https://github.com/explise/verdigris/actions/runs/45980958838)

| Warm metric | 1M 20,000 → 4,096 | 20M 20,000 → 4,096 |
| --- | ---: | ---: |
| Exact hit p50/p95, ms | 17.69/25.59 → 16.20/19.91 | 14.06/20.23 → 14.20/22.40 |
| Exact miss p95, ms | 7.25 → 7.55 | 5.60 → 9.69 |
| Cross-tier trace p95, ms | 35.63 → 30.75 | 27.97 → 30.60 |
| Recent search p95, ms | 27.16 → 27.68 | 98.08 → 107.37 |
| Raw minute aggregation p95, ms | 51.42 → 47.72 | 366.38 → 388.58 |
| Cross-tier returned Parquet bytes, median | 1.74 → 1.43 MB | 1.64 → 1.35 MB |
| Cross-tier Parquet ranges, median | 45 → 48 | 45 → 48 |
| Recent search Parquet ranges, median | 32 → 112 | 253 → 868 |
| Total bucket bytes | 19.54 → 19.84 MB | 367.14 → 377.56 MB |
| Explicit compaction wall time | 1.93 → 1.88 s | 20.67 → 22.84 s |

Latency uses 100 warm requests with diagnostics off per shape and arm.
Read counts and bytes use the separate diagnostics-on cohorts.
All warm and restart result checks passed. Returned rows retained the
same fields and order where required. Each profiled trace matched the
file count and byte budget from its own quote. Warm misses performed
zero Parquet data reads, and all arms ran with zero observed OOMs. The warm audit counters
showed zero failures, acknowledgement timeouts or saturation. The raw archives include
each shape's independent first query after process restart.

The 1M cross-tier median read-byte reduction was 17.5%. At 20M the
reduction was 18.0%, while compacted bucket bytes increased 2.8%. Range
volume alone cannot establish how much delay a cache would remove.
Run #85 only once a control identifies at least 3 ms p50 of removable
read delay. Keep two scan partitions and the current writer settings
while reading out the separate #86 recent-search experiment.

- 1M [20,000 rows/page](1m-page-20000.json.gz) · [4,096 rows/page](1m-page-4096.json.gz)
- 20M [20,000 rows/page](20m-page-20000.json.gz) · [4,096 rows/page](20m-page-4096.json.gz)
