# Issue #21 · Sustained compaction-window control

Both arms used one fixed 600-second, 220,000 offered rows/s workload,
individual durable commits and the same host. Verdigris had 3 CPU /
4 GiB and disk-backed MinIO had 1 CPU / 2 GiB. The second arm held
compaction during load and used the same bounded offline drain.
[Workflow](https://github.com/explise/verdigris/actions/runs/45990089910)

| Measurement | Compaction during load | Compaction after load |
| --- | ---: | ---: |
| Durable rows/s in the load window | 29,883 | 35,883 |
| Durable rows/s including drain | 28,997 | 34,441 |
| Acknowledged rows recovered exactly | 18,080,000 | 21,690,000 |
| Compaction passes during load | 87 | 0 |
| Offline drain | 13 s | 22 s |
| Peak sampled active files | 3,590 | 6,459 |
| Parquet write amplification | 13.81× | 1.56× |
| Sampled offers acknowledged | 78/600 | 96/600 |
| Sampled offers observed within one second | 8/600 | 9/600 |
| Sampled requests observed within five seconds | 26/600 | 41/600 |

The **20.1%** timed throughput increase missed the **25%** pre-set
advance threshold. Both drains finished and all acknowledged batches
recovered exactly across restart. Write amplification measures total
ingest and compaction Parquet PUT bytes divided by ingest Parquet PUT
bytes, including the offline drain. It excludes manifest, sidecar and
store-internal writes.

Visibility p95 could not be resolved because too few sampled rows
were visible by the five-second deadline. Most sampled offers hit the
in-flight cap. The 1-second target also fails within the acknowledged
sample population. The 8+4 CPU reference target remains open.

Keep the existing scheduling and commit settings. A future bounded
compaction candidate should identify repeated input/output rewrites and
reduce them during load while reporting backlog and visibility
separately from acknowledgement.

[Decision and counters](decision.json) · [Raw logs and recovery evidence](trial.tar.gz)
