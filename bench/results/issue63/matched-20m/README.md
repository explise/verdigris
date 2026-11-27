# Issue #63 · First matched 20M log-query comparison

One fresh deployment per engine ran sequentially on the same four-CPU
runner. Each complete deployment had 4 CPU and 6 GiB. Verdigris and
disk-backed MinIO each had 2 CPU and 3 GiB. All engines used the same
timestamp, data, trace-ID schedule and durable response policy for
Verdigris. Storage and acknowledgement policies differ across engines.
Each shape has 100 measured warm requests and 10 warmups. The harness
checked every row, required order and aggregate bucket. Verdigris
recorded all 550 expected audit acknowledgements in the warm run with
zero timeouts, failures or saturation. [Workflow](https://github.com/explise/verdigris/actions/runs/45984927081)

| Workload | Verdigris + MinIO p50/p95 | ClickHouse p50/p95 | VictoriaLogs p50/p95 |
| --- | ---: | ---: | ---: |
| Exact trace hit | 13.11/18.68 | 2.96/119.06 | 3.92/4.61 |
| Exact trace miss | 4.34/4.48 | 1.74/1.95 | 3.39/3.85 |
| Cross-tier trace | 19.60/28.62 | 4.63/6.11 | 4.57/5.57 |
| Recent search | 92.05/104.47 | 3.80/3.99 | 27.49/30.73 |
| Raw minute aggregation | 367.05/388.72 | 158.15/166.00 | 138.00/142.76 |

Warm latencies are in milliseconds. ClickHouse exact hit had 94 samples
at or below 4.44 ms and six between 119.06 and 154.29 ms. The tail is
real in this run. Repeat on fresh deployments before applying it to a
target or claiming a durable comparative advantage.

| Engine | First exact hit | First exact miss | First cross-tier | First recent | First aggregate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Verdigris + MinIO | 26.15 | 16.07 | 35.02 | 107.11 | 383.63 |
| ClickHouse | 176.27 | 160.32 | 123.50 | 9.11 | 132.36 |
| VictoriaLogs | 10.88 | 10.10 | 18.30 | 43.13 | 157.05 |

Each first query follows its own process restart. The disk and OS
cache remain. First-query measurements are single samples per shape.

| Engine | Short ingest ack rate | Wait for verified visibility | Retained on-disk bytes |
| --- | ---: | ---: | ---: |
| Verdigris + MinIO | 228,042 rows/s | 0.701 s | 2.281 GB |
| ClickHouse | 1,041,241 rows/s | 0.003 s | 2.501 GB |
| VictoriaLogs | 526,163 rows/s | 1.008 s | 424.07 MB |

Verdigris had 368.01 MB in its bucket. A separate snapshot measured
1.914 GB of MinIO internal bytes, including 1.914 GB under
`.minio.sys/tmp/.trash`. The bucket and process-volume snapshots use
different sampling times. Keep the entire MinIO volume in the
deployment footprint. These short ingest measurements use the
benchmark's ingest and visibility policies. The ten-minute ingest
gate has its own driver and recovery checks.

[Verdigris raw](vdg.json.gz) · [ClickHouse raw](clickhouse.json.gz) ·
[VictoriaLogs raw](victorialogs.json.gz) · [Repeated 1M cohort](../four-cpu/README.md)
