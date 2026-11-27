# Issue #63 · Matched 20M log-query comparison

For the first trial, one fresh deployment per engine ran sequentially on the same four-CPU
runner. Each complete deployment had 4 CPU and 6 GiB. Verdigris and
disk-backed MinIO each had 2 CPU and 3 GiB. All engines used the same
timestamp, data, trace-ID schedule and durable response policy for
Verdigris. Storage and acknowledgement policies differ across engines.
Each shape has 100 measured warm requests and 10 warmups. The harness
checked every row, required order and aggregate bucket. Verdigris
recorded all 550 expected audit acknowledgements in the warm run with
zero timeouts, failures or saturation. [Workflow](https://github.com/explise/verdigris/actions/runs/45984927081)

## Three fresh trials

Every trial used the same 20M-row corpus, 4 CPU / 6 GiB per deployment,
100 requests per workload and independent restarts. Replications rotated
engine order and recorded request order. The first runner used an AWS
kernel; the replication runners used an Azure kernel. Each trial kept
all three engines on its own runner. All 1,650 expected Verdigris warm
audit acknowledgements persisted without a timeout, failure or
saturation. [Replication workflow](https://github.com/explise/verdigris/actions/runs/45989136210)

| Warm p95 (ms) | Verdigris + MinIO | ClickHouse | VictoriaLogs |
| --- | ---: | ---: | ---: |
| Exact trace hit | 18.68–26.79 | 119.06–199.66 | 4.61–7.22 |
| Exact trace miss | 4.48–7.72 | 1.95–2.63 | 3.85–5.74 |
| Cross-tier trace | 28.62–36.19 | 6.11–8.64 | 5.57–7.77 |
| Recent service search | 104.47–133.38 | 3.99–6.90 | 30.73–39.96 |
| Raw minute aggregation | 388.72–485.53 | 166.00–233.60 | 142.76–228.41 |

The ClickHouse exact-hit p95 includes first visits to six trace IDs.
After ten warmups, the first six measured requests are the first visits
to IDs 11–16. In both replications these were the only requests above
100 ms. The remaining 94 repeat-ID requests had 3.87 and 5.07 ms p95.
The first trial has only sorted timings. Its six slow samples are
consistent with that pattern. Keep the mixed first-visit/repeat p95 and
the repeat-ID result visible. Use a full 16-ID warmup in future
repeat-only comparisons.

Verdigris exact-hit p50 was 13.11–18.14 ms, and first exact hit after
restart was 26.15–43.32 ms. The 20M warm trace-hit target of 20 ms p95
held in one trial. Recent search and raw aggregation missed their 75 ms
and 220 ms p95 targets in all three. Verdigris's total retained bytes
were 2.281 GB, 376.60 MB and 375.54 MB at the observed snapshots, with
367.68–368.01 MB of bucket data. The first trial's MinIO deletion
backlog accounts for the bulk of this footprint variation.

| Replication | Engine order | Raw reports |
| --- | --- | --- |
| 2 | ClickHouse, VictoriaLogs, Verdigris | [Verdigris](trial-2-vdg.json.gz) · [ClickHouse](trial-2-clickhouse.json.gz) · [VictoriaLogs](trial-2-victorialogs.json.gz) |
| 3 | VictoriaLogs, Verdigris, ClickHouse | [Verdigris](trial-3-vdg.json.gz) · [ClickHouse](trial-3-clickhouse.json.gz) · [VictoriaLogs](trial-3-victorialogs.json.gz) |

## First trial detail

| Workload | Verdigris + MinIO p50/p95 | ClickHouse p50/p95 | VictoriaLogs p50/p95 |
| --- | ---: | ---: | ---: |
| Exact trace hit | 13.11/18.68 | 2.96/119.06 | 3.92/4.61 |
| Exact trace miss | 4.34/4.48 | 1.74/1.95 | 3.39/3.85 |
| Cross-tier trace | 19.60/28.62 | 4.63/6.11 | 4.57/5.57 |
| Recent search | 92.05/104.47 | 3.80/3.99 | 27.49/30.73 |
| Raw minute aggregation | 367.05/388.72 | 158.15/166.00 | 138.00/142.76 |

Warm latencies are in milliseconds. ClickHouse exact hit had 94 samples
at or below 4.44 ms and six between 119.06 and 154.29 ms. The tail is
real in this run. The replications above retain request order and show when the slow samples occur.

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
