# Equivalent log-query comparison

Same deterministic corpus, same 8 CPU / 6 GiB total budget per complete deployment, one system at a time. Verdigris's budget includes its MinIO object store (4 CPU / 3 GiB + 4 CPU / 3 GiB).

| System | Retained | Container memory after queries |
|---|---:|---:|
| **Verdigris + MinIO** | 174.2 MiB | vdgbench-vdg=114.1MiB / 3GiB,vdgbench-minio=342MiB / 3GiB |
| **ClickHouse** | 124.7 MiB | chbench=372.8MiB / 6GiB |
| **VictoriaLogs** | 19.9 MiB | vlbench=138.1MiB / 6GiB |

## Ingest (reported separately from query latency)

| System | Acked rows/s | MiB/s | Visibility wait |
|---|---:|---:|---:|
| Verdigris + MinIO | 293,576 | 73.2 | 0.0s |
| ClickHouse | 676,114 | 182.0 | 0.0s |
| VictoriaLogs | 843,475 | 227.1 | 1.0s |

## Query latency (ms, after merges/compaction settle)

### Recent service search (`checkout` + `ERROR`, newest 200)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 23.0 | 28.3 | 32.6 | 32.6 | 50 |
| ClickHouse | 4.4 | 6.0 | 6.3 | 6.3 | 50 |
| VictoriaLogs | 8.3 | 10.0 | 11.1 | 11.1 | 50 |

### Trace lookup (rotating exact `trace_id` hits)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 17.6 | 20.5 | 21.2 | 21.2 | 50 |
| ClickHouse | 2.9 | 7.2 | 10.5 | 10.5 | 50 |
| VictoriaLogs | 1.3 | 2.6 | 2.6 | 2.6 | 50 |

### Trace lookup (missing `trace_id`)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 7.1 | 7.9 | 8.1 | 8.1 | 50 |
| ClickHouse | 1.5 | 2.1 | 2.3 | 2.3 | 50 |
| VictoriaLogs | 0.4 | 0.5 | 0.6 | 0.6 | 50 |

### Trace lookup (one id in all three tiers)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 17.5 | 19.5 | 21.3 | 21.3 | 50 |
| ClickHouse | 4.2 | 4.9 | 5.6 | 5.6 | 50 |
| VictoriaLogs | 2.2 | 3.5 | 4.5 | 4.5 | 50 |

### 1-minute aggregation (total + errors)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 24.8 | 27.0 | 27.5 | 27.5 | 50 |
| ClickHouse | 7.0 | 7.9 | 17.2 | 17.2 | 50 |
| VictoriaLogs | 5.5 | 6.7 | 7.8 | 7.8 | 50 |

## Equivalence gate

| System | Recent count | Search rows | Trace rows | Cross-tier | Missing | Buckets | Total | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 5,883 | 200 | 1 | 3 | 0 | 50 | 1,000,000 | 58,824 |
| ClickHouse | 5,883 | 200 | 1 | 3 | 0 | 50 | 1,000,000 | 58,824 |
| VictoriaLogs | 5,883 | 200 | 1 | 3 | 0 | 50 | 1,000,000 | 58,824 |

_Corpus and query definitions are in `bench/compare/logs/compare.py`; resource limits and image digests are in `bench/compare/logs/run.sh`._

