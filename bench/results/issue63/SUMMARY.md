# Equivalent log-query comparison

Same deterministic corpus, same 8 CPU / 6 GiB total budget per complete deployment, one system at a time. Verdigris's budget includes its MinIO object store (4 CPU / 3 GiB + 4 CPU / 3 GiB).

| System | Retained | Container memory after queries |
|---|---:|---:|
| **Verdigris + MinIO** | 18.8 MiB | vdgbench-20261127070459-228124-vdg=118.1MiB / 3GiB,vdgbench-20261127070459-228124-minio=294.8MiB / 3GiB |
| **ClickHouse** | 125.4 MiB | chbench-20261127070459-228124=906.6MiB / 6GiB |
| **VictoriaLogs** | 20.0 MiB | vlbench-20261127070459-228124=144.6MiB / 6GiB |

## Ingest (reported separately from query latency)

| System | Acked rows/s | MiB/s | Visibility wait |
|---|---:|---:|---:|
| Verdigris + MinIO | 274,956 | 68.5 | 0.0s |
| ClickHouse | 664,311 | 178.8 | 0.0s |
| VictoriaLogs | 881,013 | 237.2 | 1.0s |

## Query latency (ms, after merges/compaction settle)

### Recent service search (`checkout` + `ERROR`, newest 200)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 25.7 | 28.4 | 29.7 | 30.1 | 100 |
| ClickHouse | 5.0 | 5.6 | 5.9 | 6.0 | 100 |
| VictoriaLogs | 21.7 | 24.4 | 25.1 | 26.3 | 100 |

### Trace lookup (rotating exact `trace_id` hits)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 20.4 | 23.9 | 25.9 | 25.9 | 100 |
| ClickHouse | 3.4 | 7.3 | 8.6 | 9.0 | 100 |
| VictoriaLogs | 2.3 | 3.8 | 5.5 | 6.3 | 100 |

### Trace lookup (missing `trace_id`)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 8.6 | 9.5 | 11.0 | 12.3 | 100 |
| ClickHouse | 1.7 | 2.5 | 3.1 | 4.4 | 100 |
| VictoriaLogs | 0.5 | 0.7 | 0.8 | 0.9 | 100 |

### Trace lookup (one id in all three tiers)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 19.8 | 23.1 | 24.4 | 24.7 | 100 |
| ClickHouse | 4.7 | 5.6 | 6.4 | 6.6 | 100 |
| VictoriaLogs | 2.6 | 3.8 | 4.3 | 4.9 | 100 |

### 1-minute aggregation (total + errors)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 27.8 | 31.1 | 33.0 | 33.9 | 100 |
| ClickHouse | 7.4 | 8.9 | 9.6 | 9.7 | 100 |
| VictoriaLogs | 12.3 | 13.8 | 14.3 | 15.0 | 100 |

## Equivalence gate

| System | Recent count | Search rows | Trace rows | Cross-tier | Missing | Buckets | Total | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 5,883 | 200 | 1 | 3 | 0 | 50 | 1,000,000 | 58,824 |
| ClickHouse | 5,883 | 200 | 1 | 3 | 0 | 50 | 1,000,000 | 58,824 |
| VictoriaLogs | 5,883 | 200 | 1 | 3 | 0 | 50 | 1,000,000 | 58,824 |

_Corpus and query definitions are in `bench/compare/logs/compare.py`; resource limits and image digests are in `bench/compare/logs/run.sh`._

