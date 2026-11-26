# Equivalent log-query comparison

Same deterministic corpus, same 8 CPU / 6 GiB total budget per complete deployment, one system at a time. Verdigris's budget includes its MinIO object store (4 CPU / 3 GiB + 4 CPU / 3 GiB).

| System | Retained | Container memory after queries |
|---|---:|---:|
| **Verdigris + MinIO** | 165.0 MiB | vdgbench-vdg=102.1MiB / 3GiB,vdgbench-minio=273.2MiB / 3GiB |
| **ClickHouse** | 95.2 MiB | chbench=230.5MiB / 6GiB |
| **VictoriaLogs** | 18.7 MiB | vlbench=236.5MiB / 6GiB |

## Ingest (reported separately from query latency)

| System | Acked rows/s | MiB/s | Visibility wait |
|---|---:|---:|---:|
| Verdigris + MinIO | 316,546 | 78.9 | 0.1s |
| ClickHouse | 209,907 | 43.7 | 0.0s |
| VictoriaLogs | 945,176 | 196.5 | 2.0s |

## Query latency (ms, after merges/compaction settle)

### Recent service search (`checkout` + `ERROR`, newest 200)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 36.4 | 38.8 | 38.8 | 38.8 | 10 |
| ClickHouse | 4.1 | 5.3 | 5.3 | 5.3 | 10 |
| VictoriaLogs | 4.6 | 5.0 | 5.0 | 5.0 | 10 |

### Trace lookup (exact `trace_id`)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 31.6 | 32.8 | 32.8 | 32.8 | 10 |
| ClickHouse | 2.3 | 3.1 | 3.1 | 3.1 | 10 |
| VictoriaLogs | 0.7 | 0.7 | 0.7 | 0.7 | 10 |

### 1-minute aggregation (total + errors)

| System | p50 | p95 | p99 | max | Samples |
|---|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 38.1 | 39.6 | 39.6 | 39.6 | 10 |
| ClickHouse | 6.8 | 7.5 | 7.5 | 7.5 | 10 |
| VictoriaLogs | 6.2 | 7.3 | 7.3 | 7.3 | 10 |

## Equivalence gate

| System | Recent count | Search rows | Trace rows | Buckets | Total | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Verdigris + MinIO | 5,883 | 200 | 1 | 50 | 1,000,000 | 58,824 |
| ClickHouse | 5,883 | 200 | 1 | 50 | 1,000,000 | 58,824 |
| VictoriaLogs | 5,883 | 200 | 1 | 50 | 1,000,000 | 58,824 |

_Corpus and query definitions are in `bench/compare/logs/compare.py`; resource limits and image digests are in `bench/compare/logs/run.sh`._

