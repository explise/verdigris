# Issue #63 · Normalized three-engine log comparison

One fresh 1M-row deployment per engine, same corpus timestamp and query schedule,
100 measured warm samples per shape. Verdigris runs with durable audit enabled.
Budget: **8 CPU / 6 GiB per complete deployment**; Verdigris is 4 CPU / 3 GiB for
`vdg` plus 4 CPU / 3 GiB for MinIO. ClickHouse and VictoriaLogs each get one
8 CPU / 6 GiB container.

Image: `verdigris-bench:perf81-cfe5146`
`sha256:861a0f0974ea6fab517059789880a038186be777099fb2b3c68bc380575e4ce2`.
All external images are digest-pinned in `bench/compare/logs/run.sh`. Each run
allocates unique container/network/volume names and removes only those resources.

## Warm query latency (p50 / p95 / p99, ms)

| System | Service search | Trace hit | Trace miss | Cross-tier | Aggregation |
|---|---:|---:|---:|---:|---:|
| **Verdigris + MinIO** | 25.7 / 28.4 / 29.7 | 20.4 / 23.9 / 25.9 | 8.6 / 9.5 / 11.0 | 19.8 / 23.1 / 24.4 | 27.8 / 31.1 / 33.0 |
| **ClickHouse** | 5.0 / 5.6 / 5.9 | 3.4 / 7.3 / 8.6 | 1.7 / 2.5 / 3.1 | 4.7 / 5.6 / 6.4 | 7.4 / 8.9 / 9.6 |
| **VictoriaLogs** | 21.7 / 24.4 / 25.1 | 2.3 / 3.8 / 5.5 | 0.5 / 0.7 / 0.8 | 2.6 / 3.8 / 4.3 | 12.3 / 13.8 / 14.3 |

All engines returned 5,883 recent matches, 200-row pages in newest-first order,
one exact trace hit, three cross-tier rows, zero miss rows, and the same 50
one-minute buckets with identical totals/errors (1,000,000 / 58,824). Every
search and trace row's level, service, message, trace id and `attrs_json` value
was validated. Verdigris additionally passed one tier per row for the cross-tier
id.

## First query after each restart (ms)

Each shape is a separate process restart; MinIO and the OS cache stay warm.

| System | Trace | Search | Aggregation |
|---|---:|---:|---:|
| Verdigris + MinIO | 30.2 | 45.7 | 33.9 |
| ClickHouse | 9.8 | 7.4 | 10.2 |
| VictoriaLogs | 15.8 | 76.8 | 16.5 |

The exact-trace restart target (≤40 ms) is met.

## Storage accounting

`retained_bytes` is **18.8 MiB** total MinIO `/data`:

| Component | Bytes | MiB |
|---|---:|---:|
| `/data/verdigris` (Parquet + `_metadata`) | 19,669,428 | 18.76 |
| `/data/.minio.sys` | 12,132 | 0.01 |
| **Total** | **19,681,706** | **18.77** |

The manifest reports **3 active Parquet files, 0 retired**, `totalGB` =
**18.5 MiB**. The earlier ~155 MiB `.minio.sys` residual was stale state in a
reused volume, not live data; a fresh volume has no such gap. Do not compare
across engines using a reused `/data` directory.

## Notes

- Search uses the same predicate and newest-first order on all engines; the
  VDG query is SQL over the same columns, not a relative `last 1h`, so separately
  started runs share one schedule.
- The corpus timestamp is computed once (`CORPUS_TS_MS`) and exported to every
  engine process.
- Verdigris ingest acknowledgement is 275k rows/s vs 664k (ClickHouse) and
  881k (VictoriaLogs); query latency is reported separately.
- Raw JSON, restart files, machine/config metadata, summary and run log are in
  this directory.
