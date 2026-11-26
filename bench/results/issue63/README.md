# Issue #63 — normalized three-engine log comparison

One million events, same corpus, same query schedule, each complete deployment
at **8 CPU / 6 GiB** (Verdigris includes MinIO at 4+4 CPU / 3+3 GiB). 5 warmups
+ 50 measured requests after compaction/merge settle; audit enabled on the
Verdigris image. First-query-after-restart measurements are separate.

Result validation checks returned values, not just counts: every row carries
`attrs_json`; search rows must match level/service/message/attributes; trace
truth covers rotating hits, a miss, and one ID in each tier; every one-minute
bucket's total and error count must match the generator.

Image: `verdigris-bench:issue79-final`
`sha256:9a5d372afb94390521e19c2134822202eb1611cddcb7f08ee046e2db2c641bd7`.

## Warm query latency (p50 / p95, ms)

| System | Service search | Trace hit | Trace miss | Cross-tier | Aggregation |
|---|---:|---:|---:|---:|---:|
| **Verdigris + MinIO** | 23.0 / 28.3 | 17.6 / 20.5 | 7.1 / 7.9 | 17.5 / 19.5 | 24.8 / 27.0 |
| **ClickHouse** | 4.4 / 6.0 | 2.9 / 7.2 | 1.5 / 2.1 | 4.2 / 4.9 | 7.0 / 7.9 |
| **VictoriaLogs** | 8.3 / 10.0 | 1.3 / 2.6 | 0.4 / 0.5 | 2.2 / 3.5 | 5.5 / 6.7 |

All three returned 5,883 recent matches, 200-row pages, the expected trace
cardinalities, and identical per-minute buckets.

## First query after restart (single request, ms)

| System | Trace | Search | Aggregation |
|---|---:|---:|---:|
| Verdigris + MinIO | 35.5 | 42.4 | 32.7 |
| ClickHouse | 10.0 | 5.1 | 6.2 |
| VictoriaLogs | 15.5 | 54.2 | 15.1 |

This is a process restart, not a cold-object restore; the OS page cache is not
flushed. It measures the cost of an empty process-level schema/metadata cache.

## Verdigris storage accounting

`retained_bytes` from the MinIO data path is **174.2 MiB**, which is not the
Parquet footprint:

| Component | Bytes | MiB |
|---|---:|---:|
| `/data/verdigris` (bucket: Parquet + `_metadata`) | 19,629,957 | 18.7 |
| `/data/.minio.sys` (MinIO internal format/metadata) | 163,289,208 | 155.7 |
| **Total `/data`** | **182,690,519** | **174.2** |

The manifest reports **3 active Parquet files, 0 retired**, `totalGB` =
**18.5 MiB**. The ~155 MiB gap is MinIO's own on-disk format, not unreclaimed
Verdigris objects. Comparing "on-disk bytes" across engines must either exclude
`.minio.sys` (use bucket bytes) or report it as object-store overhead.

## Remaining gap

Warm trace is ~17.6 ms with durable audit (~4 ms wait) and ~12–13 ms engine; the
other engines are 1–3 ms. Aggregation is ~25 ms vs 5–7 ms because Verdigris
scans every selected file. Sustained mixed-load admission results are in
`bench/results/issue29/`.

## Next engineering cycle

The storage question is closed: the gap is `.minio.sys` overhead, not unreclaimed
Parquet. There is no storage reclamation work worth prioritizing on this fixture.

The query data points the next cycle at the read path:

- warm trace is 17.6 ms vs 1.3–2.9 ms for the other engines, with ~4 ms of that
  being durable audit and ~12–13 ms engine/planning/scan;
- aggregation is 24.8 ms vs 5.5–7.0 ms and scans every selected file;
- under sustained heavy load, trace stays available but p95 rises to ~99 ms,
  while heavy work is mostly shed by admission.

Recommended order: reduce fixed DataFusion planning/scan overhead for warm
queries (especially exact/point and aggregate shapes), then re-evaluate the
admission split using the faster per-query service time. Do not spend the cycle
on storage reclamation or on raising admission concurrency first — both would
mask the read-path gap rather than close it.
