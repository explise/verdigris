# Equivalent log queries: Verdigris vs ClickHouse vs VictoriaLogs

This is the query-side companion to the ingest comparison in
`bench/compare/`. It answers the question the ingest numbers cannot: for the
same log events and the same three query shapes, what does each engine return,
and how long does it take under an equal resource budget?

```sh
bench/compare/logs/run.sh
```

Useful knobs:

```sh
ROWS=200000 ITERATIONS=5 SYSTEMS="vdg clickhouse" bench/compare/logs/run.sh
VDG_SKIP_BUILD=1 VDG_IMAGE=verdigris-bench:<tag> bench/compare/logs/run.sh
RESTART=1 bench/compare/logs/run.sh             # also restart once per first-query shape
TOTAL_CPUS=4 bench/compare/logs/run.sh          # equal budget on a 4-vCPU host
```

Results land in `bench/results/compare-logs/` (`<system>.json`,
`<system>.meta.json`, `SUMMARY.md`). The script tears its containers down on
exit.

## Method

- **One complete deployment at a time**, each with the same **8 CPU / 6 GiB**
  budget. Verdigris is charged for its object store: `vdg` gets 4 CPU / 3 GiB
  and MinIO gets 4 CPU / 3 GiB. ClickHouse and VictoriaLogs each get 8 CPU /
  6 GiB in one container. Running them sequentially avoids one system stealing
  the other's CPU during measurement. Supply `TOTAL_CPUS=4` for a 4-vCPU
  host. Verdigris and MinIO then get 2 CPUs each, and each competitor gets
  4 CPUs. Report each CPU budget as its own cohort.
- **Same corpus, generated from row index**, not ingested once and copied:
  1,000,000 events over a 50-minute window, ten services, ~5.9% `ERROR`,
  unique `trace_id` per row, log-like messages and attributes. Every system
  receives the same values; only the timestamp encoding differs (epoch millis
  for Verdigris, RFC3339 for ClickHouse/VictoriaLogs).
- **Ingest is reported separately** from query timing. Acknowledgement and
  visibility are different policies, so the driver waits until the exact row
  count is queryable and prints that wait rather than folding it into a query
  number.
- **Merges/compaction settle before queries.** ClickHouse runs `OPTIMIZE TABLE
  … FINAL`; Verdigris runs its background compaction until `/v1/metrics`
  reports `pendingFiles == 0`; VictoriaLogs is given a fixed settle window.
- **Result validation.** Every warmup, timed query, and first query after a restart
  is checked against the fixture. Search rows are checked in newest-first order.
  Trace hits, misses, and cross-tier traces are checked for exact row values.
  Every minute bucket is checked for timestamp, total, and error count in order.
- **Latency** is one warm connection per backend, with `WARMUP` warmups and
  `ITERATIONS` measured requests. Validation runs outside the measured interval.
  p50 is the median, with nearest rank for p95 and p99. The restart run makes a
  fresh connection and restarts the engine separately for each of the five shapes.

## Query shapes

| Shape | Verdigris | ClickHouse | VictoriaLogs |
|---|---|---|---|
| Recent service search | `WHERE service='checkout' AND level='ERROR' ORDER BY ts DESC LIMIT 200` | same predicate/order | `service:checkout level:ERROR \| sort by (_time) desc \| limit 200` |
| Trace lookup | `trace_id:<id>` | `WHERE trace_id='<id>'` | `trace_id:="<id>"` |
| Minute aggregation | `date_bin(INTERVAL '60 seconds', …)` + `count(*) FILTER` | `toStartOfInterval(ts, INTERVAL 60 SECOND)` + `countIf` | `stats by (_time:1m) count() total, count() if (level:ERROR) errors \| sort by (_time)` |

All three search queries use the same predicate and newest-first order. Verdigris
uses SQL for this comparison so a relative `last 1h` cannot drift between
separately started engine processes; VictoriaLogs uses LogsQL for aggregation.

The trace workload rotates through a fixed set of real IDs spread across the
corpus, and also checks a missing ID and one ID deliberately written to a row in
each tier. Verdigris additionally checks each tier independently. Rows carry the
same `attrs_json` values on all three systems, and every one-minute bucket's
total/error counts are validated against the generator (not just the sums).
Raw per-request timings and the shared metadata-cache hit delta are recorded in
the JSON. See [`bench/results/issue63/`](../../results/issue63/README.md) for the
normalized three-engine run and [`bench/results/issue70/`](../../results/issue70/README.md)
for the trace-latency work.

## Measured 2026-11-27

The current run is in [`bench/results/issue63/`](../../results/issue63/README.md):
three-engine, 100 warm samples per shape, with per-query restart samples and
storage decomposed into bucket bytes vs MinIO internals. The earlier
2026-11-26 numbers in history used 10 samples and a relative search window; do
not mix the two tables.

Host: 16 vCPU / 14 GiB, Docker 29.9; one system at a time. Verdigris + MinIO is
charged 8 CPU / 6 GiB total, same as each single-container competitor. Raw
machine/config metadata, image IDs and every request sample are in the result
directory.
