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

## Results on the four-CPU reference host

Each range below is p95 across three fresh 1M-row deployments. Each deployment
ran 100 warm samples per shape and validated the full query results. Verdigris
ran with durable audit and disk-backed MinIO inside the same total CPU and RAM
budget as each competitor.

| Warm p95 (ms) | Verdigris + MinIO | ClickHouse | VictoriaLogs |
| --- | ---: | ---: | ---: |
| Exact trace hit | 17.27–18.65 | 5.84–6.00 | 1.83–3.21 |
| Exact trace miss | 4.27–4.44 | 1.61–1.65 | 0.63–0.66 |
| Cross-tier trace | 24.48–26.26 | 3.81–3.88 | 1.25–3.13 |
| Recent service search | 17.89–18.67 | 3.78–3.85 | 7.56–16.01 |
| Raw minute aggregation | 37.84–38.10 | 8.55–8.81 | 10.63–15.45 |

Verdigris met its 1M warm targets for trace hit, trace miss and recent search.
Both reference engines were faster on every measured shape. The separate 20M
Verdigris sizing run met the warm trace-hit and trace-miss targets and missed
cross-tier trace, recent search and raw aggregation. These measurements use a
local S3-compatible store and a specific synthetic corpus. [Raw three-engine
baseline](../../results/issue63/four-cpu/README.md) · [20M sizing
run](../../results/issue83/four-cpu/README.md)

The [earlier eight-CPU cohort](../../results/issue63/README.md) used a different
host and budget. Its values are retained as a separate reference.

## First matched 20M sizing comparison

The same four-CPU budget now has one matched 20M deployment per
engine. Every warm query and first query after restart passed the
same result checks as the 1M cohort.

| Warm p95 (ms) | Verdigris + MinIO | ClickHouse | VictoriaLogs |
| --- | ---: | ---: | ---: |
| Exact trace hit | 18.68 | 119.06 | 4.61 |
| Exact trace miss | 4.48 | 1.95 | 3.85 |
| Cross-tier trace | 28.62 | 6.11 | 5.57 |
| Recent service search | 104.47 | 3.99 | 30.73 |
| Raw minute aggregation | 388.72 | 166.00 | 142.76 |

ClickHouse exact hits had a 2.96 ms median, with six of 100 requests
above 119 ms. Repeat the matched 20M run before drawing a conclusion
from that tail. Verdigris completed all 550 expected warm audit
acknowledgements within the configured deadline. The short ingest
comparison reports acknowledgement, visibility and storage separately.
[Raw samples and method](../../results/issue63/matched-20m/README.md)
