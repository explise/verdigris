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
VDG_SKIP_BUILD=1 bench/compare/logs/run.sh      # reuse a built verdigris-bench image
```

Results land in `bench/results/compare-logs/` (`<system>.json`,
`<system>.meta.json`, `SUMMARY.md`). The script tears its containers down on
exit.

## Method

- **One complete deployment at a time**, each with the same **8 CPU / 6 GiB**
  budget. Verdigris is charged for its object store: `vdg` gets 4 CPU / 3 GiB
  and MinIO gets 4 CPU / 3 GiB. ClickHouse and VictoriaLogs each get 8 CPU /
  6 GiB in one container. Running them sequentially avoids one system stealing
  the other's CPU during measurement.
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
- **Equivalence is a gate, not a hope.** Before timing, each system must return
  the same exact recent-match count, the same 200-row search page size, exactly
  one row for the middle trace id, and the same total/error counts across the
  same number of one-minute buckets. A mismatch exits nonzero.
- **Latency** is one warm connection per backend, `WARMUP` warmups then
  `ITERATIONS` measured requests; p50/p95/p99 use nearest-rank percentiles.

## Query shapes

| Shape | Verdigris | ClickHouse | VictoriaLogs |
|---|---|---|---|
| Recent service search | `service:checkout level:error \| last 1h` (the product DSL) | `WHERE service='checkout' AND level='ERROR' ORDER BY ts DESC LIMIT 200` | `service:checkout level:ERROR \| sort by (_time) desc \| limit 200` |
| Trace lookup | `trace_id:<id>` | `WHERE trace_id='<id>'` | `trace_id:="<id>"` |
| Minute aggregation | `date_bin(INTERVAL '60 seconds', …)` + `count(*) FILTER` | `toStartOfInterval(ts, INTERVAL 60 SECOND)` + `countIf` | `stats by (_time:1m) count() total, count() if (level:ERROR) errors` |

The Verdigris search uses its search DSL rather than raw SQL because that is the
path the Logs explorer uses (and the path that applies file-level
`service`/`level` pruning). The aggregation is SQL on all three because the DSL
does not express it.

The trace workload rotates through a fixed set of real IDs spread across the
corpus, and also checks a missing ID and one ID deliberately written to a row in
each tier. Verdigris additionally checks each tier independently. Raw per-request
timings and the shared metadata-cache hit delta are recorded in the JSON for
issue #70; see [`bench/results/issue70/`](../../results/issue70/README.md).

## Measured 2026-11-26

Host: 16 vCPU / 14 GiB, Docker 29.9; one system at a time. 1,000,000 events,
249.2 MiB Verdigris wire bytes (208 MiB for the flat ClickHouse/VictoriaLogs
wire encoding), 50 one-minute buckets. Full machine/config record is written to
`bench/results/compare-logs/machine.txt`.

| System | Retained after settle | Memory after queries | Ingest acked | Visible after |
|---|---:|---:|---:|---:|
| **Verdigris + MinIO** | 165.0 MiB | 375 MiB | 317k rows/s (79 MiB/s) | 0.1 s |
| **ClickHouse** | 95.2 MiB | 231 MiB | 210k rows/s (44 MiB/s) | 0.0 s |
| **VictoriaLogs** | 18.7 MiB | 237 MiB | 945k rows/s (197 MiB/s) | 2.0 s |

Query latency, p50 / p95 (ms), 10 samples after warmup:

| System | Service search | Trace lookup | 1-minute aggregation |
|---|---:|---:|---:|
| **Verdigris + MinIO** | 36.4 / 38.8 | 31.6 / 32.8 | 38.1 / 39.6 |
| **ClickHouse** | 4.1 / 5.3 | 2.3 / 3.1 | 6.8 / 7.5 |
| **VictoriaLogs** | 4.6 / 5.0 | 0.7 / 0.7 | 6.2 / 7.3 |

All three returned the same `5,883` recent matches, the same 200-row page, one
row for the trace id, and the same `1,000,000 / 58,824` total/error split across
50 buckets.

What the Verdigris numbers are actually measuring: half of its 8-CPU budget is
the MinIO object store, so query execution gets 4 cores, and every scanned file
crosses an HTTP object-store boundary instead of a local page cache. The
30–40 ms band is the same for all three shapes, which points at fixed per-query
planning/object-store overhead rather than bytes scanned. ClickHouse and
VictoriaLogs read local files with all 8 cores. This is the architectural trade the product makes for data sovereignty
and no rehydration, and the comparison should be read that way — not as a
like-for-like CPU benchmark of three identical storage models.

Ingest rates are also not comparable across acknowledgement policies:
Verdigris acknowledges after the Parquet PUT and manifest CAS commit;
VictoriaLogs acknowledged the same corpus ~2 s before it became fully queryable.
The visibility column exists so that difference stays visible.

## What this does and does not prove

It proves relative query latency for three common log-and-archive shapes on one
host, with the corpus and result cardinalities held constant. It does **not**
prove performance at billion-row scale, across cold S3/Glacier tiers, or under
concurrent query load. All three deployments use local disk; Verdigris's bytes
go through MinIO's S3 API, which is slow relative to real S3 but is charged to
its budget as the production deployment would be.

The retained-bytes column is the on-disk footprint after settling, not S3
storage-class charges. A cold-storage comparison needs a lifecycle/restore run
(see `docs/cost.md`), not this harness.

## Profile Verdigris query latency

Build the release image with the repository Dockerfile. Then run:

```sh
docker build -t verdigris-bench:issue83 .
VDG_IMAGE=verdigris-bench:issue83 OUT=/tmp/verdigris-profile-1 \
  bench/compare/logs/run-profile.sh
```

Use a new output path for each deployment. Run three fresh 1M-row
deployments and one with `ROWS=20000000`. Each run prunes its own benchmark
network and volume. The pinned MinIO server, client and BusyBox images need
to be present locally. The runner enables the checked-out branch's audit
policy and records audit counters. Compare results only against builds with
the intended durable audit policy.

The seed step checks result counts and waits for compaction. The warm step
uses the same schedule with diagnostics enabled and disabled in alternating
order. Each restart result is the first query of its shape after restarting
the Verdigris process. MinIO and the operating system's file cache stay warm.

`X-VDG-Query-Profile: 1` enables query timing in microseconds and returns it
in the response header of the same name for JSON or Arrow. The `plan` value
also adds a bounded display of the physical plan. The header is a diagnostic
interface. It does not change rows or the normal response body. Enable it
only on sample requests because it adds instrumentation and header overhead.
There is no query admission timer in builds without the admission gate.

Execution includes object reads, decode and runtime scheduling. Parquet
physical metrics cover the primary scan, while the timed histogram is a
separate phase if requested. The request-local object counters distinguish
GETs, HEADs and logical range batches. Range bytes are before backend
coalescing and are not provider-billed metrics. Manifest and audit I/O are
outside the Parquet counters. The client latency includes HTTP transfer and
response parsing. Server phase totals end after encoding the response,
before profiling-header serialization and network transmission.
