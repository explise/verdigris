#!/usr/bin/env python3
"""Equivalent log-query comparison: Verdigris vs ClickHouse vs VictoriaLogs.

One driver per backend. The corpus is generated deterministically from row index,
so every system receives byte-identical events (modulo each API's timestamp
encoding), and the same three query families run against each:

  1. recent service search  service=checkout level=ERROR, newest 200
  2. high-cardinality trace lookup  trace_id = <the middle row's id>
  3. one-minute aggregation  total + error counts per bucket

The point is *query* latency under a matched resource budget, not ingestion
throughput (bench/compare/oo_ch.py covers that). Ingest timing is printed only to
show the corpus actually landed; query timings start after the corpus is visible
and, for Verdigris/ClickHouse, after compaction/parts are settled.

Usage:
    compare.py --system vdg           --port 8080
    compare.py --system clickhouse    --port 8123
    compare.py --system victorialogs  --port 9428
"""
import argparse
import concurrent.futures
import datetime
import http.client
import itertools
import json
import math
import os
import statistics
import sys
import threading
import time
import urllib.parse
from collections import Counter

SERVICES = [
    "gateway", "checkout", "auth", "billing", "search",
    "catalog", "ingest", "query", "alerts", "settings",
]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
LEVELS = ("ERROR", "WARN", "INFO", "DEBUG")

DEFAULT_ROWS = 1_000_000
DEFAULT_BATCH_ROWS = 10_000
DEFAULT_THREADS = 4
DEFAULT_ITERATIONS = 10
DEFAULT_WARMUP = 3
MAX_IDENTITY_SMOKE_ROWS = 100_000

# One trace id is deliberately written to one row in each tier. A full lookup
# must return all three; a tier-scoped Verdigris query must return exactly one.
CROSS_TRACE = "cross-tier-trace"
CROSS_TRACE_ROWS = (0, 1, 3)  # ERROR/hot, INFO/warm, DEBUG/cold

# The corpus sits wholly inside "the last hour" so a `| last 1h` DSL query is
# equivalent to an absolute [start, end) filter on the other two systems.
NOW_MS = int(time.time() * 1_000)
# One run computes this once and exports it to every backend process, so all
# three engines ingest the exact same timestamps. A per-process default keeps
# the standalone invocation useful.
BASE_TS = int(
    os.environ.get(
        "CORPUS_TS_MS", str((NOW_MS - 55 * 60_000) // 60_000 * 60_000)
    )
)
WINDOW_MS = 50 * 60_000


def iso_ms(ms: int) -> str:
    """RFC3339 with milliseconds, accepted by ClickHouse and VictoriaLogs."""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + f".{ms % 1000:03d}Z"


def normalize_rows(rows):
    """Map each engine's native field names to the common log-row contract."""
    out = []
    for r in rows:
        r = dict(r)
        if "_msg" in r and "message" not in r:
            r["message"] = r.pop("_msg")
        if "_time" in r and "ts" not in r:
            r["ts"] = r.pop("_time")
        out.append(r)
    return out


def bucket_ms(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace("Z", "+00:00")
    dt = datetime.datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return round(dt.timestamp() * 1000)


def row_ts_ms(row):
    value = row.get("ts", row.get("_time"))
    if value is None:
        raise SystemExit(f"row has no timestamp: {row}")
    return bucket_ms(value)


MISSING_TRACE = "ffffffffffffffff"
RECENT_SEARCH_SQL = (
    "SELECT ts, level, service, status, message, trace_id, attrs_json "
    "FROM bench WHERE service = 'checkout' AND level = 'ERROR' "
    "ORDER BY ts DESC LIMIT 200"
)


def trace_schedule(rows):
    return [f"{i:016x}" for i in sorted({max(4, k * rows // 16) for k in range(16)}) if i < rows]


def expected_row(index, rows):
    payload = record(index, rows, "vdg")
    return (payload["ts_millis"], payload["level"], payload["service"],
            payload["status"], payload["message"], payload["trace_id"], payload["attrs"])


def canonical_row(row):
    row = normalize_rows([row])[0]
    return (row_ts_ms(row), row["level"], row["service"], int(row["status"]),
            row["message"], row["trace_id"], json.loads(row["attrs_json"]))


def expected_search(rows):
    indices = (i for i in range(rows - 1, -1, -1)
               if level_for(i) == "ERROR" and service_for(i) == "checkout")
    return [expected_row(i, rows) for i in itertools.islice(indices, 200)]


def validate_search(got, expected):
    if [canonical_row(r) for r in got] != expected:
        raise SystemExit("search returned different fields, rows, or order")


def validate_trace(trace_id: str, got, rows: int):
    if trace_id == CROSS_TRACE:
        indices = CROSS_TRACE_ROWS
    elif trace_id == MISSING_TRACE:
        indices = ()
    else:
        indices = (int(trace_id, 16),)
    actual = sorted((canonical_row(r) for r in got), key=lambda r: (r[0], r[4]))
    expected = sorted((expected_row(i, rows) for i in indices), key=lambda r: (r[0], r[4]))
    if actual != expected:
        raise SystemExit(f"trace {trace_id} returned {len(actual)} rows with different fields or identities (expected {len(expected)})")


def validate_aggregation(got, expected):
    actual = [(bucket_ms(r.get("bucket") or r["_time"]), int(r["total"]), int(r["errors"]))
              for r in got]
    want = [(bucket, *counts) for bucket, counts in expected.items()]
    if actual != want:
        raise SystemExit(f"aggregate returned different ordered buckets (got {len(actual)}, expected {len(want)})")


def expected_buckets(rows: int):
    buckets = {}
    for i in range(rows):
        bucket = (ts_for(i, rows) // 60_000) * 60_000
        total, errors = buckets.get(bucket, (0, 0))
        buckets[bucket] = (total + 1, errors + (1 if level_for(i) == "ERROR" else 0))
    return buckets


def level_for(i: int) -> str:
    if i % 17 == 0:
        return "ERROR"
    if i % 7 == 0:
        return "WARN"
    if i % 3 == 0:
        return "DEBUG"
    return "INFO"


def status_for(level: str) -> int:
    return 500 if level == "ERROR" else (429 if level == "WARN" else 200)


def service_for(i: int) -> str:
    return SERVICES[i % len(SERVICES)]


def ts_for(i: int, rows: int) -> int:
    return BASE_TS + (i * WINDOW_MS) // rows


def message_for(i: int, level: str, service: str) -> str:
    # Deliberately log-like rather than tiny: message bytes dominate a row fetch.
    return (
        f"{level.lower()} request {i} route=/{service}/{i % 97} "
        f"duration_ms={i % 250} region={REGIONS[i % len(REGIONS)]} "
        f"trace={i:016x}"
    )


def validate_row_identity(messages, rows):
    # Each generated message contains its unique input index even when three
    # rows deliberately share a trace ID. Compare multiplicities exactly.
    expected = Counter(message_for(i, level_for(i), service_for(i)) for i in range(rows))
    actual = Counter(messages)
    if actual != expected:
        missing = expected - actual
        unexpected = actual - expected
        raise SystemExit(
            f"row identity mismatch: {sum(missing.values())} missing; "
            f"{sum(unexpected.values())} duplicate or unexpected "
            f"(examples: {next(iter(missing), None)!r}, {next(iter(unexpected), None)!r})"
        )


def record(i: int, rows: int, system: str) -> dict:
    level = level_for(i)
    service = service_for(i)
    msg = message_for(i, level, service)
    ts = ts_for(i, rows)
    trace_id = CROSS_TRACE if i in CROSS_TRACE_ROWS else f"{i:016x}"
    if system == "vdg":
        return {
            "ts_millis": ts,
            "level": level,
            "service": service,
            "status": status_for(level),
            "message": msg,
            "trace_id": trace_id,
            "attrs": {
                "region": REGIONS[i % len(REGIONS)],
                "host": f"node-{i % 50:02d}",
            },
        }
    if system == "clickhouse":
        return {
            "ts": iso_ms(ts),
            "level": level,
            "service": service,
            "status": status_for(level),
            "message": msg,
            "trace_id": trace_id,
            "attrs_json": json.dumps(
                {"region": REGIONS[i % len(REGIONS)], "host": f"node-{i % 50:02d}"},
                separators=(",", ":"),
            ),
        }
    return {
        "_time": iso_ms(ts),
        "_msg": msg,
        "level": level,
        "service": service,
        "status": status_for(level),
        "trace_id": trace_id,
        "attrs_json": json.dumps(
            {"region": REGIONS[i % len(REGIONS)], "host": f"node-{i % 50:02d}"},
            separators=(",", ":"),
        ),
    }


def body_for(system: str, start: int, end: int, rows: int) -> bytes:
    lines = [
        json.dumps(record(i, rows, system), separators=(",", ":"))
        for i in range(start, end)
    ]
    return ("\n".join(lines) + "\n").encode()


def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0
    idx = max(0, math.ceil(len(sorted_values) * p) - 1)
    return sorted_values[idx]


class HttpClient:
    """One keep-alive connection *per thread*, with one reconnect on a dropped
    keep-alive. A single shared HTTPConnection cannot carry concurrent requests:
    interleaved responses corrupt a body, and retrying an actually-completed
    insert duplicates rows. Not retrying a failed query: a failed query is a
    result."""

    def __init__(self, host, port, timeout=300):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.local = threading.local()

    def _connect(self):
        conn = getattr(self.local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            self.local.conn = conn
        return conn

    def request(self, method, path, body=None, headers=None):
        headers = dict(headers or {})
        for attempt in (0, 1):
            conn = self._connect()
            try:
                conn.request(method, path, body=body, headers=headers)
                res = conn.getresponse()
                data = res.read()
                return res.status, res.getheaders(), data
            except (http.client.HTTPException, OSError):
                try:
                    conn.close()
                finally:
                    self.local.conn = None
                if attempt == 1:
                    raise
        raise AssertionError("unreachable")

    def close(self):
        conn = getattr(self.local, "conn", None)
        if conn is not None:
            conn.close()
            self.local.conn = None


class Backend:
    name = "base"

    def __init__(self, args):
        self.args = args
        self.client = HttpClient(args.host, args.port)
        self.last_stats = {}

    def ingest_one(self, body: bytes) -> int:
        raise NotImplementedError

    def settle(self):
        pass

    def validate(self):
        raise NotImplementedError

    def fixture_messages(self, limit):
        raise NotImplementedError

    def count_recent(self):
        raise NotImplementedError

    def search_recent(self):
        raise NotImplementedError

    def trace_lookup(self):
        raise NotImplementedError

    def aggregate_minute(self):
        raise NotImplementedError

    def close(self):
        self.client.close()


class Vdg(Backend):
    name = "vdg"

    def ingest_one(self, body: bytes) -> int:
        status, _, data = self.client.request(
            "POST", "/v1/ingest", body,
            {"content-type": "application/x-ndjson"},
        )
        if status != 200:
            raise RuntimeError(f"vdg ingest {status}: {data[:300]!r}")
        return int(json.loads(data)["ingested"])

    profile_mode = None
    last_profile = None

    def _query(self, sql: str, tiers=()):
        body = json.dumps({"sql": sql, "tiers": list(tiers), "histogram": False}).encode()
        request_headers = {"content-type": "application/json"}
        if self.profile_mode:
            request_headers["x-vdg-query-profile"] = self.profile_mode
        status, headers, data = self.client.request("POST", "/v1/query", body, request_headers)
        if status != 200:
            raise RuntimeError(f"vdg query {status}: {data[:300]!r}")
        payload = json.loads(data)
        self.last_stats = payload.get("stats") or {}
        self.last_profile = None
        if self.profile_mode:
            value = next((v for k, v in headers if k.lower() == "x-vdg-query-profile"), None)
            if value is None:
                raise RuntimeError("server did not return the requested query profile")
            self.last_profile = json.loads(value)
        return payload["rows"]

    def settle(self):
        # Background compaction runs on a 5s cadence in vdg.toml. Wait until the
        # scheduler has both run and drained the pending-file count, so queries
        # are not timed against a half-compacted table (ClickHouse gets an
        # explicit OPTIMIZE; VictoriaLogs merges asynchronously).
        deadline = time.monotonic() + 300
        last = None
        while time.monotonic() < deadline:
            status, _, data = self.client.request("GET", "/v1/metrics")
            if status == 200:
                comp = json.loads(data)["compaction"]
                last = (comp["runsTotal"], comp["pendingFiles"])
                if comp["runsTotal"] > 0 and comp["pendingFiles"] == 0:
                    return
            time.sleep(2)
        raise RuntimeError(f"vdg compaction did not settle: runs/pending={last}")

    def validate(self):
        rows = self._query("SELECT count(*) AS n FROM bench")
        return int(rows[0]["n"])

    def fixture_messages(self, limit):
        return [row["message"] for row in self._query(f"SELECT message FROM bench LIMIT {limit}")]

    def count_recent(self):
        rows = self._query(
            "SELECT count(*) AS n FROM bench WHERE service = 'checkout' AND level = 'ERROR'"
        )
        return int(rows[0]["n"])

    def search_recent(self):
        # SQL over the same columns/predicate/order as the other engines. A
        # relative `last 1h` would drift between separately started runs; the
        # corpus is one fixed window, so the explicit query schedule is stable.
        return self._query(RECENT_SEARCH_SQL)

    def trace_lookup(self, trace_id=None):
        trace_id = trace_id or self.args.trace_id
        return self._trace_query(trace_id, [])

    def trace_lookup_tiered(self, trace_id: str, tiers):
        return self._trace_query(trace_id, tiers)

    def _trace_query(self, trace_id: str, tiers):
        return self._query(f"trace_id:{trace_id}", tiers)

    def aggregate_minute(self):
        return self._query(
            "SELECT date_bin(INTERVAL '60 seconds', ts, "
            "TIMESTAMP '1970-01-01T00:00:00') AS bucket, "
            "count(*) AS total, "
            "count(*) FILTER (WHERE level = 'ERROR') AS errors "
            "FROM bench GROUP BY bucket ORDER BY bucket"
        )

    def cache_stats(self):
        status, _, data = self.client.request("GET", "/v1/metrics")
        if status != 200:
            return {}
        return json.loads(data).get("queryCache") or {}


class ClickHouse(Backend):
    name = "clickhouse"
    auth_headers = {
        "Authorization": "Basic " + __import__("base64").b64encode(b"default:benchpass").decode(),
        "Content-Type": "text/plain; charset=utf-8",
    }

    def setup(self):
        status, _, data = self.client.request(
            "POST", "/", """CREATE TABLE IF NOT EXISTS bench (
                ts DateTime64(3),
                level LowCardinality(String),
                service LowCardinality(String),
                status Nullable(Int32),
                message String,
                trace_id String,
                attrs_json String
            ) ENGINE = MergeTree ORDER BY (service, level, ts)""",
            self.auth_headers,
        )
        if status != 200:
            raise RuntimeError(f"clickhouse create {status}: {data[:300]!r}")
        status, _, data = self.client.request(
            "POST", "/", "TRUNCATE TABLE bench", self.auth_headers,
        )
        if status != 200:
            raise RuntimeError(f"clickhouse truncate {status}: {data[:300]!r}")

    def ingest_one(self, body: bytes) -> int:
        q = urllib.parse.quote("INSERT INTO bench FORMAT JSONEachRow")
        status, _, data = self.client.request(
            "POST", f"/?query={q}&date_time_input_format=best_effort", body,
            {**self.auth_headers, "Content-Type": "application/x-ndjson"},
        )
        if status != 200:
            raise RuntimeError(f"clickhouse insert {status}: {data[:300]!r}")
        return body.count(b"\n")

    def settle(self):
        # Merge parts explicitly so a small-file/deferred-merge advantage does not
        # masquerade as a query-engine advantage.
        status, _, data = self.client.request(
            "POST", "/", "OPTIMIZE TABLE bench FINAL", self.auth_headers,
        )
        if status != 200:
            raise RuntimeError(f"clickhouse optimize {status}: {data[:300]!r}")

    def _sql_json(self, sql: str):
        status, _, data = self.client.request(
            "POST", "/", sql + " FORMAT JSON", self.auth_headers,
        )
        if status != 200:
            raise RuntimeError(f"clickhouse query {status}: {data[:300]!r}")
        return json.loads(data)["data"]

    def validate(self):
        return int(self._sql_json("SELECT count() AS n FROM bench")[0]["n"])

    def fixture_messages(self, limit):
        return [row["message"] for row in self._sql_json(f"SELECT message FROM bench LIMIT {limit}")]

    def count_recent(self):
        return int(self._sql_json(
            "SELECT count() AS n FROM bench WHERE service = 'checkout' AND level = 'ERROR'"
        )[0]["n"])

    def search_recent(self):
        return self._sql_json(
            "SELECT ts, level, service, status, message, trace_id, attrs_json FROM bench "
            "WHERE service = 'checkout' AND level = 'ERROR' "
            "ORDER BY ts DESC LIMIT 200"
        )

    def trace_lookup(self, trace_id=None):
        trace_id = trace_id or self.args.trace_id
        return self._sql_json(
            "SELECT ts, level, service, status, message, trace_id, attrs_json FROM bench "
            f"WHERE trace_id = '{trace_id}' LIMIT 10"
        )

    def aggregate_minute(self):
        return self._sql_json(
            "SELECT toStartOfInterval(ts, INTERVAL 60 SECOND) AS bucket, "
            "count() AS total, countIf(level = 'ERROR') AS errors "
            "FROM bench GROUP BY bucket ORDER BY bucket"
        )


class VictoriaLogs(Backend):
    name = "victorialogs"
    ingest_path = "/insert/jsonline?_time_field=_time&_msg_field=_msg&_stream_fields=service,level"

    def ingest_one(self, body: bytes) -> int:
        status, _, data = self.client.request(
            "POST", self.ingest_path, body,
            {"Content-Type": "application/stream+json"},
        )
        if status != 200:
            raise RuntimeError(f"victorialogs ingest {status}: {data[:300]!r}")
        return body.count(b"\n")

    def _form(self, path: str, fields: dict):
        body = urllib.parse.urlencode(fields).encode()
        status, _, data = self.client.request(
            "POST", path, body,
            {"Content-Type": "application/x-www-form-urlencoded"},
        )
        if status != 200:
            raise RuntimeError(f"victorialogs {path} {status}: {data[:300]!r}")
        return data

    def _query_rows(self, query: str, **extra):
        fields = {"query": query, **extra}
        data = self._form("/select/logsql/query", fields)
        return [json.loads(line) for line in data.splitlines() if line.strip()]

    def validate(self):
        rows = self._query_rows("* | stats count() total")
        return int(rows[0]["total"])

    def fixture_messages(self, limit):
        return [row["_msg"] for row in self._query_rows(f"* | fields _msg | limit {limit}")]

    def count_recent(self):
        rows = self._query_rows(
            "service:checkout level:ERROR | stats count() total"
        )
        return int(rows[0]["total"])

    def search_recent(self):
        return self._query_rows(
            "service:checkout level:ERROR "
            "| fields _time, level, service, status, _msg, trace_id, attrs_json "
            "| sort by (_time) desc | limit 200"
        )

    def trace_lookup(self, trace_id=None):
        trace_id = trace_id or self.args.trace_id
        return self._query_rows(
            f'trace_id:="{trace_id}" '
            "| fields _time, level, service, status, _msg, trace_id, attrs_json | limit 10"
        )

    def aggregate_minute(self):
        return self._query_rows(
            "* | stats by (_time:1m) count() total, count() if (level:ERROR) errors | sort by (_time)"
        )


def make_backend(system: str, args) -> Backend:
    if system == "vdg":
        return Vdg(args)
    if system == "clickhouse":
        return ClickHouse(args)
    if system == "victorialogs":
        return VictoriaLogs(args)
    raise SystemExit(f"unknown system {system}")


def timed(fn, iterations: int, warmup: int, validate):
    for _ in range(warmup):
        validate(fn())
    samples = []
    last = None
    for _ in range(iterations):
        t0 = time.perf_counter()
        last = fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
        validate(last)
    samples.sort()
    return {
        "samples": len(samples),
        "min_ms": samples[0],
        "p50_ms": statistics.median(samples),
        "p95_ms": percentile(samples, 0.95),
        "p99_ms": percentile(samples, 0.99),
        "max_ms": samples[-1],
        "result_rows": len(last) if hasattr(last, "__len__") else None,
        "raw_ms": samples,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", required=True, choices=["vdg", "clickhouse", "victorialogs"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    ap.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    ap.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    ap.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    ap.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    ap.add_argument("--verify-all-row-ids", action="store_true",
                    help="verify every generated identity on a smoke corpus (up to 100,000 rows)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    rows = args.rows
    # Spread hit lookups across the corpus instead of timing one convenient id.
    # Every id is a real row; keep away from the cross-tier sentinel rows.
    if rows < 5:
        ap.error("--rows requires at least five fixture rows")
    if args.verify_all_row_ids and rows > MAX_IDENTITY_SMOKE_ROWS:
        ap.error(f"--verify-all-row-ids supports up to {MAX_IDENTITY_SMOKE_ROWS:,} smoke rows")
    args.trace_ids = trace_schedule(rows)
    args.trace_id = args.trace_ids[len(args.trace_ids) // 2]
    args.missing_trace_id = MISSING_TRACE
    expected_errors = sum(1 for i in range(rows) if level_for(i) == "ERROR")
    expected_recent = sum(
        1 for i in range(rows) if service_for(i) == "checkout" and level_for(i) == "ERROR"
    )

    backend = make_backend(args.system, args)
    if hasattr(backend, "setup"):
        backend.setup()

    batches = [(start, min(start + args.batch_rows, rows)) for start in range(0, rows, args.batch_rows)]
    bodies = {i: body_for(args.system, start, end, rows) for i, (start, end) in enumerate(batches)}
    total_bytes = sum(len(b) for b in bodies.values())

    print(
        f"{args.system}: ingesting {rows} rows in {len(batches)} requests "
        f"({total_bytes / 1048576:.1f} MiB, {args.threads} threads)",
        flush=True,
    )

    ingested = 0

    def send(i):
        return backend.ingest_one(bodies[i])

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
        for n in ex.map(send, range(len(batches))):
            ingested += n
    ingest_s = time.perf_counter() - t0
    # The 20M fixture holds several GiB of encoded ingest requests. Release
    # them before settling compaction and sampling query latency.
    del bodies
    print(
        f"{args.system}: acked {ingested} rows in {ingest_s:.2f}s "
        f"({ingested / ingest_s:,.0f} rows/s, {total_bytes / ingest_s / 1048576:.1f} MiB/s)",
        flush=True,
    )
    if ingested != rows:
        raise SystemExit(f"{args.system}: acked {ingested}, expected {rows}")

    # Visibility is reported separately from acknowledgement: the three systems
    # have different durability/visibility policies.
    vis_start = time.monotonic()
    deadline = vis_start + 120
    visible = -1
    while time.monotonic() < deadline:
        try:
            visible = backend.validate()
        except Exception as exc:  # noqa: BLE001 - printed, then retried until deadline
            print(f"{args.system}: visibility check failed: {exc}", flush=True)
        if visible == rows:
            break
        time.sleep(1)
    visibility_s = time.monotonic() - vis_start
    print(f"{args.system}: visible={visible} after {visibility_s:.1f}s", flush=True)
    if visible != rows:
        raise SystemExit(f"{args.system}: visible {visible}, expected {rows}")

    backend.settle()
    # Let background merge work fall out of the measurement window. Verdigris has
    # already compacted (run.sh invokes `vdg compact`), ClickHouse has run
    # OPTIMIZE, and VictoriaLogs merges asynchronously.
    time.sleep(3)

    # Correctness gate: all three must agree on the exact result cardinalities
    # before any latency number is trusted.
    recent_count = backend.count_recent()
    if recent_count != expected_recent:
        raise SystemExit(
            f"{args.system}: recent count {recent_count}, expected {expected_recent}"
        )
    search_expected = expected_search(rows)
    search_rows = backend.search_recent()
    validate_search(search_rows, search_expected)
    trace_rows = backend.trace_lookup(args.trace_id)
    validate_trace(args.trace_id, trace_rows, rows)
    cross_rows = backend.trace_lookup(CROSS_TRACE)
    validate_trace(CROSS_TRACE, cross_rows, rows)
    missing_rows = backend.trace_lookup(args.missing_trace_id)
    validate_trace(args.missing_trace_id, missing_rows, rows)
    tier_rows = {}
    if args.system == "vdg":
        for tier in ("hot", "warm", "cold"):
            tier_rows[tier] = len(backend.trace_lookup_tiered(CROSS_TRACE, [tier]))
            if tier_rows[tier] != 1:
                raise SystemExit(
                    f"vdg: tier-scoped cross trace {tier} returned {tier_rows[tier]}, expected 1"
                )
        if len(backend.trace_lookup_tiered(CROSS_TRACE, [])) != len(CROSS_TRACE_ROWS):
            raise SystemExit("vdg: all-tier cross trace lost a row")

    buckets = backend.aggregate_minute()
    total = sum(int(r["total"]) for r in buckets)
    errors = sum(int(r.get("errors", 0) or 0) for r in buckets)
    if total != rows or errors != expected_errors:
        raise SystemExit(
            f"{args.system}: aggregate total={total} errors={errors}, "
            f"expected {rows}/{expected_errors}"
        )
    bucket_expected = expected_buckets(rows)
    validate_aggregation(buckets, bucket_expected)
    print(
        f"{args.system}: equivalence OK (recent={recent_count}, "
        f"buckets={len(buckets)}, total={total}, errors={errors})",
        flush=True,
    )

    trace_cursor = [0]
    trace_engine_ms = []
    trace_manifest_ms = []
    trace_plan_ms = []

    def next_trace():
        tid = args.trace_ids[trace_cursor[0] % len(args.trace_ids)]
        trace_cursor[0] += 1
        rows = backend.trace_lookup(tid)
        if args.system == "vdg":
            trace_engine_ms.append(backend.last_stats.get("elapsedMs"))
            trace_manifest_ms.append(backend.last_stats.get("manifestMs"))
            trace_plan_ms.append(backend.last_stats.get("planMs"))
        return rows

    cache_before = backend.cache_stats() if args.system == "vdg" else {}
    q_search = timed(backend.search_recent, args.iterations, args.warmup,
                     lambda got: validate_search(got, search_expected))
    q_trace = timed(next_trace, args.iterations, args.warmup,
                    lambda got: validate_trace(args.trace_ids[(trace_cursor[0] - 1) % len(args.trace_ids)], got, rows))
    q_trace_miss = timed(
        lambda: backend.trace_lookup(args.missing_trace_id), args.iterations, args.warmup,
        lambda got: validate_trace(args.missing_trace_id, got, rows),
    )
    q_trace_cross = timed(
        lambda: backend.trace_lookup(CROSS_TRACE), args.iterations, args.warmup,
        lambda got: validate_trace(CROSS_TRACE, got, rows),
    )
    q_aggregate = timed(backend.aggregate_minute, args.iterations, args.warmup,
                        lambda got: validate_aggregation(got, bucket_expected))
    cache_after = backend.cache_stats() if args.system == "vdg" else {}

    results = {
        "system": args.system,
        "corpus": {
            "rows": rows,
            "raw_bytes": total_bytes,
            "base_ts_ms": BASE_TS,
            "window_ms": WINDOW_MS,
            "trace_id": args.trace_id,
            "trace_ids": args.trace_ids,
            "cross_trace": CROSS_TRACE,
            "missing_trace": args.missing_trace_id,
        },
        "ingest": {
            "elapsed_s": ingest_s,
            "rows_per_s": ingested / ingest_s,
            "mib_per_s": total_bytes / ingest_s / 1048576,
            "visibility_s": visibility_s,
        },
        "equivalence": {
            "recent_count": recent_count,
            "search_rows": len(search_rows),
            "trace_rows": len(trace_rows),
            "cross_trace_rows": len(cross_rows),
            "missing_trace_rows": len(missing_rows),
            "tier_rows": tier_rows,
            "buckets": len(buckets),
            "total": total,
            "errors": errors,
        },
        "trace_phase_ms": {
            "engine": trace_engine_ms,
            "manifest": trace_manifest_ms,
            "plan": trace_plan_ms,
        },
        "query_cache": {
            "before": cache_before,
            "after": cache_after,
            "metadata_hits_delta": (
                int(cache_after.get("metadataHits", 0))
                - int(cache_before.get("metadataHits", 0))
                if cache_before and cache_after
                else None
            ),
        },
        "queries": {
            "recent_service_search": q_search,
            "trace_lookup": q_trace,
            "trace_lookup_miss": q_trace_miss,
            "trace_lookup_cross_tier": q_trace_cross,
            "minute_aggregation": q_aggregate,
        },
    }
    if args.verify_all_row_ids:
        # Run outside the measured workloads and audit/cache counter windows.
        validate_row_identity(backend.fixture_messages(rows + 1), rows)
        results["equivalence"]["verified_row_identities"] = rows
        print(f"{args.system}: verified all {rows} row identities", flush=True)
    backend.close()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results["queries"], indent=2), flush=True)


if __name__ == "__main__":
    main()
