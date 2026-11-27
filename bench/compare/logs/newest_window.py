"""Screen a newest-window hint against a full-scope recent-search query."""
import itertools
import json
import time

from compare import (
    RECENT_SEARCH_SQL, expected_row, level_for, service_for, ts_for, validate_search,
)

WINDOW_MS = 120_000
LIMIT = 200


def add_filter(sql, predicate):
    query, order = sql.rsplit(" ORDER BY ", 1)
    return f"{query} AND ({predicate}) ORDER BY {order}"


def fixtures(rows):
    # The anchor is the latest seeded timestamp. It is independent of the
    # timestamps of the qualifying rows and remains fixed for this deployment.
    upper = ts_for(rows - 1, rows)
    lower = upper - WINDOW_MS
    # Model a service whose newest data stops well before the table's upper
    # bound. Keep the disjunction so the SQL has no global upper-bound clause.
    stop = upper - 300_000
    sparse = add_filter(RECENT_SEARCH_SQL, f"ts < to_timestamp_millis({stop}) "
                        "OR message = 'benchmark sparse-fixture sentinel'")
    indices = (i for i in range(rows - 1, -1, -1)
               if ts_for(i, rows) < stop and level_for(i) == "ERROR" and service_for(i) == "checkout")
    sparse_expected = [expected_row(i, rows) for i in itertools.islice(indices, LIMIT)]
    return lower, sparse, sparse_expected


def compare(backend, rows, dense_expected, iterations, warmup, percentiles):
    lower, sparse_sql, sparse_expected = fixtures(rows)
    cases = {"dense": (RECENT_SEARCH_SQL, dense_expected), "sparse": (sparse_sql, sparse_expected)}
    comparisons = {}
    acknowledgements = 0

    def get_json(method, path, data=None):
        status, _, payload = backend.client.request(
            method, path, data, {"content-type": "application/json"}
        )
        if status != 200:
            raise RuntimeError(f"{method} {path}: HTTP {status} {payload[:200]!r}")
        return json.loads(payload)

    def quote(sql):
        return get_json("POST", "/v1/query/estimate", json.dumps({"sql": sql, "tiers": []}).encode())

    audit_before = get_json("GET", "/v1/metrics")["audit"]

    for case, (full_sql, expected) in cases.items():
        if len(expected) != LIMIT or len({row[0] for row in expected}) != LIMIT:
            raise RuntimeError(f"{case}: fixture must have {LIMIT} distinct result timestamps")
        expected_first = [row for row in expected if row[0] >= lower]
        if (len(expected_first) == LIMIT) != (case == "dense"):
            raise RuntimeError(f"{case}: fixture did not exercise the intended window or fallback path")
        window_sql = add_filter(full_sql, f"ts >= to_timestamp_millis({lower})")
        approved = quote(full_sql)
        if quote(window_sql) != approved:
            raise RuntimeError(f"{case}: window changed the approved scope")
        arms = {arm: {mode: [] for mode in ("off", "on")} for arm in ("full", "window")}

        def run(arm, mode):
            nonlocal acknowledgements
            backend.profile_mode = "1" if mode == "on" else None
            request_start = time.perf_counter_ns()
            stages = {}

            def query(stage, sql):
                start = time.perf_counter_ns()
                result = backend._query(sql)
                elapsed_ms = (time.perf_counter_ns() - start) / 1e6
                stats = backend.last_stats
                if (stats["files"], stats["scannedBytes"]) != (approved["filesTouched"], approved["scanBytes"]):
                    raise RuntimeError(f"{case}/{stage}: scan and cost quote disagree")
                if stats.get("auditMs") is None:
                    raise RuntimeError(f"{case}/{stage}: audit acknowledgement timing missing")
                stages[stage] = {"elapsed_ms": elapsed_ms, "rows": len(result), "audit_ms": stats["auditMs"],
                                 "profile": backend.last_profile}
                return result

            if arm == "window":
                first = query("window", window_sql)
                result = first if len(first) == LIMIT else query("fallback", full_sql)
            else:
                result = query("full", full_sql)
            elapsed_ms = (time.perf_counter_ns() - request_start) / 1e6
            validate_search(result, expected)
            if arm == "window":
                validate_search(first, expected_first)
            acknowledgements += len(stages)
            return {"elapsed_ms": elapsed_ms, "stages": stages}

        for i in range(warmup + iterations):
            for mode in ("off", "on") if i % 2 == 0 else ("on", "off"):
                for arm in ("full", "window") if (i % 2 == 0) == (mode == "off") else ("window", "full"):
                    sample = run(arm, mode)
                    if i >= warmup:
                        arms[arm][mode].append(sample)

        comparisons[case] = {
            "quote": approved,
            "expected_first_rows": len(expected_first),
            "arms": {arm: {mode: {"elapsed_ms": percentiles([s["elapsed_ms"] for s in samples]),
                                 "samples": samples}
                           for mode, samples in modes.items()} for arm, modes in arms.items()},
        }

    audit_after = get_json("GET", "/v1/metrics")["audit"]
    if (audit_after["ackedTotal"] - audit_before["ackedTotal"] != acknowledgements
        or any(audit_after[k] for k in ("failedTotal", "timedOutTotal", "saturatedTotal"))):
        raise RuntimeError(f"newest-window audit was unhealthy: {audit_before=} {audit_after=}")
    return {"lower_ms": lower, "window_ms": WINDOW_MS, "cases": comparisons,
            "audit_before": audit_before, "audit_after": audit_after}
