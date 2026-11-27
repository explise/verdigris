"""Compare one-request disjoint timestamp continuation against full SQL."""
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
        approved = quote(full_sql)
        arms = {arm: {mode: [] for mode in ("off", "on")} for arm in ("full", "window")}

        def run(arm, mode):
            nonlocal acknowledgements
            backend.profile_mode = "1" if mode == "on" else None
            backend.recent_window = arm == "window"
            start = time.perf_counter_ns()
            result = backend._query(full_sql)
            elapsed_ms = (time.perf_counter_ns() - start) / 1e6
            stats = backend.last_stats
            profile = backend.last_profile
            if (stats["files"], stats["scannedBytes"]) != (
                approved["filesTouched"], approved["scanBytes"]
            ):
                raise RuntimeError(f"{case}/{arm}: execution changed the approved scope")
            validate_search(result, expected)
            if stats.get("auditMs") is None:
                raise RuntimeError(f"{case}/{arm}: missing durable audit timing")
            if profile:
                phases = profile["phases_us"]
                if ("recent_window_result" in phases) != (arm == "window"):
                    raise RuntimeError(f"{case}/{arm}: did not execute the intended plan: {phases}")
                continued = "recent_window_continuation" in phases
                if continued != (arm == "window" and case == "sparse"):
                    raise RuntimeError(f"{case}/{arm}: unexpected continuation: {phases}")
            acknowledgements += 1
            return {"elapsed_ms": elapsed_ms, "rows": len(result), "audit_ms": stats["auditMs"],
                    "profile": profile}

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

    backend.recent_window = False
    audit_after = get_json("GET", "/v1/metrics")["audit"]
    if (audit_after["ackedTotal"] - audit_before["ackedTotal"] != acknowledgements
        or any(audit_after[k] for k in ("failedTotal", "timedOutTotal", "saturatedTotal"))):
        raise RuntimeError(f"newest-window audit was unhealthy: {audit_before=} {audit_after=}")
    dense = comparisons["dense"]["arms"]["window"]["off"]["elapsed_ms"]["p95"]
    sparse = comparisons["sparse"]["arms"]
    sparse_ratio = sparse["window"]["off"]["elapsed_ms"]["p95"] / sparse["full"]["off"]["elapsed_ms"]["p95"]
    target = 25 if rows == 1000000 else 75
    return {"lower_ms": lower, "window_ms": WINDOW_MS, "cases": comparisons,
            "audit_before": audit_before, "audit_after": audit_after,
            "expected_audit_acks": acknowledgements,
            "dense_target_ms": target, "dense_pass": dense <= target,
            "sparse_ratio": sparse_ratio, "sparse_overhead_screen_pass": sparse_ratio <= 1.10}
