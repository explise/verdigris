#!/usr/bin/env python3
"""Measure an already seeded Verdigris deployment with audit enabled."""

import argparse
import json
import math
import statistics
import time
from pathlib import Path

from compare import (
    BASE_TS, CROSS_TRACE, RECENT_SEARCH_SQL, Vdg, expected_buckets, expected_search,
    row_ts_ms, validate_aggregation, validate_search, validate_trace,
)

SHAPES = ("trace_hit", "trace_miss", "trace_cross_tier", "recent_search", "minute_aggregation")


def run_one(backend, shape, index, corpus, mode, expected):
    backend.profile_mode = mode
    trace = None
    if shape == "trace_hit":
        trace = corpus["trace_ids"][index % len(corpus["trace_ids"])]
    elif shape == "trace_miss":
        trace = f"{(1 << 64) - 1 - index:016x}"
    elif shape == "trace_cross_tier":
        trace = CROSS_TRACE

    start = time.perf_counter_ns()
    if trace is not None:
        rows = backend.trace_lookup(trace)
    elif shape == "recent_search":
        rows = backend.search_recent()
    else:
        rows = backend.aggregate_minute()
    elapsed_ms = (time.perf_counter_ns() - start) / 1e6

    if shape == "trace_miss":
        if rows:
            raise RuntimeError(f"missing trace {trace} returned {len(rows)} rows")
    elif trace is not None:
        validate_trace(trace, rows, corpus["rows"])
    elif shape == "recent_search":
        validate_search(rows, expected[shape])
    else:
        validate_aggregation(rows, expected[shape])

    return {
        "query_index": index,
        "trace_id": trace,
        "elapsed_ms": elapsed_ms,
        "rows": len(rows),
        "audit_ms": backend.last_stats.get("auditMs"),
        "planned_files": backend.last_stats.get("files"),
        "planned_bytes": backend.last_stats.get("scannedBytes"),
        "profile": backend.last_profile,
    }


def percentiles(values):
    values = sorted(values)
    return {
        "p50": statistics.median(values),
        "p95": values[math.ceil(0.95 * len(values)) - 1],
    }


def summarize(samples):
    summary = {
        "elapsed_ms": percentiles([sample["elapsed_ms"] for sample in samples]),
        "samples": samples,
    }
    profiles = [sample["profile"] for sample in samples if sample["profile"]]
    if profiles:
        summary["phases_us"] = {
            phase: percentiles([p["phases_us"][phase] for p in profiles])
            for phase in profiles[0]["phases_us"]
        }
        read_keys = (
            "gets", "heads", "range_batches", "ranges", "result_bytes",
            "read_wait_sum_us", "read_wait_max_us", "read_wait_critical_us",
            "read_max_concurrent",
        )
        summary["object_reads"] = {
            key: percentiles([p["object_reads"][key] for p in profiles])
            for key in read_keys
        }
    return summary


def compare_search(backend, expected, iterations, warmup):
    """Compare equivalent SQL and search requests on one settled corpus."""
    inputs = (RECENT_SEARCH_SQL, "service:checkout level:ERROR")

    def estimates():
        quotes = []
        for sql in inputs:
            args = json.dumps({"sql": sql, "tiers": []}).encode()
            status, _, data = backend.client.request(
                "POST", "/v1/query/estimate", args, {"content-type": "application/json"}
            )
            if status != 200:
                raise RuntimeError(f"search estimate HTTP {status}: {data[:300]!r}")
            quotes.append(json.loads(data))
        return quotes

    def query(index, mode):
        backend.profile_mode = mode
        start = time.perf_counter_ns()
        rows = backend._query(inputs[index])
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        validate_search(rows, expected)
        stats = backend.last_stats
        quote = quotes[index]
        if (stats["files"], stats["scannedBytes"]) != (quote["filesTouched"], quote["scanBytes"]):
            raise RuntimeError("search estimate and execution disagree on file count or bytes")
        return {"elapsed_ms": elapsed_ms, "rows": len(rows), "profile": backend.last_profile}

    quotes = estimates()
    samples = [[], []]
    for i in range(warmup + iterations):
        for index in (0, 1) if i % 2 == 0 else (1, 0):
            result = query(index, "1")
            if i >= warmup:
                samples[index].append(result)
    comparison = {
        name: {"estimate": quotes[i], "samples": summarize(samples[i]), "plan": query(i, "plan")}
        for i, name in enumerate(("sql", "dsl"))
    }
    if estimates() != quotes:
        raise RuntimeError("search estimates changed during the comparison")
    return comparison


def cutoff_queries(cutoff_ms):
    projection, order = RECENT_SEARCH_SQL.rsplit(" ORDER BY ", 1)
    _, table_and_filter = projection.split(" FROM ", 1)
    narrow = f"SELECT ts FROM {table_and_filter} ORDER BY {order}"
    bounded = f"{projection} AND ts >= to_timestamp_millis({cutoff_ms}) ORDER BY {order}"
    return narrow, bounded


def compare_cutoff(backend, expected, iterations, warmup):
    """Measure a fixed-fixture cutoff and a staged cutoff with two audited calls."""
    if len(expected) != 200:
        raise ValueError("cutoff benchmark requires a full 200-row page")
    expected_ts = [row[0] for row in expected]
    narrow_sql, bounded_sql = cutoff_queries(expected_ts[-1])

    def quote(sql):
        body = json.dumps({"sql": sql, "tiers": []}).encode()
        status, _, data = backend.client.request(
            "POST", "/v1/query/estimate", body, {"content-type": "application/json"}
        )
        if status != 200:
            raise RuntimeError(f"cutoff estimate HTTP {status}: {data[:300]!r}")
        return json.loads(data)

    queries = {"baseline": RECENT_SEARCH_SQL, "narrow": narrow_sql, "bounded": bounded_sql}
    quotes = {name: quote(sql) for name, sql in queries.items()}
    scope = (quotes["baseline"]["filesTouched"], quotes["baseline"]["scanBytes"])
    if any((q["filesTouched"], q["scanBytes"]) != scope for q in quotes.values()):
        raise RuntimeError("cutoff variants changed the approved file and byte scope")

    def query(sql):
        start = time.perf_counter_ns()
        rows = backend._query(sql)
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        stats = backend.last_stats
        if (stats["files"], stats["scannedBytes"]) != scope:
            raise RuntimeError("cutoff execution and estimate disagree on file or byte scope")
        return rows, {
            "elapsed_ms": elapsed_ms,
            "audit_ms": stats.get("auditMs"),
            "profile": backend.last_profile,
        }

    def run(name, mode):
        backend.profile_mode = mode
        start = time.perf_counter_ns()
        stages = {}
        if name == "staged":
            narrow_rows, stages["narrow"] = query(narrow_sql)
            if len(narrow_rows) != 200:
                raise RuntimeError("cutoff query returned a short page")
            cutoff = row_ts_ms(narrow_rows[-1])
            _, sql = cutoff_queries(cutoff)
            rows, stages["bounded"] = query(sql)
        else:
            sql = RECENT_SEARCH_SQL if name == "baseline" else bounded_sql
            rows, stages[name] = query(sql)
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        validate_search(rows, expected)
        if name == "staged" and [row_ts_ms(row) for row in narrow_rows] != expected_ts:
            raise RuntimeError("narrow cutoff query returned different timestamps or order")
        return {"elapsed_ms": elapsed_ms, "rows": len(rows), "stages": stages}

    names = ("baseline", "staged", "oracle")
    samples = {name: {"off": [], "on": []} for name in names}
    for i in range(warmup + iterations):
        order = names[i % 3:] + names[:i % 3]
        for mode in ("off", "on") if i % 2 == 0 else ("on", "off"):
            for name in order:
                sample = run(name, "1" if mode == "on" else None)
                if i >= warmup:
                    samples[name][mode].append(sample)

    status, _, data = backend.client.request("GET", "/v1/metrics")
    if status != 200:
        raise RuntimeError(f"query metrics failed with HTTP {status}")
    audit = json.loads(data)["audit"]
    if any(audit[k] for k in ("failedTotal", "timedOutTotal", "saturatedTotal")):
        raise RuntimeError(f"cutoff audit was unhealthy: {audit}")
    if {name: quote(sql) for name, sql in queries.items()} != quotes:
        raise RuntimeError("cutoff estimates changed during the comparison")

    return {
        "estimate_by_query": quotes,
        "audit": audit,
        "fixture_cutoff_ms": expected_ts[-1],
        "arms": {
            name: {
                mode: {
                    "elapsed_ms": percentiles([sample["elapsed_ms"] for sample in cohort]),
                    "samples": cohort,
                }
                for mode, cohort in modes.items()
            }
            for name, modes in samples.items()
        },
        "plans": {name: run(name, "plan") for name in names},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--seed-report", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--first", choices=SHAPES)
    mode.add_argument("--compare-search", action="store_true")
    mode.add_argument("--compare-cutoff", action="store_true")
    args = parser.parse_args()
    if args.iterations < 1 or args.warmup < 0:
        parser.error("iterations must be positive and warmup cannot be negative")

    corpus = json.loads(args.seed_report.read_text())["corpus"]
    if corpus["base_ts_ms"] != BASE_TS:
        parser.error("CORPUS_TS_MS must match the seed report's corpus timestamp")
    expected = {"recent_search": expected_search(corpus["rows"])}
    if not (args.compare_search or args.compare_cutoff) and args.first in (None, "minute_aggregation"):
        expected["minute_aggregation"] = expected_buckets(corpus["rows"])
    backend = Vdg(args)
    out = {"corpus": corpus, "mode": "first_after_restart" if args.first else "warm", "queries": {}}
    try:
        if args.compare_cutoff:
            out.pop("queries")
            out["mode"] = "search_cutoff"
            out["comparison"] = compare_cutoff(backend, expected["recent_search"], args.iterations, args.warmup)
        elif args.compare_search:
            out.pop("queries")
            out["mode"] = "search_comparison"
            out["comparison"] = compare_search(backend, expected["recent_search"], args.iterations, args.warmup)
        elif args.first:
            out["queries"][args.first] = run_one(backend, args.first, 0, corpus, "1", expected)
        else:
            for shape in SHAPES:
                for index in range(args.warmup):
                    run_one(backend, shape, index, corpus, "1", expected)
                samples = {"on": [], "off": []}
                for index in range(args.iterations):
                    modes = ("on", "off") if index % 2 == 0 else ("off", "on")
                    for mode in modes:
                        samples[mode].append(
                            run_one(
                                backend, shape, index, corpus, "1" if mode == "on" else None, expected
                            )
                        )
                out["queries"][shape] = {mode: summarize(samples[mode]) for mode in samples}

            out["representative_plan"] = run_one(backend, "trace_hit", 0, corpus, "plan", expected)["profile"]
            out["recent_search_plan"] = run_one(backend, "recent_search", 0, corpus, "plan", expected)
            status, _, data = backend.client.request("GET", "/v1/metrics")
            if status != 200:
                raise RuntimeError(f"query metrics failed with HTTP {status}")
            out["audit_counters_after"] = json.loads(data)["audit"]
    finally:
        backend.close()

    out["validated_exact_results"] = True
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main()
