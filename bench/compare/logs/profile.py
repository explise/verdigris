#!/usr/bin/env python3
"""Measure an already seeded Verdigris deployment with audit enabled."""

import argparse
import json
import math
import statistics
import time
from pathlib import Path

from compare import CROSS_TRACE, Vdg

SHAPES = ("trace_hit", "trace_miss", "trace_cross_tier", "recent_search", "minute_aggregation")


def run_one(backend, shape, index, corpus, mode):
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

    if shape in ("trace_hit", "trace_miss", "trace_cross_tier"):
        expected = {"trace_hit": 1, "trace_miss": 0, "trace_cross_tier": 3}[shape]
        if len(rows) != expected or any(row["trace_id"] != trace for row in rows):
            raise RuntimeError(f"{shape} returned {len(rows)} rows for {trace}, expected {expected}")
    elif shape == "recent_search":
        if any(row["service"] != "checkout" or row["level"] != "ERROR" for row in rows) or not rows:
            raise RuntimeError("recent search returned an unexpected page")
    elif sum(row["total"] for row in rows) != corpus["rows"]:
        raise RuntimeError("aggregate did not cover the corpus")

    return {
        "query_index": index,
        "trace_id": trace,
        "elapsed_ms": elapsed_ms,
        "rows": len(rows),
        "audit_ms": backend.last_stats.get("auditMs"),
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
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--seed-report", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--first", choices=SHAPES)
    args = parser.parse_args()
    if args.iterations < 1 or args.warmup < 0:
        parser.error("iterations must be positive and warmup cannot be negative")

    corpus = json.loads(args.seed_report.read_text())["corpus"]
    backend = Vdg(args)
    out = {"corpus": corpus, "mode": "first_after_restart" if args.first else "warm", "queries": {}}
    try:
        if args.first:
            out["queries"][args.first] = run_one(backend, args.first, 0, corpus, "1")
        else:
            for shape in SHAPES:
                for index in range(args.warmup):
                    run_one(backend, shape, index, corpus, "1")
                samples = {"on": [], "off": []}
                for index in range(args.iterations):
                    modes = ("on", "off") if index % 2 == 0 else ("off", "on")
                    for mode in modes:
                        samples[mode].append(
                            run_one(backend, shape, index, corpus, "1" if mode == "on" else None)
                        )
                out["queries"][shape] = {mode: summarize(samples[mode]) for mode in samples}

            out["representative_plan"] = run_one(backend, "trace_hit", 0, corpus, "plan")["profile"]
            status, _, data = backend.client.request("GET", "/v1/metrics")
            if status != 200:
                raise RuntimeError(f"query metrics failed with HTTP {status}")
            out["audit_counters_after"] = json.loads(data)["audit"]
    finally:
        backend.close()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main()
