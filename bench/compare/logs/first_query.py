#!/usr/bin/env python3
"""Run and validate one query shape after a process restart."""
import argparse
import json
import time

import compare

SHAPES = ("trace", "trace-miss", "trace-cross-tier", "search", "aggregate")


def run_shape(args):
    backend = compare.make_backend(args.system, args)
    traces = compare.trace_schedule(args.rows)
    trace_by_shape = {
        "trace": traces[len(traces) // 2],
        "trace-miss": compare.MISSING_TRACE,
        "trace-cross-tier": compare.CROSS_TRACE,
    }
    try:
        start = time.perf_counter()
        if args.shape in trace_by_shape:
            rows = backend.trace_lookup(trace_by_shape[args.shape])
        elif args.shape == "search":
            rows = backend.search_recent()
        else:
            rows = backend.aggregate_minute()
        ms = (time.perf_counter() - start) * 1000
    finally:
        backend.close()

    if args.shape in trace_by_shape:
        compare.validate_trace(trace_by_shape[args.shape], rows, args.rows)
    elif args.shape == "search":
        compare.validate_search(rows, compare.expected_search(args.rows))
    else:
        compare.validate_aggregation(rows, compare.expected_buckets(args.rows))
    return {"status": 200, "ms": ms, "rows": len(rows)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", required=True, choices=["vdg", "clickhouse", "victorialogs"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--shape", required=True, choices=SHAPES)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.rows < 5:
        ap.error("--rows requires at least five fixture rows")
    result = json.dumps({args.shape: run_shape(args)}, indent=2)
    with open(args.out, "w") as out:
        out.write(result + "\n")
    print(result)


if __name__ == "__main__":
    main()
