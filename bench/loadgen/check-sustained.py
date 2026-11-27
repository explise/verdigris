#!/usr/bin/env python3
"""Snapshot or verify batch-level ingest durability against a disposable fixture."""
import argparse
import json
import re
import urllib.request
from pathlib import Path


def snapshot(url, table):
    sql = f"SELECT substr(trace_id, 1, 22) AS batch_id, count(*) AS n FROM {table} GROUP BY 1"
    request = urllib.request.Request(url.rstrip("/") + "/v1/query",
                                     data=json.dumps({"sql": sql, "histogram": False}).encode(),
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=300) as response:
        rows = json.load(response)["rows"]
    stored = {}
    for row in rows:
        marker = row["batch_id"]
        if not isinstance(marker, str) or not re.fullmatch(r"sustained-[0-9]{12}", marker) or marker in stored:
            raise ValueError("expected only unique sustained batch markers in the fixture")
        stored[marker] = int(row["n"])
    return stored


def reconcile(report, before, after):
    acked = {f"sustained-{row['id']:012}": row["acked_rows"] for row in report["outcomes"]
             if row["acked_rows"] is not None and row["ack_ms"] is not None}
    expected_ids = list(range(report["planned_batches"]))
    if [row["id"] for row in report["outcomes"]] != expected_ids or report["generation_error"]:
        raise ValueError("incomplete offered-arrival ledger")
    if sum(acked.values()) != report["acked_rows"]:
        raise ValueError("inconsistent acknowledgement ledger")
    wrong = {}
    extra = {}
    ambiguous = {f"sustained-{row['id']:012}" for row in report["outcomes"]
                 if row["error"] in ("transport", "invalid_ack")}
    for phase, stored in (("before", before), ("after", after)):
        wrong[phase] = {key: {"acked": count, "visible": stored.get(key, 0)}
                        for key, count in acked.items() if stored.get(key) != count}
        extra[phase] = {key: count for key, count in stored.items() if key not in acked}
    results = {"acked_batches": len(acked), "acked_rows": sum(acked.values()),
               "acknowledged_batch_mismatches": wrong, "unacknowledged_batches": extra,
               "ambiguous_transport_or_ack_batches": sorted(ambiguous),
               "unexplained_batch_ids": sorted((set(extra["before"]) | set(extra["after"])) - ambiguous)}
    results["passed"] = bool(acked) and not any(wrong.values()) and not results["unexplained_batch_ids"]
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    capture = modes.add_parser("snapshot")
    capture.add_argument("--url", required=True)
    capture.add_argument("--table", required=True)
    capture.add_argument("--out", type=Path, required=True)
    check = modes.add_parser("verify")
    for arg in ("run", "before", "after", "out"):
        check.add_argument(f"--{arg}", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "snapshot":
        if not re.fullmatch(r"[a-zA-Z][a-zA-Z_0-9]*", args.table):
            parser.error("invalid benchmark table name")
        result = snapshot(args.url, args.table)
        status = 0
    else:
        result = reconcile(*(json.loads(p.read_text()) for p in (args.run, args.before, args.after)))
        status = 0 if result["passed"] else 1
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
