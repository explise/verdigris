#!/usr/bin/env python3
"""Snapshot or verify batch-level ingest durability against a disposable fixture."""
import argparse
import json
import re
import urllib.error
import urllib.request
from pathlib import Path


SNAPSHOT_CHUNK_ROWS = 250_000


def query_rows(url, sql):
    request = urllib.request.Request(url.rstrip("/") + "/v1/query",
                                     data=json.dumps({"sql": sql, "histogram": False}).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            return json.load(response)["rows"]
    except urllib.error.HTTPError as error:
        detail = error.read(512).decode(errors="replace")
        raise RuntimeError(f"recovery query HTTP {error.code}: {detail}") from error


def batch_counts(rows):
    counts = {}
    for row in rows:
        marker = row["batch_id"]
        if not isinstance(marker, str) or not re.fullmatch(r"sustained-[0-9]{12}", marker) or marker in counts:
            raise ValueError("expected only unique sustained batch markers in the fixture")
        counts[marker] = int(row["n"])
    return counts


def batch_windows(counts):
    """Keep distinct-offset checks around 250k rows without splitting a batch."""
    window = {}
    window_rows = 0
    for marker, count in sorted(counts.items()):
        if window and window_rows + count > SNAPSHOT_CHUNK_ROWS:
            yield window
            window, window_rows = {}, 0
        window[marker] = count
        window_rows += count
    if window:
        yield window


def snapshot(url, table):
    # Count every prefix first, including unexpected or malformed ones, then
    # check distinct offsets in disjoint windows to stay within query memory.
    counts = batch_counts(query_rows(
        url, f"SELECT substr(trace_id, 1, 22) AS batch_id, count(*) AS n FROM {table} GROUP BY 1"
    ))
    for window in batch_windows(counts):
        first, last = min(window), max(window)
        rows = query_rows(url, f"""
            WITH fixture AS (
                SELECT trace_id, substr(trace_id, 1, 22) AS batch_id,
                       try_cast(substr(trace_id, 24) AS bigint) AS row_index
                FROM {table} WHERE trace_id >= '{first}' AND trace_id < '{last}~'
            )
            SELECT batch_id, count(*) AS n, count(DISTINCT row_index) AS unique_offsets,
                   min(row_index) AS first_offset, max(row_index) AS last_offset,
                   count(*) FILTER (
                       WHERE trace_id = concat(batch_id, '-',
                           lpad(cast(row_index AS varchar),
                                greatest(6, character_length(cast(row_index AS varchar))), '0'))
                   ) AS canonical_ids
            FROM fixture GROUP BY batch_id
        """)
        if batch_counts(rows) != window:
            raise ValueError(f"incomplete or changed recovery window {first} through {last}")
        for row in rows:
            count = int(row["n"])
            if (row["unique_offsets"], row["first_offset"], row["last_offset"], row["canonical_ids"]) != (
                count, 0, count - 1, count
            ):
                raise ValueError(f"missing, duplicated, or invalid record IDs in {row['batch_id']}")
    return counts

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
