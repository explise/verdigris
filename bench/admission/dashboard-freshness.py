#!/usr/bin/env python3
"""Check dashboard freshness around an acknowledged write and its compaction."""
import argparse
import concurrent.futures
import json
import math
from pathlib import Path
import time

from mixed import SERVICES, WINDOW_MS, post, record, request


def verify(port, mutating, observations, expect_cache_hits):
    rows = mutating["rows"]
    stamp = mutating["base_ts_ms"]
    probes = mutating["committed_probe_rows"]
    expected = rows + len(probes)
    bucket_seconds = max(1, ((rows - 1) * WINDOW_MS // rows) // 60_000)

    def dashboard(label, expected_rows):
        status, body, _ = request(port, "GET", "/v1/metrics")
        observation = {"label": label, "status": status, "expected_rows": expected_rows}
        observations.append(observation)
        if status != 200:
            observation["error"] = body[:200].decode(errors="replace")
            raise RuntimeError(f"{label}: dashboard HTTP {status}")
        payload = json.loads(body)
        rates = payload["ingestRate"]
        actual = round(math.fsum(rates) * bucket_seconds)
        compaction = payload["compaction"]
        observation.update(rows=actual, compaction=compaction,
                           snapshot_hits=payload["dashboardSnapshotHits"])
        if (not rates or len(rates) != len(payload["errorRate"])
                or {svc["name"] for svc in payload["volumeByService"]} != set(SERVICES)
                or actual != expected_rows):
            raise RuntimeError(f"{label}: dashboard count {actual}, expected {expected_rows}")
        return compaction

    def trace():
        payload = json.dumps({"sql": "trace_id:0000000000000000", "histogram": False}).encode()
        status, data = post(port, "/v1/query", payload)
        if status != 200 or [r["trace_id"] for r in json.loads(data)["rows"]] != ["0000000000000000"]:
            raise RuntimeError(f"concurrent exact trace failed: HTTP {status}")

    def read(label, expected_rows):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            check = pool.submit(trace)
            compaction = dashboard(label, expected_rows)
            check.result()
        return compaction

    def settled(compaction):
        return compaction["pendingFiles"] == 0

    before = read("after-load", expected)
    deadline = time.monotonic() + 60
    while not settled(before):
        if time.monotonic() >= deadline:
            raise RuntimeError("previous compaction did not settle")
        time.sleep(2)
        before = read("drain-previous-compaction", expected)
    read("prime-before-write", expected)
    primed_hits = observations[-1]["snapshot_hits"]
    before = read("warm-before-write", expected)
    if expect_cache_hits and observations[-1]["snapshot_hits"] <= primed_hits:
        raise RuntimeError("cache-enabled arm did not reuse the current dashboard snapshot")

    row = record(0, rows)
    row.update(ts_millis=stamp + WINDOW_MS // 2, trace_id="ffffffffffffffff",
               message="dashboard freshness after sustained mix")
    status, data = post(port, "/v1/ingest", (json.dumps(row) + "\n").encode(),
                        {"content-type": "application/x-ndjson"})
    if status != 200 or json.loads(data).get("ingested") != 1:
        raise RuntimeError(f"freshness write was not acknowledged: HTTP {status}")

    after = read("post-acknowledgement", expected + 1)
    deadline = time.monotonic() + 60
    while not (settled(after) and after["runsTotal"] > before["runsTotal"]
               and after["filesMergedTotal"] > before["filesMergedTotal"]):
        if time.monotonic() >= deadline:
            raise RuntimeError("the acknowledged write did not trigger an observed compaction")
        time.sleep(2)
        after = read("wait-for-compaction", expected + 1)
    read("after-compaction", expected + 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--mutating", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--expect-cache-hits", action="store_true")
    args = parser.parse_args()
    mutating = json.loads(args.mutating.read_text())
    observations = []
    report = {"decision": "unverified", "observations": observations}
    try:
        verify(args.port, mutating, observations, args.expect_cache_hits)
        report["decision"] = "pass"
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        args.out.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
