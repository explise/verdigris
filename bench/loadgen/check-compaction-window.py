#!/usr/bin/env python3
"""Read out a bounded compaction-deferral control with exact recovery."""
import argparse
import json
from pathlib import Path
import re

IO = re.compile(r'io="(ingest_put|compact_get|compact_put)" bytes=(\d+)')
ANSI = re.compile(r'\x1b\[[0-9;]*m')
KINDS = ("ingest_put", "compact_get", "compact_put")
COMPACT_PASSES = re.compile(r'^verdigris_ingest_phase_observations_total\{phase="compact_pass"\} (\d+)$', re.M)


def io_totals(path):
    events = IO.findall(ANSI.sub("", path.read_text()))
    return {kind: sum(int(size) for name, size in events if name == kind) for kind in KINDS}


def read(arm):
    def load(name):
        return json.loads((arm / name).read_text())

    run, recovery, drain = (load(name) for name in ("run.json", "recovery.json", "drain.json"))
    online = io_totals(arm / "online.log")
    offline = io_totals(arm / "drain.log")
    if not online["ingest_put"] or not (online["compact_put"] + offline["compact_put"]):
        raise ValueError(f"{arm.name}: incomplete recorded Parquet I/O")
    if offline["ingest_put"]:
        raise ValueError(f"{arm.name}: ingestion ran while the offline compactor owned the table")
    if not recovery["passed"] or recovery["acked_rows"] != run["acked_rows"]:
        raise ValueError(f"{arm.name}: exact durable recovery failed")
    if load("before.json") != load("after.json"):
        raise ValueError(f"{arm.name}: rows changed through the drain and restart")
    passes = int(COMPACT_PASSES.search((arm / "phases-final.prom").read_text())[1])
    files = [v["compaction"]["smallFiles"] + v["compaction"]["compacted"]
             for v in run["storage_samples"] if v.get("compaction")]
    if not files:
        raise ValueError(f"{arm.name}: no active-file measurements")
    duration = run["duration_s"]
    last_ack_s = max(row["ack_ms"] for row in run["outcomes"] if row["ack_ms"] is not None) / 1000
    return {
        "compaction_passes_during_load": passes,
        "drain": drain,
        "rate_in_window": run["acked_rows_per_s"],
        "rate_including_drain": run["acked_rows"] / (max(duration, last_ack_s) + drain["elapsed_s"]),
        "acknowledged_rows": run["acked_rows"],
        "visibility_p95_ms": run["visibility_p95_ms"],
        "peak_sampled_active_files": max(files),
        "online_parquet_io": online,
        "offline_parquet_io": offline,
        "parquet_write_amplification": 1 + (online["compact_put"] + offline["compact_put"]) / online["ingest_put"],
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    args = p.parse_args()
    normal, deferred = (read(args.root / arm) for arm in ("normal", "deferred"))
    if normal["compaction_passes_during_load"] <= 0 or deferred["compaction_passes_during_load"] != 0:
        raise ValueError("the selected treatment did not isolate compaction from the load window")
    gates = {
        "gain_at_least_25_percent": deferred["rate_in_window"] >= normal["rate_in_window"] * 1.25,
        "both_drains_complete": normal["drain"]["completed"] and deferred["drain"]["completed"],
        "write_amplification_within_10_percent": (
            deferred["parquet_write_amplification"] <= normal["parquet_write_amplification"] * 1.10
        ),
        "peak_active_files_within_2x": deferred["peak_sampled_active_files"] <= normal["peak_sampled_active_files"] * 2,
    }
    print(json.dumps({"normal": normal, "deferred": deferred, "gates": gates,
                      "decision": "investigate" if all(gates.values()) else "no-go"}, indent=2))


if __name__ == "__main__":
    main()
