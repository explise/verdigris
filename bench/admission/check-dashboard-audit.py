#!/usr/bin/env python3
"""Verify mutating dashboard and audit gates and retain slow-worker intervals."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
NUMBER = re.compile(r"\b(\w+)=([0-9]+)\b")


def read(arm):
    def load(name):
        return json.loads((arm / name).read_text())

    steady = subprocess.run(
        [sys.executable, "-B", str(Path(__file__).with_name("check-fixed.py")), "--expect-backing", "disk", str(arm)],
        capture_output=True, text=True,
    )
    if not steady.stdout.strip():
        raise ValueError(f"{arm.name}: steady validation failed: {steady.stderr}")
    steady = json.loads(steady.stdout)
    run = load("mutating.json")
    freshness = load("freshness.json")
    before, after = (load(f"metrics-{name}.json")["audit"] for name in ("after", "mutating"))
    errors = {key: after[key] - before[key] for key in ("failedTotal", "timedOutTotal", "saturatedTotal")}
    if run["rows"] != 20_000_000 or run["duration_s"] != 60 or len(run["committed_probe_rows"]) != 5:
        raise ValueError(f"{arm.name}: mutating workload differs from the fixed fixture")
    if any(value < 0 for value in errors.values()):
        raise ValueError(f"{arm.name}: audit counters reset")
    starts_at, ends_at = run["window_start_ms"], run["window_end_ms"]
    log = ANSI.sub("", (arm / "vdg.log").read_text())

    def events(marker):
        matches = []
        for line in log.splitlines():
            if not all(part in line for part in marker):
                continue
            event = {key: int(value) for key, value in NUMBER.findall(line)}
            if event["start_ms"] <= ends_at and event["end_ms"] >= starts_at:
                matches.append(event)
        return matches

    compaction = events(("vdg_bench_compaction", "compaction pass"))
    if not compaction:
        raise ValueError(f"{arm.name}: no compaction intervals were captured during writes")
    slow = events(("vdg_bench_audit", "slow audit batch"))
    for batch in slow:
        batch["overlapped_compaction"] = any(
            stage["start_ms"] <= batch["end_ms"] and stage["end_ms"] >= batch["start_ms"]
            for stage in compaction
        )

    trace = run["trace"]
    gates = {
        "steady": steady["decision"] == "pass",
        "freshness": freshness["decision"] == "pass",
        "trace": trace["attempts"] == 660 and trace["ok"] >= 654 and trace["p95_ms"] <= 120,
        "heavy_progress": run["heavy"]["ok"] > 0 and run["heavy"]["other"] == 0,
        "audit": not any(errors.values()),
    }
    return {
        "gates": gates,
        "audit_deltas": errors,
        "trace_p95_ms": trace["p95_ms"],
        "trace_success": trace["ok"],
        "heavy_goodput": run["heavy"]["ok_per_s"],
        "dashboard_complete": run["dashboard"]["ok"],
        "dashboard_post_acknowledgement": run["dashboard_post_ack_observations"],
        "slow_audit_batches": slow,
        "compaction_intervals": compaction,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    arms = {name: read(args.root / f"cache-{name}") for name in ("false", "true")}
    passed = all(all(arm["gates"].values()) for arm in arms.values())
    print(json.dumps({"arms": arms, "decision": "pass" if passed else "no-go"}, indent=2))


if __name__ == "__main__":
    main()
