#!/usr/bin/env python3
"""Validate #29 mixed-load artifacts. Exits 0 for pass, 1 for no-go, 2 for invalid evidence."""
import argparse
import gzip
import json
import math
import sys
import tomllib
from pathlib import Path

from mixed import FIXED_RATES

TRIAL_SECONDS = 60
TRACE_SUCCESS = 0.99
TRACE_P95_MS = 120


def load_trial(directory, trial):
    path = directory / f"trial-{trial}.json"
    if path.is_file():
        return json.loads(path.read_text())
    with gzip.open(path.with_suffix(".json.gz"), "rt") as src:
        return json.load(src)


def verify_trial(data, trial):
    if (data["rows"], data["duration_s"], data["fixed_rates"]) != (20_000_000, TRIAL_SECONDS, FIXED_RATES):
        raise ValueError(f"trial {trial} has a different corpus size, duration, or schedule")
    if set(data["raw"]) != FIXED_RATES.keys():
        raise ValueError(f"trial {trial} has a different request mix")
    memory = data["peak_rss_mib"]
    if len(memory) != 2 or any(not math.isfinite(value) or value <= 0 for value in memory.values()):
        raise ValueError(f"trial {trial} is missing the memory sample for one or both containers")

    classes = {}
    for kind, rate in FIXED_RATES.items():
        samples = data["raw"][kind]
        if len(samples) != math.ceil(rate * TRIAL_SECONDS):
            raise ValueError(f"trial {trial} {kind} has the wrong number of arrivals")
        if any(item["error"] == "driver_inflight_limit" for item in samples):
            raise ValueError(f"trial {trial} {kind} dropped offered work in the driver")
        success_status = 201 if kind == "alert" else 200
        successes = sorted(sample["scheduled_ms"] for sample in samples if sample["status"] == success_status)
        if any(not math.isfinite(value) or value < 0 for value in successes):
            raise ValueError(f"trial {trial} {kind} contains invalid latency measurements")
        shed = sum(sample["status"] == 429 for sample in samples)
        other = len(samples) - len(successes) - shed
        p95 = successes[math.ceil(len(successes) * 0.95) - 1] if successes else None
        reported = data[kind]
        if (reported["attempts"], reported["ok"], reported["shed_429"], reported["other"]) != (
            len(samples), len(successes), shed, other
        ) or not math.isclose(reported["ok_per_s"], len(successes) / TRIAL_SECONDS):
            raise ValueError(f"trial {trial} {kind} counts or goodput disagree with raw samples")
        # Raw samples are rounded to microseconds while the reported summary
        # uses the original client timer.
        if (p95 is None and reported["p95_ms"] is not None) or (
            p95 is not None and (reported["p95_ms"] is None or not math.isclose(reported["p95_ms"], p95, rel_tol=0, abs_tol=0.00051))
        ):
            raise ValueError(f"trial {trial} {kind} p95 disagrees with nearest rank on raw samples")
        classes[kind] = {"arrivals": len(samples), "ok": len(successes), "shed": shed,
                         "other": other, "goodput_per_s": len(successes) / TRIAL_SECONDS,
                         "p95_ms": reported["p95_ms"]}

    trace = classes["trace"]
    success = trace["ok"] / trace["arrivals"]
    trace_waits = sorted(
        sample["phases_us"]["admission"] / 1000
        for sample in data["raw"]["trace"] if sample["status"] == 200
    )
    wait = trace_waits[math.ceil(len(trace_waits) * 0.95) - 1] if trace_waits else None
    alerts = data.get("background_alert_counters")
    if alerts is not None and any(type(value) is not int or value < 0 for value in alerts.values()):
        raise ValueError(f"trial {trial} has invalid background-alert counter deltas")
    return {
        "trace_gate": success >= TRACE_SUCCESS and trace["p95_ms"] <= TRACE_P95_MS,
        "trace_success": success,
        "trace_admission_p95_ms": wait,
        "heavy_progress": classes["heavy"]["ok"] > 0,
        "background_alerts": alerts,
        "sampled_memory_mib": memory,
        "classes": classes,
    }


def summarize(directory):
    with (directory / "config.toml").open("rb") as config:
        query = tomllib.load(config)["query"]
    if query["max_concurrent"] != 4:
        raise ValueError("expected the four-permit configuration")
    states = {}
    for name in ("vdg", "minio"):
        state_path = directory / f"{name}-state.txt"
        state = dict(field.split("=", 1) for field in state_path.read_text().strip().split())
        restarted = state.get("restarts", state.get("restart"))
        if state.get("OOMKilled") not in ("true", "false") or restarted is None:
            raise ValueError(f"missing teardown state for {name}")
        states[name] = {"oom_killed": state["OOMKilled"] == "true", "restarts": int(restarted)}
    trials = []
    corpus = set()
    for trial in range(1, 4):
        data = load_trial(directory, trial)
        corpus.add(data["base_ts_ms"])
        trials.append(verify_trial(data, trial))
    if len(corpus) != 1:
        raise ValueError("the trials used different corpus timestamps")
    healthy = all(not state["oom_killed"] and state["restarts"] == 0 for state in states.values())
    passed = healthy and all(trial["trace_gate"] and trial["heavy_progress"] for trial in trials)
    return {
        "decision": "pass" if passed else "no-go",
        "configured_query_permits": query["max_concurrent"],
        "configured_reserved_permits": query.get("cheap_permits", 1),
        "containers": states,
        "trials": trials,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        report = summarize(args.directory)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print(f"invalid benchmark evidence: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2))
    return 0 if report["decision"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
