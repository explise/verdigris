#!/usr/bin/env python3
"""Compare the manifest-backed minute-summary pilot with its paired raw control."""
import json
from pathlib import Path
import sys


def read(path, enabled):
    def load(name):
        return json.loads((path / name).read_text())

    seed, warm, meta = (load(name) for name in ("seed.json", "warm.json", "manifest-object.json"))
    if not warm["validated_exact_results"] or "OOMKilled=false" not in (path / "vdg-state.txt").read_text():
        raise ValueError(f"{path.name}: data or container verification failed")
    if any(warm["audit_counters_after"][key] for key in ("failedTotal", "timedOutTotal", "saturatedTotal")):
        raise ValueError(f"{path.name}: audit was unhealthy")
    quote = warm["minute_summary_quote"]
    cohorts = warm["queries"]["minute_aggregation"]
    for mode, cohort in cohorts.items():
        if len(cohort["samples"]) != 100:
            raise ValueError(f"{path.name}/{mode}: expected 100 samples")
        for sample in cohort["samples"]:
            if ((sample["execution"] == "minute_summary") != enabled
                or sample["planned_bytes"] != quote["scanBytes"]
                or sample["planned_files"] != quote["filesTouched"]
                or sample["rows"] != 50 or sample["audit_ms"] is None):
                raise ValueError(f"{path.name}/{mode}: query used the wrong path, quote, or result shape")
            if enabled and mode == "on" and sample["profile"]["object_reads"]["result_bytes"] != 0:
                raise ValueError(f"{path.name}: summary made a Parquet data read")
    return {
        "rows": seed["corpus"]["rows"],
        "compaction_input": seed["explicit_compaction"]["before"],
        "compaction_output": seed["explicit_compaction"]["after"],
        "compaction_s": seed["explicit_compaction"]["elapsed_s"],
        "ingest_rows_per_s": seed["ingest"]["rows_per_s"],
        "manifest_bytes": int(meta["size"]),
        "quote": quote,
        "warm_p95_ms": {shape: modes["off"]["elapsed_ms"]["p95"]
                        for shape, modes in warm["queries"].items()},
    }


def main():
    root = Path(sys.argv[1])
    raw = read(root / "raw", False)
    summary = read(root / "summary", True)
    for key in ("rows", "compaction_input", "compaction_output", "quote"):
        if raw[key] != summary[key]:
            raise ValueError(f"treatment changed {key}")
    metadata = summary["manifest_bytes"] - raw["manifest_bytes"]
    if not 0 < metadata <= min(64 * 1024, raw["quote"]["scanBytes"] // 100):
        raise ValueError("summary exceeded the manifest byte budget")
    # Report non-aggregate workloads too, since every request loads the manifest.
    result = {
        "rows": raw["rows"], "additional_manifest_bytes": metadata, "raw_quote": raw["quote"],
        "raw_p95_ms": raw["warm_p95_ms"], "summary_p95_ms": summary["warm_p95_ms"],
        "raw_ingest_rows_per_s": raw["ingest_rows_per_s"],
        "summary_ingest_rows_per_s": summary["ingest_rows_per_s"],
        "raw_compaction_s": raw["compaction_s"], "summary_compaction_s": summary["compaction_s"],
        "summary_meets_50ms_p95": summary["warm_p95_ms"]["minute_aggregation"] <= 50,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
