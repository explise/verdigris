#!/usr/bin/env python3
"""Render bench/results/compare-logs/*.json as a single markdown summary."""
import glob
import json
import os
import sys

out_dir = sys.argv[1] if len(sys.argv) > 1 else "bench/results/compare-logs"
order = {"vdg": 0, "clickhouse": 1, "victorialogs": 2}
systems = []
for path in sorted(glob.glob(os.path.join(out_dir, "*.json"))):
    if path.endswith(".meta.json") or path.endswith("-restart.json"):
        continue
    with open(path) as f:
        systems.append(json.load(f))
systems.sort(key=lambda s: order.get(s["system"], 99))

if not systems:
    raise SystemExit(f"no result JSONs in {out_dir}")

meta = {}
for path in glob.glob(os.path.join(out_dir, "*.meta.json")):
    with open(path) as f:
        m = json.load(f)
    meta[m["system"]] = m

labels = {
    "vdg": "Verdigris + MinIO",
    "clickhouse": "ClickHouse",
    "victorialogs": "VictoriaLogs",
}

lines = []
lines.append("# Equivalent log-query comparison\n")
lines.append(
    "Same deterministic corpus, same 8 CPU / 6 GiB total budget per complete "
    "deployment, one system at a time. Verdigris's budget includes its MinIO "
    "object store (4 CPU / 3 GiB + 4 CPU / 3 GiB).\n"
)
lines.append("| System | Retained | Container memory after queries |")
lines.append("|---|---:|---:|")
for s in systems:
    m = meta.get(s["system"], {})
    retained = m.get("retained_bytes")
    retained_s = f"{retained / 1048576:.1f} MiB" if retained else "n/a"
    mem = m.get("container_memory") or "n/a"
    lines.append(f"| **{labels.get(s['system'], s['system'])}** | {retained_s} | {mem} |")

lines.append("\n## Ingest (reported separately from query latency)\n")
lines.append("| System | Acked rows/s | MiB/s | Visibility wait |")
lines.append("|---|---:|---:|---:|")
for s in systems:
    i = s["ingest"]
    lines.append(
        f"| {labels.get(s['system'], s['system'])} | {i['rows_per_s']:,.0f} | "
        f"{i['mib_per_s']:.1f} | {i['visibility_s']:.1f}s |"
    )

lines.append("\n## Query latency (ms, after merges/compaction settle)\n")
names = [
    ("recent_service_search", "Recent service search (`checkout` + `ERROR`, newest 200)"),
    ("trace_lookup", "Trace lookup (rotating exact `trace_id` hits)"),
    ("trace_lookup_miss", "Trace lookup (missing `trace_id`)"),
    ("trace_lookup_cross_tier", "Trace lookup (one id in all three tiers)"),
    ("minute_aggregation", "1-minute aggregation (total + errors)"),
]
for key, label in names:
    lines.append(f"### {label}\n")
    lines.append("| System | p50 | p95 | p99 | max | Samples |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for s in systems:
        q = s["queries"][key]
        lines.append(
            f"| {labels.get(s['system'], s['system'])} | {q['p50_ms']:.1f} | "
            f"{q['p95_ms']:.1f} | {q['p99_ms']:.1f} | {q['max_ms']:.1f} | {q['samples']} |"
        )
    lines.append("")

lines.append("## Equivalence gate\n")
lines.append("| System | Recent count | Search rows | Trace rows | Cross-tier | Missing | Buckets | Total | Errors |")
lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for s in systems:
    e = s["equivalence"]
    lines.append(
        f"| {labels.get(s['system'], s['system'])} | {e['recent_count']:,} | "
        f"{e['search_rows']} | {e['trace_rows']} | {e.get('cross_trace_rows', 0)} | "
        f"{e.get('missing_trace_rows', 0)} | {e['buckets']} | "
        f"{e['total']:,} | {e['errors']:,} |"
    )

lines.append("\n_Corpus and query definitions are in `bench/compare/logs/compare.py`; "
             "resource limits and image digests are in `bench/compare/logs/run.sh`._\n")

text = "\n".join(lines)
path = os.path.join(out_dir, "SUMMARY.md")
with open(path, "w") as f:
    f.write(text + "\n")
print(text)
print(f"\nwrote {path}")
