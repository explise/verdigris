#!/usr/bin/env bash
# Sample per-container CPU and memory during a load run.
#
# Exists because a bottleneck found under resource starvation is an artifact, not
# a finding: if MinIO is pinned at its CPU limit, "ingest saturated at X MiB/s"
# really means "the object store ran out of CPU". This records enough to tell the
# two apart afterwards.
#
#   bench/sample-resources.sh bench/results/resources.csv &
#   SAMPLER=$!
#   ... run the ramp ...
#   kill $SAMPLER
set -euo pipefail

OUT="${1:-bench/results/resources.csv}"
INTERVAL="${2:-2}"
mkdir -p "$(dirname "$OUT")"

echo "elapsed_s,container,cpu_pct,mem_used_bytes,mem_limit_bytes,net_io,block_io" > "$OUT"

start=$SECONDS
while true; do
  elapsed=$(( SECONDS - start ))
  # --no-stream gives one snapshot; the format string avoids parsing the table.
  docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}' 2>/dev/null \
  | while IFS=$'\t' read -r name cpu mem net blk; do
      used="${mem%% /*}"
      limit="${mem##*/ }"
      printf '%s,%s,%s,%s,%s,%s,%s\n' \
        "$elapsed" "$name" "${cpu%\%}" "$used" "$limit" "${net// /}" "${blk// /}" >> "$OUT"
    done
  sleep "$INTERVAL"
done
