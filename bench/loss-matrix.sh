#!/usr/bin/env bash
# Durability check: does every acked record survive?
#
# `POST /v1/ingest` returns 200 only after the Parquet is written and the manifest
# commit succeeds — the store is the write-ahead log, so an acked record is
# supposed to be durable. This compares records acked against records queryable,
# across the compaction x concurrency matrix, to isolate what loses them.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f bench/docker-compose.yml"
CONFIG="bench/config/loadtest.toml"
LOADGEN=./bench/loadgen/target/release/verdigris-loadgen

# Section-aware edit. A bare `sed 's/^enabled = .*/'` also rewrites [auth],
# which silently turns auth on and makes every ingest 401 — the results then look
# like total data loss for entirely the wrong reason.
set_compaction() {
  python3 - "$1" <<'EOF'
import re, sys
val = sys.argv[1]
p = 'bench/config/loadtest.toml'
s = open(p).read()
s = re.sub(r'(\[compaction\]\n)((?:(?!\[)[\s\S])*)',
           lambda m: m.group(1) + re.sub(r'^enabled = .*$', f'enabled = {val}', m.group(2), flags=re.M),
           s)
open(p, 'w').write(s)
EOF
}

row() {
  local name="$1" compaction="$2" conc="$3" secs="${4:-12}"
  set_compaction "$compaction"
  $COMPOSE exec -T minio mc rm -r --force l/verdigris >/dev/null 2>&1 || true
  $COMPOSE exec -T minio mc mb --ignore-existing l/verdigris >/dev/null 2>&1 || true
  $COMPOSE restart vdg >/dev/null 2>&1
  for _ in $(seq 1 30); do curl -sf http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 2; done

  local out="bench/results/loss-$name.json"
  $LOADGEN --url http://localhost:8080 --start-mibps 400 --steps 1 \
    --step-secs "$secs" --settle-secs 0 --concurrency "$conc" \
    --body-bytes 1048576 --out "$out" >/dev/null 2>&1

  # Let any in-flight compaction pass finish before counting.
  sleep 8

  local stored
  stored=$(curl -s -X POST http://localhost:8080/v1/query \
    -H 'content-type: application/json' \
    -d '{"sql":"SELECT COUNT(*) AS n FROM logs"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['rows'][0]['n'])")

  python3 - "$name" "$compaction" "$conc" "$out" "$stored" <<'EOF'
import json, sys
name, comp, conc, out, stored = sys.argv[1:6]
s = json.load(open(out))['steps'][0]
acked = round(s['accepted_records_per_sec'] * s['elapsed_secs'])
stored = int(stored)
loss = 100 * (1 - stored / acked) if acked else 0.0
print(f"{name:<24} compaction={comp:<5} conc={conc:<2} acked={acked:>9,} stored={stored:>9,} loss={loss:6.2f}%")
EOF
}

printf '%s\n' "--- durability matrix (1 MiB bodies) ---"
row "no-compaction-serial" false 1
row "compaction-serial"    true  1
row "no-compaction-conc8"  false 8
row "compaction-conc8"     true  8

set_compaction true
