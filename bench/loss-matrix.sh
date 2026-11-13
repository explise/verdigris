#!/usr/bin/env bash
# Durability check: does every acked record survive?
#
# `POST /v1/ingest` returns 200 only after the Parquet is written and the manifest
# commit succeeds — the store is the write-ahead log, so an acked record is
# supposed to be durable. This compares records acked against records queryable,
# across the compaction x concurrency matrix, to isolate what loses them.
#
# The compaction arms are only labelled as such if a pass actually merged files
# during the arm; the script asks the node and refuses the label otherwise (#48).
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f bench/docker-compose.yml"
CONFIG="bench/config/loadtest.toml"
LOADGEN=./bench/loadgen/target/release/verdigris-loadgen
RESULTS=bench/results
mkdir -p "$RESULTS"

# Each arm edits the committed config in place, so restore it however we exit —
# a failed run must not leave a dirty tree behind.
CLEAN=$(mktemp)
cp "$CONFIG" "$CLEAN"
trap 'cp "$CLEAN" "$CONFIG"; rm -f "$CLEAN"' EXIT

FAILED=0

# Section-aware edit of `key = value` lines inside [compaction]. A bare
# `sed 's/^enabled = .*/'` also rewrites [auth], which silently turns auth on and
# makes every ingest 401 — the results then look like total data loss for
# entirely the wrong reason.
#
# Rewrites the existing file rather than temp-and-rename: it is bind-mounted into
# the vdg container, and a rename would leave the mount pointing at the old inode.
set_compaction() {
  python3 - "$@" <<'EOF'
import re, sys
p = 'bench/config/loadtest.toml'

def edit(section):
    for assign in sys.argv[1:]:
        key, val = assign.split('=', 1)
        pattern = rf'^{re.escape(key)} = .*$'
        if not re.search(pattern, section, flags=re.M):
            sys.exit(f'set_compaction: no `{key}` in [compaction]')
        section = re.sub(pattern, f'{key} = {val}', section, count=1, flags=re.M)
    return section

s = open(p).read()
s = re.sub(r'(\[compaction\]\n)((?:(?!\[)[\s\S])*)',
           lambda m: m.group(1) + edit(m.group(2)), s)
open(p, 'w').write(s)
EOF
}

# Compaction counters straight from the node. `runsTotal` only moves when a pass
# actually merged files, so this is evidence a pass ran — `enabled = true` in a
# config file is not, which is the whole of #48.
compaction_stats() {
  curl -s --max-time 120 http://localhost:8080/v1/metrics \
    | python3 -c '
import sys, json
c = json.load(sys.stdin)["compaction"]
print(str(c["enabled"]).lower(), c["runsTotal"], c["filesMergedTotal"])
'
}

row() {
  local name="$1" compaction="$2" conc="$3" secs="${4:-12}" interval="${5:-4}"
  set_compaction "enabled=$compaction" "interval_secs=$interval"

  # Stop the node before emptying the bucket, and start it after. A running
  # scheduler ticking on the old manifest while the bucket is wiped would commit
  # a manifest naming objects that no longer exist, and those phantom rows would
  # show up in the count below.
  $COMPOSE stop vdg >/dev/null 2>&1 || true
  $COMPOSE exec -T minio mc rm -r --force l/verdigris >/dev/null 2>&1 || true
  $COMPOSE exec -T minio mc mb --ignore-existing l/verdigris >/dev/null 2>&1 || true
  $COMPOSE start vdg >/dev/null 2>&1
  for _ in $(seq 1 30); do curl -sf http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 2; done

  local out="$RESULTS/loss-$name.json" tmp="$RESULTS/loss-$name.tmp.json"
  $LOADGEN --url http://localhost:8080 --start-mibps 400 --steps 1 \
    --step-secs "$secs" --settle-secs 0 --concurrency "$conc" \
    --body-bytes 1048576 --out "$tmp" >/dev/null 2>&1

  # Let in-flight passes finish before counting: a pass only publishes its
  # counters and its manifest commit at the end.
  sleep 8

  # Verify the arm is what it claims. A pass can still be merging when we first
  # ask, so poll for a completed one on the `on` arm rather than sampling once.
  local stats="" enabled="" runs="" merged="" waited=0
  while :; do
    stats=$(compaction_stats 2>/dev/null) || stats=""
    if [[ -n "$stats" ]]; then
      read -r enabled runs merged <<<"$stats"
    fi
    if [[ "$compaction" == true && "${runs:-0}" -ge 1 ]]; then break; fi
    if [[ "$compaction" == false || "$waited" -ge 90 ]]; then break; fi
    sleep 3
    waited=$((waited + 3))
  done

  local verdict=ok
  if [[ "$compaction" == true ]]; then
    if [[ "${enabled:-}" != true || "${runs:-0}" -lt 1 ]]; then verdict=unverified; fi
  elif [[ "${enabled:-}" != false || "${runs:-0}" -ne 0 ]]; then
    verdict=unverified
  fi

  local stored
  stored=$(curl -s -X POST http://localhost:8080/v1/query \
    -H 'content-type: application/json' \
    -d '{"sql":"SELECT COUNT(*) AS n FROM logs"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['rows'][0]['n'])")

  python3 - "$name" "$compaction" "$conc" "$tmp" "$stored" "$runs" "$merged" "$verdict" <<'EOF'
import json, sys
name, comp, conc, out, stored, runs, merged, verdict = sys.argv[1:9]
s = json.load(open(out))['steps'][0]
acked = round(s['accepted_records_per_sec'] * s['elapsed_secs'])
stored = int(stored)
loss = 100 * (1 - stored / acked) if acked else 0.0
state = 'on' if comp == 'true' else 'off'
tag = 'ok' if verdict == 'ok' else 'UNVERIFIED'
print(f"{name:<22} compaction={state:<3} (runs={runs} merged={merged}) conc={conc:<2} "
      f"acked={acked:>9,} stored={stored:>9,} loss={loss:6.2f}% [{tag}]")
EOF

  if [[ "$verdict" == ok ]]; then
    mv "$tmp" "$out"
  else
    mv "$tmp" "${out%.json}.UNVERIFIED.json"
    printf '%s\n' \
      "!! $name: compaction=$compaction was NOT exercised as labelled" \
      "(node reports enabled=${enabled:-?} runs=${runs:-?} merged=${merged:-?})." \
      "   Kept as ${out%.json}.UNVERIFIED.json; any loss-$name.json on disk is" \
      "   from an earlier run and does not describe this arm." >&2
    FAILED=$((FAILED + 1))
  fi
}

printf '%s\n' "--- durability matrix (1 MiB bodies) ---"
row "no-compaction-serial" false 1
row "compaction-serial"    true  1
row "no-compaction-conc8"  false 8
row "compaction-conc8"     true  8

if [[ "$FAILED" -ne 0 ]]; then
  printf '%s\n' "!! $FAILED arm(s) did not verify compaction; see above." >&2
  exit 1
fi
