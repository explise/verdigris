#!/usr/bin/env bash
# Equivalent-log-query comparison, one system at a time so each complete
# deployment gets the same 8 CPU / 6 GiB budget.
#
#   bench/compare/logs/run.sh
#   SYSTEMS="vdg clickhouse" ROWS=200000 ITERATIONS=5 bench/compare/logs/run.sh
#
# Verdigris is charged for its object store: vdg 4 CPU/3 GiB + MinIO 4 CPU/3 GiB.
# ClickHouse and VictoriaLogs each get 8 CPU/6 GiB in one container.
#
# Outputs land in bench/results/compare-logs/:
#   <system>.json       measurements
#   <system>.meta.json  image, resource limits, retained bytes, container memory
#   SUMMARY.md          generated after all systems finish
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$repo"

CH=clickhouse/clickhouse-server@sha256:4f491abc44d9f4a1747c033c35ddd5bc385521305d85ca8251ba0824ff597035
VL=victoriametrics/victoria-logs@sha256:ee4e820cac3721f5f5d91641b950c22439593adc4224f9655e5333d5a84c1dfe
MINIO=cgr.dev/chainguard/minio@sha256:634c989a2f1029162fc2676c263caa181213b9410449968532f750a6fb77532d
MC=cgr.dev/chainguard/minio-client@sha256:36ac7ab6d181f259ab62defe0bb55ad1dbf89b152f0b8f49b1c63049374d812f
BUSYBOX=busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec

SYSTEMS=${SYSTEMS:-"vdg clickhouse victorialogs"}
ROWS=${ROWS:-1000000}
BATCH_ROWS=${BATCH_ROWS:-10000}
THREADS=${THREADS:-4}
ITERATIONS=${ITERATIONS:-10}
WARMUP=${WARMUP:-3}
identity_args=()
if [[ "${VERIFY_ALL_ROW_IDS:-0}" == 1 ]]; then
  identity_args+=(--verify-all-row-ids)
fi
OUT=${OUT:-bench/results/compare-logs}
VDG_IMAGE=${VDG_IMAGE:-verdigris-bench}
TOTAL_CPUS=${TOTAL_CPUS:-8}
if [[ ! "$TOTAL_CPUS" =~ ^[1-9][0-9]*$ ]]; then
  echo "TOTAL_CPUS must be an even integer of at least 2" >&2
  exit 2
fi
if (( TOTAL_CPUS < 2 || TOTAL_CPUS % 2 != 0 )); then
  echo "TOTAL_CPUS must be divisible by two for the Verdigris split" >&2
  exit 2
fi
SERVICE_CPUS=$((TOTAL_CPUS / 2))
RUN_ID=${RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}
NET=verdigris-logs-bench-$RUN_ID
VOL=verdigris-logs-bench-$RUN_ID-minio
VDG_C=vdgbench-$RUN_ID-vdg
MINIO_C=vdgbench-$RUN_ID-minio
CH_C=chbench-$RUN_ID
VL_C=vlbench-$RUN_ID
CORPUS_TS_MS=${CORPUS_TS_MS:-$(python3 -c 'import time; print((int(time.time()*1000)-55*60_000)//60_000*60_000)')}
export CORPUS_TS_MS

mkdir -p "$OUT"

{
  echo "date_utc=$(date -u +%FT%TZ)"
  echo "host_cpus=$(nproc)"
  echo "host_mem_kb=$(awk '/MemTotal/{print $2}' /proc/meminfo)"
  echo "kernel=$(uname -r)"
  echo "docker=$(docker version --format '{{.Server.Version}}')"
  echo "git_commit=$(git rev-parse --short HEAD)"
  echo "git_dirty=$(git status --porcelain | wc -l)"
  echo "rows=$ROWS batch_rows=$BATCH_ROWS threads=$THREADS iterations=$ITERATIONS warmup=$WARMUP"
  echo "corpus_ts_ms=$CORPUS_TS_MS"
  echo "run_id=$RUN_ID"
  echo "vdg_image=$VDG_IMAGE"
  echo "budget=${TOTAL_CPUS}cpu/6GiB per system; vdg=${SERVICE_CPUS}cpu/3GiB + minio=${SERVICE_CPUS}cpu/3GiB"
} > "$OUT/machine.txt"

ids=()
net_created=0
vol_created=0
vl_data_dir=
cleanup() {
  for id in "${ids[@]:-}"; do
    [[ -n "$id" ]] && docker rm -f "$id" >/dev/null 2>&1 || true
  done
  if (( net_created )); then
    docker network rm "$NET" >/dev/null 2>&1 || true
    net_created=0
  fi
  if (( vol_created )); then
    docker volume rm "$VOL" >/dev/null 2>&1 || true
    vol_created=0
  fi
  if [[ -n "$vl_data_dir" ]]; then
    docker run --rm -v "$vl_data_dir:/d" "$BUSYBOX" sh -c 'rm -rf /d/* /d/.[!.]* 2>/dev/null' || true
    rmdir "$vl_data_dir" 2>/dev/null || true
    vl_data_dir=
  fi
}
trap cleanup EXIT
cleanup_vdg() {
  docker rm -f "$VDG_C" "$MINIO_C" >/dev/null 2>&1 || true
  if (( net_created )); then
    docker network rm "$NET" >/dev/null 2>&1 || true
    net_created=0
  fi
  if (( vol_created )); then
    docker volume rm "$VOL" >/dev/null 2>&1 || true
    vol_created=0
  fi
}
restart_first_queries() {
  local system=$1 service_port=$2 container=$3 health_path=$4 shape port
  for shape in trace trace-miss trace-cross-tier search aggregate; do
    docker restart "$container" >/dev/null
    port=$(local_port "$container" "$service_port")
    wait_ready "$system restart" "http://127.0.0.1:$port$health_path"
    python3 -B bench/compare/logs/first_query.py --system "$system" --port "$port" \
      --rows "$ROWS" --shape "$shape" --out "$OUT/$system-restart-$shape.json"
  done
}

local_port() {
  docker port "$1" "$2/tcp" | awk -F: '{print $NF}'
}

wait_ready() {
  local name=$1 url=$2 deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  printf '%s did not become ready at %s\n' "$name" "$url" >&2
  return 1
}

container_mem() {
  docker stats --no-stream --format '{{.Name}}={{.MemUsage}}' "$@" 2>/dev/null | paste -sd, - || true
}

write_meta() {
  local system=$1 image=$2 cpus=$3 mem=$4 bytes=$5 image_id
  image_id=$(docker image inspect "$image" --format '{{.Id}}' 2>/dev/null || true)
  python3 - "$OUT/$system.meta.json" "$system" "$image" "$image_id" "$cpus" "$mem" "$bytes" <<'PY'
import json, sys
path, system, image, image_id, cpus, mem, data_bytes = sys.argv[1:]
json.dump({
    "system": system,
    "image": image,
    "image_id": image_id or None,
    "cpu_limit": int(cpus),
    "memory_limit": mem,
    "retained_bytes": int(data_bytes) if data_bytes.isdigit() else None,
}, open(path, "w"), indent=2)
PY
}

run_vdg() {
  echo "==> Verdigris (${SERVICE_CPUS} CPU / 3 GiB each for vdg and MinIO)"
  local config
  config=$(realpath "$OUT")/verdigris-config.toml
  sed "s/^cores = .*/cores = $SERVICE_CPUS/" bench/compare/logs/vdg.toml > "$config"
  chmod 644 "$config"
  docker network create "$NET" >/dev/null
  net_created=1
  docker volume create "$VOL" >/dev/null
  vol_created=1
  docker run --rm -v "$VOL:/data" "$BUSYBOX" chown -R 65532:65532 /data

  local minio minio_port
  minio=$(docker run -d --rm --name "$MINIO_C" --network "$NET" \
    --network-alias vdgbench-minio \
    --cpus="$SERVICE_CPUS" --memory=3g -p 127.0.0.1::9000 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    -e MINIO_UPDATE=off -v "$VOL:/data" "$MINIO" server /data)
  ids+=("$minio")
  minio_port=$(local_port "$minio" 9000)
  wait_ready MinIO "http://127.0.0.1:$minio_port/minio/health/ready"
  docker run --rm --network "$NET" \
    -e MC_HOST_local=http://minioadmin:minioadmin@$MINIO_C:9000 \
    "$MC" mb --ignore-existing local/verdigris >/dev/null 2>&1

  local vdg port
  vdg=$(docker run -d --rm --name "$VDG_C" --network "$NET" \
    --cpus="$SERVICE_CPUS" --memory=3g -p 127.0.0.1::8080 \
    -e VERDIGRIS_CONFIG=/app/config/bench.toml \
    -e TOKIO_WORKER_THREADS="$SERVICE_CPUS" -e RUST_LOG=warn \
    -v "$config:/app/config/bench.toml:ro" \
    "$VDG_IMAGE" serve --port 8080 --table bench --role all)
  ids+=("$vdg")
  port=$(local_port "$vdg" 8080)
  wait_ready vdg "http://127.0.0.1:$port/healthz"

  python3 bench/compare/logs/compare.py \
    --system vdg --port "$port" --rows "$ROWS" --batch-rows "$BATCH_ROWS" \
    --threads "$THREADS" --iterations "$ITERATIONS" --warmup "$WARMUP" "${identity_args[@]}" \
    --out "$OUT/vdg.json"

  local bytes mem
  bytes=$(docker exec "$MINIO_C" du -sb /data 2>/dev/null | awk '{print $1}' || echo 0)
  mem=$(container_mem "$VDG_C" "$MINIO_C")
  if [[ "${RESTART:-1}" == "1" ]]; then
    restart_first_queries vdg 8080 "$vdg" /healthz
    port=$(local_port "$vdg" 8080)
  fi

  write_meta vdg "$VDG_IMAGE" "$TOTAL_CPUS" "vdg 3g + minio 3g" "$bytes"
  storage=$(curl -fsS --max-time 5 "http://127.0.0.1:$port/v1/storage/tiers" 2>/dev/null || echo '{}')
  bucket_bytes=$(docker exec "$MINIO_C" du -sb /data/verdigris 2>/dev/null | awk '{print $1}' || echo 0)
  internal_bytes=$(docker exec "$MINIO_C" du -sb /data/.minio.sys 2>/dev/null | awk '{print $1}' || echo 0)
  docker exec "$MINIO_C" du -b -d 2 /data/.minio.sys > "$OUT/minio-internal.txt"
  python3 - "$OUT/vdg.meta.json" "$mem" "$storage" "$bucket_bytes" "$internal_bytes" <<'PY'
import json, sys
p, mem, raw, bucket, internal = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
d = json.load(open(p))
d["container_memory"] = mem
d["minio_bucket_bytes"] = int(bucket) if bucket.isdigit() else None
d["minio_internal_bytes"] = int(internal) if internal.isdigit() else None
try:
    s = json.loads(raw)
    c = s.get("compaction") or {}
    d["active_files"] = int(c.get("smallFiles", 0)) + int(c.get("compacted", 0))
    d["retired_files"] = int(c.get("retiredFiles", 0))
    d["total_gb"] = s.get("totalGB")
except Exception:
    pass
json.dump(d, open(p, "w"), indent=2)
PY
  cleanup_vdg
}

run_clickhouse() {
  echo "==> ClickHouse ($TOTAL_CPUS CPU / 6 GiB)"
  local ch port
  ch=$(docker run -d --rm --name "$CH_C" --cpus="$TOTAL_CPUS" --memory=6g \
    -p 127.0.0.1::8123 \
    -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=benchpass \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 "$CH")
  ids+=("$ch")
  port=$(local_port "$ch" 8123)
  wait_ready ClickHouse "http://127.0.0.1:$port/ping"

  python3 bench/compare/logs/compare.py \
    --system clickhouse --port "$port" --rows "$ROWS" --batch-rows "$BATCH_ROWS" \
    --threads "$THREADS" --iterations "$ITERATIONS" --warmup "$WARMUP" "${identity_args[@]}" \
    --out "$OUT/clickhouse.json"

  local bytes mem
  bytes=$(docker exec "$CH_C" du -sb /var/lib/clickhouse 2>/dev/null | awk '{print $1}' || echo 0)
  mem=$(container_mem "$CH_C")
  if [[ "${RESTART:-1}" == "1" ]]; then
    restart_first_queries clickhouse 8123 "$ch" /ping
  fi

  write_meta clickhouse "$CH" "$TOTAL_CPUS" "6g" "$bytes"
  python3 - "$OUT/clickhouse.meta.json" "$mem" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p)); d["container_memory"] = sys.argv[2]
json.dump(d, open(p, "w"), indent=2)
PY
  docker rm -f "$CH_C" >/dev/null 2>&1 || true
}

run_victorialogs() {
  echo "==> VictoriaLogs ($TOTAL_CPUS CPU / 6 GiB)"
  local vl data_dir port
  # The VictoriaLogs image is a single static binary with no shell/du, so bind
  # its data path to the host and measure retained bytes from here.
  data_dir=$(mktemp -d "${TMPDIR:-/tmp}/vl-bench.XXXXXX")
  vl_data_dir=$data_dir
  vl=$(docker run -d --rm --name "$VL_C" --cpus="$TOTAL_CPUS" --memory=6g \
    -p 127.0.0.1::9428 \
    -v "$data_dir:/victoria-logs-data" \
    "$VL" -storageDataPath=/victoria-logs-data)
  ids+=("$vl")
  port=$(local_port "$vl" 9428)
  wait_ready VictoriaLogs "http://127.0.0.1:$port/health"

  python3 bench/compare/logs/compare.py \
    --system victorialogs --port "$port" --rows "$ROWS" --batch-rows "$BATCH_ROWS" \
    --threads "$THREADS" --iterations "$ITERATIONS" --warmup "$WARMUP" "${identity_args[@]}" \
    --out "$OUT/victorialogs.json"

  local bytes mem
  # VictoriaLogs buffers recent rows in memory and flushes/merges on a timer;
  # let that settle before counting retained bytes.
  sleep 10
  bytes=$(du -sb "$data_dir" 2>/dev/null | awk '{print $1}' || echo 0)
  mem=$(container_mem "$VL_C")
  if [[ "${RESTART:-1}" == "1" ]]; then
    restart_first_queries victorialogs 9428 "$vl" /health
  fi

  write_meta victorialogs "$VL" "$TOTAL_CPUS" "6g" "$bytes"
  python3 - "$OUT/victorialogs.meta.json" "$mem" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p)); d["container_memory"] = sys.argv[2]
json.dump(d, open(p, "w"), indent=2)
PY
  docker rm -f "$VL_C" >/dev/null 2>&1 || true
  # The container wrote as root; clear the bind mount from a throwaway root
  # container rather than failing on host-owned permissions.
  docker run --rm -v "$data_dir:/d" "$BUSYBOX" sh -c 'rm -rf /d/* /d/.[!.]* 2>/dev/null' || true
  rmdir "$data_dir" 2>/dev/null || true
  vl_data_dir=
}

if [[ "$SYSTEMS" == *vdg* && "${VDG_SKIP_BUILD:-0}" != "1" ]]; then
  if ! docker image inspect "$VDG_IMAGE" >/dev/null 2>&1; then
    echo "==> building $VDG_IMAGE"
    docker build -f bench/Dockerfile.vdg -t "$VDG_IMAGE" .
  fi
fi

for system in $SYSTEMS; do
  case "$system" in
    vdg) run_vdg ;;
    clickhouse) run_clickhouse ;;
    victorialogs) run_victorialogs ;;
    *) echo "unknown system: $system" >&2; exit 2 ;;
  esac
done

python3 bench/compare/logs/summarize.py "$OUT"
