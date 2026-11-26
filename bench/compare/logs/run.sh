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
OUT=${OUT:-bench/results/compare-logs}
VDG_IMAGE=${VDG_IMAGE:-verdigris-bench}
NET=verdigris-logs-bench
VOL=verdigris-logs-bench-minio

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
  echo "budget=8cpu/6GiB per system; vdg=4cpu/3GiB + minio=4cpu/3GiB"
} > "$OUT/machine.txt"

ids=()
vl_data_dir=
cleanup() {
  for id in "${ids[@]:-}"; do
    [[ -n "$id" ]] && docker rm -f "$id" >/dev/null 2>&1 || true
  done
  docker network rm "$NET" >/dev/null 2>&1 || true
  # Keep the MinIO volume only on success? No: benchmark input is disposable.
  docker volume rm "$VOL" >/dev/null 2>&1 || true
  if [[ -n "$vl_data_dir" ]]; then
    docker run --rm -v "$vl_data_dir:/d" "$BUSYBOX" sh -c 'rm -rf /d/* /d/.[!.]* 2>/dev/null' || true
    rmdir "$vl_data_dir" 2>/dev/null || true
  fi
}
trap cleanup EXIT

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
  echo "==> Verdigris (vdg 4 CPU / 3 GiB + MinIO 4 CPU / 3 GiB)"
  docker network create "$NET" >/dev/null
  docker volume create "$VOL" >/dev/null
  docker run --rm -v "$VOL:/data" "$BUSYBOX" chown -R 65532:65532 /data

  local minio
  minio=$(docker run -d --rm --name vdgbench-minio --network "$NET" \
    --cpus=4 --memory=3g -p 127.0.0.1:9000:9000 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    -e MINIO_UPDATE=off -v "$VOL:/data" "$MINIO" server /data)
  ids+=("$minio")
  wait_ready MinIO http://127.0.0.1:9000/minio/health/ready
  docker run --rm --network "$NET" \
    -e MC_HOST_local=http://minioadmin:minioadmin@vdgbench-minio:9000 \
    "$MC" mb --ignore-existing local/verdigris >/dev/null 2>&1

  local vdg
  vdg=$(docker run -d --rm --name vdgbench-vdg --network "$NET" \
    --cpus=4 --memory=3g -p 127.0.0.1:8080:8080 \
    -e VERDIGRIS_CONFIG=/app/config/bench.toml \
    -e TOKIO_WORKER_THREADS=4 -e RUST_LOG=warn \
    -v "$repo/bench/compare/logs/vdg.toml:/app/config/bench.toml:ro" \
    "$VDG_IMAGE" serve --port 8080 --table bench --role all)
  ids+=("$vdg")
  wait_ready vdg http://127.0.0.1:8080/healthz

  python3 bench/compare/logs/compare.py \
    --system vdg --port 8080 --rows "$ROWS" --batch-rows "$BATCH_ROWS" \
    --threads "$THREADS" --iterations "$ITERATIONS" --warmup "$WARMUP" \
    --out "$OUT/vdg.json"

  local bytes mem
  bytes=$(docker exec vdgbench-minio du -sb /data 2>/dev/null | awk '{print $1}' || echo 0)
  mem=$(container_mem vdgbench-vdg vdgbench-minio)
  if [[ "${RESTART:-1}" == "1" ]]; then
    docker restart "$vdg" >/dev/null
    wait_ready vdg http://127.0.0.1:8080/healthz
    python3 bench/compare/logs/first_query.py --system vdg --port 8080       --rows "$ROWS" --out "$OUT/vdg-restart.json"
  fi

  write_meta vdg "$VDG_IMAGE" 8 "vdg 3g + minio 3g" "$bytes"
  storage=$(curl -fsS --max-time 5 http://127.0.0.1:8080/v1/storage/tiers 2>/dev/null || echo '{}')
  bucket_bytes=$(docker exec vdgbench-minio du -sb /data/verdigris 2>/dev/null | awk '{print $1}' || echo 0)
  internal_bytes=$(docker exec vdgbench-minio du -sb /data/.minio.sys 2>/dev/null | awk '{print $1}' || echo 0)
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
  docker rm -f "$vdg" "$minio" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  docker volume rm "$VOL" >/dev/null 2>&1 || true
}

run_clickhouse() {
  echo "==> ClickHouse (8 CPU / 6 GiB)"
  local ch
  ch=$(docker run -d --rm --name chbench --cpus=8 --memory=6g \
    -p 127.0.0.1:8123:8123 \
    -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=benchpass \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 "$CH")
  ids+=("$ch")
  wait_ready ClickHouse http://127.0.0.1:8123/ping

  python3 bench/compare/logs/compare.py \
    --system clickhouse --port 8123 --rows "$ROWS" --batch-rows "$BATCH_ROWS" \
    --threads "$THREADS" --iterations "$ITERATIONS" --warmup "$WARMUP" \
    --out "$OUT/clickhouse.json"

  local bytes mem
  bytes=$(docker exec chbench du -sb /var/lib/clickhouse 2>/dev/null | awk '{print $1}' || echo 0)
  mem=$(container_mem chbench)
  if [[ "${RESTART:-1}" == "1" ]]; then
    docker restart "$ch" >/dev/null
    wait_ready ClickHouse http://127.0.0.1:8123/ping
    python3 bench/compare/logs/first_query.py --system clickhouse --port 8123 \
      --rows "$ROWS" --out "$OUT/clickhouse-restart.json"
  fi

  write_meta clickhouse "$CH" 8 "6g" "$bytes"
  python3 - "$OUT/clickhouse.meta.json" "$mem" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p)); d["container_memory"] = sys.argv[2]
json.dump(d, open(p, "w"), indent=2)
PY
  docker rm -f "$ch" >/dev/null 2>&1 || true
}

run_victorialogs() {
  echo "==> VictoriaLogs (8 CPU / 6 GiB)"
  local vl data_dir
  # The VictoriaLogs image is a single static binary with no shell/du, so bind
  # its data path to the host and measure retained bytes from here.
  data_dir=$(mktemp -d "${TMPDIR:-/tmp}/vl-bench.XXXXXX")
  vl_data_dir=$data_dir
  vl=$(docker run -d --rm --name vlbench --cpus=8 --memory=6g \
    -p 127.0.0.1:9428:9428 \
    -v "$data_dir:/victoria-logs-data" \
    "$VL" -storageDataPath=/victoria-logs-data)
  ids+=("$vl")
  wait_ready VictoriaLogs http://127.0.0.1:9428/health

  python3 bench/compare/logs/compare.py \
    --system victorialogs --port 9428 --rows "$ROWS" --batch-rows "$BATCH_ROWS" \
    --threads "$THREADS" --iterations "$ITERATIONS" --warmup "$WARMUP" \
    --out "$OUT/victorialogs.json"

  local bytes mem
  # VictoriaLogs buffers recent rows in memory and flushes/merges on a timer;
  # let that settle before counting retained bytes.
  sleep 10
  bytes=$(du -sb "$data_dir" 2>/dev/null | awk '{print $1}' || echo 0)
  mem=$(container_mem vlbench)
  if [[ "${RESTART:-1}" == "1" ]]; then
    docker restart "$vl" >/dev/null
    wait_ready VictoriaLogs http://127.0.0.1:9428/health
    python3 bench/compare/logs/first_query.py --system victorialogs --port 9428 \
      --rows "$ROWS" --out "$OUT/victorialogs-restart.json"
  fi

  write_meta victorialogs "$VL" 8 "6g" "$bytes"
  python3 - "$OUT/victorialogs.meta.json" "$mem" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p)); d["container_memory"] = sys.argv[2]
json.dump(d, open(p, "w"), indent=2)
PY
  docker rm -f "$vl" >/dev/null 2>&1 || true
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
