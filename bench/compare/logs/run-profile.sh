#!/usr/bin/env bash
# Reproduce #83 on an already-built Verdigris release image and pinned MinIO.
set -euo pipefail

cd "$(dirname "$0")/../../.."
: "${VDG_IMAGE:?set VDG_IMAGE to the instrumented release image}"
: "${OUT:?set OUT to a new result directory}"
MINIO=cgr.dev/chainguard/minio@sha256:634c989a2f1029162fc2676c263caa181213b9410449968532f750a6fb77532d
MC=cgr.dev/chainguard/minio-client@sha256:36ac7ab6d181f259ab62defe0bb55ad1dbf89b152f0b8f49b1c63049374d812f
BUSYBOX=busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
ROWS=${ROWS:-1000000}
BATCH_ROWS=${BATCH_ROWS:-10000}
ITERATIONS=${ITERATIONS:-100}
WARMUP=${WARMUP:-10}
mkdir "$OUT"

scratch=$(mktemp -d)
nonce="vdg-profile-${scratch##*.}"
net=
volume=
minio=
vdg=
cleanup() {
  if [[ -n "$vdg" ]]; then docker rm -f "$vdg" >/dev/null || true; fi
  if [[ -n "$minio" ]]; then docker rm -f "$minio" >/dev/null || true; fi
  if [[ -n "$volume" ]]; then docker volume rm "$volume" >/dev/null || true; fi
  if [[ -n "$net" ]]; then docker network rm "$net" >/dev/null || true; fi
  if [[ -f "$scratch/config.toml" ]]; then unlink "$scratch/config.toml"; fi
  rmdir "$scratch"
}
trap cleanup EXIT

net=$(docker network create "$nonce")
volume=$(docker volume create "$nonce")
docker run --rm --pull never -v "$volume:/data" "$BUSYBOX" chown 65532:65532 /data
minio=$(docker run -d --pull never --network "$net" --network-alias minio   --cpus=4 --memory=3g -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin   -e MINIO_UPDATE=off -v "$volume:/data" "$MINIO" server /data)
for attempt in {1..30}; do
  if docker run --rm --pull never --network "$net"     -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000     "$MC" mb --ignore-existing local/verdigris; then
    break
  fi
  if [[ "$attempt" == 30 ]]; then exit 1; fi
  sleep 1
done
sed 's/vdgbench-minio/minio/g' bench/compare/logs/vdg.toml > "$scratch/config.toml"
chmod 644 "$scratch/config.toml"

vdg=$(docker run -d --pull never --network "$net" --cpus=4 --memory=3g   -p 127.0.0.1::8080 -e VERDIGRIS_CONFIG=/app/config/bench.toml -e TOKIO_WORKER_THREADS=4   -e RUST_LOG=warn -v "$scratch/config.toml:/app/config/bench.toml:ro"   "$VDG_IMAGE" serve --port 8080 --table bench --role all)
wait_ready() {
  port=$(docker port "$vdg" 8080/tcp)
  port=${port##*:}
  for _ in {1..90}; do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  docker logs "$vdg" >&2
  return 1
}
wait_ready

{
  echo "source_commit=$(git rev-parse HEAD)"
  echo "source_dirty_paths=$(git status --porcelain | wc -l)"
  echo "verdigris_image=$(docker image inspect "$VDG_IMAGE" --format '{{.Id}}')"
  echo "minio_image=$(docker image inspect "$MINIO" --format '{{.Id}}')"
  echo "cpu=$(uname -m) host_threads=$(nproc) $(awk '/^MemTotal/{print $1,$2,$3}' /proc/meminfo)"
  echo "kernel=$(uname -r) budget=4cpu/3GiB per service"
  echo "rows=$ROWS batch_rows=$BATCH_ROWS iterations=$ITERATIONS warmup=$WARMUP audit=durable-before-response"
} > "$OUT/machine.txt"
cp "$scratch/config.toml" "$OUT/config.toml"

python3 -B bench/compare/logs/compare.py --system vdg --port "$port" --rows "$ROWS" --batch-rows "$BATCH_ROWS" --iterations 1 --warmup 0 --out "$OUT/seed.json"
profile=(python3 -B bench/compare/logs/profile.py --seed-report "$OUT/seed.json")
"${profile[@]}" --port "$port" --iterations "$ITERATIONS" --warmup "$WARMUP" --out "$OUT/warm.json"

for shape in trace_hit trace_miss trace_cross_tier recent_search minute_aggregation; do
  docker restart "$vdg" >/dev/null
  wait_ready
  "${profile[@]}" --port "$port" --first "$shape" --out "$OUT/restart-$shape.json"
done
