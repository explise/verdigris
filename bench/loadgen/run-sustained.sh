#!/usr/bin/env bash
# Run #21 against a fresh, disk-backed MinIO volume using the release binaries.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${VDG_IMAGE:?set VDG_IMAGE to the release benchmark image}"
: "${LOADGEN:?set LOADGEN to the absolute release loadgen binary path}"
vdg_cpus=${VDG_CPUS:-8}
minio_cpus=${MINIO_CPUS:-4}
if [[ ! "$vdg_cpus" =~ ^[1-9][0-9]*$ || ! "$minio_cpus" =~ ^[1-9][0-9]*$ ]]; then
  echo 'CPU limits must be positive integers' >&2
  exit 2
fi
query_cores=$(( vdg_cpus < 4 ? vdg_cpus : 4 ))
out=$(realpath -m "${OUT:-$(mktemp -d)}")
mkdir -p "$out"
minio_image=cgr.dev/chainguard/minio@sha256:634c989a2f1029162fc2676c263caa181213b9410449968532f750a6fb77532d
mc=cgr.dev/chainguard/minio-client@sha256:36ac7ab6d181f259ab62defe0bb55ad1dbf89b152f0b8f49b1c63049374d812f
busybox=busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
nonce="vdg-sustained-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
net=
vol=
minio=
vdg=
cleanup() {
  if [[ -n "$vdg" ]]; then
    docker inspect --format 'OOMKilled={{.State.OOMKilled}} restarts={{.RestartCount}}' "$vdg" > "$out/vdg-state.txt" || true
    docker logs "$vdg" > "$out/vdg.log" 2>&1 || true
    docker rm -f "$vdg" >/dev/null || true
  fi
  if [[ -n "$minio" ]]; then
    docker inspect --format 'OOMKilled={{.State.OOMKilled}} restarts={{.RestartCount}}' "$minio" > "$out/minio-state.txt" || true
    docker logs "$minio" > "$out/minio.log" 2>&1 || true
    docker rm -f "$minio" >/dev/null || true
  fi
  if [[ -n "$vol" ]]; then docker volume rm "$vol" >/dev/null || true; fi
  if [[ -n "$net" ]]; then docker network rm "$net" >/dev/null || true; fi
}
trap cleanup EXIT
net=$(docker network create "$nonce")
vol=$(docker volume create "$nonce")
docker run --rm -v "$vol:/data" "$busybox" chown 65532:65532 /data
minio=$(docker run -d --network "$net" --network-alias minio \
  --cpus="$minio_cpus" --memory=2g -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -e MINIO_UPDATE=off -v "$vol:/data" "$minio_image" server /data)
mc_run() {
  docker run --rm -i --network "$net" \
    -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 "$mc" "$@"
}
for attempt in {1..30}; do
  if mc_run mb --ignore-existing local/verdigris; then break; fi
  if [[ "$attempt" == 30 ]]; then exit 1; fi
  sleep 1
done
# Check writes before generating the corpus. MinIO can serve reads below its
# minimum free-drive threshold while every upload is rejected.
printf 'fixture preflight\n' | mc_run pipe local/verdigris/_benchmark_probe
mc_run rm local/verdigris/_benchmark_probe
sed "s/^cores = .*/cores = $query_cores/" bench/config/loadtest.toml > "$out/config.toml"
chmod 644 "$out/config.toml"
vdg=$(docker run -d --pull never --network "$net" --cpus="$vdg_cpus" --memory=4g -p 127.0.0.1::8080 \
  -e VERDIGRIS_CONFIG=/app/config/bench.toml -e TOKIO_WORKER_THREADS="$vdg_cpus" -e RUST_LOG=warn \
  --mount "type=bind,src=$out/config.toml,dst=/app/config/bench.toml,readonly" \
  "$VDG_IMAGE" serve --port 8080 --table logs --role all)
wait_ready() {
  port=$(docker port "$vdg" 8080/tcp | awk -F: '{print $NF}')
  url="http://127.0.0.1:$port"
  for attempt in {1..60}; do
    if curl -fsS --max-time 1 "$url/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}
wait_ready
{
  echo "source_commit=$(git rev-parse HEAD)"
  echo "vdg_image=$(docker image inspect "$VDG_IMAGE" --format '{{.Id}}')"
  echo "minio_image=$minio_image"
  echo "budget=vdg ${vdg_cpus} CPU / 4 GiB, disk-backed MinIO ${minio_cpus} CPU / 2 GiB"
  echo "driver=shared host"
  uname -srm
  lscpu
} > "$out/machine.txt"
"$LOADGEN" --url "$url" --table logs --sustain-rows-per-sec "${ROWS_PER_S:-220000}" \
  --duration-secs "${DURATION_SECS:-600}" --batch-rows "${BATCH_ROWS:-10000}" \
  --concurrency "${LOADGEN_CONCURRENCY:-16}" --out "$out/run.json" > "$out/loadgen.log" 2>&1 &
load_pid=$!
while kill -0 "$load_pid" 2>/dev/null; do
  date -u +%FT%TZ >> "$out/containers.txt"
  docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' "$vdg" "$minio" \
    >> "$out/containers.txt" || true
  sleep 10
done
wait "$load_pid"
snapshot_args=()
if [[ "${VDG_BENCH_ALLOW_PRELOAD:-0}" == 1 ]]; then
  snapshot_args+=(--allow-preload)
fi
python3 -B bench/loadgen/check-sustained.py snapshot --url "$url" --table logs "${snapshot_args[@]}" --out "$out/before.json"
docker kill --signal KILL "$vdg" >/dev/null
docker start "$vdg" >/dev/null
wait_ready
python3 -B bench/loadgen/check-sustained.py snapshot --url "$url" --table logs "${snapshot_args[@]}" --out "$out/after.json"
python3 -B bench/loadgen/check-sustained.py verify --run "$out/run.json" --before "$out/before.json" \
  --after "$out/after.json" --out "$out/recovery.json"
