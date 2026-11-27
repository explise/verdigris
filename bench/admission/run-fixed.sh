#!/usr/bin/env bash
# Run #29 against a fresh, disk-backed MinIO volume. The supplied release image
# must be built from this checkout. This script deletes only its own containers,
# network and volume. The result directory is kept even when a step fails.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${VDG_IMAGE:?set VDG_IMAGE to the release benchmark image}"
out=$(realpath -m "${OUT:-$(mktemp -d)}")
mkdir -p "$out"
minio_image=cgr.dev/chainguard/minio@sha256:634c989a2f1029162fc2676c263caa181213b9410449968532f750a6fb77532d
mc=cgr.dev/chainguard/minio-client@sha256:36ac7ab6d181f259ab62defe0bb55ad1dbf89b152f0b8f49b1c63049374d812f
busybox=busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
nonce="vdg-mixed-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
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
  --cpus=2 --memory=2g -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
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
cheap_permits=${CHEAP_PERMITS:-1}
if [[ "$cheap_permits" != 1 && "$cheap_permits" != 2 ]]; then
  echo "CHEAP_PERMITS must be 1 or 2 for this experiment" >&2
  exit 2
fi
scheduled_wait_ms=${SCHEDULED_ALERT_WAIT_MS:-50}
if [[ "$scheduled_wait_ms" != 50 && "$scheduled_wait_ms" != 1000 ]]; then
  echo "SCHEDULED_ALERT_WAIT_MS must be 50 or 1000 for this experiment" >&2
  exit 2
fi
sed -e 's/perf81-mixed-minio/minio/g' -e "/^\[query\]$/a cheap_permits = $cheap_permits" \
  -e "/^\[query\]$/a scheduled_alert_wait_ms = $scheduled_wait_ms" \
  bench/results/issue29/vdg.toml > "$out/config.toml"
if [[ -n "${DASHBOARD_SNAPSHOT_CACHE:-}" ]]; then
  sed -i "/^\[query\]$/a dashboard_snapshot_cache = $DASHBOARD_SNAPSHOT_CACHE" "$out/config.toml"
fi
if [[ -n "${RETIRED_GRACE_SECS:-}" ]]; then
  sed -i "s/^retired_grace_secs = .*/retired_grace_secs = $RETIRED_GRACE_SECS/" "$out/config.toml"
fi
chmod 644 "$out/config.toml"
vdg=$(docker run -d --pull never --network "$net" --cpus=4 --memory=2g -p 127.0.0.1::8080 \
  -e VERDIGRIS_CONFIG=/app/config/bench.toml -e TOKIO_WORKER_THREADS=4 -e RUST_LOG=warn \
  --mount "type=bind,src=$out/config.toml,dst=/app/config/bench.toml,readonly" \
  "$VDG_IMAGE" serve --port 8080 --table bench --role all)
port=$(docker port "$vdg" 8080/tcp | awk -F: '{print $NF}')
for attempt in {1..60}; do
  if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then break; fi
  if [[ "$attempt" == 60 ]]; then exit 1; fi
  sleep 1
done
: "${MIXED_BASE_TS_MS:=$(python3 -c 'import time; print((int(time.time()*1000)-200*60_000)//60_000*60_000)')}"
export MIXED_BASE_TS_MS
{
  git rev-parse HEAD
  echo "VDG_IMAGE=$(docker image inspect "$VDG_IMAGE" --format '{{.Id}}')"
  echo "MINIO_IMAGE=$minio_image"
  echo "VDG_CONTAINER=$vdg"
  echo "MINIO_CONTAINER=$minio"
  echo "MIXED_BASE_TS_MS=$MIXED_BASE_TS_MS"
  echo 'Budget: Verdigris 4 CPU / 2 GiB, disk-backed MinIO 2 CPU / 2 GiB'
  uname -srm
  lscpu
} > "$out/machine.txt"
python3 -B bench/admission/mixed.py --port "$port" --rows 20000000 --batches 400 --threads 4 \
  --seed-only --out "$out/seed.json" | tee "$out/seed.log"
for attempt in {1..90}; do
  pending=$(curl -fsS --max-time 8 "http://127.0.0.1:$port/v1/metrics" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["compaction"]["pendingFiles"])') || pending=unknown
  if [[ "$pending" == 0 ]]; then break; fi
  if [[ "$attempt" == 90 ]]; then echo "compaction did not settle" >&2; exit 1; fi
  sleep 2
done
for trial in 1 2 3; do
  python3 -B bench/admission/mixed.py --port "$port" --rows 20000000 --reuse --duration 60 --fixed-rates \
    --trial "$trial" --stats-container "$vdg" --stats-container "$minio" \
    --out "$out/trial-$trial.json" > "$out/trial-$trial.log"
  echo "Completed trial $trial"
done
curl -fsS --max-time 10 "http://127.0.0.1:$port/v1/metrics" > "$out/metrics-after.json"
if [[ "${RUN_MUTATING_TRIAL:-0}" == 1 ]]; then
  python3 -B bench/admission/mixed.py --port "$port" --rows 20000000 --reuse --duration 60 --fixed-rates \
    --trial mutating --stats-container "$vdg" --stats-container "$minio" --ingest-probe-every 10 \
    --out "$out/mutating.json" > "$out/mutating.log"
  curl -fsS --max-time 10 "http://127.0.0.1:$port/v1/metrics" > "$out/metrics-mutating.json"
  if [[ "${CHECK_DRAINED_FRESHNESS:-0}" == 1 ]]; then
    freshness_flags=()
    if [[ "${DASHBOARD_SNAPSHOT_CACHE:-false}" == true ]]; then
      freshness_flags+=(--expect-cache-hits)
    fi
    python3 -B bench/admission/dashboard-freshness.py --port "$port" --mutating "$out/mutating.json" \
      --out "$out/freshness.json" "${freshness_flags[@]}"
  fi
fi
