#!/usr/bin/env bash
# One command: bring up MinIO + a Verdigris ingest node, ramp load until it
# saturates, record what saturated, tear down.
#
#   bench/run-loadtest.sh                       # default ramp
#   bench/run-loadtest.sh --steps 6 --step-secs 10
#   KEEP_UP=1 bench/run-loadtest.sh             # leave the stack running to poke at
#
# Any extra arguments are passed through to the load generator.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f bench/docker-compose.yml"
RESULTS="bench/results"
mkdir -p "$RESULTS"

cleanup() {
  [[ -n "${SAMPLER:-}" ]] && kill "$SAMPLER" 2>/dev/null || true
  if [[ -z "${KEEP_UP:-}" ]]; then
    echo "==> tearing down"
    $COMPOSE down -v >/dev/null 2>&1 || true
  else
    echo "==> stack left up (KEEP_UP set); 'bench/docker-compose.yml down -v' when done"
  fi
}
trap cleanup EXIT

echo "==> building load generator"
cargo build --release --manifest-path bench/loadgen/Cargo.toml

echo "==> starting stack"
$COMPOSE up -d --build

echo "==> waiting for ingest node"
# Poll rather than sleep: the vdg image runs a MinIO health probe at startup and
# how long that takes depends on the machine.
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8080/healthz >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -sf http://localhost:8080/healthz >/dev/null || {
  echo "!! ingest node never came up; logs:" >&2
  $COMPOSE logs --tail 50 vdg >&2
  exit 1
}

# Record what the numbers were produced on. Without this the results are not
# reproducible and the MiB/s-per-core figure cannot be interpreted.
{
  echo "date_utc=$(date -u +%FT%TZ)"
  echo "host_cpus=$(nproc)"
  echo "host_mem_kb=$(awk '/MemTotal/{print $2}' /proc/meminfo)"
  echo "kernel=$(uname -r)"
  echo "docker=$(docker version --format '{{.Server.Version}}')"
  echo "vdg_cpus=${VDG_CPUS:-8}"
  echo "vdg_worker_threads=${VDG_WORKER_THREADS:-8}"
  echo "git_commit=$(git rev-parse --short HEAD)"
  echo "git_dirty=$(git status --porcelain | wc -l)"
} > "$RESULTS/machine.txt"
echo "==> machine recorded to $RESULTS/machine.txt"

bench/sample-resources.sh "$RESULTS/resources.csv" 2 &
SAMPLER=$!

echo "==> ramping"
./bench/loadgen/target/release/verdigris-loadgen \
  --url http://localhost:8080 \
  --out "$RESULTS/latest.json" \
  "$@"

kill "$SAMPLER" 2>/dev/null || true
SAMPLER=""

echo "==> peak CPU by container"
awk -F, 'NR>1 {if ($3+0 > m[$2]) m[$2]=$3+0} END {for (c in m) printf "  %-28s %6.1f%%\n", c, m[c]}' \
  "$RESULTS/resources.csv" | sort
