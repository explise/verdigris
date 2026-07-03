#!/usr/bin/env bash
#
# run.sh — build and run Verdigris (vdg) end to end.
#
# Builds the `vdg` binary with the `serve` feature (which pulls in the
# DataFusion query engine), seeds the local store with synthetic logs the
# first time, then starts the HTTP API + static frontend.
#
# Everything runs FULLY OFFLINE against the local filesystem backend
# (./data), per config/verdigris.toml — no S3/AWS credentials needed.
#
# Usage:
#   ./run.sh                 # build, seed (if empty), serve on :8080
#   ./run.sh --port 9090     # serve on a different port
#   ./run.sh --records 40000 # seed N synthetic records (default 20000)
#   ./run.sh --reseed        # wipe ./data and re-ingest fresh logs
#   ./run.sh --no-serve      # build + seed only, then exit (no server)
#   ./run.sh --release        # build in release mode (slower build, faster run)
#
set -euo pipefail

# Always operate from the repo root (directory this script lives in).
cd "$(dirname "$0")"

# ---- defaults ---------------------------------------------------------------
PORT=8080
TABLE="logs"
RECORDS=20000
RESEED=0
SERVE=1
PROFILE="dev"          # cargo dev profile -> target/debug
BUILD_FLAGS=()

# ---- arg parsing ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)     PORT="$2"; shift 2 ;;
    --records)  RECORDS="$2"; shift 2 ;;
    --table)    TABLE="$2"; shift 2 ;;
    --reseed)   RESEED=1; shift ;;
    --no-serve) SERVE=0; shift ;;
    --release)  PROFILE="release"; BUILD_FLAGS+=("--release"); shift ;;
    -h|--help)
      sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

# cargo's "dev" profile emits to target/debug; release to target/release.
if [[ "$PROFILE" == "release" ]]; then BIN="target/release/vdg"; else BIN="target/debug/vdg"; fi

# ---- preflight --------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo not found. Install Rust from https://rustup.rs" >&2
  exit 1
fi

echo "==> Building vdg (--features serve, ${PROFILE} profile)…"
cargo build --features serve -p vdg ${BUILD_FLAGS[@]+"${BUILD_FLAGS[@]}"}

# ---- seed data --------------------------------------------------------------
if [[ "$RESEED" -eq 1 ]]; then
  echo "==> --reseed: removing ./data"
  rm -rf ./data
fi

# Seed only if the table has no manifest yet (avoids piling up on every run).
MANIFEST="./data/${TABLE}/_metadata/manifest.json"
if [[ ! -f "$MANIFEST" ]]; then
  echo "==> Seeding '${TABLE}' with ${RECORDS} synthetic log records…"
  "$BIN" ingest --table "$TABLE" --generate "$RECORDS"
else
  echo "==> '${TABLE}' already has data (${MANIFEST}) — skipping seed."
  echo "    Use --reseed to wipe and regenerate."
fi

echo "==> Manifest:"
"$BIN" manifest --table "$TABLE" | head -1

# ---- serve ------------------------------------------------------------------
if [[ "$SERVE" -eq 0 ]]; then
  echo "==> --no-serve: build + seed done."
  exit 0
fi

# Fail early with a clear message if the port is taken.
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "error: port ${PORT} is already in use. Stop the other process or pass --port <N>." >&2
  exit 1
fi

echo ""
echo "==> Starting Verdigris on http://localhost:${PORT}"
echo "    UI:        http://localhost:${PORT}/"
echo "    Query API: POST http://localhost:${PORT}/v1/query   body: {\"sql\":\"error\"}"
echo "    Ctrl-C to stop."
echo ""
exec "$BIN" serve --port "$PORT" --table "$TABLE"
