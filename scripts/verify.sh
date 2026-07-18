#!/usr/bin/env bash
# Verdigris verification entry point.
#
# Tool-neutral by design: a human, any coding agent, a git hook, or CI can call
# this. It is the single source of truth for "what must pass" — .github/workflows/
# runs the same commands, and editor/agent integrations should shell out here
# rather than re-encoding the command list.
#
# Usage:
#   scripts/verify.sh <path>...        Run only the checks relevant to those paths.
#                                      Fast (sub-3s); intended for edit-time hooks.
#   scripts/verify.sh --all [group]    Run the full gate set. group = rust|web|docs
#                                      (default: all three). Minutes, not seconds.
#   scripts/verify.sh --help
#
# Exit: 0 = everything run passed. 1 = something failed (output goes to stderr).
#
# Path routing mirrors the `paths:` filters in .github/workflows/:
#   crates/**   -> rust.yml   fmt lane      (cargo fmt)
#   web/**      -> web.yml    web job       (npm run typecheck)
#   frontend/** -> web.yml    prototype job (node frontend/_verify.js)
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

fail=0
have() { command -v "$1" >/dev/null 2>&1; }

# Run a command, printing its output only on failure. Keeps the happy path quiet
# enough to use as an edit-time hook.
run() {
  local label=$1; shift
  local out
  if ! out=$("$@" 2>&1); then
    printf '%s FAILED: %s\n' "$label" "$*" >&2
    printf '%s\n' "$out" >&2
    fail=1
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------- per-path mode

check_paths() {
  local want_rust=0 want_web=0 want_prototype=0 p

  for p in "$@"; do
    case "$p" in
      */node_modules/*|node_modules/*|*/dist/*|dist/*|*/target/*|target/*) continue ;;
    esac
    case "$p" in
      *crates/*)   want_rust=1 ;;
      *web/*)      want_web=1 ;;
      *frontend/*) want_prototype=1 ;;
    esac
  done

  # cargo fmt REWRITES rather than --check. Formatting is not worth failing an
  # edit over, so this never sets `fail`; CI's `--check` lane is the real gate.
  if [ "$want_rust" = 1 ] && have cargo; then
    cargo fmt --all >/dev/null 2>&1 || true
  fi

  if [ "$want_web" = 1 ] && have npm; then
    run "web typecheck" bash -c 'cd web && npm run typecheck'
  fi

  if [ "$want_prototype" = 1 ] && have node; then
    run "frontend verify" node frontend/_verify.js
  fi
}

# -------------------------------------------------------------------- full mode

# The three-lane matrix is not optional. Per rust.yml's own comment: the default
# build has NO query engine, so code behind datafusion/serve is invisible to a
# default-features run. A broken example and three clippy warnings once hid there.
check_rust() {
  have cargo || { echo "skip rust: cargo not installed" >&2; return; }
  run "fmt"              cargo fmt --all -- --check
  run "test (default)"   cargo test --workspace
  run "test (datafusion)" cargo test --workspace --features vdg/datafusion
  run "test (serve)"     cargo test --workspace --features vdg/serve
  run "clippy (default)" cargo clippy --workspace --all-targets -- -D warnings
  run "clippy (serve)"   cargo clippy --workspace --all-targets --features vdg/serve -- -D warnings
  # Type-check only: `apply` needs real AWS at runtime, but must not rot to the
  # point of not compiling.
  run "check-apply"      cargo check -p vdg --features apply
}

check_web() {
  if have npm; then
    run "npm ci"        bash -c 'cd web && npm ci'
    run "web typecheck" bash -c 'cd web && npm run typecheck'
    run "web build"     bash -c 'cd web && npm run build'
  else
    echo "skip web: npm not installed" >&2
  fi
  # frontend/ is dependency-free by design — plain node, no install step.
  if have node; then
    run "frontend verify" node frontend/_verify.js
  else
    echo "skip frontend: node not installed" >&2
  fi
}

check_docs() {
  if have mkdocs; then
    # --strict: a broken in-site link fails the build instead of shipping a 404.
    run "mkdocs" mkdocs build --strict --site-dir /tmp/verdigris-docs-verify
  else
    echo "skip docs: mkdocs not installed (pip install mkdocs==1.6.1 mkdocs-material==9.7.6)" >&2
  fi
}

# ------------------------------------------------------------------------ entry

case "${1:-}" in
  --help|-h|"")
    # Print the header comment block, stopping at the first non-comment line so
    # this never drifts when the header grows.
    awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"
    exit 0
    ;;
  --all)
    case "${2:-all}" in
      rust) check_rust ;;
      web)  check_web ;;
      docs) check_docs ;;
      all)  check_rust; check_web; check_docs ;;
      *)    echo "unknown group '${2}' (want: rust|web|docs)" >&2; exit 1 ;;
    esac
    ;;
  *)
    check_paths "$@"
    ;;
esac

if [ "$fail" = 0 ]; then
  echo "verify: OK"
fi
exit "$fail"
