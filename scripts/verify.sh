#!/usr/bin/env bash
# Verdigris verification entry point.
#
# Tool-neutral by design: a human, any coding agent, a git hook, or CI can call
# this. It is the single source of truth for WHICH CHECKS must pass, and
# .github/workflows/ runs the same commands. Integrations should shell out here
# rather than re-encoding the command list.
#
# It does NOT mirror CI's pinned toolchain versions (CI pins node 20 and exact
# mkdocs versions; this runs whatever is on your PATH). A green run here means
# "the same checks passed with my toolchain", not "CI will be green". Where a
# mismatch is detectable it is reported as a warning.
#
# Usage:
#   scripts/verify.sh [--] <path>...   Run only the checks relevant to those paths.
#                                      Fast (sub-3s); intended for edit-time hooks.
#                                      Paths may be absolute or relative to $PWD.
#   scripts/verify.sh --all [group]    Run the full gate set. group = rust|web|docs
#                                      (default: all three). Minutes, not seconds.
#   scripts/verify.sh --help
#
# Exit codes:
#   0  every applicable check ran and passed
#   1  a check failed (details on stderr)
#   2  INCOMPLETE — a check could not run (missing toolchain). Nothing was
#      verified for that area, so this is deliberately not success.
#
# Path routing mirrors the `paths:` filters in .github/workflows/:
#   crates/**   -> rust.yml   fmt lane      (rustfmt on the named files)
#   web/**      -> web.yml    web job       (npm run typecheck)
#   frontend/** -> web.yml    prototype job (node frontend/_verify.js)
set -uo pipefail

# Resolve the script and repo root BEFORE any cd, and keep the caller's cwd so
# relative path arguments can be resolved against it rather than the repo root.
SELF=$(realpath "${BASH_SOURCE[0]}" 2>/dev/null) || SELF=${BASH_SOURCE[0]}
ORIG_PWD=$PWD
REPO_ROOT=$(cd "$(dirname "$SELF")/.." && pwd -P) || exit 1
cd "$REPO_ROOT" || exit 1

failed=0
skipped=0
ran=0

have() { command -v "$1" >/dev/null 2>&1; }

# A check that could not run. Never silently folded into success — an unrun
# check is not a passed check.
note_skip() { skipped=1; printf 'SKIP: %s\n' "$1" >&2; }

# Run a command, printing its output only on failure. Keeps the happy path quiet
# enough to use as an edit-time hook.
run() {
  local label=$1; shift
  local out
  ran=1
  if ! out=$("$@" 2>&1); then
    printf '%s FAILED: %s\n' "$label" "$*" >&2
    printf '%s\n' "$out" >&2
    failed=1
    return 1
  fi
  return 0
}

# Turn a caller-supplied path into a repo-relative one. Relative paths resolve
# against the caller's original cwd, NOT the repo root — otherwise running from
# a subdirectory silently matches nothing and reports success.
# Prints the repo-relative path, or returns 1 if the path is outside the repo.
# Does not require the file to exist (it may have just been deleted).
repo_rel() {
  local p=$1 abs dir base
  [ -n "$p" ] || return 1
  case "$p" in
    /*) abs=$p ;;
     *) abs="$ORIG_PWD/$p" ;;
  esac
  dir=$(dirname "$abs")
  base=$(basename "$abs")
  # Normalize the directory portion; if it doesn't exist, fall back to the
  # lexical path rather than dropping the argument on the floor.
  if dir=$(cd "$dir" 2>/dev/null && pwd -P); then
    abs="$dir/$base"
  fi
  case "$abs" in
    "$REPO_ROOT"/*) printf '%s\n' "${abs#"$REPO_ROOT"/}" ;;
    "$REPO_ROOT")   printf '%s\n' "." ;;
    *) return 1 ;;
  esac
}

# CI pins node 20 (web.yml). Warn on a major-version mismatch rather than
# pretending a local pass implies a CI pass.
warn_node_mismatch() {
  local pinned local_major
  pinned=$(grep -m1 -o 'node-version: *[0-9]*' .github/workflows/web.yml 2>/dev/null | grep -o '[0-9]*')
  [ -n "$pinned" ] || return 0
  local_major=$(node --version 2>/dev/null | sed 's/^v//; s/\..*//')
  [ -n "$local_major" ] || return 0
  if [ "$local_major" != "$pinned" ]; then
    printf 'WARN: node v%s locally, CI pins v%s — a local pass does not guarantee CI.\n' \
      "$local_major" "$pinned" >&2
  fi
}

# ---------------------------------------------------------------- per-path mode

check_paths() {
  local rust_files=() want_web=0 want_prototype=0 p rel

  for p in "$@"; do
    if ! rel=$(repo_rel "$p"); then
      printf 'WARN: ignoring path outside the repo: %s\n' "$p" >&2
      continue
    fi
    case "$rel" in
      node_modules/*|*/node_modules/*|dist/*|*/dist/*|target/*|*/target/*) continue ;;
    esac
    case "$rel" in
      crates/*)   rust_files+=("$rel") ;;
      web/*)      want_web=1 ;;
      frontend/*) want_prototype=1 ;;
    esac
  done

  # Format ONLY the named files. `cargo fmt --all` would rewrite the entire
  # workspace, so editing one file would silently mutate unrelated crates.
  # Formatting is not worth failing an edit over, so this never sets `failed`;
  # CI's `cargo fmt --all -- --check` lane is the real gate.
  if [ ${#rust_files[@]} -gt 0 ]; then
    if have rustfmt; then
      ran=1
      rustfmt --edition "$(rust_edition)" "${rust_files[@]}" >/dev/null 2>&1 || true
    else
      note_skip "rustfmt not installed; crates/ formatting not applied"
    fi
  fi

  if [ "$want_web" = 1 ]; then
    if have npm; then
      warn_node_mismatch
      run "web typecheck" bash -c 'cd web && npm run typecheck'
    else
      note_skip "npm not installed; web/ typecheck not run"
    fi
  fi

  if [ "$want_prototype" = 1 ]; then
    if have node; then
      run "frontend verify" node frontend/_verify.js
    else
      note_skip "node not installed; frontend/ verify not run"
    fi
  fi
}

# Read the workspace edition so rustfmt matches the crates it formats.
rust_edition() {
  grep -m1 '^edition' Cargo.toml 2>/dev/null | sed 's/.*"\(.*\)".*/\1/' || echo 2021
}

# -------------------------------------------------------------------- full mode

# The three-lane matrix is not optional. Per rust.yml's own comment: the default
# build has NO query engine, so code behind datafusion/serve is invisible to a
# default-features run. A broken example and three clippy warnings once hid there.
check_rust() {
  have cargo || { note_skip "cargo not installed; the entire rust group was not run"; return; }
  run "fmt"               cargo fmt --all -- --check
  run "test (default)"    cargo test --workspace
  run "test (datafusion)" cargo test --workspace --features vdg/datafusion
  run "test (serve)"      cargo test --workspace --features vdg/serve
  run "clippy (default)"  cargo clippy --workspace --all-targets -- -D warnings
  run "clippy (serve)"    cargo clippy --workspace --all-targets --features vdg/serve -- -D warnings
  # Type-check only: `apply` needs real AWS at runtime, but must not rot to the
  # point of not compiling.
  run "check-apply"       cargo check -p vdg --features apply
}

check_web() {
  if have npm; then
    warn_node_mismatch
    # CI uses `npm ci` because every runner is cold. Locally that would delete
    # the developer's node_modules on every pre-push run, discarding npm-linked
    # or patched dependencies — so only install when it's actually stale.
    if [ ! -d web/node_modules ] || [ web/package-lock.json -nt web/node_modules ]; then
      run "npm ci" bash -c 'cd web && npm ci'
    fi
    run "web typecheck" bash -c 'cd web && npm run typecheck'
    run "web build"     bash -c 'cd web && npm run build'
  else
    note_skip "npm not installed; the entire web group was not run"
  fi
  # frontend/ is dependency-free by design — plain node, no install step.
  if have node; then
    run "frontend verify" node frontend/_verify.js
  else
    note_skip "node not installed; frontend/ verify not run"
  fi
}

check_docs() {
  if have mkdocs; then
    # A private temp dir: a fixed /tmp path collides between users on a shared
    # host and leaks the build output. Cleaned up on exit.
    local site
    site=$(mktemp -d "${TMPDIR:-/tmp}/verdigris-docs-XXXXXX") || {
      note_skip "could not create a temp dir for the docs build"; return; }
    # --strict: a broken in-site link fails the build instead of shipping a 404.
    run "mkdocs" mkdocs build --strict --site-dir "$site"
    rm -rf "$site"
  else
    note_skip "mkdocs not installed (pip install mkdocs==1.6.1 mkdocs-material==9.7.6)"
  fi
}

usage() {
  # Print the header comment block, stopping at the first non-comment line so
  # this never drifts when the header grows. $SELF is absolute, so this works
  # from any cwd.
  awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$SELF"
}

# ------------------------------------------------------------------------ entry

# Distinguish "no arguments" (help) from "an empty argument" (a caller
# interpolated an unset variable — verifying nothing must not look like success).
if [ $# -eq 0 ]; then
  usage
  exit 0
fi

case "$1" in
  --help|-h)
    usage
    exit 0
    ;;
  --all)
    case "${2:-all}" in
      rust) check_rust ;;
      web)  check_web ;;
      docs) check_docs ;;
      all)  check_rust; check_web; check_docs ;;
      *)    printf 'unknown group %s (want: rust|web|docs)\n' "'${2}'" >&2; exit 1 ;;
    esac
    ;;
  --)
    shift
    check_paths "$@"
    ;;
  -*)
    printf 'unknown option %s (see --help)\n' "'$1'" >&2
    exit 1
    ;;
  *)
    for a in "$@"; do
      if [ -z "$a" ]; then
        echo "empty path argument — refusing to report success without checking anything" >&2
        exit 1
      fi
    done
    check_paths "$@"
    ;;
esac

if [ "$failed" != 0 ]; then
  echo "verify: FAILED" >&2
  exit 1
fi
if [ "$skipped" != 0 ]; then
  echo "verify: INCOMPLETE — some checks could not run (see SKIP above)" >&2
  exit 2
fi
if [ "$ran" = 0 ]; then
  echo "verify: no checks apply to those paths"
else
  echo "verify: OK"
fi
exit 0
