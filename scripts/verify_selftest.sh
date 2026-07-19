#!/usr/bin/env bash
# Self-test for scripts/verify.sh.
#
# verify.sh gates every push and AGENTS.md tells contributors to trust it, so it
# needs its own coverage — otherwise a regression in the gate is invisible until
# it waves through a broken change.
#
# Most of these cases are regressions with a history: verify.sh once reported
# "verify: OK" with exit 0 in three separate situations where it had checked
# nothing at all. Those are marked REGRESSION below. Do not delete them.
#
# Usage:  scripts/verify_selftest.sh
# Exit:   0 all passed, 1 something failed.
#
# Cases needing a toolchain that isn't installed are reported as SKIP and do not
# fail the run — but CI installs everything, so nothing is skipped there.
set -uo pipefail

SELF=$(realpath "${BASH_SOURCE[0]}")
REPO_ROOT=$(cd "$(dirname "$SELF")/.." && pwd -P)
cd "$REPO_ROOT" || exit 1

VERIFY="$REPO_ROOT/scripts/verify.sh"
BASH_BIN=$(command -v bash)

pass=0; failed=0; skipped=0
TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/verify-selftest-XXXXXX")

# The format-scoping cases temporarily dirty two REAL files in two DIFFERENT
# crates — they have to be real crate members or `cargo fmt --all` would not
# reach them and the test would pass vacuously. Contents are captured up front
# and restored by the exit trap, so an interrupted run cannot leave them dirty.
FMT_NAMED="crates/query/src/lib.rs"
FMT_UNNAMED="crates/ingest/src/schema.rs"
FMT_NAMED_ORIG=$(cat "$FMT_NAMED")
FMT_UNNAMED_ORIG=$(cat "$FMT_UNNAMED")

# shellcheck disable=SC2329  # invoked via `trap ... EXIT`, not directly.
cleanup() {
  rm -rf "$TMPROOT"
  # Restore verbatim rather than via git, so this is safe on a dirty tree.
  printf '%s\n' "$FMT_NAMED_ORIG"   > "$REPO_ROOT/$FMT_NAMED"
  printf '%s\n' "$FMT_UNNAMED_ORIG" > "$REPO_ROOT/$FMT_UNNAMED"
}
trap cleanup EXIT

ok()   { printf '  ok    %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  FAIL  %s\n     %s\n' "$1" "$2" >&2; failed=$((failed+1)); }
skip() { printf '  skip  %s (%s)\n' "$1" "$2"; skipped=$((skipped+1)); }

have() { command -v "$1" >/dev/null 2>&1; }

# Assert verify.sh exits with an expected code. Extra args go to verify.sh.
expect_exit() {
  local want=$1 name=$2; shift 2
  local got
  "$VERIFY" "$@" >/dev/null 2>&1
  got=$?
  if [ "$got" = "$want" ]; then ok "$name"; else bad "$name" "exit=$got want=$want"; fi
}

# A PATH containing the shell utilities verify.sh needs, but deliberately no
# cargo / rustfmt / npm / node / mkdocs — to exercise the missing-toolchain path.
make_toolless_path() {
  local d="$TMPROOT/toolless"
  mkdir -p "$d"
  local t
  for t in bash sh awk sed grep dirname basename realpath mktemp rm ls cat env; do
    if have "$t"; then ln -sf "$(command -v "$t")" "$d/$t"; fi
  done
  printf '%s\n' "$d"
}

echo "verify.sh self-test"

# --------------------------------------------------------------- argument handling

expect_exit 0 "no arguments prints help"            --help
expect_exit 0 "--help"                              --help
expect_exit 1 "unknown option is rejected"          --bogus
expect_exit 1 "unknown --all group is rejected"     --all bogus

# REGRESSION: `verify.sh ""` — what a caller interpolating an unset variable
# produces — used to print help and exit 0, reporting success having checked
# nothing.
expect_exit 1 "REGRESSION empty path argument rejected" ""

# Bare invocation with zero args is help, distinct from an empty first arg.
if "$VERIFY" >/dev/null 2>&1; then
  ok "zero args is help, not an error"
else
  bad "zero args is help" "expected exit 0"
fi

# --------------------------------------------------------------- path resolution

expect_exit 0 "unrelated path is a clean no-op"     README.md
expect_exit 0 "node_modules is skipped"             web/node_modules/x/y.ts
expect_exit 0 "dist is skipped"                     web/dist/x.js
expect_exit 0 "target is skipped"                   target/debug/x.rs
expect_exit 0 "path outside the repo is ignored"    /etc/hosts
expect_exit 0 "-- separator is honoured"            -- README.md

# REGRESSION: routing used to glob-match the raw argument string, so a path
# relative to a subdirectory matched nothing and reported OK without checking.
if have npm && [ -d web/node_modules ]; then
  out=$(cd web && "$VERIFY" src/lib/api.ts 2>&1); rc=$?
  if [ "$rc" = 0 ] && ! printf '%s' "$out" | grep -q "no checks apply"; then
    ok "REGRESSION relative path from a subdir runs its check"
  else
    bad "REGRESSION relative path from a subdir runs its check" "exit=$rc out=$out"
  fi
else
  skip "REGRESSION relative path from a subdir runs its check" "npm or web/node_modules missing"
fi

# --help reads the script source; it must not break once verify.sh has cd'd to
# the repo root. REGRESSION: this used to emit an awk error from a subdirectory.
out=$(cd crates && "$VERIFY" --help 2>&1)
if printf '%s' "$out" | grep -qi "awk\|cannot open"; then
  bad "REGRESSION --help works from a subdirectory" "$out"
else
  ok "REGRESSION --help works from a subdirectory"
fi

# ------------------------------------------------------- missing-toolchain accounting

# REGRESSION: a skipped check used to leave the failure counter at zero, so a
# machine with no toolchain printed "verify: OK" and exited 0 having verified
# nothing. Skips must surface as exit 2 (INCOMPLETE).
TOOLLESS=$(make_toolless_path)
for group in rust web docs; do
  PATH="$TOOLLESS" "$BASH_BIN" "$VERIFY" --all "$group" >/dev/null 2>&1
  rc=$?
  if [ "$rc" = 2 ]; then
    ok "REGRESSION --all $group without toolchain is INCOMPLETE"
  else
    bad "REGRESSION --all $group without toolchain is INCOMPLETE" "exit=$rc want=2"
  fi
done

PATH="$TOOLLESS" "$BASH_BIN" "$VERIFY" --all >/dev/null 2>&1
rc=$?
if [ "$rc" = 2 ]; then
  ok "REGRESSION --all with no toolchain at all is INCOMPLETE"
else
  bad "REGRESSION --all with no toolchain at all is INCOMPLETE" "exit=$rc want=2"
fi

# A skip must be announced, not silent.
out=$(PATH="$TOOLLESS" "$BASH_BIN" "$VERIFY" --all rust 2>&1)
if printf '%s' "$out" | grep -q "SKIP:"; then
  ok "a skipped check prints SKIP"
else
  bad "a skipped check prints SKIP" "$out"
fi

# ------------------------------------------------------------- format scoping

# REGRESSION: per-path mode ran `cargo fmt --all`, reformatting the whole
# workspace — so editing one file silently rewrote unrelated crates.
#
# This MUST use real files that belong to real crates. An earlier version of this
# test used scratch files under crates/ that were not part of any crate, so
# `cargo fmt --all` never reached them and the assertion passed vacuously — it
# could not detect the very regression it exists for. Mutation-tested: reverting
# verify.sh to `cargo fmt --all` now fails the unnamed-file assertion.
if have rustfmt; then
  named="$FMT_NAMED"      # the file we ask verify.sh to check
  unnamed="$FMT_UNNAMED"  # a file in a DIFFERENT crate, must be left alone

  # Both are restored from FMT_*_ORIG by the exit trap.
  printf '%s\n' "$FMT_NAMED_ORIG"   | sed 's/^use /use      /' > "$named"
  printf '%s\n' "$FMT_UNNAMED_ORIG" | sed 's/^use /use      /' > "$unnamed"
  before_unnamed=$(cat "$unnamed")

  "$VERIFY" "$named" >/dev/null 2>&1

  if [ "$(cat "$unnamed")" = "$before_unnamed" ]; then
    ok "REGRESSION formatting does not touch files in other crates"
  else
    bad "REGRESSION formatting does not touch files in other crates" \
        "$unnamed was rewritten while verifying $named"
  fi

  if [ "$(cat "$named")" = "$FMT_NAMED_ORIG" ]; then
    ok "the named file IS formatted"
  else
    bad "the named file IS formatted" "$named was left unformatted"
  fi

  printf '%s\n' "$FMT_NAMED_ORIG"   > "$named"
  printf '%s\n' "$FMT_UNNAMED_ORIG" > "$unnamed"
else
  skip "REGRESSION formatting scoping" "rustfmt not installed"
fi

# --------------------------------------------------------------- failure paths

# A real type error must fail, not pass. Guards against the checks silently
# becoming no-ops.
if have npm && [ -d web/node_modules ]; then
  probe="web/src/__selftest_probe.ts"
  printf 'const n: number = "not a number";\nexport default n;\n' > "$probe"
  "$VERIFY" "$probe" >/dev/null 2>&1; rc=$?
  rm -f "$probe"
  if [ "$rc" = 1 ]; then
    ok "a genuine type error fails the web check"
  else
    bad "a genuine type error fails the web check" "exit=$rc want=1"
  fi
else
  skip "a genuine type error fails the web check" "npm or web/node_modules missing"
fi

# The Claude Code adapter must translate a verify.sh failure (1) into the exit 2
# that Claude Code reads as "surface this to the model".
HOOK="$REPO_ROOT/.claude/hooks/on-edit.sh"
if [ -x "$HOOK" ] && have python3 && have npm && [ -d web/node_modules ]; then
  probe="web/src/__selftest_hook.ts"
  printf 'const n: number = "not a number";\nexport default n;\n' > "$probe"
  printf '{"tool_name":"Edit","tool_input":{"file_path":"%s/%s"}}' "$REPO_ROOT" "$probe" \
    > "$TMPROOT/payload.json"
  "$HOOK" < "$TMPROOT/payload.json" >/dev/null 2>&1; rc=$?
  rm -f "$probe"
  if [ "$rc" = 2 ]; then
    ok "claude hook maps a failure to exit 2"
  else
    bad "claude hook maps a failure to exit 2" "exit=$rc want=2"
  fi

  # Malformed payload must be a silent no-op, never a spurious failure.
  echo 'not json' | "$HOOK" >/dev/null 2>&1; rc=$?
  if [ "$rc" = 0 ]; then
    ok "claude hook ignores a malformed payload"
  else
    bad "claude hook ignores a malformed payload" "exit=$rc want=0"
  fi
else
  skip "claude hook adapter cases" "hook, python3, npm or web/node_modules missing"
fi

# ------------------------------------------------------------------- shell syntax

for s in scripts/verify.sh scripts/verify_selftest.sh .claude/hooks/on-edit.sh; do
  if [ -f "$s" ]; then
    if "$BASH_BIN" -n "$s" 2>/dev/null; then
      ok "bash -n $s"
    else
      bad "bash -n $s" "syntax error"
    fi
  fi
done

# ------------------------------------------------------------------------ summary

echo
printf 'passed=%d failed=%d skipped=%d\n' "$pass" "$failed" "$skipped"
[ "$failed" = 0 ] || exit 1
exit 0
