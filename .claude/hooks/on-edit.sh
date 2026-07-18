#!/usr/bin/env bash
# PostToolUse hook: mirror the CI gates in .github/workflows/ locally, so a
# failure surfaces the moment a file is edited rather than after a red CI run.
#
# Dispatch is by path, matching the `paths:` filters in the workflows:
#   crates/**   -> rust.yml   fmt lane      (cargo fmt --all)
#   web/**      -> web.yml    web job       (npm run typecheck)
#   frontend/** -> web.yml    prototype job (node frontend/_verify.js)
#
# Reads the hook payload as JSON on stdin. Exit 2 tells Claude Code the check
# failed and feeds the output back to the model; exit 0 is silent success.
#
# Note: `cargo fmt --all` REWRITES files (it doesn't just check like CI does) —
# formatting is not a failure worth interrupting for, so it never exits 2.
# The heavier rust.yml lanes (cargo test across the feature matrix, clippy) are
# deliberately NOT hooked: too slow to run on every edit. Run those before push.
set -uo pipefail

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

# jq isn't installed on all dev machines; python3 is the portable floor here.
file_path=$(python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(d.get("tool_input", {}).get("file_path", ""))' 2>/dev/null)

[ -n "$file_path" ] || exit 0

case "$file_path" in
  */crates/*)
    cargo fmt --all 2>/dev/null || true
    ;;
  */web/*)
    # Skip generated/vendored trees — editing those shouldn't trigger a check.
    case "$file_path" in */node_modules/*|*/dist/*) exit 0 ;; esac
    if ! out=$(cd web && npm run typecheck 2>&1); then
      echo "web typecheck failed (mirrors the web.yml gate):" >&2
      echo "$out" >&2
      exit 2
    fi
    ;;
  */frontend/*)
    if ! out=$(node frontend/_verify.js 2>&1); then
      echo "frontend/_verify.js failed (mirrors the web.yml prototype gate):" >&2
      echo "$out" >&2
      exit 2
    fi
    ;;
esac

exit 0
