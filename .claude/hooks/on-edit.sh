#!/usr/bin/env bash
# Claude Code PostToolUse adapter.
#
# Deliberately thin: this file's ONLY job is to turn a Claude Code hook payload
# into an argument for scripts/verify.sh. All knowledge of what to check lives
# there, so other agents, CI, and humans run the identical checks.
#
# If you are not using Claude Code, ignore this file and call scripts/verify.sh
# directly. See AGENTS.md.
#
# Payload arrives as JSON on stdin. Exit 2 tells Claude Code the check failed and
# feeds the output back to the model; exit 0 is silent success.
set -uo pipefail

cd "${CLAUDE_PROJECT_DIR:-$(dirname "${BASH_SOURCE[0]}")/../..}" || exit 0

# jq isn't installed on all dev machines; python3 is the portable floor here.
file_path=$(python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(d.get("tool_input", {}).get("file_path", ""))' 2>/dev/null)

[ -n "$file_path" ] || exit 0

# verify.sh exits 1 on failure; Claude Code wants 2 to surface it to the model.
if ! out=$(scripts/verify.sh "$file_path" 2>&1); then
  printf '%s\n' "$out" >&2
  exit 2
fi

exit 0
