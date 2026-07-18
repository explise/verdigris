---
description: Run the full CI gate set locally via scripts/verify.sh and report what fails.
argument-hint: "[rust|web|docs]  (omit to run everything)"
allowed-tools: Bash, Read, Grep, Glob, Edit
---

Run every gate CI runs, so a push doesn't come back red.

```sh
scripts/verify.sh --all $ARGUMENTS
```

If `$ARGUMENTS` is empty this runs all three groups (rust, web, docs); otherwise it
runs just the named one. Exit 0 = passed, exit 1 = failed with output on stderr.

`scripts/verify.sh` is the single source of truth for what must pass — the same
script CI, other agents, and humans use. **Do not expand the command list inline
here**; if a check is missing, add it to `scripts/verify.sh` and
`.github/workflows/` together, or the two will silently diverge.

The edit-time hook already runs the fast per-file checks. This covers what it
deliberately skips: the three-lane test matrix, both clippy lanes, `check-apply`,
the production web build, and `mkdocs --strict`.

Context worth carrying into your report — see `AGENTS.md` §1: the default build has
no query engine, so `cargo test --workspace` alone is *not* green. The matrix is
load-bearing.

## Reporting

Report a compact pass/fail table, then the actual error output for failures — not a
summary of it. If everything passes, say so in one line.

Fix failures only if they are clearly incidental to work already in progress this
session. Otherwise report and ask: a failing lane may be a real bug worth discussing
rather than something to paper over.
