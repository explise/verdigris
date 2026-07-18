---
description: Run the full CI gate set locally — every lane from .github/workflows/ — and report what fails.
argument-hint: "[rust|web|docs]  (omit to run everything)"
allowed-tools: Bash, Read, Grep, Glob, Edit
---

Reproduce locally exactly what CI runs, so a push doesn't come back red.

Scope: `$ARGUMENTS` — if empty, run all three groups. If it names `rust`, `web`, or
`docs`, run only that group.

The PostToolUse hook in this repo already runs the *fast* per-file checks on every
edit (`cargo fmt` on `crates/**`, typecheck on `web/**`, `_verify.js` on
`frontend/**`). This command covers what the hook deliberately leaves out: the
feature matrix, clippy, and the production builds.

## rust — mirrors `.github/workflows/rust.yml`

```
cargo fmt --all -- --check
cargo test --workspace
cargo test --workspace --features vdg/datafusion
cargo test --workspace --features vdg/serve
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features vdg/serve -- -D warnings
cargo check -p vdg --features apply
```

Why the matrix matters, from the workflow's own comment: the default build has **no
query engine**, so code behind `datafusion`/`serve` is *invisible* to a
default-features run. A broken example and three clippy warnings once hid exactly
there. Do not shortcut to `cargo test --workspace` alone and call it green.

`check-apply` is type-check only — its runtime needs real AWS, but it must not rot
to the point of not compiling.

## web — mirrors `.github/workflows/web.yml`

```
cd web && npm ci && npm run typecheck && npm run build
node frontend/_verify.js
```

`frontend/` is dependency-free by design — plain node, no install step. Don't add one.

## docs — mirrors `.github/workflows/docs.yml`

```
mkdocs build --strict
```

`--strict` makes any broken in-site link fail rather than shipping a 404. Requires
`mkdocs==1.6.1` + `mkdocs-material==9.7.6`; if mkdocs isn't installed locally, say
so and skip this group rather than installing it unasked.

## Reporting

Run the groups and report a compact pass/fail table, then the actual error output
for anything that failed — not a summary of it. If everything passes, say so in one
line.

Fix failures only if they are clearly incidental to work already in progress this
session. Otherwise report them and ask — a failing lane may be a real bug worth
discussing rather than something to paper over.
