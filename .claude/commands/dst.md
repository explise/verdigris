---
description: Run the deterministic-simulation (DST) suite and interpret the result against ADR-001.
argument-hint: "[test name substring]  (omit to run all)"
allowed-tools: Bash, Read, Grep, Glob
---

Run the deterministic simulation tests:

```
cargo test -p verdigris-storage --test dst
```

If `$ARGUMENTS` is non-empty, pass it as a filter:
`cargo test -p verdigris-storage --test dst -- $ARGUMENTS`

The whole suite runs in well under a second. That speed is the entire point — these
tests cover trillion-row / petabyte scenarios in **logical time**, so no real bytes
move. A slow DST run is itself a signal that something started doing real work.

## What these tests actually assert

Per `docs/dst-architecture.md`, a "trillion-scale" test works by: a fabricated
Iceberg catalog *declaring* the table at scale (no bytes exist), `SimObjectStore`
answering metadata reads instantly with modeled latency added to the sim clock, the
planner planning over the real declared file count, and `ModeledExecutor` returning
per-file latency from the calibration model rather than executing anything.

So a green run proves: orchestration, scheduling, tiering decisions, restore
workflows, cost-estimate accuracy, and the *timing model* at scale.

It does **not** prove absolute GB/s. That comes from separate real-scale calibration
runs. The ADR is emphatic: **DST + calibration, never DST instead of calibration** —
the sim is only as truthful as the latency model fed to `ModeledExecutor`. Never
report a green DST run as evidence of real-world throughput.

## On failure

A DST failure is deterministic and therefore fully reproducible — one seed
reproduces an entire run. Report the seed and the failing assertion, then trace it
back to which seam or state machine is implicated (`ObjectStore`, `Clock`,
`ScanExecutor`, `Rng`) rather than only quoting the panic.

Be especially suspicious of a failure in `estimator_matches_what_the_store_bills`:
the `SimObjectStore` cost model and the user-facing cost estimator are deliberately
the same code, so that test drifting means the two have diverged — which is exactly
the bug the shared-model design exists to prevent.

If the change under review touched control-plane logic, consider also running the
`dst-reviewer` agent, which checks the sans-I/O and four-seam discipline that these
tests assume but cannot themselves enforce.
