---
description: Run the deterministic-simulation (DST) suite and interpret the result against ADR-001.
argument-hint: "[test name substring]  (omit to run all)"
allowed-tools: Bash, Read, Grep, Glob
---

```sh
cargo test -p verdigris-storage --test dst
```

If `$ARGUMENTS` is non-empty, append `-- $ARGUMENTS` to filter.

The suite runs in well under a second. That speed is the point: these tests cover
trillion-row / petabyte scenarios in **logical time**, so no real bytes move. A slow
DST run is itself a signal that something started doing real work.

Read **`AGENTS.md` §2.5** before reporting the result. The two things that matter:

- A green run proves orchestration, scheduling, tiering, restore workflows,
  cost-estimate accuracy, and the timing model. It does **not** prove absolute GB/s
  — that needs calibration. Never present green DST as evidence of throughput.
- `estimator_matches_what_the_store_bills` failing means the sim cost model and the
  user-facing estimator have diverged — the exact bug the shared-model design
  exists to prevent. Treat it as higher-severity than a generic test failure.

Failures are deterministic and fully reproducible from one seed. Report the seed and
the failing assertion, and trace it to the implicated seam (`ObjectStore`, `Clock`,
`ScanExecutor`, `Rng`) rather than only quoting the panic.

If the change touched control-plane logic, also consider the `dst-reviewer` agent —
it checks the sans-I/O and four-seam discipline that these tests assume but cannot
themselves enforce.
