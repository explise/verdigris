# AGENTS.md — working in this repo

Contract for any coding agent or contributor working on Verdigris: how to verify a
change, and the architectural rules that are easy to violate by accident.

Tool-neutral on purpose. Nothing here assumes a particular agent, editor, or model.
Tool-specific wiring lives elsewhere and should *reference* this file rather than
restate it:

| File | Purpose |
|---|---|
| `AGENTS.md` (this file) | Cross-tool contract. The source of truth. |
| `CLAUDE.md` | Product brief + architecture. Read for *what* Verdigris is. |
| `.claude/` | Claude Code wiring (hooks, slash commands). Thin; calls `scripts/verify.sh`. |
| `frontend/AGENTS.md` | Contract for the prototype frontend specifically. |
| `docs/dst-architecture.md` | ADR-001. The authority on everything in §2 below. |

---

## 1. Verifying a change

**One entry point.** Do not hand-assemble command lists — they drift from CI.

```sh
scripts/verify.sh <path>...       # checks relevant to those paths; sub-3s
scripts/verify.sh --all           # the full gate set; minutes
scripts/verify.sh --all rust      # or: web | docs
scripts/verify.sh --help
```

Exit 0 = passed. Exit 1 = failed, with output on stderr.

`.github/workflows/` runs the same commands. If you add a check, add it there and in
`scripts/verify.sh` together, or the two silently diverge.

Per-path routing mirrors the workflows' `paths:` filters:

| Edited | Runs | Time |
|---|---|---|
| `crates/**` | `cargo fmt --all` | ~0.4s |
| `web/**` | `npm run typecheck` | ~2.3s |
| `frontend/**` | `node frontend/_verify.js` | ~0.8s |

Two deliberate asymmetries, so nobody "fixes" them:

- **`cargo fmt` rewrites rather than `--check`s in per-path mode, and never fails.**
  Formatting is not worth interrupting an edit over. CI's `--check` lane is the gate.
- **The heavy lanes are not in per-path mode.** The three-lane test matrix and both
  clippy lanes only run under `--all`. Too slow for every edit.

**Run `--all` before pushing.** And do not shortcut the matrix: per `rust.yml`'s own
comment, the default build has *no query engine*, so code behind `datafusion`/`serve`
is invisible to a default-features run. A broken example and three clippy warnings
once hid exactly there. `cargo test --workspace` alone is not green.

The deterministic-simulation suite is fast enough to run constantly:

```sh
cargo test -p verdigris-storage --test dst    # 4 tests, ~0.4s
```

---

## 2. The DST discipline — read `docs/dst-architecture.md` first

ADR-001 makes deterministic simulation a **core architecture constraint, not a
test-time add-on.** These rules cannot be retrofitted, so a violation is much cheaper
to catch in review than after it spreads.

The control plane is **sans-I/O**: pure `(state, event) -> (state, effects)`. No
direct time, I/O, threads, or randomness in core logic. The shell executes effects
through four seams: `ObjectStore`, `Clock`, `ScanExecutor`, `Rng`.

### 2.1 The layering rule — the one most often gotten backwards

> **Core holds seam *traits* and their *deterministic* implementations only.
> Every real, nondeterministic implementation lives in the shell.**

`crates/vdg/src/realclock.rs` says so in its own doc comment: *"Lives in the shell
(not core) so the core stays free of any real time source."*

Concretely:

- `crates/core/src/clock.rs` — the `Clock` trait + `SimClock`. No real time source.
- `crates/core/src/rng.rs` — the `Rng` trait + `SeededRng`. No real entropy.
- `crates/vdg/src/realclock.rs` — `RealClock`, the production impl. In the shell.

The seam files are **not** exempt from the rules. A genuine `SystemTime::now()`
appearing in `core/src/clock.rs` would be a real violation — it would mean a real
time source had migrated into core. Do not read "this file defines the Clock seam"
as "wall-clock calls are fine here."

**Core / control plane — rules apply strictly:** `crates/core`, `crates/storage`,
`crates/query`, `crates/ingest`, and any logic DST must simulate (ingest batching,
compaction scheduling, tiering state machine, Glacier restore, Iceberg
catalog/planner, cost estimator, scan fan-out).

**Shell — real impls belong here:** `crates/vdg` (`main.rs`, `serve.rs`,
`realclock.rs`, `lifecycle_apply.rs`) — CLI parsing, HTTP handlers, telemetry, and
the production seam impls.

### 2.2 What counts as a violation

1. **Wall-clock reads in core** — `SystemTime::now()`, `Instant::now()`.
2. **Unseeded randomness in core** — `rand::thread_rng()`, `rand::random()`. One
   seed must reproduce an entire run.
3. **Raw concurrency** — `std::thread::spawn`, `std::thread::sleep`. All concurrency
   goes through the runtime so the simulator controls scheduling.
4. **I/O bypassing the store seam** — direct `std::fs`, direct `aws-sdk-s3`, or
   network calls in core rather than through `object_store::ObjectStore`.
5. **Execution bypassing `ScanExecutor`** — core calling DataFusion directly. Prod
   gets `DataFusionExecutor`; sim gets `ModeledExecutor`.
6. **Cost-model divergence** — the `SimObjectStore` latency/cost model and the
   user-facing cost estimator are deliberately *the same code*. ADR-001 cites this
   as the reason we hand-rolled `SimObjectStore` instead of using
   `madsim-aws-sdk-s3`. Computing cost in one without the other is a real finding.
7. **`#[cfg]` threaded through logic** — real vs sim is the crate-swap pattern, not
   conditional compilation sprinkled through the control plane.

### 2.3 Known-legitimate hits — do not report these

Grepping the rules above surfaces exactly these. Every one is correct as written.
Verified against the tree; line numbers drift, so confirm the line still does what
this says before dismissing it.

| Location | Why it is fine |
|---|---|
| `core/src/clock.rs:2`, `core/src/rng.rs:2` | `//!` doc comments *naming* the banned calls to explain the rule. Not code. |
| `vdg/src/realclock.rs:16` | The production `Clock` impl. Reading the wall clock is its job; it lives in the shell so core needn't. |
| `vdg/src/main.rs:278,447` | CLI-level timestamps, in the shell. |
| `vdg/src/serve.rs:343,1345` | HTTP request-latency telemetry, in the shell. |
| `vdg/src/serve.rs:1132` | `gen_secret()` mints a 256-bit auth token. **Must** use OS entropy — routing it through the seeded `Rng` would make auth tokens reproducible from a seed. Never "fix" this one. |

### 2.4 What a green DST run does and does not prove

Proves: orchestration, scheduling, tiering decisions, restore workflows,
cost-estimate accuracy, and the *timing model* at scale.

Does not prove: absolute GB/s. That comes from separate real-scale calibration runs.
ADR-001 is emphatic — **DST + calibration, never DST instead of calibration.** The
sim is only as truthful as the latency model fed to `ModeledExecutor`. Never present
a green DST run as evidence of real-world throughput.

A DST failure is deterministic and fully reproducible: one seed reproduces the run.
Report the seed and the failing assertion, and trace it to the implicated seam.

Be especially suspicious of `estimator_matches_what_the_store_bills` failing — that
means the sim cost model and the user-facing estimator have diverged, which is the
exact bug the shared-model design exists to prevent.

---

## 3. Frontend

Read `web/STATUS.md` first for current status across both frontends, then
`frontend/AGENTS.md` if touching the prototype.

There are two: `frontend/` (vanilla no-build prototype, the reference) and `web/`
(Vite + SolidJS, what ships). Backend integration is one file in each —
`frontend/api.js` and `web/src/lib/api.ts` — and those return shapes are the API
contract the UI renders.

Brand is oxidized-copper green. Design tokens live in `:root` in
`frontend/styles.css`. Never hardcode colors.

---

## 4. Conventions

- **SQL, not a proprietary DSL.** Portability is a feature.
- **Make cost legible.** No surprise bills — estimate before you scan.
- **Deterministic by construction.** Nondeterminism lives only behind seams.
- Match surrounding code: its comment density, naming, and idiom.
- The binary is `vdg`. The brand is `Verdigris`.
