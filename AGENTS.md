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
scripts/verify.sh [--] <path>...  # checks relevant to those paths; sub-3s
scripts/verify.sh --all           # the full gate set; minutes
scripts/verify.sh --all rust      # or: web | docs
scripts/verify.sh --help
```

Paths may be absolute or relative to your current directory — running it from
inside `web/` or `crates/` works.

| Exit | Meaning |
|---|---|
| `0` | Every applicable check ran and passed. |
| `1` | A check failed; details on stderr. |
| `2` | **INCOMPLETE** — a check could not run (missing toolchain). Nothing was verified for that area, so this is deliberately not success. |

Exit 2 matters: a machine without `cargo` must not be able to run
`scripts/verify.sh --all` and be told everything is fine. An unrun check is not a
passed check.

`.github/workflows/` runs the same *commands*. It does **not** run the same
*toolchain versions* — CI pins node 20 and exact mkdocs versions; this script uses
whatever is on your PATH, and warns on a detectable node major-version mismatch. A
green run here means "these checks passed with my toolchain", not "CI will be
green."

If you add a check, add it to `scripts/verify.sh` **and** `.github/workflows/`
together, or the two silently diverge.

Per-path routing mirrors the workflows' `paths:` filters:

| Edited | Runs | Time |
|---|---|---|
| `crates/**` | `rustfmt` on the named files | ~0.4s |
| `web/**` | `npm run typecheck` | ~2.3s |
| `frontend/**` | `node frontend/_verify.js` | ~0.8s |

Two deliberate asymmetries, so nobody "fixes" them:

- **Per-path mode formats rather than `--check`s, and never fails.** Formatting is
  not worth interrupting an edit over; CI's `cargo fmt --all -- --check` lane is the
  gate. It runs `rustfmt` on the *named files only* — `cargo fmt --all` would rewrite
  the whole workspace, so editing one file would silently mutate unrelated crates.
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

**This rule is enforced, not just documented.** `crates/vdg/tests/seams.rs` greps
for every banned call and fails the build on any hit that is not in its `EXEMPT`
table. It runs in every lane of the matrix, including the default offline one.

**That test is the source of truth for exemptions — not this file.** An earlier
version of this section listed symbols by hand, and went stale the moment the
Clock seam was wired: it still named four functions as legitimate wall-clock
callers after every one of them had been converted. Read `EXEMPT` in that test
rather than trusting a prose list here.

As of writing there are exactly two exemptions, and a second test pins that count
so a third cannot be added without the change appearing in the diff:

| Exemption | Why |
|---|---|
| `crates/vdg/src/realclock.rs` (whole file) | The production `Clock` impl. Reading the wall clock is its job; it lives in the shell so core needn't. |
| `gen_secret()` in `vdg/src/serve.rs` (function-scoped) | Mints a 256-bit auth token. **Must** use OS entropy — routing it through the seeded `Rng` would make auth tokens reproducible from a seed. Never "fix" this one. |

Doc comments naming a banned call are not violations — the gate strips comments
and string literals before matching, so explaining *why* a seam is used does not
break the build.

### 2.4 Measuring durations

`Clock` exposes two readings, and mixing them up is the mistake to avoid:

- `now_millis()` — wall time, for **stamping events**. Can step forwards or
  backwards (NTP). Never subtract two of these to time something.
- `monotonic_micros()` — monotonic, for **measuring durations**. Subtract two
  readings. Microsecond resolution, because millisecond rounding flattens every
  sub-millisecond request to zero and destroys the p50 of fast endpoints.

### 2.5 What a green DST run does and does not prove

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
