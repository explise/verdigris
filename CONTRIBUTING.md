# Contributing to Verdigris

Thanks for your interest in Verdigris. This document explains how to build, test, and
structure changes. The single most important thing to internalize is the **sans-I/O,
seam-based discipline** (below) — it is what makes the system deterministically testable
at scale, and it cannot be retrofitted.

## Ground rules

- **Be excellent to each other.** See [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
- **License.** By submitting a contribution you agree to license it under
  [Apache-2.0](LICENSE). Keep the `SPDX-License-Identifier: Apache-2.0` convention when
  adding source-file headers.
- **Discuss large changes first.** Open an issue before a big refactor or a new component
  so the design (and any ADR) can be agreed before code is written.

## Building

Verdigris is a Cargo workspace. The default build is offline and dependency-light; the
heavy dependencies live behind feature flags.

```bash
cargo build                        # core + CLI, offline, no query engine
cargo build -p vdg --features serve   # + DataFusion query engine + axum HTTP server
```

Feature flags:

| Feature | Pulls in |
|---|---|
| `datafusion` | the real DataFusion query engine (`vdg sql`) |
| `serve` | the HTTP API + static UI (implies `datafusion`) |

The DataFusion dependency stack is **version-pinned** (DataFusion 54 → `object_store`
0.13, `arrow`/`parquet` 58) so our `Arc<dyn ObjectStore>` and Parquet bytes interop
cleanly with the engine. Do not bump these independently.

## Testing

```bash
cargo test --workspace             # fast, offline, deterministic — run this before every PR
cargo test -p verdigris-storage    # includes the SimObjectStore / DST tests
```

All tests must be **deterministic**: no wall-clock dependence, no unseeded randomness, no
network. A test that flakes is a bug in the test or in a seam, not "just flaky."

## The sans-I/O discipline (read this before touching `crates/core`)

Verdigris is architected so its **control plane is a pure, sans-I/O core**: business logic
is `(state, event) -> (state, effects)` with **no** direct I/O, time, threads, or
randomness. Every source of nondeterminism is injected through one of four seams:

| Seam | Production impl | Simulation impl |
|---|---|---|
| `ObjectStore` (the `object_store` crate trait) | `AmazonS3` / local / in-memory | `SimObjectStore` (modeled latency, Glacier-restore semantics, fault injection) |
| `Clock` | real time | madsim logical time |
| `ScanExecutor` | `DataFusionExecutor` | `ModeledExecutor` |
| `Rng` | OS entropy | seeded (one seed reproduces a whole run) |

Concrete rules, enforced in review:

- **No `SystemTime::now()` / `Instant::now()`** outside a `Clock` impl.
- **No raw thread spawning, no `std::thread::sleep`** — concurrency goes through the async
  runtime so the simulator controls scheduling.
- **All randomness through the injected `Rng`.** No `rand::thread_rng()` in core logic.
- **All storage through the `ObjectStore` seam.** No direct filesystem or AWS SDK calls in
  `crates/core`.

The payoff: a trillion events become a trillion cheap function calls, so trillion-row
scale is testable in seconds. See [`docs/dst-architecture.md`](docs/dst-architecture.md).

## Code layout

```
crates/core      pure control plane (no I/O). Where most logic lives.
crates/storage   ObjectStore seam + SimObjectStore.
crates/query     ScanExecutor seam + DataFusion engine (feature-gated).
crates/ingest    the write path: records → Arrow → Parquet → store; compaction.
crates/vdg       the shell: CLI, config loading, real Clock, HTTP serve.
```

If you find yourself importing `tokio::time`, `std::fs`, or `aws_sdk_*` inside
`crates/core`, stop — that logic belongs behind a seam in `crates/vdg` or `crates/storage`.

## Commit & PR conventions

- **Conventional-commit-ish** subjects: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`,
  `chore:`. Keep the subject imperative and under ~72 chars.
- Keep PRs focused. A behavioral change and a wide reformat should be separate PRs.
- Every PR must keep `cargo test --workspace` green and `cargo clippy` clean.
- Update the relevant status doc (`STATUS.md`, `BACKEND_STATUS.md`) and any ADR when you
  change architecture or the API contract.

## Decision records (ADRs)

Architecturally significant decisions are recorded in [`docs/adr/`](docs/adr/). If your
change alters a seam, the storage/catalog format, the query interface, or the deployment
topology, add or update an ADR in the same PR.

## Frontend

The web UI has its own contract. If you touch `web/` or `frontend/`, read
[`frontend/AGENTS.md`](frontend/AGENTS.md) and [`STATUS.md`](STATUS.md) first — the backend
integration is a single swap point in each (`web/src/lib/api.ts`, `frontend/api.js`), and
the return shapes there are the API contract the UI renders.
