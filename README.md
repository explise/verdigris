<div align="center">

# Verdigris

**The layer your infrastructure leaves behind.**

S3-native log storage and query engine — a self-hostable Datadog alternative
that keeps your log data in *your own* cloud account.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Language](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/status-alpha-yellow.svg)](STATUS.md)

</div>

---

Verdigris is a single, plug-and-play Rust binary that deploys on **EKS + S3** and
gives you tiered, queryable log storage at object-storage prices. Logs are written
as compacted Parquet to the customer's **own** S3 bucket and queried **in place** —
no per-GB ingestion margin, no rehydration toll, no proprietary query language.

> *Verdigris* is the green patina that forms on copper as it sits exposed over time —
> the layer a metal accumulates simply by existing in the world. That's what logs are:
> the layer your infrastructure accumulates as it runs. (It's also a quiet Rust pun —
> verdigris is what oxidation matures into.)

## Why Verdigris

Two things the commercial incumbents structurally *can't* fix without breaking their
own business model — and therefore our wedge:

1. **Data sovereignty.** Data never leaves your AWS account. There is no vendor cloud
   in the path charging an ingestion margin on every GB that flows through it.
2. **No rehydration tax.** Queries read Parquet straight out of S3 in place. There is
   no "pull cold logs back into an expensive index to search them" step. Cold logs are
   always live; you pay compute only when you actually query, plus the underlying
   S3/Glacier retrieval cost.

The core design principle, learned from studying Datadog Flex Logs: **never price or
architect around log severity.** Storage is priced by bytes in S3 (effectively free
relative to SaaS vendors); **query speed is a separately provisioned dial** (compute),
decoupled from storage. Severity only decides which S3 prefix / storage class a log
lands in — it is placement, never price.

## Architecture

```
  pods on EKS
      │  (stdout/stderr, OTLP)
      ▼
  Vector / Fluent Bit (DaemonSet)  ·  OTel Collector      ← ingestion
      │
      ▼
  Verdigris ingest  ── batches → Parquet, writes catalog metadata to S3
      │
      ▼
  S3 (your own bucket)                                     ← tiered via S3 lifecycle
      ├─ hot    : S3 Standard          (recent, interactive)
      ├─ warm   : Glacier Instant / Standard-IA
      └─ cold   : Glacier Flexible     (cheapest queryable)
      ▲
      │  (query in place — no rehydration)
  Verdigris query engine  (Apache DataFusion on Parquet)
      │
      ▼
  Query API + Web UI + Grafana datasource
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full component breakdown and
[`docs/adr/`](docs/adr/) for the decisions behind it.

## Features

- **Ingest** — native `POST /v1/ingest` (NDJSON / JSON) for Vector & Fluent Bit, plus a
  native OTLP/HTTP logs receiver (`POST /v1/otlp/logs`) for OpenTelemetry Collectors.
- **Query in place** — Apache DataFusion reads Parquet directly from object storage.
  **SQL** is the query language (portable, no proprietary DSL), plus a concise search
  DSL (`service:auth status>=500 | last 1h`) that compiles to SQL.
- **Tiering** — severity-based write-time routing (hot/warm/cold prefixes) + S3 lifecycle
  policies, applied to the bucket automatically on install.
- **Compaction** — a background job merges the millions of tiny Parquet files streaming
  logs produce into 100 MB–1 GB files (query speed + the Glacier per-object tax).
- **Cost estimator** — before a query scans cold storage, Verdigris surfaces
  "this will scan ~X GB from Glacier and cost ~$Y, continue?" — so Glacier-backed logs
  are *safe to actually use*.
- **Live tail** — `GET /v1/tail` streams matching log events over Server-Sent Events.
- **Deterministic by construction** — nondeterminism lives only behind four seams; the
  control plane is sans-I/O so trillion-row scale is testable in simulation
  ([DST](docs/dst-architecture.md)), not just in production.
- **One `helm install`, done** — a single binary + Helm chart brings up ingest, query,
  UI, and tiering on EKS. Data lands in your bucket via IRSA — no static keys.

## Quickstart

### Run locally (Cargo)

```bash
# 1. Start the server (first build pulls DataFusion, ~1.5 min).
cargo run -p vdg --features serve -- serve --table logs

# 2. In another terminal, keep synthetic logs flowing so `last 1h` stays populated.
cargo run -- ingest --table logs --follow

# 3. Open the UI.
open http://localhost:8080
```

The default build is offline and dependency-light; the heavy bits (DataFusion, the HTTP
server) are behind the `serve` feature. Storage is config-driven
([`config/verdigris.toml`](config/verdigris.toml)) — local filesystem (default),
in-memory, or S3/MinIO, with no recompile to switch.

### Deploy on EKS + S3 (Helm)

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1 \
  --set storage.backend=s3 \
  --set storage.s3.bucket=my-company-logs \
  --set storage.s3.region=us-east-1 \
  --set replicaCount=3 \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<acct>:role/verdigris-s3
```

Data lands in **your** bucket; the query tier is stateless and scales freely, while a
single ingest writer keeps the catalog consistent. Full deployment guide (including the
zero-config local demo, Vector DaemonSet, and MinIO) is in
[`deploy/README.md`](deploy/README.md).

## The `vdg` CLI

| Command | Purpose |
|---|---|
| `vdg ingest` | Ingest logs (`--generate`, `--from <file>`, `--follow`) |
| `vdg query` | Query with a modeled cost estimate |
| `vdg sql` | Run raw SQL (requires `--features datafusion`) |
| `vdg compact` | Merge small Parquet files per tier |
| `vdg manifest` | Inspect the table catalog |
| `vdg lifecycle` | Print (or `--apply`) the S3 lifecycle policy |
| `vdg serve` | Serve the HTTP API + web UI (`--features serve`) |
| `vdg config` / `vdg check` | Show / validate configuration |

The HTTP API surface served by `vdg serve` is documented in
[`docs/API.md`](docs/API.md).

## Project layout

```
crates/
  core/       sans-I/O control plane — batch, clock, cost, estimate, lifecycle,
              manifest, model, rng, search. No I/O, no time, no threads.
  storage/    the ObjectStore seam — real S3 / local / in-memory + SimObjectStore.
  query/      the ScanExecutor seam — ModeledExecutor + DataFusion engine.
  ingest/     records → Arrow → Parquet → store; manifest, routing, compaction.
  vdg/        the CLI + HTTP shell (real Clock, config, all commands, serve).
deploy/       Dockerfile, Helm chart, Grafana datasource.
web/          production web UI (Vite + SolidJS + TypeScript).
frontend/     original no-build prototype (reference for the UI contract).
docs/         architecture, ADRs, HTTP API reference.
```

## Testing — Deterministic Simulation Testing

Verdigris tests at trillion-row / petabyte scale **without running at scale**. The
control plane is sans-I/O and every source of nondeterminism (`Clock`, `Rng`,
`ObjectStore`, `ScanExecutor`) is injected through a seam, so a "trillion-row query"
completes in seconds under a deterministic simulator — no real bytes move. This is a
core constraint that shapes how every component is written, not a test-time add-on.
See [`docs/dst-architecture.md`](docs/dst-architecture.md).

```bash
cargo test --workspace          # fast, offline, deterministic
cargo test -p verdigris-storage # includes the SimObjectStore / DST tests
```

## Status & roadmap

Verdigris is **alpha**. The full local loop works end to end — ingest → tier → compact →
query → cost-estimate → serve to a browser UI — and deploys via Helm. See
[`STATUS.md`](STATUS.md) (UI) and [`BACKEND_STATUS.md`](BACKEND_STATUS.md) (backend &
system) for the current state and what's left.

## Contributing

Contributions are welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to
build, test, and structure changes — in particular the sans-I/O discipline that keeps
the system deterministically testable. By contributing you agree to license your work
under Apache-2.0.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [`NOTICE`](NOTICE) for
attribution of embedded third-party components.
