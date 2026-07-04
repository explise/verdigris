# Quickstart

Get the whole loop — ingest → tier → query → cost-estimate → browser UI — running
on your laptop in about five minutes. No AWS, no S3: the default storage backend is
your local filesystem, so everything here is fully offline.

!!! note "What you need"
    A recent Rust toolchain (`cargo`). The first `serve` build pulls in DataFusion and
    the HTTP stack, so budget ~1.5 minutes for it; subsequent runs are fast. The default
    build has neither DataFusion nor the server (those are behind feature flags), which
    keeps ordinary builds quick and dependency-light.

## 1. Start the server

`vdg serve` hosts both the `/v1/*` HTTP API and the static web UI on port `8080`.

```bash
# feature `serve` implies `datafusion` — you get the real query engine + HTTP API.
cargo run -p vdg --features serve -- serve --table logs
```

By default this runs in the `all` role (read + write + UI on one process) against the
local-filesystem store (`./data`). You'll see it print the URLs it's serving, including
the ingest and tail endpoints.

## 2. Stream some logs

In a second terminal, keep synthetic logs flowing so time-relative queries like
`last 1h` stay populated. `--follow` appends a fresh batch every few seconds, anchored
to "now":

```bash
cargo run -- ingest --table logs --follow
```

Prefer a one-shot load instead of a live stream? Generate a fixed number of records:

```bash
cargo run -- ingest --table logs --generate 20000
```

Both write compacted Parquet into the table's tier prefixes and update the manifest.
The synthetic generator is deterministic (seeded), so the same seed always produces the
same logs.

!!! tip "Ingesting your own logs"
    You're not limited to synthetic data. `vdg ingest --from <file.ndjson>` loads real
    NDJSON logs (one JSON object per line), and once the server is up you can `POST`
    logs straight to `/v1/ingest`. See [Sending logs](sending-logs.md).

## 3. Open the UI

```bash
open http://localhost:8080
```

You get the log explorer: a query bar, a severity histogram, tier pills with a live
cost estimate, and expandable rows. There's also a **Live tail** page that streams new
rows as they arrive (backed by the `GET /v1/tail` SSE endpoint).

## 4. Run your first query

The query bar accepts **either** a concise search DSL **or** raw SQL — Verdigris detects
which and compiles the DSL down to SQL. Try the DSL first:

```text
service:auth status>=500 | last 1h
```

That means "records from the `auth` service with an HTTP-ish status ≥ 500, in the last
hour." Then try raw SQL against the same table:

```sql
SELECT level, service, count(*)
FROM logs
GROUP BY level, service
ORDER BY 3 DESC
```

Both read Parquet **in place** from the store — there is no rehydration step. See
[Querying](querying.md) for the full DSL grammar and the SQL surface.

### From the command line

You can also query without the server, straight from the CLI (needs the `datafusion`
feature):

```bash
cargo run --features datafusion -- sql --table logs \
  "SELECT service, count(*) FROM logs GROUP BY service ORDER BY 2 DESC"
```

## What just happened

```
  vdg ingest ──► Parquet + manifest in ./data/logs/{hot,warm,cold}/
                     │
  vdg serve ──► DataFusion reads those Parquet files in place ──► UI / API
```

Records were **routed by severity** to hot/warm/cold prefixes at write time (ERROR→hot,
WARN/INFO→warm, DEBUG→cold by default), batched into Parquet, and recorded in a JSON
manifest that acts as the table catalog. Queries register exactly the files the manifest
lists and scan them directly.

## Where to next

<div class="grid cards" markdown>

- :material-kubernetes: **[Install on EKS](install.md)** — take the same binary to
  production against your own S3 bucket, with a single `helm install`.
- :material-upload-network: **[Sending logs](sending-logs.md)** — Vector, Fluent Bit,
  and native OTLP/HTTP.
- :material-database-search: **[Querying](querying.md)** — the search DSL, SQL, Arrow
  vs JSON, and the live tail.
- :material-cash-multiple: **[Cost & tiering](cost.md)** — how storage, compute, and
  the cold-scan cost estimator fit together.

</div>
