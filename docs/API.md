# Verdigris HTTP API

The API served by `vdg serve` (feature `serve`). All bodies are JSON unless noted.
Shapes below were verified against the running server; the web UI renders exactly these
(the client contract lives in `web/src/lib/types.ts`).

- Base path: `/v1/*` for the API; `/` serves the web UI; `/config.json` and `/healthz` are
  unversioned operational endpoints.
- CORS is permissive (`access-control-allow-origin: *`).
- Errors use `{ "error": "<message>" }` with an appropriate 4xx/5xx status.

## Roles

`vdg serve --role {all,ingest,query}` selects which surface a node exposes
(see [ADR-0003](adr/0003-ingest-query-role-split.md)):

| Endpoint group | `all` | `ingest` | `query` |
|---|:--:|:--:|:--:|
| Writes (`/v1/ingest`, `/v1/otlp/logs`) | ✅ | ✅ | **405** |
| Reads (`/v1/query`, metrics, tail, …) + UI + `/config.json` | ✅ | 404 | ✅ |
| `/healthz` | ✅ | ✅ | ✅ |

In the `query` role the write endpoints deliberately answer **405** (not 404) so a
misrouted writer gets a clear method error.

## Authentication

Optional bearer-token auth, off by default. Enable via config:

```toml
[auth]
enabled = true
token   = "your-secret"   # or set VERDIGRIS_API_TOKEN (env overrides config)
```

When enabled, every `/v1/*` request must carry `Authorization: Bearer <token>`; a
missing/incorrect token returns **401**. `/healthz` and `/config.json` stay **open** so
the UI (and kubelet probes) can boot before auth. `vdg serve` refuses to start if
`[auth].enabled` is set but no token resolves.

---

## Operational endpoints

### `GET /healthz`
Liveness/readiness probe. Available in **every** role, never gated by auth.
→ `200 { "status": "ok" }`. Kubernetes probes target this (the ingest role serves no `/`).

### `GET /config.json`
Runtime deployment config the `web/` SPA reads at boot. Pins the UI to this backend:
```json
{ "mode": "onprem", "apiBaseUrl": "", "useMocks": false, "wire": "json",
  "auth": { "kind": "none" },
  "orgs": [ { "id": "local", "name": "Verdigris" } ],
  "environments": [ { "id": "<table>", "label": "<table>", "region": "<region>", "bucket": "<bucket>" } ] }
```

---

## Query

### `POST /v1/query`  *(read role)*
Run a query and get a page of rows, a time histogram, and stats in one envelope.

Request:
```json
{ "sql": "SELECT * FROM logs LIMIT 200" }
```
`sql` accepts **either** raw SQL (DataFusion) **or** the search DSL
(`service:auth status>=500 | last 1h`), which is compiled to SQL. A malformed query
returns **400** `{ "error": ... }` (so the client distinguishes a broken query from zero
matches).

Response:
```json
{
  "rows": [
    { "ts": "2026-07-04T12:37:23.403", "level": "ERROR", "service": "auth",
      "status": 503, "message": "…", "trace_id": "abc123", "attrs_json": "{…}" }
  ],
  "stats": { "events": 60000, "scannedBytes": 787601, "elapsedMs": 6,
             "engine": "datafusion", "files": 3 },
  "histogram": [ { "total": 807, "errors": 331 }, … ]   // ~60 buckets over the table's time range
}
```
`stats.events` is the **total matched count** (histogram sum), not the returned row page.
Rows carry attributes as the `attrs_json` string (schema-evolution escape hatch); the UI
parses it into an `attrs` object.

**Wire negotiation (Arrow).** Send `Accept: application/vnd.apache.arrow.stream` to get the
rows as a columnar **Arrow IPC stream** in the body, with `stats` and `histogram` returned
as JSON in the `x-verdigris-stats` and `x-verdigris-histogram` response headers (still one
round-trip). Any other `Accept` yields the JSON envelope above. The web UI negotiates Arrow
(config `wire: "arrow"`) and falls back to JSON transparently. Note: `ts` is a Timestamp
column, so the Arrow wire delivers it as epoch-ms; the client normalizes it to the same ISO
string the JSON wire sends.

### `POST /v1/query/estimate`  *(read role)*
Pre-query scan-size + dollar estimate — powers the cold-scan confirm gate.

Request: `{ "tiers": ["hot","warm","cold"], "sql": "… | last 1h" }` (`sql` optional; when
present, prunes by the query's time window).

Response:
```json
{ "scanGB": 0.00008, "scanBytes": 87656, "costUsd": 0.0000008, "coldRestore": true,
  "restoreMs": 18000000, "scanMs": 3, "filesTouched": 1, "filesTotal": 3,
  "perTier": [ { "tier": "cold", "gb": 0.00008, "costUsd": 0.0000008 } ] }
```
`costUsd = scanGB × per-tier retrieval rate` (hot `$0`, warm ≈ `$0.03/GB` Glacier Instant,
cold ≈ `$0.01/GB` Glacier Flexible standard).

### `GET /v1/tail`  *(read role, SSE)*
Server-Sent Events stream of recent log rows. `Content-Type: text/event-stream`; each event
is `data: {json}` where json is one row `{ ts, level, service, message, trace_id?, status?,
attrs_json? }`, plus keepalive comments. Polls the newest file; bounded per poll.
The web UI consumes this via `EventSource` (note: `EventSource` cannot send an
`Authorization` header — front a token via query param or ingress auth if `[auth]` is on).

---

## Ingest  *(write role)*

### `POST /v1/ingest`
Accepts **NDJSON** (one JSON object per line — Vector's HTTP sink), a single JSON object,
or a JSON array. Wire schema: `ts_millis, level, service, message` (required) + optional
`status, trace_id, attrs`. `level` is case-insensitive (`error`/`ERROR`/`warning`/…).
Malformed lines are skipped and counted, not fatal.

Response: `{ "ingested": 1, "skipped": 0, "filesWritten": 1, "bytesWritten": 2281 }`.
Empty/all-malformed body → **400**.

### `POST /v1/otlp/logs`
Native OpenTelemetry logs receiver — OTLP/**HTTP JSON** (`application/json`). Point an OTel
Collector's `otlphttp` logs exporter here. The mapping: `timeUnixNano`→ts,
`severityText`/`severityNumber`→level, `body.stringValue`→message,
resource `service.name`→service, `http.status_code` (or `status`)→status, remaining
resource/record attributes→`attrs`. Reuses the same write path + per-process write lock as
`/v1/ingest`.

Response: `{ "ingested": 1, "filesWritten": 1, "bytesWritten": 2433 }`.

---

## Dashboards & operations  *(read role)*

All return `200` with the shapes the UI renders (see `web/src/lib/types.ts`). Figures are
computed from the manifest + cost model; a few series that require subsystems not yet built
(alerting, query history, per-request latency) return shape-correct empties.

| Endpoint | Returns |
|---|---|
| `GET /v1/metrics` | `{ ingestRate[], errorRate[], p99[], volumeByService[], tiles{} }` — series time-bucketed from the data; `p99` is **modeled** (no latency field yet). |
| `GET /v1/storage/tiers` | `{ tiers[], lifecycle[], compaction{}, totalGB, totalPerMonth }` — real per-tier bytes/objects/cost + compaction generation. |
| `GET /v1/cost?days=N` | `{ rangeDays, monthToDate, projected, lastMonth, breakdown[], spendSeries[], vsHosted{}, expensiveQueries[] }` — `days` (default 30, clamped to 1–3650) sets the projection horizon; `expensiveQueries` comes from the query-history audit doc; `spendSeries` is empty until [#33](https://github.com/explise/verdigris/issues/33). |
| `GET /v1/pipelines` | `{ sources[], transforms[], throughput[], dropRate, ingestLag, parquetRolls }` — lag/rolls derived from the manifest. |
| `GET /v1/settings` | `{ bucket, region, retentionDays, queryCompute, confirmColdScans, routing[] }` — from live config. |
| `GET /v1/alerts` | `[]` — no alerting engine yet. |

---

## Status codes

| Code | Meaning |
|---|---|
| `200` | Success. |
| `400` | Malformed query (`/v1/query`) or ingest body (`/v1/ingest`, `/v1/otlp/logs`). |
| `401` | Auth enabled and token missing/incorrect. |
| `404` | Read endpoint requested on an `ingest`-role node. |
| `405` | Write endpoint requested on a `query`-role node. |
| `500` | Internal error (storage/engine). |
