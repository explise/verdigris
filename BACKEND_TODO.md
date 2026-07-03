# Backend TODO — API fixes for the UI

Findings from probing the live `vdg` backend (`http://localhost:8080`) against the
contract the frontend renders (`frontend/api.js`, `web/src/lib/types.ts`).

**Status:** all 8 endpoints exist and respond `200`; CORS is open (`access-control-allow-origin: *`). The items below are field/shape mismatches and stubs, grouped by priority. Endpoints **not** listed (`/v1/alerts`, `/v1/storage/tiers`, `/v1/settings`) match the contract and need no change.

Generated 2026-06-28.

---

## ✅ Resolved (backend, 2026-06-28)

- **#2** `stats.events` now = total matched count (histogram sum), not the row page. Verified `events=40000` while `rows=200`.
- **#3** Malformed query now returns **400** with `{"error":...}` (parse + exec failures). Note: the search **DSL** treats free text as a `message ILIKE` match, so only raw-SQL errors surface as 400 — `!!! invalid @@@` is a valid (zero-match) DSL search by design.
- **#4** `/v1/metrics` — `ingestRate` + `errorRate` now real (time-bucketed from the data); `p99` is **modeled** (logs have no latency field yet — flagged, not faked-silently). `placeholder` dropped; tiles populated.
- **#5** `/v1/cost` — added `lastMonth`, `vsDatadog {ours,datadog}`, `expensiveQueries` (empty until query-history tracking exists). Numbers scale with data (tiny on demo volumes).
- **#6** `/v1/pipelines` — added `dropRate` (0, no drop stage yet), `ingestLag`, `parquetRolls` (derived from the manifest).
- **#9** Histogram now ~60 buckets tied to the table's time range.

### Not changed (with reasons)
- **#1** Cost estimator already computes `costUsd` (`scanGB × per-tier rate`). The observed `0.0` was a **data artifact** — the probed table had no warm/cold files. With tiered data it's non-zero. Note: per `CLAUDE.md` pricing, warm (Glacier IR) ≈ `$0.03/GB` and cold (Glacier Flexible, standard) ≈ `$0.01/GB` — the TODO had these swapped. `scanGB` is still tier-total (doesn't yet prune by the query's time range).
- **#7** Rows still carry `attrs_json`; `frontend/api.js` parses it into `attrs`. Left client-side for now.
- **#8 / #11** No change (`ts` ISO is fine; `engine: "datafusion"` is the canonical decision — see ADR-001).
- **#10** `/v1/tail` SSE not added — the client `tail()` is mock-only with no fetch path, so nothing consumes it yet.

---

## P0 — breaks a core feature

### 1. Cost estimator returns `costUsd: 0.0` for every tier
`POST /v1/query/estimate`

Observed:
```
{"tiers":["cold"]} → {"coldRestore":true,"costUsd":0.0,"scanGB":0.0}
{"tiers":["hot"]}  → {"coldRestore":false,"costUsd":0.0,"scanGB":0.0013991491869091988}
```
Problems:
- `costUsd` is never computed. It must be `scanGB × per-tier retrieval rate`:
  hot `$0`, warm `~$0.01/GB`, cold `~$0.03/GB` (see pricing table in `CLAUDE.md`).
- `scanGB` looks like a fixed constant and **ignores `sql` + time range** — it should
  estimate the bytes the *actual* query would scan.

Why it matters: this powers the pre-query cost gate (the "this will scan ~X GB,
~$Y, continue?" confirm on cold scans) — the product's headline differentiator.
Expected shape is correct: `{ scanGB, costUsd, coldRestore }`.

### 2. `stats.events` is the returned row count, not the total match count
`POST /v1/query`

Observed: `stats.events = 200`, `rows = 200`, but the histogram sums to
**100,600 events / 40,763 errors**. The footer renders "200 events", which is wrong.

Fix: `stats.events` = **total matched events** (the histogram sum / full count).
The 200 is a separate page/`LIMIT` of rows and should not be conflated with the total.

### 3. Invalid SQL returns `200` with empty rows instead of an error
`POST /v1/query`

Observed: `{"sql":"!!! invalid @@@"}` → `200 {rows:[], histogram, stats}`, no `error` key.
The UI can't distinguish a broken query from zero matches (it shows "no matches").

Fix: on parse/exec failure return **`4xx` with `{"error":"<message>"}`**.
The client already reads `err.message` from that shape (`frontend/api.js queryLogs`).

---

## P1 — endpoints return stub/empty data (pages render blank or `undefined`)

### 4. `GET /v1/metrics` — time-series arrays are empty
`ingestRate`, `errorRate`, `p99` all have length 0; response also has a leftover
`placeholder` key. Dashboards line charts render flat.
Fix: populate the three series (arrays of numbers). `tiles` and `volumeByService`
are already correct. Drop `placeholder`.

### 5. `GET /v1/cost` — missing fields
Has: `monthToDate, projected, breakdown, spendSeries, placeholder`.
Missing (UI reads these): **`lastMonth`, `vsDatadog` (`{ours, datadog}`),
`expensiveQueries` (`[{q, tier, scanGB, usd, user, when}]`)**.
Without them the "vs Datadog" tile, projected-delta, and expensive-queries table show `undefined`.

### 6. `GET /v1/pipelines` — missing fields
Has: `sources, transforms, throughput, placeholder`.
Missing (UI reads these): **`dropRate` (number), `ingestLag` (string), `parquetRolls` (string)**.
Three stat tiles render `undefined`.

---

## P2 — contract consistency / polish

### 7. Query rows use `attrs_json` (stringified) instead of `attrs` (object)
Row keys: `attrs_json, level, message, service, status, trace_id, ts`.
The contract asks for a nested `attrs` object. The UI currently parses `attrs_json`
as a fallback, but pick one and align (prefer sending `attrs` as a JSON object).

### 8. `ts` is ISO (`2026-06-28T05:51:54.096`)
Fine to leave — the UI now formats it to time-of-day. Noted only because the
contract example used `HH:MM:SS`.

### 9. Histogram returns 171 arbitrary-width buckets
The UI normalizes to any count now, but ideally the bucket count is **tied to the
query's time range** (e.g. a fixed ~60 buckets) so the strip is stable across queries.

### 10. `GET /v1/tail` → 404 (no streaming endpoint)
Live tail is mock-only on the client. For real streaming, add **SSE or WebSocket at
`/v1/tail`** emitting log events (`{ts, level, service, message, ...}`).

### 11. Engine label mismatch (decision, not a bug)
`stats.engine` is `"datafusion"`; product docs / UI footer say "duckdb". Decide which
is canonical. (UI side can read `stats.engine` so the footer is always correct —
pending product call.)

---

## Reference: full contract
Field-by-field shapes the UI expects are in **`web/src/lib/types.ts`** (typed) and
mirrored in **`frontend/api.js`** mock returns. Match those and the UI needs no changes.
