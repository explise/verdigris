# Querying

Verdigris queries your logs **in place**: DataFusion reads Parquet straight out of object
storage, registering exactly the files the manifest lists. There is no rehydration step —
cold logs are always live, and you pay compute only when you actually query.

The query language is **SQL** (portable, no proprietary DSL to learn), plus a concise search
DSL for the common case that compiles down to SQL. `POST /v1/query` accepts either; the
server auto-detects which.

## The log schema

Rows carry these fields:

| Column | Type | Notes |
|---|---|---|
| `ts` | timestamp | event time |
| `level` | string | `ERROR` / `WARN` / `INFO` / `DEBUG` |
| `service` | string | |
| `status` | int | e.g. an HTTP status code |
| `message` | string | the log body |
| `trace_id` | string | |
| `attrs_json` | string | the schema-evolution escape hatch — extra fields as a JSON blob |

`attrs_json` is a JSON string; the web UI parses it into an `attrs` object for display.

## The search DSL

The search bar speaks a Datadog-ish query language. The grammar (v1):

```text
query   := term* ( '|' command )*
term    := field ':' value        # service:auth, level:error
         | field op value          # status >= 500, status == 200
         | word                     # free text → message ILIKE '%word%'
op      := == | = | != | >= | <= | > | <
command := last <duration>         # last 1h, last 30m, last 7d  (s/m/h/d)
```

Known columns are `ts, level, service, status, message, trace_id`. Any **other** key matches
inside the `attrs_json` blob (equality only). A few worked examples:

| DSL | Compiles to (roughly) |
|---|---|
| `service:auth status>=500 \| last 1h` | `service = 'auth' AND status >= 500 AND ts >= …` |
| `level:error timeout` | `level = 'ERROR' AND message ILIKE '%timeout%'` |
| `service:api status == 200` | `service = 'api' AND status = 200` |
| `region:us-east-1` | `attrs_json LIKE '%"region":"us-east-1"%'` |

Notes on behavior, verified against the translator:

- `level` values are upper-cased (`level:error` → `level = 'ERROR'`).
- `status` must be numeric — a non-numeric value is an error (surfaced as a **400**).
- A bare word becomes a free-text `message ILIKE '%word%'` contains-match.
- Attribute keys support only `:`/`==` equality; other operators on an attribute are an error.
- Results default to `ORDER BY ts DESC LIMIT 200`.

## Raw SQL

Anything starting with `SELECT` or `WITH` is treated as raw SQL and passed straight to
DataFusion — the full SQL surface, not a subset:

```sql
SELECT service, count(*) AS n, count(*) FILTER (WHERE level = 'ERROR') AS errors
FROM logs
GROUP BY service
ORDER BY n DESC
```

The table name in the query must match the served table (`--table`, default `logs`).

### From the CLI

```bash
vdg sql --table logs "SELECT level, count(*) FROM logs GROUP BY level"
```

`vdg sql` also accepts the search DSL (same auto-detection as the HTTP path). It requires a
build with the `datafusion` feature.

## The query API

`POST /v1/query` (a **read**-role endpoint) runs the query and returns a page of rows, a
time histogram, and stats in one envelope.

Request:

```json
{ "sql": "SELECT * FROM logs LIMIT 200" }
```

A malformed query returns **400** `{ "error": ... }` — so the client can tell a broken query
from a query that simply matched zero rows.

Response:

```json
{
  "rows": [
    { "ts": "2026-07-04T12:37:23.403", "level": "ERROR", "service": "auth",
      "status": 503, "message": "…", "trace_id": "abc123", "attrs_json": "{…}" }
  ],
  "stats": { "events": 60000, "scannedBytes": 787601, "elapsedMs": 6,
             "engine": "datafusion", "files": 3 },
  "histogram": [ { "total": 807, "errors": 331 }, … ]
}
```

- `stats.events` is the **total matched count** (the histogram sum), not the returned row page.
- `stats.engine` is always `datafusion`.
- `histogram` is ~60 buckets (total vs error counts) over the table's time range.

## Arrow vs JSON responses

By default the body is the JSON envelope above. Send `Accept:
application/vnd.apache.arrow.stream` to get the rows as a columnar **Arrow IPC stream** in
the body instead, with `stats` and `histogram` returned as JSON in the `x-verdigris-stats`
and `x-verdigris-histogram` response headers (still a single round-trip). The web UI
negotiates Arrow (config `wire: "arrow"`) and falls back to JSON transparently.

!!! note "Arrow wire detail"
    On the Arrow wire, `ts` is a Timestamp column delivered as epoch-ms (the client
    normalizes it to the same ISO string the JSON wire sends). The server also casts
    `Utf8View`/`BinaryView` columns down to plain `Utf8`/`Binary` so any Arrow decoder can
    read the stream.

## Live tail

`GET /v1/tail` (a read-role endpoint) is a live stream as Server-Sent Events
(`Content-Type: text/event-stream`). Each event is `data: {json}` for one row
(`{ts, level, service, message, trace_id?, status?, attrs_json?}`), with keepalive comments
in between. A fresh connection streams only newly-arriving rows, not the backlog: it starts
from the current manifest max and polls the newest file each second, bounded per poll so a
bursty table can't flood the stream. The web UI consumes it via `EventSource`.

!!! warning "Auth + SSE"
    `EventSource` cannot send an `Authorization` header. If bearer auth (`[auth]`) is on,
    front the tail with a query-param token or ingress auth.

## Scope & known limits

Grounded in the current implementation, so you know what to expect:

- **Text search** over columnar Parquet is `ILIKE`-based today. Fast "grep this stack trace"
  search (bloom filters / inverted index) is future work — `WHERE level='error'` is fast,
  arbitrary substring search over huge scans is not yet optimized.
- **Query-time tier filtering** is not wired: a query currently scans all tiers regardless of
  the UI's tier pills. Only the *cost estimate* is tier-aware (see [Cost & tiering](cost.md)).
- Estimate/scan pruning is by time window and coarse file min/max stats; finer
  predicate/row-group pruning is on the roadmap.
