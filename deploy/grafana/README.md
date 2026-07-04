# Grafana integration for Verdigris

Read logs and storage stats from a Verdigris install inside your existing
Grafana, using the community **Infinity** datasource. No custom plugin, no code
in Grafana — just a provisioned datasource plus an importable dashboard.

Why Infinity: Verdigris speaks a plain REST API (`vdg serve`, feature `serve` —
see `crates/vdg/src/serve.rs`), not the Grafana datasource protocol. The
[Infinity datasource](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/)
can `POST`/`GET` JSON to any endpoint and map the response into Grafana frames,
which is exactly what we need. A dedicated Verdigris plugin (Go + React) is a
much larger project and is **not** required for a working integration.

## What's in this directory

| File | Purpose |
|---|---|
| `datasource.yaml` | Grafana provisioning file defining a `Verdigris` datasource (type `yesoreyeram-infinity-datasource`) pointed at the serve URL. |
| `dashboard-logs.json` | Importable dashboard: a Logs panel, an event histogram bar panel, and three stat tiles. |
| `README.md` | This file. |

## Endpoints each panel uses

All calls target the Verdigris serve API. Response shapes are as implemented in
`serve.rs` — nothing here references a field that does not exist.

| Panel | Method + path | Maps |
|---|---|---|
| **Logs** | `POST /v1/query` body `{"sql": "<query>"}` | `rows[]` → columns `ts, level, service, message, trace_id, status` |
| **Event histogram (total vs errors)** | `POST /v1/query` (same body) | `histogram[]` → numeric series `total`, `errors` |
| **Matched events (current query)** | `POST /v1/query` (same body) | `stats.events` |
| **Total stored** | `GET /v1/storage/tiers` | `totalGB` |
| **Estimated storage $/month** | `GET /v1/storage/tiers` | `totalPerMonth` |

The three `POST /v1/query` panels all send the **same** query, taken from the
dashboard's `query` template variable (a textbox at the top of the dashboard).
So the logs, their histogram, and the matched-event count always reflect one
query. The two `GET /v1/storage/tiers` stat tiles are query-independent.

### How the JSON mapping works

Each panel target is an Infinity query with:

- `type: json`, `source: url`, `parser: backend`
- `url`: a **relative** path (e.g. `/v1/query`) resolved against the datasource
  base `url` set in `datasource.yaml`.
- For query panels: `url_options.method: POST`, `body_type: raw`,
  `body_content_type: application/json`, and
  `data: {"sql": "${query:raw}"}` — Grafana substitutes the `query` template
  variable before sending.
- `root_selector`: the JSON path to the array/object to read — `rows`,
  `histogram`, `stats`, or `""` (document root) for `/v1/storage/tiers`.
- `columns[]`: `{selector, text, type}` triples that pull named fields out and
  assign Grafana types (`timestamp` for `ts`, `number` for counts, etc.).

## Install

### 1. Install the Infinity plugin

```bash
grafana-cli plugins install yesoreyeram-infinity-datasource
# then restart Grafana
```

Or, if you provision plugins declaratively (Helm `grafana` chart), add it to
`plugins:` / `GF_INSTALL_PLUGINS`:

```
GF_INSTALL_PLUGINS=yesoreyeram-infinity-datasource
```

### 2. Provision the datasource

Copy `datasource.yaml` into Grafana's datasource provisioning directory
(default `/etc/grafana/provisioning/datasources/`) and restart Grafana. It
defines a datasource named **Verdigris** with a fixed UID `verdigris-infinity`
(the dashboard references that UID).

Point it at your Verdigris install by editing the `url:` field:

- **In-cluster Service (default)** — `http://vdg-verdigris:8080`.
  The Verdigris Helm chart names its Service `<release>-verdigris`, so
  `helm install vdg deploy/helm/verdigris` gives `vdg-verdigris`. If Grafana
  runs in another namespace, use the FQDN:
  `http://vdg-verdigris.<namespace>.svc.cluster.local:8080`.
- **Port-forward** (Grafana and Verdigris reachable locally) —
  `kubectl port-forward svc/vdg-verdigris 8080:8080`, then set
  `url: http://localhost:8080`.
- **Ingress** — `url: https://verdigris.example.com`.

### 3. Import the dashboard

Grafana → Dashboards → **New → Import** → upload `dashboard-logs.json` (or paste
its contents). When prompted for a datasource, pick **Verdigris**. You can also
drop the file into a dashboard provisioning path if you provision dashboards.

### 4. Query it

At the top of the dashboard, the **Query (SQL or search DSL)** textbox drives the
Logs / histogram / events panels. Verdigris accepts either:

- **Real SQL** (DataFusion) over the log table, e.g.
  `SELECT * FROM logs ORDER BY ts DESC LIMIT 200` (the default), or
  `SELECT service, count(*) FROM logs GROUP BY service`.
- **Search DSL**, e.g. `service:auth status>=500 | last 1h`, which Verdigris
  compiles to SQL server-side.

Change the textbox and the three query panels refresh together.

> The log table is named `logs` by default (`vdg serve --table logs`). If you
> served a different table name, change it in your SQL.

## Auth

The Verdigris API is currently **unauthenticated**. It is gaining *optional*
bearer auth. When that lands, wire it in `datasource.yaml` (kept commented
there):

```yaml
    jsonData:
      auth_method: bearerToken
    secureJsonData:
      bearerToken: ${VERDIGRIS_TOKEN}
```

or send a raw `Authorization` header via `httpHeaderName1` /
`httpHeaderValue1`. Until the backend enforces auth, leave `auth_method: none`.

## Limitations / notes

- **Infinity plugin is a hard dependency.** The dashboard's targets are Infinity
  queries; without the plugin installed they won't run.
- **Histogram has no per-bucket timestamps.** `/v1/query` returns
  `histogram[]` as `{total, errors}` objects with no time field, so the bar
  panel's x-axis is bucket index (chronological order), not wall-clock time.
  That's a property of the API response, not a mapping choice.
- **The dashboard time picker does not filter the query.** Verdigris scoping is
  done inside the query text (SQL `WHERE` / `ORDER BY LIMIT`, or the DSL
  `| last 1h`). The Grafana time range only affects panels that carry their own
  time field.
- **Query is injected as a raw string into a JSON body.** A query containing a
  literal double-quote will break the `{"sql": "..."}` body. Use single quotes
  inside SQL string literals (standard SQL anyway), e.g.
  `... WHERE service = 'auth'`.
- **Not tier-filtered.** `/v1/query` scans all tiers; there is no cost-confirm
  gate on this path (that's the estimate endpoint `/v1/query/estimate`, not used
  by this dashboard). Large cold scans issued from here are not gated — mind
  what you `SELECT`.
