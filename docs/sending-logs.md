# Sending logs

Verdigris has two write endpoints, both served by `vdg serve` on nodes in the `ingest` or
`all` role:

- **`POST /v1/ingest`** — line-oriented JSON. What Vector and Fluent Bit ship to.
- **`POST /v1/otlp/logs`** — a native OpenTelemetry OTLP/HTTP **JSON** logs receiver.

Every path — a curl one-liner, an NDJSON file, the Vector DaemonSet, an OTel Collector —
maps onto one canonical record and lands in one Parquet schema. Records are routed by
severity to a hot/warm/cold tier at write time (see [Cost & tiering](cost.md)).

!!! note "Roles"
    Write endpoints live on the `ingest` and `all` roles. On a `query`-role node they
    deliberately answer **405** (a clear method error, not a 404) so a misrouted writer is
    obvious. In production the chart routes all writes to the single `--role ingest` pod.

## The ingest wire format

`POST /v1/ingest` accepts three shapes so any sender works: **NDJSON** (one JSON object per
line — what Vector's HTTP sink emits), a single JSON object, or a JSON array of objects.

Each object:

| Field | Required | Notes |
|---|---|---|
| `ts_millis` | yes | epoch milliseconds (i64) |
| `service` | yes | service name (string) |
| `message` | yes | log body (string) |
| `level` | no | parsed **case-insensitively**, defaults to `info` |
| `status` | no | integer, e.g. an HTTP status code |
| `trace_id` | no | string |
| `attrs` | no | `map<string,string>` of extra fields |

`level` is lenient: `error`/`err`/`fatal`/`critical`/`panic` → ERROR, `warn`/`warning` →
WARN, `debug`/`trace`/`verbose` → DEBUG, and anything unrecognized (or absent) → INFO — so a
stray severity never drops a line.

A single record via curl:

```bash
curl -s -X POST http://localhost:8080/v1/ingest --data-binary @- <<'JSON'
{"ts_millis":1751630000000,"level":"error","service":"auth","status":503,"message":"upstream timeout","trace_id":"abc123","attrs":{"region":"us-east-1"}}
JSON
```

NDJSON (one object per line) is the batch form:

```bash
printf '%s\n' \
  '{"ts_millis":1751630000000,"level":"info","service":"api","status":200,"message":"ok"}' \
  '{"ts_millis":1751630000500,"level":"warn","service":"api","message":"slow query"}' \
  | curl -s -X POST http://localhost:8080/v1/ingest --data-binary @-
```

Response:

```json
{ "ingested": 2, "skipped": 0, "filesWritten": 1, "bytesWritten": 2281 }
```

Malformed lines are **skipped and counted**, not fatal — a log shipper shouldn't lose 999
good lines to one bad one. An empty or all-malformed body returns **400**.

### From a file (CLI)

The same wire format loads from an NDJSON file without the server:

```bash
vdg ingest --table logs --from mylogs.ndjson
```

## Vector

The Helm chart ships an opt-in Vector DaemonSet that tails every pod's stdout/stderr,
reshapes it into the ingest wire format, and batches NDJSON to `POST /v1/ingest`:

```bash
helm upgrade vdg deploy/helm/verdigris --reuse-values --set vector.enabled=true
```

Its sink defaults to the dedicated ingest Service (on S3 with a dedicated ingest tier),
otherwise the single serve Service — so `--set vector.enabled=true` alone is enough, and
writes always land on the single writer, never on the scalable query replicas.

The reshape the chart uses (Vector `remap`/VRL) emits exactly the wire schema:

```yaml
sources:
  k8s:
    type: kubernetes_logs

transforms:
  to_verdigris:
    type: remap
    inputs: [k8s]
    source: |
      ts = to_unix_timestamp(.timestamp, unit: "milliseconds") ?? to_unix_timestamp(now(), unit: "milliseconds")
      . = {
        "ts_millis": ts,
        "level": to_string(.level) ?? "info",
        "service": to_string(.kubernetes.container_name) ?? "unknown",
        "message": to_string(.message) ?? "",
        "trace_id": to_string(.trace_id) ?? "",
        "attrs": {
          "namespace": to_string(.kubernetes.pod_namespace) ?? "",
          "pod": to_string(.kubernetes.pod_name) ?? "",
          "node": to_string(.kubernetes.pod_node_name) ?? ""
        }
      }

sinks:
  verdigris:
    type: http
    inputs: [to_verdigris]
    uri: http://vdg-verdigris-ingest:8080/v1/ingest
    method: post
    encoding:
      codec: json
    framing:
      method: newline_delimited   # NDJSON
    batch:
      max_events: 1000
      timeout_secs: 5
    request:
      retry_attempts: 5
```

Override the sink target with `--set vector.sinkEndpoint=...` if you run Verdigris elsewhere.

## Fluent Bit

Fluent Bit ships to the same `POST /v1/ingest` endpoint. Use the `http` output with a JSON
format; shape the records to carry `ts_millis`, `service`, and `message` (a Lua filter or
`modify`/`nest` filters can rename fields to match the wire schema above). The endpoint
accepts a JSON array or newline-delimited JSON, and parses `level` case-insensitively.

!!! tip "The contract is the wire schema, not the shipper"
    Any HTTP client that can POST JSON with `ts_millis`, `service`, and `message` works.
    Fluent Bit, Fluentd, Logstash, a cron job with curl — all land in the same Parquet.

## Native OTLP/HTTP

Point an OpenTelemetry Collector's `otlphttp` logs exporter at `POST /v1/otlp/logs`. This is
**OTLP/JSON only** (`Content-Type: application/json`) — no protobuf/gRPC, which keeps the
dependency surface small and covers the common OTel HTTP exporter.

The OTLP → Verdigris mapping:

| OTLP field | Verdigris field |
|---|---|
| `timeUnixNano` (falls back to `observedTimeUnixNano`) | `ts` (ns ÷ 1e6) |
| `severityText` if recognized, else `severityNumber` | `level` |
| `body.stringValue` | `message` |
| resource attribute `service.name` | `service` |
| `http.status_code` / `http.response.status_code` / `status_code` / `status` | `status` |
| `traceId` | `trace_id` |
| remaining record + resource attributes | `attrs` |

`severityNumber` follows the OTel ranges (1–8 → DEBUG, 9–12 → INFO, 13–16 → WARN, ≥17 →
ERROR); missing severity defaults to INFO.

Example collector config:

```yaml
exporters:
  otlphttp/verdigris:
    logs_endpoint: http://vdg-verdigris-ingest:8080/v1/otlp/logs
    encoding: json

service:
  pipelines:
    logs:
      exporters: [otlphttp/verdigris]
```

`POST /v1/otlp/logs` reuses the exact same write path (severity routing + batching) and the
same per-process write lock as `/v1/ingest`. Response:

```json
{ "ingested": 1, "filesWritten": 1, "bytesWritten": 2433 }
```

## What happens after a write

1. Each record is **routed by severity** to a tier prefix (`<table>/{hot,warm,cold}/`).
2. Records are batched and encoded as zstd-compressed Parquet.
3. Data files are content-addressed and appended to the table manifest via an optimistic
   compare-and-swap commit — so concurrent writers to one table never corrupt or lose data.

Streaming ingest naturally produces many small Parquet files; a background compaction step
merges them into larger files. See [Cost & tiering](cost.md#compaction).
