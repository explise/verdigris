# Quickstart

Verdigris runs **inside your own EKS cluster** and writes to **your own S3 bucket** — a
drop-in replacement for your logging backend that never moves data out of your AWS account.
This page takes you from nothing to logs landing in S3 and queried in place, in three steps:

1. **Install** the chart into your cluster (pointed at your bucket).
2. **Point your log pipeline** at it (Vector / Fluent Bit / OTLP — no app changes).
3. **Query** in place — SQL or the search DSL, in the UI or over HTTP.

!!! note "What you need"
    An EKS cluster, an S3 bucket you own, and an IRSA role that can read/write that bucket —
    so **no static keys** live in the cluster. The container image + Helm chart are the only
    artifacts. Just want to kick the tires without AWS? Jump to [Evaluate locally](#evaluate-locally).

## 1. Install into your cluster

Point the chart at your bucket and attach an IRSA role:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1 \
  --set storage.backend=s3 \
  --set storage.s3.bucket=my-company-logs \
  --set storage.s3.region=us-east-1 \
  --set replicaCount=3 \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<acct>:role/verdigris-s3
```

The chart splits a single-writer **ingest** tier from a freely-scaled **query** tier, and — on
an `s3` install — auto-applies the S3 lifecycle policy that ages logs hot → warm → cold. See
[Install on EKS](install.md) for IAM, MinIO, and every value you can set.

## 2. Point your log pipeline at it

Verdigris ingests what your apps already emit — you don't change how they log. Turn on the
bundled Vector DaemonSet that tails every pod's stdout/stderr:

```bash
helm upgrade vdg deploy/helm/verdigris --reuse-values --set vector.enabled=true
```

Already running Vector, Fluent Bit, or an OpenTelemetry Collector? Just add Verdigris as a
sink — `POST /v1/ingest` for NDJSON, `POST /v1/otlp/logs` for native OTLP. See
[Sending logs](sending-logs.md).

## 3. Query in place

Reach the UI/API through your ingress (or a quick port-forward):

```bash
kubectl port-forward svc/vdg-verdigris 8080:8080
# UI + API at http://localhost:8080
```

The query bar takes **either** a concise search DSL **or** raw SQL — Verdigris detects which
and compiles the DSL to SQL. Try the DSL:

```text
service:auth status>=500 | last 1h
```

…or raw SQL against the same table:

```sql
SELECT level, service, count(*)
FROM logs
GROUP BY level, service
ORDER BY 3 DESC
```

Both read Parquet **in place** from your bucket — no rehydration, no data leaving your account.
Records are routed by severity to hot/warm/cold prefixes at write time, and queries scan only
the files the manifest lists. See [Querying](querying.md) for the full surface and
[Cost & tiering](cost.md) for how the cold-scan estimator keeps queries cheap.

## Evaluate locally

Want to see it work before touching a cluster? The same image runs a zero-config local demo —
a filesystem backend seeded with synthetic logs, no AWS:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1
kubectl port-forward svc/vdg-verdigris 8080:8080   # http://localhost:8080
```

Or run the binary straight from a checkout (needs a Rust toolchain; the first `serve` build
pulls in DataFusion, ~1.5 min):

```bash
cargo run -p vdg --features serve -- serve --table logs      # server + UI on :8080
cargo run -- ingest --table logs --follow                    # stream synthetic logs
```

These are for evaluation only — production is the S3 path above.

## Where to next

<div class="grid cards" markdown>

- :material-kubernetes: **[Install on EKS](install.md)** — IAM/IRSA, the ingest/query split,
  lifecycle auto-apply, and every chart value.
- :material-upload-network: **[Sending logs](sending-logs.md)** — Vector, Fluent Bit, and
  native OTLP/HTTP.
- :material-database-search: **[Querying](querying.md)** — the search DSL, SQL, Arrow vs JSON,
  and the live tail.
- :material-cash-multiple: **[Cost & tiering](cost.md)** — storage, the compute dial, and the
  cold-scan cost estimator.

</div>
