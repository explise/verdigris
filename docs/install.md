# Install on EKS + S3

Verdigris ships as a single container image and a Helm chart, so a production install is
one `helm install`. Data lands in **your own S3 bucket**; the pods are stateless and scale
freely. This page is the full production reference — the [Quickstart](getting-started.md) is
the fast path from zero to logs in S3.

!!! note "The whole promise"
    One binary, `helm install`, done — running against your bucket, with no static keys
    in the cluster. The same image runs the local demo and the EKS deployment; only the
    config differs.

## Build & push the image

The multi-stage `Dockerfile` at the repo root builds the production web UI (`web/` — Vite +
SolidJS), compiles `vdg --features serve` (DataFusion + axum), and ships a slim non-root
Debian runtime (uid 10001) with the UI baked in.

```bash
# from the repo root
docker build -t <registry>/verdigris:0.0.1 .
docker push <registry>/verdigris:0.0.1
```

## The ingest / query role split

The binary takes `serve --role {all,ingest,query}`. For an S3 backend the chart splits the
write and read surfaces so the query tier scales without ever corrupting writes:

| Role | What it serves | Replicas |
|---|---|---|
| `ingest` | **only** the write endpoints (`/v1/ingest`, `/v1/otlp/logs`) — the single manifest writer | 1 |
| `query` | read/UI endpoints + `/config.json` + static UI; write endpoints answer **405** | `replicaCount` (scale freely) |
| `all` | everything, one process (local demo, or S3 without a dedicated ingest tier) | 1 |

Why the split: multiple writers would race on the JSON manifest. At the data layer this is
already made safe — data files are content-addressed and the manifest commits via optimistic
compare-and-swap with retry — so the single-writer `ingest` role is defense-in-depth, and
`replicaCount` scaling the **query** tier is always safe. (Full Apache Iceberg commits, for
scale/partitioning rather than correctness, remain future work.)

## Install: zero-config local demo (no AWS)

A filesystem backend on a PVC, auto-seeded with synthetic logs — a queryable UI out of the
box, useful to smoke-test the chart before wiring S3:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1

kubectl port-forward svc/vdg-verdigris 8080:8080
# open http://localhost:8080
```

This is always a single `--role all` pod — a pod's local filesystem isn't shared across
replicas.

## Install: production on EKS + S3

Point the chart at your bucket and attach an IRSA role. Preferred auth is IRSA, so **no
static keys** live in the cluster:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1 \
  --set storage.backend=s3 \
  --set storage.s3.bucket=my-company-logs \
  --set storage.s3.region=us-east-1 \
  --set replicaCount=3 \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<acct>:role/verdigris-s3
```

Credentials resolve through the standard AWS chain (`AmazonS3Builder::from_env`), so an
IRSA web-identity "just works" with no keys in the chart.

### IAM permissions

The IRSA role needs, on the bucket:

- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`

If you let the chart auto-apply the lifecycle policy (below), the role also needs
`s3:PutLifecycleConfiguration`.

### MinIO or static keys

For MinIO or an environment without IRSA, put `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
in a Secret and reference it (avoid inline plaintext keys):

```bash
--set storage.s3.endpoint=http://minio:9000 --set storage.s3.allowHttp=true \
--set storage.s3.existingSecret=verdigris-s3-creds
```

## S3 lifecycle policy (auto-applied)

Tiering rules (`hot → warm → cold → expire`) are an S3-API concern, so the binary owns them.
`object_store` has no lifecycle API, so this path uses the AWS SDK behind the optional
`apply` feature (already built into the image). On an **s3** install/upgrade the chart runs a
hook Job that calls `vdg lifecycle --table logs --apply`, pushing the policy via
`PutBucketLifecycleConfiguration` with the pod's IRSA creds — so a fresh install actually
tiers, with no manual step. The age thresholds come from the `lifecycle.*` values.

Disable it (to manage the policy out-of-band) with `--set lifecycle.autoApply=false`, then
apply it yourself:

```bash
kubectl exec deploy/vdg-verdigris -- vdg lifecycle --table logs           # print the policy JSON
kubectl exec deploy/vdg-verdigris -- vdg lifecycle --table logs --apply   # or apply it directly
```

## Values you actually set

| Value | Default | Purpose |
|---|---|---|
| `storage.backend` | `local` | `s3` for production |
| `storage.s3.bucket` | `""` | **required** for `s3` |
| `storage.s3.region` | `us-east-1` | bucket region |
| `storage.s3.prefix` | `""` | optional key prefix within the bucket |
| `storage.s3.endpoint` / `allowHttp` | `""` / `false` | set for MinIO |
| `replicaCount` | `1` | scales the **query** tier (s3 + dedicated ingest) |
| `ingest.dedicated` | `true` | separate single-writer ingest Deployment (s3) |
| `serviceAccount.annotations` | `{}` | attach the IRSA `role-arn` here |
| `query.cores` / `query.modeledMibpsPerCore` | `4` / `250` | the query compute dial |
| `routing.{error,warn,info,debug}` | hot/warm/warm/cold | severity → tier placement |
| `lifecycle.{hotToWarmDays,warmToColdDays,expireDays}` | `3` / `30` / `400` | age transitions |
| `lifecycle.autoApply` | `true` | push the lifecycle policy on install/upgrade (s3) |
| `seed.enabled` / `seed.records` | `true` / `20000` | one-shot synthetic seed |
| `vector.enabled` | `false` | opt-in Vector log-shipping DaemonSet |

## Ship real logs

The chart includes an **opt-in** Vector DaemonSet that tails every pod's stdout/stderr and
ships NDJSON to the single writer:

```bash
helm upgrade vdg deploy/helm/verdigris --reuse-values --set vector.enabled=true
```

Its sink defaults to the dedicated ingest Service when one exists, so writes always land on
the single writer, never on the scalable query replicas. See [Sending logs](sending-logs.md)
for Vector, Fluent Bit, and OTLP details.

## Health checks

`GET /healthz` returns `200 {"status":"ok"}` in **every** role and is never gated by auth,
so kubelet probes can target it. The ingest role serves no web root, so point its probes at
`/healthz` rather than `/`.

## Validate the chart without a cluster

```bash
helm lint deploy/helm/verdigris
helm template vdg deploy/helm/verdigris                                          # local backend
helm template vdg deploy/helm/verdigris --set storage.backend=s3 --set storage.s3.bucket=x
```

## Locking down the API

The `/v1/*` surface can be gated with an optional bearer token — off by default. See
[Configuration → Authentication](configuration.md#authentication-auth). Note that the
live-tail SSE endpoint is consumed by the browser's `EventSource`, which can't send an
`Authorization` header; front it with a query-param token or ingress auth when `[auth]` is on.
