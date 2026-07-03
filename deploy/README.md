# Deploying Verdigris

Build order step 6: **one `helm install` on EKS.** This directory has the
container image (`../Dockerfile`) and the Helm chart (`helm/verdigris`).

## What deploys

| Resource | Purpose |
|---|---|
| Deployment | `vdg serve` — the `/v1/*` query API + static UI on `:8080` |
| Service | `ClusterIP` in front of the serve pods |
| ConfigMap | rendered `verdigris.toml` (storage/query/routing/lifecycle) |
| ServiceAccount | S3 access on EKS via IRSA annotation |
| PVC | local-backend store (demo mode only) |
| Job (hook) | one-shot synthetic seed for the S3 backend |
| Ingress | optional external access |
| DaemonSet | **Vector log-shipper — scaffold, disabled** (see below) |

## Build & push the image

```bash
# from the repo root
docker build -t <registry>/verdigris:0.0.1 .
docker push <registry>/verdigris:0.0.1
```

The build compiles `vdg --features serve` (DataFusion + axum) and ships the
binary + static `frontend/` on a slim Debian runtime as a non-root user.

## Install

### 1. Zero-config local demo (no AWS)

Filesystem backend on a PVC, auto-seeded with synthetic logs — a queryable UI
out of the box:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1

kubectl port-forward svc/vdg-verdigris 8080:8080
# open http://localhost:8080
```

Single replica (the local filesystem isn't shared across pods).

### 2. Production on EKS + S3

Data lands in **your** bucket; pods are stateless and scale freely. Preferred
auth is IRSA — no static keys:

```bash
helm install vdg deploy/helm/verdigris \
  --set image.repository=<registry>/verdigris --set image.tag=0.0.1 \
  --set storage.backend=s3 \
  --set storage.s3.bucket=my-company-logs \
  --set storage.s3.region=us-east-1 \
  --set replicaCount=3 \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<acct>:role/verdigris-s3
```

The IAM role needs `s3:GetObject`/`PutObject`/`ListBucket`/`DeleteObject` on the
bucket. Credentials resolve through the AWS chain (`AmazonS3Builder::from_env`),
so IRSA web-identity "just works" with no keys in the chart.

For MinIO or static keys, put `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in a
Secret and reference it:

```bash
--set storage.s3.endpoint=http://minio:9000 --set storage.s3.allowHttp=true \
--set storage.s3.existingSecret=verdigris-s3-creds
```

## Apply the S3 lifecycle policy

Tiering rules (`hot→warm→cold→expire`) are rendered by the binary, not the
chart — object storage lifecycle is an S3 API concern:

```bash
kubectl exec deploy/vdg-verdigris -- vdg lifecycle --table logs   # prints the policy JSON
# aws s3api put-bucket-lifecycle-configuration --bucket my-company-logs --lifecycle-configuration file://policy.json
```

## Ship real logs (Vector DaemonSet)

`vdg serve` exposes `POST /v1/ingest` — it accepts NDJSON (one JSON object per
line), a single JSON object, or a JSON array, routes each record by severity to
a tier, writes Parquet, and updates the manifest. The wire schema is
`ts_millis, level, service, message` (required) + optional `status, trace_id,
attrs`; `level` is parsed case-insensitively (`error`/`ERROR`/`warning`/…).

Enable the Vector DaemonSet to tail every pod's stdout/stderr and ship to it —
off by default (opt-in), `sinkEndpoint` defaults to the in-cluster serve Service:

```bash
helm upgrade vdg deploy/helm/verdigris --reuse-values --set vector.enabled=true
```

Or push logs directly:

```bash
kubectl exec deploy/vdg-verdigris -- sh -c \
  'printf "%s\n" "{\"ts_millis\":$(date +%s000),\"level\":\"info\",\"service\":\"demo\",\"message\":\"hello\"}" \
   | curl -s -X POST http://localhost:8080/v1/ingest --data-binary @-'
```

Synthetic data (no shipper) still works too:

```bash
kubectl exec deploy/vdg-verdigris -- vdg ingest --table logs --generate 20000
```

> Single-writer caveat: `/v1/ingest` serializes writes within a process, but
> multiple serve replicas ingesting to the same S3 table would still race on the
> JSON manifest (real Iceberg commits fix this — a known gap). For S3 + Vector
> today, keep ingest on a single writer (e.g. `replicaCount: 1`, or a dedicated
> ingest Deployment) until Iceberg lands.

## Validate the chart without a cluster

```bash
helm lint deploy/helm/verdigris
helm template vdg deploy/helm/verdigris                       # local backend
helm template vdg deploy/helm/verdigris --set storage.backend=s3 --set storage.s3.bucket=x
```
