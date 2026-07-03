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

## The Vector DaemonSet is a scaffold (disabled)

`vector.enabled=false` by default, on purpose. It tails every pod's
stdout/stderr and is meant to POST batches to a Verdigris HTTP ingest endpoint —
**which does not exist yet.** Today `vdg` ingests only via the CLI (synthetic
generator + NDJSON file); there is no `/v1/ingest` route. The DaemonSet +
`kubernetes_logs → remap → http` config are shipped as ready-to-activate wiring.
When the ingest endpoint lands, set `vector.sinkEndpoint` and flip the flag. Until
then, seed data with the CLI:

```bash
kubectl exec deploy/vdg-verdigris -- vdg ingest --table logs --generate 20000
```

## Validate the chart without a cluster

```bash
helm lint deploy/helm/verdigris
helm template vdg deploy/helm/verdigris                       # local backend
helm template vdg deploy/helm/verdigris --set storage.backend=s3 --set storage.s3.bucket=x
```
