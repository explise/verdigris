# Deploying Verdigris

Build order step 6: **one `helm install` on EKS.** This directory has the
container image (`../Dockerfile`) and the Helm chart (`helm/verdigris`).

## What deploys

| Resource | Purpose |
|---|---|
| Deployment (query) | `vdg serve --role query` — the `/v1/*` query API + UI on `:8080`, scales with `replicaCount` (s3) |
| Deployment (ingest) | `vdg serve --role ingest` — the single writer pod (s3 only; `replicas: 1`) |
| Deployment (all) | `vdg serve --role all` — combined writer+query, single pod (local demo, or s3 without a dedicated ingest tier) |
| Service | `ClusterIP` in front of the query/serve pods |
| Service (ingest) | `ClusterIP` in front of the ingest pod (s3 + dedicated ingest) — Vector targets this |
| ConfigMap | rendered `verdigris.toml` (storage/query/routing/lifecycle) |
| ServiceAccount | S3 access on EKS via IRSA annotation |
| PVC | local-backend store (demo mode only) |
| Job (hook) | one-shot synthetic seed for the S3 backend |
| Job (hook) | applies the S3 lifecycle policy on install/upgrade (`vdg lifecycle --apply`) |
| Ingress | optional external access |
| DaemonSet | **Vector log-shipper — scaffold, disabled** (see below) |

### Topology: ingest vs. query (S3)

The binary takes a `--role {all,ingest,query}` flag. For **`storage.backend=s3`**
the chart splits the two so the query tier scales without corrupting writes:

- an **ingest** Deployment — `replicas: 1`, `--role ingest`, fronted by its own
  `<release>-ingest` Service. This is the single writer (multiple writers would
  race on the JSON manifest until real Iceberg commits land — a known gap).
- a **query** Deployment — `replicas: {{ replicaCount }}`, `--role query`,
  fronted by the main Service. Stateless read/UI replicas; scale freely.

`replicaCount` now scales the **query tier** safely — the writer stays a single
pod regardless. Set `ingest.dedicated=false` to collapse back to one combined
`--role all` writer (still `replicas: 1`; no query scale-out). For
**`storage.backend=local`** it's always a single `--role all` pod (a pod's
filesystem isn't shared) that also carries the seed initContainer.

## Build & push the image

```bash
# from the repo root
docker build -t <registry>/verdigris:0.0.1 .
docker push <registry>/verdigris:0.0.1
```

The build has three stages: a Node stage builds the production web UI (`web/` —
Vite + SolidJS) into a static bundle, a Rust stage compiles
`vdg --features serve` (DataFusion + axum), and a slim Debian runtime ships the
binary + the built UI at `/app/web` (served by default) as a non-root user
(uid 10001). The original vanilla `frontend/` prototype is also copied to
`/app/frontend` for reference. The UI reads `GET /config.json` (served by the
binary) at boot, so the same image works cloud or on-prem.

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

## S3 lifecycle policy (auto-applied)

Tiering rules (`hot→warm→cold→expire`) are an S3 API concern, so the binary owns
them. On an **s3** install/upgrade the chart runs a post-install/post-upgrade
hook Job that calls `vdg lifecycle --table logs --apply`, which pushes the policy
via `PutBucketLifecycleConfiguration` using the pod's IRSA creds — so a fresh
install actually tiers, with no manual step. It's gated behind
`lifecycle.autoApply` (default `true` for s3); the age thresholds come from the
`lifecycle.*` values.

Disable it (e.g. to manage the policy out-of-band) with
`--set lifecycle.autoApply=false`, then apply it yourself:

```bash
kubectl exec deploy/vdg-verdigris -- vdg lifecycle --table logs           # print the policy JSON
kubectl exec deploy/vdg-verdigris -- vdg lifecycle --table logs --apply   # or apply it directly
```

## Ship real logs (Vector DaemonSet)

`vdg serve` exposes `POST /v1/ingest` — it accepts NDJSON (one JSON object per
line), a single JSON object, or a JSON array, routes each record by severity to
a tier, writes Parquet, and updates the manifest. The wire schema is
`ts_millis, level, service, message` (required) + optional `status, trace_id,
attrs`; `level` is parsed case-insensitively (`error`/`ERROR`/`warning`/…).

Enable the Vector DaemonSet to tail every pod's stdout/stderr and ship to it —
off by default (opt-in). `sinkEndpoint` defaults to the **dedicated ingest
Service** (`<release>-ingest`) when one exists (s3 + `ingest.dedicated`),
otherwise the single serve Service — so writes always land on the single writer,
never on the scalable query replicas:

```bash
helm upgrade vdg deploy/helm/verdigris --reuse-values --set vector.enabled=true
```

Or push logs directly (target the ingest pod on s3):

```bash
kubectl exec deploy/vdg-verdigris-ingest -- sh -c \
  'printf "%s\n" "{\"ts_millis\":$(date +%s000),\"level\":\"info\",\"service\":\"demo\",\"message\":\"hello\"}" \
   | curl -s -X POST http://localhost:8080/v1/ingest --data-binary @-'
```

Synthetic data (no shipper) still works too (write it on the ingest pod):

```bash
kubectl exec deploy/vdg-verdigris-ingest -- vdg ingest --table logs --generate 20000
```

> Single-writer nuance: `/v1/ingest` serializes writes within a process, and
> multiple writers to the same S3 table would still race on the JSON manifest
> (real Iceberg commits fix this — a known gap). The chart already handles this:
> on s3 all writes go to the **single** `--role ingest` Deployment
> (`replicas: 1`), while `replicaCount` only scales the **`--role query`** tier,
> which never writes. So you can scale queries freely — you do **not** need
> `replicaCount: 1`. (For local backend it's always one `--role all` pod.)

## Validate the chart without a cluster

```bash
helm lint deploy/helm/verdigris
helm template vdg deploy/helm/verdigris                       # local backend
helm template vdg deploy/helm/verdigris --set storage.backend=s3 --set storage.s3.bucket=x
```
