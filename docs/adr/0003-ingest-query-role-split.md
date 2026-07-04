# ADR-0003: Split ingest (writer) from query (readers) for single-writer safety

**Status:** Accepted
**Date:** 2026-07-04

## Context

Verdigris's catalog is currently a JSON manifest, which has **no concurrent-commit safety**
(see [ADR-0002](0002-manifest-as-iceberg-standin.md)). Ingest is a read-modify-write of that
manifest.

The production promise is "one `helm install` → it works, and the query tier scales." But
the two obvious ways to deploy the single `vdg serve` binary both break on the manifest:

- **One Deployment, N replicas, all roles.** A Vector DaemonSet fans logs into the Service,
  which round-robins across replicas. Now N pods read-modify-write the same S3 manifest
  concurrently → races, lost writes, catalog corruption.
- **Force `replicaCount: 1`.** Safe for the manifest, but there is no query HA and ingest
  throughput is capped at one pod. This contradicts "scale the query tier freely."

We need horizontal read scaling without multiple concurrent writers, *before* Iceberg lands.

## Decision

Add a `vdg serve --role {all,ingest,query}` flag that selects which HTTP surface a node
exposes, and split the deployment accordingly:

- **`ingest`** — only the write endpoints (`/v1/ingest`, `/v1/otlp/logs`). Run **exactly
  one** as the sole manifest writer.
- **`query`** — read/UI endpoints (`/v1/query`, `/v1/query/estimate`, metrics, storage,
  cost, pipelines, settings, `/v1/tail`) + the static web UI + `/config.json`. The write
  endpoints are present but return **405**, so a misrouted writer gets a clear method error
  rather than a 404. Run **N** of these as stateless readers.
- **`all`** — every endpoint on one node; the single-pod local demo default.

The Helm chart renders this automatically: the S3 backend produces one `--role ingest`
Deployment (`replicas: 1`) behind a dedicated `-ingest` Service plus a `--role query`
Deployment scaled by `replicaCount`; the Vector DaemonSet sink targets the `-ingest`
Service. The local backend stays a single `--role all` pod.

## Consequences

**Easier.** The query tier scales freely and is HA; a fresh `helm install` on EKS is
*correct* (no manifest races) with no manual replica pinning. Ingest and query resource
profiles can be tuned independently.

**Harder / owed.** Ingest is now a single point of write throughput and a single point of
failure for the write path (reads and already-written data are unaffected if it restarts).
This is an acceptable interim posture: logs buffer at the shipper (Vector) during a brief
ingest-pod restart.

**This is a bridge, not the destination.** The proper fix is optimistic-concurrency
commits, which let *any* replica write safely and make the `ingest`/`query` split an
optimization rather than a correctness requirement.

**Update (2026-07-04):** that concurrency safety now exists — the manifest commits via
compare-and-swap and data files are content-addressed
(see [ADR-0002](0002-manifest-as-iceberg-standin.md)), so concurrent writers no longer
corrupt or lose data. The `--role` split is therefore no longer *load-bearing for data
integrity*; it remains valuable for **resource isolation** (tuning the write tier and the
stateless read tier independently) and as defense-in-depth. Operators may now run more than
one ingest node if they wish, at the cost of manifest contention until Iceberg's
manifest-list structure replaces the single-object catalog.

**Operational note.** Because the `ingest` role does not serve the static UI, health probes
must target an endpoint present in all roles (a lightweight health check), not `/`.
