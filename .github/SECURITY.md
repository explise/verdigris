# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately** — do not open a public issue for a
suspected vulnerability. Use GitHub's private vulnerability reporting
("Report a vulnerability" under the repository's **Security** tab) or email the
maintainers.

Include, where possible:

- a description of the issue and its impact,
- steps to reproduce or a proof of concept,
- affected version(s) / commit,
- any suggested remediation.

We aim to acknowledge reports within a few business days and will keep you updated as we
investigate and prepare a fix. Please give us a reasonable window to remediate before any
public disclosure.

## Supported versions

Verdigris is pre-1.0 (**alpha**). Security fixes land on `main`; there is not yet a
backported-release process. Pin to a commit you have reviewed for production use.

## Security model & operator responsibilities

Verdigris is designed to run **inside the customer's own AWS account**, so much of its
security posture is inherited from how you deploy it:

- **Data at rest** lives in *your* S3 bucket. Enable bucket encryption (SSE-S3 / SSE-KMS),
  block public access, and restrict the bucket policy. Verdigris never copies your data
  out of your account.
- **Credentials.** On EKS, prefer **IRSA** (IAM Roles for Service Accounts) over static
  keys — the Helm chart wires this via a ServiceAccount annotation. The IAM role should be
  scoped to the specific bucket/prefix with only `GetObject`/`PutObject`/`ListBucket`/
  `DeleteObject`.
- **API authentication.** `vdg serve` supports optional bearer-token auth on the `/v1/*`
  API (config `[auth]`, or `VERDIGRIS_API_TOKEN`). It is **off by default**. For any
  non-local deployment, either enable it **and/or** place the service behind your
  ingress/mesh authentication. The static UI and `/config.json` are intentionally left
  open so the UI can boot a login state.
- **Network.** The service listens on plain HTTP; terminate TLS at your ingress/load
  balancer. Do not expose `vdg serve` directly to the public internet without auth + TLS.
- **Cost as a safety property.** The pre-query cost estimator exists partly to prevent a
  careless cold-tier scan from producing a large Glacier retrieval bill. Keep the
  cold-scan confirm gate enabled for untrusted or interactive users.

## Known gaps (tracked, pre-1.0)

- Multi-replica ingest to one S3 table relies on a single designated writer
  (`--role ingest`); concurrent writers are not yet safe (awaiting Iceberg commits).
- The bearer-token auth is coarse (single shared token); per-user auth / OIDC and
  multi-tenant authorization are not yet implemented.
