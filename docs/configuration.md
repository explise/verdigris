# Configuration

Verdigris is configured by a single TOML file. Everything that touches the outside world is
selected here, so the **same binary** runs fully offline against the local filesystem or
against S3/MinIO with only a config change — no recompile.

## Where the config comes from

`vdg` resolves the config file in this order:

1. an explicit `--config <path>`,
2. else the `VERDIGRIS_CONFIG` environment variable,
3. else `./config/verdigris.toml` if it exists,
4. else built-in defaults.

Print the fully resolved config (defaults filled in) with:

```bash
vdg config
```

Every section is optional; omit a section to take its defaults. The sections are `[storage]`,
`[query]`, `[routing]`, `[lifecycle]`, and `[auth]`.

## Storage (`[storage]`)

Selected by a `backend` tag. Three backends:

=== "Local (default)"

    ```toml
    [storage]
    backend = "local"
    path = "./data"        # defaults to ./data
    ```

    Fully offline; no S3 needed.

=== "S3 / MinIO"

    ```toml
    [storage]
    backend = "s3"
    bucket = "my-company-logs"
    region = "us-east-1"           # optional
    endpoint = "http://minio:9000" # optional — set for MinIO
    allow_http = true              # optional — needed for local MinIO over plain HTTP
    prefix = "logs"                # optional key prefix within the bucket
    # Credentials are optional here — prefer the AWS env chain / IRSA:
    access_key_id = "…"            # optional
    secret_access_key = "…"        # optional
    ```

    Any S3-compatible endpoint works. On EKS, leave the credentials out and use IRSA; on
    MinIO, set `endpoint` + `allow_http`.

=== "Memory"

    ```toml
    [storage]
    backend = "memory"
    ```

    In-memory store, for tests and ephemeral runs.

## Query (`[query]`)

The compute dial — decoupled from storage on purpose (query speed is provisioned
separately). These knobs drive the modeled executor and the `scanMs` figure in the cost
estimate.

```toml
[query]
modeled_mibps_per_core = 250.0   # modeled per-core scan throughput (MiB/s)
cores = 4                        # provisioned query cores
```

Provisioned throughput used by the estimator is `cores × modeled_mibps_per_core`.

## Routing (`[routing]`)

Severity → tier placement, decided at write time. **Severity decides placement, never
price.** Defaults:

```toml
[routing]
error = "hot"
warn  = "warm"
info  = "warm"
debug = "cold"
```

Each value is one of `hot` / `warm` / `cold`.

## Lifecycle (`[lifecycle]`)

Age-based transitions, rendered into an S3 lifecycle policy by `vdg lifecycle`. Defaults:

```toml
[lifecycle]
hot_to_warm_days  = 3
warm_to_cold_days = 30
expire_days       = 400
```

These move data hot → warm → cold across colder storage classes as it ages, then expire it.
See [Cost & tiering](cost.md#the-lifecycle-policy).

## Authentication (`[auth]`)

An optional bearer-token gate on the `/v1/*` HTTP API. **Off by default**, so the local loop
and existing behavior are unchanged.

```toml
[auth]
enabled = true
token   = "your-secret"   # or set VERDIGRIS_API_TOKEN (env overrides config)
```

Behavior when enabled:

- Every `/v1/*` request must carry `Authorization: Bearer <token>`; a missing or wrong token
  returns **401** `{ "error": ... }`.
- The static frontend and `/config.json` stay **open** so the UI can boot pre-auth;
  `/healthz` is also never gated (so kubelet probes work).
- The effective token is `VERDIGRIS_API_TOKEN` if set (non-empty), otherwise `auth.token` —
  so the secret needn't live in a config file in production.
- If `enabled` is true but no token resolves, `vdg serve` refuses to start.

!!! warning "Live tail + auth"
    The live-tail SSE endpoint is consumed by the browser's `EventSource`, which can't send
    an `Authorization` header. Front it with a query-param token or ingress auth when
    `[auth]` is on.

## Environment overrides

| Variable | Effect |
|---|---|
| `VERDIGRIS_CONFIG` | path to the config file (see resolution order above) |
| `VERDIGRIS_API_TOKEN` | overrides `auth.token` (preferred way to supply the secret) |
| Standard AWS chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, IRSA web-identity, …) | resolves S3 credentials for the `s3` backend when they aren't set inline |

## A complete example

```toml
[storage]
backend = "s3"
bucket = "my-company-logs"
region = "us-east-1"

[query]
cores = 8

[routing]
error = "hot"
warn  = "warm"
info  = "warm"
debug = "cold"

[lifecycle]
hot_to_warm_days  = 3
warm_to_cold_days = 30
expire_days       = 400

[auth]
enabled = true
# token supplied via VERDIGRIS_API_TOKEN, not here
```

On EKS you normally don't hand-write this: the Helm chart renders `verdigris.toml` from its
`storage.*`, `query.*`, `routing.*`, and `lifecycle.*` values. See [Install](install.md).
