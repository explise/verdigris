# syntax=docker/dockerfile:1
#
# Verdigris (vdg) container image.
#
# Multi-stage: a Node stage builds the production web UI (web/ — Vite + SolidJS)
# into a static bundle, a Rust builder compiles the `vdg` binary with the `serve`
# feature (which pulls in the DataFusion query engine + axum HTTP server), and a
# slim Debian runtime carries the binary, the built UI, and a default config. The
# image runs `vdg serve` — the HTTP API + UI — as a non-root user.
#
#   docker build -t verdigris:dev .
#   docker run --rm -p 8080:8080 verdigris:dev
#
# By default it serves the production UI from /app/web and runs FULLY OFFLINE
# against a local filesystem store at /app/data (empty until you ingest). Point
# it at S3 by mounting a config with `[storage] backend = "s3"` and setting
# VERDIGRIS_CONFIG (the Helm chart does this for you). To seed demo data into a
# running container:
#   docker exec <id> vdg ingest --table logs --generate 20000

# ---- web builder ------------------------------------------------------------
# Build the production SolidJS SPA (web/) into a static bundle. Vite emits to
# web/dist (see web/vite.config.ts); the Rust binary serves it via --frontend.
FROM node:20-slim AS web-builder
WORKDIR /web

# Install deps against the lockfile first so this layer caches on manifest churn
# only. `npm ci` is exact/reproducible and requires package-lock.json.
COPY web/package.json web/package-lock.json ./
RUN npm ci

# Copy the rest of the web sources and build. (web/node_modules and web/dist are
# excluded via .dockerignore, so the npm ci output above is not clobbered.)
COPY web/ ./
RUN npm run build

# ---- builder ----------------------------------------------------------------
FROM rust:1-bookworm AS builder
WORKDIR /src

# Copy the workspace manifests first so the dependency graph is cached
# independently of source churn.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the release binary with the serve + apply features: `serve` = HTTP API +
# static frontend + DataFusion; `apply` = the aws-sdk path so `vdg lifecycle
# --apply` works in-cluster (the Helm post-install hook Job runs it). This is the
# slow layer (DataFusion + AWS SDK are large dependency trees).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --features "serve apply" -p vdg && \
    cp /src/target/release/vdg /usr/local/bin/vdg

# ---- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: required for TLS to real AWS S3. tini: proper PID-1 signal
# handling so `docker stop` / pod termination is clean.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates tini && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin verdigris

WORKDIR /app

COPY --from=builder /usr/local/bin/vdg /usr/local/bin/vdg
# Production UI (Vite/SolidJS build) served at "/" by `vdg serve --frontend`.
# This is the default served UI (see CMD below).
COPY --from=web-builder /web/dist /app/web
# The original vanilla no-build prototype, kept for reference / fallback. Serve
# it with `--frontend /app/frontend` if desired.
COPY frontend /app/frontend
# Default (offline, local-fs) config. The Helm chart overrides this by mounting
# a ConfigMap and setting VERDIGRIS_CONFIG.
COPY config/verdigris.toml /app/config/verdigris.toml

# Local-store scratch dir (used only when backend = "local").
RUN mkdir -p /app/data && chown -R verdigris:verdigris /app

USER verdigris

# Config resolution order in the binary: --config flag, else $VERDIGRIS_CONFIG,
# else ./config/verdigris.toml, else built-in defaults.
ENV VERDIGRIS_CONFIG=/app/config/verdigris.toml

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "vdg"]
CMD ["serve", "--port", "8080", "--table", "logs", "--frontend", "/app/web"]
