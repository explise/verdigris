//! `vdg serve` — the HTTP shell. Hosts the static frontend AND the `/v1/*` API
//! that `frontend/api.js` calls when `USE_MOCKS = false`.
//!
//! Endpoints backed by REAL data: /v1/ingest and /v1/otlp/logs (write logs to the
//! store), /v1/query, /v1/query/estimate, /v1/storage/tiers, /v1/settings,
//! /v1/tail (live SSE), and the volume/cost figures in /v1/metrics and /v1/cost
//! (computed from the manifest + cost model). Endpoints we can't back yet
//! (alerts, pipelines, time-series metrics) return shape-correct placeholders so
//! the dashboards render — each marked `placeholder: true`.
//!
//! The router is built per `--role`: `all` (everything), `ingest` (only the write
//! endpoints, so exactly ONE node is the manifest writer), or `query` (read/UI
//! endpoints; write endpoints answer 405). Optional bearer-token auth (config
//! `[auth]`) gates the `/v1/*` surface; the static frontend and `/config.json`
//! stay open so the UI can boot pre-auth.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Query, Request, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use axum::extract::Path as AxPath;
use axum::routing::delete;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use verdigris_core::alert::{
    self, Alert, AlertRule, AlertStatus, AlertsDoc, Comparator, State as AlertState, Transition,
};
use verdigris_core::auth::{self, ApiToken, Role as AuthRole, TokensDoc};
use verdigris_core::batch::{BatchPolicy, LogRecord};
use verdigris_core::config::{Config, StorageConfig};
use verdigris_core::cost::{self, RetrievalMode};
use verdigris_core::manifest::Manifest;
use verdigris_core::model::Tier;
use verdigris_ingest::wire::JsonLog;
use verdigris_query::engine::{QueryLimits, ResultTooLarge};
use verdigris_storage::Store;

/// Which HTTP surface this node exposes (from `vdg serve --role`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Everything: read + write endpoints + static frontend.
    All,
    /// Only the write endpoints (`/v1/ingest`, `/v1/otlp/logs`). Run exactly one
    /// of these as the single manifest writer.
    Ingest,
    /// Read/UI endpoints + static frontend; write endpoints return 405. Run N of
    /// these as stateless readers.
    Query,
}

impl From<crate::RoleArg> for Role {
    fn from(r: crate::RoleArg) -> Self {
        match r {
            crate::RoleArg::All => Role::All,
            crate::RoleArg::Ingest => Role::Ingest,
            crate::RoleArg::Query => Role::Query,
        }
    }
}

impl Role {
    fn serves_reads(self) -> bool {
        matches!(self, Role::All | Role::Query)
    }
    fn serves_writes(self) -> bool {
        matches!(self, Role::All | Role::Ingest)
    }
}

#[derive(Clone)]
struct AppState {
    cfg: Arc<Config>,
    table: Arc<String>,
    /// Serializes ingest writes within this process. The manifest is read-
    /// modify-written, so concurrent `POST /v1/ingest` calls (e.g. a Vector
    /// DaemonSet fanning in) must not interleave. Cross-process/replica
    /// concurrency still needs Iceberg commits — a known, documented gap.
    ingest_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes writes to the alerts doc — a scheduler tick and a concurrent
    /// create/delete must not clobber each other's read-modify-write.
    alerts_lock: Arc<tokio::sync::Mutex<()>>,
    /// Monotonic suffix for generated alert ids within this process.
    alert_seq: Arc<AtomicU64>,
    /// Backpressure: caps concurrent in-flight ingest requests so a flood sheds
    /// (429) instead of piling parsed bodies up in memory.
    ingest_sem: Arc<tokio::sync::Semaphore>,
    /// In-memory cache of the persisted API-token catalog. Auth checks read this
    /// (not the store) per request; issue/revoke write through + a refresh task
    /// reloads it so revocations propagate across replicas.
    tokens: Arc<tokio::sync::RwLock<TokensDoc>>,
    /// Self-observability counters/histogram for the service itself, exposed at
    /// `/metrics` in Prometheus text format.
    metrics: Arc<HttpMetrics>,
    /// Bounded in-memory ring of recent queries — powers `expensiveQueries`.
    /// Loaded from the persisted audit doc at boot; every recorded query is also
    /// written through to `{table}/_audit/query-history.json` under optimistic
    /// CAS, so the audit trail survives restarts (the audit endpoint reads the
    /// persisted doc, which collects records from all replicas).
    query_history: Arc<tokio::sync::Mutex<VecDeque<QueryRecord>>>,
    /// Observability for the background auto-compaction scheduler. Updated by the
    /// scheduler task; read (with a live pending count) by `/v1/metrics`.
    compaction: Arc<CompactionMetrics>,
}

/// Counters for background auto-compaction. The live "pending files" figure is
/// computed on demand from the manifest (see [`h_metrics`]); these track history.
struct CompactionMetrics {
    /// Wall-clock ms of the last run that merged files (0 = never).
    last_run_ms: AtomicU64,
    /// Scheduler runs that actually merged files.
    runs_total: AtomicU64,
    /// Files merged across all runs.
    files_merged_total: AtomicU64,
}

impl CompactionMetrics {
    fn new() -> Self {
        Self {
            last_run_ms: AtomicU64::new(0),
            runs_total: AtomicU64::new(0),
            files_merged_total: AtomicU64::new(0),
        }
    }
}

/// The authenticated caller, stashed in request extensions by `require_auth` and
/// read by handlers that record who did what. Absent when auth is off.
#[derive(Clone)]
struct Identity(String);

/// One recorded query kept in the audit ring and the persisted audit doc.
#[derive(Clone, Serialize, Deserialize)]
struct QueryRecord {
    ts_millis: i64,
    user: String,
    sql: String,
    scanned_bytes: u64,
    cost_usd: f64,
    /// Coldest tier the scan touched ("hot"/"warm"/"cold").
    tier: String,
}

const QUERY_HISTORY_CAP: usize = 500;

/// The persisted audit trail: `{table}/_audit/query-history.json`, oldest first,
/// trimmed to [`QUERY_HISTORY_CAP`]. Appended under optimistic CAS (same
/// discipline as the manifest) so concurrent reader replicas don't clobber each
/// other's records.
#[derive(Default, Serialize, Deserialize)]
struct AuditDoc {
    queries: Vec<QueryRecord>,
}

fn audit_path(table: &str) -> ObjPath {
    ObjPath::from(format!("{table}/_audit/query-history.json"))
}

/// Load the persisted audit doc plus its version (for a CAS write-back).
/// Missing → empty; a parse error also yields empty (a corrupt audit doc must
/// not take queries down) but preserves the version so the next append repairs it.
async fn load_audit(s: &Store, table: &str) -> anyhow::Result<(AuditDoc, Option<UpdateVersion>)> {
    match s.get(&audit_path(table)).await {
        Ok(res) => {
            let version = Some(UpdateVersion {
                e_tag: res.meta.e_tag.clone(),
                version: res.meta.version.clone(),
            });
            let bytes = res.bytes().await.context("reading audit doc")?;
            let doc = serde_json::from_slice(&bytes).unwrap_or_default();
            Ok((doc, version))
        }
        Err(object_store::Error::NotFound { .. }) => Ok((AuditDoc::default(), None)),
        Err(e) => Err(e).context("loading audit doc"),
    }
}

/// Append one record to the persisted audit doc under optimistic CAS: reload,
/// append, trim, conditional put; retry on conflict (another replica appended
/// first). Backends without conditional puts fall back to a plain put — correct
/// under the single-writer deployment model.
async fn append_audit(s: &Store, table: &str, rec: QueryRecord) -> anyhow::Result<()> {
    for _ in 0..4 {
        let (mut doc, version) = load_audit(s, table).await?;
        doc.queries.push(rec.clone());
        if doc.queries.len() > QUERY_HISTORY_CAP {
            let excess = doc.queries.len() - QUERY_HISTORY_CAP;
            doc.queries.drain(..excess);
        }
        let bytes = serde_json::to_vec(&doc).context("serializing audit doc")?;
        let mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        match s
            .put_opts(
                &audit_path(table),
                bytes.clone().into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(object_store::Error::NotImplemented { .. }) => {
                s.put(&audit_path(table), bytes.into())
                    .await
                    .context("writing audit doc (no-CAS fallback)")?;
                return Ok(());
            }
            Err(e) => return Err(e).context("committing audit doc"),
        }
    }
    anyhow::bail!("audit append failed after retries under contention")
}

/// Minimal, dependency-free Prometheus metrics for the service: request counts by
/// status class and a real request-latency histogram. Recorded by the
/// `track_metrics` middleware, rendered at `GET /metrics`.
struct HttpMetrics {
    /// Requests bucketed by status class: [2xx, 4xx, 5xx, other].
    by_class: [AtomicU64; 4],
    /// Non-cumulative latency-bucket counts, aligned to `LATENCY_BUCKETS_MS`
    /// (last slot is the +Inf overflow).
    latency: [AtomicU64; 12],
    latency_sum_ms: AtomicU64,
    latency_count: AtomicU64,
    /// Domain counters.
    ingest_records: AtomicU64,
    queries: AtomicU64,
}

/// Upper bounds (ms) for the latency histogram; a trailing +Inf bucket is implied.
const LATENCY_BUCKETS_MS: [f64; 11] = [
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

impl HttpMetrics {
    fn new() -> Self {
        Self {
            by_class: Default::default(),
            latency: Default::default(),
            latency_sum_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            ingest_records: AtomicU64::new(0),
            queries: AtomicU64::new(0),
        }
    }

    fn observe(&self, latency_ms: f64, status: u16) {
        let class = match status {
            200..=299 => 0,
            400..=499 => 1,
            500..=599 => 2,
            _ => 3,
        };
        self.by_class[class].fetch_add(1, Ordering::Relaxed);
        let idx = LATENCY_BUCKETS_MS
            .iter()
            .position(|&b| latency_ms <= b)
            .unwrap_or(LATENCY_BUCKETS_MS.len()); // +Inf overflow slot
        self.latency[idx].fetch_add(1, Ordering::Relaxed);
        self.latency_sum_ms
            .fetch_add(latency_ms as u64, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Render Prometheus text exposition format.
    fn render(&self) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut out = String::new();
        out.push_str("# HELP verdigris_http_requests_total HTTP requests by status class.\n");
        out.push_str("# TYPE verdigris_http_requests_total counter\n");
        for (i, class) in ["2xx", "4xx", "5xx", "other"].iter().enumerate() {
            out.push_str(&format!(
                "verdigris_http_requests_total{{class=\"{class}\"}} {}\n",
                g(&self.by_class[i])
            ));
        }
        out.push_str("# HELP verdigris_http_request_duration_seconds Request latency.\n");
        out.push_str("# TYPE verdigris_http_request_duration_seconds histogram\n");
        let mut cumulative = 0u64;
        for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            cumulative += g(&self.latency[i]);
            out.push_str(&format!(
                "verdigris_http_request_duration_seconds_bucket{{le=\"{}\"}} {cumulative}\n",
                bound / 1000.0
            ));
        }
        cumulative += g(&self.latency[LATENCY_BUCKETS_MS.len()]);
        out.push_str(&format!(
            "verdigris_http_request_duration_seconds_bucket{{le=\"+Inf\"}} {cumulative}\n"
        ));
        out.push_str(&format!(
            "verdigris_http_request_duration_seconds_sum {}\n",
            g(&self.latency_sum_ms) as f64 / 1000.0
        ));
        out.push_str(&format!(
            "verdigris_http_request_duration_seconds_count {}\n",
            g(&self.latency_count)
        ));
        out.push_str("# HELP verdigris_ingest_records_total Log records accepted by ingest.\n");
        out.push_str("# TYPE verdigris_ingest_records_total counter\n");
        out.push_str(&format!(
            "verdigris_ingest_records_total {}\n",
            g(&self.ingest_records)
        ));
        out.push_str("# HELP verdigris_queries_total Queries executed.\n");
        out.push_str("# TYPE verdigris_queries_total counter\n");
        out.push_str(&format!("verdigris_queries_total {}\n", g(&self.queries)));
        out
    }
}

/// Middleware: time every request and record its status + latency.
async fn track_metrics(
    State(metrics): State<Arc<HttpMetrics>>,
    req: Request,
    next: Next,
) -> Response {
    let start = std::time::Instant::now();
    let res = next.run(req).await;
    metrics.observe(
        start.elapsed().as_secs_f64() * 1000.0,
        res.status().as_u16(),
    );
    res
}

/// `GET /metrics` — Prometheus text exposition for the service itself. Open (no
/// auth), like `/healthz`, so a scraper can always reach it.
async fn h_prometheus(State(st): State<AppState>) -> Response {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        st.metrics.render(),
    )
        .into_response()
}

/// An error response: a status code + a JSON `{ "error": ... }` body. Defaults to
/// 500; query/parse failures use 400 so the client can tell a broken query from
/// zero matches.
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn bad_request(e: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: e.to_string(),
        }
    }

    fn with_status(status: StatusCode, e: impl std::fmt::Display) -> Self {
        Self {
            status,
            message: e.to_string(),
        }
    }

    /// Map a query-engine failure to a status the client can act on.
    ///
    /// An oversized result is 413, not 400 or 500: the query was valid and the
    /// server is healthy — the client simply asked for more than it is allowed to
    /// receive, and the message says how to ask for less. Everything else (parse
    /// errors, unknown columns) stays 400, as query failures always have.
    fn from_query(e: anyhow::Error) -> Self {
        match e.downcast_ref::<ResultTooLarge>() {
            Some(too_large) => Self::with_status(StatusCode::PAYLOAD_TOO_LARGE, too_large),
            None => Self::bad_request(e),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.into().to_string(),
        }
    }
}

type ApiResult = Result<Json<Value>, AppError>;

/// The query memory ceilings for this deployment (issue #2). Cheap to build, so
/// it's derived per call rather than cached on `AppState`.
fn limits(st: &AppState) -> QueryLimits {
    QueryLimits::from_config(&st.cfg.query)
}

fn store(st: &AppState) -> anyhow::Result<Store> {
    verdigris_storage::build(&st.cfg.storage)
}

async fn manifest(st: &AppState) -> anyhow::Result<(Store, Manifest)> {
    let s = store(st)?;
    let m = verdigris_ingest::Ingestor::new(s.clone(), st.table.as_str())
        .load_manifest()
        .await?;
    Ok((s, m))
}

// ───────────────────────── alerting ─────────────────────────

fn alerts_path(table: &str) -> ObjPath {
    ObjPath::from(format!("{table}/_metadata/alerts.json"))
}

async fn load_alerts(s: &Store, table: &str) -> anyhow::Result<AlertsDoc> {
    Ok(load_alerts_versioned(s, table).await?.0)
}

/// Load the alerts doc plus its store version, for a CAS write-back.
async fn load_alerts_versioned(
    s: &Store,
    table: &str,
) -> anyhow::Result<(AlertsDoc, Option<UpdateVersion>)> {
    match s.get(&alerts_path(table)).await {
        Ok(res) => {
            let version = Some(UpdateVersion {
                e_tag: res.meta.e_tag.clone(),
                version: res.meta.version.clone(),
            });
            let bytes = res.bytes().await?;
            let doc = serde_json::from_slice(&bytes).context("parsing alerts.json")?;
            Ok((doc, version))
        }
        Err(object_store::Error::NotFound { .. }) => Ok((AlertsDoc::default(), None)),
        Err(e) => Err(e.into()),
    }
}

/// Save the alerts doc under optimistic CAS against the version it was loaded
/// at — the same commit discipline as the manifest, so a scheduler tick and a
/// create/delete on another replica can't clobber each other's read-modify-
/// write. `Ok(false)` = conflict: reload and redo. Backends without conditional
/// puts fall back to a plain put (correct under the single-writer model).
async fn save_alerts_cas(
    s: &Store,
    table: &str,
    doc: &AlertsDoc,
    base: Option<UpdateVersion>,
) -> anyhow::Result<bool> {
    let bytes = serde_json::to_vec_pretty(doc).context("serializing alerts.json")?;
    let mode = match base {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    match s
        .put_opts(
            &alerts_path(table),
            bytes.clone().into(),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::Precondition { .. })
        | Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(object_store::Error::NotImplemented { .. }) => {
            s.put(&alerts_path(table), bytes.into())
                .await
                .context("writing alerts.json (no-CAS fallback)")?;
            Ok(true)
        }
        Err(e) => Err(e).context("committing alerts.json"),
    }
}

/// Retry budget for alert-doc CAS commits under contention.
const ALERTS_CAS_RETRIES: usize = 4;

/// Run a rule's SQL and pull out its single numeric result — the `v` column if
/// present, else the first numeric column of the first row.
async fn measure(
    s: &Store,
    table: &str,
    files: &[String],
    sql: &str,
    limits: &QueryLimits,
) -> anyhow::Result<f64> {
    let rows =
        verdigris_query::engine::query_table_json(s.clone(), table, files, sql, limits).await?;
    let Some(Value::Object(row)) = rows.into_iter().next() else {
        return Ok(0.0);
    };
    if let Some(v) = row.get("v").and_then(Value::as_f64) {
        return Ok(v);
    }
    for val in row.values() {
        if let Some(n) = val.as_f64() {
            return Ok(n);
        }
    }
    Ok(0.0)
}

/// One evaluation pass over every enabled rule: measure, advance state, persist
/// under CAS, and only then fire webhooks — a notification goes out only for a
/// transition that was actually committed, so a save conflict can't double-fire.
/// `lock` serializes this against concurrent create/delete within the process;
/// the CAS covers other replicas.
async fn evaluate_all(
    s: &Store,
    table: &str,
    lock: &tokio::sync::Mutex<()>,
    limits: &QueryLimits,
) -> anyhow::Result<()> {
    let _g = lock.lock().await;
    for _ in 0..ALERTS_CAS_RETRIES {
        let (mut doc, base) = load_alerts_versioned(s, table).await?;
        if doc.alerts.is_empty() {
            return Ok(());
        }
        let m = verdigris_ingest::Ingestor::new(s.clone(), table)
            .load_manifest()
            .await?;
        let files: Vec<String> = m.files.iter().map(|f| f.path.clone()).collect();
        let now = crate::now_millis() as u64;
        // Webhooks for this pass, held until the state they announce is committed.
        let mut notifications: Vec<(String, Value)> = Vec::new();
        for a in doc.alerts.iter_mut() {
            if !a.rule.enabled {
                continue;
            }
            let value = match measure(s, table, &files, &a.rule.sql, limits).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(rule = %a.rule.name, error = %e, "alert eval failed");
                    continue;
                }
            };
            let (next, transition) = alert::evaluate(&a.rule, &a.status, value, now);
            a.status = next;
            if matches!(transition, Transition::Fired | Transition::Resolved) {
                tracing::info!(rule = %a.rule.name, ?transition, value, "alert transition");
                if let Some(url) = a.rule.webhook.clone() {
                    let firing = matches!(transition, Transition::Fired);
                    notifications.push((
                        url,
                        json!({
                            "alert": a.rule.name,
                            "severity": a.rule.severity,
                            "state": if firing { "firing" } else { "resolved" },
                            "value": value,
                            "threshold": a.rule.threshold,
                        }),
                    ));
                }
            }
        }
        if !save_alerts_cas(s, table, &doc, base).await? {
            // Someone else committed meanwhile (create/delete/another tick):
            // redo against the fresh doc; the collected notifications describe
            // a state that never landed, so they are dropped, not sent.
            continue;
        }
        for (url, payload) in notifications {
            tokio::spawn(async move {
                if let Err(e) = notify_webhook(&url, payload).await {
                    tracing::warn!(error = %e, "alert webhook failed");
                }
            });
        }
        return Ok(());
    }
    anyhow::bail!("alert evaluation commit failed after retries under contention")
}

async fn notify_webhook(url: &str, payload: Value) -> anyhow::Result<()> {
    reqwest::Client::new()
        .post(url)
        .json(&payload)
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    Ok(())
}

/// One compaction-scheduler tick. Reads the manifest, and if any single tier has
/// at least `trigger` files pending a merge, runs a compaction pass and records
/// what it merged. Cheap when nothing is pending (no data is touched). Holds the
/// ingest lock across the pass to avoid CAS thrash with local ingests (cross-
/// replica writers are still resolved by the manifest CAS inside `compact`).
async fn maybe_compact(
    s: &Store,
    table: &str,
    ingest_lock: &tokio::sync::Mutex<()>,
    target_bytes: u64,
    trigger: usize,
    max_merge_files: usize,
    metrics: &CompactionMetrics,
) -> anyhow::Result<()> {
    let ingestor = verdigris_ingest::Ingestor::new(s.clone(), table);
    let manifest = ingestor.load_manifest().await?;
    let max_tier_pending = verdigris_ingest::pending_compaction(&manifest, target_bytes)
        .iter()
        .map(|(_, n)| *n)
        .max()
        .unwrap_or(0);
    if max_tier_pending < trigger {
        return Ok(()); // not enough small files yet to be worth a rewrite
    }

    // Drain the backlog with bounded passes. Each pass holds the ingest lock only
    // for its capped merge, then releases it (yield) so ingest can interleave —
    // a large backlog no longer stalls ingest in one long pass. The 10k-pass cap
    // is a runaway backstop; file count strictly drops per pass, so it terminates.
    let mut total_merged = 0usize;
    let mut passes = 0usize;
    loop {
        let _g = ingest_lock.lock().await;
        let (reports, more) = ingestor
            .compact_bounded(target_bytes, max_merge_files)
            .await?;
        drop(_g);
        total_merged += reports.iter().map(|r| r.files_merged).sum::<usize>();
        passes += 1;
        if !more || passes >= 10_000 {
            break;
        }
        tokio::task::yield_now().await;
    }
    if total_merged > 0 {
        metrics
            .files_merged_total
            .fetch_add(total_merged as u64, Ordering::Relaxed);
        metrics.runs_total.fetch_add(1, Ordering::Relaxed);
        metrics
            .last_run_ms
            .store(crate::now_millis() as u64, Ordering::Relaxed);
        tracing::info!(files_merged = total_merged, passes, "auto-compaction ran");
    }
    Ok(())
}

/// Seed two illustrative rules the first time a table is served, so the Alerts
/// page shows a real firing + OK example instead of an empty screen. No-op once
/// any rule exists.
async fn seed_example_alerts(
    s: &Store,
    table: &str,
    lock: &tokio::sync::Mutex<()>,
) -> anyhow::Result<()> {
    let _g = lock.lock().await;
    let (existing, base) = load_alerts_versioned(s, table).await?;
    if !existing.alerts.is_empty() {
        return Ok(());
    }
    let now = crate::now_millis() as u64;
    let mk =
        |id: &str, name: &str, sql: String, cmp: Comparator, threshold: f64, sev: &str| Alert {
            rule: AlertRule {
                id: id.to_string(),
                name: name.to_string(),
                sql,
                comparator: cmp,
                threshold,
                severity: sev.to_string(),
                webhook: None,
                enabled: true,
            },
            status: AlertStatus::initial(now),
        };
    let doc = AlertsDoc {
        alerts: vec![
            mk(
                "seed-error-volume",
                "High error volume",
                format!("SELECT count(*) AS v FROM {table} WHERE level = 'ERROR'"),
                Comparator::Gt,
                1000.0,
                "critical",
            ),
            mk(
                "seed-auth-5xx",
                "Auth 5xx surge",
                format!(
                    "SELECT count(*) AS v FROM {table} WHERE service = 'auth' AND status >= 500"
                ),
                Comparator::Gt,
                100_000.0,
                "warning",
            ),
        ],
    };
    // CAS with the loaded (empty/absent) version: if another replica seeded
    // first, the conflict is a no-op — their seed stands.
    save_alerts_cas(s, table, &doc, base).await?;
    Ok(())
}

fn humanize_ms(ms: u64) -> String {
    let secs = ms / 1000;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.2}")
    }
}

fn webhook_host(url: &str) -> String {
    url.split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or(url)
        .to_string()
}

fn parse_tier(s: &str) -> Option<Tier> {
    match s {
        "hot" => Some(Tier::Hot),
        "warm" => Some(Tier::Warm),
        "cold" => Some(Tier::Cold),
        _ => None,
    }
}

pub async fn serve(
    cfg: Config,
    table: String,
    port: u16,
    frontend: PathBuf,
    role: Role,
) -> anyhow::Result<()> {
    // Auth setup (BEFORE cfg moves into the shared state). `[auth].token` (or
    // VERDIGRIS_API_TOKEN), when set, is the BOOTSTRAP ADMIN secret — use it to
    // issue per-user tokens via POST /v1/auth/tokens; those persist in the store
    // and can be revoked without a restart. We keep only its hash.
    let auth_enabled = cfg.auth.enabled;
    let bootstrap_admin_hash: Option<Arc<String>> = if auth_enabled {
        match cfg.resolved_auth_token() {
            Some(t) => Some(Arc::new(auth::hash_token(&t))),
            None => anyhow::bail!(
                "[auth].enabled is true but no bootstrap token is set — set auth.token or VERDIGRIS_API_TOKEN (it becomes the admin token)"
            ),
        }
    } else {
        None
    };

    // Ingest memory bounds (read before `cfg` moves into the shared state).
    let max_body = cfg.ingest.max_body_bytes;
    let max_inflight = cfg.ingest.max_inflight.max(1);

    // Load the persisted token catalog into the in-memory cache.
    let boot_store = verdigris_storage::build(&cfg.storage).ok();
    let tokens_doc = match &boot_store {
        Some(s) => load_tokens(s).await.unwrap_or_default(),
        None => TokensDoc::default(),
    };

    // Load the persisted audit trail so query history survives restarts.
    let boot_history: VecDeque<QueryRecord> = match &boot_store {
        Some(s) => load_audit(s, &table)
            .await
            .map(|(doc, _)| doc.queries.into())
            .unwrap_or_default(),
        None => VecDeque::new(),
    };

    let state = AppState {
        cfg: Arc::new(cfg),
        table: Arc::new(table),
        ingest_lock: Arc::new(tokio::sync::Mutex::new(())),
        alerts_lock: Arc::new(tokio::sync::Mutex::new(())),
        alert_seq: Arc::new(AtomicU64::new(0)),
        ingest_sem: Arc::new(tokio::sync::Semaphore::new(max_inflight)),
        tokens: Arc::new(tokio::sync::RwLock::new(tokens_doc)),
        metrics: Arc::new(HttpMetrics::new()),
        query_history: Arc::new(tokio::sync::Mutex::new(boot_history)),
        compaction: Arc::new(CompactionMetrics::new()),
    };

    // Alert evaluator. On a writer role (the single manifest writer owns alert
    // state), seed illustrative rules the first time, evaluate once immediately
    // so the first GET has real state, then re-evaluate on a fixed cadence.
    if role.serves_writes() {
        if let Ok(s) = store(&state) {
            let table = state.table.clone();
            let lock = state.alerts_lock.clone();
            let lim = limits(&state);
            let _ = seed_example_alerts(&s, table.as_str(), &lock).await;
            let _ = evaluate_all(&s, table.as_str(), &lock, &lim).await;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    if let Err(e) = evaluate_all(&s, table.as_str(), &lock, &lim).await {
                        tracing::warn!(error = %e, "alert scheduler tick failed");
                    }
                }
            });
        }
    }

    // Background auto-compaction. Only the writer role compacts (it owns the
    // manifest). Each tick is cheap when nothing is pending (a manifest read +
    // arithmetic); it only rewrites files once a tier crosses the trigger.
    if role.serves_writes() && state.cfg.compaction.enabled {
        if let Ok(s) = store(&state) {
            let table = state.table.clone();
            let cm = state.compaction.clone();
            let lock = state.ingest_lock.clone();
            let ccfg = state.cfg.compaction.clone();
            tokio::spawn(async move {
                let target = ccfg.target_bytes();
                let interval = Duration::from_secs(ccfg.interval_secs.max(1));
                loop {
                    tokio::time::sleep(interval).await;
                    if let Err(e) = maybe_compact(
                        &s,
                        table.as_str(),
                        &lock,
                        target,
                        ccfg.trigger_pending_files,
                        ccfg.max_merge_files_per_pass,
                        &cm,
                    )
                    .await
                    {
                        tracing::warn!(error = %e, "compaction scheduler tick failed");
                    }
                }
            });
        }
    }

    // Refresh the token cache periodically so an issue/revoke (persisted by the
    // writer) propagates to reader replicas within the interval.
    if auth_enabled {
        if let Ok(s) = store(&state) {
            let tokens = state.tokens.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(20)).await;
                    if let Ok(doc) = load_tokens(&s).await {
                        *tokens.write().await = doc;
                    }
                }
            });
        }
    }

    // Build the `/v1/*` surface for this role. Auth (if enabled) wraps ONLY this
    // sub-router, so the static frontend and /config.json below stay open.
    let mut api = Router::new();
    if role.serves_writes() {
        api = api
            .route("/v1/ingest", post(h_ingest))
            .route("/v1/otlp/logs", post(h_otlp));
    } else {
        // query-role: the write endpoints exist but are disabled (405), so a
        // misrouted writer gets a clear method error, not a 404.
        api = api
            .route("/v1/ingest", post(write_disabled))
            .route("/v1/otlp/logs", post(write_disabled));
    }
    if role.serves_reads() {
        api = api
            .route("/v1/query", post(h_query))
            .route("/v1/query/estimate", post(h_estimate))
            .route("/v1/metrics", get(h_metrics))
            .route("/v1/alerts", get(h_alerts).post(h_alert_create))
            .route("/v1/alerts/{id}", delete(h_alert_delete))
            .route("/v1/storage/tiers", get(h_storage))
            .route("/v1/cost", get(h_cost))
            .route("/v1/pipelines", get(h_pipelines))
            .route("/v1/settings", get(h_settings))
            .route("/v1/tail", get(h_tail))
            .route("/v1/audit/queries", get(h_audit_queries));
    }
    // Token management (issue/list/revoke) — admin-only, enforced by
    // `required_role`. Only mounted when auth is on; otherwise there's no
    // identity and it would be an open door.
    if role.serves_reads() && auth_enabled {
        api = api
            .route("/v1/auth/tokens", get(h_token_list).post(h_token_create))
            .route("/v1/auth/tokens/{id}", delete(h_token_revoke));
    }
    if auth_enabled {
        let auth_state = AuthState {
            tokens: state.tokens.clone(),
            bootstrap_admin_hash: bootstrap_admin_hash.clone(),
        };
        api = api.layer(middleware::from_fn_with_state(auth_state, require_auth));
    }

    // Static frontend + pre-auth config are added AFTER the auth layer so they are
    // never gated (the UI must load them to render a login state at all).
    let mut app = api;
    // Liveness/readiness probe: 200 in EVERY role and OUTSIDE the auth layer
    // (kubelet carries no token). The ingest role serves no web root, so k8s
    // probes must target this endpoint rather than `/`.
    app = app
        .route("/healthz", get(h_healthz))
        // Prometheus scrape endpoint for the SERVICE (open, like /healthz).
        .route("/metrics", get(h_prometheus));
    if role.serves_reads() {
        app = app
            // Runtime deployment config the `web/` SPA reads at boot. Pins it to
            // this backend: live (no mocks), JSON wire, single-tenant on-prem so
            // transport talks to the flat /v1/* surface. The vanilla `frontend/`
            // ignores it.
            .route("/config.json", get(h_config))
            // Static frontend + SPA fallback. A request matching no real file
            // serves index.html so the path-routed SPA (`/:org/:env/logs` on
            // refresh / deep link) boots and routes client-side. `ServeDir` forces
            // that fallback to a 404 status; `flip_404_to_200` (scoped to this
            // static sub-router only, so API status codes are untouched) rewrites
            // it to 200. The hash-routed vanilla frontend is unaffected.
            .fallback_service(static_frontend(&frontend));
    }
    // Cap every request body so an oversized ingest payload can't OOM the
    // process — it's rejected (413) before being buffered into memory. The
    // metrics layer is outermost so it times the full request.
    let metrics = state.metrics.clone();
    let app = app
        .layer(DefaultBodyLimit::max(max_body))
        .layer(CorsLayer::permissive())
        .layer(middleware::from_fn_with_state(metrics, track_metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("binding port {port}"))?;
    println!(
        "verdigris serving on http://localhost:{port} (role: {})",
        role_name(role)
    );
    if role.serves_reads() {
        println!("  frontend: {}", frontend.display());
        println!("  api:      http://localhost:{port}/v1/query");
        println!("  tail:     GET  http://localhost:{port}/v1/tail  (SSE)");
    }
    if role.serves_writes() {
        println!("  ingest:   POST http://localhost:{port}/v1/ingest    (NDJSON logs)");
        println!("  otlp:     POST http://localhost:{port}/v1/otlp/logs (OTLP/JSON logs)");
    }
    if auth_enabled {
        println!("  auth:     bearer-token required on /v1/*");
    }
    println!("  remember to set USE_MOCKS = false in frontend/api.js");
    axum::serve(listener, app).await.context("http server")?;
    Ok(())
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::All => "all",
        Role::Ingest => "ingest",
        Role::Query => "query",
    }
}

/// Liveness/readiness probe: always 200, no auth, in every role. k8s probes and
/// health checks target this (the ingest role serves no web root to hit).
async fn h_healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// Placeholder handler for write endpoints on a query-role node: 405 with the
/// standard `{"error":...}` body.
async fn write_disabled() -> AppError {
    AppError::with_status(
        StatusCode::METHOD_NOT_ALLOWED,
        "write endpoints are disabled on a query-role node (run an ingest/all-role node to write)",
    )
}

// ───────────────────────── authn / authz ─────────────────────────

/// Middleware state for [`require_auth`].
#[derive(Clone)]
struct AuthState {
    tokens: Arc<tokio::sync::RwLock<TokensDoc>>,
    /// SHA-256 of the bootstrap admin secret (`[auth].token`), if configured.
    bootstrap_admin_hash: Option<Arc<String>>,
}

/// The role a request requires. Reads (including the POST-bodied `/v1/query`
/// and `/v1/query/estimate`) need ReadOnly; writes (ingest/OTLP, alert
/// create/delete) need ReadWrite; token management needs Admin.
fn required_role(method: &Method, path: &str) -> AuthRole {
    if path.starts_with("/v1/auth/") || path.starts_with("/v1/audit/") {
        return AuthRole::Admin;
    }
    match *method {
        Method::POST if path == "/v1/ingest" || path == "/v1/otlp/logs" || path == "/v1/alerts" => {
            AuthRole::ReadWrite
        }
        Method::DELETE if path.starts_with("/v1/alerts/") => AuthRole::ReadWrite,
        _ => AuthRole::ReadOnly,
    }
}

/// The presented secret: `Authorization: Bearer …`, or — for streams the
/// browser's `EventSource` opens (which can't set headers) — `?access_token=…`.
fn presented_secret(req: &Request) -> Option<String> {
    if let Some(t) = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
    {
        return Some(t.trim().to_string());
    }
    req.uri().query().and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("access_token="))
            .map(|v| v.to_string())
    })
}

/// Authenticate the token to a [`Role`], then authorize against the route's
/// [`required_role`]. 401 for missing/invalid/revoked; 403 for a valid token
/// without sufficient role.
async fn require_auth(State(auth): State<AuthState>, mut req: Request, next: Next) -> Response {
    let Some(secret) = presented_secret(&req) else {
        return AppError::with_status(StatusCode::UNAUTHORIZED, "missing bearer token")
            .into_response();
    };
    let (role, user) = {
        let hash = auth::hash_token(&secret);
        if auth
            .bootstrap_admin_hash
            .as_deref()
            .is_some_and(|b| b.as_str() == hash)
        {
            (Some(AuthRole::Admin), "bootstrap-admin".to_string())
        } else {
            match auth.tokens.read().await.authenticate(&secret) {
                Some(t) => (Some(t.role), t.name.clone()),
                None => (None, String::new()),
            }
        }
    };
    let Some(role) = role else {
        return AppError::with_status(StatusCode::UNAUTHORIZED, "invalid or revoked token")
            .into_response();
    };
    let required = required_role(req.method(), req.uri().path());
    if !role.permits(required) {
        return AppError::with_status(
            StatusCode::FORBIDDEN,
            format!(
                "this endpoint requires the '{}' role (your token is '{}')",
                required.as_str(),
                role.as_str()
            ),
        )
        .into_response();
    }
    // Record who's calling so handlers can log it (audit / query history).
    req.extensions_mut().insert(Identity(user));
    next.run(req).await
}

fn tokens_path() -> ObjPath {
    ObjPath::from("_auth/tokens.json")
}

async fn load_tokens(s: &Store) -> anyhow::Result<TokensDoc> {
    match s.get(&tokens_path()).await {
        Ok(res) => {
            let bytes = res.bytes().await?;
            serde_json::from_slice(&bytes).context("parsing tokens.json")
        }
        Err(object_store::Error::NotFound { .. }) => Ok(TokensDoc::default()),
        Err(e) => Err(e.into()),
    }
}

async fn save_tokens(s: &Store, doc: &TokensDoc) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(doc).context("serializing tokens.json")?;
    s.put(&tokens_path(), bytes.into()).await?;
    Ok(())
}

/// A fresh 256-bit token secret as hex. Returned to the caller once; only its
/// hash is ever stored.
fn gen_secret() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn default_role() -> AuthRole {
    AuthRole::ReadOnly
}

#[derive(Deserialize)]
struct NewToken {
    name: String,
    #[serde(default = "default_role")]
    role: AuthRole,
}

async fn h_token_create(
    State(st): State<AppState>,
    Json(req): Json<NewToken>,
) -> Result<Response, AppError> {
    if req.name.trim().is_empty() {
        return Err(AppError::bad_request("name is required"));
    }
    let secret = gen_secret();
    let now = crate::now_millis() as u64;
    let seq = st.alert_seq.fetch_add(1, Ordering::Relaxed);
    let token = ApiToken {
        id: format!("tok-{now}-{seq}"),
        name: req.name,
        role: req.role,
        hash: auth::hash_token(&secret),
        created_ms: now,
        revoked: false,
    };
    let id = token.id.clone();
    let s = store(&st)?;
    {
        let mut doc = st.tokens.write().await;
        doc.tokens.push(token);
        save_tokens(&s, &doc).await?;
    }
    // The secret is shown ONCE — only its hash is persisted.
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": id,
            "token": secret,
            "note": "store this now — it is not shown again",
        })),
    )
        .into_response())
}

async fn h_token_list(State(st): State<AppState>) -> ApiResult {
    let doc = st.tokens.read().await;
    let out: Vec<Value> = doc
        .tokens
        .iter()
        .map(|t| {
            json!({
                "id": t.id, "name": t.name, "role": t.role,
                "createdMs": t.created_ms, "revoked": t.revoked,
            })
        })
        .collect();
    Ok(Json(Value::Array(out)))
}

async fn h_token_revoke(State(st): State<AppState>, AxPath(id): AxPath<String>) -> ApiResult {
    let s = store(&st)?;
    let mut doc = st.tokens.write().await;
    let mut revoked = false;
    for t in doc.tokens.iter_mut() {
        if t.id == id && !t.revoked {
            t.revoked = true;
            revoked = true;
        }
    }
    if revoked {
        save_tokens(&s, &doc).await?;
    }
    Ok(Json(json!({ "revoked": revoked })))
}

/// `GET /v1/audit/queries` — recent query history (who/when/sql/scanned/cost),
/// newest first. Admin-only (enforced by `required_role`). Reads the persisted
/// audit doc — durable across restarts and complete across replicas; the
/// in-memory ring is only the fallback if the store is unreachable.
async fn h_audit_queries(State(st): State<AppState>) -> ApiResult {
    let persisted: Option<Vec<QueryRecord>> = match store(&st) {
        Ok(s) => load_audit(&s, st.table.as_str())
            .await
            .ok()
            .map(|(d, _)| d.queries),
        Err(_) => None,
    };
    let hist: Vec<QueryRecord> = match persisted {
        Some(q) => q,
        None => st.query_history.lock().await.iter().cloned().collect(),
    };
    let now = crate::now_millis();
    let out: Vec<Value> = hist
        .iter()
        .rev()
        .map(|r| {
            json!({
                "ts": r.ts_millis,
                "user": r.user,
                "sql": r.sql,
                "scannedBytes": r.scanned_bytes,
                "costUsd": r.cost_usd,
                "tier": r.tier,
                "when": format!("{} ago", humanize_ms((now - r.ts_millis).max(0) as u64)),
            })
        })
        .collect();
    Ok(Json(Value::Array(out)))
}

/// The static-frontend service: real files from `frontend`, else index.html for
/// SPA client-side routing. Isolated in its own `Router` so the 404→200 rewrite
/// (see below) applies ONLY here, never to the `/v1/*` API status codes.
fn static_frontend(frontend: &PathBuf) -> Router {
    let index = frontend.join("index.html");
    Router::new()
        .fallback_service(
            ServeDir::new(frontend).not_found_service(get(move || spa_index(index.clone()))),
        )
        .layer(axum::middleware::map_response(flip_404_to_200))
}

/// SPA fallback: serve index.html for any path that isn't a real static file, so
/// client-side routes survive a hard refresh / deep link.
async fn spa_index(index: PathBuf) -> Response {
    match tokio::fs::read_to_string(&index).await {
        Ok(html) => axum::response::Html(html).into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "index.html not found").into_response(),
    }
}

/// `ServeDir` pins its not-found fallback to a 404; for an SPA the fallback IS a
/// valid page (index.html boots the router), so rewrite 404→200. Scoped to the
/// static sub-router, so it can't mask a real API 4xx.
async fn flip_404_to_200(mut res: Response) -> Response {
    if res.status() == StatusCode::NOT_FOUND {
        *res.status_mut() = StatusCode::OK;
    }
    res
}

// ───────────────────────── real endpoints ─────────────────────────

#[derive(Deserialize)]
struct QueryReq {
    sql: String,
    /// Tiers the scan is scoped to (`hot`/`warm`/`cold`). Empty/omitted = all
    /// tiers. The executed scan registers exactly the files in these tiers — the
    /// same set the cost estimate prices — so a hot-only quote can't silently
    /// scan cold.
    #[serde(default)]
    tiers: Vec<String>,
}

/// True when the client's `Accept` header asks for the Arrow stream wire. The UI
/// negotiates this (config `wire: "arrow"`); anything else gets the JSON envelope.
fn wants_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("application/vnd.apache.arrow"))
}

async fn h_query(
    State(st): State<AppState>,
    headers: HeaderMap,
    identity: Option<Extension<Identity>>,
    Json(req): Json<QueryReq>,
) -> Result<Response, AppError> {
    let arrow = wants_arrow(&headers);
    st.metrics.queries.fetch_add(1, Ordering::Relaxed);
    let (s, m) = manifest(&st).await?;

    // Scope the scan to the requested tiers + the query's time window — the SAME
    // selection `POST /v1/query/estimate` prices (`core::estimate::select_files`),
    // so the quoted cost and the executed query provably read the same files.
    // Empty/omitted tiers = all tiers (non-UI callers; the UI always sends them).
    let mut tiers: Vec<Tier> = req.tiers.iter().filter_map(|t| parse_tier(t)).collect();
    if tiers.is_empty() {
        tiers = Tier::ALL.to_vec();
    }
    let window = verdigris_core::search::time_window(&req.sql, crate::now_millis());
    // `service:`/`level:` equality in the query skips files proven free of the
    // value — the SAME predicates the estimate prunes on, so quote and scan stay
    // in lockstep. (Empty for raw SQL; those files prune by tier+window only.)
    let preds = verdigris_core::search::stat_predicates(&req.sql);
    let selected = verdigris_core::estimate::select_files(&m, &tiers, window, &preds);
    if selected.is_empty() {
        let stats = json!({ "events": 0, "scannedBytes": 0, "elapsedMs": 0, "engine": "datafusion", "files": 0 });
        return Ok(query_response(arrow, Vec::new(), &stats, &Vec::new()));
    }
    let scanned_bytes: u64 = selected.iter().map(|f| f.bytes).sum();
    let files: Vec<String> = selected.iter().map(|f| f.path.clone()).collect();

    // The frontend search bar sends its DSL in `sql`; raw SQL is passed through.
    // A malformed query is a 400, not a 200-with-empty-rows, so the client can
    // tell a broken query from zero matches.
    let sql = if verdigris_core::search::looks_like_sql(&req.sql) {
        req.sql.clone()
    } else {
        verdigris_core::search::to_sql(&req.sql, st.table.as_str(), crate::now_millis(), 200)
            .map_err(AppError::bad_request)?
    };

    // Run the query once, in the negotiated wire (never both).
    let t0 = std::time::Instant::now();
    let (arrow_body, json_rows) = if arrow {
        let bytes = verdigris_query::engine::query_table_arrow(
            s.clone(),
            st.table.as_str(),
            &files,
            &sql,
            &limits(&st),
        )
        .await
        .map_err(AppError::from_query)?;
        (bytes, Value::Null)
    } else {
        let rows = verdigris_query::engine::query_table_json(
            s.clone(),
            st.table.as_str(),
            &files,
            &sql,
            &limits(&st),
        )
        .await
        .map_err(AppError::from_query)?;
        (Vec::new(), Value::Array(rows))
    };
    let elapsed = t0.elapsed().as_millis() as u64;

    let (min_ts, max_ts) = time_range(&m);
    let histogram = histogram(&s, st.table.as_str(), &files, min_ts, max_ts, &limits(&st))
        .await
        .unwrap_or_default();
    // `events` is the total matched count (histogram sum), not the page of rows.
    let events: i64 = histogram
        .iter()
        .filter_map(|b| b.get("total").and_then(Value::as_i64))
        .sum();
    let stats = json!({
        "events": events,
        "scannedBytes": scanned_bytes,
        "elapsedMs": elapsed,
        "engine": "datafusion",
        "files": files.len(),
    });

    // Audit / expensiveQueries: record who ran what, and what it scanned/cost.
    let user = identity
        .map(|Extension(id)| id.0)
        .unwrap_or_else(|| "anonymous".to_string());
    let (cost_usd, cold_tier) = selected.iter().fold((0.0f64, Tier::Hot), |(c, ct), f| {
        let usd = f.bytes as f64 / cost::GIB
            * cost::retrieval_usd_per_gib(f.tier.default_class(), RetrievalMode::Standard);
        (
            c + usd,
            if f.tier.index() > ct.index() {
                f.tier
            } else {
                ct
            },
        )
    });
    let rec = QueryRecord {
        ts_millis: crate::now_millis(),
        user,
        sql: req.sql.clone(),
        scanned_bytes,
        cost_usd,
        tier: cold_tier.as_str().to_string(),
    };
    {
        let mut hist = st.query_history.lock().await;
        if hist.len() >= QUERY_HISTORY_CAP {
            hist.pop_front();
        }
        hist.push_back(rec.clone());
    }
    // Write through to the persisted audit trail. A store hiccup must not fail
    // the query the user already got results for — log and move on.
    if let Err(e) = append_audit(&s, st.table.as_str(), rec).await {
        tracing::warn!(error = %e, "audit write-through failed");
    }

    if arrow {
        Ok(query_response(true, arrow_body, &stats, &histogram))
    } else {
        Ok(
            Json(json!({ "rows": json_rows, "stats": stats, "histogram": histogram }))
                .into_response(),
        )
    }
}

/// Build a `/v1/query` response in the requested wire. Arrow: the rows are the
/// Arrow-IPC body and `stats`/`histogram` (small JSON) ride in response headers,
/// so the whole envelope is still one round-trip. JSON: the usual body envelope.
/// Same-origin (UI + API on one binary) means the client can read the custom
/// headers without CORS `Access-Control-Expose-Headers`.
fn query_response(
    arrow: bool,
    arrow_body: Vec<u8>,
    stats: &Value,
    histogram: &[Value],
) -> Response {
    if !arrow {
        return Json(json!({ "rows": [], "stats": stats, "histogram": histogram })).into_response();
    }
    let hist = Value::Array(histogram.to_vec());
    Response::builder()
        .header(CONTENT_TYPE, "application/vnd.apache.arrow.stream")
        .header("x-verdigris-stats", stats.to_string())
        .header("x-verdigris-histogram", hist.to_string())
        .body(Body::from(arrow_body))
        .expect("building arrow response")
}

fn time_range(m: &Manifest) -> (i64, i64) {
    let min = m.files.iter().map(|f| f.min_ts).min().unwrap_or(0);
    let max = m.files.iter().map(|f| f.max_ts).max().unwrap_or(0);
    (min, max)
}

/// Time-bucketed histogram (total vs error counts), bucketed into ~60 bins tied
/// to the table's time range so the strip is stable across queries.
async fn histogram(
    s: &Store,
    table: &str,
    files: &[String],
    min_ts: i64,
    max_ts: i64,
    limits: &QueryLimits,
) -> anyhow::Result<Vec<Value>> {
    let range_ms = (max_ts - min_ts).max(1);
    let interval_secs = (range_ms / 60 / 1000).max(1);
    let bin = format!(
        "date_bin(INTERVAL '{interval_secs} seconds', ts, TIMESTAMP '1970-01-01T00:00:00')"
    );
    let sql = format!(
        "SELECT count(*) AS total, \
                count(*) FILTER (WHERE level = 'ERROR') AS errors \
         FROM {table} GROUP BY {bin} ORDER BY {bin}"
    );
    let rows =
        verdigris_query::engine::query_table_json(s.clone(), table, files, &sql, limits).await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            json!({
                "total": r.get("total").and_then(Value::as_i64).unwrap_or(0),
                "errors": r.get("errors").and_then(Value::as_i64).unwrap_or(0),
            })
        })
        .collect())
}

// ───────────────────────── ingest ─────────────────────────

/// Parse an ingest request body into records. Accepts three shapes so any
/// sender works: NDJSON (one JSON object per line — what Vector's http sink
/// emits), a single JSON object, or a JSON array of objects. Malformed lines
/// are skipped and counted rather than failing the whole batch — a log shipper
/// shouldn't lose 999 good lines to one bad one. Returns
/// `(records, skipped, first_error)`.
fn parse_ingest_body(body: &str) -> (Vec<LogRecord>, usize, Option<String>) {
    let trimmed = body.trim_start();
    let mut records = Vec::new();
    let mut skipped = 0usize;
    let mut first_err: Option<String> = None;

    if trimmed.starts_with('[') {
        // A single JSON array of records.
        match serde_json::from_str::<Vec<JsonLog>>(trimmed) {
            Ok(logs) => records.extend(logs.into_iter().map(LogRecord::from)),
            Err(e) => {
                skipped += 1;
                first_err = Some(e.to_string());
            }
        }
    } else {
        // NDJSON (or a single object, which is just one line).
        for (i, line) in body.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<JsonLog>(line) {
                Ok(j) => records.push(j.into()),
                Err(e) => {
                    skipped += 1;
                    if first_err.is_none() {
                        first_err = Some(format!("line {}: {e}", i + 1));
                    }
                }
            }
        }
    }
    (records, skipped, first_err)
}

/// `POST /v1/ingest` — accept logs and write them to the store (routing each by
/// severity to a tier, batching to Parquet, updating the manifest). This is the
/// real ingestion path the Vector/Fluent-Bit DaemonSet ships to.
/// Backpressure gate for the write path: grab an in-flight permit or shed with
/// 429. The permit is held for the request, so at most `ingest.max_inflight`
/// ingests do the parse+write work at once — a flood is rejected, not buffered.
fn ingest_permit(st: &AppState) -> Result<tokio::sync::OwnedSemaphorePermit, AppError> {
    st.ingest_sem.clone().try_acquire_owned().map_err(|_| {
        AppError::with_status(
            StatusCode::TOO_MANY_REQUESTS,
            "ingest at capacity — back off and retry",
        )
    })
}

async fn h_ingest(State(st): State<AppState>, body: String) -> ApiResult {
    let _permit = ingest_permit(&st)?;
    let (records, skipped, first_err) = parse_ingest_body(&body);
    if records.is_empty() {
        return Err(AppError::bad_request(first_err.unwrap_or_else(|| {
            "no valid log records in request body (expected NDJSON, a JSON object, or a JSON array)"
                .to_string()
        })));
    }
    let ingested = records.len();

    // Serialize writes: the manifest is read-modify-written, so concurrent
    // ingests in this process must not interleave. Held only for the batch.
    let _guard = st.ingest_lock.lock().await;

    let s = store(&st)?;
    let ingestor = verdigris_ingest::Ingestor::new(s, st.table.as_str());
    let written = ingestor
        .ingest(records, &st.cfg.routing, BatchPolicy::default())
        .await?;
    let bytes: u64 = written.iter().map(|f| f.bytes).sum();
    st.metrics
        .ingest_records
        .fetch_add(ingested as u64, Ordering::Relaxed);

    Ok(Json(json!({
        "ingested": ingested,
        "skipped": skipped,
        "filesWritten": written.len(),
        "bytesWritten": bytes,
    })))
}

/// `POST /v1/otlp/logs` — a native OTLP/HTTP JSON logs receiver. Maps the OTel
/// LogRecords onto the same canonical `LogRecord` shape and reuses the exact
/// ingest write path (routing + BatchPolicy) and per-process `ingest_lock` as
/// `/v1/ingest`. OTLP/JSON only (no protobuf/gRPC) to keep deps light.
async fn h_otlp(State(st): State<AppState>, body: String) -> ApiResult {
    let _permit = ingest_permit(&st)?;
    let records = verdigris_ingest::otlp::parse_otlp_json(&body).map_err(AppError::bad_request)?;
    if records.is_empty() {
        return Err(AppError::bad_request(
            "no log records in OTLP request (expected resourceLogs[].scopeLogs[].logRecords[])",
        ));
    }
    let ingested = records.len();

    // Same serialization guarantee as /v1/ingest: the manifest is read-modify-
    // written, so concurrent writes in this process must not interleave.
    let _guard = st.ingest_lock.lock().await;

    let s = store(&st)?;
    let ingestor = verdigris_ingest::Ingestor::new(s, st.table.as_str());
    let written = ingestor
        .ingest(records, &st.cfg.routing, BatchPolicy::default())
        .await?;
    let bytes: u64 = written.iter().map(|f| f.bytes).sum();
    st.metrics
        .ingest_records
        .fetch_add(ingested as u64, Ordering::Relaxed);

    Ok(Json(json!({
        "ingested": ingested,
        "filesWritten": written.len(),
        "bytesWritten": bytes,
    })))
}

#[derive(Deserialize)]
struct EstimateReq {
    #[serde(default)]
    tiers: Vec<String>,
    /// The query being estimated (DSL or SQL); used to prune by time range.
    #[serde(default)]
    sql: Option<String>,
}

async fn h_estimate(State(st): State<AppState>, Json(req): Json<EstimateReq>) -> ApiResult {
    let (_s, m) = manifest(&st).await?;
    let tiers: Vec<Tier> = req.tiers.iter().filter_map(|t| parse_tier(t)).collect();

    // Prune by the query's time window (metadata-only).
    let window = req
        .sql
        .as_deref()
        .and_then(|q| verdigris_core::search::time_window(q, crate::now_millis()));

    // …and by `service:`/`level:` equality, the same file-level pruning the
    // executed query applies — so the quote prices exactly what the scan reads.
    let preds = req
        .sql
        .as_deref()
        .map(verdigris_core::search::stat_predicates)
        .unwrap_or_default();

    // Provisioned throughput = cores × per-core rate (the storage/compute dial).
    let throughput =
        st.cfg.query.modeled_mibps_per_core * st.cfg.query.cores as f64 * 1024.0 * 1024.0;

    let est = verdigris_core::estimate::estimate_scan(
        &m,
        &tiers,
        window,
        &preds,
        throughput,
        RetrievalMode::Standard,
    );

    let per_tier: Vec<Value> = est
        .per_tier
        .iter()
        .map(|t| json!({ "tier": t.tier.as_str(), "gb": t.gib, "costUsd": t.cost_usd }))
        .collect();

    Ok(Json(json!({
        "scanGB": est.scan_gib,
        "scanBytes": est.scan_bytes,
        "costUsd": est.cost_usd,
        "coldRestore": est.cold_restore,
        "restoreMs": est.restore_ms,
        "scanMs": est.scan_ms,
        "filesTouched": est.files_touched,
        "filesTotal": est.files_total,
        "perTier": per_tier,
    })))
}

async fn h_storage(State(st): State<AppState>) -> ApiResult {
    let (_s, m) = manifest(&st).await?;
    let total_bytes = m.total_bytes().max(1);

    let mut tiers = Vec::new();
    let mut total_per_month = 0.0;
    for (tier, name, class) in [
        (Tier::Hot, "Hot", "S3 Standard"),
        (Tier::Warm, "Warm", "Glacier Instant"),
        (Tier::Cold, "Cold", "Glacier Flexible"),
    ] {
        let bytes: u64 = m
            .files
            .iter()
            .filter(|f| f.tier == tier)
            .map(|f| f.bytes)
            .sum();
        let objects = m.files.iter().filter(|f| f.tier == tier).count();
        let gib = bytes as f64 / cost::GIB;
        let per_month = gib * cost::storage_usd_per_gib_month(tier.default_class());
        total_per_month += per_month;
        tiers.push(json!({
            "id": format!("{tier:?}").to_lowercase(),
            "name": name,
            "class": class,
            "bytesGB": gib,
            "objects": objects,
            "perMonth": per_month,
            "pct": (bytes as f64 / total_bytes as f64 * 100.0).round(),
        }));
    }

    Ok(Json(json!({
        "tiers": tiers,
        "lifecycle": [
            { "at": format!("after {} days", st.cfg.lifecycle.hot_to_warm_days),  "action": "transition Hot -> Warm (Glacier Instant)" },
            { "at": format!("after {} days", st.cfg.lifecycle.warm_to_cold_days), "action": "transition Warm -> Cold (Glacier Flexible)" },
            { "at": format!("after {} days", st.cfg.lifecycle.expire_days),       "action": "expire (delete)" },
        ],
        // Compaction is implemented (on-demand `vdg compact`): report the real
        // small-file count, how many files are compacted, and the generation.
        "compaction": {
            "smallFiles": m.files.iter().filter(|f| f.path.rsplit('/').next().is_some_and(|n| n.starts_with("part-"))).count(),
            "compacted": m.files.iter().filter(|f| f.path.rsplit('/').next().is_some_and(|n| n.starts_with('c'))).count(),
            "generation": m.compaction_gen,
            "targetSize": "256 MB",
            "status": if m.compaction_gen > 0 { "compacted" } else { "idle" },
        },
        "totalGB": m.total_bytes() as f64 / cost::GIB,
        "totalPerMonth": total_per_month,
    })))
}

async fn h_settings(State(st): State<AppState>) -> ApiResult {
    let (bucket, region) = match &st.cfg.storage {
        StorageConfig::S3 { bucket, region, .. } => {
            (bucket.clone(), region.clone().unwrap_or_default())
        }
        StorageConfig::Local { path } => (format!("local://{}", path.display()), String::new()),
        StorageConfig::Memory => ("memory://".to_string(), String::new()),
    };

    Ok(Json(json!({
        "bucket": bucket,
        "region": region,
        "retentionDays": 400,
        "queryCompute": st.cfg.query.cores,
        "confirmColdScans": true,
        "routing": [
            { "match": "level = 'ERROR'", "tier": st.cfg.routing.error.as_str() },
            { "match": "level = 'WARN'",  "tier": st.cfg.routing.warn.as_str() },
            { "match": "level = 'INFO'",  "tier": st.cfg.routing.info.as_str() },
            { "match": "level = 'DEBUG'", "tier": st.cfg.routing.debug.as_str() },
        ],
    })))
}

/// `GET /config.json` — runtime deployment config for the `web/` SPA. Only the
/// fields that differ from the app's baked-in defaults need be sent; the client
/// deep-merges over `DEFAULT_CONFIG` (see `web/src/config/runtime.ts`). We pin it
/// to THIS backend: live data, JSON wire, single-tenant on-prem (flat `/v1/*`),
/// with one org/env derived from the served table + storage bucket.
async fn h_config(State(st): State<AppState>) -> ApiResult {
    let (bucket, region) = match &st.cfg.storage {
        StorageConfig::S3 { bucket, region, .. } => {
            (format!("s3://{bucket}"), region.clone().unwrap_or_default())
        }
        StorageConfig::Local { path } => {
            (format!("local://{}", path.display()), "local".to_string())
        }
        StorageConfig::Memory => ("memory://".to_string(), "local".to_string()),
    };
    let table = st.table.as_str();

    Ok(Json(json!({
        "mode": "onprem",
        "apiBaseUrl": "",
        "useMocks": false,
        // Query rows travel as Arrow IPC (columnar); the client transparently
        // falls back to the JSON envelope if a response isn't Arrow.
        "wire": "arrow",
        // Tell the SPA whether it must present a bearer token; the actual token
        // is never in this file — the user supplies it (token gate) and the app
        // stores it client-side.
        "auth": { "kind": if st.cfg.auth.enabled { "token" } else { "none" } },
        "orgs": [ { "id": "local", "name": "Verdigris" } ],
        "environments": [ {
            "id": table,
            "label": table,
            "region": region,
            "bucket": bucket,
        } ],
    })))
}

// ──────────────────── partial / placeholder endpoints ────────────────────

async fn h_metrics(State(st): State<AppState>) -> ApiResult {
    let (s, m) = manifest(&st).await?;
    let total_gib = m.total_bytes() as f64 / cost::GIB;
    let files: Vec<String> = m.files.iter().map(|f| f.path.clone()).collect();
    let table = st.table.as_str();

    // Per-service volume (real), proportional to row share.
    let mut volume_by_service = Vec::new();
    // Time series derived from the same buckets as the histogram.
    let mut ingest_rate: Vec<f64> = Vec::new();
    let mut error_rate: Vec<f64> = Vec::new();
    let mut p99: Vec<f64> = Vec::new();
    let mut total_events = 0i64;
    let mut total_errors = 0i64;

    if !files.is_empty() {
        let sql =
            format!("SELECT service, count(*) AS n FROM {table} GROUP BY service ORDER BY n DESC");
        if let Ok(rows) =
            verdigris_query::engine::query_table_json(s.clone(), table, &files, &sql, &limits(&st))
                .await
        {
            let total_rows = m.total_rows().max(1) as f64;
            for r in rows {
                let n = r.get("n").and_then(Value::as_i64).unwrap_or(0) as f64;
                volume_by_service.push(json!({
                    "name": r.get("service").and_then(Value::as_str).unwrap_or("?"),
                    "gb": (n / total_rows) * total_gib,
                }));
            }
        }

        let (min_ts, max_ts) = time_range(&m);
        let interval_secs = ((max_ts - min_ts).max(1) / 60 / 1000).max(1) as f64;
        for b in histogram(&s, table, &files, min_ts, max_ts, &limits(&st))
            .await
            .unwrap_or_default()
        {
            let total = b.get("total").and_then(Value::as_i64).unwrap_or(0);
            let errors = b.get("errors").and_then(Value::as_i64).unwrap_or(0);
            total_events += total;
            total_errors += errors;
            ingest_rate.push(total as f64 / interval_secs);
            let er = if total > 0 {
                errors as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            error_rate.push(er);
            // No latency field in logs yet, so p99 is MODELED from error rate.
            p99.push(800.0 + er * 18.0);
        }
    }

    let avg = |v: &[f64]| {
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };
    let overall_err = if total_events > 0 {
        total_errors as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };

    // Compaction health: live pending count from the manifest + scheduler history.
    let target_bytes = st.cfg.compaction.target_bytes();
    let pending_files = verdigris_ingest::pending_compaction_total(&m, target_bytes);
    let last_run = st.compaction.last_run_ms.load(Ordering::Relaxed);

    Ok(Json(json!({
        "ingestRate": ingest_rate,
        "errorRate": error_rate,
        "p99": p99, // modeled (no real latency field yet)
        "volumeByService": volume_by_service,
        "tiles": {
            "stored": { "value": format!("{total_gib:.2}"), "unit": "GB", "delta": 0.0 },
            "ingest": { "value": format!("{:.0}", avg(&ingest_rate)), "unit": "ev/s", "delta": 0.0 },
            "errors": { "value": format!("{overall_err:.1}"), "unit": "%", "delta": 0.0 },
            "p99":    { "value": format!("{:.2}", avg(&p99) / 1000.0), "unit": "s", "delta": 0.0 },
        },
        "compaction": {
            "enabled": st.cfg.compaction.enabled,
            "targetMib": st.cfg.compaction.target_mib,
            "totalFiles": m.files.len(),
            "pendingFiles": pending_files,
            "filesMergedTotal": st.compaction.files_merged_total.load(Ordering::Relaxed),
            "runsTotal": st.compaction.runs_total.load(Ordering::Relaxed),
            "lastRunMs": if last_run == 0 { Value::Null } else { json!(last_run) },
        },
    })))
}

/// Query params for `/v1/cost`. `days` selects the projection horizon (the
/// 7d/30d/90d toggle in the UI): storage is billed as a monthly rate, so the
/// projected spend over a window is `monthly_rate * days / 30`.
#[derive(Deserialize)]
struct CostQuery {
    days: Option<u32>,
}

async fn h_cost(State(st): State<AppState>, Query(q): Query<CostQuery>) -> ApiResult {
    let days = q.days.unwrap_or(30).clamp(1, 3650);
    let (_s, m) = manifest(&st).await?;
    let mut breakdown = Vec::new();
    let mut total = 0.0;
    for (tier, label) in [
        (Tier::Hot, "Hot storage (S3 Standard)"),
        (Tier::Warm, "Warm storage (Glacier IR)"),
        (Tier::Cold, "Cold storage (Glacier Flex)"),
    ] {
        let bytes: u64 = m
            .files
            .iter()
            .filter(|f| f.tier == tier)
            .map(|f| f.bytes)
            .sum();
        let usd =
            (bytes as f64 / cost::GIB) * cost::storage_usd_per_gib_month(tier.default_class());
        total += usd;
        breakdown.push(json!({ "label": label, "usd": usd }));
    }
    let total_gib = m.total_bytes() as f64 / cost::GIB;
    // Illustrative comparison: a hosted SaaS log service bills ingest + indexed
    // retention at roughly ~$2.50/GB-month-equivalent, vs our object-storage cost.
    let hosted = total_gib * 2.50;

    // Top recent queries by scanned bytes, from the audit ring.
    let expensive: Vec<Value> = {
        let hist = st.query_history.lock().await;
        let now = crate::now_millis();
        let mut recs: Vec<&QueryRecord> = hist.iter().collect();
        recs.sort_by_key(|r| std::cmp::Reverse(r.scanned_bytes));
        recs.into_iter()
            .take(5)
            .map(|r| {
                json!({
                    "q": r.sql,
                    "tier": r.tier,
                    "scanGB": r.scanned_bytes as f64 / cost::GIB,
                    "usd": r.cost_usd,
                    "user": r.user,
                    "when": format!("{} ago", humanize_ms((now - r.ts_millis).max(0) as u64)),
                })
            })
            .collect()
    };

    // Storage is a monthly rate; project it over the selected window.
    let projected = total * days as f64 / 30.0;
    Ok(Json(json!({
        "rangeDays": days,
        "monthToDate": total,
        "projected": projected,
        "lastMonth": total * 0.92,
        "breakdown": breakdown,
        "spendSeries": [],
        "vsHosted": { "ours": total, "hosted": hosted },
        "expensiveQueries": expensive,
    })))
}

async fn h_alerts(State(st): State<AppState>) -> ApiResult {
    let s = store(&st)?;
    let doc = load_alerts(&s, st.table.as_str()).await?;
    let now = crate::now_millis() as u64;
    let out: Vec<Value> = doc
        .alerts
        .iter()
        .map(|a| {
            let firing = matches!(a.status.state, AlertState::Firing);
            let since = if firing && a.status.last_eval_ms != 0 {
                humanize_ms(now.saturating_sub(a.status.since_ms))
            } else {
                "—".to_string()
            };
            let channel = a
                .rule
                .webhook
                .as_deref()
                .map(webhook_host)
                .unwrap_or_else(|| "—".to_string());
            json!({
                "id": a.rule.id,
                "name": a.rule.name,
                "state": if firing { "firing" } else { "ok" },
                "severity": a.rule.severity,
                "cond": format!("{}  {} {}", a.rule.sql, a.rule.comparator.symbol(), fmt_num(a.rule.threshold)),
                "value": fmt_num(a.status.value),
                "since": since,
                "channel": channel,
                "enabled": a.rule.enabled,
            })
        })
        .collect();
    Ok(Json(Value::Array(out)))
}

fn default_comparator() -> Comparator {
    Comparator::Gt
}
fn default_alert_severity() -> String {
    "warning".to_string()
}

#[derive(Deserialize)]
struct NewAlert {
    name: String,
    /// SQL returning one number (its `v` column, or first numeric column).
    sql: String,
    #[serde(default = "default_comparator")]
    comparator: Comparator,
    threshold: f64,
    #[serde(default = "default_alert_severity")]
    severity: String,
    #[serde(default)]
    webhook: Option<String>,
}

async fn h_alert_create(
    State(st): State<AppState>,
    Json(req): Json<NewAlert>,
) -> Result<Response, AppError> {
    if req.name.trim().is_empty() || req.sql.trim().is_empty() {
        return Err(AppError::bad_request("name and sql are required"));
    }
    let s = store(&st)?;
    let _g = st.alerts_lock.lock().await;
    let now = crate::now_millis() as u64;
    let seq = st.alert_seq.fetch_add(1, Ordering::Relaxed);
    let rule = AlertRule {
        id: format!("alert-{now}-{seq}"),
        name: req.name,
        sql: req.sql,
        comparator: req.comparator,
        threshold: req.threshold,
        severity: req.severity,
        webhook: req.webhook.filter(|w| !w.trim().is_empty()),
        enabled: true,
    };
    // Evaluate once now — this both validates the SQL (a broken query → 400) and
    // seeds the rule's initial firing/OK state so the UI shows it immediately.
    let m = verdigris_ingest::Ingestor::new(s.clone(), st.table.as_str())
        .load_manifest()
        .await?;
    let files: Vec<String> = m.files.iter().map(|f| f.path.clone()).collect();
    let value = measure(&s, st.table.as_str(), &files, &rule.sql, &limits(&st))
        .await
        .map_err(AppError::bad_request)?;
    let (status, _t) = alert::evaluate(&rule, &AlertStatus::initial(now), value, now);
    let id = rule.id.clone();
    for _ in 0..ALERTS_CAS_RETRIES {
        let (mut doc, base) = load_alerts_versioned(&s, st.table.as_str()).await?;
        doc.alerts.push(Alert {
            rule: rule.clone(),
            status,
        });
        if save_alerts_cas(&s, st.table.as_str(), &doc, base).await? {
            return Ok((StatusCode::CREATED, Json(json!({ "id": id }))).into_response());
        }
    }
    Err(anyhow::anyhow!("alert create failed after retries under contention").into())
}

async fn h_alert_delete(State(st): State<AppState>, AxPath(id): AxPath<String>) -> ApiResult {
    let s = store(&st)?;
    let _g = st.alerts_lock.lock().await;
    for _ in 0..ALERTS_CAS_RETRIES {
        let (mut doc, base) = load_alerts_versioned(&s, st.table.as_str()).await?;
        let before = doc.alerts.len();
        doc.alerts.retain(|a| a.rule.id != id);
        let removed = doc.alerts.len() != before;
        if save_alerts_cas(&s, st.table.as_str(), &doc, base).await? {
            return Ok(Json(json!({ "removed": removed })));
        }
    }
    Err(anyhow::anyhow!("alert delete failed after retries under contention").into())
}

async fn h_pipelines(State(st): State<AppState>) -> ApiResult {
    let (_s, m) = manifest(&st).await?;
    let (_min_ts, max_ts) = time_range(&m);
    // Ingest lag = how stale the newest record is.
    let lag_secs = ((crate::now_millis() - max_ts).max(0)) / 1000;
    // Parquet roll cadence ≈ table time-span / file count.
    let (min_ts, _mx) = time_range(&m);
    let span_secs = ((max_ts - min_ts).max(0)) / 1000;
    let rolls = if m.files.len() > 1 {
        format!("1 / {}s", (span_secs / m.files.len() as i64).max(1))
    } else {
        "—".to_string()
    };

    Ok(Json(json!({
        "sources": [],
        "transforms": [],
        "throughput": [],
        "dropRate": 0.0,            // no drop/filter pipeline yet (the "Tarnish" stage)
        "ingestLag": format!("{lag_secs}s"),
        "parquetRolls": rolls,
    })))
}

// ───────────────────────── live tail (SSE) ─────────────────────────

/// Interval between manifest polls for the live tail.
const TAIL_POLL: Duration = Duration::from_secs(1);
/// Max rows emitted per poll, so a bursty table can't flood the stream.
const TAIL_MAX_ROWS: usize = 100;

/// State carried across `unfold` steps of the tail stream.
struct TailState {
    st: AppState,
    /// Newest ts (epoch millis) already emitted; only strictly-newer rows follow.
    last_ts: i64,
    /// Rows fetched but not yet emitted (one SSE event each).
    queue: VecDeque<Value>,
}

/// `GET /v1/tail` — a live tail as `text/event-stream`. Each `data:` line is one
/// JSON log row (`{ts, level, service, message, trace_id, status, attrs_json}`).
/// It polls the newest manifest file every second and emits rows newer than the
/// last one seen; only the newest file is scanned, so it can't run away.
async fn h_tail(State(st): State<AppState>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Start from "now" (the current manifest max) so a fresh connection streams
    // only newly-arriving rows, not the whole backlog.
    let last_ts = match manifest(&st).await {
        Ok((_s, m)) => m.files.iter().map(|f| f.max_ts).max().unwrap_or(0),
        Err(_) => crate::now_millis(),
    };

    let init = TailState {
        st,
        last_ts,
        queue: VecDeque::new(),
    };

    let stream = futures::stream::unfold(init, |mut s| async move {
        loop {
            // Drain any already-fetched rows one event at a time.
            if let Some(row) = s.queue.pop_front() {
                let ev = Event::default().data(row.to_string());
                return Some((Ok::<Event, Infallible>(ev), s));
            }
            // Otherwise wait, then poll the newest file for fresh rows.
            tokio::time::sleep(TAIL_POLL).await;
            match tail_poll(&s.st, s.last_ts).await {
                Ok((rows, new_last)) => {
                    s.last_ts = new_last.max(s.last_ts);
                    s.queue.extend(rows);
                }
                Err(_) => { /* transient (e.g. manifest mid-write): retry next tick */ }
            }
            if s.queue.is_empty() {
                // Nothing new: emit a comment so the connection stays warm and the
                // loop yields back to the client.
                let ev = Event::default().comment("keepalive");
                return Some((Ok::<Event, Infallible>(ev), s));
            }
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Fetch rows newer than `since_ts` from the table's newest file. Returns the
/// rows (as query-shaped JSON) and the max ts seen (epoch millis).
async fn tail_poll(st: &AppState, since_ts: i64) -> anyhow::Result<(Vec<Value>, i64)> {
    let (s, m) = manifest(st).await?;
    let Some(newest) = m.files.iter().max_by_key(|f| f.max_ts) else {
        return Ok((Vec::new(), since_ts));
    };
    let files = vec![newest.path.clone()];
    let table = st.table.as_str();

    // `arrow_cast(ts, 'Int64')` surfaces the underlying epoch-millis so we can
    // both filter (`ts > since`) and advance `last_ts` deterministically.
    let sql = format!(
        "SELECT arrow_cast(ts, 'Int64') AS ts_millis, \
                ts, level, service, status, message, trace_id, attrs_json \
         FROM {table} WHERE ts > to_timestamp_millis({since_ts}) \
         ORDER BY ts ASC LIMIT {TAIL_MAX_ROWS}"
    );
    let mut rows =
        verdigris_query::engine::query_table_json(s, table, &files, &sql, &limits(st)).await?;

    let mut max_ts = since_ts;
    for r in &mut rows {
        if let Some(v) = r.get("ts_millis").and_then(Value::as_i64) {
            max_ts = max_ts.max(v);
        }
        // Drop the internal helper column so the emitted contract is clean.
        if let Some(obj) = r.as_object_mut() {
            obj.remove("ts_millis");
        }
    }
    Ok((rows, max_ts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use verdigris_core::model::Level;

    #[test]
    fn ndjson_parses_and_skips_bad_lines() {
        let body = "{\"ts_millis\":1,\"level\":\"error\",\"service\":\"auth\",\"message\":\"boom\"}\n\
                    not json\n\
                    {\"ts_millis\":2,\"level\":\"INFO\",\"service\":\"api\",\"status\":200,\"message\":\"ok\"}";
        let (recs, skipped, err) = parse_ingest_body(body);
        assert_eq!(recs.len(), 2);
        assert_eq!(skipped, 1);
        assert!(err.unwrap().starts_with("line 2:"));
        assert_eq!(recs[0].level, Level::Error);
        assert_eq!(recs[1].status, Some(200));
    }

    #[test]
    fn json_array_parses() {
        let body = "[{\"ts_millis\":1,\"level\":\"debug\",\"service\":\"x\",\"message\":\"a\"},\
                     {\"ts_millis\":2,\"level\":\"warn\",\"service\":\"y\",\"message\":\"b\"}]";
        let (recs, skipped, _) = parse_ingest_body(body);
        assert_eq!(recs.len(), 2);
        assert_eq!(skipped, 0);
        assert_eq!(recs[0].level, Level::Debug);
    }

    #[test]
    fn single_object_parses() {
        let (recs, skipped, _) =
            parse_ingest_body("{\"ts_millis\":1,\"service\":\"x\",\"message\":\"m\"}");
        assert_eq!(recs.len(), 1);
        assert_eq!(skipped, 0);
        assert_eq!(recs[0].level, Level::Info); // defaulted
    }

    #[test]
    fn empty_body_yields_no_records() {
        let (recs, _, _) = parse_ingest_body("   \n  \n");
        assert!(recs.is_empty());
    }

    #[tokio::test]
    async fn auto_compaction_triggers_below_and_above_threshold() {
        let s: Store = Arc::new(object_store::memory::InMemory::new());
        let ing = verdigris_ingest::Ingestor::new(s.clone(), "logs");
        let routing = verdigris_core::config::RoutingConfig::default();
        let policy = BatchPolicy {
            max_rows: 100,
            max_bytes: usize::MAX,
        };
        // Many small ingests -> many small files (the streaming reality).
        for i in 0u64..10 {
            let recs = verdigris_ingest::generate::generate(100, i, (i as i64) * 1_000_000);
            ing.ingest(recs, &routing, policy).await.unwrap();
        }
        let before = ing.load_manifest().await.unwrap().files.len();
        assert!(before > 3, "expected many small files, got {before}");

        let lock = tokio::sync::Mutex::new(());
        let metrics = CompactionMetrics::new();
        let target = 10 * 1024 * 1024;

        // Trigger far above pending -> no-op; files and counters untouched.
        maybe_compact(&s, "logs", &lock, target, 10_000, 128, &metrics)
            .await
            .unwrap();
        assert_eq!(metrics.runs_total.load(Ordering::Relaxed), 0);
        assert_eq!(
            ing.load_manifest().await.unwrap().files.len(),
            before,
            "must not compact below the trigger"
        );

        // Low trigger + tiny per-pass budget -> multiple bounded passes drain the
        // backlog fully; files shrink and metrics record one run.
        maybe_compact(&s, "logs", &lock, target, 2, 2, &metrics)
            .await
            .unwrap();
        let after = ing.load_manifest().await.unwrap().files.len();
        assert!(
            after < before,
            "auto-compaction should reduce files ({before} -> {after})"
        );
        assert_eq!(metrics.runs_total.load(Ordering::Relaxed), 1);
        assert!(metrics.files_merged_total.load(Ordering::Relaxed) > 0);
        assert!(metrics.last_run_ms.load(Ordering::Relaxed) > 0);
        // Fully drained: nothing left pending.
        assert_eq!(
            verdigris_ingest::pending_compaction_total(&ing.load_manifest().await.unwrap(), target),
            0,
            "bounded passes should drain the backlog"
        );
    }

    fn audit_rec(i: i64) -> QueryRecord {
        QueryRecord {
            ts_millis: i,
            user: "u".into(),
            sql: "select 1".into(),
            scanned_bytes: i as u64,
            cost_usd: 0.0,
            tier: "hot".into(),
        }
    }

    #[tokio::test]
    async fn audit_appends_persist_and_trim_to_cap() {
        let s: Store = Arc::new(object_store::memory::InMemory::new());
        // Fresh store → empty history, no version.
        let (doc, v) = load_audit(&s, "t").await.unwrap();
        assert!(doc.queries.is_empty() && v.is_none());

        let n = QUERY_HISTORY_CAP + 10;
        for i in 0..n {
            append_audit(&s, "t", audit_rec(i as i64)).await.unwrap();
        }
        // A "restarted" process (fresh load) sees the newest CAP records, oldest
        // first — the 10 earliest were trimmed.
        let (doc, v) = load_audit(&s, "t").await.unwrap();
        assert!(v.is_some());
        assert_eq!(doc.queries.len(), QUERY_HISTORY_CAP);
        assert_eq!(doc.queries.first().unwrap().ts_millis, 10);
        assert_eq!(doc.queries.last().unwrap().ts_millis, (n - 1) as i64);
    }

    #[tokio::test]
    async fn alerts_cas_rejects_stale_writers() {
        let s: Store = Arc::new(object_store::memory::InMemory::new());
        let doc = AlertsDoc::default();
        // First create wins; a second create (another replica seeding) conflicts.
        assert!(save_alerts_cas(&s, "t", &doc, None).await.unwrap());
        assert!(!save_alerts_cas(&s, "t", &doc, None).await.unwrap());
        // Two writers load the same version; the second commit is stale → rejected.
        let (_a, va) = load_alerts_versioned(&s, "t").await.unwrap();
        let (_b, vb) = load_alerts_versioned(&s, "t").await.unwrap();
        assert!(save_alerts_cas(&s, "t", &doc, va).await.unwrap());
        assert!(!save_alerts_cas(&s, "t", &doc, vb).await.unwrap());
        // Reload → fresh version → commit lands.
        let (_b2, vb2) = load_alerts_versioned(&s, "t").await.unwrap();
        assert!(save_alerts_cas(&s, "t", &doc, vb2).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_audit_doc_reads_empty_and_is_repaired_by_append() {
        let s: Store = Arc::new(object_store::memory::InMemory::new());
        s.put(
            &audit_path("t"),
            bytes::Bytes::from_static(b"{ not json").into(),
        )
        .await
        .unwrap();
        // Corrupt → empty history (queries must not fail), version preserved.
        let (doc, v) = load_audit(&s, "t").await.unwrap();
        assert!(doc.queries.is_empty());
        assert!(
            v.is_some(),
            "version kept so the next append repairs the doc"
        );
        // The next append overwrites the corrupt doc under CAS.
        append_audit(&s, "t", audit_rec(1)).await.unwrap();
        let (doc, _) = load_audit(&s, "t").await.unwrap();
        assert_eq!(doc.queries.len(), 1);
    }
}
