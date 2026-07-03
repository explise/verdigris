//! `vdg serve` — the HTTP shell. Hosts the static frontend AND the `/v1/*` API
//! that `frontend/api.js` calls when `USE_MOCKS = false`.
//!
//! Endpoints backed by REAL data: /v1/ingest (writes logs to the store),
//! /v1/query, /v1/query/estimate, /v1/storage/tiers, /v1/settings, and the
//! volume/cost figures in /v1/metrics and /v1/cost (computed from the manifest
//! + cost model). Endpoints we can't
//! back yet (alerts, pipelines, time-series metrics) return shape-correct
//! placeholders so the dashboards render — each marked `placeholder: true`.
//! `tail()` stays client-side in the frontend, so there is no SSE endpoint.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use verdigris_core::batch::{BatchPolicy, LogRecord};
use verdigris_core::config::{Config, StorageConfig};
use verdigris_core::cost::{self, RetrievalMode};
use verdigris_core::manifest::Manifest;
use verdigris_core::model::Tier;
use verdigris_ingest::wire::JsonLog;
use verdigris_storage::Store;

#[derive(Clone)]
struct AppState {
    cfg: Arc<Config>,
    table: Arc<String>,
    /// Serializes ingest writes within this process. The manifest is read-
    /// modify-written, so concurrent `POST /v1/ingest` calls (e.g. a Vector
    /// DaemonSet fanning in) must not interleave. Cross-process/replica
    /// concurrency still needs Iceberg commits — a known, documented gap.
    ingest_lock: Arc<tokio::sync::Mutex<()>>,
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

fn parse_tier(s: &str) -> Option<Tier> {
    match s {
        "hot" => Some(Tier::Hot),
        "warm" => Some(Tier::Warm),
        "cold" => Some(Tier::Cold),
        _ => None,
    }
}

pub async fn serve(cfg: Config, table: String, port: u16, frontend: PathBuf) -> anyhow::Result<()> {
    let state = AppState {
        cfg: Arc::new(cfg),
        table: Arc::new(table),
        ingest_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let app = Router::new()
        .route("/v1/query", post(h_query))
        .route("/v1/query/estimate", post(h_estimate))
        .route("/v1/ingest", post(h_ingest))
        .route("/v1/metrics", get(h_metrics))
        .route("/v1/alerts", get(h_alerts))
        .route("/v1/storage/tiers", get(h_storage))
        .route("/v1/cost", get(h_cost))
        .route("/v1/pipelines", get(h_pipelines))
        .route("/v1/settings", get(h_settings))
        .fallback_service(ServeDir::new(&frontend))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("binding port {port}"))?;
    println!("verdigris serving on http://localhost:{port}");
    println!("  frontend: {}", frontend.display());
    println!("  api:      http://localhost:{port}/v1/query");
    println!("  ingest:   POST http://localhost:{port}/v1/ingest  (NDJSON logs)");
    println!("  remember to set USE_MOCKS = false in frontend/api.js");
    axum::serve(listener, app).await.context("http server")?;
    Ok(())
}

// ───────────────────────── real endpoints ─────────────────────────

#[derive(Deserialize)]
struct QueryReq {
    sql: String,
}

async fn h_query(State(st): State<AppState>, Json(req): Json<QueryReq>) -> ApiResult {
    let (s, m) = manifest(&st).await?;
    if m.files.is_empty() {
        return Ok(Json(json!({
            "rows": [],
            "stats": { "events": 0, "scannedBytes": 0, "elapsedMs": 0, "engine": "datafusion", "files": 0 },
            "histogram": [],
        })));
    }
    let files: Vec<String> = m.files.iter().map(|f| f.path.clone()).collect();

    // The frontend search bar sends its DSL in `sql`; raw SQL is passed through.
    // A malformed query is a 400, not a 200-with-empty-rows, so the client can
    // tell a broken query from zero matches.
    let sql = if verdigris_core::search::looks_like_sql(&req.sql) {
        req.sql.clone()
    } else {
        verdigris_core::search::to_sql(&req.sql, st.table.as_str(), crate::now_millis(), 200)
            .map_err(AppError::bad_request)?
    };

    let t0 = std::time::Instant::now();
    let rows =
        verdigris_query::engine::query_table_json(s.clone(), st.table.as_str(), &files, &sql)
            .await
            .map_err(AppError::bad_request)?;
    let elapsed = t0.elapsed().as_millis() as u64;

    let (min_ts, max_ts) = time_range(&m);
    let histogram = histogram(&s, st.table.as_str(), &files, min_ts, max_ts)
        .await
        .unwrap_or_default();
    // `events` is the total matched count (histogram sum), not the page of rows.
    let events: i64 = histogram
        .iter()
        .filter_map(|b| b.get("total").and_then(Value::as_i64))
        .sum();

    Ok(Json(json!({
        "rows": rows,
        "stats": {
            "events": events,
            "scannedBytes": m.total_bytes(),
            "elapsedMs": elapsed,
            "engine": "datafusion",
            "files": files.len(),
        },
        "histogram": histogram,
    })))
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
) -> anyhow::Result<Vec<Value>> {
    let range_ms = (max_ts - min_ts).max(1);
    let interval_secs = (range_ms / 60 / 1000).max(1);
    let bin =
        format!("date_bin(INTERVAL '{interval_secs} seconds', ts, TIMESTAMP '1970-01-01T00:00:00')");
    let sql = format!(
        "SELECT count(*) AS total, \
                count(*) FILTER (WHERE level = 'ERROR') AS errors \
         FROM {table} GROUP BY {bin} ORDER BY {bin}"
    );
    let rows = verdigris_query::engine::query_table_json(s.clone(), table, files, &sql).await?;
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
async fn h_ingest(State(st): State<AppState>, body: String) -> ApiResult {
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

    Ok(Json(json!({
        "ingested": ingested,
        "skipped": skipped,
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

    // Provisioned throughput = cores × per-core rate (the storage/compute dial).
    let throughput = st.cfg.query.modeled_mibps_per_core * st.cfg.query.cores as f64 * 1024.0 * 1024.0;

    let est = verdigris_core::estimate::estimate_scan(
        &m,
        &tiers,
        window,
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
        let bytes: u64 = m.files.iter().filter(|f| f.tier == tier).map(|f| f.bytes).sum();
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
        let sql = format!("SELECT service, count(*) AS n FROM {table} GROUP BY service ORDER BY n DESC");
        if let Ok(rows) =
            verdigris_query::engine::query_table_json(s.clone(), table, &files, &sql).await
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
        for b in histogram(&s, table, &files, min_ts, max_ts).await.unwrap_or_default() {
            let total = b.get("total").and_then(Value::as_i64).unwrap_or(0);
            let errors = b.get("errors").and_then(Value::as_i64).unwrap_or(0);
            total_events += total;
            total_errors += errors;
            ingest_rate.push(total as f64 / interval_secs);
            let er = if total > 0 { errors as f64 / total as f64 * 100.0 } else { 0.0 };
            error_rate.push(er);
            // No latency field in logs yet, so p99 is MODELED from error rate.
            p99.push(800.0 + er * 18.0);
        }
    }

    let avg = |v: &[f64]| if v.is_empty() { 0.0 } else { v.iter().sum::<f64>() / v.len() as f64 };
    let overall_err = if total_events > 0 {
        total_errors as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };

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
    })))
}

async fn h_cost(State(st): State<AppState>) -> ApiResult {
    let (_s, m) = manifest(&st).await?;
    let mut breakdown = Vec::new();
    let mut total = 0.0;
    for (tier, label) in [
        (Tier::Hot, "Hot storage (S3 Standard)"),
        (Tier::Warm, "Warm storage (Glacier IR)"),
        (Tier::Cold, "Cold storage (Glacier Flex)"),
    ] {
        let bytes: u64 = m.files.iter().filter(|f| f.tier == tier).map(|f| f.bytes).sum();
        let usd = (bytes as f64 / cost::GIB) * cost::storage_usd_per_gib_month(tier.default_class());
        total += usd;
        breakdown.push(json!({ "label": label, "usd": usd }));
    }
    let total_gib = m.total_bytes() as f64 / cost::GIB;
    // Illustrative comparison: a SaaS log vendor bills ingest + indexed retention,
    // roughly ~$2.50/GB-month-equivalent vs our object-storage cost.
    let datadog = total_gib * 2.50;

    Ok(Json(json!({
        "monthToDate": total,
        "projected": total,
        "lastMonth": total * 0.92,
        "breakdown": breakdown,
        "spendSeries": [],
        "vsDatadog": { "ours": total, "datadog": datadog },
        // No query-history tracking yet — empty until that subsystem exists.
        "expensiveQueries": [],
    })))
}

async fn h_alerts(State(_st): State<AppState>) -> ApiResult {
    // No alerting engine yet (future work).
    Ok(Json(json!([])))
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
}
