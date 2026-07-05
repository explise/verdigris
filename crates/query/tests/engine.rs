//! End-to-end tests for the DataFusion engine path (`--features datafusion`).
//!
//! Real Parquet is written through the production ingest path (routing, batching,
//! bloom filters, manifest commit) into an in-memory object store, then queried
//! in place through the same `engine` functions `vdg serve` calls. This is the
//! suite that makes `cargo test -p verdigris-query --features datafusion` mean
//! something beyond the modeled executor.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::reader::StreamReader;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use verdigris_core::batch::{BatchPolicy, LogRecord};
use verdigris_core::config::RoutingConfig;
use verdigris_core::estimate::select_files;
use verdigris_core::manifest::Manifest;
use verdigris_core::model::{Level, Tier};
use verdigris_ingest::Ingestor;
use verdigris_query::engine;

const BASE_TS: i64 = 1_700_000_000_000;
const TRACE: &str = "traceme-123";

fn rec(i: i64, level: Level, service: &str, trace_id: Option<&str>) -> LogRecord {
    LogRecord {
        ts_millis: BASE_TS + i * 1_000,
        level,
        service: service.into(),
        status: Some(200),
        message: format!("{service} event {i}"),
        trace_id: trace_id.map(Into::into),
        attrs: BTreeMap::new(),
    }
}

/// Ingest a fixed corpus through the production write path: 20 ERROR/auth rows
/// (routed hot, one carrying a known trace id), 20 INFO/api rows (warm), and
/// 20 DEBUG/worker rows (cold) — one Parquet file per tier.
async fn seeded_table() -> (Arc<dyn ObjectStore>, Manifest) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(store.clone(), "logs");
    let mut records = Vec::new();
    for i in 0..20 {
        records.push(rec(i, Level::Error, "auth", (i == 7).then_some(TRACE)));
        records.push(rec(i, Level::Info, "api", None));
        records.push(rec(i, Level::Debug, "worker", None));
    }
    let written = ingestor
        .ingest(records, &RoutingConfig::default(), BatchPolicy::default())
        .await
        .expect("ingest");
    assert_eq!(written.len(), 3, "one file per tier");
    let manifest = ingestor.load_manifest().await.expect("manifest");
    (store, manifest)
}

fn all_paths(m: &Manifest) -> Vec<String> {
    m.files.iter().map(|f| f.path.clone()).collect()
}

#[tokio::test]
async fn sql_filters_and_aggregates_in_place() {
    let (store, manifest) = seeded_table().await;
    let files = all_paths(&manifest);

    let rows = engine::query_table_json(
        store.clone(),
        "logs",
        &files,
        "SELECT service, count(*) AS c FROM logs GROUP BY service ORDER BY service",
    )
    .await
    .expect("group-by query");
    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| {
            (
                r["service"].as_str().unwrap().to_string(),
                r["c"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("api".into(), 20),
            ("auth".into(), 20),
            ("worker".into(), 20)
        ]
    );

    let rows = engine::query_table_json(
        store,
        "logs",
        &files,
        "SELECT count(*) AS c FROM logs WHERE level = 'ERROR' AND status = 200",
    )
    .await
    .expect("filtered count");
    assert_eq!(rows[0]["c"].as_i64(), Some(20));
}

// The "find this trace" path: an equality lookup on a bloom-filtered column
// (`writer_props` puts bloom filters on trace_id; the session enables
// bloom_filter_on_read). Correctness bar: the exact row comes back — a bloom
// false-negative would silently drop it.
#[tokio::test]
async fn trace_id_equality_lookup_returns_the_exact_row() {
    let (store, manifest) = seeded_table().await;

    let rows = engine::query_table_json(
        store,
        "logs",
        &all_paths(&manifest),
        &format!("SELECT service, message, trace_id FROM logs WHERE trace_id = '{TRACE}'"),
    )
    .await
    .expect("trace lookup");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["service"].as_str(), Some("auth"));
    assert_eq!(rows[0]["message"].as_str(), Some("auth event 7"));
    assert_eq!(rows[0]["trace_id"].as_str(), Some(TRACE));
}

// The M4.1 guarantee at the execution layer: the engine reads exactly the files
// it is given, so registering `select_files(tiers=[Hot])` — the same selector
// the cost estimate prices — means a hot-only query cannot touch warm/cold data.
#[tokio::test]
async fn engine_reads_only_the_registered_file_set() {
    let (store, manifest) = seeded_table().await;
    let hot: Vec<String> = select_files(&manifest, &[Tier::Hot], None, &[])
        .into_iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(hot.len(), 1, "one hot file in the corpus");

    let rows = engine::query_table_json(
        store,
        "logs",
        &hot,
        "SELECT count(*) AS c, count(DISTINCT service) AS s FROM logs",
    )
    .await
    .expect("hot-only query");
    // Only the 20 hot (ERROR/auth) rows are visible; warm/cold never enter the plan.
    assert_eq!(rows[0]["c"].as_i64(), Some(20));
    assert_eq!(rows[0]["s"].as_i64(), Some(1));
}

// Free-text ("grep") end to end: the DSL term prunes files via the manifest's
// trigram stats, and the pruned file set returns byte-identical results to
// scanning everything — pruning is invisible except in bytes scanned.
#[tokio::test]
async fn free_text_pruning_never_changes_results() {
    let (store, manifest) = seeded_table().await;
    let all = all_paths(&manifest);

    // "auth" only ever appears in the hot (ERROR/auth) file's messages.
    let dsl = "auth";
    let sql = verdigris_core::search::to_sql(dsl, "logs", BASE_TS + 3_600_000, 1000).unwrap();
    assert!(sql.contains("message ILIKE '%auth%'"), "free text compiles to ILIKE: {sql}");
    let preds = verdigris_core::search::stat_predicates(dsl);
    assert_eq!(preds, vec![verdigris_core::manifest::Predicate::message_contains("auth")]);

    let pruned: Vec<String> = select_files(&manifest, &[Tier::Hot, Tier::Warm, Tier::Cold], None, &preds)
        .into_iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(pruned.len(), 1, "trigram stats prune to the auth file only");

    let from_pruned = engine::query_table_json(store.clone(), "logs", &pruned, &sql)
        .await
        .expect("pruned query");
    let from_all = engine::query_table_json(store, "logs", &all, &sql)
        .await
        .expect("full query");
    assert_eq!(from_pruned.len(), 20, "all auth rows found");
    assert_eq!(from_pruned, from_all, "pruning must not change results");
}

// The Arrow wire must stay decodable by plain IPC readers: no Utf8View/BinaryView
// columns (DataFusion's Parquet reader yields them; `deview_batch` casts them
// down), same rows as the JSON path, and an empty result is an empty buffer.
#[tokio::test]
async fn arrow_wire_is_view_free_and_matches_the_json_path() {
    let (store, manifest) = seeded_table().await;
    let files = all_paths(&manifest);
    let sql = "SELECT ts, level, service, message FROM logs ORDER BY ts, service";

    let buf = engine::query_table_arrow(store.clone(), "logs", &files, sql)
        .await
        .expect("arrow query");
    let reader = StreamReader::try_new(Cursor::new(&buf), None).expect("ipc stream");
    for field in reader.schema().fields() {
        assert!(
            !matches!(
                field.data_type(),
                DataType::Utf8View | DataType::BinaryView
            ),
            "view type leaked onto the wire: {field:?}"
        );
    }
    let arrow_rows: usize = reader.map(|b| b.expect("ipc batch").num_rows()).sum();

    let json_rows = engine::query_table_json(store.clone(), "logs", &files, sql)
        .await
        .expect("json query");
    assert_eq!(arrow_rows, 60);
    assert_eq!(arrow_rows, json_rows.len());

    let empty = engine::query_table_arrow(
        store,
        "logs",
        &files,
        "SELECT * FROM logs WHERE service = 'no-such-service'",
    )
    .await
    .expect("empty arrow query");
    assert!(empty.is_empty(), "empty result must be an empty buffer");
}
