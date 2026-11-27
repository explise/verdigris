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
use verdigris_query::engine::{self, QueryCache, QueryLimits, ResultTooLarge};
use verdigris_query::index::IndexedFile;

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
    // Hydrated: these tests exercise the pruning path, and trigrams now live in
    // a sidecar that only text queries fetch (see Ingestor::load_manifest_for).
    let manifest = ingestor
        .load_manifest_with_trigrams()
        .await
        .expect("manifest");
    (store, manifest)
}

/// Every file, scanned whole — the baseline these tests compare pruning against.
fn all_paths(m: &Manifest) -> Vec<IndexedFile> {
    m.files.iter().map(IndexedFile::whole).collect()
}

/// The production ceilings. The corpus is 60 rows — nowhere near them — so these
/// tests exercise the normal path; the bounds themselves are tested below.
fn limits() -> QueryLimits {
    QueryLimits::default()
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
        &limits(),
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
        &limits(),
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
        &limits(),
    )
    .await
    .expect("trace lookup");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["service"].as_str(), Some("auth"));
    assert_eq!(rows[0]["message"].as_str(), Some("auth event 7"));
    assert_eq!(rows[0]["trace_id"].as_str(), Some(TRACE));
}

// A warm request must reuse the schema and Parquet footer metadata instead of
// re-listing the store and re-reading every footer. The cache is still only an
// optimization: hits, misses, and the returned row are identical.
#[tokio::test]
async fn shared_query_cache_reuses_metadata_without_changing_results() {
    let (store, manifest) = seeded_table().await;
    let files = all_paths(&manifest);
    let cache = QueryCache::new(8 * 1024 * 1024);
    let sql = format!("SELECT service, message FROM logs WHERE trace_id = '{TRACE}'");

    let first = engine::query_table_json_cached(
        store.clone(),
        "logs",
        &files,
        &sql,
        &limits(),
        Some(&cache),
    )
    .await
    .expect("cold trace lookup");
    let (entries_after_cold, _, hits_after_cold) = cache.metadata_stats();
    assert!(entries_after_cold > 0, "cold query must populate metadata");

    let second = engine::query_table_json_cached(
        store.clone(),
        "logs",
        &files,
        &sql,
        &limits(),
        Some(&cache),
    )
    .await
    .expect("warm trace lookup");
    let (entries_after_warm, _, hits_after_warm) = cache.metadata_stats();
    assert_eq!(first, second, "cache must not change the result");
    assert_eq!(second.len(), 1);
    assert!(
        hits_after_warm > hits_after_cold,
        "warm query must hit the shared metadata cache ({hits_after_cold} -> {hits_after_warm})"
    );
    assert_eq!(entries_after_cold, entries_after_warm, "no footer re-reads");

    let missing = engine::query_table_json_cached(
        store,
        "logs",
        &files,
        "SELECT service FROM logs WHERE trace_id = 'no-such-trace'",
        &limits(),
        Some(&cache),
    )
    .await
    .expect("cache miss lookup");
    assert!(missing.is_empty(), "a missing trace returns no rows");
}

// The M4.1 guarantee at the execution layer: the engine reads exactly the files
// it is given, so registering `select_files(tiers=[Hot])` — the same selector
// the cost estimate prices — means a hot-only query cannot touch warm/cold data.
#[tokio::test]
async fn engine_reads_only_the_registered_file_set() {
    let (store, manifest) = seeded_table().await;
    let hot: Vec<IndexedFile> = select_files(&manifest, &[Tier::Hot], None, &[])
        .into_iter()
        .map(IndexedFile::whole)
        .collect();
    assert_eq!(hot.len(), 1, "one hot file in the corpus");

    let rows = engine::query_table_json(
        store,
        "logs",
        &hot,
        "SELECT count(*) AS c, count(DISTINCT service) AS s FROM logs",
        &limits(),
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
    assert!(
        sql.contains("message ILIKE '%auth%'"),
        "free text compiles to ILIKE: {sql}"
    );
    let preds = verdigris_core::search::stat_predicates(dsl);
    assert_eq!(
        preds,
        vec![verdigris_core::manifest::Predicate::message_contains(
            "auth"
        )]
    );

    let pruned: Vec<IndexedFile> = select_files(
        &manifest,
        &[Tier::Hot, Tier::Warm, Tier::Cold],
        None,
        &preds,
    )
    .into_iter()
    .map(|f| IndexedFile::plan(f, &preds))
    .collect();
    assert_eq!(pruned.len(), 1, "trigram stats prune to the auth file only");

    let from_pruned = engine::query_table_json(store.clone(), "logs", &pruned, &sql, &limits())
        .await
        .expect("pruned query");
    let from_all = engine::query_table_json(store, "logs", &all, &sql, &limits())
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

    let buf = engine::query_table_arrow(store.clone(), "logs", &files, sql, &limits())
        .await
        .expect("arrow query");
    let reader = StreamReader::try_new(Cursor::new(&buf), None).expect("ipc stream");
    for field in reader.schema().fields() {
        assert!(
            !matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView),
            "view type leaked onto the wire: {field:?}"
        );
    }
    let arrow_rows: usize = reader.map(|b| b.expect("ipc batch").num_rows()).sum();

    let json_rows = engine::query_table_json(store.clone(), "logs", &files, sql, &limits())
        .await
        .expect("json query");
    assert_eq!(arrow_rows, 60);
    assert_eq!(arrow_rows, json_rows.len());

    let empty = engine::query_table_arrow(
        store,
        "logs",
        &files,
        "SELECT * FROM logs WHERE service = 'no-such-service'",
        &limits(),
    )
    .await
    .expect("empty arrow query");
    assert!(empty.is_empty(), "empty result must be an empty buffer");
}

// ── Bounded execution (issue #2) ────────────────────────────────────────────
//
// The acceptance bar is "oversized queries fail gracefully" — so what these pin
// is that the failure is a *typed, actionable error* rather than an OOM kill,
// and that the ceiling is enforced while streaming rather than after the whole
// result is already resident.

/// A `SELECT *` past the row ceiling is refused, and the error carries the
/// numbers a client needs to retry sensibly.
#[tokio::test]
async fn oversized_result_is_refused_with_a_typed_error() {
    let (store, manifest) = seeded_table().await;
    let tight = QueryLimits {
        max_result_rows: 10, // corpus is 60
        ..QueryLimits::default()
    };

    let err = engine::query_table_json(
        store,
        "logs",
        &all_paths(&manifest),
        "SELECT * FROM logs",
        &tight,
    )
    .await
    .expect_err("60 rows must not pass a 10-row ceiling");

    let too_large = err
        .downcast_ref::<ResultTooLarge>()
        .expect("must be a typed ResultTooLarge, not an opaque error");
    assert_eq!(too_large.max_rows, 10);
    assert!(too_large.rows > 10, "reports what it actually accumulated");
    // The message has to tell the operator what to do about it.
    let msg = err.to_string();
    assert!(msg.contains("LIMIT"), "message should suggest a fix: {msg}");
}

/// The byte ceiling is independent of the row ceiling: a result well under the
/// row limit is still refused if it is too fat.
#[tokio::test]
async fn byte_ceiling_trips_independently_of_the_row_ceiling() {
    let (store, manifest) = seeded_table().await;
    let tight = QueryLimits {
        max_result_rows: u64::MAX, // rows can never trip
        max_result_bytes: 1,       // ...but one byte certainly will
        ..QueryLimits::default()
    };

    let err = engine::query_table_json(
        store,
        "logs",
        &all_paths(&manifest),
        "SELECT * FROM logs",
        &tight,
    )
    .await
    .expect_err("a 1-byte ceiling must refuse a 60-row result");
    let too_large = err.downcast_ref::<ResultTooLarge>().expect("typed error");
    assert_eq!(too_large.max_bytes, 1);
    assert!(too_large.bytes > 1);
}

/// A result exactly at the ceiling is allowed — the limit is a maximum, not a
/// strict bound, so a `LIMIT 60` against a 60-row table must still work.
#[tokio::test]
async fn a_result_exactly_at_the_ceiling_is_allowed() {
    let (store, manifest) = seeded_table().await;
    let exact = QueryLimits {
        max_result_rows: 60,
        ..QueryLimits::default()
    };

    let rows = engine::query_table_json(
        store,
        "logs",
        &all_paths(&manifest),
        "SELECT * FROM logs",
        &exact,
    )
    .await
    .expect("60 rows must pass a 60-row ceiling");
    assert_eq!(rows.len(), 60);
}

/// A tiny execution pool must not break ordinary work: the sort and the GROUP BY
/// spill to disk rather than fail. This is the difference between "bounded" and
/// "broken" — the box stays up *and* still answers.
#[tokio::test]
async fn a_tiny_memory_pool_still_serves_sorts_and_aggregates() {
    let (store, manifest) = seeded_table().await;
    let files = all_paths(&manifest);
    let squeezed = QueryLimits {
        memory_pool_bytes: 2 * 1024 * 1024, // 2 MiB to run everything in
        target_partitions: 1,
        ..QueryLimits::default()
    };

    let rows = engine::query_table_json(
        store.clone(),
        "logs",
        &files,
        "SELECT service, count(*) AS c FROM logs GROUP BY service ORDER BY service",
        &squeezed,
    )
    .await
    .expect("an aggregate must survive a small pool");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["c"].as_i64(), Some(20));

    let rows = engine::query_table_json(
        store,
        "logs",
        &files,
        "SELECT ts, service FROM logs ORDER BY ts DESC, service",
        &squeezed,
    )
    .await
    .expect("a sort must survive a small pool");
    assert_eq!(rows.len(), 60);
}

/// The config knobs are what an operator actually turns; they must land on the
/// limits the engine enforces.
#[tokio::test]
async fn limits_are_derived_from_the_config_knobs() {
    let cfg = verdigris_core::config::QueryConfig {
        memory_pool_mib: 64,
        max_result_rows: 123,
        max_result_mib: 2,
        cores: 3,
        ..Default::default()
    };
    let l = QueryLimits::from_config(&cfg);
    assert_eq!(l.memory_pool_bytes, 64 * 1024 * 1024);
    assert_eq!(l.max_result_rows, 123);
    assert_eq!(l.max_result_bytes, 2 * 1024 * 1024);
    assert_eq!(l.target_partitions, 3);
}

#[tokio::test]
async fn trace_partition_cap_preserves_tied_cross_tier_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(store.clone(), "logs");
    let records = vec![
        rec(7, Level::Error, "auth", Some(TRACE)),
        rec(7, Level::Info, "api", Some(TRACE)),
        rec(7, Level::Debug, "worker", Some(TRACE)),
        rec(9, Level::Error, "auth", Some("other")),
    ];
    let files: Vec<_> = ingestor
        .ingest(records, &RoutingConfig::default(), BatchPolicy::default())
        .await
        .unwrap()
        .iter()
        .map(IndexedFile::whole)
        .collect();
    let query =
        format!("SELECT * FROM logs WHERE trace_id = '{TRACE}' ORDER BY ts DESC, service ASC");
    let mut results = Vec::new();
    for cap in [None, Some(1), Some(0), Some(9)] {
        let cfg = verdigris_core::config::QueryConfig {
            cores: 2,
            profile_scan_partitions: cap,
            ..Default::default()
        };
        let limits = QueryLimits::from_config(&cfg);
        assert_eq!(limits.target_partitions, cap.unwrap_or(2).clamp(1, 2));
        let rows = engine::query_table_json(store.clone(), "logs", &files, &query, &limits)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        results.push(rows);
    }
    assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
}

/// Cache safety for legacy keys. A legacy object can be replaced in place with
/// different bytes of the same length; its key proves nothing about identity, so
/// the shared schema/footer cache must be bypassed. A content-addressed key in
/// the same crate keeps using the warm path (covered above).
#[tokio::test]
async fn legacy_key_replacement_bypasses_shared_caches() {
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use verdigris_ingest::encode;

    fn encode_uncompressed(records: &[LogRecord]) -> Vec<u8> {
        let batch = encode::records_to_batch(records).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        buf
    }

    fn rows(trace: &str, marker: &str) -> Vec<LogRecord> {
        (0..200)
            .map(|i| LogRecord {
                ts_millis: BASE_TS + i,
                level: Level::Info,
                service: "api".into(),
                status: Some(200),
                message: format!("{marker} event {i}"),
                trace_id: Some(trace.into()),
                attrs: BTreeMap::new(),
            })
            .collect()
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let legacy = "logs/hot/part-00001.parquet";
    let current = "logs/hot/part-0123456789abcdef.parquet";
    let first = encode_uncompressed(&rows("trace-aaaa", "same"));
    let replacement = encode_uncompressed(&rows("trace-bbbb", "same"));
    assert_eq!(
        first.len(),
        replacement.len(),
        "fixture requires a same-length replacement"
    );
    let other = encode_uncompressed(&rows("trace-cccc", "same"));

    store
        .put(&ObjPath::from(legacy), first.clone().into())
        .await
        .unwrap();
    store
        .put(&ObjPath::from(current), other.clone().into())
        .await
        .unwrap();

    let legacy_file = IndexedFile {
        path: legacy.into(),
        bytes: first.len() as u64,
        row_groups: None,
        immutable: false,
    };
    let current_file = IndexedFile {
        path: current.into(),
        bytes: other.len() as u64,
        row_groups: None,
        immutable: true,
    };
    let cache = QueryCache::new(8 * 1024 * 1024);

    let cold = engine::query_table_json_cached(
        store.clone(),
        "logs",
        std::slice::from_ref(&legacy_file),
        "SELECT trace_id FROM logs WHERE trace_id = 'trace-aaaa'",
        &limits(),
        Some(&cache),
    )
    .await
    .unwrap();
    assert_eq!(cold.len(), 200);

    // Replace in place, same path and length.
    store
        .put(&ObjPath::from(legacy), replacement.into())
        .await
        .unwrap();

    let old = engine::query_table_json_cached(
        store.clone(),
        "logs",
        std::slice::from_ref(&legacy_file),
        "SELECT trace_id FROM logs WHERE trace_id = 'trace-aaaa'",
        &limits(),
        Some(&cache),
    )
    .await
    .unwrap();
    assert!(
        old.is_empty(),
        "the replaced object must not return old rows"
    );
    let new = engine::query_table_json_cached(
        store.clone(),
        "logs",
        std::slice::from_ref(&legacy_file),
        "SELECT trace_id FROM logs WHERE trace_id = 'trace-bbbb'",
        &limits(),
        Some(&cache),
    )
    .await
    .unwrap();
    assert_eq!(new.len(), 200);
    assert!(new.iter().all(|r| r["trace_id"] == "trace-bbbb"));

    // Mixed legacy + current objects must equal the uncached scan.
    let mixed = vec![legacy_file, current_file.clone()];
    let cached = engine::query_table_json_cached(
        store.clone(),
        "logs",
        &mixed,
        "SELECT trace_id FROM logs ORDER BY trace_id",
        &limits(),
        Some(&cache),
    )
    .await
    .unwrap();
    let uncached = engine::query_table_json(
        store,
        "logs",
        &mixed,
        "SELECT trace_id FROM logs ORDER BY trace_id",
        &limits(),
    )
    .await
    .unwrap();
    assert_eq!(cached, uncached);
    assert_eq!(
        cached
            .iter()
            .filter(|r| r["trace_id"] == "trace-bbbb")
            .count(),
        200
    );
    assert_eq!(
        cached
            .iter()
            .filter(|r| r["trace_id"] == "trace-cccc")
            .count(),
        200
    );
}
