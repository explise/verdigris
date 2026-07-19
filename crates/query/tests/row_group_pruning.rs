//! Row-group pruning through the real query engine (issue #23).
//!
//! The write-side properties — index/row-group alignment, and no set ever missing a
//! substring of its own rows — are pinned in
//! `crates/ingest/tests/row_group_index.rs`. What has to hold *here* is the thing a
//! user actually experiences: turning the index on changes how much gets read and
//! nothing else. Every assertion below is therefore a comparison against the same
//! query run with pruning disabled, not against a hardcoded expected result — a
//! hardcoded expectation would pass just as happily if both paths were wrong.
//!
//! Files are written with 512-row row groups (`MergeWriter::with_row_group_rows`)
//! so a few thousand rows produce enough groups to prune; production cuts at 128Ki.

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use verdigris_core::batch::LogRecord;
use verdigris_core::manifest::{DataFile, Predicate};
use verdigris_core::model::{Level, Tier};
use verdigris_core::search;
use verdigris_ingest::encode::{self, MergeWriter};
use verdigris_ingest::schema::log_schema;
use verdigris_query::engine::{self, QueryLimits};
use verdigris_query::index::{IndexedFile, IndexedParquetTable};

const RG: usize = 512;
const ROWS: usize = 60 * RG;
const BASE_TS: i64 = 1_700_000_000_000;
const PATH: &str = "logs/hot/part-idx.parquet";

/// Templated log lines with three tokens of deliberately different rarity:
/// `quasar` (once every ~30 row groups — prunes to almost nothing), `billing`
/// (a service name, in a quarter of all rows — prunes nothing), and `zzz-absent`
/// (never written — prunes everything).
fn corpus() -> Vec<LogRecord> {
    let services = ["auth", "api", "worker", "billing"];
    let templates = [
        "connection established to db-primary",
        "request completed status=200 latency=12ms",
        "cache miss for key user-profile",
        "retrying upstream call attempt=2",
    ];
    (0..ROWS)
        .map(|i| {
            let message = if i % (30 * RG) == 13 {
                format!("panic: unrecoverable quasar failure in shard {i}")
            } else {
                format!("{} seq={i}", templates[i % templates.len()])
            };
            LogRecord {
                ts_millis: BASE_TS + i as i64 * 10,
                level: if i % 9 == 0 {
                    Level::Error
                } else {
                    Level::Info
                },
                service: services[i % services.len()].into(),
                status: Some(200),
                message,
                trace_id: None,
                attrs: BTreeMap::new(),
            }
        })
        .collect()
}

/// One multi-row-group Parquet file in an in-memory store, plus the manifest entry
/// describing it — index and row-group count included, exactly as the ingest path
/// would have committed them.
async fn seeded() -> (Arc<dyn ObjectStore>, DataFile) {
    let batch = encode::records_to_batch(&corpus()).expect("batch");
    let mut w = MergeWriter::with_row_group_rows(log_schema(), RG).expect("writer");
    w.write(&batch).expect("write");
    let (bytes, stats) = w.finish().expect("finish");
    assert!(
        stats.row_groups >= 50,
        "need many row groups to prune; got {}",
        stats.row_groups
    );

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let len = bytes.len() as u64;
    store
        .put(&ObjPath::from(PATH), bytes.into())
        .await
        .expect("put");

    let file = DataFile {
        path: PATH.into(),
        bytes: len,
        rows: stats.rows,
        min_ts: stats.min_ts,
        max_ts: stats.max_ts,
        tier: Tier::Hot,
        services: stats.services,
        levels: stats.levels,
        row_groups: stats.row_groups,
        message_trigrams: Some(stats.message_trigrams),
        row_group_trigrams: Some(stats.row_group_trigrams),
    };
    (store, file)
}

fn limits() -> QueryLimits {
    QueryLimits::default()
}

/// Run `term` as a free-text DSL search, with pruning on and off, returning
/// `(pruned_rows, unpruned_rows, scanned_row_groups, total_row_groups)`.
async fn both_ways(
    store: &Arc<dyn ObjectStore>,
    file: &DataFile,
    term: &str,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>, usize, usize) {
    let sql = search::to_sql(term, "logs", BASE_TS + 86_400_000, 100_000).expect("dsl");
    let preds = search::stat_predicates(term);

    let pruned = IndexedFile::plan(file, &preds);
    let (scanned, total) = pruned.scanned_row_groups().unwrap_or((0, 0));

    let with = engine::query_table_json(store.clone(), "logs", &[pruned], &sql, &limits())
        .await
        .expect("pruned query");
    let without = engine::query_table_json(
        store.clone(),
        "logs",
        &[IndexedFile::whole(file)],
        &sql,
        &limits(),
    )
    .await
    .expect("unpruned query");
    (with, without, scanned, total)
}

/// The headline correctness bar, extending `free_text_pruning_never_changes_results`
/// from file granularity to row-group granularity: for a spread of terms — common,
/// rare, absent, in-word, punctuated, and case-mismatched — the pruned and unpruned
/// queries return byte-identical rows.
#[tokio::test]
async fn row_group_pruning_never_changes_results() {
    let (store, file) = seeded().await;

    for term in [
        "quasar",     // rare: prunes to a handful of row groups
        "cache",      // common: prunes nothing
        "zzz-absent", // absent: prunes everything
        "nnect",      // in-word substring — a token index would wrongly prune this
        "db-primary", // punctuation collapses to the "other" bucket
        "QUASAR",     // ILIKE is case-insensitive; so is the trigram alphabet
        "shard",
        "latency=12ms",
    ] {
        let (with, without, _, _) = both_ways(&store, &file, term).await;
        assert_eq!(
            with, without,
            "row-group pruning changed the result set for {term:?}"
        );
    }
}

/// Every substring of length ≥ 3 of a sampled set of real messages returns the same
/// rows pruned as unpruned.
///
/// The exhaustive form of this property — every substring of *every* message — is
/// checked directly against the trigram sets in
/// `ingest/tests/row_group_index.rs::no_row_group_set_misses_a_substring_of_its_own_rows`,
/// where it costs a bitmap lookup per substring. Here each case is a full query
/// through DataFusion, so this samples instead: it is the end-to-end confirmation
/// that the set-level property survives the translation into an access plan, not an
/// independent proof of it.
#[tokio::test]
async fn sampled_substrings_return_identical_rows_pruned_or_not() {
    let (store, file) = seeded().await;
    let messages = [
        "panic: unrecoverable quasar failure in shard 15373",
        "cache miss for key user-profile",
        "retrying upstream call attempt=2",
    ];

    let mut checked = 0;
    let mut skipped = 0;
    for msg in messages {
        let chars: Vec<char> = msg.chars().collect();
        // Every 3-, 7- and 11-char window: short enough to be cheap, long enough to
        // span the whole message, and differing lengths so the windows do not all
        // land on the same token boundaries.
        for len in [3, 7, 11] {
            for start in (0..chars.len().saturating_sub(len)).step_by(5) {
                let term: String = chars[start..start + len].iter().collect();
                // An arbitrary substring is not necessarily a valid DSL term — `:`
                // makes it a field selector, `%`/`_` make it a wildcard that
                // `stat_predicates` deliberately excludes from pruning. Those are
                // properties of the search grammar, not of the index, so skip them
                // rather than assert on them.
                if search::to_sql(&term, "logs", BASE_TS, 10).is_err()
                    || term.contains(['%', '_', ':'])
                    || term.trim().is_empty()
                {
                    skipped += 1;
                    continue;
                }
                let (with, without, _, _) = both_ways(&store, &file, &term).await;
                assert_eq!(with, without, "pruning changed results for {term:?}");
                checked += 1;
            }
        }
    }
    // Guard against the skip list quietly swallowing the whole sample.
    assert!(checked > 30, "sampled too few substrings ({checked})");
    assert!(
        skipped < checked,
        "skipped more substrings ({skipped}) than were checked ({checked})"
    );
}

/// The selectivity bar: a rare term must read a small fraction of the row groups,
/// while still returning every matching row. Correctness and the performance claim
/// asserted together — either alone is easy to satisfy wrongly.
#[tokio::test]
async fn a_rare_term_reads_under_five_percent_of_row_groups() {
    let (store, file) = seeded().await;
    let (with, without, scanned, total) = both_ways(&store, &file, "quasar").await;

    assert_eq!(with, without, "pruning must not change results");
    assert!(!with.is_empty(), "the rare term does occur");
    assert!(
        (scanned as f64 / total as f64) < 0.05,
        "read {scanned}/{total} row groups for a rare term; the issue's bar is < 5%"
    );
}

/// A term that was never written prunes every row group — and the file drops out of
/// the scan entirely rather than being opened to read nothing.
#[tokio::test]
async fn an_absent_term_prunes_every_row_group() {
    let (store, file) = seeded().await;
    let (with, _, scanned, total) = both_ways(&store, &file, "zzz-absent").await;
    assert!(with.is_empty(), "an absent term matches nothing");
    assert_eq!(scanned, 0, "every one of {total} row groups is skippable");

    let table = IndexedParquetTable::new(
        log_schema(),
        datafusion::execution::object_store::ObjectStoreUrl::parse("verdigris://store").unwrap(),
        vec![IndexedFile::plan(
            &file,
            &[Predicate::message_contains("zzz-absent")],
        )],
    );
    assert_eq!(
        table.row_group_selectivity(),
        (0, 0),
        "a fully-pruned file is dropped from the scan, not opened"
    );
    let _ = store;
}

/// A common term prunes nothing — the control that keeps the selectivity assertions
/// above honest. An index that pruned aggressively regardless of the term would
/// pass those and fail this.
#[tokio::test]
async fn a_common_term_prunes_nothing() {
    let (store, file) = seeded().await;
    let (with, without, scanned, total) = both_ways(&store, &file, "cache").await;
    assert_eq!(with, without);
    assert_eq!(
        scanned, total,
        "a ubiquitous term must keep every row group"
    );
}

/// Without the index — a legacy file, or a sidecar that never loaded — the query
/// still returns exactly the same rows. This is the degradation path the whole
/// design leans on, so it is asserted rather than assumed.
#[tokio::test]
async fn an_unindexed_file_returns_the_same_rows() {
    let (store, mut file) = seeded().await;
    let sql = search::to_sql("quasar", "logs", BASE_TS + 86_400_000, 100_000).unwrap();
    let preds = search::stat_predicates("quasar");

    let indexed = engine::query_table_json(
        store.clone(),
        "logs",
        &[IndexedFile::plan(&file, &preds)],
        &sql,
        &limits(),
    )
    .await
    .expect("indexed query");

    file.row_group_trigrams = None;
    let planned = IndexedFile::plan(&file, &preds);
    assert!(
        planned.row_groups.is_none(),
        "no index means no mask, not an empty one"
    );
    let unindexed = engine::query_table_json(store, "logs", &[planned], &sql, &limits())
        .await
        .expect("unindexed query");

    assert_eq!(indexed, unindexed, "the index must be invisible in results");
    assert!(!indexed.is_empty());
}
