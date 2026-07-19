//! The per-row-group trigram index, on the write side (issue #23).
//!
//! Two things have to hold for the index to be usable at all, and they are the
//! reason this file exists rather than folding the cases into `encode.rs`'s unit
//! tests: the recorded sets must line up **one-for-one with the row groups the
//! Parquet writer actually emitted**, and each set must be a superset of its own
//! rows' trigrams. The first is an alignment property only readable from the
//! finished file's footer; the second is the no-false-negatives guarantee that the
//! whole pruning scheme rests on, one level down from
//! `text.rs::no_false_negatives_for_any_recorded_substring`.
//!
//! Row groups are cut every 512 rows here rather than the production 128Ki
//! (`MergeWriter::with_row_group_rows`) — same code, few enough rows to stay fast.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Array;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use verdigris_core::batch::{BatchPolicy, LogRecord};
use verdigris_core::config::RoutingConfig;
use verdigris_core::manifest::Predicate;
use verdigris_core::model::Level;
use verdigris_core::text::TrigramSet;
use verdigris_ingest::encode::{self, FileStats, MergeWriter};
use verdigris_ingest::schema::log_schema;
use verdigris_ingest::Ingestor;

const RG: usize = 512;
const BASE_TS: i64 = 1_700_000_000_000;

/// Realistic log lines: templated and repetitive, like real service output, with a
/// handful of distinctive tokens sprinkled thinly. That shape is what the index is
/// for — a rare term confined to a few row groups — and also what makes the sparse
/// encoding pay, so the overhead assertion below is measured against something
/// representative rather than random noise (which would be the worst case for both).
fn corpus(n: usize) -> Vec<LogRecord> {
    corpus_every(n, 700)
}

/// As [`corpus`], with the rare `quasar` token planted once every `needle_period`
/// rows.
///
/// The period is explicit because selectivity is a property of *rarity relative to
/// row-group size*, not of the index: a token appearing every 700 rows genuinely is
/// in most 512-row groups, and an index reporting otherwise would be broken. Only a
/// token rarer than the group size can be pruned to a small fraction of groups.
fn corpus_every(n: usize, needle_period: usize) -> Vec<LogRecord> {
    let services = ["auth", "api", "worker", "billing"];
    let templates = [
        "connection established to db-primary",
        "request completed status=200 latency=12ms",
        "cache miss for key user-profile",
        "retrying upstream call attempt=2",
    ];
    (0..n)
        .map(|i| {
            let message = if i % needle_period == 13 {
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

/// Encode `records` through `MergeWriter` at the small row-group size.
fn encode_at(records: &[LogRecord], row_group_rows: usize) -> (Vec<u8>, FileStats) {
    let batch = encode::records_to_batch(records).expect("batch");
    let mut w = MergeWriter::with_row_group_rows(log_schema(), row_group_rows).expect("writer");
    w.write(&batch).expect("write");
    w.finish().expect("finish")
}

fn row_groups_in(bytes: &[u8]) -> usize {
    ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
        .expect("open parquet")
        .metadata()
        .num_row_groups()
}

/// The alignment invariant. Everything else is only meaningful if the index has
/// exactly one entry per row group *of the file that was written* — an off-by-one
/// would silently shift every mask by a group, and a shifted mask skips row groups
/// that hold real matches.
#[test]
fn one_trigram_set_per_row_group_actually_emitted() {
    // Deliberately not a multiple of RG: the trailing partial group is the case a
    // cut-on-full-group-only implementation drops.
    for rows in [1, RG - 1, RG, RG + 1, 3 * RG, 3 * RG + 7] {
        let (bytes, stats) = encode_at(&corpus(rows), RG);
        let actual = row_groups_in(&bytes);
        assert_eq!(
            stats.row_group_trigrams.len(),
            actual,
            "{rows} rows: index has {} sets for {actual} row groups",
            stats.row_group_trigrams.len()
        );
        assert_eq!(
            stats.row_groups as usize, actual,
            "{rows} rows: manifest count must agree with the footer"
        );
        assert_eq!(stats.rows as usize, rows, "{rows} rows: no rows lost");
    }
}

/// The safety property, at row-group granularity: for every row, every substring of
/// its message that is at least a trigram long must be admitted by the set of the
/// row group that row landed in.
///
/// This is the assertion that makes a skip a proof. It is checked against the row
/// groups the *reader* reports rather than against the writer's own bookkeeping, so
/// a fold that drifted from the actual cuts fails here rather than passing by
/// agreeing with itself.
#[test]
fn no_row_group_set_misses_a_substring_of_its_own_rows() {
    let records = corpus(3 * RG + 7);
    let (bytes, stats) = encode_at(&records, RG);

    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .expect("open parquet")
        .with_batch_size(RG)
        .build()
        .expect("reader");

    // Walk the decoded rows in order, tracking which row group each falls in.
    let mut row = 0usize;
    for batch in reader {
        let batch = batch.expect("batch");
        let col = batch
            .column_by_name("message")
            .expect("message column")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("utf8")
            .clone();
        for i in 0..col.len() {
            let group = row / RG;
            let set = &stats.row_group_trigrams[group];
            let msg = col.value(i);
            let chars: Vec<char> = msg.chars().collect();
            for start in 0..chars.len() {
                for end in (start + 3)..=chars.len() {
                    let term: String = chars[start..end].iter().collect();
                    assert_ne!(
                        set.contains_term(&term),
                        Some(false),
                        "row group {group} claims {term:?} absent, but row {row} is {msg:?}"
                    );
                }
            }
            row += 1;
        }
    }
    assert_eq!(row, records.len(), "every row checked");
}

/// The file-level set must be exactly the union of the row-group sets.
///
/// Not merely a tidiness check: `select_files` prunes on the file-level set and the
/// row-group masks prune inside what survives. A file-level set *narrower* than the
/// union would prune away files whose row groups still admit the term.
#[test]
fn file_level_set_is_the_union_of_its_row_groups() {
    let (_, stats) = encode_at(&corpus(3 * RG + 7), RG);
    let mut union = TrigramSet::new();
    for g in &stats.row_group_trigrams {
        union.union_with(g);
    }
    assert_eq!(
        union, stats.message_trigrams,
        "file-level trigrams must equal the union of the row-group sets"
    );
    assert!(!union.is_empty(), "the corpus records something");
}

/// The overhead ceiling from the issue: the index must cost well under 1% of the
/// data bytes it indexes. Measured on the serialized (sparse-encoded) form, since
/// that is what is actually stored in the sidecar.
#[test]
fn index_overhead_stays_far_below_one_percent_of_data() {
    // At the production row-group size, so the ratio is the real one rather than an
    // artifact of the tiny test groups (more groups per byte = more overhead).
    let records = corpus(200_000);
    let (bytes, stats) = encode_at(&records, encode::ROWS_PER_ROW_GROUP);
    assert!(
        stats.row_group_trigrams.len() > 1,
        "need multiple row groups to measure per-group overhead"
    );

    let index_bytes: usize = stats
        .row_group_trigrams
        .iter()
        .map(|s| s.to_base64().len())
        .sum();
    let ratio = index_bytes as f64 / bytes.len() as f64;
    assert!(
        ratio < 0.01,
        "row-group index is {ratio:.4} of data bytes ({index_bytes} B index / {} B data) \
         — the issue's ceiling is 1%",
        bytes.len()
    );
}

/// A rare term must be confined to a small minority of row groups — otherwise the
/// index is correct but worthless. This is the write-side half of the issue's
/// "touches < 5% of row groups" bar; the read-side half is in
/// `crates/query/tests/row_group_pruning.rs`.
#[test]
fn a_rare_term_survives_in_only_a_few_row_groups() {
    // One occurrence per ~40 row groups: the "rare stack trace in a big corpus"
    // case, where a term is confined to a handful of groups out of hundreds.
    let (_, stats) = encode_at(&corpus_every(200 * RG, 40 * RG), RG);
    let total = stats.row_group_trigrams.len();
    let admitting = stats
        .row_group_trigrams
        .iter()
        .filter(|s| s.contains_term("quasar") != Some(false))
        .count();
    assert!(
        (admitting as f64 / total as f64) < 0.05,
        "'quasar' admitted by {admitting}/{total} row groups; the index is not selective"
    );
    // …and the common template text is in essentially all of them, which is the
    // control: the selectivity above is a property of the term, not of a broken set.
    let common = stats
        .row_group_trigrams
        .iter()
        .filter(|s| s.contains_term("cache miss") != Some(false))
        .count();
    assert_eq!(
        common, total,
        "a ubiquitous term must not be pruned anywhere"
    );
}

/// Compaction has to rebuild the index, not inherit or drop it — the same rule the
/// value stats already follow. A compacted file cuts row groups at its own
/// boundaries, so the inputs' sets do not describe it and reusing them would
/// misalign every mask.
#[tokio::test]
async fn compaction_rebuilds_the_row_group_index_for_the_merged_file() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ing = Ingestor::new(store.clone(), "logs");

    // Several small files, all hot, so compaction has something to merge.
    for chunk in corpus(4_000).chunks(500) {
        ing.ingest(
            chunk.to_vec(),
            &RoutingConfig::default(),
            BatchPolicy::default(),
        )
        .await
        .expect("ingest");
    }
    let before = ing.load_manifest().await.expect("manifest");
    assert!(before.files.len() > 1, "need multiple files to compact");

    ing.compact(u64::MAX).await.expect("compact");

    let after = ing
        .load_manifest_with_trigrams()
        .await
        .expect("manifest with trigrams");
    assert_eq!(
        after.total_rows(),
        before.total_rows(),
        "compaction must not lose rows"
    );

    for f in &after.files {
        let sets = f
            .row_group_trigrams
            .as_ref()
            .unwrap_or_else(|| panic!("{} has no row-group index after compaction", f.path));
        assert_eq!(
            sets.len() as u64,
            f.row_groups,
            "{}: sidecar sets must match the manifest's row-group count",
            f.path
        );

        // And the rebuilt index still admits the rare term wherever it survived —
        // an index rebuilt from the wrong rows would prune it away entirely.
        let preds = vec![Predicate::message_contains("quasar")];
        let mask = f
            .row_groups_to_scan(&preds, f.row_groups as usize)
            .expect("index is usable");
        assert!(
            mask.iter().any(|b| *b),
            "{}: 'quasar' pruned out of every row group after compaction",
            f.path
        );
    }
}

/// A file whose recorded set count disagrees with the manifest's row-group count is
/// treated as unindexed. This is the guard that keeps a stale or mismatched sidecar
/// from skipping row groups by index into the wrong file.
#[test]
fn a_count_mismatch_disables_pruning_rather_than_misapplying_it() {
    let (_, stats) = encode_at(&corpus(3 * RG), RG);
    let mut f = verdigris_core::manifest::DataFile {
        path: "logs/hot/x.parquet".into(),
        bytes: 1,
        rows: stats.rows,
        min_ts: stats.min_ts,
        max_ts: stats.max_ts,
        tier: verdigris_core::model::Tier::Hot,
        services: stats.services.clone(),
        levels: stats.levels.clone(),
        row_groups: stats.row_groups,
        message_trigrams: Some(stats.message_trigrams.clone()),
        row_group_trigrams: Some(stats.row_group_trigrams.clone()),
    };
    let preds = vec![Predicate::message_contains("quasar")];
    assert!(
        f.row_groups_to_scan(&preds, f.row_groups as usize)
            .is_some(),
        "matching counts should yield a usable mask"
    );

    // Drop one set: the index no longer describes this file.
    f.row_group_trigrams.as_mut().unwrap().pop();
    assert!(
        f.row_groups_to_scan(&preds, f.row_groups as usize)
            .is_none(),
        "a short index must disable pruning, not shift the mask"
    );
}
