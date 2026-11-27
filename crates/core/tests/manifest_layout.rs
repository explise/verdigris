//! Layout conventions the planner and live tail both depend on.

use verdigris_core::manifest::DataFile;
use verdigris_core::model::Tier;

fn file(path: &str) -> DataFile {
    DataFile {
        path: path.into(),
        bytes: 1,
        rows: 1,
        min_ts: 0,
        max_ts: 1,
        tier: Tier::Hot,
        services: vec![],
        levels: vec![],
        message_trigrams: None,
        row_groups: 0,
        row_group_trigrams: None,
    }
}

#[test]
fn ingest_parts_are_distinguishable_from_compaction_output() {
    assert!(file("logs/hot/part-deadbeef.parquet").is_ingest_part());
    assert!(!file("logs/hot/c3-00000-deadbeef.parquet").is_ingest_part());
}

#[test]
fn only_content_addressed_keys_prove_immutability() {
    assert!(file("logs/hot/part-0123456789abcdef.parquet").is_immutable_key());
    assert!(file("logs/hot/c3-00007-0123456789abcdef.parquet").is_immutable_key());
    // Legacy generation/counter keys can be replaced at the same path and size.
    assert!(!file("logs/hot/part-00001.parquet").is_immutable_key());
    assert!(!file("logs/hot/c3-00007.parquet").is_immutable_key());
    assert!(!file("logs/hot/c3-00007-nothex-at-all.parquet").is_immutable_key());
}
