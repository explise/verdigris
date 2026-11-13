//! The invariant behind narrowing `ingest_lock`: encoding and PUTting may run
//! concurrently, but committing must be serialized.
//!
//! `crates/vdg/src/serve.rs` no longer holds a lock across the whole write path —
//! only across `commit_files`. These tests pin the two properties that makes safe,
//! so a future refactor that widens or drops the remaining lock fails here rather
//! than in production.

use std::collections::BTreeMap;
use std::sync::Arc;
use verdigris_core::batch::{BatchPolicy, LogRecord};
use verdigris_core::config::RoutingConfig;
use verdigris_core::model::Level;
use verdigris_ingest::Ingestor;

type Store = Arc<dyn object_store::ObjectStore>;

fn store() -> Store {
    Arc::new(object_store::memory::InMemory::new())
}

fn records(service: &str, n: usize) -> Vec<LogRecord> {
    (0..n)
        .map(|i| LogRecord {
            ts_millis: 1_700_000_000_000 + i as i64,
            level: Level::Info,
            service: service.to_string(),
            status: Some(200),
            // Distinct per record, so batches never collide on content hash and
            // this measures real appends rather than idempotent dedup.
            message: format!("{service} event {i}"),
            trace_id: Some(format!("trace-{service}-{i}")),
            attrs: BTreeMap::new(),
        })
        .collect()
}

/// Splitting `ingest` into `write_batches` + `commit_files` must be behaviour-
/// preserving: the same records land, in the same files.
#[tokio::test]
async fn split_matches_the_combined_call() {
    let routing = RoutingConfig::default();
    let policy = BatchPolicy::default();

    let a = Ingestor::new(store(), "logs");
    let combined = a
        .ingest(records("auth", 500), &routing, policy)
        .await
        .unwrap();

    let b = Ingestor::new(store(), "logs");
    let split = b
        .write_batches(records("auth", 500), &routing, policy)
        .await
        .unwrap();
    b.commit_files(&split).await.unwrap();

    assert_eq!(combined.len(), split.len());
    // Content-addressed, so identical records must produce identical paths.
    let pa: Vec<_> = combined.iter().map(|f| &f.path).collect();
    let pb: Vec<_> = split.iter().map(|f| &f.path).collect();
    assert_eq!(pa, pb, "split must write the same content-addressed files");

    let ma = a.load_manifest().await.unwrap();
    let mb = b.load_manifest().await.unwrap();
    assert_eq!(ma.files.len(), mb.files.len());
    assert_eq!(
        ma.files.iter().map(|f| f.rows).sum::<u64>(),
        mb.files.iter().map(|f| f.rows).sum::<u64>(),
    );
}

/// The production shape: many writers encode and PUT at once against one store,
/// then commit one at a time. Every record must end up in the manifest — this is
/// what makes it safe to drop the lock around the expensive half.
#[tokio::test]
async fn concurrent_writes_then_serialized_commits_lose_nothing() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 400;

    let s = store();
    let routing = RoutingConfig::default();
    let policy = BatchPolicy::default();

    // Phase 1, fully concurrent — no lock, exactly as the HTTP handler now runs it.
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let s = s.clone();
        let routing = routing.clone();
        handles.push(tokio::spawn(async move {
            let ing = Ingestor::new(s, "logs");
            ing.write_batches(records(&format!("svc{w}"), PER_WRITER), &routing, policy)
                .await
                .unwrap()
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }

    // Phase 2, serialized — what the shell's ingest_lock guarantees.
    let ing = Ingestor::new(s.clone(), "logs");
    for f in &all {
        ing.commit_files(std::slice::from_ref(f)).await.unwrap();
    }

    let manifest = ing.load_manifest().await.unwrap();
    assert_eq!(
        manifest.files.len(),
        all.len(),
        "every concurrently written file must be in the manifest"
    );
    assert_eq!(
        manifest.files.iter().map(|f| f.rows).sum::<u64>(),
        (WRITERS * PER_WRITER) as u64,
        "no records lost across concurrent encode + serialized commit"
    );
}

/// Committing the same files twice must be a no-op, not a duplicate. A retried
/// request (or a crash between PUT and commit, then a replay) must not double-count.
#[tokio::test]
async fn commit_is_idempotent() {
    let s = store();
    let ing = Ingestor::new(s, "logs");
    let routing = RoutingConfig::default();

    let files = ing
        .write_batches(records("auth", 300), &routing, BatchPolicy::default())
        .await
        .unwrap();

    ing.commit_files(&files).await.unwrap();
    let after_first = ing.load_manifest().await.unwrap().files.len();

    ing.commit_files(&files).await.unwrap();
    let after_second = ing.load_manifest().await.unwrap();

    assert_eq!(
        after_first,
        after_second.files.len(),
        "re-commit duplicated files"
    );
    assert_eq!(after_second.files.iter().map(|f| f.rows).sum::<u64>(), 300);
}
