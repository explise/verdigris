//! Compaction output keys must be immutable even when two attempts observed the
//! same manifest generation. This is the property that keeps a losing attempt
//! from overwriting the file the winning manifest references.

use std::sync::Arc;

use verdigris_core::batch::BatchPolicy;
use verdigris_core::config::RoutingConfig;
use verdigris_ingest::{generate, Ingestor};

type Store = Arc<dyn object_store::ObjectStore>;

async fn compact_paths(seed: u64) -> Vec<String> {
    let routing = RoutingConfig::default();
    let policy = BatchPolicy {
        max_rows: 100,
        max_bytes: usize::MAX,
    };
    let store: Store = Arc::new(object_store::memory::InMemory::new());
    let ing = Ingestor::new(store, "logs");
    for i in 0u64..4 {
        ing.ingest(
            generate::generate(100, seed + i, (i as i64) * 1_000_000),
            &routing,
            policy,
        )
        .await
        .unwrap();
    }
    ing.compact(u64::MAX).await.unwrap();
    ing.load_manifest()
        .await
        .unwrap()
        .files
        .iter()
        .map(|f| f.path.rsplit('/').next().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn concurrent_compaction_attempts_with_different_inputs_get_different_keys() {
    // Same table name, same generation, same bin/sequence — only the content
    // differs. A generation-only key would collide and let the loser clobber
    // the file the winner's manifest points at.
    let a = compact_paths(1).await;
    let b = compact_paths(1).await;
    let c = compact_paths(2).await;
    assert_eq!(a, b, "identical bytes may reuse the same immutable key");
    assert_ne!(a, c, "different bytes must not overwrite one another");
}
