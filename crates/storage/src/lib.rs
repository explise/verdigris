//! verdigris-storage — the object-store seam.
//!
//! Everything in the system reads/writes bytes through `object_store::ObjectStore`
//! (a trait). [`build`] turns config into a concrete backend, so the same code
//! path runs against the local filesystem, an in-memory store, or S3/MinIO with
//! only a config change. The [`SimObjectStore`] (DST) plugs in here too — it is a
//! real `ObjectStore`, so it drops into the same `Store` handle, but it is wired
//! by the simulation harness (it needs seam handles) rather than by [`build`],
//! which stays prod-only.

use std::sync::Arc;

use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
// 0.13 moved put/get/delete into the ObjectStoreExt convenience trait.
use object_store::{ObjectStore, ObjectStoreExt};
use verdigris_core::config::StorageConfig;

pub mod sim;
pub use sim::SimObjectStore;

/// A handle to whichever backend the config selected.
pub type Store = Arc<dyn ObjectStore>;

/// Build the configured object store.
pub fn build(cfg: &StorageConfig) -> anyhow::Result<Store> {
    match cfg {
        StorageConfig::Local { path } => {
            std::fs::create_dir_all(path)
                .with_context(|| format!("creating local store dir {}", path.display()))?;
            let fs = LocalFileSystem::new_with_prefix(path)
                .with_context(|| format!("opening local store at {}", path.display()))?;
            Ok(Arc::new(fs))
        }
        StorageConfig::Memory => Ok(Arc::new(InMemory::new())),
        StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
            access_key_id,
            secret_access_key,
            prefix: _prefix,
        } => {
            // Start from the standard AWS env/profile chain, then let explicit
            // config override. This makes MinIO and real S3 both "just config".
            let mut b = AmazonS3Builder::from_env().with_bucket_name(bucket);
            if let Some(r) = region {
                b = b.with_region(r);
            }
            if let Some(e) = endpoint {
                b = b.with_endpoint(e);
            }
            if *allow_http {
                b = b.with_allow_http(true);
            }
            if let Some(k) = access_key_id {
                b = b.with_access_key_id(k);
            }
            if let Some(s) = secret_access_key {
                b = b.with_secret_access_key(s);
            }
            let s3 = b.build().context("building S3 client")?;
            Ok(Arc::new(s3))
        }
    }
}

/// A human-readable one-liner describing the active backend, for `vdg` output.
pub fn describe(cfg: &StorageConfig) -> String {
    match cfg {
        StorageConfig::Local { path } => format!("local filesystem at {}", path.display()),
        StorageConfig::Memory => "in-memory (ephemeral)".to_string(),
        StorageConfig::S3 {
            bucket, endpoint, ..
        } => match endpoint {
            Some(e) => format!("s3 bucket '{bucket}' via {e}"),
            None => format!("s3 bucket '{bucket}' (aws)"),
        },
    }
}

/// Round-trip a small probe object to prove the backend is reachable and
/// writable. Returns the path it used.
pub async fn health_probe(store: &Store) -> anyhow::Result<ObjPath> {
    let path = ObjPath::from("_verdigris/.health-probe");
    let payload = b"verdigris-ok".to_vec();
    store
        .put(&path, payload.clone().into())
        .await
        .context("probe put")?;
    let got = store.get(&path).await.context("probe get")?;
    let bytes = got.bytes().await.context("probe read")?;
    anyhow::ensure!(bytes.as_ref() == payload.as_slice(), "probe payload mismatch");
    store.delete(&path).await.context("probe delete")?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_backend_round_trips() {
        let store = build(&StorageConfig::Memory).unwrap();
        let path = health_probe(&store).await.unwrap();
        assert_eq!(path.as_ref(), "_verdigris/.health-probe");
    }
}
