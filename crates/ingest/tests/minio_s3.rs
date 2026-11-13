//! The S3 CAS lane (issue #9): the manifest commit against a store that can
//! actually reject a stale write.
//!
//! Every other test in this crate runs against `LocalFileSystem` or `InMemory`.
//! Neither is S3: the manifest commit is an optimistic compare-and-swap
//! (`PutMode::Create` for a new table, `PutMode::Update(etag)` after that), and
//! the property that makes it worth having is that the *store* answers with a
//! precondition failure instead of overwriting. Only a real S3 implementation
//! can demonstrate that, so these tests talk to MinIO, which implements the same
//! `If-Match` / `If-None-Match` contract.
//!
//! `#[ignore]`d on purpose, and gated on `VERDIGRIS_MINIO_ENDPOINT`: the lane
//! needs a service container and a request sequence that must not run on a
//! developer machine or in `cargo test --workspace` (see
//! `.github/workflows/minio-s3.yml`, which is where it runs, and `AGENTS.md` §1).
//! A run without a MinIO to talk to fails rather than passes — an unrun check is
//! not a passed check.

use std::collections::HashSet;

use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use verdigris_core::batch::{BatchPolicy, LogRecord};
use verdigris_core::config::{RoutingConfig, StorageConfig};
use verdigris_core::manifest::DataFile;
use verdigris_core::model::Level;
use verdigris_ingest::Ingestor;
use verdigris_storage::Store;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// The production S3 arm of `build()`, not a hand-rolled `AmazonS3Builder`: if
/// the shipped config path stops producing a store with working conditional put,
/// this lane has to see it. The endpoint is the gate — without it these tests
/// panic rather than quietly pass.
fn minio() -> Store {
    let endpoint = std::env::var("VERDIGRIS_MINIO_ENDPOINT").expect(
        "VERDIGRIS_MINIO_ENDPOINT is not set: this lane needs a live MinIO \
         (see .github/workflows/minio-s3.yml) and must not pass without one",
    );
    let cfg = StorageConfig::S3 {
        bucket: env_or("VERDIGRIS_MINIO_BUCKET", "verdigris-test"),
        region: Some(env_or("VERDIGRIS_MINIO_REGION", "us-east-1")),
        endpoint: Some(endpoint),
        allow_http: true,
        access_key_id: Some(env_or("VERDIGRIS_MINIO_ACCESS_KEY", "minioadmin")),
        secret_access_key: Some(env_or("VERDIGRIS_MINIO_SECRET_KEY", "minioadmin")),
        prefix: None,
    };
    verdigris_storage::build(&cfg).expect("building the S3 store from config")
}

fn put_opts(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        ..Default::default()
    }
}

fn manifest_path(table: &str) -> ObjPath {
    ObjPath::from(format!("{table}/_metadata/manifest.json"))
}

/// Drop the table's metadata so a rerun starts from the same state as the first
/// run. Data files are content-addressed and therefore identical across runs;
/// only the manifest and its sidecar carry history.
async fn reset(store: &Store, table: &str) {
    for path in [
        manifest_path(table),
        ObjPath::from(format!("{table}/_metadata/trigrams.json")),
    ] {
        match store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => panic!("clearing {path}: {e}"),
        }
    }
}

fn records(service: &str, n: usize) -> Vec<LogRecord> {
    (0..n)
        .map(|i| LogRecord {
            ts_millis: 1_700_000_000_000 + i as i64,
            level: Level::Info,
            service: service.to_string(),
            status: Some(200),
            // Distinct per record, so batches never collide on content hash and
            // these tests measure real appends rather than idempotent dedup.
            message: format!("{service} event {i}"),
            trace_id: Some(format!("trace-{service}-{i}")),
            attrs: Default::default(),
        })
        .collect()
}

fn update(e_tag: &str) -> PutMode {
    PutMode::Update(UpdateVersion {
        e_tag: Some(e_tag.to_string()),
        version: None,
    })
}

/// The contract the commit path assumes, verified against MinIO directly: a
/// create must not clobber, a stale update must be rejected *as a failed
/// precondition*, and an update at the current version must win.
///
/// This is the issue's "verify MinIO's semantics match what `object_store`
/// expects" item. Assertions are on the error variant, because a test that
/// accepts any error proves nothing about CAS.
#[tokio::test]
#[ignore = "needs a live MinIO: CI-only lane, see .github/workflows/minio-s3.yml"]
async fn conditional_put_contract() {
    let store = minio();
    let path = ObjPath::from("issue-9/conditional-put.json");
    let _ = store.delete(&path).await;

    store
        .put_opts(
            &path,
            Bytes::from_static(b"v1").into(),
            put_opts(PutMode::Create),
        )
        .await
        .expect("Create on a path that must not exist");

    // A second Create must be refused, not applied. `object_store`'s S3 arm maps
    // MinIO's 412 on If-None-Match:* to AlreadyExists (aws/mod.rs:188-200); some
    // S3-compatible stores report Precondition instead, and either is a refusal.
    let err = store
        .put_opts(
            &path,
            Bytes::from_static(b"clobber").into(),
            put_opts(PutMode::Create),
        )
        .await
        .expect_err("Create over an existing object must be rejected");
    assert!(
        matches!(
            err,
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
        ),
        "Create over an existing object: expected AlreadyExists or Precondition, got {err:?}"
    );

    let got = store.get(&path).await.expect("reading back v1");
    let v1 = got
        .meta
        .e_tag
        .clone()
        .expect("S3 must return an ETag: PutMode::Update requires one");
    assert_eq!(
        got.bytes().await.expect("v1 bytes").as_ref(),
        b"v1",
        "the rejected Create wrote anyway"
    );

    // Bodies differ on every write on purpose: an S3 single-part ETag is the
    // content's MD5, so rewriting identical bytes would leave the version
    // unchanged and make the next CAS assertion vacuous.
    let v2 = store
        .put_opts(
            &path,
            Bytes::from_static(b"v2").into(),
            put_opts(update(&v1)),
        )
        .await
        .expect("Update at the current version")
        .e_tag
        .expect("the update must return the new ETag");

    let err = store
        .put_opts(
            &path,
            Bytes::from_static(b"stale write").into(),
            put_opts(update(&v1)),
        )
        .await
        .expect_err("Update against a superseded ETag must be rejected");
    assert!(
        matches!(err, object_store::Error::Precondition { .. }),
        "stale Update: expected Precondition, got {err:?}"
    );
    assert_eq!(
        store
            .get(&path)
            .await
            .expect("reading back after the rejected update")
            .bytes()
            .await
            .expect("bytes")
            .as_ref(),
        b"v2",
        "the stale Update clobbered the winning write"
    );

    store
        .put_opts(
            &path,
            Bytes::from_static(b"v4").into(),
            put_opts(update(&v2)),
        )
        .await
        .expect("Update at the version the previous write returned");
    assert_eq!(
        store
            .get(&path)
            .await
            .expect("reading back v4")
            .bytes()
            .await
            .expect("bytes")
            .as_ref(),
        b"v4"
    );

    // `load_manifest_versioned` treats a missing manifest as "create a new
    // table"; that branch depends on NotFound arriving as its own variant.
    let err = store
        .get(&ObjPath::from("issue-9/definitely-absent.json"))
        .await
        .expect_err("a missing object must not read as empty");
    assert!(
        matches!(err, object_store::Error::NotFound { .. }),
        "missing object: expected NotFound, got {err:?}"
    );
}

/// Two writers append to one table at the same time. Both must land, and the
/// manifest must list each file exactly once.
///
/// The commit is *this* close to atomic — the base version is read and used with
/// no await in between — so whether a given run actually loses the race and
/// takes the `Precondition` path depends on MinIO's response ordering, and is not
/// observable from outside the `Ingestor`. The precondition contract itself is
/// pinned deterministically above; what this test adds is that the retry loop
/// converges on the real backend instead of losing a writer's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a live MinIO: CI-only lane, see .github/workflows/minio-s3.yml"]
async fn concurrent_commits_keep_both_writers_files() {
    const TABLE: &str = "issue-9/two-writers";
    const PER_WRITER: usize = 200;

    let store = minio();
    reset(&store, TABLE).await;
    let routing = RoutingConfig::default();
    let policy = BatchPolicy {
        max_rows: 40,
        max_bytes: usize::MAX,
    };

    let a = Ingestor::new(store.clone(), TABLE);
    let b = Ingestor::new(store.clone(), TABLE);
    let fa = a
        .write_batches(records("alpha", PER_WRITER), &routing, policy)
        .await
        .expect("writer a: encode and put");
    let fb = b
        .write_batches(records("beta", PER_WRITER), &routing, policy)
        .await
        .expect("writer b: encode and put");

    // Both writers commit from the same manifest state: this is the lost-update
    // window the CAS exists to close.
    let (ra, rb) = tokio::join!(a.commit_files(&fa), b.commit_files(&fb));
    ra.expect("writer a: commit");
    rb.expect("writer b: commit");

    let manifest = a.load_manifest().await.expect("loading the manifest");
    let paths: HashSet<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
    for f in fa.iter().chain(fb.iter()) {
        assert!(
            paths.contains(f.path.as_str()),
            "{} was dropped by a concurrent commit",
            f.path
        );
    }
    assert_eq!(
        manifest.files.len(),
        fa.len() + fb.len(),
        "the manifest must hold every file from both writers exactly once"
    );
    assert_eq!(
        manifest.total_rows(),
        (2 * PER_WRITER) as u64,
        "no rows lost across two concurrent commits"
    );
}

/// N writers, all acked, every row still readable afterwards — the MinIO
/// counterpart of `concurrent_ingests_preserve_all_rows`, which can only prove
/// this in-process.
///
/// A genuine race, and deliberately so: it is the production shape (several
/// replicas encoding and committing into one bucket) and the assertion is the
/// property that must hold under any interleaving — every acked row is present.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a live MinIO: CI-only lane, see .github/workflows/minio-s3.yml"]
async fn concurrent_writers_preserve_all_rows() {
    const TABLE: &str = "issue-9/many-writers";
    const WRITERS: usize = 4;
    const PER_WRITER: usize = 250;

    let store = minio();
    reset(&store, TABLE).await;
    let routing = RoutingConfig::default();
    let policy = BatchPolicy {
        max_rows: 25,
        max_bytes: usize::MAX,
    };

    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let store = store.clone();
        let routing = routing.clone();
        tasks.push(tokio::spawn(async move {
            Ingestor::new(store, TABLE)
                .ingest(records(&format!("svc{w}"), PER_WRITER), &routing, policy)
                .await
                .expect("concurrent ingest")
        }));
    }
    let mut written: Vec<DataFile> = Vec::new();
    for t in tasks {
        written.extend(t.await.expect("writer task panicked"));
    }

    let manifest = Ingestor::new(store.clone(), TABLE)
        .load_manifest()
        .await
        .expect("loading the manifest");
    assert_eq!(
        manifest.files.len(),
        written.len(),
        "every acked file must appear in the manifest exactly once"
    );
    assert_eq!(
        manifest.total_rows(),
        (WRITERS * PER_WRITER) as u64,
        "no acked row lost across concurrent writers"
    );

    // "Queryable" at this layer means the bytes are really there: read every
    // manifest entry back through the store and decode it. A manifest claiming
    // rows that were never stored would pass the row count above.
    let mut decoded = 0u64;
    for f in &manifest.files {
        let bytes = store
            .get(&ObjPath::from(f.path.clone()))
            .await
            .unwrap_or_else(|e| panic!("reading {}: {e}", f.path))
            .bytes()
            .await
            .unwrap_or_else(|e| panic!("reading bytes of {}: {e}", f.path));
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap_or_else(|e| panic!("opening {} as parquet: {e}", f.path))
            .build()
            .unwrap_or_else(|e| panic!("building a reader for {}: {e}", f.path));
        let rows: u64 = reader
            .map(|batch| batch.expect("decoding a parquet batch").num_rows() as u64)
            .sum();
        assert_eq!(
            rows, f.rows,
            "{}: manifest claims {} rows, the file holds {rows}",
            f.path, f.rows
        );
        decoded += rows;
    }
    assert_eq!(
        decoded,
        (WRITERS * PER_WRITER) as u64,
        "every acked row must be readable back out of the bucket"
    );
}
