//! Benchmark-only comparison with a single versioned bundle of per-file summaries.
use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use chrono::NaiveDateTime;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use verdigris_core::config::Config;
use verdigris_core::manifest::DataFile;
use verdigris_ingest::Ingestor;
use verdigris_query::engine::{query_table_json, QueryLimits};
use verdigris_query::index::IndexedFile;

pub const TABLE: &str = "bench";
pub const KEY: &str = "bench/_metadata/benchmark-minute-summary-v1.json";
pub const SQL: &str = "SELECT date_bin(INTERVAL '60 seconds', ts, TIMESTAMP '1970-01-01T00:00:00') AS bucket, count(*) AS total, count(*) FILTER (WHERE level = 'ERROR') AS errors FROM bench GROUP BY bucket ORDER BY bucket";
const VERSION: u32 = 1;
const MAX_FILES: usize = 64;
const MAX_BUNDLE_BYTES: u64 = 64 * 1024;
const MINUTE_MS: i64 = 60_000;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bucket {
    bucket: String,
    total: u64,
    errors: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSummary {
    path: String,
    parquet_bytes: u64,
    rows: u64,
    min_ts: i64,
    max_ts: i64,
    buckets: Vec<Bucket>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bundle {
    version: u32,
    checksum: String,
    compaction_gen: u64,
    files: Vec<FileSummary>,
}

fn checksum(files: &[FileSummary]) -> serde_json::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(files)?)))
}

fn byte_budget(files: &[&DataFile]) -> Option<(u64, u64)> {
    if files.is_empty() || files.len() > MAX_FILES || files.iter().any(|f| !f.is_immutable_key()) {
        return None;
    }
    let active_bytes = files
        .iter()
        .try_fold(0u64, |bytes, f| bytes.checked_add(f.bytes))?;
    Some((active_bytes, (active_bytes / 100).min(MAX_BUNDLE_BYTES)))
}

fn merge(bundle: &Bundle, files: &[&DataFile], limits: &QueryLimits) -> Option<Vec<Value>> {
    if bundle.version != VERSION
        || bundle.files.len() != files.len()
        || checksum(&bundle.files).ok()? != bundle.checksum
    {
        return None;
    }
    let by_path: BTreeMap<_, _> = bundle
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    if by_path.len() != files.len() {
        return None;
    }
    let mut merged = BTreeMap::<i64, (&str, u64, u64)>::new();
    for file in files {
        let summary = by_path.get(file.path.as_str())?;
        if summary.parquet_bytes != file.bytes
            || summary.rows != file.rows
            || summary.min_ts != file.min_ts
            || summary.max_ts != file.max_ts
        {
            return None;
        }
        let mut previous = None;
        let mut total = 0u64;
        for bucket in &summary.buckets {
            let time = NaiveDateTime::parse_from_str(&bucket.bucket, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()?
                .and_utc();
            let millis = time.timestamp_millis();
            if time.timestamp_subsec_nanos() != 0
                || millis.rem_euclid(MINUTE_MS) != 0
                || millis.div_euclid(MINUTE_MS) < file.min_ts.div_euclid(MINUTE_MS)
                || millis.div_euclid(MINUTE_MS) > file.max_ts.div_euclid(MINUTE_MS)
                || previous.is_some_and(|last| last >= millis)
                || bucket.total == 0
                || bucket.errors > bucket.total
            {
                return None;
            }
            previous = Some(millis);
            total = total.checked_add(bucket.total)?;
            let (wire, total, errors) = merged.entry(millis).or_insert((&bucket.bucket, 0, 0));
            if *wire != bucket.bucket {
                return None;
            }
            *total = total.checked_add(bucket.total)?;
            *errors = errors.checked_add(bucket.errors)?;
        }
        if total != file.rows {
            return None;
        }
    }
    if merged.len() as u64 > limits.max_result_rows {
        return None;
    }
    let rows = merged
        .into_values()
        .map(
            |(bucket, total, errors)| json!({ "bucket": bucket, "total": total, "errors": errors }),
        )
        .collect::<Vec<_>>();
    if serde_json::to_vec(&rows).ok()?.len() as u64 > limits.max_result_bytes {
        return None;
    }
    Some(rows)
}

pub async fn read(
    store: &Arc<dyn ObjectStore>,
    files: &[&DataFile],
    compaction_gen: u64,
    limits: &QueryLimits,
) -> Option<(Vec<Value>, u64)> {
    let (_, budget) = byte_budget(files)?;
    let object = store.get(&Path::from(KEY)).await.ok()?;
    if object.meta.size > budget {
        return None;
    }
    let bytes = object.bytes().await.ok()?;
    if bytes.len() as u64 > budget {
        return None;
    }
    let bundle: Bundle = serde_json::from_slice(&bytes).ok()?;
    if bundle.compaction_gen != compaction_gen {
        return None;
    }
    Some((merge(&bundle, files, limits)?, bytes.len() as u64))
}

pub async fn build(cfg: &Config) -> anyhow::Result<()> {
    let store = verdigris_storage::build(&cfg.storage)?;
    let ingestor = Ingestor::new(store.clone(), TABLE);
    let manifest = ingestor.load_manifest().await?;
    let files = manifest.files.iter().collect::<Vec<_>>();
    let (active_bytes, budget) =
        byte_budget(&files).context("fixture is ineligible for a bundle")?;
    let limits = QueryLimits::from_config(&cfg.query);
    let mut file_summaries = Vec::new();
    for file in &files {
        let rows = query_table_json(
            store.clone(),
            TABLE,
            &[IndexedFile::whole(file)],
            SQL,
            &limits,
        )
        .await
        .with_context(|| format!("aggregating {}", file.path))?;
        let buckets = rows
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<_, _>>()?;
        file_summaries.push(FileSummary {
            path: file.path.clone(),
            parquet_bytes: file.bytes,
            rows: file.rows,
            min_ts: file.min_ts,
            max_ts: file.max_ts,
            buckets,
        });
    }
    let bundle = Bundle {
        version: VERSION,
        checksum: checksum(&file_summaries)?,
        compaction_gen: manifest.compaction_gen,
        files: file_summaries,
    };
    merge(&bundle, &files, &limits).context("bundle failed snapshot and row-count validation")?;
    let encoded = serde_json::to_vec(&bundle)?;
    anyhow::ensure!(
        encoded.len() as u64 <= budget,
        "summary exceeds the auxiliary byte budget"
    );
    let size = encoded.len();
    store.put(&Path::from(KEY), encoded.into()).await?;
    println!(
        "{}",
        json!({ "sidecar_bytes": size, "active_parquet_bytes": active_bytes,
                            "files": files.len(), "rows": manifest.total_rows() })
    );
    Ok(())
}
