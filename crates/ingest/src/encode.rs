//! Encode a batch of records into a single Parquet file (in memory).
//!
//! The output is a `Vec<u8>` of Parquet bytes plus the per-file stats the
//! manifest needs. This is deterministic and does no I/O — the bytes are handed
//! to the object-store seam by the `Ingestor`.

use crate::schema::log_schema;
use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, Int32Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::sync::Arc;
use verdigris_core::batch::LogRecord;

/// Parquet writer settings shared by ingest and compaction: zstd + **bloom
/// filters** on the string lookup columns. A bloom filter lets the reader skip
/// whole row groups that can't contain an equality match — the fast path for
/// "find this `trace_id`", "this `service`'s errors", "`level = 'ERROR'`" — so a
/// rare-value lookup reads a handful of row groups instead of every row.
/// (Substring `message ILIKE '%…%'` still scans; an inverted index for arbitrary
/// grep is future work — M1.2 stretch.)
fn writer_props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("service"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("level"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("message"), true)
        .build())
}

/// Read all record batches out of a Parquet file's bytes (used by compaction).
pub fn read_parquet_bytes(bytes: Bytes) -> Result<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .context("opening parquet")?
        .build()
        .context("building parquet reader")?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.context("reading record batch")?);
    }
    Ok(out)
}

/// Re-encode already-Arrow record batches to a single Parquet file (compaction).
pub fn encode_record_batches(schema: SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_props()?))
            .context("creating parquet writer")?;
        for batch in batches {
            writer.write(batch).context("writing record batch")?;
        }
        writer.close().context("closing parquet writer")?;
    }
    Ok(buf)
}

/// Per-file statistics recorded in the manifest for planning/pruning/pricing.
#[derive(Debug, Clone, PartialEq)]
pub struct FileStats {
    pub rows: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    /// Distinct `service` values in the file (sorted, deduped) — plan-time file skip.
    pub services: Vec<String>,
    /// Distinct `level` values in the file (sorted, deduped) — plan-time file skip.
    pub levels: Vec<String>,
}

/// Distinct non-null values of a Utf8 column across `batches` (sorted, deduped).
/// Used to record the per-file `service`/`level` stats the planner prunes on —
/// authoritative because it reads the actual rows (so compaction recomputes it
/// from merged data rather than trusting inputs that may predate value stats).
pub fn distinct_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut set = std::collections::BTreeSet::new();
    for batch in batches {
        let Some(col) = batch.column_by_name(column) else { continue };
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    set.insert(arr.value(i).to_string());
                }
            }
        }
    }
    set.into_iter().collect()
}

/// Encode `records` to Parquet bytes (zstd-compressed). Errors on an empty batch
/// — callers roll non-empty files only.
pub fn encode_parquet(records: &[LogRecord]) -> Result<(Vec<u8>, FileStats)> {
    anyhow::ensure!(!records.is_empty(), "refusing to encode an empty batch");

    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values(
        records.iter().map(|r| r.ts_millis),
    ));
    let level: ArrayRef = Arc::new(StringArray::from_iter_values(
        records.iter().map(|r| r.level.as_str()),
    ));
    let service: ArrayRef = Arc::new(StringArray::from_iter_values(
        records.iter().map(|r| r.service.as_str()),
    ));
    let status: ArrayRef = Arc::new(Int32Array::from_iter(records.iter().map(|r| r.status)));
    let message: ArrayRef = Arc::new(StringArray::from_iter_values(
        records.iter().map(|r| r.message.as_str()),
    ));
    let trace_id: ArrayRef = Arc::new(StringArray::from_iter(
        records.iter().map(|r| r.trace_id.clone()),
    ));
    let attrs_json: ArrayRef = Arc::new(StringArray::from_iter(records.iter().map(|r| {
        if r.attrs.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&r.attrs).unwrap_or_default())
        }
    })));

    let schema = log_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![ts, level, service, status, message, trace_id, attrs_json],
    )
    .context("building record batch")?;

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_props()?))
            .context("creating parquet writer")?;
        writer.write(&batch).context("writing record batch")?;
        writer.close().context("closing parquet writer")?;
    }

    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for r in records {
        min_ts = min_ts.min(r.ts_millis);
        max_ts = max_ts.max(r.ts_millis);
    }

    // Distinct service/level values, so a `service:auth` / `level:error` query can
    // skip this whole file at plan time when the value is absent (sorted+deduped
    // via BTreeSet — deterministic, no RNG/HashMap order).
    let mut svc = std::collections::BTreeSet::new();
    let mut lvl = std::collections::BTreeSet::new();
    for r in records {
        svc.insert(r.service.clone());
        lvl.insert(r.level.as_str().to_string());
    }

    let stats = FileStats {
        rows: records.len() as u64,
        min_ts,
        max_ts,
        services: svc.into_iter().collect(),
        levels: lvl.into_iter().collect(),
    };
    Ok((buf, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use verdigris_core::model::Level;

    fn rec(ts: i64) -> LogRecord {
        LogRecord {
            ts_millis: ts,
            level: Level::Error,
            service: "auth".into(),
            status: Some(503),
            message: "boom".into(),
            trace_id: Some("4ac9d21".into()),
            attrs: BTreeMap::from([("region".into(), "us-east-1".into())]),
        }
    }

    #[test]
    fn encodes_nonempty_and_reports_stats() {
        let (bytes, stats) = encode_parquet(&[rec(30), rec(10), rec(20)]).unwrap();
        assert!(bytes.starts_with(b"PAR1")); // parquet magic
        assert_eq!(stats.rows, 3);
        assert_eq!(stats.min_ts, 10);
        assert_eq!(stats.max_ts, 30);
        // All three sample rows are service=auth level=ERROR → one distinct each.
        assert_eq!(stats.services, vec!["auth".to_string()]);
        assert_eq!(stats.levels, vec!["ERROR".to_string()]);
    }

    #[test]
    fn empty_batch_is_rejected() {
        assert!(encode_parquet(&[]).is_err());
    }

    #[test]
    fn writes_bloom_filters_on_lookup_columns() {
        let (bytes, _) = encode_parquet(&[rec(10), rec(20)]).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        let rg = builder.metadata().row_group(0);
        let bloomed: Vec<String> = (0..rg.num_columns())
            .filter(|&i| rg.column(i).bloom_filter_offset().is_some())
            .map(|i| rg.column(i).column_path().string())
            .collect();
        // The string lookup columns carry bloom filters (fast equality pruning)…
        for col in ["trace_id", "service", "level", "message"] {
            assert!(bloomed.contains(&col.to_string()), "{col} needs a bloom filter; got {bloomed:?}");
        }
        // …but the time/int columns (pruned by min/max stats) do not.
        assert!(!bloomed.contains(&"ts".to_string()));
        assert!(!bloomed.contains(&"status".to_string()));
    }
}
