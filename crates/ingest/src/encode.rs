//! Encode a batch of records into a single Parquet file (in memory).
//!
//! The output is a `Vec<u8>` of Parquet bytes plus the per-file stats the
//! manifest needs. This is deterministic and does no I/O — the bytes are handed
//! to the object-store seam by the `Ingestor`.

use crate::schema::log_schema;
use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use verdigris_core::batch::LogRecord;

fn zstd_props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
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
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(zstd_props()?))
            .context("creating parquet writer")?;
        for batch in batches {
            writer.write(batch).context("writing record batch")?;
        }
        writer.close().context("closing parquet writer")?;
    }
    Ok(buf)
}

/// Per-file statistics recorded in the manifest for planning/pruning/pricing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileStats {
    pub rows: u64,
    pub min_ts: i64,
    pub max_ts: i64,
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

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
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

    let stats = FileStats {
        rows: records.len() as u64,
        min_ts,
        max_ts,
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
    }

    #[test]
    fn empty_batch_is_rejected() {
        assert!(encode_parquet(&[]).is_err());
    }
}
