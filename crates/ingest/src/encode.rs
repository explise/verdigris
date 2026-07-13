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
use std::collections::BTreeSet;
use std::sync::Arc;
use verdigris_core::batch::LogRecord;
use verdigris_core::text::TrigramSet;

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

/// Rows per decoded batch when streaming a source file through compaction.
/// Caps the live Arrow batch regardless of how large the source file's row
/// groups are, so one pathologically wide row group can't blow the bound.
const MERGE_BATCH_ROWS: usize = 8192;

/// Stream every record batch of a Parquet file's bytes into `w`, holding at most
/// one decompressed batch in memory at a time. Compaction's memory bound rests
/// on this: a bin is fed through file by file, batch by batch, and never
/// materialized as a whole.
pub fn stream_parquet_into(bytes: Bytes, w: &mut MergeWriter) -> Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .context("opening parquet")?
        .with_batch_size(MERGE_BATCH_ROWS)
        .build()
        .context("building parquet reader")?;
    for batch in reader {
        w.write(&batch.context("reading record batch")?)?;
    }
    Ok(())
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
    /// Trigram presence set over the `message` column — plan-time file skip for
    /// free-text search (see `verdigris_core::text`).
    pub message_trigrams: TrigramSet,
}

/// Fold a Utf8 column's distinct non-null values into `out` (sorted+deduped by
/// the `BTreeSet` — deterministic, no HashMap order). Only allocates on a value
/// not already seen, so a low-cardinality column like `level` costs ~nothing per
/// row.
fn fold_distinct(batch: &RecordBatch, column: &str, out: &mut BTreeSet<String>) {
    let Some(col) = batch.column_by_name(column) else {
        return;
    };
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        for i in 0..arr.len() {
            if arr.is_valid(i) && !out.contains(arr.value(i)) {
                out.insert(arr.value(i).to_string());
            }
        }
    }
}

/// A streaming merge target: compaction's replacement for "read the whole bin
/// into `Vec<RecordBatch>`, then re-encode it".
///
/// The old shape made peak memory one bin's *decompressed* size — a 256 MiB bin
/// of zstd Parquet expands to ~1 GB of Arrow — plus the re-encode buffer, which
/// is how a 1.3 GB backlog reached ~4 GB RSS. Here, batches are written straight
/// through to the Parquet encoder and the manifest stats are folded in as they
/// pass, so nothing needs to see the bin as a whole and at most one batch is
/// live at a time.
///
/// What remains in memory is the encoder's in-progress row group plus the output
/// buffer (the encoded file, ~`target_bytes`, since `object_store::put` wants
/// contiguous bytes). Streaming the output too would need multipart upload.
pub struct MergeWriter {
    writer: ArrowWriter<Vec<u8>>,
    rows: u64,
    min_ts: i64,
    max_ts: i64,
    services: BTreeSet<String>,
    levels: BTreeSet<String>,
    trigrams: TrigramSet,
}

impl MergeWriter {
    pub fn new(schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            writer: ArrowWriter::try_new(Vec::new(), schema, Some(writer_props()?))
                .context("creating parquet merge writer")?,
            rows: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            services: BTreeSet::new(),
            levels: BTreeSet::new(),
            trigrams: TrigramSet::new(),
        })
    }

    /// Write one batch and fold its stats.
    ///
    /// Stats come from the rows actually merged rather than being inherited from
    /// the inputs' manifest entries, so a compacted file still prunes by
    /// service/level/trigram even when some input predates those stats.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows += batch.num_rows() as u64;
        if let Some(col) = batch.column_by_name("ts") {
            if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        self.min_ts = self.min_ts.min(arr.value(i));
                        self.max_ts = self.max_ts.max(arr.value(i));
                    }
                }
            }
        }
        fold_distinct(batch, "service", &mut self.services);
        fold_distinct(batch, "level", &mut self.levels);
        if let Some(col) = batch.column_by_name("message") {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        self.trigrams.insert_text(arr.value(i));
                    }
                }
            }
        }
        self.writer.write(batch).context("writing record batch")?;
        Ok(())
    }

    /// Bytes buffered in the encoder for the row group still being built — the
    /// writer's own live footprint, so the caller can report what a merge is
    /// actually costing rather than guessing from `target_bytes`.
    pub fn in_progress_size(&self) -> usize {
        self.writer.in_progress_size()
    }

    /// Finish the file: the encoded Parquet bytes plus the stats folded from the
    /// rows written.
    pub fn finish(self) -> Result<(Vec<u8>, FileStats)> {
        let Self {
            writer,
            rows,
            min_ts,
            max_ts,
            services,
            levels,
            trigrams,
        } = self;
        let buf = writer
            .into_inner()
            .context("closing parquet merge writer")?;
        let stats = FileStats {
            rows,
            // An empty merge has no timestamps; don't leak the i64 sentinels into
            // the manifest, where they'd read as a file spanning all of time.
            min_ts: if rows == 0 { 0 } else { min_ts },
            max_ts: if rows == 0 { 0 } else { max_ts },
            services: services.into_iter().collect(),
            levels: levels.into_iter().collect(),
            message_trigrams: trigrams,
        };
        Ok((buf, stats))
    }
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
    // via BTreeSet — deterministic, no RNG/HashMap order), plus the message
    // trigram set so free-text searches can skip the file the same way.
    let mut svc = std::collections::BTreeSet::new();
    let mut lvl = std::collections::BTreeSet::new();
    let mut trigrams = TrigramSet::new();
    for r in records {
        svc.insert(r.service.clone());
        lvl.insert(r.level.as_str().to_string());
        trigrams.insert_text(&r.message);
    }

    let stats = FileStats {
        rows: records.len() as u64,
        min_ts,
        max_ts,
        services: svc.into_iter().collect(),
        levels: lvl.into_iter().collect(),
        message_trigrams: trigrams,
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
            assert!(
                bloomed.contains(&col.to_string()),
                "{col} needs a bloom filter; got {bloomed:?}"
            );
        }
        // …but the time/int columns (pruned by min/max stats) do not.
        assert!(!bloomed.contains(&"ts".to_string()));
        assert!(!bloomed.contains(&"status".to_string()));
    }
}
