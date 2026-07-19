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
/// Substring `message ILIKE '%…%'` is not something a bloom filter can answer, so
/// it is pruned instead by the trigram sets `MergeWriter` folds per row group —
/// coarser than a full inverted index (no positions, no ranking) but enough to skip
/// the row groups a term provably cannot appear in.
fn writer_props(row_group_rows: usize) -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("service"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("level"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("message"), true)
        .set_max_row_group_row_count(Some(row_group_rows))
        .build())
}

/// Rows per row group — the granularity of the per-row-group trigram index.
///
/// Pinned rather than left to arrow-rs's default (1,024×1,024) for two reasons.
/// It is the unit `MergeWriter` slices and folds trigrams against, so a default
/// that shifted under us would silently misalign index entries from the row groups
/// they describe. And it is a search-latency knob: file-level pruning stops paying
/// once compaction produces 256 MiB files, and at a million rows a group such a
/// file holds only a handful of them — too coarse to make grep interactive. 128Ki
/// keeps groups big enough to compress and scan well while giving a compacted file
/// enough of them for a rare term to skip nearly all of it.
///
/// Cost of the finer granularity is the index itself: real sets run 0.2–0.7% full,
/// so a sparse-encoded group set is well under 1 KiB, and even a few hundred groups
/// per file stay far inside the 1%-of-data-bytes ceiling (asserted in
/// `crates/ingest/tests/row_group_index.rs`).
pub const ROWS_PER_ROW_GROUP: usize = 128 * 1024;

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
    /// Row groups emitted. Always equals `row_group_trigrams.len()` — they are
    /// derived from the same cuts — and is carried separately because it goes to
    /// the manifest while the sets go to the sidecar.
    pub row_groups: u64,
    /// The same, one entry per row group in row-group order, so a file that
    /// survives plan-time pruning can still skip most of its own row groups.
    /// `message_trigrams` is exactly the union of these.
    pub row_group_trigrams: Vec<TrigramSet>,
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
    /// Trigrams of the row group currently open. Folded into `row_group_trigrams`
    /// at each cut; the file-level set is their union, taken in `finish`.
    group_trigrams: TrigramSet,
    /// Rows written into the currently open row group.
    rows_in_group: usize,
    /// Rows per row group. [`ROWS_PER_ROW_GROUP`] in production; overridable so a
    /// test can produce many row groups from few rows.
    row_group_rows: usize,
    row_group_trigrams: Vec<TrigramSet>,
}

impl MergeWriter {
    pub fn new(schema: SchemaRef) -> Result<Self> {
        Self::with_row_group_rows(schema, ROWS_PER_ROW_GROUP)
    }

    /// A writer cutting row groups every `row_group_rows` rows.
    ///
    /// Exists for tests: exercising multi-row-group behaviour at the production
    /// 128Ki would mean encoding hundreds of thousands of rows per case, so the
    /// index tests would be slow enough not to run. The cut/fold logic is the same
    /// at any size — only the constant differs — so a test at 512 rows covers the
    /// same code the production path takes.
    pub fn with_row_group_rows(schema: SchemaRef, row_group_rows: usize) -> Result<Self> {
        anyhow::ensure!(row_group_rows > 0, "row group size must be positive");
        Ok(Self {
            writer: ArrowWriter::try_new(Vec::new(), schema, Some(writer_props(row_group_rows)?))
                .context("creating parquet merge writer")?,
            rows: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            services: BTreeSet::new(),
            levels: BTreeSet::new(),
            group_trigrams: TrigramSet::new(),
            rows_in_group: 0,
            row_group_rows,
            row_group_trigrams: Vec::new(),
        })
    }

    /// Close the open row group and bank its trigram set.
    ///
    /// `ArrowWriter::flush` is a no-op when nothing is buffered, which is what
    /// makes this safe to call unconditionally: the writer auto-flushes on
    /// reaching `row_group_rows` too, so at a cut point the group may already
    /// be closed. Either way exactly one row group has been emitted for the rows
    /// folded into `group_trigrams`, which is the invariant the index rests on.
    fn cut_row_group(&mut self) -> Result<()> {
        if self.rows_in_group == 0 {
            return Ok(());
        }
        self.writer.flush().context("flushing row group")?;
        self.row_group_trigrams.push(std::mem::replace(
            &mut self.group_trigrams,
            TrigramSet::new(),
        ));
        self.rows_in_group = 0;
        Ok(())
    }

    /// Write one batch and fold its stats.
    ///
    /// Stats come from the rows actually merged rather than being inherited from
    /// the inputs' manifest entries, so a compacted file still prunes by
    /// service/level/trigram even when some input predates those stats.
    ///
    /// The batch is sliced at row-group boundaries and each slice folded before it
    /// is written, so a trigram set never spans a cut. Doing it any other way — say
    /// folding the whole batch then letting the writer split it — would attribute a
    /// slice's trigrams to whichever group happened to be open, and a row group
    /// whose recorded set is missing a term its rows actually contain is a false
    /// negative: the scan would skip a real match.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let total = batch.num_rows();
        let mut offset = 0;
        while offset < total {
            let room = self.row_group_rows - self.rows_in_group;
            let take = room.min(total - offset);
            let slice = batch.slice(offset, take);

            self.rows += take as u64;
            if let Some(col) = slice.column_by_name("ts") {
                if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.min_ts = self.min_ts.min(arr.value(i));
                            self.max_ts = self.max_ts.max(arr.value(i));
                        }
                    }
                }
            }
            fold_distinct(&slice, "service", &mut self.services);
            fold_distinct(&slice, "level", &mut self.levels);
            if let Some(col) = slice.column_by_name("message") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.group_trigrams.insert_text(arr.value(i));
                        }
                    }
                }
            }
            self.writer.write(&slice).context("writing record batch")?;

            self.rows_in_group += take;
            offset += take;
            if self.rows_in_group == self.row_group_rows {
                self.cut_row_group()?;
            }
        }
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
    pub fn finish(mut self) -> Result<(Vec<u8>, FileStats)> {
        // Bank the trailing partial group before the writer closes it for us, so
        // the index has an entry for every row group the file ends up with.
        self.cut_row_group()?;
        let Self {
            writer,
            rows,
            min_ts,
            max_ts,
            services,
            levels,
            row_group_trigrams,
            ..
        } = self;
        let buf = writer
            .into_inner()
            .context("closing parquet merge writer")?;
        let mut trigrams = TrigramSet::new();
        for g in &row_group_trigrams {
            trigrams.union_with(g);
        }
        let stats = FileStats {
            rows,
            // An empty merge has no timestamps; don't leak the i64 sentinels into
            // the manifest, where they'd read as a file spanning all of time.
            min_ts: if rows == 0 { 0 } else { min_ts },
            max_ts: if rows == 0 { 0 } else { max_ts },
            services: services.into_iter().collect(),
            levels: levels.into_iter().collect(),
            message_trigrams: trigrams,
            row_groups: row_group_trigrams.len() as u64,
            row_group_trigrams,
        };
        Ok((buf, stats))
    }
}

/// Build the Arrow batch for `records`, in the table's schema.
///
/// Split out from [`encode_parquet`] so a caller that needs a differently
/// configured writer — the row-group index tests, which cut groups far smaller than
/// production — builds its rows through the same code the production path uses,
/// rather than a lookalike that could drift from the real schema.
pub fn records_to_batch(records: &[LogRecord]) -> Result<RecordBatch> {
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

    RecordBatch::try_new(
        log_schema(),
        vec![ts, level, service, status, message, trace_id, attrs_json],
    )
    .context("building record batch")
}

/// Encode `records` to Parquet bytes (zstd-compressed). Errors on an empty batch
/// — callers roll non-empty files only.
pub fn encode_parquet(records: &[LogRecord]) -> Result<(Vec<u8>, FileStats)> {
    anyhow::ensure!(!records.is_empty(), "refusing to encode an empty batch");
    let batch = records_to_batch(records)?;

    // Through `MergeWriter` rather than a second hand-rolled writer + stats fold.
    // The stats it produces — distinct service/level for plan-time file skip, the
    // message trigrams for free-text skip, and the per-row-group sets — are then
    // the same code on both write paths, so an ingested file and a compacted one
    // cannot come to disagree about what a stat means. (That mattered here: a
    // second fold would also have had to re-derive row-group boundaries, and
    // getting those subtly wrong is a false negative, not a slow query.)
    let mut w = MergeWriter::new(log_schema())?;
    w.write(&batch)?;
    w.finish()
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
