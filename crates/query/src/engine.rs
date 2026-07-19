//! Real query execution via DataFusion — "query in place" (build step 2).
//!
//! Parquet is read straight from the object store; there is no rehydration step.
//! We register our `object_store` under a synthetic `verdigris://` URL, point a
//! `ListingTable` at the table's prefix, and run SQL. The same code path works
//! against local fs, in-memory, or S3/MinIO because it only ever talks to the
//! `ObjectStore` seam.
//!
//! Note: DataFusion brings its own tokio tasks and CPU thread pool. Forcing a
//! single partition for deterministic in-sim execution (the ADR-001 question) is
//! a later experiment; this module just proves real in-place reads work.

use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::StreamExt;
use object_store::ObjectStore;
use verdigris_core::config::QueryConfig;

use crate::index::{IndexedFile, IndexedParquetTable};

/// The result of a SQL query (CLI/pretty path).
pub struct QueryOutput {
    /// Total rows returned.
    pub rows: usize,
    /// A pretty-printed table of the result, ready to display.
    pub pretty: String,
}

/// How many of the largest memory consumers to name when execution runs out of
/// pool. Turns "resources exhausted" into something an operator can act on.
const TRACKED_CONSUMERS: usize = 5;

/// DataFusion's own default up-front reservation per external sort merge (10 MB).
/// We never exceed it; we only shrink it to fit a small pool. See
/// [`sort_spill_reservation`].
const DEFAULT_SORT_SPILL_RESERVATION: usize = 10 * 1024 * 1024;

/// The slab an external sort reserves up front, before it can spill anything.
///
/// DataFusion's 10 MB default is *larger than a small pool*, so a sort under (say)
/// a 64 MiB budget can fail to allocate before it has spilled a single byte — the
/// memory limit would then break ordinary queries instead of merely slowing them.
/// Scaling the reservation to a fraction of the pool keeps `memory_pool_mib`
/// usable across its whole range rather than only at large values.
fn sort_spill_reservation(pool_bytes: usize) -> usize {
    (pool_bytes / 8).min(DEFAULT_SORT_SPILL_RESERVATION)
}

/// The memory ceilings a query runs under (issue #2). Built from [`QueryConfig`];
/// [`QueryLimits::default`] mirrors the config defaults so tests and the CLI don't
/// have to construct a whole `Config`.
#[derive(Debug, Clone)]
pub struct QueryLimits {
    /// Ceiling on the DataFusion execution memory pool. Operators spill to disk
    /// rather than exceed it.
    pub memory_pool_bytes: usize,
    /// Cap on the accumulated result set — the pool does not cover this, since a
    /// non-aggregating `SELECT *` never asks the pool for anything.
    pub max_result_rows: u64,
    pub max_result_bytes: u64,
    /// Target partitions for execution; each carries its own buffers.
    pub target_partitions: usize,
}

impl QueryLimits {
    pub fn from_config(q: &QueryConfig) -> Self {
        Self {
            memory_pool_bytes: (q.memory_pool_mib as usize).saturating_mul(1024 * 1024),
            max_result_rows: q.max_result_rows,
            max_result_bytes: q.max_result_mib.saturating_mul(1024 * 1024),
            target_partitions: (q.cores as usize).max(1),
        }
    }
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self::from_config(&QueryConfig::default())
    }
}

/// The result set outgrew [`QueryLimits`] before it could be returned.
///
/// A distinct type rather than a bare `anyhow!` so the HTTP layer can answer 413
/// instead of 500: this is the client asking for too much, not the server
/// breaking. Carries the limit that tripped so the message can say what to do
/// (`LIMIT`, narrow the window, or raise the knob).
#[derive(Debug)]
pub struct ResultTooLarge {
    pub rows: u64,
    pub bytes: u64,
    pub max_rows: u64,
    pub max_bytes: u64,
}

impl std::fmt::Display for ResultTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "result set too large: {} rows / {:.1} MiB exceeds the limit of {} rows / {:.1} MiB. \
             Add a LIMIT, narrow the time range, or raise query.max_result_rows / \
             query.max_result_mib.",
            self.rows,
            self.bytes as f64 / (1024.0 * 1024.0),
            self.max_rows,
            self.max_bytes as f64 / (1024.0 * 1024.0),
        )
    }
}

impl std::error::Error for ResultTooLarge {}

/// Plan and execute `sql` against `table`, reading the exact Parquet `files` in
/// place from `store`. `files` are object-store keys from the manifest (our
/// catalog) — we register those rather than directory-scanning, so the planner
/// sees exactly what the catalog says, on any backend.
async fn collect_batches(
    store: Arc<dyn ObjectStore>,
    table: &str,
    files: &[IndexedFile],
    sql: &str,
    limits: &QueryLimits,
) -> anyhow::Result<Vec<RecordBatch>> {
    anyhow::ensure!(!files.is_empty(), "table '{table}' has no files to query");

    // Turn on bloom-filter row-group pruning and predicate pushdown so an
    // equality lookup (`trace_id = '…'`, `service = 'auth'`) reads only the row
    // groups whose bloom filter admits the value, instead of every row.
    let mut cfg = SessionConfig::new().with_target_partitions(limits.target_partitions);
    {
        let opts = cfg.options_mut();
        opts.execution.parquet.bloom_filter_on_read = true;
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
        opts.execution.sort_spill_reservation_bytes =
            sort_spill_reservation(limits.memory_pool_bytes);
    }

    // Bound *execution* memory. FairSpillPool divides the budget across the
    // spillable operators (sort, aggregate, join) and makes them spill to disk
    // rather than grow without limit, so a big GROUP BY degrades to slower
    // instead of killing the box. TrackConsumersPool wraps it so that if the pool
    // genuinely can't be satisfied, the error names the biggest consumers instead
    // of just saying "resources exhausted".
    //
    // The default DiskManager (an OS temp dir) is what the spill actually lands
    // in; it is left alone deliberately.
    let pool = TrackConsumersPool::new(
        FairSpillPool::new(limits.memory_pool_bytes),
        NonZeroUsize::new(TRACKED_CONSUMERS).expect("nonzero"),
    );
    let rt = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(pool))
        .build_arc()
        .context("building bounded runtime env")?;
    let ctx = SessionContext::new_with_config_rt(cfg, rt);

    // Register our object store under a synthetic scheme/authority.
    let base = ObjectStoreUrl::parse("verdigris://store").context("parsing object store url")?;
    ctx.register_object_store(base.as_ref(), store);

    // Register the exact files from the manifest as the table's paths.
    //
    // The schema still comes from `ListingTable`'s inference over those paths —
    // one read of one footer, and it keeps schema handling identical to before —
    // but the table that gets registered is [`IndexedParquetTable`], so each file
    // can carry the row-group access plan its trigram index implies.
    let urls = files
        .iter()
        .map(|f| ListingTableUrl::parse(format!("verdigris://store/{}", f.path)))
        .collect::<Result<Vec<_>, _>>()
        .context("parsing file urls")?;
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .context("inferring schema from parquet")?;
    let schema = ListingTable::try_new(config)
        .context("creating listing table")?
        .schema();
    let provider = IndexedParquetTable::new(schema, base, files.to_vec());
    ctx.register_table(table, Arc::new(provider))
        .context("registering table")?;

    let df = ctx.sql(sql).await.context("planning sql")?;
    collect_bounded(df, limits).await
}

/// Drain a plan's output stream, enforcing the result-set ceiling as it goes.
///
/// The point is that this *streams*: `DataFrame::collect` materializes every
/// batch and only then hands them over, so an oversized `SELECT *` is already in
/// RAM by the time anyone could object. Here the running totals are checked per
/// batch, so a runaway query is refused after roughly one batch past the limit
/// rather than after all of it.
///
/// `get_array_memory_size` is the batch's true Arrow footprint (buffers included),
/// which is what actually occupies the box — not the encoded-Parquet size.
async fn collect_bounded(df: DataFrame, limits: &QueryLimits) -> anyhow::Result<Vec<RecordBatch>> {
    let mut stream = df.execute_stream().await.context("executing sql")?;
    let mut out = Vec::new();
    let mut rows: u64 = 0;
    let mut bytes: u64 = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch.context("reading result batch")?;
        rows += batch.num_rows() as u64;
        bytes += batch.get_array_memory_size() as u64;
        if rows > limits.max_result_rows || bytes > limits.max_result_bytes {
            // Drop what we have before returning: the caller gets an error, and
            // holding a half-built oversized result while it unwinds is the exact
            // thing we're trying to avoid.
            drop(out);
            return Err(anyhow::Error::new(ResultTooLarge {
                rows,
                bytes,
                max_rows: limits.max_result_rows,
                max_bytes: limits.max_result_bytes,
            }));
        }
        out.push(batch);
    }
    Ok(out)
}

/// Run a query and return a pretty-printed table (for the CLI).
pub async fn query_table(
    store: Arc<dyn ObjectStore>,
    table: &str,
    files: &[IndexedFile],
    sql: &str,
    limits: &QueryLimits,
) -> anyhow::Result<QueryOutput> {
    let batches = collect_batches(store, table, files, sql, limits).await?;
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    let pretty = pretty_format_batches(&batches)
        .context("formatting results")?
        .to_string();
    Ok(QueryOutput { rows, pretty })
}

/// Run a query and return the rows as JSON objects (for the HTTP API).
pub async fn query_table_json(
    store: Arc<dyn ObjectStore>,
    table: &str,
    files: &[IndexedFile],
    sql: &str,
    limits: &QueryLimits,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let batches = collect_batches(store, table, files, sql, limits).await?;
    let mut buf = Vec::new();
    {
        let mut writer = ArrayWriter::new(&mut buf);
        for b in &batches {
            writer.write(b).context("json-encoding batch")?;
        }
        writer.finish().context("finishing json")?;
    }
    if buf.is_empty() {
        return Ok(vec![]);
    }
    serde_json::from_slice(&buf).context("parsing json rows")
}

/// Run a query and return the rows encoded as an **Arrow IPC stream** — the
/// columnar wire the UI can decode near-zero-copy (vs parsing millions of JSON
/// objects). Same in-place read path as [`query_table_json`]; only the output
/// encoding differs. An empty result set yields an empty buffer (no rows), which
/// the client treats as zero matches.
pub async fn query_table_arrow(
    store: Arc<dyn ObjectStore>,
    table: &str,
    files: &[IndexedFile],
    sql: &str,
    limits: &QueryLimits,
) -> anyhow::Result<Vec<u8>> {
    let batches = collect_batches(store, table, files, sql, limits).await?;
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    // DataFusion 54's Parquet reader yields Arrow `Utf8View`/`BinaryView` columns,
    // a type too new for many IPC decoders (e.g. apache-arrow JS in the web UI),
    // which reject it outright. Cast view columns down to plain `Utf8`/`Binary` so
    // the Arrow wire is broadly decodable. (The JSON path is unaffected — it
    // serializes views to strings fine.)
    let batches = batches
        .iter()
        .map(deview_batch)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, schema.as_ref()).context("arrow stream writer")?;
        for b in &batches {
            writer.write(b).context("arrow-encoding batch")?;
        }
        writer.finish().context("finishing arrow stream")?;
    }
    Ok(buf)
}

/// Cast any `Utf8View`/`BinaryView` columns of a batch down to `Utf8`/`Binary`.
/// Other columns pass through untouched. Returns the batch unchanged if it has no
/// view columns (the common case once Parquet stores non-view types).
fn deview_batch(batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let target = match field.data_type() {
            DataType::Utf8View => Some(DataType::Utf8),
            DataType::BinaryView => Some(DataType::Binary),
            _ => None,
        };
        match target {
            Some(dt) => {
                changed = true;
                columns.push(cast(batch.column(i), &dt).context("casting view column")?);
                fields.push(Field::new(field.name(), dt, field.is_nullable()));
            }
            None => {
                columns.push(batch.column(i).clone());
                fields.push(field.as_ref().clone());
            }
        }
    }
    if !changed {
        return Ok(batch.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context("rebuilding de-viewed batch")
}
