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

use std::sync::Arc;

use anyhow::Context;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::ObjectStore;

/// The result of a SQL query (CLI/pretty path).
pub struct QueryOutput {
    /// Total rows returned.
    pub rows: usize,
    /// A pretty-printed table of the result, ready to display.
    pub pretty: String,
}

/// Plan and execute `sql` against `table`, reading the exact Parquet `files` in
/// place from `store`. `files` are object-store keys from the manifest (our
/// catalog) — we register those rather than directory-scanning, so the planner
/// sees exactly what the catalog says, on any backend.
async fn collect_batches(
    store: Arc<dyn ObjectStore>,
    table: &str,
    files: &[String],
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    anyhow::ensure!(!files.is_empty(), "table '{table}' has no files to query");

    // Turn on bloom-filter row-group pruning and predicate pushdown so an
    // equality lookup (`trace_id = '…'`, `service = 'auth'`) reads only the row
    // groups whose bloom filter admits the value, instead of every row.
    let mut cfg = SessionConfig::new();
    {
        let opts = cfg.options_mut();
        opts.execution.parquet.bloom_filter_on_read = true;
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
    }
    let ctx = SessionContext::new_with_config(cfg);

    // Register our object store under a synthetic scheme/authority.
    let base = ObjectStoreUrl::parse("verdigris://store").context("parsing object store url")?;
    ctx.register_object_store(base.as_ref(), store);

    // Register the exact files from the manifest as the table's paths.
    let urls = files
        .iter()
        .map(|f| ListingTableUrl::parse(format!("verdigris://store/{f}")))
        .collect::<Result<Vec<_>, _>>()
        .context("parsing file urls")?;
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .context("inferring schema from parquet")?;
    let provider = ListingTable::try_new(config).context("creating listing table")?;
    ctx.register_table(table, Arc::new(provider))
        .context("registering table")?;

    let df = ctx.sql(sql).await.context("planning sql")?;
    df.collect().await.context("executing sql")
}

/// Run a query and return a pretty-printed table (for the CLI).
pub async fn query_table(
    store: Arc<dyn ObjectStore>,
    table: &str,
    files: &[String],
    sql: &str,
) -> anyhow::Result<QueryOutput> {
    let batches = collect_batches(store, table, files, sql).await?;
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
    files: &[String],
    sql: &str,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let batches = collect_batches(store, table, files, sql).await?;
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
    files: &[String],
    sql: &str,
) -> anyhow::Result<Vec<u8>> {
    let batches = collect_batches(store, table, files, sql).await?;
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
