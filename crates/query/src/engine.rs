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
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
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

    let ctx = SessionContext::new();

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
