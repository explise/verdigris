//! Prove the read funnel on our ACTUAL engine config: run EXPLAIN ANALYZE and
//! print DataFusion's own scan metrics (bytes scanned, row groups pruned by
//! statistics vs bloom filter, pushdown rows pruned).
//!
//! Usage:
//!   cargo run -p verdigris-query --features datafusion --example explain_pruning \
//!     -- <file.parquet> "<SQL over table `logs`>"

use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: explain_pruning <file> <sql>");
    let sql = std::env::args()
        .nth(2)
        .expect("usage: explain_pruning <file> <sql>");

    // EXACTLY the flags crates/query/src/engine.rs sets in collect_batches.
    let mut cfg = SessionConfig::new();
    {
        let opts = cfg.options_mut();
        opts.execution.parquet.bloom_filter_on_read = true;
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
    }
    // Report defaults we rely on (not set explicitly — DataFusion turns them on):
    println!("--- effective parquet scan options ---");
    {
        let p = &cfg.options().execution.parquet;
        println!("pushdown_filters      = {}", p.pushdown_filters);
        println!("reorder_filters       = {}", p.reorder_filters);
        println!("bloom_filter_on_read  = {}", p.bloom_filter_on_read);
        println!("pruning (row-group)   = {}", p.pruning);
        println!("enable_page_index     = {}", p.enable_page_index);
    }

    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_parquet("logs", &path, ParquetReadOptions::default())
        .await?;

    println!("\n--- EXPLAIN ANALYZE ---\n{sql}\n");
    let df = ctx.sql(&format!("EXPLAIN ANALYZE {sql}")).await?;
    let batches = df.collect().await?;
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    // The plan is verbose; surface the scan line and its metrics.
    for line in text.lines() {
        let l = line.to_lowercase();
        if l.contains("datasourceexec")
            || l.contains("parquet")
            || l.contains("row_groups")
            || l.contains("bytes_scanned")
            || l.contains("pushdown")
            || l.contains("page_index")
            || l.contains("bloom")
            || l.contains("num_predicate")
            || l.contains("pruned")
        {
            println!("{}", line.trim());
        }
    }
    Ok(())
}
