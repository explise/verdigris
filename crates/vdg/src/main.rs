//! vdg — the Verdigris CLI and the shell that wires the sans-I/O core to the
//! real world (real clock, real object store). Everything nondeterministic lives
//! at this layer; the crates below it stay simulation-friendly.

#[cfg(feature = "apply")]
mod lifecycle_apply;
mod realclock;
#[cfg(feature = "serve")]
mod serve;

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use verdigris_core::config::Config;
use verdigris_core::cost::{self, RetrievalMode};
use verdigris_core::model::Tier;
use verdigris_query::{ModeledExecutor, ScanExecutor, ScanFile, ScanPlan};

#[derive(Parser)]
#[command(
    name = "vdg",
    version,
    about = "Verdigris — S3-native log storage & query"
)]
struct Cli {
    /// Path to a config file (else $VERDIGRIS_CONFIG, else ./config/verdigris.toml).
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the resolved configuration.
    Config,
    /// Verify the configured storage backend by round-tripping a probe object.
    Check,
    /// Ingest logs: encode to Parquet and write to the configured store, then
    /// update the table manifest. Use `--generate N` for synthetic logs or
    /// `--from <file.ndjson>` for real ones (one JSON log per line).
    Ingest {
        /// Logical table name (the key prefix in the store).
        #[arg(long, default_value = "logs")]
        table: String,
        /// Generate N synthetic, deterministic log records.
        #[arg(long)]
        generate: Option<usize>,
        /// Seed for the synthetic generator.
        #[arg(long, default_value_t = 1337)]
        seed: u64,
        /// Read NDJSON log records from this file instead of generating.
        #[arg(long)]
        from: Option<PathBuf>,
        /// Roll a new Parquet file every N rows.
        #[arg(long, default_value_t = 100_000)]
        max_rows: usize,
        /// Keep appending fresh synthetic logs forever (simulates live traffic),
        /// so recent-time queries like `last 1h` stay populated. Ctrl-C to stop.
        #[arg(long)]
        follow: bool,
        /// With --follow: seconds between batches.
        #[arg(long, default_value_t = 5)]
        interval: u64,
    },
    /// Show a table's manifest (files, rows, bytes, time ranges).
    Manifest {
        #[arg(long, default_value = "logs")]
        table: String,
    },
    /// Print the S3 lifecycle policy (age-based hot→warm→cold→expire) for a table.
    /// With `--apply`, PUT it onto the configured S3 bucket instead of only
    /// printing (requires an S3 backend and building with `--features apply`).
    Lifecycle {
        #[arg(long, default_value = "logs")]
        table: String,
        /// Apply the policy to the bucket via S3 PutBucketLifecycleConfiguration
        /// (credentials resolve through the standard AWS chain / IRSA). Without
        /// this flag the policy is only printed.
        #[arg(long)]
        apply: bool,
    },
    /// Compact a table's many small Parquet files into fewer ~target-sized files.
    Compact {
        #[arg(long, default_value = "logs")]
        table: String,
        /// Target compacted file size in MiB.
        #[arg(long, default_value_t = 64)]
        target_mb: u64,
    },
    /// Run SQL against a table, reading Parquet in place from the store.
    /// Requires building with `--features datafusion`.
    Sql {
        /// SQL query. The table name in the query must match `--table`.
        query: String,
        #[arg(long, default_value = "logs")]
        table: String,
    },
    /// Serve the HTTP API + static frontend. Requires `--features serve`.
    Serve {
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(long, default_value = "logs")]
        table: String,
        /// Directory of static frontend files to serve at `/`.
        #[arg(long, default_value = "frontend")]
        frontend: PathBuf,
        /// Which surface this node exposes. `all` = everything (default). `ingest`
        /// = only the write endpoints (`/v1/ingest`, `/v1/otlp/logs`) — run ONE of
        /// these as the single manifest writer. `query` = read/UI endpoints only;
        /// write endpoints return 405 — run N of these as stateless readers.
        #[arg(long, value_enum, default_value = "all")]
        role: RoleArg,
    },
    /// Model a scan over `--scan-gib` of a tier and print the cost estimate.
    /// (Placeholder for the real query path; exercises the executor + cost seams.)
    Query {
        /// Simulated amount of data scanned, in GiB.
        #[arg(long, default_value_t = 1.0)]
        scan_gib: f64,
        /// Tier being scanned (decides storage class -> cost & restore latency).
        #[arg(long, value_enum, default_value = "hot")]
        tier: TierArg,
        /// Glacier retrieval mode for cold scans.
        #[arg(long, value_enum, default_value = "standard")]
        retrieval: RetrievalArg,
    },
}

/// Which HTTP surface `vdg serve` exposes. Lets the deploy chart run one ingest
/// writer + N stateless query readers so replicas don't race on the JSON manifest.
#[derive(Clone, Copy, clap::ValueEnum)]
enum RoleArg {
    All,
    Ingest,
    Query,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum TierArg {
    Hot,
    Warm,
    Cold,
}

impl From<TierArg> for Tier {
    fn from(t: TierArg) -> Self {
        match t {
            TierArg::Hot => Tier::Hot,
            TierArg::Warm => Tier::Warm,
            TierArg::Cold => Tier::Cold,
        }
    }
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum RetrievalArg {
    Bulk,
    Standard,
    Expedited,
}

impl From<RetrievalArg> for RetrievalMode {
    fn from(r: RetrievalArg) -> Self {
        match r {
            RetrievalArg::Bulk => RetrievalMode::Bulk,
            RetrievalArg::Standard => RetrievalMode::Standard,
            RetrievalArg::Expedited => RetrievalMode::Expedited,
        }
    }
}

/// Resolve and read the config file (the I/O the core deliberately doesn't do):
/// explicit `--config`, else `$VERDIGRIS_CONFIG`, else `./config/verdigris.toml`,
/// else built-in defaults.
fn load_config(explicit: Option<&std::path::Path>) -> anyhow::Result<Config> {
    let path = explicit
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("VERDIGRIS_CONFIG").map(PathBuf::from))
        .or_else(|| {
            let default = PathBuf::from("config/verdigris.toml");
            default.exists().then_some(default)
        });

    match path {
        Some(p) => {
            let text = std::fs::read_to_string(&p)
                .with_context(|| format!("reading config {}", p.display()))?;
            Config::from_toml_str(&text).with_context(|| format!("in {}", p.display()))
        }
        None => Ok(Config::default()),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let cfg = load_config(cli.config.as_deref())?;

    match cli.command {
        Command::Config => {
            let text = toml::to_string_pretty(&cfg).context("serializing config")?;
            println!("# resolved configuration\n{text}");
        }
        Command::Check => {
            println!(
                "storage backend: {}",
                verdigris_storage::describe(&cfg.storage)
            );
            let store = verdigris_storage::build(&cfg.storage)?;
            let path = verdigris_storage::health_probe(&store).await?;
            println!("round-trip OK (probe key: {path})");
        }
        Command::Ingest {
            table,
            generate,
            seed,
            from,
            max_rows,
            follow,
            interval,
        } => {
            run_ingest(
                &cfg,
                &table,
                generate,
                seed,
                from.as_deref(),
                max_rows,
                follow,
                interval,
            )
            .await?;
        }
        Command::Manifest { table } => {
            run_manifest(&cfg, &table).await?;
        }
        Command::Lifecycle { table, apply } => {
            run_lifecycle(&cfg, &table, apply).await?;
        }
        Command::Compact { table, target_mb } => {
            run_compact(&cfg, &table, target_mb).await?;
        }
        Command::Sql { query, table } => {
            run_sql(&cfg, &table, &query).await?;
        }
        Command::Serve {
            port,
            table,
            frontend,
            role,
        } => {
            run_serve(cfg, table, port, frontend, role).await?;
        }
        Command::Query {
            scan_gib,
            tier,
            retrieval,
        } => {
            run_query(&cfg, scan_gib, tier.into(), retrieval.into()).await?;
        }
    }

    Ok(())
}

/// Generate `n` synthetic records anchored so the newest is ~now, keeping
/// relative time windows (`last 1h`) populated. Avg inter-arrival ~200ms.
fn gen_anchored(n: usize, seed: u64) -> Vec<verdigris_core::batch::LogRecord> {
    let start = shell_now_millis() - (n as i64) * 200;
    verdigris_ingest::generate::generate(n, seed, start)
}

#[allow(clippy::too_many_arguments)]
async fn run_ingest(
    cfg: &Config,
    table: &str,
    generate: Option<usize>,
    seed: u64,
    from: Option<&std::path::Path>,
    max_rows: usize,
    follow: bool,
    interval: u64,
) -> anyhow::Result<()> {
    use verdigris_core::batch::{BatchPolicy, LogRecord};

    let store = verdigris_storage::build(&cfg.storage)?;
    let ingestor = verdigris_ingest::Ingestor::new(store, table);
    let policy = BatchPolicy {
        max_rows,
        ..BatchPolicy::default()
    };

    // Live mode: keep appending fresh synthetic batches anchored to "now", so a
    // long-running server's `last 1h` queries stay populated. (This naturally
    // produces many small files — exactly what compaction, build step 4, solves.)
    if follow {
        anyhow::ensure!(
            from.is_none(),
            "--follow generates synthetic logs; remove --from"
        );
        let per_tick = generate.unwrap_or(200);
        println!(
            "following '{table}': +{per_tick} records every {interval}s (Ctrl-C to stop) — backend: {}",
            verdigris_storage::describe(&cfg.storage)
        );
        let mut tick: u64 = 0;
        loop {
            let recs = gen_anchored(per_tick, seed.wrapping_add(tick));
            let written = ingestor.ingest(recs, &cfg.routing, policy).await?;
            let bytes: u64 = written.iter().map(|f| f.bytes).sum();
            println!(
                "  tick {tick}: +{per_tick} records, {} file(s), {bytes} bytes",
                written.len()
            );
            tick += 1;
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }
    }

    // One-shot mode: synthetic, or NDJSON from a file.
    let records: Vec<LogRecord> = match (generate, from) {
        (Some(n), _) => gen_anchored(n, seed),
        (None, Some(path)) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            text.lines()
                .filter(|l| !l.trim().is_empty())
                .map(|line| {
                    serde_json::from_str::<verdigris_ingest::wire::JsonLog>(line)
                        .map(LogRecord::from)
                        .with_context(|| format!("parsing NDJSON line: {line}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        (None, None) => {
            anyhow::bail!(
                "nothing to ingest: pass --generate <N>, --from <file.ndjson>, or --follow"
            )
        }
    };
    let total = records.len();

    let written = ingestor.ingest(records, &cfg.routing, policy).await?;
    let bytes: u64 = written.iter().map(|f| f.bytes).sum();
    println!(
        "ingested {total} records into '{table}' -> {} file(s), {} bytes",
        written.len(),
        bytes
    );
    println!("backend: {}", verdigris_storage::describe(&cfg.storage));
    Ok(())
}

async fn run_manifest(cfg: &Config, table: &str) -> anyhow::Result<()> {
    let store = verdigris_storage::build(&cfg.storage)?;
    let ingestor = verdigris_ingest::Ingestor::new(store, table);
    let manifest = ingestor.load_manifest().await?;

    println!(
        "table '{}': {} file(s), {} rows, {} bytes",
        manifest.table,
        manifest.files.len(),
        manifest.total_rows(),
        manifest.total_bytes()
    );
    for f in &manifest.files {
        println!(
            "  {:<28} {:>7} rows  {:>9} bytes  ts[{}..{}]  {:?}",
            f.path, f.rows, f.bytes, f.min_ts, f.max_ts, f.tier
        );
    }
    Ok(())
}

async fn run_compact(cfg: &Config, table: &str, target_mb: u64) -> anyhow::Result<()> {
    let store = verdigris_storage::build(&cfg.storage)?;
    let ingestor = verdigris_ingest::Ingestor::new(store, table);
    let before = ingestor.load_manifest().await?.files.len();

    let reports = ingestor.compact(target_mb * 1024 * 1024).await?;
    for r in &reports {
        println!(
            "  {:<4?} {} -> {} files ({} merged)",
            r.tier, r.files_before, r.files_after, r.files_merged
        );
    }
    let after = ingestor.load_manifest().await?.files.len();
    println!("compaction: '{table}' {before} -> {after} files (target {target_mb} MiB)");
    Ok(())
}

async fn run_lifecycle(cfg: &Config, table: &str, apply: bool) -> anyhow::Result<()> {
    let policy = verdigris_core::lifecycle::policy_for(table, &cfg.lifecycle);
    println!("{}", serde_json::to_string_pretty(&policy)?);

    if apply {
        return apply_lifecycle(&cfg.storage, &policy).await;
    }

    match &cfg.storage {
        verdigris_core::config::StorageConfig::S3 { bucket, .. } => {
            eprintln!("\n# apply to the bucket with `vdg lifecycle --apply` (needs --features apply), or:");
            eprintln!("#   aws s3api put-bucket-lifecycle-configuration --bucket {bucket} \\");
            eprintln!("#     --lifecycle-configuration file://lifecycle.json");
        }
        _ => {
            eprintln!("\n# (local backend has no lifecycle — this is the policy you'd apply on S3)")
        }
    }
    Ok(())
}

#[cfg(feature = "apply")]
async fn apply_lifecycle(
    storage: &verdigris_core::config::StorageConfig,
    policy: &verdigris_core::lifecycle::LifecyclePolicy,
) -> anyhow::Result<()> {
    lifecycle_apply::apply(storage, policy).await
}

#[cfg(not(feature = "apply"))]
async fn apply_lifecycle(
    _storage: &verdigris_core::config::StorageConfig,
    _policy: &verdigris_core::lifecycle::LifecyclePolicy,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "lifecycle --apply requires the AWS SDK: rebuild with `cargo build --features apply`"
    )
}

/// Current time in epoch millis, read through the `Clock` seam rather than
/// calling `SystemTime::now()` here. The CLI is one-shot so it constructs
/// `RealClock` per call; `serve` threads a shared `Arc<dyn Clock>` instead.
fn shell_now_millis() -> i64 {
    use verdigris_core::clock::Clock;
    crate::realclock::RealClock.now_millis() as i64
}

/// Accept either raw SQL (passed through) or the search DSL (compiled to SQL).
#[cfg(feature = "datafusion")]
fn resolve_sql(query: &str, table: &str) -> anyhow::Result<String> {
    use verdigris_core::search;
    if search::looks_like_sql(query) {
        Ok(query.to_string())
    } else {
        search::to_sql(query, table, shell_now_millis(), 200)
            .map_err(|e| anyhow::anyhow!("search query error: {e}"))
    }
}

#[cfg(feature = "datafusion")]
async fn run_sql(cfg: &Config, table: &str, query: &str) -> anyhow::Result<()> {
    let store = verdigris_storage::build(&cfg.storage)?;
    // The manifest is our catalog: it tells us exactly which files back the table.
    let ingestor = verdigris_ingest::Ingestor::new(store.clone(), table);
    let manifest = ingestor.load_manifest().await?;
    anyhow::ensure!(
        !manifest.files.is_empty(),
        "table '{table}' has no files — ingest some logs first"
    );
    let files: Vec<String> = manifest.files.iter().map(|f| f.path.clone()).collect();

    let sql = resolve_sql(query, table)?;
    let limits = verdigris_query::engine::QueryLimits::from_config(&cfg.query);
    let out = verdigris_query::engine::query_table(store, table, &files, &sql, &limits).await?;
    println!("{}", out.pretty);
    println!(
        "({} row(s) from {} file(s), engine: datafusion, read in place — no rehydration)",
        out.rows,
        files.len()
    );
    Ok(())
}

#[cfg(not(feature = "datafusion"))]
async fn run_sql(_cfg: &Config, _table: &str, _query: &str) -> anyhow::Result<()> {
    anyhow::bail!("SQL requires the datafusion engine: rebuild with `cargo run --features datafusion -- sql ...`")
}

#[cfg(feature = "serve")]
async fn run_serve(
    cfg: Config,
    table: String,
    port: u16,
    frontend: PathBuf,
    role: RoleArg,
) -> anyhow::Result<()> {
    serve::serve(cfg, table, port, frontend, role.into()).await
}

#[cfg(not(feature = "serve"))]
async fn run_serve(
    _cfg: Config,
    _table: String,
    _port: u16,
    _frontend: PathBuf,
    _role: RoleArg,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "serve requires the serve feature: rebuild with `cargo run --features serve -- serve`"
    )
}

async fn run_query(
    cfg: &Config,
    scan_gib: f64,
    tier: Tier,
    retrieval: RetrievalMode,
) -> anyhow::Result<()> {
    let bytes = (scan_gib * cost::GIB) as u64;
    let class = tier.default_class();

    // Cost estimate (the pre-query gate). Same model the SimObjectStore will use.
    let est = cost::estimate_scan(bytes, class, retrieval);
    println!(
        "estimate: scan {:.2} GiB from {:?} ({:?}) -> ${:.4}, restore ~{} ms",
        est.gib, class, retrieval, est.retrieval_usd, est.restore_latency_ms
    );

    // Model the scan itself through the ScanExecutor seam.
    let exec = ModeledExecutor::new(cfg.query.modeled_mibps_per_core, cfg.query.cores);
    let plan = ScanPlan {
        files: vec![ScanFile {
            path: "modeled.parquet".into(),
            bytes,
        }],
        predicate: None,
    };
    let result = exec.scan(&plan).await?;
    println!(
        "modeled scan: {} file(s), {} bytes, ~{} ms at {} core(s) x {} MiB/s",
        result.files_scanned,
        result.bytes_scanned,
        result.modeled_ms,
        cfg.query.cores,
        cfg.query.modeled_mibps_per_core,
    );
    Ok(())
}
