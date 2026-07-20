//! Configuration types + parsing. Everything that touches the outside world is
//! selected here so the same binary runs fully offline (local filesystem) or
//! against S3/MinIO with only a config change — no recompile.
//!
//! This module is pure: it defines the types and parses a TOML *string*. Locating
//! and *reading* the config file is I/O and lives in the `vdg` shell.

use crate::batch::BatchPolicy;
use crate::model::{Level, Tier};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub storage: StorageConfig,
    pub query: QueryConfig,
    pub routing: RoutingConfig,
    pub lifecycle: LifecycleConfig,
    pub auth: AuthConfig,
    pub ingest: IngestConfig,
    pub compaction: CompactionConfig,
}

/// Ingest backpressure & memory bounds for the `/v1/ingest` + `/v1/otlp/logs`
/// write path. Acked data is already durable — each POST is synchronously written
/// to the object store and atomically committed to the manifest, so the store is
/// the write-ahead log. These settings instead bound *process memory* under load:
/// oversized bodies and a flood of concurrent ingests piling up in RAM.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    /// Max accepted request-body size (bytes); larger payloads get 413 before
    /// being buffered, bounding per-request memory.
    pub max_body_bytes: usize,
    /// Max concurrent in-flight ingest requests; beyond this the server sheds with
    /// 429 (backpressure) instead of queueing bodies in memory unboundedly.
    pub max_inflight: usize,
    /// Roll a Parquet file once a tier's buffer reaches this many rows.
    ///
    /// Exposed on the HTTP path (not just the CLI) because otherwise the *client's*
    /// request size silently decides the PUT rate: `Ingestor::ingest` flushes any
    /// leftover buffer at the end of every call, so each POST writes at least one
    /// file per tier it touches regardless of these thresholds. Without a server-side
    /// knob, a batch-size-vs-throughput sweep would only be measuring the load
    /// generator. See `docs/load-test.md`.
    pub max_batch_rows: usize,
    /// Roll a Parquet file once a tier's buffer reaches this many bytes
    /// (`LogRecord::approx_bytes`, so in-memory size — not the compressed size).
    pub max_batch_bytes: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        let batch = BatchPolicy::default();
        Self {
            max_body_bytes: 16 * 1024 * 1024, // 16 MiB
            max_inflight: 32,
            max_batch_rows: batch.max_rows,
            max_batch_bytes: batch.max_bytes,
        }
    }
}

impl IngestConfig {
    /// The configured file-rolling policy for the HTTP ingest path.
    pub fn batch_policy(&self) -> BatchPolicy {
        BatchPolicy {
            max_rows: self.max_batch_rows,
            max_bytes: self.max_batch_bytes,
        }
    }
}

/// Background auto-compaction: merge small Parquet files into larger ones without
/// operator intervention. Streaming ingest produces many small files — worse at
/// low, steady volume — which slow scans and bloat the manifest; the scheduler
/// keeps file count bounded so the system stays a product, not a toy. Files only
/// ever merge within their own tier, so severity/tiering is unaffected.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactionConfig {
    /// Run the background compaction scheduler (writer role only).
    pub enabled: bool,
    /// Target size for a compacted file (MiB); bins fill to ~this size.
    pub target_mib: u64,
    /// Compact once any tier has at least this many files pending a merge (files
    /// that would fall into a multi-file bin). Avoids rewriting on every tick.
    pub trigger_pending_files: usize,
    /// Max source files a single compaction *pass* merges before committing and
    /// releasing the ingest lock. Bounds how long one pass can stall ingest (a
    /// full 2k-file backlog in one pass held the lock ~43s in testing); the
    /// scheduler runs repeated bounded passes to drain a large backlog, yielding
    /// the lock between them so ingest can interleave.
    pub max_merge_files_per_pass: usize,
    /// How often the scheduler checks for pending compaction (seconds).
    pub interval_secs: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_mib: 256,
            trigger_pending_files: 16,
            max_merge_files_per_pass: 128,
            interval_secs: 300, // 5 min
        }
    }
}

impl CompactionConfig {
    /// Target compacted-file size in bytes.
    pub fn target_bytes(&self) -> u64 {
        self.target_mib * 1024 * 1024
    }
}

/// Optional bearer-token auth for the `/v1/*` HTTP API. Off by default so the
/// local/offline loop and existing tests are unchanged. When enabled, every
/// `/v1/*` request must carry `Authorization: Bearer <token>`; the static
/// frontend and `/config.json` (which the UI needs pre-auth) stay open.
///
/// The token may be set here or, preferably in production, via the
/// `VERDIGRIS_API_TOKEN` environment variable (which overrides this field so the
/// secret never has to live in a config file). See `Config::resolved_auth_token`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub enabled: bool,
    pub token: Option<String>,
}

impl Config {
    /// The effective API token: `VERDIGRIS_API_TOKEN` if set (non-empty), else the
    /// configured `auth.token`. Returns `None` if neither is present.
    pub fn resolved_auth_token(&self) -> Option<String> {
        std::env::var("VERDIGRIS_API_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| self.auth.token.clone())
    }
}

/// Severity-based write-time routing: which tier (and thus prefix / storage
/// class) a log lands in, decided by its level. Severity decides *placement*,
/// never price.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    pub error: Tier,
    pub warn: Tier,
    pub info: Tier,
    pub debug: Tier,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            error: Tier::Hot,
            warn: Tier::Warm,
            info: Tier::Warm,
            debug: Tier::Cold,
        }
    }
}

impl RoutingConfig {
    pub fn tier_for(&self, level: Level) -> Tier {
        match level {
            Level::Error => self.error,
            Level::Warn => self.warn,
            Level::Info => self.info,
            Level::Debug => self.debug,
        }
    }
}

/// Age-based lifecycle transitions, rendered into an S3 lifecycle policy. These
/// move data hot → warm → cold (colder storage classes) as it ages, then expire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleConfig {
    pub hot_to_warm_days: u32,
    pub warm_to_cold_days: u32,
    pub expire_days: u32,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            hot_to_warm_days: 3,
            warm_to_cold_days: 30,
            expire_days: 400,
        }
    }
}

/// Storage backend selection. Internally tagged by `backend` so the TOML reads:
///
/// ```toml
/// [storage]
/// backend = "local"
/// path = "./data"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum StorageConfig {
    /// Local filesystem — the default. Fully offline, no S3 needed.
    Local {
        #[serde(default = "default_local_path")]
        path: PathBuf,
    },
    /// In-memory store. For tests and ephemeral runs.
    Memory,
    /// S3 (or any S3-compatible endpoint, e.g. MinIO). Credentials may be set
    /// here or left to the standard AWS env vars / profile.
    S3 {
        bucket: String,
        #[serde(default)]
        region: Option<String>,
        /// Custom endpoint, e.g. `http://localhost:9000` for MinIO.
        #[serde(default)]
        endpoint: Option<String>,
        /// Allow plain HTTP (needed for local MinIO).
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        access_key_id: Option<String>,
        #[serde(default)]
        secret_access_key: Option<String>,
        /// Optional key prefix within the bucket.
        #[serde(default)]
        prefix: Option<String>,
    },
}

fn default_local_path() -> PathBuf {
    PathBuf::from("./data")
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Local {
            path: default_local_path(),
        }
    }
}

/// Knobs for the modeled executor / calibration. Decoupled from storage on
/// purpose: query speed is a separately provisioned dial (see CLAUDE.md).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    /// Modeled per-core scan throughput (MiB/s) used by the ModeledExecutor and
    /// as the calibration target for the real executor.
    pub modeled_mibps_per_core: f64,
    /// Provisioned query cores. Also caps the real engine's target partitions —
    /// each partition carries its own buffers, so this bounds concurrency and
    /// therefore peak execution memory.
    pub cores: u32,

    // ── Memory bounds (issue #2: run comfortably on a 1–2 GB box) ──────────
    //
    // Two independent ceilings, because a query can blow up in two different
    // places. `memory_pool_mib` bounds *execution* (the sorts, joins and
    // aggregates DataFusion runs); past it, operators spill to disk, and if even
    // that can't be satisfied the query fails with a message naming the biggest
    // consumers. `max_result_*` bound the *result set* being accumulated for the
    // client, which the pool does not cover: a `SELECT *` with no aggregation is
    // pure streaming output, so nothing would stop it from filling RAM.
    /// Ceiling on the DataFusion execution memory pool, in MiB.
    pub memory_pool_mib: u64,
    /// Reject a result larger than this many rows...
    pub max_result_rows: u64,
    /// ...or this much Arrow data, whichever trips first. In MiB.
    pub max_result_mib: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            modeled_mibps_per_core: 250.0,
            cores: 4,
            // Sized so the engine, the accumulated result and the rest of the
            // process all fit inside a 2 GB box with headroom.
            memory_pool_mib: 512,
            max_result_rows: 1_000_000,
            max_result_mib: 256,
        }
    }
}

impl Config {
    /// Parse config from a TOML string. Pure — no file I/O. The shell reads the
    /// file (or supplies defaults) and calls this.
    pub fn from_toml_str(text: &str) -> anyhow::Result<Self> {
        toml::from_str(text).map_err(|e| anyhow::anyhow!("parsing config: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_local() {
        let c = Config::default();
        assert!(matches!(c.storage, StorageConfig::Local { .. }));
    }

    #[test]
    fn parses_s3_minio_toml() {
        let toml = r#"
            [storage]
            backend = "s3"
            bucket = "verdigris"
            endpoint = "http://localhost:9000"
            allow_http = true

            [query]
            cores = 8
        "#;
        let c: Config = toml::from_str(toml).unwrap();
        match c.storage {
            StorageConfig::S3 {
                bucket,
                allow_http,
                endpoint,
                ..
            } => {
                assert_eq!(bucket, "verdigris");
                assert!(allow_http);
                assert_eq!(endpoint.as_deref(), Some("http://localhost:9000"));
            }
            _ => panic!("expected s3"),
        }
        assert_eq!(c.query.cores, 8);
    }

    #[test]
    fn batch_policy_defaults_match_and_are_overridable() {
        // Absent [ingest] section must reproduce BatchPolicy::default() exactly:
        // wiring the HTTP path to config is only safe if it is a no-op by default.
        let c = Config::default();
        let d = BatchPolicy::default();
        assert_eq!(c.ingest.batch_policy().max_rows, d.max_rows);
        assert_eq!(c.ingest.batch_policy().max_bytes, d.max_bytes);

        let toml = r#"
            [ingest]
            max_batch_rows = 5000
            max_batch_bytes = 8388608
        "#;
        let c: Config = toml::from_str(toml).unwrap();
        assert_eq!(c.ingest.batch_policy().max_rows, 5_000);
        assert_eq!(c.ingest.batch_policy().max_bytes, 8 * 1024 * 1024);
        // Untouched neighbours keep their defaults.
        assert_eq!(c.ingest.max_inflight, 32);
    }

    #[test]
    fn compaction_defaults_on_and_parses() {
        // Absent [compaction] section -> sensible defaults (auto-compaction on).
        let c = Config::default();
        assert!(c.compaction.enabled);
        assert_eq!(c.compaction.target_mib, 256);
        assert_eq!(c.compaction.target_bytes(), 256 * 1024 * 1024);
        assert_eq!(c.compaction.trigger_pending_files, 16);
        assert_eq!(c.compaction.max_merge_files_per_pass, 128);

        let toml = r#"
            [compaction]
            enabled = false
            target_mib = 512
            trigger_pending_files = 8
            max_merge_files_per_pass = 64
            interval_secs = 60
        "#;
        let c: Config = toml::from_str(toml).unwrap();
        assert!(!c.compaction.enabled);
        assert_eq!(c.compaction.target_mib, 512);
        assert_eq!(c.compaction.trigger_pending_files, 8);
        assert_eq!(c.compaction.max_merge_files_per_pass, 64);
        assert_eq!(c.compaction.interval_secs, 60);
    }

    #[test]
    fn auth_defaults_off_and_parses() {
        // Absent [auth] section -> disabled, no token.
        let c = Config::default();
        assert!(!c.auth.enabled);
        assert!(c.auth.token.is_none());

        let toml = r#"
            [auth]
            enabled = true
            token = "s3cr3t"
        "#;
        let c: Config = toml::from_str(toml).unwrap();
        assert!(c.auth.enabled);
        assert_eq!(c.auth.token.as_deref(), Some("s3cr3t"));
        // With no env override, the resolved token is the configured one.
        assert_eq!(c.resolved_auth_token().as_deref(), Some("s3cr3t"));
    }
}
