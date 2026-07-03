//! Configuration types + parsing. Everything that touches the outside world is
//! selected here so the same binary runs fully offline (local filesystem) or
//! against S3/MinIO with only a config change — no recompile.
//!
//! This module is pure: it defines the types and parses a TOML *string*. Locating
//! and *reading* the config file is I/O and lives in the `vdg` shell.

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
    /// Provisioned query cores.
    pub cores: u32,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            modeled_mibps_per_core: 250.0,
            cores: 4,
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
}
