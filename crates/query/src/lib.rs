//! verdigris-query — the `ScanExecutor` seam.
//!
//! The default executor is [`ModeledExecutor`]: pure Rust, no engine, it *models*
//! scan latency from throughput instead of running anything. That is what DST
//! uses at trillion scale — you never actually execute a trillion rows, you model
//! and calibrate (see ADR-001).
//!
//! Real execution is DataFusion (pure Rust, builds on the same `object_store`
//! crate we standardize on), gated behind the `datafusion` feature so the default
//! build stays offline and fast. DuckDB is intentionally not used: native C++ is
//! opaque to the simulator.

use async_trait::async_trait;

/// One physical file the scan must read.
#[derive(Debug, Clone)]
pub struct ScanFile {
    pub path: String,
    pub bytes: u64,
}

/// A planned scan: the files to read and an optional predicate. At trillion
/// scale `files` comes from a fabricated catalog — the entries are real, the
/// bytes need not exist.
#[derive(Debug, Clone, Default)]
pub struct ScanPlan {
    pub files: Vec<ScanFile>,
    pub predicate: Option<String>,
}

impl ScanPlan {
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }
}

/// The outcome of (or model of) a scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanResult {
    pub files_scanned: usize,
    pub bytes_scanned: u64,
    /// Modeled wall time the scan would take, in milliseconds. Under DST this is
    /// added to the simulated clock instead of being really waited out.
    pub modeled_ms: u64,
    /// Rows produced, when a real engine ran it. `None` for modeled-only.
    pub rows: Option<u64>,
}

#[async_trait]
pub trait ScanExecutor: Send + Sync {
    async fn scan(&self, plan: &ScanPlan) -> anyhow::Result<ScanResult>;
}

/// Deterministic, dependency-free executor. Computes how long a scan *would*
/// take from a throughput model rather than executing it. The throughput figure
/// is what real calibration runs measure and feed back in.
#[derive(Debug, Clone)]
pub struct ModeledExecutor {
    pub mibps_per_core: f64,
    pub cores: u32,
}

impl ModeledExecutor {
    pub fn new(mibps_per_core: f64, cores: u32) -> Self {
        Self {
            mibps_per_core,
            cores,
        }
    }

    fn bytes_per_sec(&self) -> f64 {
        self.mibps_per_core * self.cores as f64 * 1024.0 * 1024.0
    }
}

#[async_trait]
impl ScanExecutor for ModeledExecutor {
    async fn scan(&self, plan: &ScanPlan) -> anyhow::Result<ScanResult> {
        let bytes = plan.total_bytes();
        let bps = self.bytes_per_sec();
        let modeled_ms = if bps > 0.0 {
            (bytes as f64 / bps * 1000.0) as u64
        } else {
            0
        };
        Ok(ScanResult {
            files_scanned: plan.files.len(),
            bytes_scanned: bytes,
            modeled_ms,
            rows: None,
        })
    }
}

/// Real query execution via DataFusion: reads Parquet in place from the object
/// store and runs SQL. Feature-gated behind `datafusion`.
#[cfg(feature = "datafusion")]
pub mod engine;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn modeled_scan_time_scales_with_bytes() {
        let exec = ModeledExecutor::new(100.0, 1); // 100 MiB/s
        let plan = ScanPlan {
            files: vec![ScanFile {
                path: "a.parquet".into(),
                bytes: 100 * 1024 * 1024, // 100 MiB -> ~1000 ms
            }],
            predicate: None,
        };
        let r = exec.scan(&plan).await.unwrap();
        assert_eq!(r.files_scanned, 1);
        assert_eq!(r.bytes_scanned, 100 * 1024 * 1024);
        assert!((r.modeled_ms as i64 - 1000).abs() <= 1);
    }
}
