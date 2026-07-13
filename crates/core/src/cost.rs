//! The shared cost model.
//!
//! This is deliberately one module: the `SimObjectStore` (which models retrieval
//! latency/price under DST) and the user-facing cost estimator MUST compute from
//! the same numbers, or the simulation lies about what production will bill. All
//! prices are AWS us-east-1 approximations from CLAUDE.md — verify before relying.

use crate::model::StorageClass;
use serde::{Deserialize, Serialize};

/// 1 GiB in bytes.
pub const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// Glacier Flexible retrieval modes (speed vs price trade-off).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RetrievalMode {
    /// 5–12 h, free.
    Bulk,
    /// 3–5 h, ~$0.01/GB.
    Standard,
    /// 1–5 min, ~$0.03/GB.
    Expedited,
}

/// Storage price in USD per GiB-month.
pub fn storage_usd_per_gib_month(class: StorageClass) -> f64 {
    match class {
        StorageClass::Standard => 0.023,
        StorageClass::GlacierInstant => 0.004,
        StorageClass::GlacierFlexible => 0.0036,
        StorageClass::GlacierDeepArchive => 0.00099,
    }
}

/// Retrieval price in USD per GiB scanned, given class + mode.
pub fn retrieval_usd_per_gib(class: StorageClass, mode: RetrievalMode) -> f64 {
    match class {
        // Standard is queried in place; no separate retrieval charge.
        StorageClass::Standard => 0.0,
        // Glacier Instant: pay per GET regardless of mode.
        StorageClass::GlacierInstant => 0.03,
        StorageClass::GlacierFlexible => match mode {
            RetrievalMode::Bulk => 0.0,
            RetrievalMode::Standard => 0.01,
            RetrievalMode::Expedited => 0.03,
        },
        // Deep Archive: bulk ~free-ish to standard; placeholder until verified.
        StorageClass::GlacierDeepArchive => match mode {
            RetrievalMode::Bulk => 0.0025,
            _ => 0.02,
        },
    }
}

/// Modeled time-to-first-byte for a single GET against a storage class, in
/// milliseconds. This is the per-object request latency the `SimObjectStore`
/// adds to the simulated clock on each read — distinct from [`restore_latency_ms`]
/// (the one-time Glacier thaw) and from scan *throughput* (a query-engine dial,
/// modeled by `ScanExecutor`, not the store). Lives here so the sim store and any
/// estimator that wants per-request latency draw from one model. Rough us-east-1
/// order-of-magnitude figures — verify before relying.
pub fn first_byte_latency_ms(class: StorageClass) -> u64 {
    match class {
        // Warm/interactive object GET: tens of ms.
        StorageClass::Standard => 20,
        StorageClass::GlacierInstant => 30,
        // Flexible/Deep are not directly GET-able; first byte is only paid once a
        // restore has staged the object into Standard, so the per-GET cost then
        // looks like Standard. The big wait is restore_latency_ms, not this.
        StorageClass::GlacierFlexible | StorageClass::GlacierDeepArchive => 20,
    }
}

/// Approximate restore latency in milliseconds, for the DST timing model.
pub fn restore_latency_ms(class: StorageClass, mode: RetrievalMode) -> u64 {
    match class {
        StorageClass::Standard | StorageClass::GlacierInstant => 0,
        StorageClass::GlacierFlexible | StorageClass::GlacierDeepArchive => match mode {
            RetrievalMode::Expedited => 5 * 60 * 1000,     // ~5 min
            RetrievalMode::Standard => 4 * 60 * 60 * 1000, // ~4 h
            RetrievalMode::Bulk => 8 * 60 * 60 * 1000,     // ~8 h
        },
    }
}

/// The pre-query estimate surfaced to the user before scanning cold data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanEstimate {
    pub gib: f64,
    pub retrieval_usd: f64,
    pub restore_latency_ms: u64,
}

/// Estimate the cost + restore latency of scanning `bytes` from `class`.
pub fn estimate_scan(bytes: u64, class: StorageClass, mode: RetrievalMode) -> ScanEstimate {
    let gib = bytes as f64 / GIB;
    ScanEstimate {
        gib,
        retrieval_usd: gib * retrieval_usd_per_gib(class, mode),
        restore_latency_ms: restore_latency_ms(class, mode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_scan_is_free_and_instant() {
        let e = estimate_scan(10 * GIB as u64, StorageClass::Standard, RetrievalMode::Bulk);
        assert_eq!(e.retrieval_usd, 0.0);
        assert_eq!(e.restore_latency_ms, 0);
    }

    #[test]
    fn cold_expedited_scan_costs_money_and_takes_minutes() {
        let e = estimate_scan(
            100 * GIB as u64,
            StorageClass::GlacierFlexible,
            RetrievalMode::Expedited,
        );
        assert!((e.retrieval_usd - 3.0).abs() < 1e-6); // 100 GiB * $0.03
        assert!(e.restore_latency_ms > 0);
    }
}
