//! Deterministic synthetic log generator.
//!
//! Seeded via the core `Rng` seam, so `generate(n, seed, start)` is fully
//! reproducible — the same seed yields the same logs every time. This is the
//! cheap, store-nothing data source for local runs, benchmarks, and (later)
//! feeding the DST harness. The fixtures mirror the frontend's mock data so the
//! UI looks the same against real ingest.

use std::collections::BTreeMap;
use verdigris_core::batch::LogRecord;
use verdigris_core::model::Level;
use verdigris_core::rng::{Rng, SeededRng};

const SERVICES: &[&str] = &[
    "auth",
    "checkout",
    "session-store",
    "gateway",
    "billing",
    "search",
    "notifier",
];

// Weighted so errors are common enough to be interesting.
const LEVELS: &[Level] = &[
    Level::Error,
    Level::Error,
    Level::Warn,
    Level::Info,
    Level::Error,
    Level::Info,
    Level::Warn,
    Level::Debug,
    Level::Error,
    Level::Info,
];

fn messages(level: Level) -> &'static [&'static str] {
    match level {
        Level::Error => &[
            "token validation failed: signature mismatch kid=v2 (expired 4m ago)",
            "upstream 503 from session-store: connection refused after 3 retries",
            "jwks refresh failed: 504 from idp.internal after 2000ms",
            "payment intent declined: card_declined (issuer 51)",
            "deadline exceeded calling inventory.Reserve (1200ms budget)",
        ],
        Level::Warn => &[
            "connection pool at 92% capacity (46/50) — shedding low-priority reads",
            "latency p99 1840ms exceeds 1500ms SLO over trailing 60s",
            "retry budget for session-store exhausted; failing open for /healthz",
            "clock skew 1.8s vs ntp peer — token exp checks may be lenient",
        ],
        Level::Info => &[
            "rotated signing key kid=v3; v2 grace window 5m",
            "circuit breaker session-store -> half-open, probing 1 req/s",
            "accepted 1.2k tokens/s; reject rate 6.4% (mostly kid=v2)",
            "checkout completed amount=142.00 USD",
        ],
        Level::Debug => &[
            "cache hit ratio 0.94 over 10s window (key prefix sess:)",
            "gc pause 12ms heap=512MB/1GB",
            "trace sampled span=authorize",
        ],
    }
}

/// Generate `n` records starting at `start_ts_millis`, advancing time forward.
pub fn generate(n: usize, seed: u64, start_ts_millis: i64) -> Vec<LogRecord> {
    let mut rng = SeededRng::from_seed(seed);
    let mut ts = start_ts_millis;
    let mut out = Vec::with_capacity(n);

    for _ in 0..n {
        let level = LEVELS[(rng.next_u64() as usize) % LEVELS.len()];
        let service = SERVICES[(rng.next_u64() as usize) % SERVICES.len()];
        let msgs = messages(level);
        let message = msgs[(rng.next_u64() as usize) % msgs.len()];
        ts += 1 + (rng.next_u64() % 400) as i64;

        let status = if level == Level::Error { 503 } else { 200 };
        let mut attrs = BTreeMap::new();
        attrs.insert(
            "pod".to_string(),
            format!("{service}-{}", rng.next_u64() % 900),
        );
        attrs.insert("region".to_string(), "us-east-1".to_string());

        out.push(LogRecord {
            ts_millis: ts,
            level,
            service: service.to_string(),
            status: Some(status),
            message: message.to_string(),
            trace_id: Some(format!("4ac9{:06x}d21", rng.next_u64() % 1_000_000)),
            attrs,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_a_seed() {
        let a = generate(50, 7, 0);
        let b = generate(50, 7, 0);
        assert_eq!(a, b);
        // timestamps strictly increase
        assert!(a.windows(2).all(|w| w[1].ts_millis > w[0].ts_millis));
    }
}
