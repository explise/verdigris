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

/// Build one record, advancing the RNG and the timestamp cursor. The single
/// place the generator's draw order lives, so the lazy and eager paths cannot
/// drift apart.
fn next_record(rng: &mut SeededRng, ts: &mut i64) -> LogRecord {
    let level = LEVELS[(rng.next_u64() as usize) % LEVELS.len()];
    let service = SERVICES[(rng.next_u64() as usize) % SERVICES.len()];
    let msgs = messages(level);
    let message = msgs[(rng.next_u64() as usize) % msgs.len()];
    *ts += 1 + (rng.next_u64() % 400) as i64;

    let status = if level == Level::Error { 503 } else { 200 };
    let mut attrs = BTreeMap::new();
    attrs.insert(
        "pod".to_string(),
        format!("{service}-{}", rng.next_u64() % 900),
    );
    attrs.insert("region".to_string(), "us-east-1".to_string());

    LogRecord {
        ts_millis: *ts,
        level,
        service: service.to_string(),
        status: Some(status),
        message: message.to_string(),
        trace_id: Some(format!("4ac9{:06x}d21", rng.next_u64() % 1_000_000)),
        attrs,
    }
}

/// [`generate`] as a lazy iterator over the identical sequence.
///
/// Holds only the RNG and the timestamp cursor, so a caller that serializes (or
/// ingests) as it goes pays for one record at a time instead of the whole run.
pub struct GenerateIter {
    rng: SeededRng,
    ts: i64,
    remaining: usize,
}

/// Generate `n` records starting at `start_ts_millis`, advancing time forward.
///
/// Materializes the whole run. Use [`generate_iter`] when `n` is large enough
/// that holding every record at once matters.
pub fn generate(n: usize, seed: u64, start_ts_millis: i64) -> Vec<LogRecord> {
    generate_iter(n, seed, start_ts_millis).collect()
}

/// [`generate`] without materializing: same records, same order, same seed.
pub fn generate_iter(n: usize, seed: u64, start_ts_millis: i64) -> GenerateIter {
    GenerateIter {
        rng: SeededRng::from_seed(seed),
        ts: start_ts_millis,
        remaining: n,
    }
}

impl Iterator for GenerateIter {
    type Item = LogRecord;

    fn next(&mut self) -> Option<LogRecord> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(next_record(&mut self.rng, &mut self.ts))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for GenerateIter {}

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

    /// The load generator's committed benchmark results are walked with this
    /// generator at a fixed seed and serialized to NDJSON. Making generation
    /// lazy must therefore not move a single byte, so the lazy path is pinned
    /// to the materialized one over several shapes (empty, one, uneven, large).
    #[test]
    fn iter_matches_generate_exactly() {
        let cases = [
            (0usize, 0u64, 0i64),
            (1, 42, 1_700_000_000_000),
            (257, 7, -5_000),
            (1_000, 42, 0),
        ];
        for (n, seed, start) in cases {
            let eager = generate(n, seed, start);
            let lazy: Vec<_> = generate_iter(n, seed, start).collect();
            assert_eq!(lazy, eager, "n={n} seed={seed} start={start}");
        }

        // The lazy path is a true iterator, not a pre-built Vec in disguise.
        let mut it = generate_iter(1_000_000, 42, 0);
        assert_eq!(it.len(), 1_000_000);
        assert_eq!(it.next(), Some(generate(1, 42, 0)[0].clone()));
    }
}
