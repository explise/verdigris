//! The canonical in-memory log record and the sans-I/O batching policy.
//!
//! Streaming logs arrive one at a time; we accumulate them and roll a Parquet
//! file when a size/row threshold is hit. That rolling *decision* is pure logic
//! and lives here (no encoding, no I/O) so it is trivially simulated. The actual
//! Arrow/Parquet encoding and the object-store write happen in `verdigris-ingest`.

use crate::model::Level;
use std::collections::BTreeMap;

/// One log line in canonical form. Known fields are columns; anything else goes
/// in `attrs` (which becomes a JSON column — this is our schema-evolution escape
/// hatch: new fields don't require a schema migration).
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub ts_millis: i64,
    pub level: Level,
    pub service: String,
    /// HTTP-ish status code. A first-class column (not buried in `attrs`) so it
    /// is cleanly filterable with range predicates, e.g. `status >= 500`.
    pub status: Option<i32>,
    pub message: String,
    pub trace_id: Option<String>,
    pub attrs: BTreeMap<String, String>,
}

impl LogRecord {
    /// Rough in-memory footprint, used only to decide when to roll a file.
    pub fn approx_bytes(&self) -> usize {
        let attrs: usize = self.attrs.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
        8 + 8 + 4 // ts + level + status
            + self.service.len()
            + self.message.len()
            + self.trace_id.as_ref().map_or(0, |s| s.len())
            + attrs
    }
}

/// When to roll the current buffer into a file.
#[derive(Debug, Clone, Copy)]
pub struct BatchPolicy {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        // ~128 MiB target rolls; compaction (step 4) merges these further.
        Self {
            max_rows: 100_000,
            max_bytes: 128 * 1024 * 1024,
        }
    }
}

/// Accumulates records and signals when a file should be rolled. Pure state —
/// no time, no I/O.
#[derive(Debug)]
pub struct Batcher {
    policy: BatchPolicy,
    buf: Vec<LogRecord>,
    bytes: usize,
}

impl Batcher {
    pub fn new(policy: BatchPolicy) -> Self {
        Self {
            policy,
            buf: Vec::new(),
            bytes: 0,
        }
    }

    /// Add a record. Returns `true` if the policy says it's time to flush.
    pub fn push(&mut self, record: LogRecord) -> bool {
        self.bytes += record.approx_bytes();
        self.buf.push(record);
        self.should_flush()
    }

    pub fn should_flush(&self) -> bool {
        self.buf.len() >= self.policy.max_rows || self.bytes >= self.policy.max_bytes
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Drain the buffer for encoding, resetting the running size.
    pub fn take(&mut self) -> Vec<LogRecord> {
        self.bytes = 0;
        std::mem::take(&mut self.buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(msg: &str) -> LogRecord {
        LogRecord {
            ts_millis: 1,
            level: Level::Info,
            service: "auth".into(),
            status: Some(200),
            message: msg.into(),
            trace_id: None,
            attrs: BTreeMap::new(),
        }
    }

    #[test]
    fn flushes_on_row_count() {
        let mut b = Batcher::new(BatchPolicy {
            max_rows: 3,
            max_bytes: usize::MAX,
        });
        assert!(!b.push(rec("a")));
        assert!(!b.push(rec("b")));
        assert!(b.push(rec("c"))); // hits max_rows
        let drained = b.take();
        assert_eq!(drained.len(), 3);
        assert!(b.is_empty());
        assert!(!b.should_flush());
    }
}
