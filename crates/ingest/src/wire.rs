//! The wire format for ingested logs — one JSON object per record.
//!
//! This is the shared contract for BOTH the `vdg ingest --from <file.ndjson>`
//! CLI path and the `POST /v1/ingest` HTTP endpoint, so a log shipped by curl,
//! an NDJSON file, or the Vector DaemonSet all decode identically.
//!
//! `level` is parsed **case-insensitively** with a lenient fallback, because
//! real log sources emit `"error"`, `"ERROR"`, `"warning"`, `"trace"`, etc.
//! Unrecognized severities map to INFO rather than dropping the line.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer};
use verdigris_core::batch::LogRecord;
use verdigris_core::model::Level;

/// One JSON log record. `ts_millis`, `service`, and `message` are required;
/// everything else is optional (`level` defaults to INFO).
#[derive(Debug, Clone, Deserialize)]
pub struct JsonLog {
    pub ts_millis: i64,
    #[serde(default = "default_level", deserialize_with = "de_level")]
    pub level: Level,
    pub service: String,
    #[serde(default)]
    pub status: Option<i32>,
    pub message: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub attrs: BTreeMap<String, String>,
}

impl From<JsonLog> for LogRecord {
    fn from(j: JsonLog) -> Self {
        LogRecord {
            ts_millis: j.ts_millis,
            level: j.level,
            service: j.service,
            status: j.status,
            message: j.message,
            trace_id: j.trace_id,
            attrs: j.attrs,
        }
    }
}

fn default_level() -> Level {
    Level::Info
}

/// Case-insensitive, lenient severity parse. Maps common aliases; anything
/// unrecognized falls back to INFO so a stray severity never drops a log.
fn de_level<'de, D>(d: D) -> Result<Level, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    Ok(match s.trim().to_ascii_lowercase().as_str() {
        "error" | "err" | "fatal" | "critical" | "crit" | "panic" => Level::Error,
        "warn" | "warning" => Level::Warn,
        "debug" | "trace" | "verbose" => Level::Debug,
        _ => Level::Info, // "info", "notice", unknown, empty
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_record() {
        let j: JsonLog = serde_json::from_str(
            r#"{"ts_millis":42,"level":"ERROR","service":"auth","status":500,"message":"boom","trace_id":"abc","attrs":{"region":"us-east-1"}}"#,
        )
        .unwrap();
        let r = LogRecord::from(j);
        assert_eq!(r.ts_millis, 42);
        assert_eq!(r.level, Level::Error);
        assert_eq!(r.status, Some(500));
        assert_eq!(r.trace_id.as_deref(), Some("abc"));
        assert_eq!(r.attrs.get("region").map(String::as_str), Some("us-east-1"));
    }

    #[test]
    fn level_is_case_insensitive_and_lenient() {
        let cases = [
            ("error", Level::Error),
            ("Error", Level::Error),
            ("FATAL", Level::Error),
            ("warning", Level::Warn),
            ("TRACE", Level::Debug),
            ("notice", Level::Info),
            ("nonsense", Level::Info),
        ];
        for (input, want) in cases {
            let j: JsonLog = serde_json::from_str(&format!(
                r#"{{"ts_millis":1,"level":"{input}","service":"s","message":"m"}}"#
            ))
            .unwrap();
            assert_eq!(j.level, want, "level {input:?}");
        }
    }

    #[test]
    fn optional_fields_default() {
        // No level, status, trace_id, or attrs.
        let j: JsonLog =
            serde_json::from_str(r#"{"ts_millis":1,"service":"s","message":"m"}"#).unwrap();
        assert_eq!(j.level, Level::Info);
        assert_eq!(j.status, None);
        assert!(j.trace_id.is_none());
        assert!(j.attrs.is_empty());
    }
}
