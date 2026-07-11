//! OTLP/HTTP JSON logs → canonical `LogRecord` mapping.
//!
//! This maps the OpenTelemetry OTLP/JSON logs encoding (the body of a
//! `POST /v1/otlp/logs` request, `Content-Type: application/json`) onto the same
//! `LogRecord` the rest of the ingest path speaks, so an OTLP exporter, a
//! `POST /v1/ingest` NDJSON shipper, and the Vector DaemonSet all land in one
//! Parquet schema.
//!
//! We deliberately support only OTLP/**JSON** (not protobuf/gRPC): it keeps the
//! dependency surface to `serde_json` and covers the common OTel HTTP exporter.
//!
//! Field mapping (OTLP → LogRecord):
//!   - `timeUnixNano` (ns, string or number) → `ts_millis` (ns / 1e6);
//!     falls back to `observedTimeUnixNano`.
//!   - `severityText` (if recognized) else `severityNumber` → `level`.
//!   - `body.stringValue` → `message`.
//!   - resource attribute `service.name` → `service`.
//!   - a status-ish attribute (`http.status_code`, `http.response.status_code`,
//!     `status_code`, `status`) → `status`.
//!   - `traceId` → `trace_id`.
//!   - remaining log-record + resource attributes → `attrs` (stringified).

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;
use verdigris_core::batch::LogRecord;
use verdigris_core::model::Level;

#[derive(Debug, Deserialize)]
pub struct OtlpLogsRequest {
    #[serde(default, rename = "resourceLogs")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Deserialize)]
pub struct ResourceLogs {
    #[serde(default)]
    pub resource: Option<Resource>,
    #[serde(default, rename = "scopeLogs")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Deserialize)]
pub struct Resource {
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeLogs {
    #[serde(default, rename = "logRecords")]
    pub log_records: Vec<OtlpLogRecord>,
}

#[derive(Debug, Deserialize)]
pub struct OtlpLogRecord {
    #[serde(default, rename = "timeUnixNano")]
    pub time_unix_nano: Option<Value>,
    #[serde(default, rename = "observedTimeUnixNano")]
    pub observed_time_unix_nano: Option<Value>,
    #[serde(default, rename = "severityNumber")]
    pub severity_number: Option<i64>,
    #[serde(default, rename = "severityText")]
    pub severity_text: Option<String>,
    #[serde(default)]
    pub body: Option<AnyValue>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default, rename = "traceId")]
    pub trace_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    #[serde(default)]
    pub value: Option<AnyValue>,
}

/// An OTLP `AnyValue`. Only the scalar variants matter for logs; complex ones are
/// rendered via their JSON form as a fallback.
#[derive(Debug, Default, Deserialize)]
pub struct AnyValue {
    #[serde(default, rename = "stringValue")]
    pub string_value: Option<String>,
    #[serde(default, rename = "boolValue")]
    pub bool_value: Option<bool>,
    #[serde(default, rename = "intValue")]
    pub int_value: Option<Value>,
    #[serde(default, rename = "doubleValue")]
    pub double_value: Option<f64>,
}

impl AnyValue {
    /// Render to a display string (used for `attrs` and `message`).
    fn to_display_string(&self) -> Option<String> {
        if let Some(s) = &self.string_value {
            return Some(s.clone());
        }
        if let Some(b) = self.bool_value {
            return Some(b.to_string());
        }
        if let Some(i) = &self.int_value {
            return Some(json_number_string(i));
        }
        if let Some(d) = self.double_value {
            return Some(d.to_string());
        }
        None
    }

    /// Best-effort integer extraction (for status-code attributes). Handles the
    /// proto3-JSON convention where int64 is encoded as a string.
    fn as_i64(&self) -> Option<i64> {
        if let Some(v) = &self.int_value {
            return json_to_i64(v);
        }
        if let Some(d) = self.double_value {
            return Some(d as i64);
        }
        if let Some(s) = &self.string_value {
            return s.trim().parse().ok();
        }
        None
    }
}

/// Parse an OTLP/JSON logs request body into canonical records. Malformed JSON is
/// an error (the whole request is rejected); well-formed-but-empty yields `[]`.
pub fn parse_otlp_json(body: &str) -> Result<Vec<LogRecord>, String> {
    let req: OtlpLogsRequest =
        serde_json::from_str(body).map_err(|e| format!("invalid OTLP/JSON logs body: {e}"))?;
    Ok(otlp_to_records(req))
}

/// Flatten an OTLP logs request into `LogRecord`s, applying resource attributes
/// (notably `service.name`) to every record under that resource.
pub fn otlp_to_records(req: OtlpLogsRequest) -> Vec<LogRecord> {
    let mut out = Vec::new();
    for rl in req.resource_logs {
        // Resource-level context, applied to every record beneath it.
        let mut service = String::new();
        let mut resource_attrs: BTreeMap<String, String> = BTreeMap::new();
        if let Some(res) = &rl.resource {
            for kv in &res.attributes {
                let val = kv.value.as_ref().and_then(AnyValue::to_display_string);
                if kv.key == "service.name" {
                    if let Some(v) = &val {
                        service = v.clone();
                    }
                }
                if let Some(v) = val {
                    resource_attrs.insert(kv.key.clone(), v);
                }
            }
        }

        for sl in rl.scope_logs {
            for lr in sl.log_records {
                out.push(map_record(lr, &service, &resource_attrs));
            }
        }
    }
    out
}

fn map_record(
    lr: OtlpLogRecord,
    service: &str,
    resource_attrs: &BTreeMap<String, String>,
) -> LogRecord {
    let ts_millis = lr
        .time_unix_nano
        .as_ref()
        .and_then(json_to_i64)
        .or_else(|| lr.observed_time_unix_nano.as_ref().and_then(json_to_i64))
        .map(|ns| ns / 1_000_000)
        .unwrap_or(0);

    let level = level_from(lr.severity_text.as_deref(), lr.severity_number);

    let message = lr
        .body
        .as_ref()
        .and_then(AnyValue::to_display_string)
        .unwrap_or_default();

    // Start from resource attributes so a per-record attribute of the same key
    // can override, and pull out any status-code attribute as a first-class field.
    let mut attrs = resource_attrs.clone();
    let mut status: Option<i32> = None;
    for kv in &lr.attributes {
        let is_status = matches!(
            kv.key.as_str(),
            "http.status_code" | "http.response.status_code" | "status_code" | "status"
        );
        if is_status && status.is_none() {
            status = kv.value.as_ref().and_then(AnyValue::as_i64).map(|i| i as i32);
        }
        if let Some(v) = kv.value.as_ref().and_then(AnyValue::to_display_string) {
            attrs.insert(kv.key.clone(), v);
        }
    }

    let trace_id = lr.trace_id.filter(|s| !s.is_empty());

    LogRecord {
        ts_millis,
        level,
        service: service.to_string(),
        status,
        message,
        trace_id,
        attrs,
    }
}

/// Map OTLP severity onto our four levels. Prefer a recognized `severityText`;
/// otherwise use the numeric range (OTel: 1-4 TRACE, 5-8 DEBUG, 9-12 INFO,
/// 13-16 WARN, 17-20 ERROR, 21-24 FATAL). Falls back to INFO.
fn level_from(text: Option<&str>, number: Option<i64>) -> Level {
    if let Some(t) = text {
        if let Some(l) = level_from_text(t) {
            return l;
        }
    }
    if let Some(n) = number {
        return match n {
            1..=8 => Level::Debug,   // TRACE + DEBUG
            9..=12 => Level::Info,   // INFO
            13..=16 => Level::Warn,  // WARN
            n if n >= 17 => Level::Error, // ERROR + FATAL
            _ => Level::Info,
        };
    }
    Level::Info
}

/// Case-insensitive severity-text parse. Returns `None` for unrecognized text so
/// the caller can fall back to the numeric severity.
fn level_from_text(s: &str) -> Option<Level> {
    match s.trim().to_ascii_lowercase().as_str() {
        "error" | "err" | "fatal" | "critical" | "crit" | "panic" => Some(Level::Error),
        "warn" | "warning" => Some(Level::Warn),
        "info" | "information" | "notice" => Some(Level::Info),
        "debug" | "trace" | "verbose" => Some(Level::Debug),
        _ => None,
    }
}

/// Interpret a JSON value as i64, accepting the proto3-JSON string-encoded ints.
fn json_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Render a JSON number-or-string value to a plain string (no surrounding quotes).
fn json_number_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_full_otlp_payload() {
        let body = r#"{
          "resourceLogs": [{
            "resource": { "attributes": [
              { "key": "service.name", "value": { "stringValue": "checkout" } },
              { "key": "deploy.env",   "value": { "stringValue": "prod" } }
            ]},
            "scopeLogs": [{
              "logRecords": [{
                "timeUnixNano": "1717171717000000000",
                "severityNumber": 17,
                "severityText": "ERROR",
                "body": { "stringValue": "payment failed" },
                "attributes": [
                  { "key": "http.status_code", "value": { "intValue": "500" } },
                  { "key": "region", "value": { "stringValue": "us-east-1" } }
                ],
                "traceId": "abc123"
              }]
            }]
          }]
        }"#;
        let recs = parse_otlp_json(body).unwrap();
        assert_eq!(recs.len(), 1);
        let r = &recs[0];
        assert_eq!(r.ts_millis, 1_717_171_717_000);
        assert_eq!(r.level, Level::Error);
        assert_eq!(r.service, "checkout");
        assert_eq!(r.status, Some(500));
        assert_eq!(r.message, "payment failed");
        assert_eq!(r.trace_id.as_deref(), Some("abc123"));
        assert_eq!(r.attrs.get("region").map(String::as_str), Some("us-east-1"));
        // resource attributes are carried onto the record too.
        assert_eq!(r.attrs.get("deploy.env").map(String::as_str), Some("prod"));
        // service.name is promoted to a column but also kept in attrs — either is fine.
    }

    #[test]
    fn severity_number_without_text() {
        let body = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
            {"timeUnixNano": 2000000, "severityNumber": 5, "body": {"stringValue":"d"}},
            {"timeUnixNano": 3000000, "severityNumber": 13, "body": {"stringValue":"w"}}
        ]}]}]}"#;
        let recs = parse_otlp_json(body).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].level, Level::Debug); // 5 -> DEBUG
        assert_eq!(recs[0].ts_millis, 2); // 2_000_000 ns -> 2 ms
        assert_eq!(recs[1].level, Level::Warn); // 13 -> WARN
    }

    #[test]
    fn missing_fields_default_gracefully() {
        // No severity, no service, no time -> INFO / empty / 0.
        let body = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
            {"body": {"stringValue":"hello"}}
        ]}]}]}"#;
        let recs = parse_otlp_json(body).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].level, Level::Info);
        assert_eq!(recs[0].service, "");
        assert_eq!(recs[0].ts_millis, 0);
        assert_eq!(recs[0].message, "hello");
        assert!(recs[0].trace_id.is_none());
    }

    #[test]
    fn observed_time_fallback_and_empty() {
        let body =
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"observedTimeUnixNano":"5000000"}]}]}]}"#;
        let recs = parse_otlp_json(body).unwrap();
        assert_eq!(recs[0].ts_millis, 5);

        // Well-formed but no records.
        assert!(parse_otlp_json(r#"{"resourceLogs":[]}"#).unwrap().is_empty());

        // Malformed JSON -> error.
        assert!(parse_otlp_json("not json").is_err());
    }
}
