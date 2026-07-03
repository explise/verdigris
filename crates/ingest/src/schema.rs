//! The Parquet/Arrow schema for log records.
//!
//! Known fields are typed columns (fast `WHERE`); arbitrary extra attributes
//! live in `attrs_json` so new fields never force a schema migration. Real
//! per-field schema evolution is a later hardening step.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::Arc;

pub fn log_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("level", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("status", DataType::Int32, true),
        Field::new("message", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("attrs_json", DataType::Utf8, true),
    ]))
}
