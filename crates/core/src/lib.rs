//! verdigris-core — the sans-I/O control plane.
//!
//! This crate is pure logic plus the *seam* trait definitions. It must never
//! perform real I/O, read the wall clock, spawn threads, or draw real
//! randomness directly — those all go through the seams (`Clock`, `Rng`, and,
//! in sibling crates, `ObjectStore` and `ScanExecutor`). That discipline is
//! what lets the whole control plane run deterministically under simulation.
//! See `docs/dst-architecture.md`.

pub mod alert;
pub mod auth;
pub mod batch;
pub mod clock;
pub mod config;
pub mod cost;
pub mod estimate;
pub mod lifecycle;
pub mod manifest;
pub mod model;
pub mod rng;
pub mod search;

pub use batch::{BatchPolicy, Batcher, LogRecord};
pub use clock::{Clock, Millis, SimClock};
pub use config::Config;
pub use manifest::{DataFile, Manifest};
pub use rng::{Rng, SeededRng};
