//! Core domain types shared across the system.

use serde::{Deserialize, Serialize};

/// Log severity. Decides routing (which tier/prefix), never price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
        }
    }

    /// Default write-time tier routing (matches the frontend's routing rules).
    /// Real, configurable routing arrives with tiering (build step 3); this is
    /// the default mapping.
    pub fn default_tier(self) -> Tier {
        match self {
            Level::Error => Tier::Hot,
            Level::Warn | Level::Info => Tier::Warm,
            Level::Debug => Tier::Cold,
        }
    }
}

/// Logical storage tier a log lands in. Severity decides the *tier*, never the
/// price (see the pricing principle in CLAUDE.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Hot,
    Warm,
    Cold,
}

/// The S3 storage class backing a tier. Drives the cost model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageClass {
    Standard,
    GlacierInstant,
    GlacierFlexible,
    GlacierDeepArchive,
}

impl Tier {
    /// All tiers in a fixed, hot→cold order. Used to iterate deterministically
    /// (no HashMap ordering) so the control plane stays simulation-stable.
    pub const ALL: [Tier; 3] = [Tier::Hot, Tier::Warm, Tier::Cold];

    /// Stable index into a `[_; 3]` keyed by tier.
    pub fn index(self) -> usize {
        match self {
            Tier::Hot => 0,
            Tier::Warm => 1,
            Tier::Cold => 2,
        }
    }

    /// Lowercase prefix name used in object keys.
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Hot => "hot",
            Tier::Warm => "warm",
            Tier::Cold => "cold",
        }
    }

    /// Default storage class for a tier. Configurable later; this is the
    /// out-of-the-box mapping.
    pub fn default_class(self) -> StorageClass {
        match self {
            Tier::Hot => StorageClass::Standard,
            Tier::Warm => StorageClass::GlacierInstant,
            Tier::Cold => StorageClass::GlacierFlexible,
        }
    }
}
