//! S3 lifecycle policy generation.
//!
//! Write-time routing (see `RoutingConfig`) decides the *initial* prefix/class a
//! log lands in. Lifecycle rules then demote objects to colder storage classes
//! as they age, and finally expire them. This module renders the policy as a
//! serde-serializable struct mirroring AWS's `PutBucketLifecycleConfiguration`,
//! so the shell can emit JSON to apply with the AWS CLI/SDK.
//!
//! It is informational on the local backend (no lifecycle there) and applied to
//! the bucket on S3.

use crate::config::LifecycleConfig;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct LifecyclePolicy {
    #[serde(rename = "Rules")]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Rule {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "Filter")]
    pub filter: Filter,
    #[serde(rename = "Transitions")]
    pub transitions: Vec<Transition>,
    #[serde(rename = "Expiration")]
    pub expiration: Expiration,
}

#[derive(Debug, Clone, Serialize)]
pub struct Filter {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Transition {
    #[serde(rename = "Days")]
    pub days: u32,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Expiration {
    #[serde(rename = "Days")]
    pub days: u32,
}

/// Build the lifecycle policy for a table prefix from the configured ages.
/// Objects transition to Glacier Instant Retrieval, then Glacier Flexible, then
/// expire — the standard hot → warm → cold → delete progression.
pub fn policy_for(table: &str, lc: &LifecycleConfig) -> LifecyclePolicy {
    LifecyclePolicy {
        rules: vec![Rule {
            id: format!("verdigris-{table}-tiering"),
            status: "Enabled".to_string(),
            filter: Filter {
                prefix: format!("{table}/"),
            },
            transitions: vec![
                Transition {
                    days: lc.hot_to_warm_days,
                    storage_class: "GLACIER_IR".to_string(),
                },
                Transition {
                    days: lc.warm_to_cold_days,
                    storage_class: "GLACIER".to_string(),
                },
            ],
            expiration: Expiration {
                days: lc.expire_days,
            },
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_ordered_transitions() {
        let p = policy_for("logs", &LifecycleConfig::default());
        assert_eq!(p.rules.len(), 1);
        let r = &p.rules[0];
        assert_eq!(r.filter.prefix, "logs/");
        assert_eq!(r.transitions[0].days, 3);
        assert_eq!(r.transitions[0].storage_class, "GLACIER_IR");
        assert_eq!(r.transitions[1].days, 30);
        assert_eq!(r.expiration.days, 400);
    }
}
