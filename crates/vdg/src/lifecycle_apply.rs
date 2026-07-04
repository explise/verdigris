//! `vdg lifecycle --apply` — push the generated lifecycle policy onto the S3
//! bucket via `PutBucketLifecycleConfiguration`.
//!
//! `object_store` has no lifecycle API, so this is the one place we reach for the
//! real `aws-sdk-s3`. It is gated behind the optional `apply` feature so the
//! default and `serve` builds stay light and offline — credentials resolve
//! through the standard AWS chain (env / profile / IRSA).

use anyhow::Context;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleFilter, Transition, TransitionStorageClass,
};
use verdigris_core::config::StorageConfig;
use verdigris_core::lifecycle::LifecyclePolicy;

/// Apply `policy` to the S3 bucket named in `storage`. Errors clearly if the
/// backend isn't S3.
pub async fn apply(storage: &StorageConfig, policy: &LifecyclePolicy) -> anyhow::Result<()> {
    let (bucket, region, endpoint) = match storage {
        StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            ..
        } => (bucket.clone(), region.clone(), endpoint.clone()),
        _ => anyhow::bail!(
            "lifecycle --apply requires an S3 storage backend \
             (set [storage] backend = \"s3\" in the config)"
        ),
    };

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(r) = region {
        loader = loader.region(aws_config::Region::new(r));
    }
    if let Some(ep) = &endpoint {
        loader = loader.endpoint_url(ep);
    }
    let shared = loader.load().await;
    let client = aws_sdk_s3::Client::new(&shared);

    let configuration = to_aws_config(policy)?;
    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(configuration)
        .send()
        .await
        .with_context(|| format!("PutBucketLifecycleConfiguration on bucket {bucket}"))?;

    println!("applied lifecycle policy to s3://{bucket} ({} rule(s))", policy.rules.len());
    Ok(())
}

/// Convert our serde policy representation into the aws-sdk builder types. Keeps
/// the applied policy identical to what `vdg lifecycle` prints.
fn to_aws_config(policy: &LifecyclePolicy) -> anyhow::Result<BucketLifecycleConfiguration> {
    let mut rules = Vec::new();
    for r in &policy.rules {
        let transitions: Vec<Transition> = r
            .transitions
            .iter()
            .map(|t| {
                Transition::builder()
                    .days(t.days as i32)
                    .storage_class(TransitionStorageClass::from(t.storage_class.as_str()))
                    .build()
            })
            .collect();

        let rule = LifecycleRule::builder()
            .id(&r.id)
            .status(ExpirationStatus::Enabled)
            .filter(
                LifecycleRuleFilter::builder()
                    .prefix(r.filter.prefix.clone())
                    .build(),
            )
            .set_transitions(Some(transitions))
            .expiration(
                LifecycleExpiration::builder()
                    .days(r.expiration.days as i32)
                    .build(),
            )
            .build()
            .context("building S3 lifecycle rule")?;
        rules.push(rule);
    }

    BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .context("building S3 lifecycle configuration")
}
