//! Optional exact epoch-minute count metadata. The manifest publishes it in the
//! same commit as the immutable file that it describes.
use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::DataFile;

pub const MAX_FILE_BUCKETS: usize = 1_440;
const MAX_QUERY_METADATA_BYTES: u64 = 64 * 1024;
const MINUTE_MS: i64 = 60_000;
const VERSION: u8 = 1;
const MANIFEST_FIELD_BYTES: u64 = ",\"minute_summary\":".len() as u64;

/// Epoch-aligned minute start in milliseconds, total rows, exact ERROR rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinuteCount(pub i64, pub u64, pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MinuteSummary {
    version: u8,
    checksum: String,
    buckets: Vec<MinuteCount>,
}

pub fn minute_start(ts: i64) -> Option<i64> {
    ts.checked_mul(1_000_000)?;
    let start = ts.div_euclid(MINUTE_MS).checked_mul(MINUTE_MS)?;
    start.checked_mul(1_000_000)?;
    Some(start)
}

// Legacy manifests omit this field. A malformed optional entry in a readable
// manifest is treated as absent so it cannot take the raw scan path down.
pub(crate) fn deserialize_optional<'de, D>(
    deserializer: D,
) -> Result<Option<MinuteSummary>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(serde_json::from_value(value).ok())
}

impl MinuteSummary {
    pub fn for_file(file: &DataFile, buckets: Vec<MinuteCount>) -> Option<Self> {
        let mut summary = Self {
            version: VERSION,
            checksum: String::new(),
            buckets,
        };
        summary.checksum = summary.checksum_for(file);
        summary.validated_len(file)?;
        Some(summary)
    }

    fn checksum_for(&self, file: &DataFile) -> String {
        let mut hash = Sha256::new();
        hash.update([self.version]);
        hash.update(file.path.as_bytes());
        hash.update(file.rows.to_le_bytes());
        hash.update(file.bytes.to_le_bytes());
        hash.update(file.min_ts.to_le_bytes());
        hash.update(file.max_ts.to_le_bytes());
        for MinuteCount(start, total, errors) in &self.buckets {
            hash.update(start.to_le_bytes());
            hash.update(total.to_le_bytes());
            hash.update(errors.to_le_bytes());
        }
        format!("{:x}", hash.finalize())
    }

    fn validated_len(&self, file: &DataFile) -> Option<u64> {
        if !file.is_immutable_key()
            || self.version != VERSION
            || self.buckets.is_empty()
            || self.buckets.len() > MAX_FILE_BUCKETS
        {
            return None;
        }
        let first = minute_start(file.min_ts)?;
        let last = minute_start(file.max_ts)?;
        if self.buckets.first()?.0 != first || self.buckets.last()?.0 != last {
            return None;
        }
        let mut previous = None;
        let mut rows = 0u64;
        for &MinuteCount(start, total, errors) in &self.buckets {
            if start.rem_euclid(MINUTE_MS) != 0
                || previous.is_some_and(|prev| start <= prev)
                || total == 0
                || errors > total
            {
                return None;
            }
            previous = Some(start);
            rows = rows.checked_add(total)?;
        }
        if rows != file.rows || self.checksum != self.checksum_for(file) {
            return None;
        }
        let bytes = self.manifest_bytes_bound()?;
        (bytes <= (file.bytes / 100).min(MAX_QUERY_METADATA_BYTES)).then_some(bytes)
    }

    fn manifest_bytes_bound(&self) -> Option<u64> {
        let bytes = serde_json::to_vec_pretty(self).ok()?;
        // A summary is nested under a DataFile in the pretty manifest. Reserve
        // eight extra indentation bytes per line, plus field punctuation.
        let lines = bytes.iter().filter(|&&byte| byte == b'\n').count() as u64 + 2;
        Some(bytes.len() as u64 + lines * 8 + MANIFEST_FIELD_BYTES)
    }

    /// Limit the added metadata on the catalogue's write path, not only when a
    /// query eventually reads it. Uncovered files retain the raw scan path.
    pub fn cap_manifest(files: &mut [DataFile]) {
        let mut budget = MAX_QUERY_METADATA_BYTES;
        for file in files {
            if let Some(summary) = &file.minute_summary {
                if let Some(bytes) = summary
                    .manifest_bytes_bound()
                    .filter(|&bytes| bytes <= budget)
                {
                    budget -= bytes;
                } else {
                    file.minute_summary = None;
                }
            }
        }
    }

    /// All selected files must have complete coverage. Keep memory bounded even
    /// when the table has many overlapping file windows.
    pub(crate) fn merge(files: &[&DataFile]) -> Option<(Vec<MinuteCount>, u64)> {
        if files.is_empty() {
            return None;
        }
        let mut merged = BTreeMap::<i64, (u64, u64)>::new();
        let mut metadata_bytes = 0u64;
        for file in files {
            let summary = file.minute_summary.as_ref()?;
            metadata_bytes += summary.validated_len(file)?;
            if metadata_bytes > MAX_QUERY_METADATA_BYTES {
                return None;
            }
            for &MinuteCount(start, total, errors) in &summary.buckets {
                let sum = merged.entry(start).or_default();
                sum.0 = sum.0.checked_add(total)?;
                sum.1 = sum.1.checked_add(errors)?;
            }
        }
        let counts = merged
            .into_iter()
            .map(|(start, (total, errors))| MinuteCount(start, total, errors))
            .collect();
        Some((counts, metadata_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{manifest::Manifest, model::Tier};

    fn file(id: usize) -> DataFile {
        DataFile {
            path: format!("logs/hot/part-{id:016x}.parquet"),
            bytes: 1_000_000,
            rows: 3,
            min_ts: -1,
            max_ts: 60_001,
            tier: Tier::Hot,
            services: Vec::new(),
            levels: Vec::new(),
            row_groups: 1,
            message_trigrams: None,
            row_group_trigrams: None,
            minute_summary: None,
        }
    }

    fn counts() -> Vec<MinuteCount> {
        vec![MinuteCount(-60_000, 1, 1), MinuteCount(60_000, 2, 0)]
    }

    #[test]
    fn entire_scope_requires_valid_immutable_coverage() {
        let mut a = file(1);
        let mut b = file(2);
        a.minute_summary = MinuteSummary::for_file(&a, counts());
        assert!(MinuteSummary::merge(&[&a, &b]).is_none());
        b.minute_summary = MinuteSummary::for_file(&b, counts());
        let (merged, _) = MinuteSummary::merge(&[&a, &b]).unwrap();
        assert_eq!(
            merged,
            [MinuteCount(-60_000, 2, 2), MinuteCount(60_000, 4, 0)]
        );
        let (mut wrong_identity, mut corrupt) = (b.clone(), b);
        wrong_identity.path = file(3).path;
        corrupt.minute_summary.as_mut().unwrap().buckets[1].2 = 1;
        assert!(MinuteSummary::merge(&[&a, &wrong_identity]).is_none());
        assert!(MinuteSummary::merge(&[&a, &corrupt]).is_none());
        assert_eq!(minute_start(-60_000), Some(-60_000));
        assert_eq!(minute_start(-60_001), Some(-120_000));
        assert_eq!(minute_start(i64::MAX), None);
    }

    #[test]
    fn optional_manifest_data_is_tolerant_and_bounded_on_the_wire() {
        let mut m = Manifest::new("logs");
        for id in 1..1_000 {
            let mut file = file(id);
            file.minute_summary = MinuteSummary::for_file(&file, counts());
            m.files.push(file);
        }
        MinuteSummary::cap_manifest(&mut m.files);
        assert!(m.files.iter().any(|f| f.minute_summary.is_some()));
        assert!(m.files.iter().any(|f| f.minute_summary.is_none()));
        let with_counts = serde_json::to_vec_pretty(&m).unwrap().len();
        let mut without = m.clone();
        for file in &mut without.files {
            file.minute_summary = None;
        }
        assert!(with_counts - serde_json::to_vec_pretty(&without).unwrap().len() <= 64 * 1_024);

        let mut optional = serde_json::to_value(m).unwrap();
        optional["files"][0]["minute_summary"] = serde_json::json!({"version": "bad"});
        let readable: Manifest = serde_json::from_value(optional).unwrap();
        assert!(readable.files[0].minute_summary.is_none());
        assert_eq!(readable.files[0].rows, 3);
    }
}
