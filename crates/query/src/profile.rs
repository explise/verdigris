//! Opt-in request-local query instrumentation.
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use futures::stream::BoxStream;
use object_store::{path::Path, *};
use serde_json::{json, Value};
use verdigris_core::clock::Clock;

pub struct QueryProfile<'a> {
    clock: &'a dyn Clock,
    started: u64,
    last: u64,
    phases: serde_json::Map<String, Value>,
    capture_plan: bool,
    plan: Option<String>,
    pub(crate) io: Arc<ReadCounters>,
    parquet_bytes: usize,
    row_groups_pruned: usize,
    pages_pruned: usize,
}

impl<'a> QueryProfile<'a> {
    pub fn new(clock: &'a dyn Clock, started: u64, capture_plan: bool) -> Self {
        Self {
            clock,
            started,
            last: started,
            phases: Default::default(),
            capture_plan,
            plan: None,
            io: Default::default(),
            parquet_bytes: 0,
            row_groups_pruned: 0,
            pages_pruned: 0,
        }
    }

    pub fn mark(&mut self, phase: &'static str) {
        let now = self.clock.monotonic_micros();
        self.phases
            .insert(phase.into(), json!(now.saturating_sub(self.last)));
        self.last = now;
    }

    pub fn finish(self) -> Value {
        json!({
            "phases_us": self.phases,
            "total_us": self.last.saturating_sub(self.started),
            "object_reads": self.io.snapshot(),
            "parquet_bytes": self.parquet_bytes,
            "row_groups_pruned": self.row_groups_pruned,
            "pages_pruned": self.pages_pruned,
            "physical_plan": self.plan,
        })
    }

    pub(crate) fn record_plan(&mut self, plan: &dyn ExecutionPlan) {
        self.collect_metrics(plan);
        if self.capture_plan {
            // Bound the size of this optional response header. JSON escapes newlines.
            self.plan = Some(
                displayable(plan)
                    .indent(false)
                    .to_string()
                    .chars()
                    .take(1024)
                    .collect(),
            );
        }
    }

    fn collect_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if let Some(metrics) = plan.metrics() {
            for metric in metrics.iter() {
                match (metric.value().name(), metric.value()) {
                    ("bytes_scanned", MetricValue::Count { count, .. }) => {
                        self.parquet_bytes += count.value()
                    }
                    (
                        name,
                        MetricValue::PruningMetrics {
                            pruning_metrics, ..
                        },
                    ) => {
                        if name.starts_with("row_groups_pruned_") {
                            self.row_groups_pruned += pruning_metrics.pruned();
                        } else if name.starts_with("page_index_pages_pruned") {
                            self.pages_pruned += pruning_metrics.pruned();
                        }
                    }
                    _ => {}
                }
            }
        }
        for child in plan.children() {
            self.collect_metrics(child.as_ref());
        }
    }

    pub(crate) fn wrap(&self, store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(ProfileStore {
            inner: store,
            reads: self.io.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReadCounters {
    gets: AtomicU64,
    heads: AtomicU64,
    range_batches: AtomicU64,
    ranges: AtomicU64,
    bytes: AtomicU64,
}

impl ReadCounters {
    fn snapshot(&self) -> Value {
        json!({
            "gets": self.gets.load(Relaxed),
            "heads": self.heads.load(Relaxed),
            "range_batches": self.range_batches.load(Relaxed),
            "ranges": self.ranges.load(Relaxed),
            "result_bytes": self.bytes.load(Relaxed),
        })
    }
}

#[derive(Debug)]
struct ProfileStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<ReadCounters>,
}

impl fmt::Display for ProfileStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ProfileStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let head = options.head;
        let counter = if head {
            &self.reads.heads
        } else {
            &self.reads.gets
        };
        counter.fetch_add(1, Relaxed);
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.reads
                .bytes
                .fetch_add(result.range.end - result.range.start, Relaxed);
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        self.reads.range_batches.fetch_add(1, Relaxed);
        self.reads.ranges.fetch_add(ranges.len() as u64, Relaxed);
        // Preserve backend range coalescing. These are logical ranges, not S3 billable GETs.
        let result = self.inner.get_ranges(location, ranges).await?;
        self.reads
            .bytes
            .fetch_add(result.iter().map(|b| b.len() as u64).sum(), Relaxed);
        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use verdigris_core::clock::SimClock;

    #[tokio::test]
    async fn clock_and_store_measure_request_local_phases_and_reads() {
        let clock = SimClock::default();
        let mut profile = QueryProfile::new(&clock, 0, false);
        let inner = Arc::new(memory::InMemory::new());
        let path = Path::from("test.parquet");
        inner.put(&path, "12345678".into()).await.unwrap();
        let wrapped = profile.wrap(inner);
        wrapped.head(&path).await.unwrap();
        assert_eq!(wrapped.get_range(&path, 0..3).await.unwrap().len(), 3);
        assert_eq!(
            wrapped
                .get_ranges(&path, &[0..2, 5..8])
                .await
                .unwrap()
                .len(),
            2
        );
        clock.advance(2);
        profile.mark("setup");
        clock.advance(7);
        profile.mark("execution");
        let metrics = profile.finish();
        assert_eq!(
            metrics["phases_us"],
            json!({"setup": 2000, "execution": 7000})
        );
        assert_eq!(metrics["total_us"], 9000);
        assert_eq!(
            metrics["object_reads"],
            json!({
                "gets": 1, "heads": 1, "range_batches": 1, "ranges": 2, "result_bytes": 8,
            })
        );
    }
}
