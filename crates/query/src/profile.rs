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

pub struct QueryProfile {
    clock: Arc<dyn Clock>,
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

impl QueryProfile {
    pub fn new(clock: Arc<dyn Clock>, started: u64, capture_plan: bool) -> Self {
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
            clock: self.clock.clone(),
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
    /// Completed read intervals in request-local monotonic microseconds.
    timeline: std::sync::Mutex<Vec<(u64, u64)>>,
    read_sum_us: AtomicU64,
    read_max_us: AtomicU64,
    active: AtomicU64,
    max_concurrent: AtomicU64,
}

impl ReadCounters {
    pub(crate) fn start(&self, clock: &dyn Clock) -> u64 {
        let start = clock.monotonic_micros();
        let active = self.active.fetch_add(1, Relaxed) + 1;
        self.max_concurrent.fetch_max(active, Relaxed);
        start
    }

    pub(crate) fn finish(&self, clock: &dyn Clock, start: u64) {
        let end = clock.monotonic_micros();
        let elapsed = end.saturating_sub(start);
        self.read_sum_us.fetch_add(elapsed, Relaxed);
        self.read_max_us.fetch_max(elapsed, Relaxed);
        self.active.fetch_sub(1, Relaxed);
        self.timeline
            .lock()
            .expect("read timeline")
            .push((start, end));
    }

    /// Union of in-flight I/O intervals. Operators can run while I/O is in
    /// flight, so this does not establish removable critical-path wait.
    pub(crate) fn critical_wait_us(&self) -> u64 {
        let mut spans = self.timeline.lock().expect("read timeline").clone();
        spans.sort_unstable();
        let mut total = 0u64;
        let mut cur: Option<(u64, u64)> = None;
        for (start, end) in spans {
            match cur {
                None => cur = Some((start, end)),
                Some((cs, ce)) if start <= ce => cur = Some((cs, ce.max(end))),
                Some((cs, ce)) => {
                    total = total.saturating_add(ce.saturating_sub(cs));
                    cur = Some((start, end));
                }
            }
        }
        if let Some((cs, ce)) = cur {
            total = total.saturating_add(ce.saturating_sub(cs));
        }
        total
    }

    fn snapshot(&self) -> Value {
        json!({
            "gets": self.gets.load(Relaxed),
            "heads": self.heads.load(Relaxed),
            "range_batches": self.range_batches.load(Relaxed),
            "ranges": self.ranges.load(Relaxed),
            "result_bytes": self.bytes.load(Relaxed),
            "read_wait_sum_us": self.read_sum_us.load(Relaxed),
            "read_wait_max_us": self.read_max_us.load(Relaxed),
            "read_wait_critical_us": self.critical_wait_us(),
            "read_max_concurrent": self.max_concurrent.load(Relaxed),
        })
    }
}

struct ProfileStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<ReadCounters>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for ProfileStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProfileStore")
            .field("inner", &self.inner)
            .finish()
    }
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
        let start = self.reads.start(self.clock.as_ref());
        let result = self.inner.get_opts(location, options).await;
        self.reads.finish(self.clock.as_ref(), start);
        let result = result?;
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
        let start = self.reads.start(self.clock.as_ref());
        let result = self.inner.get_ranges(location, ranges).await;
        self.reads.finish(self.clock.as_ref(), start);
        let result = result?;
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
        let clock = Arc::new(SimClock::default());
        let mut profile = QueryProfile::new(clock.clone(), 0, false);
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
                "read_wait_sum_us": 0, "read_wait_max_us": 0,
                "read_wait_critical_us": 0, "read_max_concurrent": 1,
            })
        );
    }

    #[test]
    fn overlapping_reads_use_the_union_not_the_sum() {
        let reads = ReadCounters::default();
        // Two reads overlap 10..30: sum is 60 us, critical path is 40 us.
        reads.timeline.lock().unwrap().extend([(0, 20), (10, 40)]);
        reads.read_sum_us.store(50, Relaxed);
        assert_eq!(reads.critical_wait_us(), 40);
    }
}
