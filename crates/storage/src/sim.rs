//! `SimObjectStore` — the simulation impl of the object-store seam (ADR-001).
//!
//! This is the fourth DST seam: in production the store is `AmazonS3` (or local
//! / in-memory) built by [`crate::build`]; under Deterministic Simulation Testing
//! it is this type. It is a real [`ObjectStore`] — a drop-in `Arc<dyn ObjectStore>`
//! — so the *same* production code paths run against it unchanged. On top of the
//! byte mechanics it layers the four things the simulator needs and a cloud bucket
//! has but `InMemory` doesn't:
//!
//! 1. **Modeled latency off the injected [`Clock`].** Every operation advances the
//!    *simulated* clock by a latency drawn from [`verdigris_core::cost`] — never real
//!    time. A "trillion-row" run still finishes in real-time seconds.
//! 2. **Per-object storage class.** Each key carries a [`StorageClass`]; severity
//!    routing / lifecycle transitions set it, queries read it.
//! 3. **Glacier-restore semantics.** A GET against a Glacier-Flexible/Deep-Archive
//!    object fails until a restore has been requested *and* enough simulated time
//!    has passed — this is the state machine the restore-UX tests drive.
//! 4. **Deterministic fault injection** via the injected [`Rng`] seam, and **cost
//!    metering** so a test can assert the estimator predicted exactly what the
//!    store actually billed.
//!
//! The latency/cost numbers come from `verdigris_core::cost`, the *same* module the
//! user-facing estimator uses — per ADR-001 they must share code, never diverge,
//! or the simulation would lie about what production bills.
//!
//! Byte storage itself is delegated to the crate's own [`InMemory`] store rather
//! than reimplemented, so the fiddly parts (ranges, conditional puts, multipart,
//! list streams) stay exactly spec-correct. The one known fidelity gap: `InMemory`
//! stamps `last_modified` from the wall clock, not our sim clock — harmless because
//! no control-plane logic reads it (we age data by manifest `min_ts`/`max_ts`).
//!
//! Construction takes seam handles (`Clock`, seed) directly, so it is wired by the
//! DST harness, not by config-driven [`crate::build`] (which stays prod-only).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use object_store::memory::InMemory;
use object_store::{Error as OsError, Result as OsResult};

use verdigris_core::clock::{Clock, Millis};
use verdigris_core::cost::{self, RetrievalMode};
use verdigris_core::model::StorageClass;
use verdigris_core::rng::{Rng, SeededRng};

/// What the store knows about one object beyond its bytes.
#[derive(Debug, Clone)]
struct ClassEntry {
    class: StorageClass,
    /// The retrieval mode a restore was requested with (drives cost + latency).
    mode: RetrievalMode,
    /// Logical time at which a requested restore completes. `None` = no restore
    /// requested yet. Only meaningful for classes that need thawing.
    restore_available_at: Option<Millis>,
}

impl ClassEntry {
    fn new(class: StorageClass) -> Self {
        Self {
            class,
            mode: RetrievalMode::Standard,
            restore_available_at: None,
        }
    }
}

/// Mutable, interior state. Kept behind one `Mutex` and never held across an
/// `.await`, so the store stays `Send + Sync` and the lock order is trivial.
struct State {
    classes: BTreeMap<Path, ClassEntry>,
    rng: SeededRng,
    /// Probability of a synthetic fault per fallible op, in parts-per-million.
    /// `0` (the default) means faults are off and runs are clean.
    fault_rate_ppm: u32,
    /// Running totals so a test can cross-check the estimator against what the
    /// store actually "billed" / served.
    metered_get_bytes: u64,
    metered_retrieval_usd: f64,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("classes", &self.classes)
            .field("fault_rate_ppm", &self.fault_rate_ppm)
            .field("metered_get_bytes", &self.metered_get_bytes)
            .field("metered_retrieval_usd", &self.metered_retrieval_usd)
            .finish_non_exhaustive()
    }
}

/// See module docs. Cheap to wrap in `Arc`.
pub struct SimObjectStore {
    inner: InMemory,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
}

impl SimObjectStore {
    /// New empty sim store driven by `clock`. Seed defaults to 0 and faults are
    /// off; use [`with_seed`](Self::with_seed) / [`with_fault_rate_ppm`](Self::with_fault_rate_ppm).
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: InMemory::new(),
            clock,
            state: Mutex::new(State {
                classes: BTreeMap::new(),
                rng: SeededRng::from_seed(0),
                fault_rate_ppm: 0,
                metered_get_bytes: 0,
                metered_retrieval_usd: 0.0,
            }),
        }
    }

    /// Reseed the fault RNG. One seed reproduces an entire run (ADR-001).
    pub fn with_seed(self, seed: u64) -> Self {
        {
            let mut s = self.lock();
            s.rng = SeededRng::from_seed(seed);
        }
        self
    }

    /// Inject faults at `ppm` parts-per-million per fallible op.
    pub fn with_fault_rate_ppm(self, ppm: u32) -> Self {
        {
            let mut s = self.lock();
            s.fault_rate_ppm = ppm;
        }
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("sim store mutex poisoned")
    }

    /// Set the storage class of an object (e.g. a lifecycle transition demoting
    /// it to a colder tier). Resets any prior restore — a demoted object must be
    /// thawed again before it reads.
    pub fn set_class(&self, path: &Path, class: StorageClass) {
        let mut s = self.lock();
        s.classes.insert(path.clone(), ClassEntry::new(class));
    }

    /// The class of `path`. Objects written without an explicit class (or via
    /// multipart) are treated as [`StorageClass::Standard`].
    pub fn class_of(&self, path: &Path) -> StorageClass {
        self.lock()
            .classes
            .get(path)
            .map(|e| e.class)
            .unwrap_or(StorageClass::Standard)
    }

    /// Request a Glacier restore of `path` with `mode`. Returns the logical time
    /// at which the object becomes readable; for classes that need no thaw this
    /// is simply "now". Drives the restore state machine the UX tests exercise.
    pub fn request_restore(&self, path: &Path, mode: RetrievalMode) -> Millis {
        let now = self.clock.now_millis();
        let mut s = self.lock();
        let entry = s
            .classes
            .entry(path.clone())
            .or_insert_with(|| ClassEntry::new(StorageClass::Standard));
        entry.mode = mode;
        let available_at = now + cost::restore_latency_ms(entry.class, mode);
        entry.restore_available_at = Some(available_at);
        available_at
    }

    /// Whether a GET on `path` would succeed at the current simulated time
    /// (i.e. the class needs no thaw, or a requested restore has completed).
    pub fn is_readable(&self, path: &Path) -> bool {
        let now = self.clock.now_millis();
        let s = self.lock();
        match s.classes.get(path) {
            None => true, // unknown ⇒ treated as Standard
            Some(e) => readable_now(e, now),
        }
    }

    /// Total bytes served by successful GETs so far.
    pub fn metered_get_bytes(&self) -> u64 {
        self.lock().metered_get_bytes
    }

    /// Total retrieval dollars the store has billed across successful GETs. A
    /// query that scans the same files the estimator priced must land here too.
    pub fn metered_retrieval_usd(&self) -> f64 {
        self.lock().metered_retrieval_usd
    }

    /// Draw one fault decision from the seeded RNG.
    fn rolled_fault(&self) -> bool {
        let mut s = self.lock();
        if s.fault_rate_ppm == 0 {
            return false;
        }
        let roll = (s.rng.next_u64() % 1_000_000) as u32;
        roll < s.fault_rate_ppm
    }

    fn fault(op: &'static str) -> OsError {
        OsError::Generic {
            store: "SimObjectStore",
            source: format!("injected fault on {op}").into(),
        }
    }
}

/// Is an object with this class/restore state readable at logical time `now`?
fn readable_now(e: &ClassEntry, now: Millis) -> bool {
    match e.class {
        // Queried in place; no thaw needed.
        StorageClass::Standard | StorageClass::GlacierInstant => true,
        StorageClass::GlacierFlexible | StorageClass::GlacierDeepArchive => {
            matches!(e.restore_available_at, Some(at) if now >= at)
        }
    }
}

impl std::fmt::Debug for SimObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimObjectStore")
            .field("now_millis", &self.clock.now_millis())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for SimObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimObjectStore")
    }
}

#[async_trait]
impl ObjectStore for SimObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.rolled_fault() {
            return Err(Self::fault("put"));
        }
        // Writes land hot (Standard) by default; lifecycle/routing demote later.
        self.lock()
            .classes
            .entry(location.clone())
            .or_insert_with(|| ClassEntry::new(StorageClass::Standard));
        self.clock
            .sleep(cost::first_byte_latency_ms(StorageClass::Standard))
            .await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        // Multipart uploads are delegated wholesale; the completed object is
        // tracked as Standard on first GET if not set explicitly.
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if self.rolled_fault() {
            return Err(Self::fault("get"));
        }
        let now = self.clock.now_millis();
        // Resolve class + restore gate under the lock, then drop it before await.
        let (class, mode) = {
            let s = self.lock();
            let entry = s
                .classes
                .get(location)
                .cloned()
                .unwrap_or_else(|| ClassEntry::new(StorageClass::Standard));
            if !readable_now(&entry, now) {
                let why = match entry.restore_available_at {
                    None => format!(
                        "object in {:?} has not been restored — request_restore first",
                        entry.class
                    ),
                    Some(at) => format!(
                        "restore of {:?} object in progress, ready at t={}ms (now t={}ms)",
                        entry.class, at, now
                    ),
                };
                return Err(OsError::Generic {
                    store: "SimObjectStore",
                    source: why.into(),
                });
            }
            (entry.class, entry.mode)
        };

        self.clock
            .sleep(cost::first_byte_latency_ms(class))
            .await;
        let result = self.inner.get_opts(location, options).await?;

        // Meter what this retrieval cost, from the shared cost model.
        let bytes = result.meta.size;
        let gib = bytes as f64 / cost::GIB;
        let usd = gib * cost::retrieval_usd_per_gib(class, mode);
        {
            let mut s = self.lock();
            s.metered_get_bytes += bytes;
            s.metered_retrieval_usd += usd;
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        // Byte deletion delegates to inner; class metadata for deleted keys is
        // left to be overwritten on any future put (stale entries are inert).
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        // List is a synchronous stream constructor — its modeled latency is folded
        // into the per-GET model rather than added here.
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await?;
        // Carry the source's class to the copy so tiering survives compaction.
        let mut s = self.lock();
        if let Some(entry) = s.classes.get(from).cloned() {
            s.classes.insert(to.clone(), entry);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use verdigris_core::clock::SimClock;

    fn store() -> (Arc<SimClock>, SimObjectStore) {
        let clock = Arc::new(SimClock::new(0));
        let store = SimObjectStore::new(clock.clone());
        (clock, store)
    }

    #[tokio::test]
    async fn put_get_round_trips_and_advances_sim_clock() {
        let (clock, store) = store();
        let p = Path::from("logs/hot/a.parquet");
        store.put(&p, PutPayload::from_static(b"hello")).await.unwrap();
        let got = store.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&*got, b"hello");
        // put + get each advanced the simulated clock; no real time elapsed.
        assert!(clock.now_millis() > 0);
    }

    #[tokio::test]
    async fn cold_object_needs_restore_before_get() {
        let (clock, store) = store();
        let p = Path::from("logs/cold/c.parquet");
        store.put(&p, PutPayload::from_static(b"frozen")).await.unwrap();
        store.set_class(&p, StorageClass::GlacierFlexible);

        // Un-restored cold read fails.
        assert!(store.get(&p).await.is_err());
        assert!(!store.is_readable(&p));

        // Request an expedited restore and fast-forward less than the thaw time.
        // The ready time is "now + thaw", measured from when the restore is asked.
        let before = clock.now_millis();
        let ready_at = store.request_restore(&p, RetrievalMode::Expedited);
        assert_eq!(
            ready_at - before,
            cost::restore_latency_ms(StorageClass::GlacierFlexible, RetrievalMode::Expedited)
        );
        clock.advance(ready_at - 1 - clock.now_millis()); // to one tick before ready
        assert!(store.get(&p).await.is_err(), "still thawing");

        // Cross the restore boundary: now it reads.
        clock.advance(1);
        assert!(store.is_readable(&p));
        let got = store.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&*got, b"frozen");
        assert!(store.metered_retrieval_usd() > 0.0);
    }

    #[tokio::test]
    async fn faults_are_deterministic_for_a_seed() {
        // Same seed ⇒ identical fault sequence ⇒ identical error count.
        async fn run(seed: u64) -> usize {
            let clock = Arc::new(SimClock::new(0));
            let store = SimObjectStore::new(clock).with_seed(seed).with_fault_rate_ppm(500_000);
            let mut errs = 0;
            for i in 0..32 {
                let p = Path::from(format!("k/{i}"));
                if store.put(&p, PutPayload::from_static(b"x")).await.is_err() {
                    errs += 1;
                }
            }
            errs
        }
        assert_eq!(run(7).await, run(7).await);
    }
}
