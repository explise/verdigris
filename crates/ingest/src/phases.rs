//! Opt-in duration counters measured through the injected clock.
use std::sync::atomic::{AtomicU64, Ordering};
use verdigris_core::clock::Clock;

#[derive(Clone, Copy)]
pub enum IngestPhase {
    Parse,
    Encode,
    ObjectPut,
    LockWait,
    TrigramMerge,
    ManifestLoad,
    ManifestCas,
    CompactLockWait,
    CompactPass,
}

impl IngestPhase {
    pub const ALL: [Self; 9] = [
        Self::Parse,
        Self::Encode,
        Self::ObjectPut,
        Self::LockWait,
        Self::TrigramMerge,
        Self::ManifestLoad,
        Self::ManifestCas,
        Self::CompactLockWait,
        Self::CompactPass,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::Encode => "encode",
            Self::ObjectPut => "object_put",
            Self::LockWait => "lock_wait",
            Self::TrigramMerge => "trigram_merge",
            Self::ManifestLoad => "manifest_load",
            Self::ManifestCas => "manifest_cas",
            Self::CompactLockWait => "compact_lock_wait",
            Self::CompactPass => "compact_pass",
        }
    }
}

#[derive(Default)]
struct PhaseTotals {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl PhaseTotals {
    fn observe(&self, elapsed: u64) {
        self.total_us.fetch_add(elapsed, Ordering::Relaxed);
        self.max_us.fetch_max(elapsed, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct IngestPhaseMetrics {
    phases: [PhaseTotals; IngestPhase::ALL.len()],
}

impl IngestPhaseMetrics {
    pub fn timer<'a>(&'a self, clock: &'a dyn Clock, phase: IngestPhase) -> PhaseTimer<'a> {
        PhaseTimer {
            totals: &self.phases[phase as usize],
            clock,
            start_us: clock.monotonic_micros(),
        }
    }

    pub fn observe(&self, phase: IngestPhase, elapsed_us: u64) {
        self.phases[phase as usize].observe(elapsed_us);
    }

    /// Returns elapsed microseconds (total and max) and the observation count.
    pub fn totals(&self, phase: IngestPhase) -> (u64, u64, u64) {
        let totals = &self.phases[phase as usize];
        (
            totals.total_us.load(Ordering::Relaxed),
            totals.max_us.load(Ordering::Relaxed),
            totals.count.load(Ordering::Relaxed),
        )
    }
}

#[must_use]
pub struct PhaseTimer<'a> {
    totals: &'a PhaseTotals,
    clock: &'a dyn Clock,
    start_us: u64,
}

impl Drop for PhaseTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.clock.monotonic_micros().saturating_sub(self.start_us);
        self.totals.observe(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use verdigris_core::clock::SimClock;

    #[test]
    fn phase_totals_follow_the_monotonic_clock() {
        let clock = SimClock::default();
        let metrics = IngestPhaseMetrics::default();
        for millis in [2, 5] {
            let _timer = metrics.timer(&clock, IngestPhase::Encode);
            clock.advance(millis);
        }
        assert_eq!(metrics.totals(IngestPhase::Encode), (7000, 5000, 2));
        assert_eq!(metrics.totals(IngestPhase::ObjectPut), (0, 0, 0));
    }
}
