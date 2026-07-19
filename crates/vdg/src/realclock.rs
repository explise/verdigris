//! The production `Clock`. Lives in the shell (not core) so the core stays free
//! of any real time source. Under DST this is replaced by `SimClock`.

use async_trait::async_trait;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use verdigris_core::clock::{Clock, Micros, Millis};

pub struct RealClock {
    /// Origin for [`Clock::monotonic_micros`]. `Instant` is monotonic by
    /// construction, so durations derived from it survive an NTP step that
    /// would corrupt a wall-clock difference.
    started: Instant,
}

impl RealClock {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for RealClock {
    fn now_millis(&self) -> Millis {
        // Degrade rather than panic. A host whose clock predates the epoch (dead
        // RTC, restored snapshot, board before its first NTP sync) should still
        // serve requests with a bad timestamp; this is called on every request,
        // so a panic here takes the process down instead of one response.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as Millis)
            .unwrap_or(0)
    }

    fn monotonic_micros(&self) -> Micros {
        self.started.elapsed().as_micros() as Micros
    }

    async fn sleep(&self, ms: Millis) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The panic this replaced was reachable on any host with a pre-epoch clock,
    /// on every request. We cannot move the system clock in a test, so assert the
    /// shape instead: `now_millis` returns rather than unwinding.
    #[test]
    fn now_millis_never_panics() {
        let c = RealClock::new();
        let _ = c.now_millis();
    }

    #[test]
    fn monotonic_micros_never_goes_backwards() {
        let c = RealClock::new();
        let a = c.monotonic_micros();
        let b = c.monotonic_micros();
        assert!(b >= a, "monotonic reading went backwards: {a} -> {b}");
    }
}
