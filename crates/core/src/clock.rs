//! The `Clock` seam. Core logic reads time only through this trait, never via
//! `SystemTime::now()` / `Instant::now()`. In production it's backed by the real
//! clock (in the `vdg` shell); under DST it's `SimClock`, where time advances
//! only when code sleeps — so a "trillion-row" run completes in real seconds.

use async_trait::async_trait;
use std::sync::Mutex;

/// Milliseconds since an arbitrary epoch. Logical, not wall-clock.
pub type Millis = u64;

#[async_trait]
pub trait Clock: Send + Sync {
    /// Current logical time in milliseconds.
    fn now_millis(&self) -> Millis;

    /// Advance/await `ms`. Under simulation this advances logical time with no
    /// real waiting; in production it really sleeps.
    async fn sleep(&self, ms: Millis);
}

/// Deterministic clock for simulation. Time is explicit state: it only moves
/// forward when something sleeps (or via [`SimClock::advance`]).
#[derive(Debug)]
pub struct SimClock {
    now: Mutex<Millis>,
}

impl SimClock {
    pub fn new(start: Millis) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Manually push time forward, e.g. to fast-forward simulated months.
    pub fn advance(&self, ms: Millis) {
        *self.now.lock().expect("sim clock poisoned") += ms;
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new(0)
    }
}

#[async_trait]
impl Clock for SimClock {
    fn now_millis(&self) -> Millis {
        *self.now.lock().expect("sim clock poisoned")
    }

    async fn sleep(&self, ms: Millis) {
        self.advance(ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sim_clock_advances_only_on_sleep() {
        let c = SimClock::new(1_000);
        assert_eq!(c.now_millis(), 1_000);
        c.sleep(500).await;
        assert_eq!(c.now_millis(), 1_500);
        c.advance(8_500);
        assert_eq!(c.now_millis(), 10_000);
    }
}
