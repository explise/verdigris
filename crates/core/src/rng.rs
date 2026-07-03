//! The `Rng` seam. Core logic draws randomness only through this trait, never
//! via `rand::thread_rng()`. Seeded once, a single seed reproduces an entire run.

use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

pub trait Rng: Send + Sync {
    fn next_u64(&mut self) -> u64;
}

/// Deterministic, seedable RNG used in both production and simulation. The seed
/// is logged at the start of every simulated run so any failure is replayable.
pub struct SeededRng {
    inner: StdRng,
}

impl SeededRng {
    pub fn from_seed(seed: u64) -> Self {
        Self {
            inner: StdRng::seed_from_u64(seed),
        }
    }
}

impl Rng for SeededRng {
    fn next_u64(&mut self) -> u64 {
        self.inner.next_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SeededRng::from_seed(42);
        let mut b = SeededRng::from_seed(42);
        for _ in 0..16 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }
}
