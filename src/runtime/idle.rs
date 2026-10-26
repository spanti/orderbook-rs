//! Idle strategies for the matching thread.
//!
//! The strategy is configuration; the mutable counter is owned by the matching
//! thread through [`Idle`], so the same configuration can be shared.

use std::time::Duration;

/// How the matching thread waits when the command queue is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdleStrategy {
    /// Pure spin: lowest latency, burns a core.
    Spin,
    /// Spin briefly, yield, then park with a short timeout.
    #[default]
    Backoff,
}

/// Number of `spin_loop` hints before a backoff idler yields the core.
pub const BACKOFF_SPIN_ITERS: u32 = 10_000;

/// How long a backoff idler parks once the yield phase is spent.
pub const BACKOFF_PARK: Duration = Duration::from_micros(50);

/// Mutable idle state owned by the matching thread.
#[derive(Debug, Clone)]
pub(crate) struct Idle {
    strategy: IdleStrategy,
    spins: u32,
}

impl Idle {
    pub fn new(strategy: IdleStrategy) -> Self {
        Self { strategy, spins: 0 }
    }

    /// Call after making progress, so the next wait starts in the spin phase.
    pub fn reset(&mut self) {
        self.spins = 0;
    }

    /// Wait for one scheduling opportunity according to the strategy.
    pub fn idle(&mut self) {
        match self.strategy {
            IdleStrategy::Spin => std::hint::spin_loop(),
            IdleStrategy::Backoff => {
                if self.spins < BACKOFF_SPIN_ITERS {
                    self.spins += 1;
                    std::hint::spin_loop();
                } else if self.spins == BACKOFF_SPIN_ITERS {
                    self.spins += 1;
                    std::thread::yield_now();
                } else {
                    std::thread::park_timeout(BACKOFF_PARK);
                }
            }
        }
    }

    #[cfg(test)]
    fn spins(&self) -> u32 {
        self.spins
    }
}

impl Default for Idle {
    fn default() -> Self {
        Self::new(IdleStrategy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_counter_advances_then_resets() {
        let mut idle = Idle::new(IdleStrategy::Backoff);
        assert_eq!(idle.spins(), 0);

        idle.idle();
        idle.idle();
        assert_eq!(idle.spins(), 2);

        idle.reset();
        assert_eq!(idle.spins(), 0);
    }

    #[test]
    fn spin_strategy_does_not_use_the_backoff_counter() {
        let mut idle = Idle::new(IdleStrategy::Spin);
        for _ in 0..(BACKOFF_SPIN_ITERS + 2) {
            idle.idle();
        }

        assert_eq!(idle.spins(), 0);
    }

    #[test]
    fn backoff_yields_then_parks_after_the_spin_budget() {
        let mut idle = Idle::new(IdleStrategy::Backoff);
        for _ in 0..BACKOFF_SPIN_ITERS {
            idle.idle();
        }
        assert_eq!(idle.spins(), BACKOFF_SPIN_ITERS);

        idle.idle();
        assert_eq!(idle.spins(), BACKOFF_SPIN_ITERS + 1);

        idle.idle();
        assert_eq!(idle.spins(), BACKOFF_SPIN_ITERS + 1);
    }
}
