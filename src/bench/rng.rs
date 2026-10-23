//! Deterministic splitmix64 random number generator.

#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform value in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    pub fn chance(&mut self, probability: f64) -> bool {
        self.next_f64() < probability
    }

    /// Uniform value in `[low, high]` (inclusive). Returns `low` when the
    /// interval is empty.
    pub fn range_inclusive(&mut self, low: u64, high: u64) -> u64 {
        if low >= high {
            return low;
        }
        low + self.next_u64() % (high - low + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_the_same_sequence() {
        let mut a = SplitMix64::new(7);
        let mut b = SplitMix64::new(7);
        for _ in 0..32 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_produce_different_sequences() {
        let mut a = SplitMix64::new(7);
        let mut b = SplitMix64::new(8);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn range_stays_within_bounds() {
        let mut rng = SplitMix64::new(1);
        for _ in 0..1_000 {
            let value = rng.range_inclusive(10, 20);
            assert!((10..=20).contains(&value));
        }
    }

    #[test]
    fn chance_is_always_true_at_one_and_false_at_zero() {
        let mut rng = SplitMix64::new(1);
        assert!(rng.chance(1.0));
        assert!(!rng.chance(0.0));
    }
}
