//! Latency sample collection and nearest-rank summary statistics.

use serde::Serialize;

/// Nearest-rank percentile over a sorted slice.
///
/// Returns the value at index `ceil(p * n) - 1`, clamped to the last sample.
/// An empty slice has no percentile and returns 0.
pub fn percentile_nearest_rank(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }

    let n = sorted.len();
    let rank = (p.clamp(0.0, 1.0) * n as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(n - 1)]
}

/// Latency samples in nanoseconds.
#[derive(Debug, Clone, Default)]
pub struct Samples {
    values: Vec<u64>,
}

impl Samples {
    pub fn push(&mut self, value: u64) {
        self.values.push(value);
    }

    /// Sorts the samples in place and computes the summary.
    pub fn summary(&mut self) -> Summary {
        if self.values.is_empty() {
            return Summary::default();
        }

        self.values.sort_unstable();

        let count = self.values.len() as u64;
        let sum: u128 = self.values.iter().map(|&value| value as u128).sum();
        let mean = (sum / count as u128) as u64;

        Summary {
            count,
            p50: percentile_nearest_rank(&self.values, 0.50),
            p99: percentile_nearest_rank(&self.values, 0.99),
            p99_9: percentile_nearest_rank(&self.values, 0.999),
            mean,
            max: *self.values.last().expect("non-empty"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub count: u64,
    pub p50: u64,
    pub p99: u64,
    pub p99_9: u64,
    pub mean: u64,
    pub max: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_returns_observed_values() {
        let sorted = [1, 2, 3, 4, 5];
        assert_eq!(percentile_nearest_rank(&sorted, 0.50), 3);
        assert_eq!(percentile_nearest_rank(&sorted, 0.99), 5);
        assert_eq!(percentile_nearest_rank(&sorted, 0.999), 5);
    }

    #[test]
    fn nearest_rank_handles_even_lengths() {
        let sorted = [10, 20, 30, 40];
        assert_eq!(percentile_nearest_rank(&sorted, 0.50), 20);
        assert_eq!(percentile_nearest_rank(&sorted, 0.99), 40);
    }

    #[test]
    fn nearest_rank_handles_empty_and_single() {
        assert_eq!(percentile_nearest_rank(&[], 0.50), 0);
        assert_eq!(percentile_nearest_rank(&[7], 0.999), 7);
    }

    #[test]
    fn nearest_rank_handles_duplicate_values() {
        let sorted = [5, 5, 5, 9, 9];
        assert_eq!(percentile_nearest_rank(&sorted, 0.50), 5);
        assert_eq!(percentile_nearest_rank(&sorted, 0.99), 9);
        assert_eq!(percentile_nearest_rank(&sorted, 0.999), 9);
    }

    #[test]
    fn summary_reports_count_mean_and_max() {
        let mut samples = Samples::default();
        for value in [10, 20, 30, 40] {
            samples.push(value);
        }
        let summary = samples.summary();
        assert_eq!(summary.count, 4);
        assert_eq!(summary.mean, 25);
        assert_eq!(summary.max, 40);
        assert_eq!(summary.p50, 20);
    }
}
