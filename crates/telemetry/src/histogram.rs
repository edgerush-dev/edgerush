//! How often values fell into each of a fixed set of ranges.

use crate::Counter;

/// A histogram over `N` upper bounds, which are not kept here: every shard of every series
/// would hold the same ones. Whoever observes and whoever reads passes the same bounds, in
/// ascending order, in the unit of the values (nanoseconds for durations: whole numbers
/// all the way, seconds only when they are written out).
#[derive(Debug)]
pub struct Histogram<const N: usize> {
    /// How many values were at most the bound of the same position and above the one
    /// before it. Not cumulative: a value is counted once.
    buckets: [Counter; N],
    /// How many were above the last bound.
    beyond: Counter,
    sum: Counter,
}

impl<const N: usize> Default for Histogram<N> {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| Counter::default()),
            beyond: Counter::default(),
            sum: Counter::default(),
        }
    }
}

impl<const N: usize> Histogram<N> {
    /// Counts a value. Never waits and never allocates.
    pub fn observe(&self, bounds: &[u64; N], value: u64) {
        let position = bounds.partition_point(|bound| *bound < value);
        self.buckets.get(position).unwrap_or(&self.beyond).inc();
        self.sum.add(value);
    }

    /// How many values fell into each range, not cumulative: one number per bound, and
    /// after them the number of values beyond the last bound.
    pub fn counts(&self) -> impl Iterator<Item = u64> {
        self.buckets
            .iter()
            .chain(std::iter::once(&self.beyond))
            .map(Counter::get)
    }

    /// The sum of all values, which wraps around as a counter does.
    #[must_use]
    pub fn sum(&self) -> u64 {
        self.sum.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const BOUNDS: [u64; 4] = [10, 100, 1_000, 10_000];

    fn counts(values: &[u64]) -> Vec<u64> {
        let histogram = Histogram::default();
        for value in values {
            histogram.observe(&BOUNDS, *value);
        }
        histogram.counts().collect()
    }

    #[test]
    fn a_value_is_counted_under_the_first_bound_it_does_not_exceed() {
        assert_eq!(counts(&[]), [0, 0, 0, 0, 0]);
        assert_eq!(counts(&[0]), [1, 0, 0, 0, 0]);
        assert_eq!(counts(&[10]), [1, 0, 0, 0, 0]);
        assert_eq!(counts(&[11]), [0, 1, 0, 0, 0]);
        assert_eq!(counts(&[10_000]), [0, 0, 0, 1, 0]);
    }

    #[test]
    fn a_value_beyond_the_last_bound_is_still_counted() {
        assert_eq!(counts(&[10_001, u64::MAX]), [0, 0, 0, 0, 2]);
    }

    #[test]
    fn the_sum_is_the_sum_of_the_values() {
        let histogram = Histogram::default();
        for value in [5, 50, 50_000] {
            histogram.observe(&BOUNDS, value);
        }
        assert_eq!(histogram.sum(), 50_055);
    }

    #[test]
    fn a_histogram_without_bounds_counts_everything_beyond() {
        let histogram = Histogram::<0>::default();
        histogram.observe(&[], 7);
        assert_eq!(histogram.counts().collect::<Vec<_>>(), [1]);
    }

    proptest! {
        /// Against the obvious way of counting: for every value, walk the bounds.
        #[test]
        fn counting_agrees_with_walking_the_bounds(
            values in prop::collection::vec(prop_oneof![0_u64..20_000, any::<u64>()], 0..50)
        ) {
            let mut expected = vec![0_u64; BOUNDS.len() + 1];
            for value in &values {
                let position = BOUNDS
                    .iter()
                    .position(|bound| value <= bound)
                    .unwrap_or(BOUNDS.len());
                expected[position] += 1;
            }
            let counted = counts(&values);
            prop_assert_eq!(counted.iter().sum::<u64>(), values.len() as u64);
            prop_assert_eq!(counted, expected);
        }
    }
}
