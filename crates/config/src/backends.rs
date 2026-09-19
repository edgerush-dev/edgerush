//! A rule's backends, ready for choosing one per request in proportion to their weights.

/// An upstream's position in [`Compiled::upstreams`](crate::Compiled): what a rule holds
/// instead of a name, so that no request looks anything up by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpstreamId(pub usize);

/// The backends of one rule. Choosing is a pure function of a number passed in: the caller
/// brings the randomness, this brings the proportions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedBackends {
    /// Backends with a share, each with the sum of the weights up to and including its
    /// own: the backend for a point is the first whose sum lies beyond it.
    shares: Box<[(u64, UpstreamId)]>,
    total: u64,
}

impl WeightedBackends {
    /// From (upstream, weight) pairs. A weight of zero is no share.
    pub fn new(backends: impl IntoIterator<Item = (UpstreamId, u32)>) -> Self {
        let mut total = 0_u64;
        let shares = backends
            .into_iter()
            .filter(|(_, weight)| *weight > 0)
            .map(|(upstream, weight)| {
                // No sum of `u32`s that fits in memory overflows a `u64`.
                total += u64::from(weight);
                (total, upstream)
            })
            .collect();
        Self { shares, total }
    }

    /// The backend for a request, given a number that is uniform over `u64` (every value of
    /// `random` modulo the total weight then comes up as often as any other, to within one
    /// part in 2⁶⁴ divided by that total). `None` if no backend has a share, which is for
    /// the caller to answer with a 500. Never allocates.
    #[must_use]
    pub fn pick(&self, random: u64) -> Option<UpstreamId> {
        let point = random.checked_rem(self.total)?;
        let at = self.shares.partition_point(|(sum, _)| *sum <= point);
        self.shares.get(at).map(|(_, upstream)| *upstream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn backends(weights: &[u32]) -> WeightedBackends {
        WeightedBackends::new(
            weights
                .iter()
                .enumerate()
                .map(|(at, weight)| (UpstreamId(at), *weight)),
        )
    }

    /// How often each backend is picked over one full turn of the points.
    fn turn(weights: &[u32]) -> Vec<u32> {
        let backends = backends(weights);
        let total: u64 = weights.iter().map(|weight| u64::from(*weight)).sum();
        let mut picked = vec![0; weights.len()];
        for point in 0..total {
            picked[backends.pick(point).unwrap().0] += 1;
        }
        picked
    }

    #[test]
    fn backends_are_picked_in_proportion_to_their_weights() {
        assert_eq!(turn(&[1]), [1]);
        assert_eq!(turn(&[90, 10]), [90, 10]);
        assert_eq!(turn(&[1, 2, 3]), [1, 2, 3]);
        assert_eq!(turn(&[5, 0, 5]), [5, 0, 5]);
    }

    #[test]
    fn a_single_backend_gets_everything() {
        let only = backends(&[7]);
        for random in [0, 1, 6, 7, 8, u64::MAX] {
            assert_eq!(only.pick(random), Some(UpstreamId(0)));
        }
    }

    #[test]
    fn no_share_anywhere_is_nowhere_to_go() {
        assert_eq!(backends(&[]).pick(0), None);
        assert_eq!(backends(&[0, 0]).pick(12345), None);
    }

    #[test]
    fn the_largest_weights_do_not_overflow() {
        let huge = backends(&[u32::MAX, u32::MAX, u32::MAX]);
        assert_eq!(huge.pick(0), Some(UpstreamId(0)));
        assert_eq!(huge.pick(u64::from(u32::MAX)), Some(UpstreamId(1)));
        assert_eq!(huge.pick(3 * u64::from(u32::MAX) - 1), Some(UpstreamId(2)));
        assert_eq!(huge.pick(3 * u64::from(u32::MAX)), Some(UpstreamId(0)));
    }

    proptest! {
        #[test]
        fn over_one_turn_every_backend_gets_exactly_its_weight(
            weights in prop::collection::vec(0_u32..50, 0..8)
        ) {
            prop_assert_eq!(turn(&weights), weights);
        }
    }
}
