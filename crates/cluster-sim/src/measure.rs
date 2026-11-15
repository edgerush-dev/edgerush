//! Measuring what a run admitted against the contract itself, not against another
//! limiter's decisions.

use edgerush_ratelimit::reference::{CountingBucket, Model};
use edgerush_ratelimit::service::Contract;

/// How many of requests arriving at `times` (non-decreasing) one bucket holding `contract`
/// would admit: what keeping the contract exactly admits.
#[must_use]
pub fn one_bucket_admits(contract: &Contract, times: &[u64]) -> u64 {
    let mut bucket = CountingBucket::new(contract.limit, contract.window, contract.burst);
    times.iter().filter(|at| bucket.decide(**at)).count() as u64
}

/// The most requests admitted at `times` (non-decreasing) over what `contract` allows, in
/// any span from one admission to another: the largest `count − (limit × span / window +
/// burst)`, in whole requests, rounded down. Zero or less means the contract was kept. With
/// nothing admitted it is `−burst`.
///
/// With `G(k) = k × window − limit × times[k]`, the excess of the span from `i` to `j`,
/// times `window`, is `G(j) − G(i) + window − burst × window`, so one pass keeping the
/// smallest `G` so far finds the largest.
#[must_use]
pub fn worst_excess(contract: &Contract, times: &[u64]) -> i64 {
    let window = i128::from(contract.window.get());
    let limit = i128::from(contract.limit.get());
    let burst = i128::from(contract.burst.get());
    let mut lowest: Option<i128> = None;
    let mut widest = i128::MIN;
    for (k, &at) in times.iter().enumerate() {
        let g = i128::try_from(k).unwrap_or(i128::MAX) * window - limit * i128::from(at);
        let low = lowest.map_or(g, |low| low.min(g));
        lowest = Some(low);
        widest = widest.max(g - low);
    }
    if widest == i128::MIN {
        return -i64::try_from(burst).unwrap_or(i64::MAX);
    }
    let excess = (widest + window - burst * window).div_euclid(window);
    i64::try_from(excess).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::num::NonZeroU64;

    fn contract(limit: u64, window: u64, burst: u64) -> Contract {
        Contract {
            limit: NonZeroU64::new(limit).unwrap(),
            window: NonZeroU64::new(window).unwrap(),
            burst: NonZeroU64::new(burst).unwrap(),
        }
    }

    /// Every span tried, the excess worked out in rationals by cross-multiplying.
    fn worst_by_hand(contract: &Contract, times: &[u64]) -> i64 {
        let (w, l, b) = (
            i128::from(contract.window.get()),
            i128::from(contract.limit.get()),
            i128::from(contract.burst.get()),
        );
        let mut worst: Option<i128> = None;
        for i in 0..times.len() {
            for j in i..times.len() {
                let count = i128::try_from(j - i + 1).unwrap();
                let span = i128::from(times[j] - times[i]);
                // count − (l × span / w + b), floored.
                let excess = (count * w - l * span - b * w).div_euclid(w);
                worst = Some(worst.map_or(excess, |worst| worst.max(excess)));
            }
        }
        i64::try_from(worst.unwrap_or(-b)).unwrap()
    }

    proptest! {
        #[test]
        fn one_pass_finds_what_every_span_finds(
            gaps in prop::collection::vec(prop_oneof![Just(0u64), 0u64..2000], 0..60),
            limit in 1u64..10, window in 1u64..3000, burst in 1u64..12,
        ) {
            let times: Vec<u64> = gaps.iter().scan(0, |at, gap| { *at += gap; Some(*at) }).collect();
            let contract = contract(limit, window, burst);
            prop_assert_eq!(worst_excess(&contract, &times), worst_by_hand(&contract, &times));
        }
    }

    #[test]
    fn a_burst_over_the_bucket_is_its_excess() {
        // 10 a second, a bucket of 5: 8 at once is 3 over.
        let contract = contract(10, 1_000_000_000, 5);
        assert_eq!(worst_excess(&contract, &[0; 8]), 3);
        assert_eq!(worst_excess(&contract, &[0; 5]), 0);
        assert_eq!(worst_excess(&contract, &[]), -5);
    }

    #[test]
    fn one_bucket_admits_its_bucket_at_once_and_its_rate_after() {
        // 10 a second, a bucket of 5: of 8 at once, 5; then one every 100 ms, each.
        let contract = contract(10, 1_000_000_000, 5);
        let mut times = vec![0; 8];
        times.extend((1..=10).map(|n| n * 100_000_000));
        assert_eq!(one_bucket_admits(&contract, &times), 5 + 10);
        assert_eq!(one_bucket_admits(&contract, &[]), 0);
    }
}
