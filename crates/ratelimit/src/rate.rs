//! A token bucket's arithmetic in units, as local mode's keys and the asynchronous mode's
//! entries keep their buckets (#350): the arrival time in units of 1/65,536 of a request's
//! interval, modulo 2^63, and a contract's conversion from nanoseconds to units and back.
//! Pure, no atomics: each mode keeps the arrival in a word of its own.

use crate::service::Contract;

/// Units in a request's interval: an interval is exactly this many, so charging one request
/// adds a whole number and the tolerance is exact; and fine enough that time's rounding,
/// under a unit at each conversion, is 1/65,536 of an interval: 15 µs under one a second,
/// 5.5 ms under ten an hour.
pub(crate) const UNITS: u64 = 1 << 16;
/// The arrival time's range: units modulo 2^63.
pub(crate) const MASK: u64 = (1 << 63) - 1;
/// How far ahead of now an arrival time may be and still be ahead, not behind: a quarter of
/// the range (2^45 intervals), so that the difference of two times within it is never
/// mistaken.
pub(crate) const AHEAD_BELOW: u64 = 1 << 61;
/// The most of a bucket kept: past it, a bucket is this big, a trillion requests, rather than
/// wrapping into no bound at all. Keeps the tolerance far within [`AHEAD_BELOW`], and so
/// does a key left untouched for the two refill times after which the asynchronous mode's
/// transport evicts it.
pub(crate) const MOST_BURST: u64 = 1 << 40;
/// How far behind `now` a full bucket's arrival is set ([`Rate::full`]): `MOST_BURST`
/// intervals, so that with the two refill times a key may be left untouched it stays in the
/// behind part of the range.
const FULL_FOR: u64 = MOST_BURST * UNITS;
/// Units added to a wait ([`Rate::wait`]) so that it is never short: the arrival and `now`
/// are each rounded by under a unit when converted, so the wait in units is off by under two.
const WAIT_SLACK: u64 = 2;

/// A contract's arithmetic: time to units and back, and its tolerance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rate {
    /// Units a nanosecond, as a 64.64 fixed-point number, rounded up: a time converts to at
    /// most a unit more than it is worth, which errs towards admitting.
    per_ns_high: u64,
    per_ns_low: u64,
    /// How far ahead of now the arrival time may be for a request to be admitted, in units:
    /// the bucket less one, and the slack's refill ([`SLACK`](crate::SLACK)).
    pub(crate) tolerance: u64,
    /// The contract's window and limit, for units back to time.
    window: u128,
    limit: u128,
    /// The time a full bucket takes to refill, rounded up: a key unused for longer is full,
    /// whatever its word says once its clock has wrapped round.
    pub(crate) refill: u64,
}

impl Rate {
    pub(crate) fn new(contract: Contract) -> Self {
        let limit = u128::from(contract.limit.get());
        let window = u128::from(contract.window.get());
        let burst = contract.burst.get().min(MOST_BURST);
        // Units a nanosecond: limit × 65,536 / window, with 64 bits after the point, rounded
        // up: a time on a whole unit converts to exactly it, and any other errs by under a
        // unit towards admitting. The limit × 65,536 fits in 80 bits, so shifted in 128 after
        // the division of the high part; a rate over 2^48 a nanosecond would not fit the high
        // part and is held to its most.
        let scaled = limit * u128::from(UNITS);
        let whole = scaled / window;
        let rest = ((scaled % window) << 64).div_ceil(window);
        let per_ns = (whole << 64) + rest;
        let refill = (u128::from(burst) * window).div_ceil(limit);
        // The slack's refill (`crate::SLACK`), rounded up as the rate is, erring towards
        // admitting; within the most a bucket keeps, so that the tolerance stays far inside
        // the range.
        let slack = u64::try_from(
            u128::from(crate::SLACK)
                .saturating_mul(per_ns)
                .div_ceil(1 << 64),
        )
        .unwrap_or(u64::MAX)
        .min(MOST_BURST * UNITS);
        Self {
            per_ns_high: u64::try_from(per_ns >> 64).unwrap_or(u64::MAX),
            per_ns_low: per_ns as u64,
            tolerance: (burst - 1) * UNITS + slack,
            window,
            limit,
            refill: u64::try_from(refill).unwrap_or(u64::MAX),
        }
    }

    /// `now`, in units, modulo the range.
    pub(crate) fn units(&self, now: u64) -> u64 {
        let high = u128::from(now) * u128::from(self.per_ns_high);
        let low = (u128::from(now) * u128::from(self.per_ns_low)) >> 64;
        (high + low) as u64 & MASK
    }

    /// How far ahead of `now` (units) `arrival` is: zero if it is behind.
    pub(crate) fn ahead(arrival: u64, now: u64) -> u64 {
        let ahead = arrival.wrapping_sub(now) & MASK;
        if ahead < AHEAD_BELOW { ahead } else { 0 }
    }

    /// Nanoseconds that `units` take to pass, rounded up.
    pub(crate) fn nanos(&self, units: u64) -> u64 {
        let nanos = (u128::from(units) * self.window).div_ceil(self.limit * u128::from(UNITS));
        u64::try_from(nanos).unwrap_or(u64::MAX)
    }

    /// Whether a bucket whose arrival time is `arrival` has room for one at `now` (units).
    pub(crate) fn admits(&self, arrival: u64, now: u64) -> bool {
        Self::ahead(arrival, now) <= self.tolerance
    }

    /// Requests the bucket has room for at `now`.
    #[cfg(test)]
    pub(crate) fn remaining(&self, arrival: u64, now: u64) -> u64 {
        self.remaining_within(arrival, now, 0)
    }

    /// Requests the bucket has room for at `now` and in `more` units refilled after,
    /// counted together and rounded down once: no fraction of the refill is lost.
    pub(crate) fn remaining_within(&self, arrival: u64, now: u64, more: u64) -> u64 {
        let allowed = self.tolerance.saturating_add(more);
        let ahead = Self::ahead(arrival, now);
        if ahead > allowed {
            return 0;
        }
        (allowed - ahead) / UNITS + 1
    }

    /// The arrival after charging `count` at `now`.
    pub(crate) fn charge(arrival: u64, now: u64, count: u64) -> u64 {
        let from = if Self::ahead(arrival, now) > 0 {
            arrival
        } else {
            now
        };
        from.wrapping_add(count.saturating_mul(UNITS)) & MASK
    }

    /// The arrival of a bucket full at `now` (units) and at any time before it, as the
    /// reference's bucket with no arrival time is: charged as of then, from then.
    pub(crate) fn full(now: u64) -> u64 {
        now.wrapping_sub(FULL_FOR) & MASK
    }

    /// The arrival `behind` from full at `now`, as [`TokenBucket::behind`] says it (time
    /// multiplied by the limit, an interval being the window), plus `extra` charges:
    /// [`full`](Self::full) if both are none, as [`TokenBucket::rebase`] leaves no arrival.
    ///
    /// [`TokenBucket::behind`]: crate::reference::TokenBucket::behind
    /// [`TokenBucket::rebase`]: crate::reference::TokenBucket::rebase
    pub(crate) fn rebased(&self, now: u64, behind: u128, extra: u64) -> u64 {
        if behind == 0 && extra == 0 {
            return Self::full(now);
        }
        let behind = u64::try_from(behind * u128::from(UNITS) / self.window).unwrap_or(u64::MAX);
        let behind = behind.min(AHEAD_BELOW - 1);
        now.wrapping_add(behind)
            .wrapping_add(extra.saturating_mul(UNITS))
            & MASK
    }

    /// Nanoseconds from `now` until the bucket has room for one, if it has none: never
    /// short of when it has, and at most a few units long.
    pub(crate) fn wait(&self, arrival: u64, now: u64) -> Option<u64> {
        let over = Self::ahead(arrival, now).checked_sub(self.tolerance)?;
        if over == 0 {
            return None;
        }
        Some(self.nanos(over + WAIT_SLACK))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::{Model, TokenBucket};
    use proptest::prelude::*;
    use std::num::NonZeroU64;

    fn contract(limit: u64, window: u64, burst: u64) -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(limit),
            window: n(window),
            burst: n(burst),
        }
    }

    /// A second, ten requests at once: the bucket's arithmetic in units.
    #[test]
    fn a_bucket_in_units_admits_its_burst_then_its_rate() {
        let bucket = Rate::new(contract(10, 1_000_000_000, 10));
        let mut arrival = 0;
        let now = bucket.units(0);
        let mut admitted = 0;
        while bucket.admits(arrival, now) {
            arrival = Rate::charge(arrival, now, 1);
            admitted += 1;
        }
        assert_eq!(admitted, 10);
        assert_eq!(bucket.remaining(arrival, now), 0);
        // One a tenth of a second less the slack (`crate::SLACK`), rounded up to a whole
        // unit: 66 units, a unit being 1/65,536 of the interval, 1,526 ns here, so 100,708
        // ns. The wait is never short of that, and up to the wait's two units longer.
        let due = 100_000_000 - 66 * 100_000_000 / 65_536;
        let wait = bucket.wait(arrival, now).unwrap();
        assert!((due..=due + 3_052).contains(&wait), "{wait}");
        assert!(bucket.admits(arrival, bucket.units(due + 1_526)));
        assert!(!bucket.admits(arrival, bucket.units(due - 1_526)));
    }

    /// Long after the last charge, the arrival time has fallen behind: still a full
    /// bucket, never one far ahead, for as long as the ahead part of the range lasts, 2^45
    /// intervals (9.8 hours at a billion a second, 1,115 years at a thousand). Past that an
    /// untouched key reads as owing; the asynchronous mode's transport evicts it long
    /// before, after two refill times idle, which `MOST_BURST` keeps to 2^41 intervals at
    /// most, and local mode reads a key idle for a refill as full.
    #[test]
    fn an_old_arrival_is_behind_not_ahead() {
        let c = contract(1_000_000_000, 1_000_000_000, 5);
        let bucket = Rate::new(c);
        let arrival = Rate::charge(0, bucket.units(0), 5);
        let margin = AHEAD_BELOW / UNITS;
        // Full: the burst, and the slack's refill, a request a nanosecond.
        let full = 5 + c.refilled(crate::SLACK);
        for later in [1_000_000, 2 * MOST_BURST, margin - 1] {
            let later = bucket.units(later);
            assert_eq!(Rate::ahead(arrival, later), 0, "{later}");
            assert_eq!(bucket.remaining(arrival, later), full);
        }
        assert!(
            2 * MOST_BURST <= margin / 2,
            "eviction comes well inside the margin"
        );
    }

    /// An answer's `behind` sets the bucket where the reference would be, plus what is
    /// charged on top.
    #[test]
    fn a_rebase_takes_the_owners_bucket_and_the_extra() {
        let c = contract(100, 1_000_000_000, 10);
        let bucket = Rate::new(c);
        let mut reference = c.bucket();
        for _ in 0..4 {
            reference.charge(0);
        }
        let now = 50_000_000;
        let behind = reference.behind(now);
        let arrival = bucket.rebased(bucket.units(now), behind, 2);
        assert_eq!(
            bucket.remaining(arrival, bucket.units(now)),
            reference.remaining(now) - 2
        );
    }

    /// One unit, in nanoseconds, rounded up: the most the bucket in units may be off by.
    fn unit_ns(c: Contract) -> u64 {
        u64::try_from(
            u128::from(c.window.get()).div_ceil(u128::from(c.limit.get()) * u128::from(UNITS)),
        )
        .unwrap()
    }

    /// An arrival close to the end of its range, with the bucket's time wrapping past it:
    /// decided as at the start of the range, the bucket admitting its burst and then its
    /// rate on either side of the wrap.
    #[test]
    fn the_arrival_wraps_without_a_wrong_decision() {
        // A request a microsecond: the range of units wraps every 2^63 / 2^16 µs, 4.5 years.
        let c = contract(1_000_000, 1_000_000_000, 3);
        let bucket = Rate::new(c);
        let interval = 1_000;
        let wrap = u64::try_from((u128::from(MASK) + 1) / u128::from(UNITS)).unwrap() * interval;
        for start in [wrap - 2 * interval, wrap - interval / 2, wrap, wrap + 1] {
            let mut reference: TokenBucket = c.bucket().with_slack(crate::SLACK);
            let mut arrival = bucket.units(start);
            for step in 0..8 {
                let now = start + step * interval / 2;
                let units = bucket.units(now);
                assert_eq!(
                    bucket.admits(arrival, units),
                    reference.admits(now),
                    "from {start}, step {step}"
                );
                if reference.admits(now) {
                    reference.charge(now);
                    arrival = Rate::charge(arrival, units, 1);
                }
            }
        }
    }

    /// A burst too large for the arrival's range is held to the largest the range keeps
    /// apart from behind, rather than becoming no bound at all.
    #[test]
    fn a_burst_past_the_range_is_still_a_bound() {
        let bucket = Rate::new(contract(1, 1_000_000_000, u64::MAX));
        let now = bucket.units(0);
        let full = bucket.remaining(0, now);
        assert!(full >= MOST_BURST, "{full}");
        let spent = Rate::charge(0, now, full);
        assert!(!bucket.admits(spent, now), "the bucket emptied");
        assert!(bucket.wait(spent, now).is_some());
    }

    proptest! {
        /// At any time, not only on interval boundaries, and at low rates, around the
        /// moment the bucket has room again: never refused where the reference admits,
        /// admitted early by under three units (3/65,536 of an interval), and a refusal's
        /// wait short of the reference's by under a unit and never three units longer.
        /// Owners' answers rebase it now and then, as the transport does. The bounds: a
        /// rebased arrival is under two units early (its `now` and its `behind` each rounded
        /// down), `now` under one unit late, and the slack (`crate::SLACK`) rounded up to a
        /// whole unit, erring towards admitting, under one unit more.
        #[test]
        fn off_the_grid_it_is_never_late_and_under_three_units_early(
            limit in 1u64..20,
            window_s in 1u64..3_600,
            burst in 1u64..12,
            steps in prop::collection::vec((0u64..2_000, any::<bool>()), 1..300),
        ) {
            let window = window_s * 1_000_000_000;
            let c = contract(limit, window, burst);
            let bucket = Rate::new(c);
            let unit = unit_ns(c);
            let interval = window / limit;
            let mut reference: TokenBucket = c.bucket().with_slack(crate::SLACK);
            let mut arrival = 0;
            let mut now = 0u64;
            for (thousandths, rebase) in steps {
                // Up to two intervals on, at any nanosecond.
                now += interval * thousandths / 1_000 + thousandths % 7;
                let units = bucket.units(now);
                if rebase {
                    arrival = bucket.rebased(units, reference.behind(now), 0);
                }
                let ours = bucket.admits(arrival, units);
                if reference.admits(now) {
                    prop_assert!(ours, "refused at {} where the reference admits", now);
                    reference.charge(now);
                    arrival = Rate::charge(arrival, units, 1);
                } else if ours {
                    let due = reference.retry_at(now).unwrap() - now;
                    prop_assert!(due < 3 * unit, "admitted {} ns early, a unit {}", due, unit);
                } else {
                    let due = reference.retry_at(now).unwrap() - now;
                    let wait = bucket.wait(arrival, units).unwrap();
                    prop_assert!(wait + unit > due, "wait {} short of {} by a unit or more", wait, due);
                    prop_assert!(wait <= due + 3 * unit, "wait {} over {} by three units or more", wait, due);
                }
            }
        }

        /// Workers' clocks lagging the true time by up to the slack (`crate::SLACK`), each
        /// request decided at its worker's clock, never leave the bucket having admitted
        /// fewer than a bucket of the contract would have on the true times: a lag refuses
        /// nothing within the limit. A request a microsecond and slower.
        #[test]
        fn a_lag_within_the_slack_never_falls_behind_the_contract(
            limit in 1u64..1_000_000,
            burst in 1u64..20,
            steps in prop::collection::vec((0u64..3_000, 0u64..=crate::SLACK), 1..300),
        ) {
            let window = 1_000_000_000;
            let c = contract(limit, window, burst);
            let bucket = Rate::new(c);
            let interval = window / limit;
            let mut exact = c.bucket();
            let (mut arrival, mut ours, mut theirs) = (0, 0u64, 0u64);
            let mut now = crate::SLACK;
            for (thousandths, lag) in steps {
                // Up to three intervals on, the request's clock up to the slack behind.
                now += interval * thousandths / 1_000;
                let clock = bucket.units(now - lag);
                if bucket.admits(arrival, clock) {
                    arrival = Rate::charge(arrival, clock, 1);
                    ours += 1;
                }
                if exact.admits(now) {
                    exact.charge(now);
                    theirs += 1;
                }
                prop_assert!(ours >= theirs, "{} admitted against {} at {}", ours, theirs, now);
            }
        }

        /// The bucket in units decides as the reference token bucket does, request by
        /// request, whatever the contract and the times (within a unit's rounding: times
        /// here are whole intervals apart or more).
        #[test]
        fn units_decide_as_the_reference(
            limit in 1u64..10_000,
            interval in 1u64..100_000_000,
            burst in 1u64..200,
            steps in prop::collection::vec(0u64..4, 1..200),
        ) {
            let window = limit * interval;
            let c = contract(limit, window, burst);
            let bucket = Rate::new(c);
            let mut reference: TokenBucket = c.bucket().with_slack(crate::SLACK);
            let mut arrival = 0;
            let mut now = 0u64;
            for step in steps {
                now += step * interval;
                let units = bucket.units(now);
                prop_assert_eq!(bucket.admits(arrival, units), reference.admits(now), "at {}", now);
                prop_assert_eq!(bucket.remaining(arrival, units), reference.remaining(now));
                if reference.admits(now) {
                    reference.charge(now);
                    arrival = Rate::charge(arrival, units, 1);
                }
            }
        }
    }
}
