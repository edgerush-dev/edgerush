//! A key's admission state as one 128-bit word, and the arithmetic on it: pure, no atomics.
//!
//! | Field | Bits | |
//! |---|---|---|
//! | arrival | 56 | the GCRA arrival time, modulo 2^56, in units of 1/65,536 of the interval |
//! | left | 16 | what the cycle under way may still admit |
//! | spent | 16 | what the cycle under way has taken, net of what was given back: `left + spent` is the cycle's allowance |
//! | cycle | 16 | the cycle number of the last advance, modulo 2^16 |
//! | shared | 1 | whether that advance shared out the copy |
//! | closed | 1 | evicted: nothing more is taken |
//! | unreported | 14 | admitted here and not yet drained by the transport into its ledger |
//! | held | 7 | charged and held for requests waiting on an ask, each reserving a place in `unreported` |
//! | set | 1 | a cycle has set the allowance: until then there is none, as a new key has none |
//!
//! Invariants, kept by every compare-and-swap: `left + spent <= MOST_LEFT`, and
//! `unreported + held <= MOST_RESERVED`.

use crate::service::Contract;

/// Units of an interval: an interval is exactly this many, so charging one request adds a
/// whole number and the tolerance is exact; and fine enough that time's rounding, under a
/// unit at each conversion, is 1/65,536 of an interval: 15 µs under one a second, 5.5 ms
/// under ten an hour.
pub(super) const UNITS: u64 = 1 << 16;
const ARRIVAL_BITS: u32 = 56;
const ARRIVAL_MASK: u64 = (1 << ARRIVAL_BITS) - 1;
/// Differences under half the arrival's range are ahead; the rest, behind.
const AHEAD_BELOW: u64 = 1 << (ARRIVAL_BITS - 1);
/// The largest burst kept: its tolerance stays well inside the ahead half of the arrival's
/// range (2^39 intervals), and so does a key left untouched for the two refill times
/// (two bursts' intervals) after which the transport evicts it. A larger burst is held to
/// this, 137 billion requests, rather than wrapping into no bound at all.
pub(super) const MOST_BURST: u64 = 1 << 37;
/// Units added to a wait so that it is never short: the arrival and `now` are each
/// rounded by under a unit when converted, so the wait in units is off by under two.
const WAIT_SLACK: u64 = 2;
/// The most a cycle's allowance can be: `left` and `spent` each, and their sum. The proxy
/// refuses a pooled configuration whose contracts could need more ([`fits`]).
pub(super) const MOST_LEFT: u64 = (1 << 16) - 1;
const CYCLE_MASK: u64 = (1 << 16) - 1;
/// The most admitted and not yet drained, holds included: past it a request waits for the
/// transport to drain the count rather than lose any of it.
pub(super) const MOST_RESERVED: u64 = (1 << 14) - 1;
pub(super) const MOST_HELD: u64 = (1 << 7) - 1;

/// Whether `contract` fits a pooled entry at `cycle`: the largest allowance a cycle can set,
/// a whole bucket and two cycles' refill (the cycle after a share keeps what the one before
/// left) and one carried, within `MOST_LEFT`.
#[must_use]
pub fn fits(contract: Contract, cycle: u64) -> bool {
    let refill = contract.refilled(cycle.max(1)).saturating_add(1);
    contract
        .burst
        .get()
        .min(MOST_BURST)
        .saturating_add(refill.saturating_mul(2))
        .saturating_add(1)
        <= MOST_LEFT
}

/// The word's fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Fields {
    pub(super) arrival: u64,
    pub(super) left: u64,
    pub(super) spent: u64,
    pub(super) cycle: u64,
    pub(super) shared: bool,
    pub(super) closed: bool,
    pub(super) unreported: u64,
    pub(super) held: u64,
    pub(super) set: bool,
}

impl Fields {
    pub(super) fn pack(self) -> u128 {
        u128::from(self.arrival & ARRIVAL_MASK)
            | u128::from(self.left & MOST_LEFT) << 56
            | u128::from(self.spent & MOST_LEFT) << 72
            | u128::from(self.cycle & CYCLE_MASK) << 88
            | u128::from(self.shared) << 104
            | u128::from(self.closed) << 105
            | u128::from(self.unreported & MOST_RESERVED) << 106
            | u128::from(self.held & MOST_HELD) << 120
            | u128::from(self.set) << 127
    }

    pub(super) fn unpack(word: u128) -> Self {
        // Each field is masked to its width first: the casts keep only what fits.
        let field = |shift: u32, mask: u64| (word >> shift) as u64 & mask;
        Self {
            arrival: field(0, ARRIVAL_MASK),
            left: field(56, MOST_LEFT),
            spent: field(72, MOST_LEFT),
            cycle: field(88, CYCLE_MASK),
            shared: field(104, 1) == 1,
            closed: field(105, 1) == 1,
            unreported: field(106, MOST_RESERVED),
            held: field(120, MOST_HELD),
            set: field(127, 1) == 1,
        }
    }
}

/// `now`'s cycle number, modulo the field's width.
pub(super) fn cycle_of(now: u64, cycle: u64) -> u64 {
    (now / cycle.max(1)) & CYCLE_MASK
}

/// Cycles from `from` to `to`, modulo the field's width.
pub(super) fn cycles_between(from: u64, to: u64) -> u64 {
    to.wrapping_sub(from) & CYCLE_MASK
}

/// A contract's bucket in units: how to turn nanoseconds into them, and its tolerance.
#[derive(Clone, Copy, Debug)]
pub(super) struct Bucket {
    /// Units a nanosecond, as a 64.64 fixed-point number, split in two.
    per_ns_high: u64,
    per_ns_low: u64,
    tolerance: u64,
    /// The contract's window and limit, for turning units back into time.
    window: u128,
    limit: u128,
}

impl Bucket {
    pub(super) fn new(contract: Contract) -> Self {
        let limit = u128::from(contract.limit.get());
        let window = u128::from(contract.window.get());
        // Units a nanosecond: limit × 65,536 / window, with 64 bits after the point,
        // rounded up: a time on a whole unit converts to exactly it, and any other errs by
        // under a unit towards admitting. The limit × 65,536 fits in 80 bits, so shifted in
        // 128 after the division of the high part; a rate over 2^48 a nanosecond would not
        // fit the high part and is held to its most.
        let scaled = limit * u128::from(UNITS);
        let whole = scaled / window;
        let rest = ((scaled % window) << 64).div_ceil(window);
        let per_ns = (whole << 64) + rest;
        Self {
            per_ns_high: u64::try_from(per_ns >> 64).unwrap_or(u64::MAX),
            per_ns_low: per_ns as u64,
            tolerance: (contract.burst.get().min(MOST_BURST) - 1) * UNITS,
            window,
            limit,
        }
    }

    /// `now`, in units, modulo the arrival's range.
    pub(super) fn units(&self, now: u64) -> u64 {
        let high = u128::from(now) * u128::from(self.per_ns_high);
        let low = (u128::from(now) * u128::from(self.per_ns_low)) >> 64;
        (high + low) as u64 & ARRIVAL_MASK
    }

    /// How far ahead of `now` (units) `arrival` is; zero if behind.
    pub(super) fn ahead(arrival: u64, now: u64) -> u64 {
        let ahead = arrival.wrapping_sub(now) & ARRIVAL_MASK;
        if ahead < AHEAD_BELOW { ahead } else { 0 }
    }

    pub(super) fn admits(&self, arrival: u64, now: u64) -> bool {
        Self::ahead(arrival, now) <= self.tolerance
    }

    /// Requests the bucket has room for at `now`.
    pub(super) fn remaining(&self, arrival: u64, now: u64) -> u64 {
        let ahead = Self::ahead(arrival, now);
        if ahead > self.tolerance {
            return 0;
        }
        (self.tolerance - ahead) / UNITS + 1
    }

    /// The arrival after charging `count` at `now`.
    pub(super) fn charge(arrival: u64, now: u64, count: u64) -> u64 {
        let from = if Self::ahead(arrival, now) > 0 {
            arrival
        } else {
            now
        };
        from.wrapping_add(count.saturating_mul(UNITS)) & ARRIVAL_MASK
    }

    /// The arrival after giving one charge back.
    pub(super) fn release(arrival: u64) -> u64 {
        arrival.wrapping_sub(UNITS) & ARRIVAL_MASK
    }

    /// The arrival `behind` from full at `now`, as [`TokenBucket::behind`] says it
    /// (time multiplied by the limit, an interval being the window), plus `extra` charges.
    ///
    /// [`TokenBucket::behind`]: crate::reference::TokenBucket::behind
    pub(super) fn rebased(&self, now: u64, behind: u128, extra: u64) -> u64 {
        let behind = u64::try_from(behind * u128::from(UNITS) / self.window).unwrap_or(u64::MAX);
        let behind = behind.min(AHEAD_BELOW - 1);
        now.wrapping_add(behind)
            .wrapping_add(extra.saturating_mul(UNITS))
            & ARRIVAL_MASK
    }

    /// Nanoseconds from `now` until the bucket has room for one, if it has none: never
    /// short of when it has, and at most a few units long.
    pub(super) fn wait(&self, arrival: u64, now: u64) -> Option<u64> {
        let over = Self::ahead(arrival, now).checked_sub(self.tolerance)?;
        if over == 0 {
            return None;
        }
        // Units back to nanoseconds: units × window / (limit × 65,536), rounded up.
        let over = u128::from(over + WAIT_SLACK);
        let ns = (over * self.window).div_ceil(self.limit * u128::from(UNITS));
        Some(u64::try_from(ns).unwrap_or(u64::MAX))
    }
}

/// What a cycle advance needs besides the word: the contract's refill a cycle, and how
/// many other participants the owner said admit the key.
#[derive(Clone, Copy, Debug)]
pub(super) struct Advance {
    pub(super) cycle: u64,
    pub(super) refill: u64,
    pub(super) others: u64,
}

/// `fields` advanced to the cycle of `now` if that is due, as `DataPlane::on_cycle` sets
/// a key's allowance: the cycle after a share gets its share of the refill and keeps what
/// it left; otherwise its share of what the copy has left and the cycle refills, at least
/// one, and one more if it left one. Skipped cycles make one advance, with one refill.
pub(super) fn advanced(bucket: &Bucket, fields: Fields, advance: Advance, now: u64) -> Fields {
    let current = cycle_of(now, advance.cycle);
    if current == fields.cycle {
        return fields;
    }
    let after_share =
        advance.others > 0 && fields.shared && cycles_between(fields.cycle, current) == 1;
    // What the cycle left: nothing while no cycle has set an allowance.
    let left = if fields.set { fields.left } else { 0 };
    let allowance = if after_share {
        (advance.refill / (advance.others + 1)).max(1) + left
    } else {
        let sees = bucket
            .remaining(fields.arrival, bucket.units(now))
            .saturating_add(advance.refill);
        (sees / (advance.others + 1)).max(1) + left.min(1)
    };
    Fields {
        // Within the most whenever the contract fits (`fits`), which the proxy requires.
        left: allowance.min(MOST_LEFT),
        spent: 0,
        cycle: current,
        shared: !after_share,
        set: true,
        ..fields
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

    #[test]
    fn every_field_packs_and_unpacks_alone() {
        let all = Fields {
            arrival: ARRIVAL_MASK,
            left: MOST_LEFT,
            spent: MOST_LEFT,
            cycle: CYCLE_MASK,
            shared: true,
            closed: true,
            unreported: MOST_RESERVED,
            held: MOST_HELD,
            set: true,
        };
        assert_eq!(Fields::unpack(all.pack()), all);
        assert_eq!(all.pack(), u128::MAX, "the fields fill the word");
        let one = Fields {
            arrival: 0,
            left: 0,
            spent: 0,
            cycle: 0,
            shared: false,
            closed: false,
            unreported: 0,
            held: 0,
            set: false,
        };
        for fields in [
            Fields {
                arrival: 12_345,
                ..one
            },
            Fields { left: 7, ..one },
            Fields { spent: 5, ..one },
            Fields { cycle: 9, ..one },
            Fields {
                shared: true,
                ..one
            },
            Fields {
                closed: true,
                ..one
            },
            Fields {
                unreported: 3,
                ..one
            },
            Fields { held: 2, ..one },
            Fields { set: true, ..one },
        ] {
            assert_eq!(Fields::unpack(fields.pack()), fields);
        }
    }

    #[test]
    fn the_cycle_number_wraps_at_its_width() {
        assert_eq!(cycles_between(CYCLE_MASK, 1), 2);
    }

    /// The largest allowance a contract can set fits the word, or the contract is not
    /// pooled: a whole bucket and two cycles' refill, at a 10 ms cycle.
    #[test]
    fn a_contract_fits_while_its_largest_allowance_does() {
        let cycle = 10_000_000;
        assert!(fits(contract(100, 1_000_000_000, 100), cycle));
        // 65,535 - 1 carried - 2 x (refill 1 + its rounding 1) = 65,530 burst.
        assert!(fits(contract(100, 1_000_000_000, 65_530), cycle));
        assert!(!fits(contract(100, 1_000_000_000, 65_531), cycle));
        // 6 M a second: 60,000 a cycle, twice, is past it whatever the burst.
        assert!(!fits(contract(6_000_000, 1_000_000_000, 1), cycle));
        assert!(fits(contract(3_000_000, 1_000_000_000, 5_000), cycle));
    }

    /// A second, ten requests at once: the bucket's arithmetic in units.
    #[test]
    fn a_bucket_in_units_admits_its_burst_then_its_rate() {
        let bucket = Bucket::new(contract(10, 1_000_000_000, 10));
        let mut arrival = 0;
        let now = bucket.units(0);
        let mut admitted = 0;
        while bucket.admits(arrival, now) {
            arrival = Bucket::charge(arrival, now, 1);
            admitted += 1;
        }
        assert_eq!(admitted, 10);
        assert_eq!(bucket.remaining(arrival, now), 0);
        // One a tenth of a second, and a wait never short of it: up to the slack's two
        // units longer, a unit being 1/65,536 of the interval, 1,526 ns here.
        let wait = bucket.wait(arrival, now).unwrap();
        assert!((100_000_000..=100_003_052).contains(&wait), "{wait}");
        assert!(bucket.admits(arrival, bucket.units(100_000_000)));
        assert!(!bucket.admits(arrival, bucket.units(99_999_999)));
        assert_eq!(bucket.remaining(Bucket::release(arrival), now), 1);
    }

    /// Long after the last charge, the arrival time has fallen behind: still a full
    /// bucket, never one far ahead, for as long as the ahead half of the range lasts,
    /// 2^39 intervals (550 s at a billion a second, 17 years at a thousand). Past that an
    /// untouched key reads as owing; the transport evicts it long before, after two refill
    /// times idle, which `MOST_BURST` keeps to 2^38 intervals at most.
    #[test]
    fn an_old_arrival_is_behind_not_ahead() {
        let bucket = Bucket::new(contract(1_000_000_000, 1_000_000_000, 5));
        let arrival = Bucket::charge(0, bucket.units(0), 5);
        let margin = AHEAD_BELOW / UNITS;
        for later in [1_000_000, 2 * MOST_BURST, margin - 1] {
            let later = bucket.units(later);
            assert_eq!(Bucket::ahead(arrival, later), 0, "{later}");
            assert_eq!(bucket.remaining(arrival, later), 5);
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
        let bucket = Bucket::new(c);
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

    /// With others admitting, cycles alternate between sharing out the copy and sharing
    /// out the refill alone; a cycle skipped — the transport and workers away — breaks the
    /// alternation, and the next advance shares out the copy afresh.
    #[test]
    fn a_skipped_cycle_shares_out_the_copy_afresh() {
        let bucket = Bucket::new(contract(1_000, 1_000_000_000, 1_000));
        let cycle = 10_000_000;
        let advance = Advance {
            cycle,
            refill: 10,
            others: 1,
        };
        let start = Fields {
            arrival: bucket.units(0),
            left: MOST_LEFT,
            spent: 0,
            cycle: 0,
            shared: false,
            closed: false,
            unreported: 0,
            held: 0,
            set: false,
        };
        let first = advanced(&bucket, start, advance, cycle);
        assert!(first.shared, "the first shares out the copy");
        assert_eq!(first.left, (1_000 + 10) / 2);
        let next = advanced(&bucket, Fields { left: 0, ..first }, advance, 2 * cycle);
        assert!(!next.shared, "the next shares out the refill");
        assert_eq!(next.left, 10 / 2);
        let skipped = advanced(&bucket, Fields { left: 0, ..first }, advance, 4 * cycle);
        assert!(skipped.shared, "after a skipped cycle, the copy again");
        assert_eq!(
            skipped.left,
            (bucket.remaining(first.arrival, bucket.units(4 * cycle)) + 10) / 2
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
        // A request a microsecond: the range of units wraps every 2^56 / 2^16 µs, 12.7 days.
        let c = contract(1_000_000, 1_000_000_000, 3);
        let bucket = Bucket::new(c);
        let interval = 1_000;
        let wrap =
            u64::try_from((u128::from(ARRIVAL_MASK) + 1) / u128::from(UNITS)).unwrap() * interval;
        for start in [wrap - 2 * interval, wrap - interval / 2, wrap, wrap + 1] {
            let mut reference: TokenBucket = c.bucket();
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
                    arrival = Bucket::charge(arrival, units, 1);
                }
            }
        }
    }

    /// A burst too large for the arrival's range is held to the largest the range keeps
    /// apart from behind, rather than becoming no bound at all.
    #[test]
    fn a_burst_past_the_range_is_still_a_bound() {
        let bucket = Bucket::new(contract(1, 1_000_000_000, u64::MAX));
        let now = bucket.units(0);
        let full = bucket.remaining(0, now);
        assert!(full >= MOST_BURST, "{full}");
        let spent = Bucket::charge(0, now, full);
        assert!(!bucket.admits(spent, now), "the bucket emptied");
        assert!(bucket.wait(spent, now).is_some());
    }

    proptest! {
        /// At any time, not only on interval boundaries, and at low rates, around the
        /// moment the bucket has room again: never refused where the reference admits,
        /// admitted early by under two units (2/65,536 of an interval), and a refusal's
        /// wait never shorter than the reference's nor three units longer. Owners' answers
        /// rebase it now and then, as the transport does. The bounds: a rebased arrival is
        /// under two units early (its `now` and its `behind` each rounded down), and `now`
        /// under one unit late.
        #[test]
        fn off_the_grid_it_is_never_late_and_under_two_units_early(
            limit in 1u64..20,
            window_s in 1u64..3_600,
            burst in 1u64..12,
            steps in prop::collection::vec((0u64..2_000, any::<bool>()), 1..300),
        ) {
            let window = window_s * 1_000_000_000;
            let c = contract(limit, window, burst);
            let bucket = Bucket::new(c);
            let unit = unit_ns(c);
            let interval = window / limit;
            let mut reference: TokenBucket = c.bucket();
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
                    arrival = Bucket::charge(arrival, units, 1);
                } else if ours {
                    let due = reference.retry_at(now).unwrap() - now;
                    prop_assert!(due < 2 * unit, "admitted {} ns early, a unit {}", due, unit);
                } else {
                    let due = reference.retry_at(now).unwrap() - now;
                    let wait = bucket.wait(arrival, units).unwrap();
                    prop_assert!(wait >= due, "wait {} short of {}", wait, due);
                    prop_assert!(wait <= due + 3 * unit, "wait {} over {} by three units or more", wait, due);
                }
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
            let bucket = Bucket::new(c);
            let mut reference: TokenBucket = c.bucket();
            let mut arrival = 0;
            let mut now = 0u64;
            for step in steps {
                now += step * interval;
                let units = bucket.units(now);
                prop_assert_eq!(bucket.admits(arrival, units), reference.admits(now), "at {}", now);
                prop_assert_eq!(bucket.remaining(arrival, units), reference.remaining(now));
                if reference.admits(now) {
                    reference.charge(now);
                    arrival = Bucket::charge(arrival, units, 1);
                }
            }
        }
    }
}
