//! A key's admission state as one 128-bit word, and the arithmetic on it: pure, no atomics.
//! Its low half is laid out as local mode's word is, the bucket's arrival and the closed bit;
//! its high half holds what syncing adds (#350).
//!
//! | Field | Bits | |
//! |---|---|---|
//! | arrival | 63 | the GCRA arrival time, modulo 2^63, in units of 1/65,536 of the interval |
//! | closed | 1 | evicted: nothing more is taken |
//! | left | 16 | what the cycle under way may still admit |
//! | cycle | 16 | the cycle number of the last advance, modulo 2^16 |
//! | shared | 1 | whether that advance shared out the copy |
//! | set | 1 | a cycle has set the allowance: until then there is none, as a new key has none |
//! | unreported | 30 | admitted here and not yet drained by the transport into its ledger |

pub(super) use crate::rate::Rate;
use crate::rate::{MASK, MOST_BURST};
use crate::service::Contract;

/// The most a cycle's allowance can be. The proxy refuses a pooled configuration whose contracts could need more ([`fits`]).
pub(super) const MOST_LEFT: u64 = (1 << 16) - 1;
const CYCLE_MASK: u64 = (1 << 16) - 1;
/// The most admitted and not yet drained: about a billion, which the transport, draining
/// every key's count every cycle it visits it, never lets a key reach. Past it, an admission
/// is not counted rather than wrap the count.
pub(super) const MOST_UNREPORTED: u64 = (1 << 30) - 1;

/// Whether `contract` fits a pooled entry at `cycle`: the largest allowance a cycle can set,
/// a whole bucket with the slack's refill ([`SLACK`](crate::SLACK)) and two cycles' refill
/// (the cycle after a share keeps what the one before left) and one carried, within
/// `MOST_LEFT`.
#[must_use]
pub fn fits(contract: Contract, cycle: u64) -> bool {
    let refill = contract.refilled(cycle.max(1)).saturating_add(1);
    let slack = contract.refilled(crate::SLACK).saturating_add(1);
    contract
        .burst
        .get()
        .min(MOST_BURST)
        .saturating_add(slack)
        .saturating_add(refill.saturating_mul(2))
        .saturating_add(1)
        <= MOST_LEFT
}

/// The word's fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Fields {
    pub(super) arrival: u64,
    pub(super) left: u64,
    pub(super) cycle: u64,
    pub(super) shared: bool,
    pub(super) closed: bool,
    pub(super) unreported: u64,
    pub(super) set: bool,
}

impl Fields {
    pub(super) fn pack(self) -> u128 {
        u128::from(self.arrival & MASK)
            | u128::from(self.closed) << 63
            | u128::from(self.left & MOST_LEFT) << 64
            | u128::from(self.cycle & CYCLE_MASK) << 80
            | u128::from(self.shared) << 96
            | u128::from(self.set) << 97
            | u128::from(self.unreported & MOST_UNREPORTED) << 98
    }

    pub(super) fn unpack(word: u128) -> Self {
        // Each field is masked to its width first: the casts keep only what fits.
        let field = |shift: u32, mask: u64| (word >> shift) as u64 & mask;
        Self {
            arrival: field(0, MASK),
            closed: field(63, 1) == 1,
            left: field(64, MOST_LEFT),
            cycle: field(80, CYCLE_MASK),
            shared: field(96, 1) == 1,
            set: field(97, 1) == 1,
            unreported: field(98, MOST_UNREPORTED),
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

/// Cycles one step of an entry's cycle record stands for: the record is the full cycle
/// number shifted down by this, so that 32 bits cover 2^40 cycles (348 years at 10 ms).
const RECORD_SHIFT: u32 = 8;

/// Cycles past the record from which the word's 16-bit number is not trusted: half its
/// range, room enough for the record's coarseness and for a record that lags its word.
const LONG_GAP: u64 = 1 << 15;

/// The record of `now`'s cycle, at `cycle` nanoseconds a cycle: what an entry keeps beside
/// its word, written after each compare-and-swap that advances it, so that a cycle number
/// that wrapped is told from one that did not.
pub(super) fn record_of(now: u64, cycle: u64) -> u32 {
    let full = (now / cycle.max(1)) >> RECORD_SHIFT;
    // Wraps past 2^40 cycles, which no process lives to see.
    u32::try_from(full & u64::from(u32::MAX)).unwrap_or(u32::MAX)
}

/// Where `now` is from the cycle a word holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// The word's cycle, or one after it: the word was advanced by a request whose clock
    /// read later than this one's, and is not to be moved back.
    Same,
    /// The cycle after the word's.
    Next,
    /// Two cycles or more after it, however long: the 16-bit number may have wrapped.
    Gap,
}

/// Where `now` is from a word's `cycle` field, the entry's `record` (no later than the
/// cycle the word was last advanced to) telling a long gap apart from a wrapped number.
pub(super) fn step(cycle: u64, record: u32, now: u64, length: u64) -> Step {
    let current = now / length.max(1);
    let floor = u64::from(record) << RECORD_SHIFT;
    // How far the word's cycle can be behind `now`: no further than the record.
    let Some(most) = current.checked_sub(floor) else {
        return Step::Same;
    };
    if most >= LONG_GAP {
        return Step::Gap;
    }
    // Within half the field's range, the number is exact; past what the record allows, the
    // word is ahead.
    match cycles_between(cycle, current & CYCLE_MASK) {
        0 => Step::Same,
        ahead if ahead > most => Step::Same,
        1 => Step::Next,
        _ => Step::Gap,
    }
}

/// What a cycle advance needs besides the word: the contract's refill a cycle, in requests
/// and in units, how many other participants the owner said admit the key, and the entry's
/// cycle record.
#[derive(Clone, Copy, Debug)]
pub(super) struct Advance {
    pub(super) cycle: u64,
    pub(super) refill: u64,
    pub(super) refill_units: u64,
    pub(super) others: u64,
    pub(super) record: u32,
}

/// `fields` advanced to the cycle of `now` if that is due, as `DataPlane::on_cycle` sets
/// a key's allowance: the cycle after a share gets its share of the refill and keeps what
/// it left; otherwise its share of what the copy has left and the cycle refills, at least
/// one, and one more if it left one. A clock behind the word's cycle leaves it as it is.
///
/// Waking after a gap of two cycles or more, however long, is one advance of the second
/// kind: the bucket refilled from the time gone by, never past its burst; the share from
/// the sharing it last heard of; at most one carried; the count untouched. Missed
/// cycles add no allowance, and the copy's freshness is not this function's to change.
pub(super) fn advanced(bucket: &Rate, fields: Fields, advance: Advance, now: u64) -> Fields {
    let step = step(fields.cycle, advance.record, now, advance.cycle);
    if step == Step::Same {
        return fields;
    }
    let current = cycle_of(now, advance.cycle);
    let after_share = advance.others > 0 && fields.shared && step == Step::Next;
    // What the cycle left: nothing while no cycle has set an allowance.
    let left = if fields.set { fields.left } else { 0 };
    let allowance = if after_share {
        (advance.refill / (advance.others + 1)).max(1) + left
    } else {
        let sees = bucket.remaining_within(fields.arrival, bucket.units(now), advance.refill_units);
        (sees / (advance.others + 1)).max(1) + left.min(1)
    };
    Fields {
        // Within the most whenever the contract fits (`fits`), which the proxy requires.
        left: allowance.min(MOST_LEFT),
        cycle: current,
        shared: !after_share,
        set: true,
        ..fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// The low half is laid out as local mode's word is: the bucket's arrival below the
    /// closed bit, all of it; what syncing adds is all in the high half.
    #[test]
    fn the_low_half_is_laid_out_as_local_modes_word() {
        let none = Fields {
            arrival: 0,
            closed: false,
            left: 0,
            cycle: 0,
            shared: false,
            set: false,
            unreported: 0,
        };
        let arrival = Fields {
            arrival: MASK,
            ..none
        };
        assert_eq!(arrival.pack(), u128::from(u64::MAX >> 1));
        let closed = Fields {
            closed: true,
            ..none
        };
        assert_eq!(closed.pack(), 1 << 63);
        let synced = Fields {
            left: MOST_LEFT,
            cycle: CYCLE_MASK,
            shared: true,
            set: true,
            unreported: MOST_UNREPORTED,
            ..none
        };
        assert_eq!(synced.pack(), u128::from(u64::MAX) << 64);
    }

    #[test]
    fn every_field_packs_and_unpacks_alone() {
        let all = Fields {
            arrival: MASK,
            left: MOST_LEFT,
            cycle: CYCLE_MASK,
            shared: true,
            closed: true,
            unreported: MOST_UNREPORTED,
            set: true,
        };
        assert_eq!(Fields::unpack(all.pack()), all);
        assert_eq!(all.pack(), u128::MAX, "the fields fill the word");
        let one = Fields {
            arrival: 0,
            left: 0,
            cycle: 0,
            shared: false,
            closed: false,
            unreported: 0,
            set: false,
        };
        for fields in [
            Fields {
                arrival: 12_345,
                ..one
            },
            Fields { left: 7, ..one },
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
    /// pooled: a whole bucket with the slack's refill and two cycles' refill, at a 10 ms
    /// cycle.
    #[test]
    fn a_contract_fits_while_its_largest_allowance_does() {
        let cycle = 10_000_000;
        assert!(fits(contract(100, 1_000_000_000, 100), cycle));
        // 65,535 - 1 carried - the slack's refill (0 + its rounding 1) - 2 x (refill 1 + its
        // rounding 1) = 65,529 burst.
        assert!(fits(contract(100, 1_000_000_000, 65_529), cycle));
        assert!(!fits(contract(100, 1_000_000_000, 65_530), cycle));
        // 6 M a second: 60,000 a cycle, twice, is past it whatever the burst.
        assert!(!fits(contract(6_000_000, 1_000_000_000, 1), cycle));
        assert!(fits(contract(3_000_000, 1_000_000_000, 5_000), cycle));
    }

    /// With others admitting, cycles alternate between sharing out the copy and sharing
    /// out the refill alone; a cycle skipped — the transport and workers away — breaks the
    /// alternation, and the next advance shares out the copy afresh.
    #[test]
    fn a_skipped_cycle_shares_out_the_copy_afresh() {
        let bucket = Rate::new(contract(1_000, 1_000_000_000, 1_000));
        let cycle = 10_000_000;
        let advance = Advance {
            cycle,
            refill: 10,
            refill_units: bucket.units(cycle),
            others: 1,
            record: 0,
        };
        let start = Fields {
            arrival: bucket.units(0),
            left: MOST_LEFT,
            cycle: 0,
            shared: false,
            closed: false,
            unreported: 0,
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
            bucket.remaining_within(first.arrival, bucket.units(4 * cycle), bucket.units(cycle))
                / 2
        );
    }

    const LENGTH: u64 = 10_000_000;

    /// `step` for a word last advanced to full cycle `last`, its record current, at `now`'s
    /// full cycle `current`.
    fn step_at(last: u64, current: u64) -> Step {
        step(
            last & CYCLE_MASK,
            record_of(last * LENGTH, LENGTH),
            current * LENGTH + LENGTH / 2,
            LENGTH,
        )
    }

    /// The same cycle, the next, a gap, and a clock behind the word's, told apart near and
    /// far from where the 16-bit number wraps; a gap of a multiple of its range is a gap.
    #[test]
    fn the_step_tells_same_next_gap_and_behind() {
        for last in [0, 5, CYCLE_MASK - 1, CYCLE_MASK, 1 << 20, (1 << 33) + 7] {
            assert_eq!(step_at(last, last), Step::Same, "{last}");
            assert_eq!(step_at(last, last + 1), Step::Next, "{last}");
            assert_eq!(step_at(last, last + 2), Step::Gap, "{last}");
            for wrapped in [1 << 16, (1 << 16) + 1, 3 << 16, (3 << 16) + 1, 1 << 30] {
                assert_eq!(
                    step_at(last, last + wrapped),
                    Step::Gap,
                    "{last} + {wrapped}"
                );
            }
            if last > 0 {
                assert_eq!(step_at(last, last - 1), Step::Same, "behind: {last}");
            }
        }
    }

    proptest! {
        /// With the record lagging its word by up to a quarter of the field's range, the step
        /// is the true one wherever the gap is short of it, and a gap past half the range
        /// is a gap whatever the 16-bit number says.
        #[test]
        fn the_step_is_true_with_a_lagging_record(
            last in 0u64..1 << 40,
            lag in 0u64..1 << 14,
            delta in -4i64..1 << 20,
        ) {
            let Some(current) = last.checked_add_signed(delta) else {
                return Ok(());
            };
            let record = record_of(last.saturating_sub(lag) * LENGTH, LENGTH);
            let got = step(last & CYCLE_MASK, record, current * LENGTH, LENGTH);
            let truth = match delta {
                ..=0 => Step::Same,
                1 => Step::Next,
                _ => Step::Gap,
            };
            if !((1 << 14) - 256..1 << 15).contains(&delta) {
                prop_assert_eq!(got, truth);
            } else {
                // Between: the number is exact, or the record says a gap; never behind.
                prop_assert_eq!(got, Step::Gap);
            }
        }

        /// Waking after a gap, however long, gives at most the share of the bucket refilled
        /// from the time gone by and one cycle's refill, and one carried; at least one; the
        /// count and the bucket as they were. Going quiet and waking gains nothing a cycle's
        /// share does not allow.
        #[test]
        fn waking_after_a_gap_is_bounded(
            limit in 1u64..10_000,
            burst in 1u64..10_000,
            others in 0u64..8,
            left in 0u64..=MOST_LEFT,
            shared: bool,
            set: bool,
            unreported in 0u64..=MOST_UNREPORTED,
            last in 0u64..1 << 30,
            gap in 2u64..1 << 20,
            charged in 0u64..10_000,
        ) {
            let c = contract(limit, 1_000_000_000, burst);
            let bucket = Rate::new(c);
            let start = last * LENGTH;
            let arrival = (0..charged.min(burst)).fold(bucket.units(start), |at, _| {
                Rate::charge(at, bucket.units(start), 1)
            });
            let fields = Fields {
                arrival, left, cycle: last & CYCLE_MASK, shared, closed: false, unreported,
                set,
            };
            let refill = c.refilled(LENGTH);
            let advance = Advance {
                cycle: LENGTH,
                refill,
                refill_units: bucket.units(LENGTH),
                others,
                record: record_of(start, LENGTH),
            };
            let now = (last + gap) * LENGTH;
            let woken = advanced(&bucket, fields, advance, now);
            let share = (bucket.remaining_within(arrival, bucket.units(now), bucket.units(LENGTH))
                / (others + 1))
                .max(1);
            prop_assert!(woken.left >= 1);
            prop_assert!(woken.left <= (share + 1).min(MOST_LEFT), "{} > {}", woken.left, share + 1);
            prop_assert_eq!(woken.cycle, (last + gap) & CYCLE_MASK);
            prop_assert_eq!(
                (woken.arrival, woken.unreported, woken.closed),
                (fields.arrival, fields.unreported, fields.closed)
            );
        }
    }
}
