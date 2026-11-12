//! A key's state as one 128-bit word, as local mode's keys and the asynchronous mode's
//! entries keep it (#350), one compare-and-swap a charge: in its low half the limit and the
//! closed bit, laid out the same in both; in its high half, in the asynchronous mode, the
//! cycle's number and what syncing adds ([`Fields`]), and in local mode the whole number of
//! the cycle its key was last charged in ([`local`]). Pure, no atomics.
//!
//! The asynchronous mode's fields:
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

use crate::rate::MASK;

/// The most a cycle's allowance can be. The proxy refuses a pooled configuration whose
/// contracts could need more (`service::pooled::fits`).
pub(crate) const MOST_LEFT: u64 = (1 << 16) - 1;
/// The cycle number's range.
pub(crate) const CYCLE_MASK: u64 = (1 << 16) - 1;
/// The most admitted and not yet drained: about a billion, which the transport, draining
/// every key's count every cycle it visits it, never lets a key reach. Past it, an admission
/// is not counted rather than wrap the count.
pub(crate) const MOST_UNREPORTED: u64 = (1 << 30) - 1;

/// The word's fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fields {
    pub(crate) arrival: u64,
    pub(crate) left: u64,
    pub(crate) cycle: u64,
    pub(crate) shared: bool,
    pub(crate) closed: bool,
    pub(crate) unreported: u64,
    pub(crate) set: bool,
}

impl Fields {
    pub(crate) fn pack(self) -> u128 {
        u128::from(self.arrival & MASK)
            | u128::from(self.closed) << 63
            | u128::from(self.left & MOST_LEFT) << 64
            | u128::from(self.cycle & CYCLE_MASK) << 80
            | u128::from(self.shared) << 96
            | u128::from(self.set) << 97
            | u128::from(self.unreported & MOST_UNREPORTED) << 98
    }

    pub(crate) fn unpack(word: u128) -> Self {
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

/// Local mode's key as one word: its word in the low half, laid out as [`Fields`]' (the
/// arrival, or a fixed window's number and count, below the closed bit), and in the high
/// half the whole number of the cycle it was last charged in. Local mode has no sync state
/// to share the high half with, so it keeps the cycle's number whole, never wrapping.
pub(crate) fn local(word: u64, cycle: u64) -> u128 {
    u128::from(cycle) << 64 | u128::from(word)
}

/// The same read back: the word and the cycle.
pub(crate) fn local_parts(state: u128) -> (u64, u64) {
    // Each half is 64 bits: the casts keep them whole.
    (state as u64, (state >> 64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Local mode's word sits in the low half as the asynchronous mode's fields do, the
    /// closed bit on top, and reads back whole with its cycle.
    #[test]
    fn local_modes_word_is_the_low_half() {
        let fields = Fields {
            arrival: 12_345,
            closed: true,
            left: 0,
            cycle: 0,
            shared: false,
            set: false,
            unreported: 0,
        };
        let word = 12_345 | 1 << 63;
        assert_eq!(local(word, 0), fields.pack());
        assert_eq!(local_parts(local(word, u64::MAX)), (word, u64::MAX));
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
}
