//! A key's make-up: the two halves a data plane hashes it into, the bit that says its mode,
//! and the store's spreading of all of it over shards and phases.
//!
//! A data plane's keyed hash keeps 64 bits of state, so a longer output alone would collide
//! as often as 64 bits do: a key is two runs of it over the same bytes, under the secret every
//! data plane shares and under a second drawn from it ([`drawn`]), joined ([`joined`]), the
//! top bit then set or cleared for its mode ([`Kind::key`]). The first run's half is the
//! data plane's own hash of the key, which its maps and an asynchronous participant's
//! matching of answers go by ([`lower`]).

use crate::Key;
use crate::table::mix;

/// The bit of a key that says its mode: clear for the synchronous mode's, set for the
/// asynchronous mode's. A data plane sets it as it hashes the key, so that the two kinds of
/// value never meet under one key, even where two hashes collide. It is the top of the second
/// run's half, leaving the first run's whole.
pub const KIND_BIT: Key = 1 << (Key::BITS - 1);

/// A key's mode, as its [`KIND_BIT`] says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// The synchronous mode's: the owner decides every request.
    Synchronous,
    /// The asynchronous mode's: data planes decide, the owner applies their hits.
    Asynchronous,
}

impl Kind {
    /// The mode of `key`.
    #[must_use]
    pub const fn of(key: Key) -> Self {
        if key & KIND_BIT == 0 {
            Self::Synchronous
        } else {
            Self::Asynchronous
        }
    }

    /// `hash` as a key of this mode: its top bit cleared or set.
    #[must_use]
    pub const fn key(self, hash: Key) -> Key {
        match self {
            Self::Synchronous => hash & !KIND_BIT,
            Self::Asynchronous => hash | KIND_BIT,
        }
    }
}

/// The first run of the keyed hash, `lower`, and the second, `upper`, as one key, its mode's
/// bit not yet said ([`Kind::key`]).
#[must_use]
pub const fn joined(lower: u64, upper: u64) -> Key {
    ((upper as Key) << 64) | lower as Key
}

/// The first run's half of `key`: the data plane's own hash of it.
#[must_use]
pub const fn lower(key: Key) -> u64 {
    // The lower 64 bits, by design.
    key as u64
}

/// The secret of the second run of the keyed hash, drawn from `secret`, the one every data
/// plane shares: alike on every data plane, unrelated to the first run's to anyone without it.
#[must_use]
pub const fn drawn(secret: [u64; 2]) -> [u64; 2] {
    [
        mix(secret[0] ^ 0x243f_6a88_85a3_08d3),
        mix(secret[1] ^ 0x1319_8a2e_0370_7344),
    ]
}

/// All of `key` folded into 64 bits and spread: what its shard and a window's phase are taken
/// from, so that two keys differing in either half fall apart. Its halves are already keyed
/// hashes, their bits spread, so one mix of the two folded together is enough.
#[must_use]
pub const fn mixed(key: Key) -> u64 {
    // The upper half, by design.
    mix(lower(key) ^ (key >> 64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A key's bit says its mode, and either mode's key of a hash keeps the rest of it.
    #[test]
    fn a_keys_top_bit_says_its_mode() {
        let hash = joined(0x1234, 0x5678);
        let synchronous = Kind::Synchronous.key(hash | KIND_BIT);
        let asynchronous = Kind::Asynchronous.key(hash);
        assert_eq!(Kind::of(synchronous), Kind::Synchronous);
        assert_eq!(Kind::of(asynchronous), Kind::Asynchronous);
        assert_eq!(synchronous, hash);
        assert_eq!(asynchronous & !KIND_BIT, hash);
        assert_ne!(synchronous, asynchronous);
    }

    /// The mode's bit is in the second run's half: the first's is the data plane's hash,
    /// whole, whichever the mode.
    #[test]
    fn the_modes_bit_leaves_the_first_runs_half_whole() {
        let hash = joined(u64::MAX, 7);
        assert_eq!(lower(Kind::Synchronous.key(hash)), u64::MAX);
        assert_eq!(lower(Kind::Asynchronous.key(hash)), u64::MAX);
    }

    /// The second run's secret differs from the first's, in both words.
    #[test]
    fn the_second_runs_secret_is_its_own() {
        for secret in [[0, 0], [1, 2], [u64::MAX, u64::MAX]] {
            let drawn = drawn(secret);
            assert_ne!(drawn[0], secret[0]);
            assert_ne!(drawn[1], secret[1]);
        }
    }

    proptest! {
        /// The halves go in and come out as they were.
        #[test]
        fn a_key_keeps_its_halves(first: u64, second: u64) {
            let key = joined(first, second);
            prop_assert_eq!(lower(key), first);
            prop_assert_eq!((key >> 64) as u64, second);
        }

        /// Two keys differing only in the second half are spread apart: the spread is taken
        /// from all of a key.
        #[test]
        fn the_spread_reads_both_halves(first: u64, second: u64, other: u64) {
            prop_assume!(second != other);
            prop_assert_ne!(mixed(joined(first, second)), mixed(joined(first, other)));
        }
    }
}
