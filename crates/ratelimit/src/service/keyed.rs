//! The data plane's hash for its keys: keyed, so that clients choosing the keys — an API
//! key, an address — cannot choose ones that collide without knowing the key, and cheap,
//! as a decision hashes each of its limits' keys once (06 §4).
//!
//! Eight bytes at a time folded in with a 64 × 64 → 128-bit multiply, the halves of the
//! product added together by exclusive or: the design of wyhash (public domain), written
//! here rather than taken as a dependency. The crate draws no randomness of its own: the
//! key is passed in.

use std::hash::{BuildHasher, Hasher};

/// Constants of no other meaning than having their bits spread: wyhash's.
const P0: u64 = 0xa076_1d64_78bd_642f;
const P1: u64 = 0xe703_7ed1_a0b4_28db;
const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
const P3: u64 = 0x5899_65cc_7537_4cc3;

/// The two halves of `a × b`, exclusive or'd: every bit of each depends on every bit of
/// both.
fn fold(a: u64, b: u64) -> u64 {
    let product = u128::from(a) * u128::from(b);
    (product as u64) ^ ((product >> 64) as u64)
}

/// The first eight bytes of `bytes`, at least eight long, as a little-endian number.
fn eight(bytes: &[u8]) -> u64 {
    bytes
        .get(..8)
        .and_then(|first| first.try_into().ok())
        .map_or(0, u64::from_le_bytes)
}

/// The first four bytes of `bytes`, at least four long, as a little-endian number.
fn four(bytes: &[u8]) -> u64 {
    bytes
        .get(..4)
        .and_then(|first| first.try_into().ok())
        .map_or(0, |word| u64::from(u32::from_le_bytes(word)))
}

/// The keyed hash, as a map's hasher: [`DataPlane::with_hasher`](super::DataPlane::with_hasher).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keyed {
    seed: u64,
    secret: u64,
}

impl Keyed {
    /// The hash under `key`, which should be random and kept from clients: a process draws
    /// one when it starts.
    #[must_use]
    pub fn new(key: [u64; 2]) -> Self {
        Self {
            seed: key[0] ^ P0,
            secret: fold(key[1] ^ P1, P2) | 1,
        }
    }
}

impl BuildHasher for Keyed {
    type Hasher = KeyedHasher;

    fn build_hasher(&self) -> KeyedHasher {
        KeyedHasher {
            state: self.seed,
            secret: self.secret,
        }
    }
}

/// A [`Keyed`] hash under way.
#[derive(Clone, Debug)]
pub struct KeyedHasher {
    state: u64,
    secret: u64,
}

impl Hasher for KeyedHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut state = self.state;
        let mut rest = bytes;
        while rest.len() > 16 {
            state = fold(eight(rest) ^ self.secret ^ P0, eight(&rest[8..]) ^ state);
            rest = &rest[16..];
        }
        // The last sixteen bytes or fewer, as two words that may overlap.
        let length = rest.len();
        let (a, b) = match length {
            0 => (0, 0),
            1..=3 => {
                let small = (u64::from(rest[0]) << 16)
                    | (u64::from(rest[length / 2]) << 8)
                    | u64::from(rest[length - 1]);
                (small, 0)
            }
            4..=8 => (four(rest) << 32 | four(&rest[length - 4..]), 0),
            _ => (eight(rest), eight(&rest[length - 8..])),
        };
        self.state = fold(
            a ^ self.secret ^ P1,
            b ^ state ^ (bytes.len() as u64).wrapping_mul(P3),
        );
    }

    fn write_u64(&mut self, n: u64) {
        self.state = fold(n ^ self.secret ^ P2, self.state ^ P3);
    }

    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }

    fn finish(&self) -> u64 {
        fold(self.state ^ P0, self.secret ^ P1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    const KEY: [u64; 2] = [0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210];

    fn hash(keyed: Keyed, bytes: &[u8]) -> u64 {
        keyed.hash_one(bytes)
    }

    #[test]
    fn the_same_bytes_under_the_same_key_hash_alike_and_under_another_differently() {
        let (one, other) = (Keyed::new(KEY), Keyed::new([KEY[0], KEY[1] + 1]));
        for bytes in [
            &b""[..],
            b"a",
            b"api/0\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9",
        ] {
            assert_eq!(hash(one, bytes), hash(Keyed::new(KEY), bytes));
            assert_ne!(hash(one, bytes), hash(other, bytes), "{bytes:?}");
        }
        assert_ne!(
            hash(Keyed::new(KEY), b"k"),
            hash(Keyed::new([KEY[0] + 1, KEY[1]]), b"k"),
            "both halves of the key count"
        );
    }

    /// A hundred thousand keys as the proxy makes them, a limit's name and an API key's
    /// value: no two alike, and spread evenly over a map's low bits.
    #[test]
    fn many_keys_hash_apart_and_spread_over_the_low_bits() {
        let keyed = Keyed::new(KEY);
        let mut seen = HashSet::new();
        let mut buckets = vec![0_u32; 1_024];
        for n in 0..100_000 {
            let key = format!("api/0\0client-{n:08}");
            let hashed = hash(keyed, key.as_bytes());
            assert!(seen.insert(hashed), "{key}");
            buckets[(hashed & 1_023) as usize] += 1;
        }
        // About 98 a bucket: none should be near twice that, or near empty.
        let (low, high) = (buckets.iter().min(), buckets.iter().max());
        assert!(
            low.is_some_and(|low| *low > 50) && high.is_some_and(|high| *high < 150),
            "{low:?} {high:?}"
        );
    }

    /// Addresses, four or sixteen bytes after a limit's name, as client keys are.
    #[test]
    fn addresses_hash_apart() {
        let keyed = Keyed::new(KEY);
        let mut seen = HashSet::new();
        for a in 0..=255_u8 {
            for b in 0..=255_u8 {
                let mut key = b"per-client\0".to_vec();
                key.extend_from_slice(&[10, 0, a, b]);
                assert!(seen.insert(hash(keyed, &key)));
            }
        }
    }

    proptest! {
        /// Changing any one byte, of a key of any length, changes its hash; so does adding
        /// one at the end.
        #[test]
        fn any_one_byte_changed_or_added_changes_the_hash(
            bytes in prop::collection::vec(any::<u8>(), 1..80),
            at in any::<prop::sample::Index>(),
            flip in 1..=255_u8,
        ) {
            let keyed = Keyed::new(KEY);
            let at = at.index(bytes.len());
            let mut changed = bytes.clone();
            changed[at] ^= flip;
            prop_assert_ne!(hash(keyed, &bytes), hash(keyed, &changed));
            let mut longer = bytes.clone();
            longer.push(0);
            prop_assert_ne!(hash(keyed, &bytes), hash(keyed, &longer));
        }
    }
}
