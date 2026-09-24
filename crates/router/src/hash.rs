//! The hash map the indexes are built on.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// A map from bytes with an unseeded hasher instead of the randomly seeded default: a pure
/// crate takes no randomness, and benchmarks that count instructions need identical runs.
/// Random seeding defends against chosen colliding keys, but the keys stored here come from
/// configuration, not from requests: a request's host or path is only looked up, and no
/// lookup costs more than the entries its key collides with, which a config chose.
pub(crate) type Map<V> = HashMap<Box<[u8]>, V, BuildHasherDefault<Keys>>;

/// Hashes a key eight bytes at a time with a rotate, an exclusive or and a multiply each,
/// the scheme of rustc's `FxHasher`. A keyed hash would buy nothing here (see [`Map`]), and
/// is several times the work on every lookup a request makes.
#[derive(Debug, Default)]
pub(crate) struct Keys(u64);

/// An odd constant whose bits are well mixed: what the multiply spreads each word with.
const MIX: u64 = 0xf135_7aea_2e62_a9c5;

impl Keys {
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(MIX);
    }
}

impl Hasher for Keys {
    fn write(&mut self, bytes: &[u8]) {
        let (words, tail) = bytes.as_chunks::<8>();
        for word in words {
            self.add(u64::from_le_bytes(*word));
        }
        if !tail.is_empty() {
            // Padded with zeroes; the length a slice is hashed with tells a key from the
            // same key padded.
            let mut last = [0; 8];
            last[..tail.len()].copy_from_slice(tail);
            self.add(u64::from_le_bytes(last));
        }
    }

    fn write_usize(&mut self, number: usize) {
        self.add(number as u64);
    }

    fn finish(&self) -> u64 {
        // A multiply mixes upwards only, so its low bits are its weakest; the map files
        // entries by the low bits, and turning the word brings the well mixed high ones down.
        self.0.rotate_left(26)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::hash::BuildHasher;

    fn hash(key: &[u8]) -> u64 {
        BuildHasherDefault::<Keys>::default().hash_one(key)
    }

    /// A map looks a key up by a slice and stores it boxed; both must hash alike.
    #[test]
    fn a_key_hashes_the_same_however_it_is_held() {
        for key in [
            &b""[..],
            b"a",
            b"example.com",
            b"a-name-longer-than-sixteen-bytes",
        ] {
            let boxed: Box<[u8]> = key.into();
            assert_eq!(
                BuildHasherDefault::<Keys>::default().hash_one(&boxed),
                hash(key),
                "{key:?}"
            );
        }
    }

    /// Bytes past the end of a key are not the key: a padded tail must not make them equal.
    #[test]
    fn a_key_does_not_hash_as_its_padded_self() {
        for (short, long) in [
            (&b""[..], &b"\0"[..]),
            (b"ab", b"ab\0"),
            (b"abcdefgh", b"abcdefgh\0"),
        ] {
            assert_ne!(hash(short), hash(long), "{short:?}");
        }
    }

    /// Keys as configs have them — host names and path segments that differ in a few
    /// characters — land apart both in the bits the map files entries by and in the top
    /// seven it tells them apart with.
    #[test]
    fn keys_a_config_would_have_hash_apart() {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..512 {
            keys.push(format!("service-{i}.example.com").into_bytes());
            keys.push(format!("{i}").into_bytes());
            keys.push(format!("v{i}").into_bytes());
        }
        let hashes: Vec<u64> = keys.iter().map(|key| hash(key)).collect();
        let whole: HashSet<u64> = hashes.iter().copied().collect();
        assert_eq!(whole.len(), keys.len(), "whole hashes collide");
        // 1,536 keys in 2,048 buckets: an even spread leaves about 55% of them used.
        let buckets: HashSet<u64> = hashes.iter().map(|hash| hash & 2047).collect();
        assert!(buckets.len() > 900, "{} buckets used", buckets.len());
        let tags: HashSet<u64> = hashes.iter().map(|hash| hash >> 57).collect();
        assert_eq!(tags.len(), 128, "{} of 128 tags used", tags.len());
    }
}
