//! A set of keys found by their names, each kept with its name's hash, in tables that grow
//! one at a time (GitHub issue #111).
//!
//! A table grows by doubling, and the decision that makes it grow pays for moving every key
//! in it. A standard `HashSet` hashes each key anew, read from its own allocation, which
//! with half a million keys held a decision 108 ms; moved by the hashes they carry, the
//! keys are not read at all. A set split into parts by bits of the hash also doubles a part
//! at a time: a worker's handles, in 64 parts, move a 64th of their keys in any one
//! decision.

use super::Key;
use super::sync::Arc;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use std::hash::BuildHasher;

/// The parts a worker's handles are split into.
pub(super) const HANDLE_PARTS: usize = 64;

/// The bits of a hash below a part's: a table places a key by the hash's low bits and tags
/// it with its top seven, and a registry chooses its shard by bits 32 to 39 (`Local`).
const PART_SHIFT: u32 = 40;

/// A key with its name's hash.
#[derive(Debug)]
struct Held {
    hash: u64,
    key: Arc<Key>,
}

/// The bytes one key takes in a set: what `key_bytes` counts for each set holding it.
pub(super) const SLOT_BYTES: u64 = size_of::<Held>() as u64 + 1;

/// Keys found by their names, hashed by `S`, in `PARTS` parts, a power of two.
#[derive(Debug)]
pub(super) struct KeySet<S, const PARTS: usize> {
    parts: [HashTable<Held>; PARTS],
    len: usize,
    hasher: S,
}

impl<S: BuildHasher, const PARTS: usize> KeySet<S, PARTS> {
    const POWER_OF_TWO: () = assert!(PARTS.is_power_of_two(), "parts: a power of two");

    /// None yet.
    pub(super) fn with_hasher(hasher: S) -> Self {
        let () = Self::POWER_OF_TWO;
        Self {
            parts: std::array::from_fn(|_| HashTable::new()),
            len: 0,
            hasher,
        }
    }

    /// The hash `name` is found by.
    pub(super) fn hash(&self, name: &[u8]) -> u64 {
        self.hasher.hash_one(name)
    }

    /// The part of a hash: a power of two parts, so the remainder is the low bits.
    fn part_of(hash: u64) -> usize {
        // Truncated to the bits a part takes, which fit any `usize`.
        (hash >> PART_SHIFT) as usize % PARTS
    }

    fn part(&self, hash: u64) -> &HashTable<Held> {
        &self.parts[Self::part_of(hash)]
    }

    fn part_mut(&mut self, hash: u64) -> &mut HashTable<Held> {
        &mut self.parts[Self::part_of(hash)]
    }

    /// The key of `name`, if held.
    pub(super) fn get(&self, name: &[u8]) -> Option<&Arc<Key>> {
        self.get_hashed(self.hash(name), name)
    }

    /// The key of `name`, whose hash is `hash`, if held.
    pub(super) fn get_hashed(&self, hash: u64, name: &[u8]) -> Option<&Arc<Key>> {
        self.part(hash)
            .find(hash, |held| *held.key.key == *name)
            .map(|held| &held.key)
    }

    /// Holds `key`, whose name's hash is `hash`, in place of any of its name.
    pub(super) fn replace_hashed(&mut self, hash: u64, key: Arc<Key>) {
        let added =
            match self
                .part_mut(hash)
                .entry(hash, |held| held.key.key == key.key, |held| held.hash)
            {
                Entry::Occupied(mut found) => {
                    found.get_mut().key = key;
                    false
                }
                Entry::Vacant(room) => {
                    room.insert(Held { hash, key });
                    true
                }
            };
        if added {
            self.len += 1;
        }
    }

    /// Lets go of the key of `name`, if held.
    pub(super) fn remove(&mut self, name: &[u8]) {
        let hash = self.hash(name);
        if let Ok(found) = self
            .part_mut(hash)
            .find_entry(hash, |held| *held.key.key == *name)
        {
            found.remove();
            self.len -= 1;
        }
    }

    /// Keeps only the keys `keep` takes.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&Arc<Key>) -> bool) {
        for part in &mut self.parts {
            part.retain(|held| keep(&held.key));
        }
        self.len = self.parts.iter().map(HashTable::len).sum();
    }

    /// How many it holds.
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Whether it holds none.
    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// How many each part holds.
    #[cfg(test)]
    pub(super) fn part_lens(&self) -> Vec<usize> {
        self.parts.iter().map(HashTable::len).collect()
    }

    /// Every key held.
    pub(super) fn iter(&self) -> impl Iterator<Item = &Arc<Key>> {
        self.parts
            .iter()
            .flat_map(|part| part.iter().map(|held| &held.key))
    }
}
