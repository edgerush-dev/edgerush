//! A set of keys found by their names, each kept with its name's hash (GitHub issue #111).
//! A set grows by doubling, and the decision that makes it grow pays for moving every key:
//! a standard `HashSet` hashes each key anew, read from its own allocation, which with half
//! a million keys held a decision 108 ms. Moved by the hashes they carry, the keys are not
//! read at all.

use super::Key;
use super::sync::Arc;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use std::hash::BuildHasher;

/// A key with its name's hash.
#[derive(Debug)]
struct Held {
    hash: u64,
    key: Arc<Key>,
}

/// The bytes one key takes in a set: what `key_bytes` counts for each set holding it.
pub(super) const SLOT_BYTES: u64 = size_of::<Held>() as u64 + 1;

/// Keys found by their names, hashed by `S`.
#[derive(Debug)]
pub(super) struct KeySet<S> {
    table: HashTable<Held>,
    hasher: S,
}

impl<S: BuildHasher> KeySet<S> {
    pub(super) fn with_hasher(hasher: S) -> Self {
        Self {
            table: HashTable::new(),
            hasher,
        }
    }

    /// The hash `name` is found by.
    pub(super) fn hash(&self, name: &[u8]) -> u64 {
        self.hasher.hash_one(name)
    }

    /// The key of `name`, if held.
    pub(super) fn get(&self, name: &[u8]) -> Option<&Arc<Key>> {
        self.get_hashed(self.hash(name), name)
    }

    /// The key of `name`, whose hash is `hash`, if held.
    pub(super) fn get_hashed(&self, hash: u64, name: &[u8]) -> Option<&Arc<Key>> {
        self.table
            .find(hash, |held| *held.key.key == *name)
            .map(|held| &held.key)
    }

    /// Holds `key`, whose name's hash is `hash`, in place of any of its name.
    pub(super) fn replace_hashed(&mut self, hash: u64, key: Arc<Key>) {
        match self
            .table
            .entry(hash, |held| held.key.key == key.key, |held| held.hash)
        {
            Entry::Occupied(mut found) => found.get_mut().key = key,
            Entry::Vacant(room) => {
                room.insert(Held { hash, key });
            }
        }
    }

    /// Lets go of the key of `name`, if held.
    pub(super) fn remove(&mut self, name: &[u8]) {
        let hash = self.hash(name);
        if let Ok(found) = self.table.find_entry(hash, |held| *held.key.key == *name) {
            found.remove();
        }
    }

    /// Keeps only the keys `keep` takes.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&Arc<Key>) -> bool) {
        self.table.retain(|held| keep(&held.key));
    }

    /// How many it holds.
    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether it holds none.
    pub(super) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Every key held.
    pub(super) fn iter(&self) -> impl Iterator<Item = &Arc<Key>> {
        self.table.iter().map(|held| &held.key)
    }
}
