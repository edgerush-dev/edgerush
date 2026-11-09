//! The keys a participant keeps, each by its hash ([`hash`]): found by the key's bytes, as a
//! worker's request and today's pods name it, or by the hash alone, as the cluster store's
//! answers name it. No more is kept a key for the second way: two keys of one hash are one
//! there, as they are on the store.

use super::Tracked;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use std::hash::{BuildHasher, Hasher};

/// The hash of the key of `bytes` under `hasher`: the bytes alone written, with no framing,
/// so that every data plane under the same hasher's key agrees on it, as the store needs.
pub(super) fn hash<S: BuildHasher>(hasher: &S, bytes: &[u8]) -> u64 {
    let mut state = hasher.build_hasher();
    state.write(bytes);
    state.finish()
}

/// The keys, by hash.
pub(super) struct Keys<S, P> {
    hasher: S,
    table: HashTable<Tracked<P>>,
}

impl<S: BuildHasher, P> Keys<S, P> {
    /// None yet, hashed by `hasher`.
    pub(super) fn new(hasher: S) -> Self {
        Self {
            hasher,
            table: HashTable::new(),
        }
    }

    /// The hash of the key of `bytes`.
    pub(super) fn hash(&self, bytes: &[u8]) -> u64 {
        hash(&self.hasher, bytes)
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<&Tracked<P>> {
        self.table
            .find(self.hash(key), |tracked| *tracked.entry.key == *key)
    }

    pub(super) fn get_mut(&mut self, key: &[u8]) -> Option<&mut Tracked<P>> {
        let hash = self.hash(key);
        self.table
            .find_mut(hash, |tracked| *tracked.entry.key == *key)
    }

    /// The key of `hash`, by the hash alone.
    pub(super) fn by_hash_mut(&mut self, hash: u64) -> Option<&mut Tracked<P>> {
        let hasher = &self.hasher;
        self.table.find_mut(hash, |tracked| {
            self::hash(hasher, &tracked.entry.key) == hash
        })
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> Option<Tracked<P>> {
        let hash = self.hash(key);
        let found = self
            .table
            .find_entry(hash, |tracked| *tracked.entry.key == *key)
            .ok()?;
        Some(found.remove().0)
    }

    /// Keeps `tracked`, in place of what it held for the key if anything.
    pub(super) fn insert(&mut self, tracked: Tracked<P>) {
        let hash = self.hash(&tracked.entry.key);
        let hasher = &self.hasher;
        match self.table.entry(
            hash,
            |held| held.entry.key == tracked.entry.key,
            |held| self::hash(hasher, &held.entry.key),
        ) {
            Entry::Occupied(mut held) => *held.get_mut() = tracked,
            Entry::Vacant(vacant) => {
                vacant.insert(tracked);
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Tracked<P>> {
        self.table.iter()
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Tracked<P>> {
        self.table.iter_mut()
    }

    /// Each key with its hash.
    pub(super) fn hashed_mut(&mut self) -> impl Iterator<Item = (u64, &mut Tracked<P>)> {
        let hasher = &self.hasher;
        self.table
            .iter_mut()
            .map(move |tracked| (hash(hasher, &tracked.entry.key), tracked))
    }

    pub(super) fn retain(&mut self, keep: impl FnMut(&mut Tracked<P>) -> bool) {
        self.table.retain(keep);
    }
}
