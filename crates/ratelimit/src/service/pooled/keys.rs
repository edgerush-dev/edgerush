//! The keys a participant keeps, each by its hash ([`hash`]): found by the key's bytes, as a
//! worker's request and today's pods name it, or by the hash alone, as the cluster store's
//! answers name it. No more is kept a key for the second way: two keys of one hash are one
//! there, as they are on the store.
//!
//! In [`SHARDS`] tables, each a few of the keys, rather than one: a table grows by doubling,
//! the old and the new both held while it moves its keys, so one table of every key steps up
//! by half again its size at once, past a cap the moment before within it (#602). Shards grow
//! one at a time, each by a little.

use super::Tracked;
use edgerush_memory::allocation;
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

/// The tables of [`Keys`]: as many as a registry has shards, so that each holds few keys.
pub(super) const SHARDS: usize = 4_096;

/// The bits of a hash below a shard's: a table places a key by the hash's low bits and tags
/// it with its top seven.
const SHARD_SHIFT: u32 = 32;

/// The keys, by hash.
pub(super) struct Keys<S, P, K> {
    hasher: S,
    shards: Box<[HashTable<Tracked<P, K>>]>,
    len: usize,
    /// What the shards' tables take, as glibc allocates them: kept as they grow, which they
    /// do only as keys come in, never shrinking as keys go.
    bytes: u64,
}

impl<S: BuildHasher, P, K> Keys<S, P, K> {
    /// None yet, hashed by `hasher`.
    pub(super) fn new(hasher: S) -> Self {
        Self {
            hasher,
            shards: (0..SHARDS).map(|_| HashTable::new()).collect(),
            len: 0,
            bytes: 0,
        }
    }

    /// The hash of the key of `bytes`.
    pub(super) fn hash(&self, bytes: &[u8]) -> u64 {
        hash(&self.hasher, bytes)
    }

    /// The shard of a key of `hash`.
    fn shard(hash: u64) -> usize {
        // Truncated to the bits a shard takes, which fit any `usize`.
        (hash >> SHARD_SHIFT) as usize % SHARDS
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<&Tracked<P, K>> {
        let hash = self.hash(key);
        self.shards[Self::shard(hash)].find(hash, |tracked| *tracked.entry.key == *key)
    }

    pub(super) fn get_mut(&mut self, key: &[u8]) -> Option<&mut Tracked<P, K>> {
        let hash = self.hash(key);
        self.shards[Self::shard(hash)].find_mut(hash, |tracked| *tracked.entry.key == *key)
    }

    /// The key of `hash`, by the hash alone.
    pub(super) fn by_hash_mut(&mut self, hash: u64) -> Option<&mut Tracked<P, K>> {
        let hasher = &self.hasher;
        self.shards[Self::shard(hash)].find_mut(hash, |tracked| {
            self::hash(hasher, &tracked.entry.key) == hash
        })
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> Option<Tracked<P, K>> {
        let hash = self.hash(key);
        let found = self.shards[Self::shard(hash)]
            .find_entry(hash, |tracked| *tracked.entry.key == *key)
            .ok()?;
        self.len -= 1;
        Some(found.remove().0)
    }

    /// Keeps `tracked`, in place of what it held for the key if anything, counting what its
    /// shard's table grows by.
    pub(super) fn insert(&mut self, tracked: Tracked<P, K>) {
        let hash = self.hash(&tracked.entry.key);
        let hasher = &self.hasher;
        let shard = &mut self.shards[Self::shard(hash)];
        let before = allocation(shard.allocation_size());
        match shard.entry(
            hash,
            |held| held.entry.key == tracked.entry.key,
            |held| self::hash(hasher, &held.entry.key),
        ) {
            Entry::Occupied(mut held) => *held.get_mut() = tracked,
            Entry::Vacant(vacant) => {
                vacant.insert(tracked);
                self.len += 1;
            }
        }
        // A table only grows: never less than it was.
        self.bytes += allocation(shard.allocation_size()) - before;
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// What its shards' tables take, as glibc allocates them.
    pub(super) const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Its shards, for the tests.
    #[cfg(test)]
    pub(super) fn shards(&self) -> &[HashTable<Tracked<P, K>>] {
        &self.shards
    }

    // The walks below pass over empty shards without reading their tables: most are empty
    // with few keys, and a walk of an empty table costs more than its count's look.

    /// The keys of one shard in `stride`.
    pub(super) fn sampled(&self, stride: usize) -> impl Iterator<Item = &Tracked<P, K>> {
        self.shards
            .iter()
            .step_by(stride)
            .filter(|shard| !shard.is_empty())
            .flat_map(HashTable::iter)
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Tracked<P, K>> {
        self.shards
            .iter_mut()
            .filter(|shard| !shard.is_empty())
            .flat_map(HashTable::iter_mut)
    }

    /// Each key with its hash.
    pub(super) fn hashed_mut(&mut self) -> impl Iterator<Item = (u64, &mut Tracked<P, K>)> {
        let hasher = &self.hasher;
        self.shards
            .iter_mut()
            .filter(|shard| !shard.is_empty())
            .flat_map(HashTable::iter_mut)
            .map(move |tracked| (hash(hasher, &tracked.entry.key), tracked))
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&mut Tracked<P, K>) -> bool) {
        for shard in &mut self.shards {
            if !shard.is_empty() {
                let before = shard.len();
                shard.retain(&mut keep);
                self.len -= before - shard.len();
            }
        }
    }
}
