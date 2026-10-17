//! What every design shares: a key's contract and its bucket's arithmetic (GCRA, as the
//! service's token bucket admits), the cycle that reports to the service, a clock, a cheap
//! hash for key ids, and a map split into shards each behind its own lock.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::RwLock;
use std::time::Instant;

/// The time between a pod's reports to the service, as EdgeRush's cycle.
pub(crate) const CYCLE: u64 = 10_000_000;
/// The time between the rooms design's syncs, as EdgeRush's.
pub(crate) const SYNC: u64 = 1_000_000;

/// A key's contract: a token every `interval` nanoseconds, up to a bucket of `burst`; and
/// what the pod may admit under it in a cycle, its share as the service gives it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Contract {
    pub(crate) interval: u64,
    pub(crate) burst: u64,
    pub(crate) share: u64,
}

impl Contract {
    /// `rate` a second with a bucket of `burst`, and a share of `share` a cycle.
    pub(crate) fn new(rate: u64, burst: u64, share: u64) -> Self {
        Self {
            interval: (1_000_000_000 / rate.max(1)).max(1),
            burst: burst.max(1),
            share,
        }
    }

    /// The same contract split among `ways` holders of their own, as each worker apart
    /// would be held to a part of it were nothing shared.
    pub(crate) fn split(self, ways: u64) -> Self {
        Self {
            interval: self.interval.saturating_mul(ways),
            burst: (self.burst / ways).max(1),
            share: (self.share / ways).max(1),
        }
    }

    fn tolerance(self) -> u64 {
        self.burst.saturating_mul(self.interval)
    }

    /// GCRA: the theoretical arrival time after one more request at `now`, if the bucket
    /// has room for it.
    pub(crate) fn admit(self, tat: u64, now: u64) -> Option<u64> {
        let next = tat.max(now) + self.interval;
        (next - now <= self.tolerance()).then_some(next)
    }

    /// How many requests the bucket has room for at `now`.
    pub(crate) fn room(self, tat: u64, now: u64) -> u64 {
        let ahead = tat.saturating_sub(now);
        self.tolerance().saturating_sub(ahead) / self.interval
    }

    /// The theoretical arrival time after `count` requests admitted elsewhere are charged.
    pub(crate) fn charge(self, tat: u64, now: u64, count: u64) -> u64 {
        tat.max(now) + count.saturating_mul(self.interval)
    }

    /// How long from `now` until the bucket has room for one, if it has none.
    pub(crate) fn wait(self, tat: u64, now: u64) -> u64 {
        (tat.max(now) + self.interval)
            .saturating_sub(now)
            .saturating_sub(self.tolerance())
    }
}

/// Nanoseconds since the run began.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Clock(Instant);

impl Clock {
    pub(crate) fn start() -> Self {
        Self(Instant::now())
    }

    pub(crate) fn now(self) -> u64 {
        u64::try_from(self.0.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Key ids hashed by one multiply: every design pays the same for it.
#[derive(Default)]
pub(crate) struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }

    fn write_u64(&mut self, id: u64) {
        self.0 = (id ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        self.0 ^= self.0 >> 31;
    }
}

pub(crate) type Ids = BuildHasherDefault<IdHasher>;
pub(crate) type IdMap<V> = HashMap<u64, V, Ids>;

/// A map split into shards, each behind a lock of its own, so that keys in different
/// shards are not read under the same lock word.
pub(crate) struct Sharded<V> {
    shards: Vec<RwLock<IdMap<V>>>,
}

/// Shards of a [`Sharded`] map: a few for each core of the machines this runs on.
const SHARDS: usize = 64;

impl<V: Clone> Sharded<V> {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| RwLock::new(IdMap::default())).collect(),
        }
    }

    fn shard(&self, key: u64) -> &RwLock<IdMap<V>> {
        // The top bits of the mix, which the map's own buckets do not use.
        let mut hasher = IdHasher::default();
        hasher.write_u64(key);
        let at = (hasher.finish() >> 58) as usize % SHARDS;
        &self.shards[at]
    }

    /// The value of `key`, read under its shard's read lock.
    pub(crate) fn get(&self, key: u64) -> Option<V> {
        let shard = self
            .shard(key)
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shard.get(&key).cloned()
    }

    /// Calls `then` with `key`'s value under its shard's read lock, without cloning it.
    pub(crate) fn with<R>(&self, key: u64, then: impl FnOnce(Option<&V>) -> R) -> R {
        let shard = self
            .shard(key)
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        then(shard.get(&key))
    }

    /// `key`'s value, made by `make` and put in first if there is none.
    pub(crate) fn get_or_insert(&self, key: u64, make: impl FnOnce() -> V) -> V {
        if let Some(value) = self.get(key) {
            return value;
        }
        let mut shard = self
            .shard(key)
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shard.entry(key).or_insert_with(make).clone()
    }

    /// Calls `each` with every value, a shard at a time under its read lock.
    pub(crate) fn each(&self, mut each: impl FnMut(u64, &V)) {
        for shard in &self.shards {
            let shard = shard
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for (key, value) in shard.iter() {
                each(*key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_admits_its_burst_and_then_its_rate() {
        let contract = Contract::new(1_000, 10, u64::MAX);
        let mut tat = 0;
        let mut admitted = 0;
        while let Some(next) = contract.admit(tat, 0) {
            tat = next;
            admitted += 1;
        }
        assert_eq!(admitted, 10);
        assert_eq!(contract.room(tat, 0), 0);
        assert_eq!(contract.wait(tat, 0), 1_000_000, "one token a millisecond");
        assert!(contract.admit(tat, 1_000_000).is_some());
        assert_eq!(contract.room(0, 0), 10);
        assert_eq!(contract.room(contract.charge(0, 0, 4), 0), 6);
    }
}
