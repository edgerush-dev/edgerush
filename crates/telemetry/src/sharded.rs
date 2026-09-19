//! One group of counters per shard, and a shard per thread.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Threads are numbered as they first count something; a thread's shard is its number
/// modulo the number of shards. With at least as many shards as worker threads, which is
/// how a data plane sets it up, no two workers share a shard — and if they do, counting is
/// still right, only slower.
static THREADS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD: usize = THREADS.fetch_add(1, Ordering::Relaxed);
}

/// A group of counters, `T`, once per shard.
///
/// A request counts in [`local`](Self::local), the shard of the thread it is on, and what it
/// writes to is in nobody else's cache. Whoever wants the numbers adds the shards up
/// ([`sum`](Self::sum)). A group should hold everything that one request counts, so that
/// one request touches one shard.
#[derive(Debug)]
pub struct Sharded<T> {
    // Never empty, which a first shard of its own says without a proof at every use.
    first: Shard<T>,
    others: Box<[Shard<T>]>,
}

/// Aligned and padded so that no two shards share a line of cache, nor lines next to each
/// other, which processors that fetch the neighbouring line would make just as bad.
#[derive(Debug, Default)]
#[repr(align(128))]
struct Shard<T>(T);

impl<T: Default> Sharded<T> {
    /// A group with `shards` shards, all at zero.
    #[must_use]
    pub fn new(shards: NonZeroUsize) -> Self {
        Self {
            first: Shard::default(),
            others: (1..shards.get()).map(|_| Shard::default()).collect(),
        }
    }
}

impl<T> Sharded<T> {
    /// The shard of the thread that asks. Never waits and never allocates.
    #[must_use]
    pub fn local(&self) -> &T {
        let shard = THREAD.with(|thread| *thread) % (1 + self.others.len());
        match shard
            .checked_sub(1)
            .and_then(|other| self.others.get(other))
        {
            Some(shard) => &shard.0,
            None => &self.first.0,
        }
    }

    /// All the shards, for adding up.
    pub fn shards(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.first)
            .chain(&self.others)
            .map(|shard| &shard.0)
    }

    /// One number of the group over all the shards. It is not a snapshot: shards are read
    /// one after the other while threads count on, which is all a scrape needs.
    pub fn sum<N: std::iter::Sum<N>>(&self, number: impl Fn(&T) -> N) -> N {
        self.shards().map(number).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Counter, Gauge};
    use std::sync::Arc;

    #[derive(Debug, Default)]
    struct Group {
        requests: Counter,
        active: Gauge,
    }

    fn shards(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap()
    }

    #[test]
    fn a_group_has_as_many_shards_as_it_was_given() {
        assert_eq!(Sharded::<Group>::new(shards(1)).shards().count(), 1);
        assert_eq!(Sharded::<Group>::new(shards(8)).shards().count(), 8);
    }

    #[test]
    fn shards_do_not_share_lines_of_cache() {
        assert_eq!(std::mem::align_of::<Shard<Group>>(), 128);
        assert_eq!(std::mem::size_of::<Shard<Group>>() % 128, 0);
    }

    #[test]
    fn a_thread_always_counts_in_the_same_shard() {
        let group = Sharded::<Group>::new(shards(4));
        let first: *const Group = group.local();
        for _ in 0..10 {
            assert!(std::ptr::eq(first, group.local()));
        }
        // And in a shard of the same number in every group.
        let other = Sharded::<Group>::new(shards(4));
        other.local().requests.inc();
        group.local().requests.inc();
        let position =
            |group: &Sharded<Group>| group.shards().position(|shard| shard.requests.get() == 1);
        assert_eq!(position(&group), position(&other));
    }

    #[test]
    fn what_many_threads_count_adds_up_whatever_the_number_of_shards() {
        for count in [1, 3, 16] {
            let group = Arc::new(Sharded::<Group>::new(shards(count)));
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let group = Arc::clone(&group);
                    std::thread::spawn(move || {
                        for _ in 0..10_000 {
                            group.local().requests.inc();
                            group.local().active.inc();
                        }
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(group.sum(|shard| shard.requests.get()), 80_000, "{count}");
            assert_eq!(group.sum(|shard| shard.active.get()), 80_000, "{count}");
        }
    }

    #[test]
    fn what_goes_up_on_one_thread_may_come_down_on_another() {
        let group = Arc::new(Sharded::<Group>::new(shards(16)));
        group.local().active.inc();
        let elsewhere = Arc::clone(&group);
        std::thread::spawn(move || elsewhere.local().active.dec())
            .join()
            .unwrap();
        assert_eq!(group.sum(|shard| shard.active.get()), 0);
    }
}
