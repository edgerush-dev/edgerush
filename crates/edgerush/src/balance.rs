//! Whose connection a new connection is, when every worker has connections of its own.
//!
//! The kernel deals connections out by a hash of their addresses, which is even over
//! thousands and not over four: a handful of long-lived HTTP/2 connections often end up on
//! one worker while the others have nothing to do. So the worker that accepts a connection
//! looks at what every worker holds and gives the connection to the one that holds least,
//! as HAProxy does. That is one look and at most one hand-over for a connection, and
//! nothing for a request.
//!
//! What is counted is connections — open, or on their way to the worker — as everybody who
//! balances at accept counts them. A connection that turns out busier than the others
//! stays where it is.
//!
//! The same count bounds what a worker holds ([14 §8](../../../docs/14-downstream-server.md)).
//! A worker at its cap does not accept: what comes meanwhile waits in the kernel's backlog,
//! as with HAProxy, and costs the worker nothing until one of its connections ends —
//! lingering to its close included, since a connection counts until it is let go of.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

/// The worker that a connection accepted by `own` goes to: the one with the least load.
/// `own` keeps it if none has less; among others that tie, the next one after `own` gets
/// it, so that they are given to in turn and not the first of them every time.
///
/// `own` must be a position in `loads`; one that is not keeps the connection.
pub(crate) fn least_loaded(loads: &[usize], own: usize) -> usize {
    let Some(least) = loads.iter().copied().min() else {
        return own;
    };
    if loads.get(own).is_none_or(|load| *load == least) {
        return own;
    }
    let after_own = (own + 1..loads.len()).chain(0..own);
    after_own
        .into_iter()
        .find(|worker| loads.get(*worker) == Some(&least))
        .unwrap_or(own)
}

/// How many connections every worker holds, and how many it may. Written to when a
/// connection comes or goes, never for a request, so the workers' numbers can share a line
/// of cache.
#[derive(Debug)]
pub(crate) struct Loads {
    held: Vec<AtomicUsize>,
    /// How many connections a worker may hold before it stops accepting.
    cap: usize,
    /// For every worker, what wakes it when it has room again.
    room: Vec<Notify>,
}

impl Loads {
    pub(crate) fn new(workers: usize, cap: usize) -> Arc<Self> {
        Arc::new(Self {
            held: (0..workers).map(|_| AtomicUsize::new(0)).collect(),
            cap,
            room: (0..workers).map(|_| Notify::new()).collect(),
        })
    }

    /// Whether `worker` holds fewer connections than its cap. One that is not there has
    /// none.
    ///
    /// A worker that accepts only while this holds stays within its cap, and so does every
    /// worker it gives connections to, which hold no more than it does — except that
    /// workers that look at the same moment may each place one on the same worker, and a
    /// worker's listeners that are woken together may each accept one: the cap is passed
    /// by at most that many.
    pub(crate) fn has_room(&self, worker: usize) -> bool {
        self.held
            .get(worker)
            .is_none_or(|load| load.load(Ordering::Relaxed) < self.cap)
    }

    /// Waits until `worker` has room.
    pub(crate) async fn room(&self, worker: usize) {
        let Some(room) = self.room.get(worker) else {
            return;
        };
        loop {
            // Asked for before looking, so that a connection that ends between the look and
            // the wait still wakes it.
            let woken = room.notified();
            if self.has_room(worker) {
                return;
            }
            woken.await;
        }
    }

    /// Finds the worker for a connection that `own` has accepted, and counts it as that
    /// worker's from now on — while it is still on its way, too, so that the next
    /// connection does not follow it to the same place.
    ///
    /// Two workers that look at the same moment may pick the same third. They are then one
    /// connection off, which the accepts after them put right.
    pub(crate) fn place(self: &Arc<Self>, own: usize) -> Held {
        let loads: Vec<usize> = self.now();
        self.hold(least_loaded(&loads, own))
    }

    /// Counts a connection as `worker`'s, without looking at the others.
    pub(crate) fn hold(self: &Arc<Self>, worker: usize) -> Held {
        if let Some(load) = self.held.get(worker) {
            load.fetch_add(1, Ordering::Relaxed);
        }
        Held {
            loads: Arc::clone(self),
            worker,
        }
    }

    /// What every worker holds at this moment.
    pub(crate) fn now(&self) -> Vec<usize> {
        self.held
            .iter()
            .map(|load| load.load(Ordering::Relaxed))
            .collect()
    }
}

/// A connection that counts as a worker's, until this is dropped: it goes where the
/// connection goes, and is let go of wherever the connection ends — or is lost.
#[derive(Debug)]
pub(crate) struct Held {
    loads: Arc<Loads>,
    worker: usize,
}

impl Held {
    pub(crate) fn worker(&self) -> usize {
        self.worker
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let Some(load) = self.loads.held.get(self.worker) else {
            return;
        };
        let before = load.fetch_sub(1, Ordering::Relaxed);
        // Only a worker that was full can be waiting, so only then is there anyone to wake.
        if before >= self.loads.cap
            && let Some(room) = self.loads.room.get(self.worker)
        {
            room.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn the_worker_that_accepts_keeps_what_nobody_has_more_room_for() {
        assert_eq!(least_loaded(&[0, 0, 0, 0], 2), 2);
        assert_eq!(least_loaded(&[3, 3, 3], 0), 0);
        assert_eq!(least_loaded(&[5, 2, 7, 2], 3), 3);
        assert_eq!(least_loaded(&[9], 0), 0);
    }

    #[test]
    fn a_worker_that_has_more_than_another_gives_the_connection_away() {
        assert_eq!(least_loaded(&[4, 1, 3, 2], 0), 1);
        assert_eq!(least_loaded(&[0, 1, 1, 1], 3), 0);
    }

    #[test]
    fn workers_that_tie_are_given_to_in_turn() {
        // The accepting worker is the busy one; the rest hold nothing.
        assert_eq!(least_loaded(&[1, 0, 0, 0], 0), 1);
        assert_eq!(least_loaded(&[0, 1, 0, 0], 1), 2);
        assert_eq!(least_loaded(&[0, 0, 0, 1], 3), 0);
        assert_eq!(least_loaded(&[0, 0, 1, 0], 2), 3);
    }

    #[test]
    fn a_worker_that_is_not_there_keeps_what_it_accepts() {
        assert_eq!(least_loaded(&[], 0), 0);
        assert_eq!(least_loaded(&[1, 0], 7), 7);
    }

    #[test]
    fn four_connections_that_one_worker_accepts_go_to_four_workers() {
        let loads = Loads::new(4, usize::MAX);
        let held: Vec<Held> = (0..4).map(|_| loads.place(2)).collect();
        assert_eq!(loads.now(), [1, 1, 1, 1]);
        let mut workers: Vec<usize> = held.iter().map(Held::worker).collect();
        assert_eq!(workers.first(), Some(&2), "the first one stays");
        workers.sort_unstable();
        assert_eq!(workers, [0, 1, 2, 3]);
    }

    #[test]
    fn a_connection_counts_until_it_is_let_go_of() {
        let loads = Loads::new(2, usize::MAX);
        let first = loads.hold(1);
        let second = loads.hold(1);
        assert_eq!(loads.now(), [0, 2]);
        drop(first);
        assert_eq!(loads.now(), [0, 1]);
        // On its way to another thread or served to its end: it is the same thing.
        std::thread::spawn(move || drop(second)).join().unwrap();
        assert_eq!(loads.now(), [0, 0]);
    }

    proptest! {
        #[test]
        fn the_one_that_is_chosen_has_the_least_and_own_wins_a_tie(
            loads in prop::collection::vec(0_usize..6, 1..9),
            own in 0_usize..8,
        ) {
            let own = own % loads.len();
            let chosen = least_loaded(&loads, own);
            let least = *loads.iter().min().unwrap();
            prop_assert_eq!(loads[chosen], least);
            if loads[own] == least {
                prop_assert_eq!(chosen, own);
            }
        }

        /// However the kernel deals connections out among the workers, as long as none
        /// ends: no worker ever holds two more than another.
        #[test]
        fn connections_that_stay_are_spread_to_within_one(
            workers in 1_usize..9,
            accepted_by in prop::collection::vec(0_usize..8, 0..200),
        ) {
            let loads = Loads::new(workers, usize::MAX);
            let mut held = Vec::new();
            for own in accepted_by {
                held.push(loads.place(own % workers));
                let now = loads.now();
                let (least, most) = (now.iter().min().unwrap(), now.iter().max().unwrap());
                prop_assert!(most - least <= 1, "{now:?}");
            }
        }

        /// With connections that end as well: a new one never goes to a worker that holds
        /// more than another does, and the count is the connections that are there.
        #[test]
        fn with_connections_that_end_the_new_one_still_goes_to_the_least(
            workers in 1_usize..9,
            events in prop::collection::vec((any::<bool>(), 0_usize..64), 0..200),
        ) {
            let loads = Loads::new(workers, usize::MAX);
            let mut held: Vec<Held> = Vec::new();
            for (ends, which) in events {
                if ends && !held.is_empty() {
                    held.swap_remove(which % held.len());
                } else {
                    let before = loads.now();
                    let placed = loads.place(which % workers);
                    prop_assert_eq!(before[placed.worker()], *before.iter().min().unwrap());
                    held.push(placed);
                }
                prop_assert_eq!(loads.now().iter().sum::<usize>(), held.len());
            }
        }

        /// A worker that accepts only while it has room never has a connection placed on it
        /// past its cap, nor on any other: whatever balancing chooses holds no more than
        /// the one that accepted, which was under it.
        #[test]
        fn a_worker_that_accepts_only_with_room_keeps_every_worker_within_the_cap(
            workers in 1_usize..9,
            cap in 1_usize..6,
            events in prop::collection::vec((any::<bool>(), 0_usize..64), 0..300),
        ) {
            let loads = Loads::new(workers, cap);
            let mut held: Vec<Held> = Vec::new();
            for (ends, which) in events {
                if ends && !held.is_empty() {
                    held.swap_remove(which % held.len());
                } else if loads.has_room(which % workers) {
                    held.push(loads.place(which % workers));
                }
                prop_assert!(loads.now().iter().all(|load| *load <= cap), "{:?}", loads.now());
            }
        }
    }

    /// A worker at its cap has no room, and is woken when one of its connections ends; a
    /// connection of another worker's that ends does not wake it.
    #[test]
    fn a_worker_at_its_cap_waits_for_one_of_its_connections_to_end() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(waits_for_one_of_its_connections_to_end());
    }

    async fn waits_for_one_of_its_connections_to_end() {
        let loads = Loads::new(2, 2);
        let first = loads.hold(0);
        let _second = loads.hold(0);
        let other = loads.hold(1);
        assert!(!loads.has_room(0));
        assert!(loads.has_room(1));

        let waiting = tokio::spawn({
            let loads = Arc::clone(&loads);
            async move { loads.room(0).await }
        });
        drop(other);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "woken by another worker's connection"
        );
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("not woken when a connection of its own ended")
            .unwrap();
        assert!(loads.has_room(0));
    }
}
