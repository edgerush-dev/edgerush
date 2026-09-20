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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// How many connections every worker holds. Written to when a connection comes or goes,
/// never for a request, so the workers' numbers can share a line of cache.
#[derive(Debug)]
pub(crate) struct Loads(Vec<AtomicUsize>);

impl Loads {
    pub(crate) fn new(workers: usize) -> Arc<Self> {
        Arc::new(Self((0..workers).map(|_| AtomicUsize::new(0)).collect()))
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
        if let Some(load) = self.0.get(worker) {
            load.fetch_add(1, Ordering::Relaxed);
        }
        Held {
            loads: Arc::clone(self),
            worker,
        }
    }

    /// What every worker holds at this moment.
    pub(crate) fn now(&self) -> Vec<usize> {
        self.0
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
        if let Some(load) = self.loads.0.get(self.worker) {
            load.fetch_sub(1, Ordering::Relaxed);
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
        let loads = Loads::new(4);
        let held: Vec<Held> = (0..4).map(|_| loads.place(2)).collect();
        assert_eq!(loads.now(), [1, 1, 1, 1]);
        let mut workers: Vec<usize> = held.iter().map(Held::worker).collect();
        assert_eq!(workers.first(), Some(&2), "the first one stays");
        workers.sort_unstable();
        assert_eq!(workers, [0, 1, 2, 3]);
    }

    #[test]
    fn a_connection_counts_until_it_is_let_go_of() {
        let loads = Loads::new(2);
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
            let loads = Loads::new(workers);
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
            let loads = Loads::new(workers);
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
    }
}
