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
//!
//! And connections are counted by listener, so that one listener cannot take every
//! connection there is room for: once seven eighths of all the workers' room is held, a
//! listener that holds its fair share stops accepting while the others carry on
//! ([03 §9](../../../docs/03-data-plane.md), [`crate::share`]). Balancing keeps the
//! workers level, so a share of all of them is a share of each.
//!
//! A WebSocket carried for an HTTP/2 or HTTP/3 client counts as one of its worker's
//! connections too, for as long as it is open: its backend is a connection of its own
//! beside the client's one, and a client connection may carry a hundred of them. It is
//! refused while its worker has no room, or its listener holds its share ([`Loads::take`]).

use crate::share::over_share;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

/// The worker that a connection accepted by `own` goes to: the one with the least load.
/// `own` keeps it if none has less; among others that tie, the next one after `own` gets
/// it, so that they are given to in turn and not the first of them every time.
///
/// `own` must be a position in `loads`; one that is not keeps the connection.
fn least_loaded(loads: &[usize], own: usize) -> usize {
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

/// How many connections every worker holds, and how many it may, and how many every
/// listener holds. Written to when a connection comes or goes, never for a request, so the
/// workers' numbers can share a line of cache.
#[derive(Debug)]
pub struct Loads {
    held: Vec<AtomicUsize>,
    /// How many connections a worker may hold before it stops accepting.
    cap: usize,
    /// For every worker and listener, at `worker * listeners + listener`, what wakes the
    /// worker's accepting on the listener when the worker has room again: one for each
    /// listener, since each accepts on its own, and the one woken may have nothing to
    /// accept while another has a backlog.
    room: Vec<Notify>,
    /// Connections held by every worker together.
    total: AtomicUsize,
    /// Connections held, by listener, whichever worker holds them.
    by_listener: Vec<AtomicUsize>,
    /// How many listeners hold at least one connection.
    holding: AtomicUsize,
    /// For every worker and listener, at `worker * listeners + listener`, what wakes the
    /// worker's accepting on the listener when the listener has room again.
    listener_room: Vec<Notify>,
}

impl Loads {
    /// The counts of `workers` workers that may each hold `cap` connections, on
    /// `listeners` listeners, none held.
    #[must_use]
    pub fn new(workers: usize, cap: usize, listeners: usize) -> Arc<Self> {
        Arc::new(Self {
            held: (0..workers).map(|_| AtomicUsize::new(0)).collect(),
            cap,
            room: (0..workers * listeners).map(|_| Notify::new()).collect(),
            total: AtomicUsize::new(0),
            by_listener: (0..listeners).map(|_| AtomicUsize::new(0)).collect(),
            holding: AtomicUsize::new(0),
            listener_room: (0..workers * listeners).map(|_| Notify::new()).collect(),
        })
    }

    /// What all the workers together may hold.
    fn limit(&self) -> usize {
        self.cap.saturating_mul(self.held.len())
    }

    /// Whether `listener` may take another connection: always, but where every worker's
    /// room is seven eighths held and the listener holds its fair share of it. One that is
    /// not there has none.
    ///
    /// Workers that look at the same moment may each take one: a share is passed by at
    /// most that many.
    #[must_use]
    pub fn listener_has_room(&self, listener: usize) -> bool {
        let Some(own) = self.by_listener.get(listener) else {
            return true;
        };
        !over_share(
            self.limit(),
            self.total.load(Ordering::Relaxed),
            own.load(Ordering::Relaxed),
            self.holding.load(Ordering::Relaxed),
            self.by_listener.len() < 2,
        )
    }

    /// Whether `worker` holds fewer connections than its cap. One that is not there has
    /// none.
    ///
    /// A worker that accepts only while this holds stays within its cap, and so does every
    /// worker it gives connections to, which hold no more than it does — except that
    /// workers that look at the same moment may each place one on the same worker, and a
    /// worker's listeners that are woken together may each accept one: the cap is passed
    /// by at most that many.
    #[must_use]
    pub fn has_room(&self, worker: usize) -> bool {
        self.held
            .get(worker)
            .is_none_or(|load| load.load(Ordering::Relaxed) < self.cap)
    }

    /// Waits until `worker` has room, and `listener` has room for another connection.
    pub async fn room(&self, worker: usize, listener: usize) {
        let at = worker * self.by_listener.len() + listener;
        let (Some(room), Some(listener_room)) = (self.room.get(at), self.listener_room.get(at))
        else {
            return;
        };
        loop {
            // Asked for before looking, so that a connection that ends between the look and
            // the wait still wakes it.
            let woken = room.notified();
            let listener_woken = listener_room.notified();
            if !self.has_room(worker) {
                woken.await;
            } else if !self.listener_has_room(listener) {
                listener_woken.await;
            } else {
                return;
            }
        }
    }

    /// Finds the worker for a connection that `own` has accepted on `listener`, and counts
    /// it as that worker's from now on — while it is still on its way, too, so that the
    /// next connection does not follow it to the same place.
    ///
    /// Two workers that look at the same moment may pick the same third. They are then one
    /// connection off, which the accepts after them put right.
    #[must_use]
    pub fn place(self: &Arc<Self>, own: usize, listener: usize) -> Held {
        let loads: Vec<usize> = self.now();
        self.hold(least_loaded(&loads, own), listener)
    }

    /// Counts a connection on `listener` as `worker`'s, if the worker has room for one and
    /// the listener is not at its share: for what holds as much as a connection the worker
    /// accepted, without being one — a WebSocket carried for an HTTP/2 or HTTP/3 client.
    /// None when there is no room, as there would be no accepting.
    #[must_use]
    pub fn take(self: &Arc<Self>, worker: usize, listener: usize) -> Option<Held> {
        (self.has_room(worker) && self.listener_has_room(listener))
            .then(|| self.hold(worker, listener))
    }

    /// Counts a connection on `listener` as `worker`'s, without looking at the others.
    #[must_use]
    pub fn hold(self: &Arc<Self>, worker: usize, listener: usize) -> Held {
        if let Some(load) = self.held.get(worker) {
            load.fetch_add(1, Ordering::Relaxed);
        }
        self.total.fetch_add(1, Ordering::Relaxed);
        if let Some(own) = self.by_listener.get(listener)
            && own.fetch_add(1, Ordering::Relaxed) == 0
        {
            self.holding.fetch_add(1, Ordering::Relaxed);
        }
        Held {
            loads: Arc::clone(self),
            worker,
            listener,
        }
    }

    /// What every worker holds at this moment.
    #[must_use]
    pub fn now(&self) -> Vec<usize> {
        self.held
            .iter()
            .map(|load| load.load(Ordering::Relaxed))
            .collect()
    }
}

/// A connection that counts as a worker's, until this is dropped: it goes where the
/// connection goes, and is let go of wherever the connection ends — or is lost.
#[derive(Debug)]
pub struct Held {
    loads: Arc<Loads>,
    worker: usize,
    listener: usize,
}

impl Held {
    /// The worker it counts as.
    #[must_use]
    pub fn worker(&self) -> usize {
        self.worker
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let loads = &self.loads;
        if let Some(own) = loads.by_listener.get(self.listener)
            && own.fetch_sub(1, Ordering::Relaxed) == 1
        {
            loads.holding.fetch_sub(1, Ordering::Relaxed);
        }
        let total = loads.total.fetch_sub(1, Ordering::Relaxed);
        // Only past seven eighths can a listener be waiting for its share, so only then is
        // there anyone to wake; and any connection that ends may be what gives it room.
        let limit = loads.limit();
        if total >= limit - limit / 8 {
            for listener_room in &loads.listener_room {
                listener_room.notify_one();
            }
        }
        let Some(load) = loads.held.get(self.worker) else {
            return;
        };
        let before = load.fetch_sub(1, Ordering::Relaxed);
        // Only a worker that was full can be waiting, so only then is there anyone to wake:
        // every one of its listeners, as each may be waiting.
        let listeners = loads.by_listener.len();
        if before >= loads.cap
            && let Some(rooms) = loads
                .room
                .get(self.worker * listeners..(self.worker + 1) * listeners)
        {
            for room in rooms {
                room.notify_one();
            }
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
        let loads = Loads::new(4, usize::MAX, 1);
        let held: Vec<Held> = (0..4).map(|_| loads.place(2, 0)).collect();
        assert_eq!(loads.now(), [1, 1, 1, 1]);
        let mut workers: Vec<usize> = held.iter().map(Held::worker).collect();
        assert_eq!(workers.first(), Some(&2), "the first one stays");
        workers.sort_unstable();
        assert_eq!(workers, [0, 1, 2, 3]);
    }

    #[test]
    fn a_connection_counts_until_it_is_let_go_of() {
        let loads = Loads::new(2, usize::MAX, 1);
        let first = loads.hold(1, 0);
        let second = loads.hold(1, 0);
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
            let loads = Loads::new(workers, usize::MAX, 1);
            let mut held = Vec::new();
            for own in accepted_by {
                held.push(loads.place(own % workers, 0));
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
            let loads = Loads::new(workers, usize::MAX, 1);
            let mut held: Vec<Held> = Vec::new();
            for (ends, which) in events {
                if ends && !held.is_empty() {
                    held.swap_remove(which % held.len());
                } else {
                    let before = loads.now();
                    let placed = loads.place(which % workers, 0);
                    prop_assert_eq!(before[placed.worker()], *before.iter().min().unwrap());
                    held.push(placed);
                }
                prop_assert_eq!(loads.now().iter().sum::<usize>(), held.len());
            }
        }

        /// Whatever connections come and go on whatever listeners and workers, what each
        /// listener holds is what is there, and whether it has room is the shared rule
        /// over those counts; a listener holding none always has room while any is left.
        #[test]
        fn a_listeners_room_is_the_fair_share_of_what_is_held(
            workers in 1_usize..5,
            cap in 1_usize..12,
            listeners in 1_usize..5,
            events in prop::collection::vec((any::<bool>(), 0_usize..64, 0_usize..8), 0..300),
        ) {
            let loads = Loads::new(workers, cap, listeners);
            let limit = workers * cap;
            let mut held: Vec<(usize, Held)> = Vec::new();
            for (ends, which, listener) in events {
                let listener = listener % listeners;
                if ends && !held.is_empty() {
                    held.swap_remove(which % held.len());
                } else if loads.has_room(which % workers) && loads.listener_has_room(listener) {
                    held.push((listener, loads.place(which % workers, listener)));
                }
                let counts: Vec<usize> = (0..listeners)
                    .map(|l| held.iter().filter(|(of, _)| *of == l).count())
                    .collect();
                let holding = counts.iter().filter(|count| **count > 0).count();
                for (l, count) in counts.iter().enumerate() {
                    let expected =
                        !over_share(limit, held.len(), *count, holding, listeners < 2);
                    prop_assert_eq!(loads.listener_has_room(l), expected, "listener {}", l);
                    if *count == 0 && held.len() < limit {
                        prop_assert!(loads.listener_has_room(l), "listener {} holds none", l);
                    }
                }
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
            let loads = Loads::new(workers, cap, 1);
            let mut held: Vec<Held> = Vec::new();
            for (ends, which) in events {
                if ends && !held.is_empty() {
                    held.swap_remove(which % held.len());
                } else if loads.has_room(which % workers) {
                    held.push(loads.place(which % workers, 0));
                }
                prop_assert!(loads.now().iter().all(|load| *load <= cap), "{:?}", loads.now());
            }
        }
    }

    /// Once seven eighths of every worker's room is held, a listener holding its share has
    /// no room and one holding less has; with a single listener there is no share to hold.
    #[test]
    fn once_short_a_listener_at_its_share_has_no_room_and_the_others_have() {
        let loads = Loads::new(2, 8, 2);
        // 16 in all, short from 14: one listener holds 14, over its half.
        let held: Vec<Held> = (0..14).map(|at| loads.hold(at % 2, 0)).collect();
        assert!(!loads.listener_has_room(0));
        assert!(loads.listener_has_room(1));
        drop(held);
        assert!(loads.listener_has_room(0));

        let alone = Loads::new(2, 8, 1);
        let _held: Vec<Held> = (0..15).map(|at| alone.hold(at % 2, 0)).collect();
        assert!(alone.listener_has_room(0));
    }

    /// A connection taken rather than accepted is counted only while the worker has room
    /// and the listener is not at its share, and counts like any other once taken.
    #[test]
    fn a_connection_is_taken_only_where_there_is_room() {
        let loads = Loads::new(1, 2, 2);
        let accepted = loads.hold(0, 0);
        let taken = loads.take(0, 1).expect("room for a second");
        assert_eq!(loads.now(), [2]);
        assert!(loads.take(0, 1).is_none(), "taken past the worker's cap");
        drop(accepted);
        assert!(loads.take(0, 0).is_some(), "room again once one ended");
        drop(taken);

        // 16 in all, short from 14: the first listener holds 14, over its half.
        let shared = Loads::new(2, 8, 2);
        let _held: Vec<Held> = (0..14).map(|at| shared.hold(at % 2, 0)).collect();
        assert!(
            shared.take(0, 0).is_none(),
            "taken past the listener's share"
        );
        assert!(shared.take(0, 1).is_some(), "the other listener refused");
    }

    /// A listener waiting at its share is woken when a connection ends and gives it room,
    /// and goes on waiting while it still holds its share.
    #[test]
    fn a_listener_at_its_share_waits_until_it_has_room() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(waits_until_it_has_room());
    }

    async fn waits_until_it_has_room() {
        let loads = Loads::new(1, 8, 2);
        // Short from 7; the first listener holds 7, its share 4.
        let mut first: Vec<Held> = (0..7).map(|_| loads.hold(0, 0)).collect();
        let waiting = tokio::spawn({
            let loads = Arc::clone(&loads);
            async move { loads.room(0, 0).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // The second listener takes the last place and lets it go: still short, and the
        // first still holds its share.
        drop(loads.hold(0, 1));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "woken while it holds its share");
        first.pop();
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("not woken when it had room again")
            .unwrap();
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
        let loads = Loads::new(2, 2, 1);
        let first = loads.hold(0, 0);
        let _second = loads.hold(0, 0);
        let other = loads.hold(1, 0);
        assert!(!loads.has_room(0));
        assert!(loads.has_room(1));

        let waiting = tokio::spawn({
            let loads = Arc::clone(&loads);
            async move { loads.room(0, 0).await }
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

    /// A worker at its cap that gets room again wakes every one of its listeners, not one
    /// of them: one that is woken may have nothing to accept, and the others would then
    /// stay parked while the worker has room.
    #[test]
    fn a_worker_back_under_its_cap_wakes_every_listener_waiting_on_it() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(wakes_every_listener_waiting_on_it());
    }

    async fn wakes_every_listener_waiting_on_it() {
        let loads = Loads::new(1, 2, 2);
        let first = loads.hold(0, 0);
        let _second = loads.hold(0, 1);
        assert!(!loads.has_room(0));

        let waiting: Vec<_> = (0..2)
            .map(|listener| {
                let loads = Arc::clone(&loads);
                tokio::spawn(async move { loads.room(0, listener).await })
            })
            .collect();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(waiting.iter().all(|wait| !wait.is_finished()));
        drop(first);
        assert!(loads.has_room(0));
        assert!(loads.listener_has_room(0) && loads.listener_has_room(1));
        for (listener, wait) in waiting.into_iter().enumerate() {
            tokio::time::timeout(std::time::Duration::from_secs(5), wait)
                .await
                .unwrap_or_else(|_| {
                    panic!("listener {listener} not woken though the worker has room")
                })
                .unwrap();
        }
    }
}
