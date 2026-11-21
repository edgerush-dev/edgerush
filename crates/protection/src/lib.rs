//! EdgeRush backend protection ([25](../../../docs/25-backend-protection.md)): a bound on
//! each backend pod's requests in flight, each data plane's own.
//!
//! A pure crate: it counts and does nothing else — no sockets, clocks or async runtime.
//! [`PodCount`] is one pod's requests in flight from this data plane, shared by its
//! workers, and [`Slot`] one request's place in it, given back when dropped. [`Queue`] is
//! the line of requests waiting for a slot in any pod of a full backend, called in turn as
//! slots are given back. [`Pods`] finds each pod's count, and each backend's queue, again
//! in the next config, so that a reload never starts a pod at zero beside the requests it
//! still has, nor a line afresh beside the requests waiting in it.
//!
//! The data plane's own count of an endpoint's exchanges, which its balancer compares, is
//! each worker's alone, so that no request writes where another core writes
//! ([03 §6](../../../docs/03-data-plane.md)). This one is shared, and costs that write —
//! two atomic writes a request, taking and giving back — only on an upstream that is
//! protected (25 §8).

mod queue;
mod sync;

pub use queue::{InLine, Queue};

#[cfg(not(loom))]
mod pods;
#[cfg(not(loom))]
pub use pods::Pods;

#[cfg(loom)]
mod loom_tests;

use std::num::NonZeroU32;
use sync::{Arc, AtomicU32, Ordering};

/// One backend pod's requests in flight from this data plane, every worker's together
/// (25 §3), and its backend's queue, which a slot given back calls the next of (25 §5).
#[derive(Debug)]
pub struct PodCount {
    in_flight: AtomicU32,
    queue: Arc<Queue>,
}

impl PodCount {
    /// A pod with nothing in flight, of a backend of its own.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Self::of(&Queue::new())
    }

    /// A pod with nothing in flight, of the backend whose queue is `queue`.
    #[must_use]
    pub fn of(queue: &Arc<Queue>) -> Arc<Self> {
        Arc::new(Self {
            in_flight: AtomicU32::new(0),
            queue: Arc::clone(queue),
        })
    }

    /// A slot in `count`, if its pod has fewer than `cap` in flight; none if it is full.
    ///
    /// Exact: workers racing for the last slot cannot both have it, so the count never
    /// passes its cap. A full pod is found by reading its count, with nothing written, so
    /// the requests that find it full do not write where the requests it serves do; one
    /// below its cap is taken by compare-and-swap, tried again with the count another
    /// worker left. Relaxed: the count orders nothing but itself, and every change to one
    /// atomic is seen in one order by every thread; a request in a backend's queue fences
    /// before it looks ([`Queue::join`]).
    #[must_use]
    pub fn take(count: &Arc<Self>, cap: NonZeroU32) -> Option<Slot> {
        let mut seen = count.in_flight.load(Ordering::Relaxed);
        loop {
            if seen >= cap.get() {
                return None;
            }
            match count.in_flight.compare_exchange_weak(
                seen,
                seen + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Slot(Arc::clone(count))),
                Err(now) => seen = now,
            }
        }
    }

    /// How many requests it has in flight now.
    #[must_use]
    pub fn in_flight(&self) -> u32 {
        self.in_flight.load(Ordering::Relaxed)
    }
}

/// One request's place in a pod's count, from the pick that took it until it is dropped:
/// with the exchange's end, or at once if the request never reached the pod (25 §6).
/// Given back, it calls the next request in its backend's queue, if any waits (25 §5).
#[derive(Debug)]
pub struct Slot(Arc<PodCount>);

impl Drop for Slot {
    fn drop(&mut self) {
        // Only a slot taken adds to the count, and each is given back once: never below
        // zero. `SeqCst`, with the read of whether anyone waits after it, so that a request
        // joining the queue as this is given back is either read as waiting or sees the
        // room (`Queue::release` says why). The same instruction as a relaxed one on x86.
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.0.queue.release();
    }
}

// Two attributes, not `all(test, not(loom))`: clippy takes only a plain `cfg(test)` for a
// test module.
#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    fn cap(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    #[test]
    fn a_pod_takes_up_to_its_cap_and_no_more() {
        let pod = PodCount::new();
        let slots: Vec<Slot> = (0..3)
            .map(|_| PodCount::take(&pod, cap(3)).unwrap())
            .collect();
        assert_eq!(pod.in_flight(), 3);
        assert!(PodCount::take(&pod, cap(3)).is_none());
        assert_eq!(pod.in_flight(), 3, "a full pod is not written to");
        drop(slots);
        assert_eq!(pod.in_flight(), 0);
    }

    #[test]
    fn a_slot_given_back_lets_the_next_request_in() {
        let pod = PodCount::new();
        let first = PodCount::take(&pod, cap(1)).unwrap();
        assert!(PodCount::take(&pod, cap(1)).is_none());
        drop(first);
        let second = PodCount::take(&pod, cap(1));
        assert!(second.is_some());
        assert_eq!(pod.in_flight(), 1);
    }

    #[test]
    fn a_lower_cap_admits_nothing_new_until_the_pod_drains_below_it() {
        let pod = PodCount::new();
        let held: Vec<Slot> = (0..5)
            .map(|_| PodCount::take(&pod, cap(5)).unwrap())
            .collect();
        // A reload lowers the cap to 2: nothing in flight is given back by it.
        assert!(PodCount::take(&pod, cap(2)).is_none());
        let mut held = held;
        held.truncate(2);
        assert!(PodCount::take(&pod, cap(2)).is_none(), "2 still in flight");
        held.truncate(1);
        assert!(PodCount::take(&pod, cap(2)).is_some());
    }

    /// Workers racing on one pod: however they interleave, never more than its cap in
    /// flight at once, and every slot comes back. Loom tries every interleaving of two and
    /// three (`loom_tests`); this runs many real threads at once.
    #[test]
    fn racing_workers_never_pass_the_cap() {
        const WORKERS: usize = 8;
        const ROUNDS: usize = 2_000;
        let pod = PodCount::new();
        let most = std::sync::atomic::AtomicU32::new(0);
        let start = Barrier::new(WORKERS);
        thread::scope(|scope| {
            for _ in 0..WORKERS {
                scope.spawn(|| {
                    start.wait();
                    for _ in 0..ROUNDS {
                        if let Some(slot) = PodCount::take(&pod, cap(3)) {
                            most.fetch_max(pod.in_flight(), Ordering::Relaxed);
                            drop(slot);
                        }
                    }
                });
            }
        });
        assert!(most.load(Ordering::Relaxed) <= 3);
        assert_eq!(pod.in_flight(), 0);
    }
}
