//! The EdgeRush rate-limit simulator.
//!
//! It runs ways of limiting one key across several pods against the contract they are meant
//! to keep, deterministically: the same run gives the same decisions on every machine. No
//! sockets, clocks or threads — time is a number, and randomness comes from a seed. It is
//! where the ways of sharing counts between pods (docs 06 §4, 10 §2) are compared, before
//! any networking exists.
//!
//! A [`Run`] is a contract, a number of pods, a [`Workload`], an [`Approach`] and a seed;
//! [`run`] plays it and measures the [`Outcome`] against the contract itself, not against
//! another limiter's decisions: how many requests were refused, and how far admissions went
//! over what the contract allows in any span of time.

mod approach;
mod measure;
mod network;
mod ownership;
mod rng;
mod workload;

pub use approach::{Approach, Cluster, Excess};
pub use measure::{Outcome, worst_excess};
pub use network::{Message, Network, Outbox};
pub use rng::Rng;
pub use workload::{Arrival, Shape, Workload};

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroU64;

/// The contract of a token bucket: `limit` requests per `window` nanoseconds on average,
/// at most `burst` at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contract {
    /// Requests a window, on average.
    pub limit: NonZeroU64,
    /// The window, in nanoseconds.
    pub window: NonZeroU64,
    /// The whole bucket: how many may come at once, from idle.
    pub burst: NonZeroU64,
}

/// One simulation: everything that decides its outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run {
    /// What the pods together are meant to keep.
    pub contract: Contract,
    /// How many pods share the key.
    pub pods: usize,
    /// The traffic.
    pub workload: Workload,
    /// How the pods limit it.
    pub approach: Approach,
    /// How messages between them travel.
    pub network: Network,
    /// The seed every random choice comes from.
    pub seed: u64,
}

/// Every request of a run with the decision it got, in the order they arrived: what a
/// failed check is replayed from.
///
/// The run is a sequence of events in time — requests arriving, each pod's timer, messages
/// delivered — taken in order of time and, at the same time, in the order they were made,
/// so that it is the same on every machine. The network draws from a stream of its own, so
/// that changing it leaves the traffic as it was.
#[must_use]
pub fn decisions(run: &Run) -> Vec<(Arrival, bool)> {
    let pods = run.pods.max(1);
    let mut rng = Rng::new(run.seed);
    let arrivals = run.workload.arrivals(&run.contract, pods, &mut rng);
    let mut network_rng = Rng::new(run.seed ^ 0x6E65_7477_6F72_6B00);
    let mut cluster = run.approach.build(&run.contract, pods);
    let mut queue = Queue::default();
    for (index, arrival) in arrivals.iter().enumerate() {
        queue.push(arrival.at, Event::Arrival(index));
    }
    let end = arrivals.last().map_or(0, |arrival| arrival.at);
    let cycle = cluster.cycle();
    if let Some(cycle) = cycle {
        for pod in 0..pods {
            // Timers spread over the cycle, as pods started at different moments are.
            queue.push(cycle * pod as u64 / pods as u64, Event::Timer(pod));
        }
    }
    let mut decided = vec![false; arrivals.len()];
    let mut outbox = Outbox::default();
    while let Some((now, event)) = queue.pop() {
        match event {
            Event::Arrival(index) => {
                decided[index] = cluster.decide(arrivals[index].pod, now, &mut outbox);
            }
            Event::Timer(pod) => {
                cluster.on_timer(pod, now, &mut outbox);
                if let Some(cycle) = cycle
                    && now <= end
                {
                    queue.push(now + cycle, Event::Timer(pod));
                }
            }
            Event::Deliver(to, message) => cluster.on_message(to, message, now, &mut outbox),
        }
        for (to, message) in outbox.sent.drain(..) {
            if let Some(at) = run.network.carry(now, &mut network_rng) {
                queue.push(at, Event::Deliver(to, message));
            }
        }
    }
    arrivals.into_iter().zip(decided).collect()
}

/// Something that happens at a moment of a run.
enum Event {
    /// The request at this index arrives.
    Arrival(usize),
    /// This pod's timer fires.
    Timer(usize),
    /// A message reaches this pod.
    Deliver(usize, Message),
}

/// Events by time, and at the same time by the order they were made.
#[derive(Default)]
struct Queue {
    order: BinaryHeap<Reverse<(u64, u64)>>,
    events: Vec<Option<Event>>,
}

impl Queue {
    fn push(&mut self, at: u64, event: Event) {
        self.order.push(Reverse((at, self.events.len() as u64)));
        self.events.push(Some(event));
    }

    fn pop(&mut self) -> Option<(u64, Event)> {
        let Reverse((at, made)) = self.order.pop()?;
        let event = self.events.get_mut(usize::try_from(made).ok()?)?.take()?;
        Some((at, event))
    }
}

/// Plays a run and measures it against its contract.
#[must_use]
pub fn run(run: &Run) -> Outcome {
    Outcome::of(&run.contract, run.pods, &decisions(run))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_come_by_time_and_at_the_same_time_in_the_order_made() {
        let mut queue = Queue::default();
        queue.push(5, Event::Timer(1));
        queue.push(3, Event::Timer(2));
        queue.push(5, Event::Timer(3));
        queue.push(3, Event::Arrival(4));
        let order: Vec<(u64, usize)> = std::iter::from_fn(|| queue.pop())
            .map(|(at, event)| match event {
                Event::Timer(n) | Event::Arrival(n) => (at, n),
                Event::Deliver(n, _) => (at, n),
            })
            .collect();
        assert_eq!(order, vec![(3, 2), (3, 4), (5, 1), (5, 3)]);
    }
}
