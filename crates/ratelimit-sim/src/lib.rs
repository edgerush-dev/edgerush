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
mod counters;
mod measure;
mod network;
mod nodes;
mod ownership;
mod rng;
mod service;
mod workload;

pub use approach::{
    Approach, Beside, Bound, Cluster, Counting, Excess, OnOwnerChange, Order, Replicate, Serving,
    Sharing,
};
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
    /// What goes wrong, and when.
    pub fault: Fault,
    /// The seed every random choice comes from.
    pub seed: u64,
}

/// Something that goes wrong during a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Nothing does.
    None,
    /// At `at` nanoseconds the pod that owns the key goes away for good: its traffic goes to
    /// the next pod, which owns the key from then on.
    OwnerLost {
        /// When.
        at: u64,
    },
    /// From `from` to `until` nanoseconds, the pods from `split` on cannot reach those
    /// before it: messages between the two sides are lost, sent or on their way.
    Partition {
        /// When it starts.
        from: u64,
        /// When it heals.
        until: u64,
        /// The first pod of the side cut off from the first pod.
        split: usize,
    },
    /// At `at` nanoseconds pod `pod` restarts: everything it held is gone, and it comes
    /// back as another instance of itself.
    Restart {
        /// When.
        at: u64,
        /// Which pod.
        pod: usize,
    },
    /// A rolling restart, as a rollout does: from `from` nanoseconds every pod taking
    /// traffic restarts in turn, the first first, `every` nanoseconds apart — the owner
    /// among them where pods own keys.
    Rolling {
        /// When the first restarts.
        from: u64,
        /// The time between one and the next.
        every: u64,
    },
    /// At `at` nanoseconds the owner of the key restarts, holding nothing: with ownership,
    /// the pod owning it; with counters, which have no owner, the first pod; with a
    /// rate-limiting service, the service pod owning it.
    OwnerRestart {
        /// When.
        at: u64,
    },
    /// From `at`, for `length` nanoseconds, the background work of the data plane taking
    /// pod `pod`'s traffic is held: its cycles do not run and its answers wait, and so do
    /// asks a pooled process's workers make of it; decisions its workers make themselves
    /// go on. As a process thread held up while its workers run.
    Pause {
        /// When it starts.
        at: u64,
        /// For how long.
        length: u64,
        /// A pod of the data plane held, as the run numbers them.
        pod: usize,
    },
}

impl Fault {
    /// Whether a message from pod `from` to pod `to` is cut off at `now`.
    #[must_use]
    pub fn cuts(&self, now: u64, from: usize, to: usize) -> bool {
        match *self {
            Self::Partition {
                from: start,
                until,
                split,
            } => (start..until).contains(&now) && (from < split) != (to < split),
            _ => false,
        }
    }
}

/// A moment of a run's fault, as the pods learn of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultEvent {
    /// The owner has gone.
    OwnerLost,
    /// The pods from `split` on can no longer reach the others.
    PartitionStarts {
        /// The first pod of the side cut off.
        split: usize,
    },
    /// They can again.
    PartitionHeals {
        /// The first pod of the side that was cut off.
        split: usize,
    },
    /// A pod restarts.
    Restart {
        /// Which pod.
        pod: usize,
    },
    /// The owner restarts.
    OwnerRestarts,
    /// A pod's background work is held until a time.
    Pause {
        /// The pod, as the run numbers them.
        pod: usize,
        /// Until when.
        until: u64,
    },
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
    play(run).decisions
}

/// Plays a run and measures it against its contract.
#[must_use]
pub fn run(run: &Run) -> Outcome {
    let played = play(run);
    Outcome {
        lost: played.lost,
        counted_twice: played.counted_twice,
        messages: played.messages,
        waited: played.waited,
        waited_for: played.waited_for,
        ..Outcome::of(&run.contract, run.pods, &played.decisions)
    }
}

/// How many cycles timers go on firing after the last request, so that what is still on
/// its way — batches sent again among them — can arrive.
const SETTLING_CYCLES: u64 = 10;

/// What playing a run gives.
struct Played {
    /// Every request with its decision.
    decisions: Vec<(Arrival, bool)>,
    /// Hits left unapplied.
    lost: u64,
    /// Hits counted twice.
    counted_twice: u64,
    /// Messages sent.
    messages: u64,
    /// Requests that waited for their decision.
    waited: u64,
    /// Nanoseconds they waited, all together.
    waited_for: u64,
}

/// Plays a run.
fn play(run: &Run) -> Played {
    let cluster = run.approach.build(&run.contract, run.pods.max(1));
    play_with(run, cluster)
}

/// [`play`], on pods given rather than made from the run's approach.
fn play_with(run: &Run, mut cluster: Box<dyn Cluster>) -> Played {
    let pods = run.pods.max(1);
    let mut rng = Rng::new(run.seed);
    let arrivals = run.workload.arrivals(&run.contract, pods, &mut rng);
    let mut network_rng = Rng::new(run.seed ^ 0x6E65_7477_6F72_6B00);
    // When the last message between two pods arrives, for a network that keeps them in
    // order.
    let mut links: std::collections::BTreeMap<(usize, usize), u64> =
        std::collections::BTreeMap::new();
    let mut queue = Queue::default();
    for (index, arrival) in arrivals.iter().enumerate() {
        queue.push(arrival.at, Event::Arrival(index));
    }
    let end = arrivals.last().map_or(0, |arrival| arrival.at);
    match run.fault {
        Fault::None => {}
        Fault::OwnerLost { at } => queue.push(at, Event::Fault(FaultEvent::OwnerLost)),
        Fault::Partition { from, until, split } => {
            queue.push(from, Event::Fault(FaultEvent::PartitionStarts { split }));
            queue.push(until, Event::Fault(FaultEvent::PartitionHeals { split }));
        }
        Fault::Restart { at, pod } => queue.push(at, Event::Fault(FaultEvent::Restart { pod })),
        Fault::OwnerRestart { at } => queue.push(at, Event::Fault(FaultEvent::OwnerRestarts)),
        Fault::Pause { at, length, pod } => queue.push(
            at,
            Event::Fault(FaultEvent::Pause {
                pod,
                until: at + length,
            }),
        ),
        Fault::Rolling { from, every } => {
            for pod in 0..pods {
                let at = from + every * pod as u64;
                queue.push(at, Event::Fault(FaultEvent::Restart { pod }));
            }
        }
    }
    let cycle = cluster.cycle();
    if let Some(cycle) = cycle {
        for pod in 0..pods {
            // Timers spread over the cycle, as pods started at different moments are.
            queue.push(cycle * pod as u64 / pods as u64, Event::Timer(pod));
        }
        let helpers = cluster.helpers();
        for helper in 0..helpers {
            queue.push(
                cycle * helper as u64 / helpers as u64,
                Event::Timer(pods + helper),
            );
        }
    }
    let mut decided = vec![false; arrivals.len()];
    let (mut messages, mut waited, mut waited_for) = (0, 0, 0);
    let mut outbox = Outbox::default();
    while let Some((now, event)) = queue.pop() {
        match event {
            Event::Arrival(index) => {
                match cluster.arrive(index, arrivals[index].pod, now, &mut outbox) {
                    Some(admitted) => decided[index] = admitted,
                    None => waited += 1,
                }
            }
            Event::Timer(pod) => {
                cluster.on_timer(pod, now, &mut outbox);
                if let Some(cycle) = cycle
                    && now <= end + SETTLING_CYCLES * cycle
                {
                    queue.push(now + cycle, Event::Timer(pod));
                }
            }
            Event::Deliver(to, message) => {
                if !run.fault.cuts(now, message.sender(), to) {
                    cluster.on_message(to, message, now, &mut outbox);
                }
            }
            Event::Fault(event) => cluster.on_fault(event, now, &mut outbox),
        }
        for (index, admitted) in cluster.take_decided() {
            decided[index] = admitted;
            waited_for += now - arrivals[index].at;
        }
        messages += outbox.sent.len() as u64;
        for (to, message) in outbox.sent.drain(..) {
            let link = links.entry((message.sender(), to)).or_insert(0);
            if let Some(at) = run.network.carry(now, &mut network_rng, link) {
                queue.push(at, Event::Deliver(to, message));
            }
        }
    }
    Played {
        lost: cluster.unapplied(),
        counted_twice: cluster.counted_twice(),
        decisions: arrivals.into_iter().zip(decided).collect(),
        messages,
        waited,
        waited_for,
    }
}

/// Something that happens at a moment of a run.
enum Event {
    /// The request at this index arrives.
    Arrival(usize),
    /// This pod's timer fires.
    Timer(usize),
    /// A message reaches this pod.
    Deliver(usize, Message),
    /// Part of the run's fault happens.
    Fault(FaultEvent),
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
                Event::Fault(_) => (at, usize::MAX),
            })
            .collect();
        assert_eq!(order, vec![(3, 2), (3, 4), (5, 1), (5, 3)]);
    }

    #[test]
    fn a_partition_cuts_only_across_its_sides_and_only_while_it_lasts() {
        let fault = Fault::Partition {
            from: 10,
            until: 20,
            split: 2,
        };
        assert!(fault.cuts(10, 0, 2) && fault.cuts(19, 3, 1));
        assert!(
            !fault.cuts(15, 0, 1) && !fault.cuts(15, 2, 3),
            "within a side"
        );
        assert!(
            !fault.cuts(9, 0, 2) && !fault.cuts(20, 0, 2),
            "before and after"
        );
        assert!(!Fault::None.cuts(15, 0, 2));
    }

    /// Notes when each pod is told to restart.
    struct Restarts(std::rc::Rc<std::cell::RefCell<Vec<(u64, usize)>>>);

    impl Cluster for Restarts {
        fn decide(&mut self, _: usize, _: u64, _: &mut Outbox) -> bool {
            true
        }
        fn on_fault(&mut self, event: FaultEvent, now: u64, _: &mut Outbox) {
            if let FaultEvent::Restart { pod } = event {
                self.0.borrow_mut().push((now, pod));
            }
        }
    }

    #[test]
    fn a_rolling_restart_restarts_every_pod_in_turn() {
        let second = 1_000_000_000;
        let n = |n| NonZeroU64::new(n).unwrap();
        let run = Run {
            contract: Contract {
                limit: n(10),
                window: n(second),
                burst: n(10),
            },
            pods: 4,
            workload: Workload {
                shape: Shape::Spread { per_window: 10 },
                span: 3 * second,
                conforming: false,
            },
            approach: Approach::Central,
            network: Network::reliable(100_000, 0),
            fault: Fault::Rolling {
                from: second,
                every: 100,
            },
            seed: 1,
        };
        let restarts = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let _ = play_with(&run, Box::new(Restarts(restarts.clone())));
        let restarts = restarts.borrow();
        let expected: Vec<(u64, usize)> =
            (0..4).map(|pod| (second + 100 * pod as u64, pod)).collect();
        assert_eq!(*restarts, expected);
    }

    /// Pod 1 sends pod 0 a message every millisecond, and pod 0 notes when each arrives.
    struct Chatter(std::rc::Rc<std::cell::RefCell<Vec<u64>>>);

    impl Cluster for Chatter {
        fn decide(&mut self, _: usize, _: u64, _: &mut Outbox) -> bool {
            true
        }
        fn cycle(&self) -> Option<u64> {
            Some(1_000_000)
        }
        fn on_timer(&mut self, pod: usize, _: u64, outbox: &mut Outbox) {
            if pod == 1 {
                let hits = Message::Hits {
                    from: 1,
                    instance: 0,
                    sequence: 0,
                    count: 0,
                    seen: 0,
                };
                outbox.send(0, hits);
            }
        }
        fn on_message(&mut self, _: usize, _: Message, now: u64, _: &mut Outbox) {
            self.0.borrow_mut().push(now);
        }
    }

    #[test]
    fn nothing_crosses_a_partition_while_it_lasts() {
        let second = 1_000_000_000;
        let n = |n| NonZeroU64::new(n).unwrap();
        let run = Run {
            contract: Contract {
                limit: n(10),
                window: n(second),
                burst: n(10),
            },
            pods: 2,
            workload: Workload {
                shape: Shape::Spread { per_window: 10 },
                span: 3 * second,
                conforming: false,
            },
            approach: Approach::Central,
            network: Network::reliable(100_000, 0),
            fault: Fault::Partition {
                from: second,
                until: 2 * second,
                split: 1,
            },
            seed: 1,
        };
        let heard = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let _ = play_with(&run, Box::new(Chatter(heard.clone())));
        let heard = heard.borrow();
        assert!(heard.iter().any(|at| *at < second) && heard.iter().any(|at| *at > 2 * second));
        assert!(heard.iter().all(|at| !(second..2 * second).contains(at)));
    }
}
