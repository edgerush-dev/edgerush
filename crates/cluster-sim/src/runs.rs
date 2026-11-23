//! A workload run on the store's pods in the asynchronous mode, with a fault or none, and
//! what it did, measured against the contract: what the workload tests and the `workloads`
//! binary run.

use crate::Rng;
use crate::measure::{one_bucket_admits, worst_excess};
use crate::world::{Beside, Fleet, Link, World};
use edgerush_cluster::{Config, NodeId, Time, ms};
use edgerush_ratelimit::service::Contract;

/// A second, in nanoseconds.
const SECOND: Time = 1_000_000_000;

/// What a run did.
#[derive(Clone, Debug, Default)]
pub struct Run {
    /// Requests the workload brought.
    pub sent: u64,
    /// Requests the data planes decided: every one sent, unless a data plane lost some.
    pub decided: u64,
    /// Requests admitted under each of its keys, the run's first.
    pub admitted: Vec<u64>,
    /// Requests refused.
    pub refused: u64,
    /// Batches the data planes sent.
    pub batches: u64,
    /// Messages sent between the data planes and the pods, both ways.
    pub messages: u64,
    /// The most any key admitted over its own contract, in any span: under the run's key first.
    pub worst_excess: Vec<i64>,
    /// Hits applied to each key, by whichever pod applied them, over every life of the key and
    /// every owner: as many as were admitted, once the run is over, unless some never reached
    /// an owner, or one applied them twice.
    pub counted: Vec<u64>,
    /// Hits each key's owner holds counted once the run is over: as many as were admitted,
    /// unless an owner took over from a copy missing some — or the key, quiet for a refill
    /// time and full again, was forgotten with its count, as a key whose refill time is under
    /// the half second the run is measured after may be.
    pub held: Vec<u64>,
    /// Hits the data planes gave up reporting, their batches older than a refill time unseen
    /// applied: never counted, by design.
    pub forgiven: u64,
    /// Requests whose decision waited for a data plane's participant, and their waits in all.
    pub waited: (u64, Time),
    /// What one bucket holding the run's contract would admit of the same traffic.
    pub one_bucket: u64,
}

/// What can happen while a run goes; each time from when the traffic starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Nothing.
    None,
    /// The run's key's owner crashes at `at`.
    OwnerLost {
        /// When.
        at: Time,
    },
    /// The run's key's owner crashes at `at` and a pod is started in its place, as Kubernetes
    /// starts one.
    OwnerRestarted {
        /// When.
        at: Time,
    },
    /// Data plane `plane`'s participant is held for `length` at `at`.
    Pause {
        /// When.
        at: Time,
        /// For how long.
        length: Time,
        /// Which data plane, in order.
        plane: usize,
    },
    /// Data plane `plane`'s process restarts at `at`.
    Restart {
        /// When.
        at: Time,
        /// Which data plane, in order.
        plane: usize,
    },
    /// Every data plane's process restarts in turn, the first at `from`, then one every
    /// `every`.
    Rolling {
        /// When the first restarts.
        from: Time,
        /// The time between one and the next.
        every: Time,
    },
    /// The first `planes` data planes are cut off from every pod, both ways, at `at` for
    /// `length`.
    PlanesCutOff {
        /// When.
        at: Time,
        /// For how long.
        length: Time,
        /// How many data planes, the first in order.
        planes: usize,
    },
}

/// One step of a fault, at its time.
enum Step {
    Crash { restarted: bool },
    Pause { plane: usize, length: Time },
    Restart { plane: usize },
    Cut { planes: usize },
    Heal { planes: usize },
}

impl Fault {
    /// Its steps, in order of time, for `planes` data planes.
    fn steps(self, planes: usize) -> Vec<(Time, Step)> {
        match self {
            Self::None => Vec::new(),
            Self::OwnerLost { at } => vec![(at, Step::Crash { restarted: false })],
            Self::OwnerRestarted { at } => vec![(at, Step::Crash { restarted: true })],
            Self::Pause { at, length, plane } => vec![(at, Step::Pause { plane, length })],
            Self::Restart { at, plane } => vec![(at, Step::Restart { plane })],
            Self::Rolling { from, every } => (0..planes)
                .map(|plane| (from + every * plane as Time, Step::Restart { plane }))
                .collect(),
            Self::PlanesCutOff { at, length, planes } => vec![
                (at, Step::Cut { planes }),
                (at + length, Step::Heal { planes }),
            ],
        }
    }
}

/// `fleet` run on three pods with `fault`, the links between the data planes and the pods,
/// both ways, `planes`, and every other `pods`; measured half a second after the last arrival,
/// once every batch has been applied. `None` if the pods never formed a cluster.
#[must_use]
pub fn run_over(fleet: &Fleet, pods: Link, planes: Link, fault: Fault) -> Option<Run> {
    let mut config = Config::defaults();
    config.shards = 64;
    let mut world = World::new(fleet.seed, config);
    world.start_cluster(3);
    world.run_while_not(ms(10_000), World::settled)?;
    world.set_default_link(pods);
    let keys = world.add_fleet(fleet);
    let ids = world.plane_ids();
    let link_planes = |world: &mut World, of: &[NodeId], link: Link| {
        for &plane in of {
            for pod in world.pods() {
                world.set_link(plane, pod, link);
                world.set_link(pod, plane, link);
            }
        }
    };
    if planes != pods {
        link_planes(&mut world, &ids, planes);
    }
    let start = world.now();
    for (at, step) in fault.steps(ids.len()) {
        world.run_until(start + at);
        match step {
            Step::Crash { restarted } => {
                if let Some(owner) = world.owner_of(keys[0]) {
                    world.crash(owner);
                    if restarted {
                        world.add_pod();
                    }
                }
            }
            Step::Pause { plane, length } => {
                if let Some(&id) = ids.get(plane) {
                    world.pause_plane(id, length);
                }
            }
            Step::Restart { plane } => {
                if let Some(&id) = ids.get(plane) {
                    let _new = world.restart_plane(id);
                }
            }
            Step::Cut { planes: count } => {
                let cut = Link {
                    cut: true,
                    ..planes
                };
                link_planes(&mut world, &ids[..count.min(ids.len())], cut);
            }
            Step::Heal { planes: count } => {
                // Those restarted meanwhile are cut off no more than the rest.
                link_planes(&mut world, &ids[..count.min(ids.len())], planes);
            }
        }
    }
    let arrivals: Vec<u64> = fleet
        .workload
        .arrivals(
            &fleet.contract,
            fleet.planes * fleet.workers,
            &mut Rng::new(fleet.seed),
        )
        .iter()
        .map(|arrival| arrival.at)
        .collect();
    let last = arrivals.last().copied().unwrap_or(0);
    world.run_until(start + last + SECOND / 2);
    let admissions = world.admissions();
    let times = |key| admissions.get(&key).cloned().unwrap_or_default();
    let contracts = contracts(fleet, &keys);
    let (decided, refused) = world.decided();
    Some(Run {
        sent: arrivals.len() as u64,
        decided,
        admitted: keys.iter().map(|key| times(*key).len() as u64).collect(),
        refused,
        batches: world.batches(),
        messages: world.plane_messages(),
        worst_excess: keys
            .iter()
            .zip(&contracts)
            .map(|(key, contract)| worst_excess(contract, &times(*key)))
            .collect(),
        counted: keys.iter().map(|key| world.counted_ever(*key)).collect(),
        held: keys
            .iter()
            .map(|key| world.counted(*key).unwrap_or(0))
            .collect(),
        forgiven: world.forgiven(),
        waited: world.waited(),
        one_bucket: one_bucket_admits(&fleet.contract, &arrivals),
    })
}

/// As [`run_over`], every link `link`.
#[must_use]
pub fn run(fleet: &Fleet, link: Link, fault: Fault) -> Option<Run> {
    run_over(fleet, link, link, fault)
}

/// The contract of each of a run's keys, in the order `add_fleet` gave them.
#[must_use]
pub fn contracts(fleet: &Fleet, keys: &[u64]) -> Vec<Contract> {
    keys.iter()
        .enumerate()
        .map(|(n, _)| match (n, fleet.beside) {
            (0, _) | (_, None) => fleet.contract,
            (_, Some(Beside::Global(other) | Beside::Routes(other))) => other,
        })
        .collect()
}
