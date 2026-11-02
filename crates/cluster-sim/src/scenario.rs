//! Real scenarios, scripted: what a cluster of pods goes through, and what it costs.

use crate::world::{Link, World};
use edgerush_cluster::{Config, NodeId, Time, ms};
use edgerush_cluster_synchronous::Rule;

/// What one scenario cost.
#[derive(Clone, Debug)]
pub struct Report {
    /// Its name.
    pub name: &'static str,
    /// Requests sent, counted and refused.
    pub sent: u64,
    /// Requests counted.
    pub counted: u64,
    /// Requests left to `when_unreachable` at their deadline: refused under `deny`, admitted
    /// under `allow`, decided locally under `local`.
    pub refused: u64,
    /// Requests counted more than once.
    pub counted_twice: u64,
    /// Half of counted requests took less than this, and 99% and 99.9%.
    pub p50: Time,
    /// See `p50`.
    pub p99: Time,
    /// See `p50`.
    pub p999: Time,
    /// Admissions the owners no longer count.
    pub lost: u64,
    /// Time, summed over shards, two pods served one.
    pub overlap_total: Time,
    /// The longest one shard had two.
    pub overlap_longest: Time,
    /// Time, summed over shards, nobody served one.
    pub unserved_total: Time,
    /// The longest one shard had nobody.
    pub unserved_longest: Time,
    /// Terms a leader was elected in.
    pub terms: u64,
    /// What must never happen.
    pub violations: Vec<String>,
    /// How long the scenario's own step took, if it has one: a failover, a rollout.
    pub took: Option<Time>,
    /// Whether it ended settled.
    pub settled: bool,
    /// Admissions owners count beyond what clients were told.
    pub overcount: u64,
    /// Parts counted for requests then refused.
    pub orphaned: u64,
    /// Bytes pods and clients sent.
    pub bytes: u64,
    /// Bytes sent by kind of message.
    pub bytes_by_kind: std::collections::BTreeMap<&'static str, u64>,
    /// Requests refused for a key's limit.
    pub limited: u64,
    /// Of those, the ones refused while their keys had room: refusals within the rate.
    pub shaved: u64,
    /// Requests refused by `when_unreachable` while their keys had room.
    pub refused_with_room: u64,
    /// Admissions beyond the limit.
    pub overshoot: u64,
    /// Shards a table handed, by plan, to a pod already gone.
    pub moved_to_gone: u64,
    /// The most one key's window was admitted beyond the limit by the store.
    pub overshoot_peak: u64,
}

/// The shape of a run.
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    /// Pods the cluster starts with.
    pub pods: usize,
    /// Shards.
    pub shards: u16,
    /// Clients, each under 32 keys.
    pub clients: u64,
    /// Requests a second each.
    pub rate: u64,
    /// A request's deadline.
    pub deadline: Time,
    /// Whether pods forward requests for shards not theirs.
    pub forwarding: bool,
    /// Keys preloaded in every shard once the cluster has formed: for moves that carry many.
    pub preload: u64,
    /// The limit clients hold every key to, and whether `when_unreachable` admits.
    pub limit: Option<(Rule, bool)>,
    /// The most a pod's keys may take; none, no cap.
    pub max_key_bytes: Option<u64>,
    /// Replicas each shard has besides its owner.
    pub replicas: usize,
}

impl Shape {
    /// Three pods, 256 shards, eight clients at 200 a second, refused after 20 ms.
    #[must_use]
    pub const fn defaults() -> Self {
        Self {
            pods: 3,
            shards: 256,
            clients: 8,
            rate: 200,
            deadline: ms(20),
            forwarding: false,
            preload: 0,
            limit: None,
            max_key_bytes: None,
            replicas: 2,
        }
    }
}

/// A cluster of `shape`, formed and settled, clients running, sampling on.
#[must_use]
pub fn cluster(seed: u64, shape: Shape) -> (World, Vec<NodeId>) {
    let mut config = Config::defaults();
    config.shards = shape.shards;
    config.max_key_bytes = shape.max_key_bytes;
    config.replicas = shape.replicas;
    let mut world = World::new(seed, config);
    world.set_forwarding(shape.forwarding);
    let pods = world.start_cluster(shape.pods);
    let _formed = world.run_while_not(ms(10_000), World::settled);
    world.preload(shape.preload);
    if let Some((limit, allow)) = shape.limit {
        world.set_limit(limit, allow);
    }
    world.add_clients(shape.clients, shape.rate, shape.deadline);
    world.start_sampling();
    world.run_for(ms(500));
    (world, pods)
}

/// `world`'s figures, as `name`.
#[must_use]
pub fn report(name: &'static str, world: &World, took: Option<Time>) -> Report {
    let stats = &world.stats;
    Report {
        name,
        sent: stats.sent,
        counted: stats.counted,
        refused: stats.refused,
        counted_twice: stats.counted_twice,
        p50: stats.latency(0.5),
        p99: stats.latency(0.99),
        p999: stats.latency(0.999),
        lost: world.lost(),
        overlap_total: stats.overlap_total,
        overlap_longest: stats.overlap_longest,
        unserved_total: stats.unserved_total,
        unserved_longest: stats.unserved_longest,
        terms: stats.terms,
        violations: stats.violations.clone(),
        took,
        settled: world.settled(),
        overcount: world.overcount(),
        orphaned: world.orphaned(),
        bytes: world.stats.bytes,
        bytes_by_kind: world.stats.bytes_by_kind.clone(),
        limited: stats.limited,
        shaved: world.shaved(),
        refused_with_room: world.refused_with_room(),
        overshoot: world.overshoot(),
        moved_to_gone: stats.moved_to_gone,
        overshoot_peak: world.overshoot_peak(),
    }
}

/// A live pod that does not lead, if there is one.
fn follower(world: &World) -> Option<NodeId> {
    let leader = world.leader();
    world.alive().find(|pod| Some(*pod) != leader)
}

/// Waits until every shard has a live, serving owner again; how long it took.
fn until_served(world: &mut World, limit: Time) -> Option<Time> {
    world.run_while_not(limit, World::settled)
}

/// Steady running, nothing happening.
#[must_use]
pub fn steady(seed: u64, shape: Shape) -> Report {
    let (mut world, _) = cluster(seed, shape);
    world.run_for(ms(5_000));
    report("steady", &world, None)
}

/// A pod that does not lead crashes.
#[must_use]
pub fn crash_follower(seed: u64, shape: Shape) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let Some(victim) = follower(&world) else {
        return report("crash follower", &world, None);
    };
    world.crash(victim);
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("crash follower", &world, took)
}

/// The leader crashes.
#[must_use]
pub fn crash_leader(seed: u64, shape: Shape) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let Some(leader) = world.leader() else {
        return report("crash leader", &world, None);
    };
    world.crash(leader);
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("crash leader", &world, took)
}

/// A pod that does not lead is cut off from the others both ways for `for_`; clients still
/// reach it.
#[must_use]
pub fn isolate_follower(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let Some(victim) = follower(&world) else {
        return report("isolate follower", &world, None);
    };
    world.isolate(victim);
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("isolate follower", &world, took)
}

/// The leader cut off both ways for `for_`.
#[must_use]
pub fn isolate_leader(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let Some(leader) = world.leader() else {
        return report("isolate leader", &world, None);
    };
    world.isolate(leader);
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("isolate leader", &world, took)
}

/// The #77 pattern: one pod's heartbeats lost one way to the rest, another's to it, for
/// `for_`.
#[must_use]
pub fn one_way(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    if let [a, b, c, ..] = pods[..] {
        // c unheard by either, and a unheard by b and c; b hears c again later.
        world.cut_one_way(c, a);
        world.cut_one_way(c, b);
        world.cut_one_way(a, b);
        world.cut_one_way(a, c);
        world.run_for(for_ / 2);
        world.heal();
        world.cut_one_way(c, a);
        world.cut_one_way(a, b);
        world.cut_one_way(a, c);
        world.run_for(for_ / 2);
    }
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("one-way loss", &world, took)
}

/// Every client cut off from a pod that does not lead, for `for_`; the pods still hear each
/// other (F12).
#[must_use]
pub fn clients_cut_from_pod(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let Some(victim) = follower(&world) else {
        return report("clients cut from a pod", &world, None);
    };
    world.cut_clients_from(victim);
    world.run_for(for_);
    world.heal();
    world.run_for(ms(1_000));
    report("clients cut from a pod", &world, None)
}

/// Every pod cut off from every other for `for_`; clients still reach them all.
#[must_use]
pub fn pods_apart(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    world.cut_pods_apart();
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("pods apart", &world, took)
}

/// A rollout replacing every pod, a Deployment surging: a new pod, ready once it holds its
/// share, then an old one drained.
#[must_use]
pub fn rollout(seed: u64, shape: Shape) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let start = world.now();
    for old in pods {
        let new = world.add_pod();
        let _ready = world.run_while_not(ms(30_000), |world| ready(world, new));
        world.drain(old);
        let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
    }
    let took = Some(world.now() - start);
    let _settled = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("rollout", &world, took)
}

/// A rollout as a Deployment with a surge and unavailability of `step` runs one: `step` new
/// pods added, once all are ready `step` old ones drained at once, and so on.
#[must_use]
pub fn rollout_surge(seed: u64, shape: Shape, step: usize) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let start = world.now();
    for olds in pods.chunks(step.max(1)) {
        let new: Vec<NodeId> = olds.iter().map(|_| world.add_pod()).collect();
        let _ready =
            world.run_while_not(ms(30_000), |world| new.iter().all(|pod| ready(world, *pod)));
        for old in olds {
            world.drain(*old);
        }
        let _gone = world.run_while_not(ms(35_000), |world| {
            olds.iter().all(|old| !world.is_alive(*old))
        });
    }
    let took = Some(world.now() - start);
    let _settled = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("rollout, surge", &world, took)
}

/// A zone cut off from the others for `for_`: `away` pods, the leader among them, cut from the
/// rest both ways, still hearing each other, every client reaching every pod. The side without
/// a majority stops serving; the rest elect and take its shards.
#[must_use]
pub fn zone_cut(seed: u64, shape: Shape, away: usize, for_: Time) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let leader = world.leader();
    let zone: Vec<NodeId> = leader
        .into_iter()
        .chain(pods.iter().copied().filter(|pod| Some(*pod) != leader))
        .take(away)
        .collect();
    for inside in &zone {
        for outside in pods.iter().filter(|pod| !zone.contains(pod)) {
            world.cut(*inside, *outside);
        }
    }
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("zone cut off", &world, took)
}

/// A pod frozen for `for_` — the leader when `leader`, else a follower — then thawed with all
/// sent to it meanwhile reaching it at once.
#[must_use]
pub fn frozen(seed: u64, shape: Shape, leader: bool, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let victim = if leader {
        world.leader()
    } else {
        follower(&world)
    };
    let Some(victim) = victim else {
        return report("frozen", &world, None);
    };
    world.freeze(victim, for_);
    world.run_for(for_);
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report(
        if leader {
            "leader frozen"
        } else {
            "follower frozen"
        },
        &world,
        took,
    )
}

/// A follower told to drain and cut off from the others both ways at once, for `for_`: it can
/// hand nothing on, so its shards are failed over as a crash's are; healed, it is no member and
/// stops.
#[must_use]
pub fn drain_cut_off(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let Some(victim) = follower(&world) else {
        return report("drain cut off", &world, None);
    };
    world.drain(victim);
    world.isolate(victim);
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("drain cut off", &world, took)
}

/// Whether `pod` is ready, as its readiness probe would say ([`Node::ready`]): a rollout
/// stops an old pod only once a new one is.
///
/// [`Node::ready`]: edgerush_cluster::Node::ready
#[must_use]
pub fn ready(world: &World, pod: NodeId) -> bool {
    world.ready(pod)
}

/// A scale-down from `shape.pods` to `to`, pods drained one at a time.
#[must_use]
pub fn scale_down(seed: u64, shape: Shape, to: usize) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let start = world.now();
    for old in pods
        .iter()
        .copied()
        .rev()
        .take(shape.pods.saturating_sub(to))
    {
        world.drain(old);
        let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
    }
    let took = Some(world.now() - start);
    let _settled = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("scale down", &world, took)
}

/// A scale-down from `shape.pods` to `to` as Kubernetes does one, every pod to go told at once,
/// the leader among them: most of the voting set may drain together.
#[must_use]
pub fn scale_down_at_once(seed: u64, shape: Shape, to: usize) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let leader = world.leader();
    let going: Vec<NodeId> = leader
        .into_iter()
        .chain(
            pods.iter()
                .copied()
                .rev()
                .filter(|pod| Some(*pod) != leader),
        )
        .take(shape.pods.saturating_sub(to))
        .collect();
    let start = world.now();
    for old in &going {
        world.drain(*old);
    }
    let _gone = world.run_while_not(ms(35_000), |world| {
        going.iter().all(|old| !world.is_alive(*old))
    });
    let took = Some(world.now() - start);
    let _settled = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("scale down at once", &world, took)
}

/// A scale-down done as a Deployment alone would: several pods killed at once, undrained.
#[must_use]
pub fn scale_down_killed(seed: u64, shape: Shape, to: usize) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    for old in pods
        .iter()
        .copied()
        .rev()
        .take(shape.pods.saturating_sub(to))
    {
        world.crash(old);
    }
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("scale down, killed", &world, took)
}

/// A scale-up from `shape.pods` to `to`, one pod at a time.
#[must_use]
pub fn scale_up(seed: u64, shape: Shape, to: usize) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let start = world.now();
    for _ in shape.pods..to {
        let new = world.add_pod();
        let _ready = world.run_while_not(ms(30_000), |world| ready(world, new));
    }
    let took = Some(world.now() - start);
    world.run_for(ms(2_000));
    report("scale up", &world, took)
}

/// Most of the voting set crashing at once, nobody started in their place: the pod left
/// forms the next generation once the API shows them gone (Q1).
#[must_use]
pub fn majority_lost(seed: u64, shape: Shape) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let lose = shape.pods / 2 + 1;
    for pod in pods.iter().copied().take(lose) {
        world.crash(pod);
    }
    world.run_for(ms(5_000));
    report("majority lost", &world, None)
}

/// A lossy, jittery network: `loss` per million lost, delays up to `jitter` more.
#[must_use]
pub fn lossy(seed: u64, shape: Shape, loss: u32, jitter: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    world.set_default_link(Link {
        jitter,
        loss_per_million: loss,
        ..Link::lan()
    });
    world.run_for(ms(10_000));
    report("lossy", &world, None)
}

/// A rollout in which an old pod crashes while the step before it drains another.
#[must_use]
pub fn rollout_with_crash(seed: u64, shape: Shape) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let start = world.now();
    let mut pods = pods.into_iter();
    let (Some(first), Some(second)) = (pods.next(), pods.next()) else {
        return report("rollout with a crash", &world, None);
    };
    let new = world.add_pod();
    let _ready = world.run_while_not(ms(30_000), |world| ready(world, new));
    world.drain(first);
    // Mid-drain, another old pod dies.
    world.run_for(ms(20));
    world.crash(second);
    let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(first));
    let replacement = world.add_pod();
    let _ready = world.run_while_not(ms(30_000), |world| ready(world, replacement));
    for old in pods {
        let new = world.add_pod();
        let _ready = world.run_while_not(ms(30_000), |world| ready(world, new));
        world.drain(old);
        let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
    }
    let took = Some(world.now() - start);
    let _settled = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("rollout with a crash", &world, took)
}

/// A rollout over a lossy, jittery network.
#[must_use]
pub fn rollout_lossy(seed: u64, shape: Shape) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    world.set_default_link(Link {
        jitter: ms(3),
        loss_per_million: 20_000,
        ..Link::lan()
    });
    let start = world.now();
    for old in pods {
        let new = world.add_pod();
        let _ready = world.run_while_not(ms(30_000), |world| ready(world, new));
        world.drain(old);
        let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
    }
    let took = Some(world.now() - start);
    world.set_default_link(Link::lan());
    let _settled = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("rollout, lossy", &world, took)
}

/// A follower nobody hears, though it hears them all, for `for_`: with the lease both ways, it
/// stops before the others take its shards, so they never have two owners.
#[must_use]
pub fn mute_follower(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let Some(victim) = follower(&world) else {
        return report("mute follower", &world, None);
    };
    for other in pods.iter().copied().filter(|other| *other != victim) {
        world.cut_one_way(victim, other);
    }
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("mute follower", &world, took)
}

/// A follower nobody hears, to which everything sent arrives `late` behind, in order — a queue
/// draining slowly after a partition — for `for_`: the others take it out, and the old
/// heartbeats reaching it, saying they hear it, must not keep its lease.
#[must_use]
pub fn late_heartbeats(seed: u64, shape: Shape, late: Time, for_: Time) -> Report {
    let (mut world, pods) = cluster(seed, shape);
    let Some(victim) = follower(&world) else {
        return report("late heartbeats", &world, None);
    };
    let slow = Link {
        delay: late,
        ..Link::lan()
    };
    for other in pods.iter().copied().filter(|other| *other != victim) {
        world.cut_one_way(victim, other);
        world.set_link(other, victim, slow);
    }
    world.run_for(for_);
    world.heal();
    let took = until_served(&mut world, ms(10_000));
    world.run_for(ms(2_000));
    report("late heartbeats", &world, took)
}

/// A follower that stops hearing the leader, the others still hearing it, for `for_`: the
/// pre-vote keeps it from unseating a leader the rest still follow, so no election is held.
#[must_use]
pub fn deaf_follower(seed: u64, shape: Shape, for_: Time) -> Report {
    let (mut world, _) = cluster(seed, shape);
    let (Some(leader), Some(victim)) = (world.leader(), follower(&world)) else {
        return report("deaf follower", &world, None);
    };
    world.cut_one_way(leader, victim);
    world.run_for(for_);
    world.heal();
    world.run_for(ms(2_000));
    report("deaf follower", &world, None)
}

/// Whether the leader, cut off both ways, stands down while still cut off: `for_` later, it
/// leads no more.
#[must_use]
pub fn isolated_leader_stands_down(seed: u64, shape: Shape, for_: Time) -> bool {
    let (mut world, _) = cluster(seed, shape);
    let Some(leader) = world.leader() else {
        return false;
    };
    world.isolate(leader);
    world.run_for(for_);
    !world
        .node(leader)
        .is_some_and(edgerush_cluster::Node::is_leader)
}

/// What happens to a request under several keys while something goes wrong (Q5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trouble {
    /// Nothing.
    None,
    /// A pod that does not lead crashes.
    Crash,
    /// Every client cut off from a pod that does not lead, for 3 s.
    ClientsCut,
}

/// Requests under two keys each — a client's own and one every client shares — asked as
/// `multi` says, through `trouble`.
#[must_use]
pub fn multi_key(seed: u64, shape: Shape, multi: crate::client::Multi, trouble: Trouble) -> Report {
    let mut config = edgerush_cluster::Config::defaults();
    config.shards = shape.shards;
    config.max_key_bytes = shape.max_key_bytes;
    config.replicas = shape.replicas;
    let mut world = World::new(seed, config);
    world.set_forwarding(shape.forwarding);
    let _pods = world.start_cluster(shape.pods);
    let _formed = world.run_while_not(ms(10_000), World::settled);
    if let Some((limit, allow)) = shape.limit {
        world.set_limit(limit, allow);
    }
    world.add_clients_multi(shape.clients, shape.rate, shape.deadline, multi, 999_999);
    world.start_sampling();
    world.run_for(ms(500));
    let name = match (multi, trouble) {
        (crate::client::Multi::Split, Trouble::None) => "two keys, split",
        (crate::client::Multi::Gather, Trouble::None) => "two keys, gathered",
        (crate::client::Multi::Split, Trouble::Crash) => "two keys, split, crash",
        (crate::client::Multi::Gather, Trouble::Crash) => "two keys, gathered, crash",
        (crate::client::Multi::Split, Trouble::ClientsCut) => "two keys, split, clients cut",
        (crate::client::Multi::Gather, Trouble::ClientsCut) => "two keys, gathered, clients cut",
    };
    match trouble {
        Trouble::None => world.run_for(ms(5_000)),
        Trouble::Crash => {
            if let Some(victim) = follower(&world) {
                world.crash(victim);
            }
            let _settled = until_served(&mut world, ms(10_000));
            world.run_for(ms(2_000));
        }
        Trouble::ClientsCut => {
            if let Some(victim) = follower(&world) {
                world.cut_clients_from(victim);
            }
            world.run_for(ms(3_000));
            world.heal();
            world.run_for(ms(1_000));
        }
    }
    // What is in flight answered, so that what owners hold is weighed against what clients
    // were told at rest.
    world.stop_clients();
    world.run_for(ms(1_000));
    report(name, &world, None)
}
