//! The asynchronous mode on the store, under the cluster's faults: today's data planes
//! deciding from their copies and reporting to the owners the table names, measured against
//! the contract as `ratelimit-sim` measures it — the most admitted over what it allows in any
//! span, and requests refused while within it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Config, NodeId, ms};
use edgerush_cluster_sim::{Asynchronous, Link, Sim};
use edgerush_ratelimit::service::Contract;
use edgerush_ratelimit_sim::worst_excess;
use std::num::NonZeroU64;

type World = Sim<Asynchronous>;

const PLANES: u64 = 8;
const KEYS: u64 = 16;

/// 100 a second, a bucket of ten, for every key.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(10),
    }
}

/// `contract` as `ratelimit-sim` holds it, `burst` its bucket and its rate `percent` of the
/// limit.
fn held_to(contract: Contract, percent: u64, burst: u64) -> edgerush_ratelimit_sim::Contract {
    edgerush_ratelimit_sim::Contract {
        limit: NonZeroU64::new(contract.limit.get() * percent).unwrap(),
        window: NonZeroU64::new(contract.window.get() * 100).unwrap(),
        burst: NonZeroU64::new(burst).unwrap(),
    }
}

/// What a run did.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "some fields are read only in the failures' messages, through Debug"
)]
struct Figures {
    name: &'static str,
    requests: u64,
    refused: u64,
    /// The most any key admitted over its contract, in any span.
    over_contract: i64,
    /// The same over the allowance contract: its rate and 5% more, a bucket for each data
    /// plane.
    over_allowance: i64,
    /// Requests admitted.
    admitted: u64,
    /// Hits the data planes gave up reporting, older than a refill time unseen applied.
    forgiven: u64,
    /// Bytes of entries pods forwarded to their owners.
    forwarded: u64,
    violations: Vec<String>,
    settled: bool,
}

/// Three pods formed, `PLANES` data planes each taking `rate` requests a second over `KEYS`
/// keys shared by all, run for half a second; and the first of its pods not leading.
fn world(seed: u64, rate: u64) -> (World, NodeId) {
    world_with(seed, PLANES, KEYS, rate, contract(), Link::lan())
}

/// The same with `planes` data planes over `keys` keys under `contract`, every link `link`.
fn world_with(
    seed: u64,
    planes: u64,
    keys: u64,
    rate: u64,
    contract: Contract,
    link: Link,
) -> (World, NodeId) {
    let mut config = Config::defaults();
    config.shards = 64;
    let mut world = World::new(seed, config);
    let pods = world.start_cluster(3);
    let formed = world.run_while_not(ms(10_000), World::settled);
    assert!(formed.is_some(), "never formed");
    world.set_default_link(link);
    let keys: Vec<u64> = (0..keys).collect();
    world.add_planes(planes, &keys, rate, contract);
    world.start_sampling();
    world.run_for(ms(500));
    let leader = world.leader();
    let follower = pods.into_iter().find(|pod| Some(*pod) != leader).unwrap();
    (world, follower)
}

/// Bytes of entries pods have forwarded to their owners.
fn forwarded(world: &World) -> u64 {
    world
        .stats
        .bytes_by_kind
        .get("batch forwarded")
        .copied()
        .unwrap_or(0)
}

fn figures(name: &'static str, world: &World) -> Figures {
    figures_under(name, world, contract())
}

/// The same for keys held to `contract`.
fn figures_under(name: &'static str, world: &World, contract: Contract) -> Figures {
    let (requests, refused) = world.decided();
    let admissions = world.admissions();
    let worst = |contract: &edgerush_ratelimit_sim::Contract| {
        admissions
            .values()
            .map(|times| worst_excess(contract, times))
            .max()
            .unwrap_or(i64::MIN)
    };
    let figures = Figures {
        name,
        requests,
        refused,
        over_contract: worst(&held_to(contract, 100, contract.burst.get())),
        over_allowance: worst(&held_to(contract, 105, contract.burst.get() * PLANES)),
        admitted: admissions.values().map(|times| times.len() as u64).sum(),
        forgiven: world.forgiven(),
        forwarded: forwarded(world),
        violations: world.stats.violations.clone(),
        settled: world.settled(),
    };
    println!("{figures:?}");
    figures
}

/// Twice the limit on every key, nothing happening.
fn steady(seed: u64) -> Figures {
    let (mut world, _) = world(seed, 400);
    world.run_for(ms(5_000));
    figures("steady", &world)
}

/// Well within the limit on every key, nothing happening.
fn within(seed: u64) -> Figures {
    let (mut world, _) = world(seed, 80);
    world.run_for(ms(5_000));
    figures("within", &world)
}

/// Twice the limit, a pod not leading killed.
fn crash(seed: u64, rate: u64) -> Figures {
    let (mut world, victim) = world(seed, rate);
    world.run_for(ms(1_000));
    world.crash(victim);
    world.run_for(ms(5_000));
    figures("crash", &world)
}

/// Twice the limit, a pod not leading cut off from the others for two seconds.
fn isolated(seed: u64, rate: u64) -> Figures {
    let (mut world, victim) = world(seed, rate);
    world.run_for(ms(1_000));
    world.isolate(victim);
    world.run_for(ms(2_000));
    world.heal();
    world.run_for(ms(3_000));
    figures("isolated", &world)
}

/// Every data plane cut off from one pod for two seconds; and the bytes forwarded from a
/// second after it healed.
fn planes_cut(seed: u64, rate: u64) -> (Figures, u64) {
    let (mut world, victim) = world(seed, rate);
    world.run_for(ms(1_000));
    world.cut_clients_from(victim);
    world.run_for(ms(2_000));
    world.heal();
    world.run_for(ms(1_000));
    let healed = forwarded(&world);
    world.run_for(ms(2_000));
    let figures = figures("planes cut from a pod", &world);
    let after = figures.forwarded - healed;
    (figures, after)
}

/// Every pod replaced in turn, as a Deployment surging does.
fn rollout(seed: u64, rate: u64) -> Figures {
    let (mut world, _) = world(seed, rate);
    let olds: Vec<NodeId> = world.alive().collect();
    for old in olds {
        let new = world.add_pod();
        let _ready = world.run_while_not(ms(30_000), |world| world.ready(new));
        world.drain(old);
        let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
    }
    let _settled = world.run_while_not(ms(10_000), World::settled);
    world.run_for(ms(2_000));
    figures("rollout", &world)
}

const SEEDS: [u64; 3] = [1, 2, 3];

/// Twice the limit on every key.
const OVER: u64 = 400;
/// Two fifths of it.
const WITHIN: u64 = 80;

/// What must hold of every run: nothing that must never happen, and the cluster settled.
fn sound(figures: &Figures) {
    assert!(figures.violations.is_empty(), "{figures:?}");
    assert!(figures.settled, "{figures:?}");
}

/// Running healthy, the keys are held to the allowance contract.
#[test]
fn healthy_running_keeps_the_allowance_contract() {
    for seed in SEEDS {
        let figures = steady(seed);
        sound(&figures);
        assert!(figures.over_allowance <= 0, "{figures:?}");
        assert_eq!(figures.forgiven, 0, "{figures:?}");
        assert_eq!(figures.forwarded, 0, "every owner answers: {figures:?}");
    }
}

/// No client within its rate is refused, healthy or through any of the faults.
#[test]
fn clients_within_their_rate_are_never_refused() {
    for seed in SEEDS {
        for figures in [
            within(seed),
            crash(seed, WITHIN),
            isolated(seed, WITHIN),
            planes_cut(seed, WITHIN).0,
            rollout(seed, WITHIN),
        ] {
            sound(&figures);
            assert_eq!(figures.refused, 0, "{figures:?}");
        }
    }
}

/// A rollout, every pod replaced, the keys handed on before their takers serve: held to the
/// allowance contract, nothing forgiven.
#[test]
fn a_rollout_keeps_the_allowance_contract() {
    for seed in SEEDS {
        let figures = rollout(seed, OVER);
        sound(&figures);
        assert!(figures.over_allowance <= 0, "{figures:?}");
        assert_eq!(figures.forgiven, 0, "{figures:?}");
    }
}

/// An owner killed, or cut off from the others: its keys unanswered until a replica takes
/// them (`silence`), the data planes deciding from their copies meanwhile, each its share,
/// their batches older than a refill time forgiven. Still within the allowance contract.
#[test]
fn an_owner_lost_keeps_the_allowance_contract() {
    for seed in SEEDS {
        for figures in [crash(seed, OVER), isolated(seed, OVER)] {
            sound(&figures);
            assert!(figures.over_allowance <= 0, "{figures:?}");
        }
    }
}

/// Every data plane cut off from one owner alone, the pods still hearing each other: the
/// owner keeps its keys, and each data plane, its batches there unanswered for four cycles,
/// sends them through one of the shard's replicas as well, which forwards them. Held to the
/// allowance contract, nothing forgiven, within the rate and over it — where without
/// forwarding each data plane decides those keys alone, up to N times the limit across N.
#[test]
fn data_planes_cut_off_from_an_owner_reach_it_through_another_pod() {
    for seed in SEEDS {
        for (figures, after) in [planes_cut(seed, OVER), planes_cut(seed, WITHIN)] {
            sound(&figures);
            assert!(figures.over_allowance <= 0, "{figures:?}");
            assert_eq!(figures.forgiven, 0, "{figures:?}");
            assert!(figures.forwarded > 0, "{figures:?}");
            assert_eq!(after, 0, "still forwarded once healed: {figures:?}");
        }
    }
}

/// One data plane on one key at 99% of its rate, links of 4.5 ms each way — a round trip
/// within a cycle — and a bucket of two cycles, the least that refuses no one within the rate
/// then (06 §4): its copy taken as of when each batch was made, the trip cancelling out,
/// nothing within the rate is refused.
#[test]
fn slow_links_refuse_no_one_within_the_rate() {
    let n = |n| NonZeroU64::new(n).unwrap();
    let tight = Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(2),
    };
    let slow = Link {
        delay: 4_500_000,
        jitter: 1,
        ..Link::lan()
    };
    for seed in SEEDS {
        let (mut world, _) = world_with(seed, 1, 1, 99, tight, slow);
        world.run_for(ms(5_000));
        let figures = figures_under("slow links", &world, tight);
        sound(&figures);
        assert_eq!(figures.refused, 0, "{figures:?}");
    }
}
