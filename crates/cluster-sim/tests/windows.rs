//! The asynchronous mode's fixed window on the store, under the cluster's faults (06 §4): data
//! planes deciding from their copies, each window an entry of its own, the owner counting
//! each report in its window. Measured per key and window, against the allowance (Lucas): at
//! most the limit and 5% more in each window in steady state, and 10% more through faults;
//! a key's first window, cold on every data plane, up to the limit on each.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Config, NodeId, Time, ms};
use edgerush_cluster_sim::{Asynchronous, Link, Sim};
use edgerush_ratelimit::service::{FixedWindow, Limit};
use std::collections::BTreeMap;
use std::num::NonZeroU64;

type World = Sim<Asynchronous>;

const PLANES: u64 = 8;
const KEYS: u64 = 4;
/// The windows' length.
const MINUTE: Time = ms(60_000);
/// Each key's limit a window: 100 a second on average.
const LIMIT: u64 = 6_000;
/// Requests a second each data plane takes, spread over the keys: twice the limit on every
/// key, and two fifths of it.
const OVER: u64 = 2 * LIMIT * KEYS / 60 / PLANES;
const WITHIN: u64 = 2 * LIMIT * KEYS / 60 / PLANES / 5;
/// How long a run goes on after the planes start: four windows, three of them whole for every
/// key whatever its offset.
const RUN: Time = ms(240_000);

const SEEDS: [u64; 3] = [1, 2, 3];

fn window() -> Limit {
    let n = |n| NonZeroU64::new(n).unwrap();
    Limit::FixedWindow(FixedWindow {
        limit: n(LIMIT),
        length: n(MINUTE),
    })
}

/// What a run did, per key and window.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "some fields are read only in the failures' messages, through Debug"
)]
struct Figures {
    name: &'static str,
    requests: u64,
    refused: u64,
    /// The most any key admitted in its first window, cold on every data plane.
    cold: u64,
    /// The most and the fewest any key admitted in a whole window after its first.
    most: u64,
    least: u64,
    /// The whole windows measured, over every key.
    windows: usize,
    violations: Vec<String>,
    settled: bool,
}

impl Figures {
    /// How far past the limit the worst whole window went, in parts per thousand.
    fn over_per_mille(&self) -> i64 {
        (i64::try_from(self.most).unwrap() - i64::try_from(LIMIT).unwrap()) * 1_000
            / i64::try_from(LIMIT).unwrap()
    }
}

/// Three pods formed, `planes` data planes each taking `rate` requests a second over `KEYS`
/// keys shared by all, every link `link`; and the first of its pods not leading.
fn world_with(seed: u64, planes: u64, rate: u64, link: Link) -> (World, NodeId) {
    let mut config = Config::defaults();
    config.shards = 64;
    let mut world = World::new(seed, config);
    let pods = world.start_cluster(3);
    let formed = world.run_while_not(ms(10_000), World::settled);
    assert!(formed.is_some(), "never formed");
    world.set_default_link(link);
    let keys: Vec<u64> = (0..KEYS).collect();
    world.add_planes(planes, &keys, rate, window());
    world.start_sampling();
    let leader = world.leader();
    let follower = pods.into_iter().find(|pod| Some(*pod) != leader).unwrap();
    (world, follower)
}

fn world(seed: u64, rate: u64) -> (World, NodeId) {
    world_with(seed, PLANES, rate, Link::lan())
}

/// What `world` admitted, each key's admissions counted in the windows the data planes
/// number: its first window apart, its last, cut short by the run's end, left out.
fn figures(name: &'static str, world: &World) -> Figures {
    let (requests, refused) = world.decided();
    let mut cold = 0;
    let mut whole = Vec::new();
    for (key, times) in world.admissions() {
        let mut counts: BTreeMap<u64, u64> = BTreeMap::new();
        for at in times {
            *counts.entry(world.window_of(key, MINUTE, at)).or_default() += 1;
        }
        let first = counts.keys().next().copied();
        let last = counts.keys().next_back().copied();
        for (number, count) in counts {
            if Some(number) == first {
                cold = cold.max(count);
            } else if Some(number) != last {
                whole.push(count);
            }
        }
    }
    let figures = Figures {
        name,
        requests,
        refused,
        cold,
        most: whole.iter().copied().max().unwrap_or(0),
        least: whole.iter().copied().min().unwrap_or(0),
        windows: whole.len(),
        violations: world.stats.violations.clone(),
        settled: world.settled(),
    };
    println!("{figures:?} over {} per mille", figures.over_per_mille());
    figures
}

/// What must hold of every run: nothing that must never happen, the cluster settled, and
/// whole windows to judge.
fn sound(figures: &Figures) {
    assert!(figures.violations.is_empty(), "{figures:?}");
    assert!(figures.settled, "{figures:?}");
    assert!(figures.windows >= 2 * KEYS as usize, "{figures:?}");
}

/// The allowance in steady state: at most the limit and 5% more in a window.
fn steady_allowance(figures: &Figures) {
    assert!(figures.over_per_mille() <= 50, "{figures:?}");
}

/// The allowance through faults: 10% more.
fn faulted_allowance(figures: &Figures) {
    assert!(figures.over_per_mille() <= 100, "{figures:?}");
}

/// Twice the limit on every key, spread over every data plane, nothing happening: each whole
/// window held to the limit and 5%, and none shaved below the limit; the first window, cold,
/// at most the limit on each data plane.
#[test]
fn steady_overload_keeps_each_window_to_the_allowance() {
    for seed in SEEDS {
        let (mut world, _) = world(seed, OVER);
        world.run_for(RUN);
        let figures = figures("steady", &world);
        sound(&figures);
        steady_allowance(&figures);
        assert!(figures.least >= LIMIT, "shaved: {figures:?}");
        assert!(figures.cold <= LIMIT * PLANES, "{figures:?}");
    }
}

/// Well within the limit, nothing happening: nothing refused.
#[test]
fn clients_within_the_limit_are_never_refused() {
    for seed in SEEDS {
        let (mut world, _) = world(seed, WITHIN);
        world.run_for(RUN);
        let figures = figures("within", &world);
        sound(&figures);
        assert_eq!(figures.refused, 0, "{figures:?}");
    }
}

/// All of a key's traffic on one data plane, twice the limit: held to the limit itself.
#[test]
fn one_data_plane_alone_holds_its_window_exactly() {
    for seed in SEEDS {
        let (mut world, _) = world_with(seed, 1, OVER * PLANES, Link::lan());
        world.run_for(RUN);
        let figures = figures("one data plane", &world);
        sound(&figures);
        assert_eq!(figures.most, LIMIT, "{figures:?}");
        assert_eq!(figures.least, LIMIT, "{figures:?}");
    }
}

/// Twice the limit, a pod not leading killed halfway through the second window.
#[test]
fn an_owner_lost_keeps_the_allowance_through_faults() {
    for seed in SEEDS {
        let (mut world, victim) = world(seed, OVER);
        world.run_for(ms(90_000));
        world.crash(victim);
        world.run_for(RUN - ms(90_000));
        let figures = figures("owner lost", &world);
        sound(&figures);
        faulted_allowance(&figures);
    }
}

/// Twice the limit, a pod not leading cut off from the others for ten seconds.
#[test]
fn a_pod_cut_off_keeps_the_allowance_through_faults() {
    for seed in SEEDS {
        let (mut world, victim) = world(seed, OVER);
        world.run_for(ms(90_000));
        world.isolate(victim);
        world.run_for(ms(10_000));
        world.heal();
        world.run_for(RUN - ms(100_000));
        let figures = figures("pod cut off", &world);
        sound(&figures);
        faulted_allowance(&figures);
    }
}

/// Twice the limit, every data plane cut off from one pod for ten seconds.
#[test]
fn data_planes_cut_off_from_a_pod_keep_the_allowance_through_faults() {
    for seed in SEEDS {
        let (mut world, victim) = world(seed, OVER);
        world.run_for(ms(90_000));
        world.cut_clients_from(victim);
        world.run_for(ms(10_000));
        world.heal();
        world.run_for(RUN - ms(100_000));
        let figures = figures("planes cut from a pod", &world);
        sound(&figures);
        faulted_allowance(&figures);
    }
}

/// Twice the limit, one packet in 200 lost everywhere: within the allowance through faults.
#[test]
fn lost_packets_keep_the_allowance_through_faults() {
    for seed in SEEDS {
        let lossy = Link {
            loss_per_million: 5_000,
            ..Link::lan()
        };
        let (mut world, _) = world_with(seed, PLANES, OVER, lossy);
        world.run_for(RUN);
        let figures = figures("one packet in 200 lost", &world);
        sound(&figures);
        faulted_allowance(&figures);
    }
}

/// Twice the limit, every pod replaced in turn from the second window, as a Deployment
/// surging does.
#[test]
fn a_rollout_keeps_the_allowance_through_faults() {
    for seed in SEEDS {
        let (mut world, _) = world(seed, OVER);
        world.run_for(ms(70_000));
        let started = world.now();
        let olds: Vec<NodeId> = world.alive().collect();
        for old in olds {
            let new = world.add_pod();
            let _ready = world.run_while_not(ms(30_000), |world| world.ready(new));
            world.drain(old);
            let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
        }
        let _settled = world.run_while_not(ms(10_000), World::settled);
        let spent = world.now() - started;
        world.run_for(RUN.saturating_sub(ms(70_000) + spent).max(ms(120_000)));
        let figures = figures("rollout", &world);
        sound(&figures);
        faulted_allowance(&figures);
    }
}

/// The most any whole window admitted, `planes` data planes each sending one request a
/// second on one key under a fixed window of `limit` a minute, over every seed.
fn small(planes: u64, limit: u64) -> u64 {
    let mut most = 0;
    for seed in SEEDS {
        let mut config = Config::defaults();
        config.shards = 64;
        let mut world = World::new(seed, config);
        world.start_cluster(3);
        let formed = world.run_while_not(ms(10_000), World::settled);
        assert!(formed.is_some(), "never formed");
        let n = |n| NonZeroU64::new(n).unwrap();
        let window = Limit::FixedWindow(FixedWindow {
            limit: n(limit),
            length: n(MINUTE),
        });
        world.add_planes(planes, &[0], 1, window);
        world.run_for(RUN);
        let mut counts: BTreeMap<u64, u64> = BTreeMap::new();
        for (key, times) in world.admissions() {
            for at in times {
                *counts.entry(world.window_of(key, MINUTE, at)).or_default() += 1;
            }
        }
        let whole = counts.len().saturating_sub(2);
        most = most.max(
            counts
                .values()
                .skip(1)
                .take(whole)
                .copied()
                .max()
                .unwrap_or(0),
        );
    }
    println!("{limit} a minute on {planes} data planes: at most {most} a window");
    most
}

/// A limit small next to the number of data planes is held only roughly (06 §4): every data
/// plane's first request of a window is decided from an empty copy, and the floor gives each
/// one more; past the limit by those, never more.
#[test]
fn a_small_limit_on_many_data_planes_is_held_only_roughly() {
    for (planes, limit) in [(8, 20), (20, 10)] {
        let most = small(planes, limit);
        assert!(most > limit, "held exactly, which the docs say it is not");
        assert!(most < limit + 2 * planes, "{most}");
    }
}
