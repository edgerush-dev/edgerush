//! Both modes on one store (docs/06 §4): a cluster serving synchronous clients and
//! asynchronous data planes at once, through the store's faults. Each mode is held to what it
//! is held to alone (G2): the synchronous mode refusing and losing nothing where it does
//! alone, the asynchronous mode within the allowance contract and refusing no one within their
//! rate. One mode's flood of keys under the cap spares the other's (G4), and a data plane
//! leaving one mode for the other starts afresh while its old keys go. No pod is ever asked
//! under a key of the other mode's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Config, NodeId, Time, ms};
use edgerush_cluster_sim::scenario::{self, Report, Shape};
use edgerush_cluster_sim::{World, worst_excess};
use edgerush_cluster_synchronous::Rule;
use edgerush_ratelimit::service::Contract;
use std::num::NonZeroU64;

const SEEDS: [u64; 3] = [1, 2, 3];
const PLANES: u64 = 8;
const KEYS: u64 = 16;
/// Twice the limit on every asynchronous key, and two fifths of it.
const OVER: u64 = 400;
const WITHIN: u64 = 80;

/// 100 a second, a bucket of ten, for every asynchronous key.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(10),
    }
}

/// The allowance contract, as the asynchronous mode's tests hold it: the rate and 5% more,
/// a bucket for each data plane.
fn allowance() -> Contract {
    let contract = contract();
    Contract {
        limit: NonZeroU64::new(contract.limit.get() * 105).unwrap(),
        window: NonZeroU64::new(contract.window.get() * 100).unwrap(),
        burst: NonZeroU64::new(contract.burst.get() * PLANES).unwrap(),
    }
}

fn shape() -> Shape {
    Shape {
        shards: 64,
        ..Shape::defaults()
    }
}

/// A cluster of three pods, formed, the synchronous mode's clients running as its scenarios
/// run them, and `PLANES` asynchronous data planes each taking `rate` requests a second over
/// `KEYS` keys shared by all.
fn mixed(seed: u64, shape: Shape, rate: u64) -> (World, Vec<NodeId>) {
    let (mut world, pods) = scenario::cluster(seed, shape);
    let keys: Vec<u64> = (0..KEYS).collect();
    world.add_planes(PLANES, &keys, rate, contract());
    world.run_for(ms(500));
    (world, pods)
}

/// What a run of both did: the synchronous mode's report, and the asynchronous mode's
/// figures.
#[derive(Debug)]
struct Both {
    synchronous: Report,
    /// The asynchronous data planes' requests refused.
    refused: u64,
    /// The most any asynchronous key admitted over the allowance contract, in any span.
    over_allowance: i64,
    forgiven: u64,
    wrong_kind: u64,
}

fn figures(name: &'static str, world: &World, took: Option<Time>) -> Both {
    let (_, refused) = world.decided();
    let over_allowance = world
        .admissions()
        .values()
        .map(|times| worst_excess(&allowance(), times))
        .max()
        .unwrap_or(i64::MIN);
    Both {
        synchronous: scenario::report(name, world, took),
        refused,
        over_allowance,
        forgiven: world.forgiven(),
        wrong_kind: world.wrong_kind(),
    }
}

/// What must hold of every run of both: nothing that must never happen, the cluster settled,
/// no key of the other mode's asked, and the synchronous mode counting nothing twice.
fn sound(both: &Both) {
    assert!(both.synchronous.violations.is_empty(), "{both:?}");
    assert!(both.synchronous.settled, "{both:?}");
    assert_eq!(both.wrong_kind, 0, "{both:?}");
    assert_eq!(both.synchronous.counted_twice, 0, "{both:?}");
}

/// The first pod not leading.
fn follower(world: &World, pods: &[NodeId]) -> NodeId {
    let leader = world.leader();
    *pods.iter().find(|pod| Some(**pod) != leader).unwrap()
}

/// Running healthy, both at once: the synchronous mode refuses and loses nothing, as alone;
/// the asynchronous mode, at twice its limit, keeps the allowance contract and forgives
/// nothing, as alone.
#[test]
fn both_modes_running_healthy_each_keep_their_own_bounds() {
    for seed in SEEDS {
        let (mut world, _) = mixed(seed, shape(), OVER);
        world.run_for(ms(5_000));
        let both = figures("steady", &world, None);
        sound(&both);
        assert_eq!(
            (both.synchronous.refused, both.synchronous.lost),
            (0, 0),
            "{both:?}"
        );
        assert!(both.over_allowance <= 0, "{both:?}");
        assert_eq!(both.forgiven, 0, "{both:?}");
    }
}

/// A pod not leading killed, both at once: the synchronous mode's shards served again within
/// a second, as alone; no asynchronous data plane within its rate refused, as alone.
#[test]
fn a_crash_beside_both_modes_is_failed_over_and_refuses_no_one_within_their_rate() {
    for seed in SEEDS {
        let (mut world, pods) = mixed(seed, shape(), WITHIN);
        world.run_for(ms(1_000));
        world.crash(follower(&world, &pods));
        let took = world.run_while_not(ms(10_000), |world| world.unserved_now().is_empty());
        world.run_for(ms(3_000));
        let both = figures("crash", &world, took);
        sound(&both);
        assert!(took.is_some_and(|took| took < ms(1_000)), "{both:?}");
        assert_eq!(both.refused, 0, "{both:?}");
    }
}

/// Every pod replaced in turn, both at once: the synchronous mode refuses and loses nothing,
/// as a deploy does alone; no asynchronous data plane within its rate refused, nothing
/// forgiven, at twice the limit the allowance contract kept.
#[test]
fn a_rollout_beside_both_modes_refuses_and_loses_nothing() {
    for (seed, rate) in SEEDS
        .into_iter()
        .flat_map(|seed| [(seed, WITHIN), (seed, OVER)])
    {
        let (mut world, pods) = mixed(seed, shape(), rate);
        for old in pods {
            let new = world.add_pod();
            let _ready = world.run_while_not(ms(30_000), |world| world.ready(new));
            world.drain(old);
            let _gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
        }
        let _settled = world.run_while_not(ms(10_000), World::settled);
        world.run_for(ms(2_000));
        let both = figures("rollout", &world, None);
        sound(&both);
        assert_eq!(
            (both.synchronous.refused, both.synchronous.lost),
            (0, 0),
            "{both:?}"
        );
        assert_eq!(both.forgiven, 0, "{both:?}");
        if rate == WITHIN {
            assert_eq!(both.refused, 0, "{both:?}");
        } else {
            assert!(both.over_allowance <= 0, "{both:?}");
        }
    }
}

/// A pod cut off from the others for two seconds, both at once: nothing that must never
/// happen, the cluster settled once healed, no asynchronous data plane within its rate
/// refused.
#[test]
fn a_pod_cut_off_beside_both_modes_refuses_no_one_within_their_rate() {
    for seed in SEEDS {
        let (mut world, pods) = mixed(seed, shape(), WITHIN);
        world.run_for(ms(1_000));
        world.isolate(follower(&world, &pods));
        world.run_for(ms(2_000));
        world.heal();
        let _settled = world.run_while_not(ms(10_000), World::settled);
        world.run_for(ms(2_000));
        let both = figures("isolated", &world, None);
        sound(&both);
        assert_eq!(both.refused, 0, "{both:?}");
    }
}

/// Past the cap, whichever mode's keys may be forgotten sooner are what is evicted, the other
/// mode's spared (G4): the synchronous mode's keys under a bucket, forgettable once full again,
/// and the asynchronous mode's once quiet for a refill time and full; each mode's kept busy,
/// more of them than the cap holds, their forget times well apart. As the store evicts by the
/// time a key may be forgotten, never by its mode, the two orders swap with the two times. (A
/// window would not do: its end falls at a phase of each key's own, anywhere in it.)
#[test]
fn past_the_cap_the_keys_forgettable_soonest_go_whatever_their_mode() {
    // The synchronous mode's sooner: its buckets full again within a millisecond of each
    // request, against the asynchronous mode's refilling over 100 s.
    let synchronous_sooner = (ms(1), 1, 6_000, 1_000);
    // The asynchronous mode's sooner: its buckets full a second after their last hit, against
    // the synchronous mode's each request putting five minutes on.
    let asynchronous_sooner = (ms(300_000), 100, 2_000, 2_000);
    for seed in [1, 2] {
        for (case, (period, limit, synchronous_keys, asynchronous_keys)) in
            [synchronous_sooner, asynchronous_sooner]
                .into_iter()
                .enumerate()
        {
            let mut config = Config::defaults();
            config.shards = 64;
            config.max_key_bytes = Some(500_000);
            let mut world = World::new(seed, config);
            let pods = world.start_cluster(3);
            assert!(world.run_while_not(ms(10_000), World::settled).is_some());
            // One request a period, a burst of a million: never refused.
            world.set_limit(
                Rule::Bucket {
                    per: 1,
                    period,
                    burst: 1_000_000,
                },
                false,
            );
            // Eight clients, each key asked every 375 ms or so.
            let each = synchronous_keys / 8;
            for client in 0..8 {
                let keys = (0..each).map(|key| client * 10_000 + key).collect();
                world.add_client(keys, 1_000_000_000 / 2_000, ms(20));
            }
            let contract = Contract {
                limit: NonZeroU64::new(limit).unwrap(),
                window: NonZeroU64::new(1_000_000_000).unwrap(),
                burst: NonZeroU64::new(100).unwrap(),
            };
            let keys: Vec<u64> = (0..asynchronous_keys).collect();
            world.add_planes(PLANES, &keys, 2_000, contract);
            world.start_sampling();
            world.run_for(ms(5_000));
            let both = figures("past the cap", &world, None);
            assert!(both.synchronous.violations.is_empty(), "{both:?}");
            assert_eq!(both.wrong_kind, 0, "{both:?}");
            let evicted = pods
                .iter()
                .filter_map(|pod| world.node(*pod))
                .map(|node| node.evicted_by_kind())
                .fold([0, 0], |sum, kinds| [sum[0] + kinds[0], sum[1] + kinds[1]]);
            let (sooner, later) = if case == 0 { (0, 1) } else { (1, 0) };
            assert!(
                evicted[sooner] > 0,
                "seed {seed}, case {case}: nothing evicted of the kind due sooner: {evicted:?}"
            );
            assert_eq!(
                evicted[later], 0,
                "seed {seed}, case {case}: the kind due later evicted: {evicted:?}"
            );
        }
    }
}

/// A data plane leaving the asynchronous mode for the synchronous: its keys of the old mode
/// are forgotten by their time, held by no pod soon after; the new mode's are made afresh,
/// refusing and losing nothing; no pod asked under a key of the other mode's.
#[test]
fn a_data_plane_switching_mode_starts_afresh_and_its_old_keys_go() {
    for seed in SEEDS {
        let mut config = Config::defaults();
        config.shards = 64;
        let mut world = World::new(seed, config);
        let pods = world.start_cluster(3);
        assert!(world.run_while_not(ms(10_000), World::settled).is_some());
        let keys: Vec<u64> = (0..KEYS).collect();
        world.add_planes(PLANES, &keys, WITHIN, contract());
        world.start_sampling();
        world.run_for(ms(2_000));
        let held = |world: &World| -> [usize; 2] {
            pods.iter()
                .filter_map(|pod| world.node(*pod))
                .map(|node| node.keys_by_kind())
                .fold([0, 0], |sum, kinds| {
                    [sum[0] + kinds[0].0, sum[1] + kinds[1].0]
                })
        };
        assert!(held(&world)[1] > 0, "seed {seed}: no asynchronous key held");
        for plane in world.plane_ids() {
            world.end_plane(plane);
        }
        let shape = Shape::defaults();
        world.add_clients(shape.clients, shape.rate, shape.deadline);
        world.run_for(ms(4_000));
        let both = figures("switched", &world, None);
        sound(&both);
        assert_eq!(
            (both.synchronous.refused, both.synchronous.lost),
            (0, 0),
            "{both:?}"
        );
        let now = held(&world);
        assert_eq!(
            now[1], 0,
            "seed {seed}: the old mode's keys still held: {now:?}"
        );
        assert!(now[0] > 0, "seed {seed}: the new mode's keys never made");
    }
}
