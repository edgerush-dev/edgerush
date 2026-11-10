//! Handing a shard on without a pause: the old owner serves until the new one has caught up,
//! so that a deploy refuses nothing however long a shard takes to copy (Goal A,
//! `membership-design.md`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::ms;
use edgerush_cluster_sim::scenario::{self, Shape};
use edgerush_cluster_sim::world::{Link, World};

const SEEDS: std::ops::RangeInclusive<u64> = 1..=5;

/// A rollout over a network of 100 Mbit/s, each shard holding enough keys to take longer than
/// a request's deadline to copy: requests to a shard on the move are answered throughout, and
/// the rollout refuses nothing.
#[test]
fn a_rollout_refuses_nothing_however_long_a_shard_takes_to_copy() {
    for seed in SEEDS {
        let shape = Shape {
            shards: 16,
            // 320 KB a shard: 26 ms at 100 Mbit/s, past the 20 ms deadline.
            preload: 20_000,
            ..Shape::defaults()
        };
        let (mut world, pods) = scenario::cluster(seed, shape);
        world.set_default_link(Link {
            bandwidth: 12_500_000,
            ..Link::lan()
        });
        for old in pods {
            let new = world.add_pod();
            let ready = world.run_while_not(ms(30_000), |world| scenario::ready(world, new));
            assert!(ready.is_some(), "seed {seed}: {new} never ready");
            world.drain(old);
            let gone = world.run_while_not(ms(35_000), |world| !world.is_alive(old));
            assert!(gone.is_some(), "seed {seed}: {old} never drained");
        }
        assert!(
            world.run_while_not(ms(10_000), World::settled).is_some(),
            "seed {seed}"
        );
        world.run_for(ms(1_000));
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
        assert_eq!(
            world.stats.refused, 0,
            "seed {seed}: refused during the rollout"
        );
        assert_eq!(world.lost(), 0, "seed {seed}");
    }
}

/// A pod joining over slow links (#306), every link 60 ms each way: the leader moves its share
/// of shards to it while it holds a lease, the echo of a heartbeat riding its next one, a
/// round trip and up to a heartbeat late. Moving them only to a pod that answered within two
/// heartbeats, the leader dropped its moves and asked for them again, table after table, from
/// 30 ms each way, and it never became ready.
#[test]
fn a_pod_joining_over_slow_links_takes_its_shards() {
    for seed in SEEDS {
        let shape = Shape {
            shards: 16,
            ..Shape::defaults()
        };
        let (mut world, _) = scenario::cluster(seed, shape);
        world.set_default_link(Link {
            delay: ms(60),
            jitter: ms(1),
            ..Link::lan()
        });
        let new = world.add_pod();
        let ready = world.run_while_not(ms(10_000), |world| scenario::ready(world, new));
        assert!(ready.is_some(), "seed {seed}: {new} never ready");
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}
