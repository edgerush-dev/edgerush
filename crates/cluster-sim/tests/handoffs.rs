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
