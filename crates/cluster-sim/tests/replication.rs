//! Replication: an owner's copies reach its replicas, and are repaired when they do not, so that
//! a replica promoted on failover holds what its owner did (`membership-design.md`, §6).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{ShardId, ms};
use edgerush_cluster_sim::scenario::{self, Shape};
use edgerush_cluster_sim::world::{Link, World};

const SEEDS: std::ops::RangeInclusive<u64> = 1..=5;

/// Sixteen shards: the same rules, measured faster.
fn shape() -> Shape {
    Shape {
        shards: 16,
        ..Shape::defaults()
    }
}

/// Copies lost on a lossy network are repaired once it heals, though the keys they were for
/// have gone quiet: every replica comes to hold its owner's counts, and an owner crashing then
/// loses nothing.
#[test]
fn copies_lost_are_repaired_once_the_network_heals() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, shape());
        world.set_default_link(Link {
            loss_per_million: 100_000,
            ..Link::lan()
        });
        world.run_for(ms(2_000));
        world.stop_clients();
        world.set_default_link(Link::lan());
        assert!(
            world.run_while_not(ms(5_000), World::settled).is_some(),
            "seed {seed}"
        );
        world.run_for(ms(1_000));
        assert_eq!(
            world.stale_copies(),
            0,
            "seed {seed}: copies never repaired"
        );
        let owner = world
            .alive()
            .find(|pod| Some(*pod) != world.leader())
            .unwrap();
        world.crash(owner);
        assert!(
            world.run_while_not(ms(5_000), World::settled).is_some(),
            "seed {seed}"
        );
        assert_eq!(
            world.lost(),
            0,
            "seed {seed}: lost on failover after the repair"
        );
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}

/// Copies in many parts, on a lossy network that loses some of them, end as copies whole
/// would: repaired once it heals, every replica holding its owner's counts, an owner crashing
/// then losing nothing; with nothing lost, no part is sent twice; and no part carries more
/// than its bound.
#[test]
fn copies_in_parts_are_repaired_and_lose_nothing_on_failover() {
    // Parts of about two keys: every whole copy of a shard's hundred keys in fifty.
    let parted = Shape {
        preload: 100,
        part_bytes: 100,
        ..shape()
    };
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, parted);
        let before = world.stats.copies_again;
        world.run_for(ms(1_000));
        assert_eq!(
            world.stats.copies_again - before,
            0,
            "seed {seed}: sent twice"
        );
        world.set_default_link(Link {
            loss_per_million: 100_000,
            ..Link::lan()
        });
        world.run_for(ms(2_000));
        world.stop_clients();
        world.set_default_link(Link::lan());
        assert!(
            world.run_while_not(ms(5_000), World::settled).is_some(),
            "seed {seed}"
        );
        world.run_for(ms(1_000));
        assert_eq!(
            world.stale_copies(),
            0,
            "seed {seed}: copies never repaired"
        );
        let owner = world
            .alive()
            .find(|pod| Some(*pod) != world.leader())
            .unwrap();
        world.crash(owner);
        assert!(
            world.run_while_not(ms(5_000), World::settled).is_some(),
            "seed {seed}"
        );
        assert_eq!(
            world.lost(),
            0,
            "seed {seed}: lost on failover after the repair"
        );
        assert!(world.stats.parts_more > 0, "seed {seed}: no copy in parts");
        assert_eq!(
            world.stats.parts_over, 0,
            "seed {seed}: parts past the bound"
        );
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}

/// Past `max_key_bytes`, every pod evicts down within it, owned keys and copies alike, and an
/// owner's evictions reach its replicas: no replica keeps a key its owner evicted, so one
/// promoted when the owner crashes brings none of them back at their old counts.
#[test]
fn evictions_keep_every_pod_within_its_cap_and_never_come_back() {
    // Preloaded keys, each held by all three pods: 64 KB a pod, against a cap of half that.
    // No clients: keys charged meanwhile would pass the cap between sweeps, once a second, by
    // what they add in one.
    let preloaded = |key: u64| key > 1 << 40;
    for seed in SEEDS {
        let cap = 32_000;
        let (mut world, _) = scenario::cluster(
            seed,
            Shape {
                preload: 100,
                clients: 0,
                max_key_bytes: Some(cap),
                ..shape()
            },
        );
        world.run_for(ms(3_000));
        let table = world.table().unwrap().clone();
        let mut evicted = 0;
        for pod in world.alive().collect::<Vec<_>>() {
            let node = world.node(pod).unwrap();
            assert!(
                node.key_bytes() <= cap,
                "seed {seed}: pod {pod} over its cap"
            );
            evicted += node.evicted();
        }
        assert!(evicted > 0, "seed {seed}");
        for (index, shard) in table.shards.iter().enumerate() {
            let id = ShardId::try_from(index).unwrap();
            let owner = world.node(shard.owner.unwrap()).unwrap().keys_of(id);
            for replica in &shard.replicas {
                let held = world.node(*replica).unwrap().keys_of(id);
                let kept: Vec<_> = held
                    .difference(&owner)
                    .filter(|key| preloaded(**key))
                    .collect();
                assert!(
                    kept.is_empty(),
                    "seed {seed}: shard {id} replica {replica} kept {kept:?}"
                );
            }
        }
        // Its owner crashed, a shard's keys on the replica promoted are among those the owner
        // held.
        let victim = table.shards[0].owner.unwrap();
        let before: Vec<_> = (0..table.shards.len())
            .filter_map(|index| {
                let id = ShardId::try_from(index).ok()?;
                (table.owner(id) == Some(victim))
                    .then(|| (id, world.node(victim).unwrap().keys_of(id)))
            })
            .collect();
        world.crash(victim);
        world.run_for(ms(2_000));
        let after = world.table().unwrap().clone();
        for (id, held) in before {
            let now = world.node(after.owner(id).unwrap()).unwrap().keys_of(id);
            let back: Vec<_> = now
                .difference(&held)
                .filter(|key| preloaded(**key))
                .collect();
            assert!(
                back.is_empty(),
                "seed {seed}: shard {id} brought back {back:?}"
            );
        }
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}

/// A pod every other hears but that hears none of them holds no lease and serves nothing, and
/// its heartbeats say so: the leader fails its shards over while the cuts last, as it would a
/// pod gone.
#[test]
fn a_pod_heard_but_not_hearing_has_its_shards_failed_over() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, Shape { pods: 5, ..shape() });
        let leader = world.leader().unwrap();
        let table = world.table().unwrap().clone();
        let deaf = world
            .alive()
            .find(|pod| *pod != leader && table.owned_by(*pod).next().is_some())
            .unwrap();
        for other in pods.iter().copied().filter(|other| *other != deaf) {
            world.cut_one_way(other, deaf);
        }
        world.run_for(ms(2_000));
        let unserved = world.unserved_now();
        assert!(unserved.is_empty(), "seed {seed}: unserved {unserved:?}");
        assert_eq!(
            world.table().unwrap().owned_by(deaf).count(),
            0,
            "seed {seed}"
        );
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}

/// A pod that cannot hear the leader alone still holds a lease through the others, and its
/// heartbeats say so: the leader leaves it its shards, which it goes on serving, rather than
/// give them a second owner while it learns of the move from the others.
#[test]
fn a_pod_not_hearing_the_leader_alone_keeps_its_shards() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, Shape { pods: 5, ..shape() });
        let leader = world.leader().unwrap();
        let table = world.table().unwrap().clone();
        let deaf = world
            .alive()
            .find(|pod| *pod != leader && table.owned_by(*pod).next().is_some())
            .unwrap();
        let its_own: Vec<_> = table.owned_by(deaf).collect();
        world.cut_one_way(leader, deaf);
        world.run_for(ms(2_000));
        let now: Vec<_> = world.table().unwrap().owned_by(deaf).collect();
        assert_eq!(now, its_own, "seed {seed}");
        let unserved = world.unserved_now();
        assert!(unserved.is_empty(), "seed {seed}: unserved {unserved:?}");
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}

/// A pod the leader hears but that cannot hear the leader, nor another pod, holds no lease;
/// an owner crashing then, the leader promotes, of the owner's replicas, the first that has
/// answered its heartbeats, so that the owner's shards are served again.
#[test]
fn a_crashed_owners_shards_go_to_a_replica_that_answers() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, Shape { pods: 5, ..shape() });
        let leader = world.leader().unwrap();
        // An owner, not the leader, of a shard whose first replica is not the leader either:
        // that replica is the one cut off, first in line for the shard.
        let table = world.table().unwrap().clone();
        let (owner, deaf) = table
            .shards
            .iter()
            .find_map(|shard| {
                let owner = shard.owner?;
                let first = *shard.replicas.first()?;
                (owner != leader && first != leader).then_some((owner, first))
            })
            .unwrap();
        let other = world
            .alive()
            .find(|pod| ![leader, owner, deaf].contains(pod))
            .unwrap();
        world.cut_one_way(leader, deaf);
        world.cut_one_way(other, deaf);
        world.run_for(ms(1_000));
        world.crash(owner);
        world.run_for(ms(2_000));
        let unserved = world.unserved_now();
        assert!(unserved.is_empty(), "seed {seed}: unserved {unserved:?}");
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
}

/// A replica crashing costs its owners no whole copies while the leader has yet to take it
/// out: unanswered, it is sent again only what changed since it last answered, not all the
/// shards hold.
#[test]
fn a_crashed_replica_is_sent_what_changed_not_whole_shards() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(
            seed,
            Shape {
                preload: 1_000,
                ..shape()
            },
        );
        let replicate = |world: &World| {
            world
                .stats
                .bytes_by_kind
                .get("replicate")
                .copied()
                .unwrap_or(0)
        };
        let leader = world.leader().unwrap();
        let victim = world.alive().find(|pod| *pod != leader).unwrap();
        // What 400 ms of copies costs with nobody down, then with a replica crashed, not yet
        // taken out: the leader times its silence from the last of its own heartbeats the
        // replica echoed, up to a heartbeat before the crash, so 500 ms first.
        let start = replicate(&world);
        world.run_for(ms(400));
        let steady = replicate(&world) - start;
        let before = replicate(&world);
        world.crash(victim);
        world.run_for(ms(400));
        let crashed = replicate(&world) - before;
        let whole = 1_000 * 16;
        assert!(
            crashed < steady + whole,
            "seed {seed}: {crashed} bytes of copies, {steady} with nobody down, a whole shard {whole}"
        );
    }
}

/// With nobody down and nothing lost, every copy is sent once: a replica answering each is
/// never taken for one behind, and sent again what it holds.
#[test]
fn with_nothing_lost_no_copy_is_sent_twice() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, shape());
        let before = world.stats.copies_again;
        world.run_for(ms(2_000));
        assert_eq!(world.stats.copies_again - before, 0, "seed {seed}");
    }
}

/// Most of the voting set lost, the cluster formed again from the pods left, an owner and
/// its replica among them, the owner keeping its shards under the same shard terms: copies
/// it numbers afresh, some lost, must still be repaired, though the replica held higher
/// numbers from before.
#[test]
fn copies_after_forming_again_are_repaired() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, Shape { pods: 5, ..shape() });
        // Long enough that the replicas hold copies numbered in the hundreds.
        world.run_for(ms(5_000));
        for pod in pods.iter().take(3) {
            world.crash(*pod);
        }
        let _ = world.add_pod();
        let formed = world.run_while_not(ms(10_000), |world| {
            world.generation() == 2 && world.settled()
        });
        assert!(formed.is_some(), "seed {seed}");
        // Copies numbered afresh, below those numbers, some lost; then the keys go quiet.
        world.set_default_link(Link {
            loss_per_million: 100_000,
            ..Link::lan()
        });
        world.run_for(ms(1_000));
        world.stop_clients();
        world.run_for(ms(200));
        world.set_default_link(Link::lan());
        assert!(
            world.run_while_not(ms(5_000), World::settled).is_some(),
            "seed {seed}"
        );
        world.run_for(ms(1_000));
        assert_eq!(
            world.stale_copies(),
            0,
            "seed {seed}: copies never repaired"
        );
    }
}
