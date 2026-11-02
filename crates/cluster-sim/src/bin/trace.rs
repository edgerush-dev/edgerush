//! Prints a rollout's timeline, a line every 100 ms: who lives, who leads, the table, and the
//! moves outstanding — to see where a scenario's time goes.
//!
//! `trace [seed] [shards] [keys preloaded per shard] [lossy|killed|atonce|bytes]`

use edgerush_cluster::{NodeId, ms};
use edgerush_cluster_sim::World;
use edgerush_cluster_sim::scenario::{self, Shape};

fn line(world: &World, label: &str) {
    let alive: Vec<NodeId> = world.alive().collect();
    let leader = world.leader();
    let Some(table) = world.table() else {
        println!(
            "{:>7} {label} alive {alive:?} leader {leader:?} no table",
            world.now() / 1_000_000
        );
        return;
    };
    let owned: Vec<(NodeId, usize)> = table
        .members
        .iter()
        .map(|member| (*member, table.owned_by(*member).count()))
        .collect();
    println!(
        "{:>7} {label} alive {alive:?} leader {leader:?} table {:?} members {:?} voting {:?} draining {:?} moves {} owned {owned:?} refused {} lost {} unserved {}",
        world.now() / 1_000_000,
        table.stamp(),
        table.members,
        table.voting,
        table.draining,
        table.moves.len(),
        world.stats.refused,
        world.lost(),
        world.stats.unserved_longest / 1_000_000,
    );
    let unserved = world.unserved_now();
    if !unserved.is_empty() {
        println!("          unserved {unserved:?}");
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let seed: u64 = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(1);
    let shards: u16 = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(16);
    let preload: u64 = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(0);
    let mode = args.next().unwrap_or_default();
    let lossy = mode == "lossy";
    if mode == "bytes" {
        // Bytes by kind of message: steady for 5 s, then a rollout of five pods.
        let shape = Shape {
            shards,
            pods: 5,
            ..Shape::defaults()
        };
        let steady = scenario::steady(seed, shape);
        let rollout = scenario::rollout(seed, shape);
        let one_way = scenario::one_way(seed, shape, ms(3_000));
        for (name, report) in [
            ("steady", steady),
            ("rollout", rollout),
            ("one way", one_way),
        ] {
            println!("{name}: {:.1} MB", report.bytes as f64 / 1e6);
            for (kind, bytes) in &report.bytes_by_kind {
                println!("  {kind:<20} {:>8.1} MB", *bytes as f64 / 1e6);
            }
        }
        return;
    }
    if mode == "atonce" {
        // Five pods, three drained at once, the leader first.
        let shape = Shape {
            shards,
            pods: 5,
            ..Shape::defaults()
        };
        let (mut world, pods) = scenario::cluster(seed, shape);
        line(&world, "start");
        let leader = world.leader();
        let going: Vec<NodeId> = leader
            .into_iter()
            .chain(
                pods.iter()
                    .copied()
                    .rev()
                    .filter(|pod| Some(*pod) != leader),
            )
            .take(3)
            .collect();
        for pod in &going {
            world.drain(*pod);
        }
        line(&world, &format!("drain {going:?}"));
        for _ in 0..80 {
            world.run_for(ms(50));
            line(&world, "      ");
        }
        return;
    }
    if mode == "killed" {
        // Five pods, the two highest killed at once.
        let shape = Shape {
            shards,
            pods: 5,
            ..Shape::defaults()
        };
        let (mut world, pods) = scenario::cluster(seed, shape);
        line(&world, "start");
        for pod in pods.iter().rev().take(2) {
            world.crash(*pod);
        }
        line(&world, "killed");
        for _ in 0..40 {
            world.run_for(ms(50));
            line(&world, "      ");
        }
        return;
    }
    let shape = Shape {
        shards,
        preload,
        ..Shape::defaults()
    };
    let (mut world, pods) = scenario::cluster(seed, shape);
    if lossy {
        world.set_default_link(edgerush_cluster_sim::Link {
            jitter: ms(3),
            loss_per_million: 20_000,
            ..edgerush_cluster_sim::Link::lan()
        });
    }
    line(&world, "start");
    for old in pods {
        let new = world.add_pod();
        line(&world, &format!("added {new}"));
        for _ in 0..80 {
            world.run_for(ms(50));
            line(&world, "      ");
            if scenario::ready(&world, new) {
                break;
            }
        }
        println!("        lost by shard {:?}", world.lost_by_shard());
        world.drain(old);
        line(&world, &format!("drain {old}"));
        for _ in 0..80 {
            world.run_for(ms(50));
            line(&world, "      ");
            if !world.is_alive(old) {
                break;
            }
        }
    }
}
