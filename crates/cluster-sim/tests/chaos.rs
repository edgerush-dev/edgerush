//! Random runs: each seed a sequence of faults drawn at random — crashes, drains, pods cut off,
//! links lost one way, loss and jitter, clocks run fast or slow, every pod cut apart — with
//! never more than one pod down at once, as the store sees it, a new one started in its place.
//! Over every run: nothing that must never happen does, two pods serve one shard only briefly,
//! and once the network is whole again the cluster settles (G2).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{NodeId, Time, ms};
use edgerush_cluster_sim::scenario::{self, Shape};
use edgerush_cluster_sim::{Link, Rng, World};

/// What one random run did, and what it measured.
struct Run {
    faults: Vec<String>,
    world: World,
    settled: Option<Time>,
}

fn pick(rng: &mut Rng, pods: &[NodeId]) -> NodeId {
    pods[usize::try_from(rng.below(pods.len() as u64)).unwrap()]
}

/// Whether the voting set keeps a live majority without `victim`.
fn survivable(world: &World, victim: NodeId) -> bool {
    world.table().is_some_and(|table| {
        edgerush_cluster::table::is_quorum(&table.voting, |member| {
            member != victim && world.is_alive(member)
        })
    })
}

fn run(seed: u64) -> Run {
    let shape = Shape {
        shards: 16,
        ..Shape::defaults()
    };
    let (mut world, _) = scenario::cluster(seed, shape);
    let mut rng = Rng::new(seed ^ 0x5eed);
    let mut faults = Vec::new();
    for _ in 0..8 {
        let pods: Vec<NodeId> = world.alive().collect();
        let lasting = ms(200 + rng.below(1_800));
        match rng.below(9) {
            0 => {
                let victim = pick(&mut rng, &pods);
                // A pod cut off earlier may not be back in yet, and is down as far as the store
                // can tell: a crash that would then leave the voting set without a majority is
                // a second pod down, so it waits for the cluster to settle first.
                if !survivable(&world, victim) {
                    let _settled = world.run_while_not(ms(10_000), World::settled);
                }
                faults.push(format!("crash {victim}"));
                world.crash(victim);
                world.add_pod();
                // One down at a time: the next fault waits for the new pod to be voted in.
                let _settled = world.run_while_not(ms(10_000), World::settled);
                world.run_for(lasting);
            }
            1 => {
                let old = pick(&mut rng, &pods);
                faults.push(format!("rollout step {old}"));
                let new = world.add_pod();
                let _ready = world.run_while_not(ms(10_000), |world| scenario::ready(world, new));
                world.drain(old);
                let _gone = world.run_while_not(ms(10_000), |world| !world.is_alive(old));
            }
            2 => {
                let victim = pick(&mut rng, &pods);
                faults.push(format!("isolate {victim} {}ms", lasting / 1_000_000));
                world.isolate(victim);
                world.run_for(lasting);
                world.heal();
            }
            3 => {
                let cuts = 1 + rng.below(3);
                for _ in 0..cuts {
                    let from = pick(&mut rng, &pods);
                    let to = pick(&mut rng, &pods);
                    if from != to {
                        faults.push(format!("one way {from}->{to}"));
                        world.cut_one_way(from, to);
                    }
                }
                world.run_for(lasting);
                world.heal();
            }
            4 => {
                faults.push(format!("lossy {}ms", lasting / 1_000_000));
                world.set_default_link(Link {
                    jitter: ms(5),
                    loss_per_million: 50_000,
                    ..Link::lan()
                });
                world.run_for(lasting);
                world.set_default_link(Link::lan());
            }
            5 => {
                let pod = pick(&mut rng, &pods);
                let ppm = i64::try_from(rng.below(2_001)).unwrap() - 1_000;
                faults.push(format!("clock {pod} {ppm}ppm"));
                world.set_clock_rate(pod, ppm);
                world.run_for(lasting);
            }
            // Scaling: two pods added, the voting set growing from three to five, or one drained
            // away when there are more than three — so the voting set changes size, not only
            // its members.
            6 => {
                faults.push("scale up by two".to_owned());
                let added = [world.add_pod(), world.add_pod()];
                let _ready = world.run_while_not(ms(10_000), |world| {
                    added.iter().all(|pod| scenario::ready(world, *pod))
                });
                world.run_for(lasting);
            }
            7 if pods.len() > 3 => {
                let old = pick(&mut rng, &pods);
                faults.push(format!("scale down {old}"));
                world.drain(old);
                let _gone = world.run_while_not(ms(10_000), |world| !world.is_alive(old));
                world.run_for(lasting);
            }
            _ => {
                faults.push(format!("pods apart {}ms", lasting / 1_000_000));
                world.cut_pods_apart();
                world.run_for(lasting);
                world.heal();
            }
        }
    }
    world.heal();
    world.set_default_link(Link::lan());
    let settled = world.run_while_not(ms(10_000), World::settled);
    world.run_for(ms(1_000));
    Run {
        faults,
        world,
        settled,
    }
}

#[test]
fn random_faults_never_break_the_rules_and_the_cluster_always_settles() {
    // CHAOS_SEEDS runs more, for a long run on a machine of its own.
    let seeds: u64 = std::env::var("CHAOS_SEEDS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(60);
    let mut longest = 0;
    let mut slowest = 0;
    // CHAOS_SEED runs that one alone, to see a failure again; CHAOS_FROM starts the CHAOS_SEEDS
    // there, for a second machine to run others.
    let only: Option<u64> = std::env::var("CHAOS_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok());
    let from: u64 = std::env::var("CHAOS_FROM")
        .ok()
        .and_then(|from| from.parse().ok())
        .unwrap_or(1);
    let range = only.map_or(from..=from + seeds - 1, |seed| seed..=seed);
    for seed in range {
        let Run {
            faults,
            world,
            settled,
        } = run(seed);
        let stats = &world.stats;
        assert!(
            stats.violations.is_empty(),
            "seed {seed}: {:?} after {faults:?}",
            stats.violations
        );
        if settled.is_none() {
            for table in world.history() {
                eprintln!(
                    "  {:?} members {:?} voting {:?} draining {:?} moves {}",
                    table.stamp(),
                    table.members.keys().collect::<Vec<_>>(),
                    table.voting,
                    table.draining,
                    table.moves.len()
                );
            }
            for pod in world.alive() {
                let node = world.node(pod).unwrap();
                eprintln!(
                    "  pod {pod} term {} {:?} committed {:?} accepted {:?}",
                    node.term(),
                    node.role(),
                    node.committed().map(edgerush_cluster::Table::stamp),
                    node.accepted()
                );
            }
        }
        assert!(
            settled.is_some(),
            "seed {seed}: never settled after {faults:?}: {:?}",
            world.table()
        );
        longest = longest.max(stats.overlap_longest);
        slowest = slowest.max(settled.unwrap_or(0));
        if stats.overlap_longest > ms(20) {
            eprintln!(
                "seed {seed}: two owners for {} ms after {faults:?}",
                stats.overlap_longest / 1_000_000
            );
            eprintln!(
                "  first overlaps {:?}",
                &stats.overlaps[..stats.overlaps.len().min(4)]
            );
        }
        // Two owners no longer than a lease: a pod taken out through a cut on one side, still
        // leased through a third pod, that then loses that one too serves on until its lease
        // runs out; while it hears anyone it learns the new table within a heartbeat.
        assert!(
            stats.overlap_longest <= edgerush_cluster::Config::defaults().stop_serving + ms(10),
            "seed {seed}: two owners for {} ms after {faults:?}",
            stats.overlap_longest / 1_000_000
        );
    }
    eprintln!(
        "{seeds} runs: longest overlap {} ms, slowest to settle after the last fault {} ms",
        longest / 1_000_000,
        slowest / 1_000_000
    );
}
