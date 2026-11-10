//! Forming a cluster, and forming it again: only the control plane names the pods that may,
//! so there is only ever one cluster of a generation, whoever starts when and whatever is cut
//! off from what (`membership-design.md`, Q1 and Q2).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Config, NodeId, Time, generation_of, ms};
use edgerush_cluster_sim::scenario::{self, Shape};
use edgerush_cluster_sim::world::{CONTROL, Link, World};

const SEEDS: std::ops::RangeInclusive<u64> = 1..=5;

/// Sixteen shards: the same rules, measured faster.
fn shape() -> Shape {
    Shape {
        shards: 16,
        ..Shape::defaults()
    }
}

/// The API's lag, a silence held, an election: a cluster formed again within this.
const REFORMED: Time = ms(3_000);

fn clean(world: &World) {
    assert!(
        world.stats.violations.is_empty(),
        "{:?}",
        world.stats.violations
    );
}

fn cut_apart(world: &mut World, these: &[NodeId], those: &[NodeId]) {
    for a in these {
        for b in those {
            world.cut(*a, *b);
        }
    }
}

/// Every set of just over half of `pods`, in order.
fn majorities(pods: &[NodeId]) -> impl Iterator<Item = Vec<NodeId>> {
    let size = pods.len() / 2 + 1;
    (0_u32..(1 << pods.len()))
        .filter(move |mask| mask.count_ones() as usize == size)
        .map(|mask| {
            pods.iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, pod)| *pod)
                .collect()
        })
}

/// Requests refused over `for_`, the cluster left alone.
fn refused_over(world: &mut World, for_: Time) -> u64 {
    let before = world.stats.refused;
    world.run_for(for_);
    world.stats.refused - before
}

/// Four new pods where three are expected — a pod replaced, or a surge, while the cluster
/// forms — cut two from two: each half is more than half of three, and before the control
/// plane named them, each formed a cluster of its own.
#[test]
fn new_pods_cut_in_two_while_forming_form_one_cluster() {
    for seed in SEEDS {
        let mut world = World::new(seed, Config::defaults());
        // The pods to be started are 1 to 4.
        cut_apart(&mut world, &[1, 2], &[3, 4]);
        let pods = world.start_cluster(4);
        assert_eq!(pods, vec![1, 2, 3, 4]);
        world.run_for(ms(3_000));
        clean(&world);
        world.heal();
        let formed = world.run_while_not(ms(5_000), World::settled);
        assert!(formed.is_some(), "seed {seed}: never settled");
        clean(&world);
        assert_eq!(world.generation(), 1);
    }
}

/// New pods cut off from a cluster that runs — a surge, or a scale-up, behind a network fault
/// — wait to be let in: they form nothing, though they are more than half of those expected.
#[test]
fn new_pods_cut_off_from_a_running_cluster_wait_to_be_let_in() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let new = [world.add_pod(), world.add_pod()];
        cut_apart(&mut world, &new, &pods);
        world.run_for(ms(3_000));
        clean(&world);
        assert_eq!(world.generation(), 1);
        for pod in new {
            assert!(
                world.node(pod).unwrap().committed().is_none(),
                "seed {seed}: {pod} formed"
            );
        }
        world.heal();
        assert!(
            world.run_while_not(ms(3_000), World::settled).is_some(),
            "seed {seed}"
        );
        clean(&world);
        assert_eq!(world.generation(), 1);
    }
}

/// Every pod restarted (F7): once the API shows the old ones gone, the new ones are named to
/// form the next generation, whose tables the data planes take over the old.
#[test]
fn a_whole_restart_forms_the_next_generation() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        for pod in &pods {
            world.crash(*pod);
        }
        for _ in &pods {
            let _ = world.add_pod();
        }
        let took = world.run_while_not(ms(10_000), World::settled);
        assert!(
            took.is_some_and(|took| took <= REFORMED),
            "seed {seed}: {took:?}"
        );
        clean(&world);
        assert_eq!(world.generation(), 2);
        assert_eq!(generation_of(world.table().unwrap().term), 2);
        assert_eq!(refused_over(&mut world, ms(1_000)), 0, "seed {seed}");
    }
}

/// Most of the voting set crashing at once (Q1, F6): with the control plane, no longer a stall.
/// Once the API shows them gone, the pods left and the replacements — more of them than the
/// pods left, whichever stands first — form the next generation from the latest table left,
/// keeping the counts it held.
#[test]
fn losing_most_of_the_voting_set_recovers_through_the_control_plane() {
    for seed in SEEDS {
        for (pods, last) in [(3, false), (3, true), (5, false), (5, true)] {
            let (mut world, ids) = scenario::cluster(seed, Shape { pods, ..shape() });
            // The first or the last majority, by id, that leaves a copy of every shard.
            let leaving = majorities(&ids).filter(|crashed| {
                world.table().is_some_and(|table| {
                    table.shards.iter().all(|shard| {
                        shard
                            .owner
                            .into_iter()
                            .chain(shard.replicas.iter().copied())
                            .any(|pod| !crashed.contains(&pod))
                    })
                })
            });
            let crashed = if last {
                leaving.last()
            } else {
                leaving.into_iter().next()
            };
            let crashed = crashed.expect("a majority leaving a copy of every shard");
            for pod in &crashed {
                world.crash(*pod);
            }
            for _ in &crashed {
                let _ = world.add_pod();
            }
            let took = world.run_while_not(ms(10_000), World::settled);
            let case = format!("seed {seed}, {pods} pods, {crashed:?} crashed");
            assert!(
                took.is_some_and(|took| took <= REFORMED),
                "{case}: {took:?}"
            );
            clean(&world);
            assert_eq!(world.generation(), 2, "{case}");
            assert_eq!(refused_over(&mut world, ms(1_000)), 0, "{case}");
            // Every shard had a copy on a pod left: only a cycle's copies lost.
            assert!(world.lost() <= 100, "{case}: lost {}", world.lost());
        }
    }
}

/// The one pod left holding the latest table, cut off from the pods named with it: forming
/// waits on it for `form_wait`, the service down meanwhile, then the control plane names them
/// again without requiring the latest table, counted, and those that hear each other form —
/// without its counts, a limit's worth of overshoot a key once.
#[test]
fn forming_waits_a_bounded_time_for_a_survivor_cut_off() {
    for seed in SEEDS {
        let (mut world, ids) = scenario::cluster(seed, Shape { pods: 3, ..shape() });
        let (survivor, crashed) = (ids[0], &ids[1..]);
        for pod in crashed {
            world.crash(*pod);
        }
        let fresh: Vec<NodeId> = crashed.iter().map(|_| world.add_pod()).collect();
        cut_apart(&mut world, &[survivor], &fresh);
        let wait = world.config().form_wait;
        world.run_for(wait - ms(500));
        assert_eq!(
            world.generation(),
            2,
            "seed {seed}: named again before the wait"
        );
        assert!(!world.unserved_now().is_empty(), "seed {seed}");
        let took = world.run_while_not(ms(10_000), |world| world.unserved_now().is_empty());
        assert!(
            took.is_some_and(|took| took <= REFORMED),
            "seed {seed}: {took:?}"
        );
        assert_eq!(world.generation(), 3, "seed {seed}");
        assert_eq!(world.named_without_the_latest(), 1, "seed {seed}");
        clean(&world);
    }
}

/// With the control plane down, a cluster that runs goes on — a crash failed over, a new pod
/// let in — while a cluster to be formed again waits for it.
#[test]
fn with_the_control_plane_down_a_running_cluster_goes_on_and_forming_waits() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        world.set_control_down(true);
        world.crash(pods[0]);
        let _ = world.add_pod();
        assert!(
            world.run_while_not(ms(3_000), World::settled).is_some(),
            "seed {seed}"
        );
        clean(&world);
        let alive: Vec<NodeId> = world.alive().collect();
        for pod in &alive {
            world.crash(*pod);
        }
        for _ in &alive {
            let _ = world.add_pod();
        }
        world.run_for(ms(5_000));
        assert_eq!(world.generation(), 1);
        assert!(
            world.leader().is_none(),
            "seed {seed}: formed without the control plane"
        );
        world.set_control_down(false);
        let took = world.run_while_not(ms(10_000), World::settled);
        assert!(
            took.is_some_and(|took| took <= REFORMED),
            "seed {seed}: {took:?}"
        );
        clean(&world);
        assert_eq!(world.generation(), 2);
    }
}

/// A control plane restarted forgets all but the generation: it forms nothing while the
/// cluster runs, and the next generation after a whole restart is above the last.
#[test]
fn a_restarted_control_plane_keeps_the_generation() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        world.restart_control();
        world.run_for(ms(3_000));
        assert_eq!(world.generation(), 1);
        assert_eq!(refused_over(&mut world, ms(500)), 0, "seed {seed}");
        world.restart_control();
        for pod in &pods {
            world.crash(*pod);
        }
        for _ in &pods {
            let _ = world.add_pod();
        }
        assert!(
            world.run_while_not(ms(10_000), World::settled).is_some(),
            "seed {seed}"
        );
        clean(&world);
        assert_eq!(world.generation(), 2);
    }
}

/// Pods judged gone while they run — Terminating on a node cut off — and holding a majority:
/// the next generation forms without them, and the two serve one shard only while the cut
/// lasts. Healed, the old pods take the newer generation's table and stop.
#[test]
fn pods_judged_gone_behind_a_cut_overshoot_only_until_it_heals() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let away = [pods[0], pods[1]];
        let left: Vec<NodeId> = pods
            .iter()
            .copied()
            .filter(|pod| !away.contains(pod))
            .collect();
        let others: Vec<NodeId> = world.alive().filter(|pod| !away.contains(pod)).collect();
        cut_apart(&mut world, &away, &others);
        cut_apart(&mut world, &away, &[CONTROL]);
        for pod in away {
            world.judge_gone(pod);
        }
        let _ = world.add_pod();
        let _ = world.add_pod();
        world.run_for(ms(3_000));
        clean(&world);
        assert_eq!(world.generation(), 2, "seed {seed}");
        assert!(
            world
                .node(left[0])
                .unwrap()
                .committed()
                .is_some_and(|table| generation_of(table.term) == 2),
            "seed {seed}"
        );
        let cut_overlap = world.stats.overlap_longest;
        world.heal();
        world.run_for(ms(1_000));
        clean(&world);
        // Two owners for no longer than the cut, and a heartbeat or two after it.
        assert!(
            world.stats.overlap_longest <= ms(3_300),
            "seed {seed}: {:?}",
            world.stats.overlap_longest
        );
        assert!(
            cut_overlap > 0,
            "seed {seed}: the old pods should have served on their side"
        );
        for pod in away {
            let table = world.node(pod).unwrap().committed().unwrap();
            assert_eq!(
                generation_of(table.term),
                2,
                "seed {seed}: {pod} still on the old table"
            );
        }
    }
}

/// Old voters judged gone and cut off hear one heartbeat of the next generation, then are cut
/// off again before they have its table: they must not elect under their old voting set in
/// the new generation's terms, above the cluster formed in it.
#[test]
fn an_old_quorum_never_elects_in_a_newer_generation() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let away = [pods[0], pods[1]];
        let others: Vec<NodeId> = world.alive().filter(|pod| !away.contains(pod)).collect();
        cut_apart(&mut world, &away, &others);
        cut_apart(&mut world, &away, &[CONTROL]);
        for pod in away {
            world.judge_gone(pod);
        }
        let new: Vec<NodeId> = vec![world.add_pod(), world.add_pod()];
        cut_apart(&mut world, &away, &new);
        world.run_for(ms(3_000));
        assert_eq!(world.generation(), 2, "seed {seed}");
        // The new generation's heartbeats reach the old side; nothing goes back.
        let sides: Vec<NodeId> = others.iter().chain(&new).copied().collect();
        for from in &sides {
            for to in away {
                world.set_link(*from, to, Link::lan());
            }
        }
        world.run_for(ms(150));
        cut_apart(&mut world, &away, &sides);
        world.run_for(ms(3_000));
        clean(&world);
        for pod in away {
            let term = world.node(pod).unwrap().term();
            let table = world.node(pod).unwrap().committed().unwrap();
            assert!(
                generation_of(table.term) == 1 || table.voting != vec![1, 2, 3],
                "seed {seed}: {pod} committed its old voting set at term {term:x}"
            );
        }
    }
}

/// The control plane cut off from a cluster that then replaces two of its three voters by
/// drains: its last table names voters now gone, but the cluster runs. New pods that reach
/// only the control plane must not be named to form another.
#[test]
fn a_stale_view_never_forms_a_cluster_beside_one_that_runs() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        cut_apart(&mut world, &pods, &[CONTROL]);
        for old in [pods[0], pods[1]] {
            let new = world.add_pod();
            world.cut(new, CONTROL);
            let _ = world.run_while_not(ms(3_000), |world| scenario::ready(world, new));
            world.drain(old);
            let _ = world.run_while_not(ms(3_000), |world| !world.is_alive(old));
        }
        assert!(
            world.run_while_not(ms(3_000), World::settled).is_some(),
            "seed {seed}"
        );
        let cluster: Vec<NodeId> = world.alive().collect();
        let fresh = [world.add_pod(), world.add_pod(), world.add_pod()];
        cut_apart(&mut world, &fresh, &cluster);
        world.run_for(ms(3_000));
        clean(&world);
        assert_eq!(
            world.generation(),
            1,
            "seed {seed}: formed beside a running cluster"
        );
    }
}

/// The first leader of a generation crashes once a pod has accepted its first table and
/// before anyone has committed it: the pods left must still form the generation.
#[test]
fn a_leader_crashing_before_its_first_table_commits_leaves_it_formable() {
    for seed in SEEDS {
        let mut world = World::new(seed, Config::defaults());
        // So that one follower has the first table and another not.
        world.cut(1, 3);
        let _ = world.start_cluster(3);
        let mut crashed = None;
        while world.now() < ms(5_000) && crashed.is_none() {
            world.run_for(50_000);
            let committed = (1..=3).any(|pod| world.node(pod).unwrap().committed().is_some());
            let accepted = (1..=3).any(|pod| {
                let node = world.node(pod).unwrap();
                !node.is_leader() && node.accepted() != (0, 0)
            });
            if accepted && !committed {
                crashed = world.leader();
                world.crash(crashed.unwrap());
            }
        }
        assert!(
            crashed.is_some(),
            "seed {seed}: never caught the first table in flight"
        );
        world.heal();
        let took = world.run_while_not(ms(5_000), World::settled);
        assert!(
            took.is_some(),
            "seed {seed}: never formed after {crashed:?} crashed"
        );
        clean(&world);
    }
}

/// Scaled down by draining to one pod, which then restarts: its replacement is the whole
/// service now, and forms the next generation alone.
#[test]
fn a_service_scaled_down_and_restarted_forms_again() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, Shape { pods: 5, ..shape() });
        for pod in pods.iter().skip(1) {
            world.drain(*pod);
            let _ = world.run_while_not(ms(5_000), |world| !world.is_alive(*pod));
        }
        assert!(
            world.run_while_not(ms(3_000), World::settled).is_some(),
            "seed {seed}"
        );
        world.crash(pods[0]);
        let _ = world.add_pod();
        let took = world.run_while_not(ms(10_000), World::settled);
        assert!(
            took.is_some_and(|took| took <= REFORMED),
            "seed {seed}: {took:?}"
        );
        clean(&world);
        assert_eq!(world.generation(), 2);
    }
}

/// Pods named to form a generation that never formed — cut apart — and judged gone while the
/// next forms without them: one heartbeat of the newer generation reaching them must not let
/// their old naming form a cluster in its terms.
#[test]
fn a_stale_naming_never_forms_in_a_newer_generation() {
    for seed in SEEDS {
        let mut world = World::new(seed, Config::defaults());
        // The pods to be started are 1 to 3: named, but none can reach another.
        for (a, b) in [(1, 2), (1, 3), (2, 3)] {
            world.cut(a, b);
        }
        let _ = world.start_cluster(3);
        world.run_for(ms(1_000));
        assert_eq!(world.generation(), 1, "seed {seed}");
        assert!(
            world.table().is_none(),
            "seed {seed}: formed though cut apart"
        );
        cut_apart(&mut world, &[1, 2], &[CONTROL]);
        world.judge_gone(1);
        world.judge_gone(2);
        let new = vec![world.add_pod(), world.add_pod()];
        cut_apart(&mut world, &[1, 2], &new);
        world.run_for(ms(3_000));
        assert_eq!(world.generation(), 2, "seed {seed}");
        // One heartbeat of generation 2 reaches pod 1; then 1 and 2 hear each other alone.
        let newer: Vec<NodeId> = std::iter::once(3).chain(new.iter().copied()).collect();
        for from in &newer {
            world.set_link(*from, 1, Link::lan());
        }
        world.run_for(ms(150));
        cut_apart(&mut world, &[1], &newer);
        world.set_link(1, 2, Link::lan());
        world.set_link(2, 1, Link::lan());
        world.run_for(ms(3_000));
        clean(&world);
        for pod in [1, 2] {
            if let Some(table) = world.node(pod).unwrap().committed() {
                assert!(
                    generation_of(table.term) == 2 && table.voting != vec![1, 2, 3],
                    "seed {seed}: {pod} committed {:?} with voters {:?}",
                    table.stamp(),
                    table.voting
                );
            }
        }
    }
}

/// A running cluster's reports reach the control plane three seconds late, in order, while two
/// of its three voters drain, a fourth member voting in their place: a steady stream of old
/// reports, delivered lately, must not pass for fresh and have new pods named to form a
/// cluster beside it.
#[test]
fn late_reports_never_form_a_cluster_beside_one_that_runs() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let late = Link {
            delay: ms(3_000),
            ..Link::lan()
        };
        for pod in &pods {
            world.set_link(*pod, CONTROL, late);
        }
        // Four members: the voting set stays the first three.
        let joined = [world.add_pod()];
        for pod in joined {
            world.set_link(pod, CONTROL, late);
        }
        let cluster: Vec<NodeId> = pods.iter().chain(&joined).copied().collect();
        let fresh = [
            world.add_pod(),
            world.add_pod(),
            world.add_pod(),
            world.add_pod(),
        ];
        cut_apart(&mut world, &fresh, &cluster);
        world.run_for(ms(4_000));
        for old in [pods[0], pods[1]] {
            world.drain(old);
            let _ = world.run_while_not(ms(3_000), |world| !world.is_alive(old));
        }
        world.run_for(ms(5_000));
        clean(&world);
        assert_eq!(
            world.generation(),
            1,
            "seed {seed}: formed beside a running cluster"
        );
    }
}

/// Pods forming over slow links (#306): every link 90 ms each way, a round trip longer than
/// most of `election_jitter` and short of the two heartbeats an election waits. Those that
/// voted for the first to stand give its word a silence to reach them, so the first term
/// forms the cluster; standing after their own part of the jitter, they unseated the leader
/// they had just elected, term after term, and from 100 ms each way it never formed.
#[test]
fn pods_forming_over_slow_links_form_in_the_first_term() {
    for seed in SEEDS {
        let mut config = Config::defaults();
        config.shards = shape().shards;
        let mut world = World::new(seed, config);
        world.set_default_link(Link {
            delay: ms(90),
            jitter: ms(1),
            ..Link::lan()
        });
        let _ = world.start_cluster(3);
        let took = world.run_while_not(ms(10_000), World::settled);
        assert!(took.is_some(), "seed {seed}: never formed");
        clean(&world);
        assert_eq!(world.stats.terms, 1, "seed {seed}");
    }
}
