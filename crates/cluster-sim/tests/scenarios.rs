//! The faults of `membership-design.md` §2 as scripted scenarios, each held to the outcome its
//! row claims, over several seeds.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Time, ms};
use edgerush_cluster_sim::World;
use edgerush_cluster_sim::scenario::{self, Report, Shape};

/// Sixteen shards rather than 256: the same rules, measured faster.
fn shape() -> Shape {
    Shape {
        shards: 16,
        ..Shape::defaults()
    }
}

fn five() -> Shape {
    Shape { pods: 5, ..shape() }
}

/// Five pods over 256 shards: enough moves at once for one to be dropped mid-way.
fn five_wide() -> Shape {
    Shape {
        shards: 256,
        ..five()
    }
}

const SEEDS: std::ops::RangeInclusive<u64> = 1..=5;

/// How long two pods may both serve one shard: a view change spreading, about a heartbeat.
const OVERLAP: Time = ms(100);

/// G4: a crashed pod's shards decided by others within a second.
const FAILOVER: Time = ms(1_000);

fn sound(report: &Report) {
    assert!(report.violations.is_empty(), "{report:?}");
    assert!(report.overlap_longest <= OVERLAP, "{report:?}");
}

#[test]
fn a_steady_cluster_refuses_and_loses_nothing() {
    for seed in SEEDS {
        let report = scenario::steady(seed, shape());
        sound(&report);
        assert_eq!((report.refused, report.lost), (0, 0), "{report:?}");
        assert_eq!(report.overlap_total, 0, "{report:?}");
        assert!(report.settled);
    }
}

#[test]
fn a_crashed_pod_or_leader_is_failed_over_within_a_second() {
    for seed in SEEDS {
        for report in [
            scenario::crash_follower(seed, shape()),
            scenario::crash_leader(seed, shape()),
            scenario::scale_down_killed(seed, five(), 3),
        ] {
            sound(&report);
            assert!(
                report.took.is_some_and(|took| took < FAILOVER),
                "{report:?}"
            );
            assert!(report.settled, "{report:?}");
        }
    }
}

/// With no replicas, a crashed pod's shards are taken empty by the pods left, within a
/// second, their counts lost: served again rather than by nobody for good.
#[test]
fn a_crashed_pods_shards_with_no_replicas_are_served_again() {
    for seed in SEEDS {
        let bare = Shape {
            replicas: 0,
            ..shape()
        };
        for report in [
            scenario::crash_follower(seed, bare),
            scenario::crash_leader(seed, bare),
        ] {
            assert!(report.violations.is_empty(), "{report:?}");
            assert!(
                report.took.is_some_and(|took| took < FAILOVER),
                "{report:?}"
            );
            assert!(report.settled, "{report:?}");
        }
    }
}

/// A pod crashed and another drained, both gone: once the cluster has settled without them and
/// a few silences more have passed, no pod sends them anything — every pod restarted is a new
/// one, and a day of rollouts would otherwise leave each pod heartbeating hundreds.
#[test]
fn pods_gone_are_sent_nothing_once_forgotten() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        world.crash(pods[0]);
        let _ = world.add_pod();
        assert!(world.run_while_not(ms(10_000), World::settled).is_some());
        world.drain(pods[1]);
        let _ = world.add_pod();
        world.run_for(ms(1_000));
        assert!(world.run_while_not(ms(10_000), World::settled).is_some());
        world.run_for(ms(3_000));
        let before = world.stats.to_gone;
        world.run_for(ms(2_000));
        assert_eq!(world.stats.to_gone - before, 0, "seed {seed}");
    }
}

/// A pod crashing while it catches up on shards a draining pod hands it: none is handed to it
/// once it is gone — it would hold them unserved until they failed over again, where the
/// draining pod could have served them on — but those it said it had caught up on just
/// before, the table committed just after: at most the sixteen moves made at once.
#[test]
fn no_shard_is_handed_to_a_taker_gone() {
    for seed in SEEDS {
        // Over 256 shards, enough moves at once for some to be on their way to it.
        let wide = Shape {
            shards: 256,
            ..shape()
        };
        let report = scenario::rollout_with_crash(seed, wide);
        sound(&report);
        assert!(report.moved_to_gone <= 16, "seed {seed}: {report:?}");
    }
}

/// A pod crashing during a rollout leaves no shard unserved for longer than a crash alone, a
/// heartbeat aside: the next step waits for the failover, as a new pod is ready only while it
/// hears every member, so a shard's replicas are never both draining when its owner's crash
/// fails it over — which would hand it on again.
#[test]
fn a_rollout_waits_out_a_crash_before_its_next_step() {
    for shards in [16, 256] {
        for seed in SEEDS {
            let wide = Shape { shards, ..shape() };
            let report = scenario::rollout_with_crash(seed, wide);
            sound(&report);
            let alone = scenario::crash_follower(seed, wide).unserved_longest;
            assert!(
                report.unserved_longest <= alone + ms(100),
                "{shards} shards seed {seed}: {} ms, a crash alone {} ms",
                report.unserved_longest / ms(1),
                alone / ms(1)
            );
        }
    }
}

/// Runs `world` for `for_` in 5 ms steps, failing if one of `pods` is ever not ready.
fn stay_ready(world: &mut World, pods: &[edgerush_cluster::NodeId], for_: Time, what: &str) {
    let until = world.now() + for_;
    while world.now() < until {
        world.run_for(ms(5));
        for pod in pods {
            assert!(
                world.ready(*pod),
                "{what}: {pod:?} not ready at {} ms",
                world.now() / ms(1)
            );
        }
    }
}

/// A ready pod stays ready while another member goes quiet, frozen or crashed: were every
/// pod to need every member heard, one going quiet would make them all unready at once, and
/// the service's DNS name answer with nobody.
#[test]
fn a_ready_pod_stays_ready_while_another_goes_quiet() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        assert!(
            pods.iter().all(|pod| world.ready(*pod)),
            "seed {seed}: formed, not ready"
        );
        let others: Vec<_> = pods[1..].to_vec();
        world.freeze(pods[0], ms(400));
        stay_ready(&mut world, &others, ms(500), "frozen");
        world.crash(pods[0]);
        stay_ready(&mut world, &others, ms(2_000), "crashed");
    }
}

/// A pod whose wall clock is off by more than `skew_warning` (250 ms) is told skewed by every
/// other, by about how far, and tells them skewed the other way; one off by less, or not at
/// all, is told skewed by none; nor is it once it has drained and been forgotten.
#[test]
fn a_pod_whose_wall_clock_is_off_is_told_skewed_by_the_others() {
    let near = |told: i64, skew: i64| (told - skew).unsigned_abs() <= ms(50);
    for seed in SEEDS {
        for skew in [0, ms(100), ms(400)] {
            for ahead in [true, false] {
                let skew = i64::try_from(skew).unwrap();
                let skew = if ahead { skew } else { -skew };
                let (mut world, pods) = scenario::cluster(seed, shape());
                world.set_wall_skew(pods[0], skew);
                world.run_for(ms(1_000));
                let off = skew.unsigned_abs() > ms(250);
                let told = |by: edgerush_cluster::NodeId| world.node(by).unwrap().skewed();
                for other in &pods[1..] {
                    let by_other = told(*other);
                    let of_it = told(pods[0]).get(other).copied();
                    if off {
                        assert!(
                            by_other.len() == 1
                                && by_other.get(&pods[0]).is_some_and(|told| near(*told, skew)),
                            "seed {seed} skew {skew}: {other} told {by_other:?}"
                        );
                        assert!(
                            of_it.is_some_and(|told| near(told, -skew)),
                            "seed {seed} skew {skew}: {other} told {of_it:?}"
                        );
                    } else {
                        assert!(
                            by_other.is_empty() && of_it.is_none(),
                            "seed {seed} skew {skew}: {other} told {by_other:?}, and {of_it:?}"
                        );
                    }
                }
                // Drained and forgotten, it is told skewed by none.
                world.drain(pods[0]);
                assert!(world.run_while_not(ms(10_000), World::settled).is_some());
                world.run_for(ms(3_000));
                for other in &pods[1..] {
                    let by_other = world.node(*other).unwrap().skewed();
                    assert!(
                        by_other.is_empty(),
                        "seed {seed} skew {skew}: {other} told {by_other:?} once it was gone"
                    );
                }
            }
        }
    }
}

/// A ready pod stays ready while shards move to a pod added: only the new pod waits for
/// them.
#[test]
fn a_ready_pod_stays_ready_while_shards_move() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let new = world.add_pod();
        let mut moved = false;
        let until = world.now() + ms(5_000);
        while world.now() < until && !world.ready(new) {
            world.run_for(ms(5));
            moved |= world.table().is_some_and(|table| !table.moves.is_empty());
            for pod in &pods {
                assert!(
                    world.ready(*pod),
                    "seed {seed}: {pod:?} not ready, shards moving"
                );
            }
        }
        assert!(
            moved && world.ready(new),
            "seed {seed}: moved {moved}, new pod not ready"
        );
    }
}

/// A pod failed over while cut off, back in the table, is ready again only once it holds
/// its share with no moves outstanding and hears every member, as a new pod is.
#[test]
fn a_pod_back_in_the_table_earns_ready_again() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let leader = world.leader();
        let cut = pods
            .iter()
            .copied()
            .find(|pod| Some(*pod) != leader)
            .unwrap();
        world.isolate(cut);
        world.run_for(ms(2_000));
        assert!(!world.ready(cut), "seed {seed}: ready, cut off");
        world.heal();
        let until = world.now() + ms(10_000);
        while world.now() < until && !world.ready(cut) {
            world.run_for(ms(5));
        }
        let table = world.node(cut).and_then(|node| node.committed()).unwrap();
        let share = usize::from(world.config().shards) / table.members.len();
        assert!(
            table.moves.is_empty() && table.owned_by(cut).count() >= share,
            "seed {seed}: ready again with {} moves, {} shards of {share}",
            table.moves.len(),
            table.owned_by(cut).count()
        );
    }
}

/// A rollout of three pods over 256 shards publishes each table to the pods holding the one
/// before as what changed, sixteen moves a table: under 1.5 MB of publishes, where whole
/// tables took ~9.5 MB, nearly all of a table being its shards, and four moves a table
/// ~2.4 MB.
#[test]
fn a_rollout_publishes_tables_as_what_changed() {
    for seed in 1..=3 {
        let shape = Shape {
            shards: 256,
            ..shape()
        };
        let report = scenario::rollout(seed, shape);
        sound(&report);
        let published = report.bytes_by_kind.get("publish").copied().unwrap_or(0);
        assert!(
            published < 1_500_000,
            "seed {seed}: {published} bytes published"
        );
    }
}

/// Goal A: rollouts, one pod at a time or surging by two, a scale-up and scale-downs by
/// draining, one pod at a time or all at once, refuse nothing and lose nothing.
#[test]
fn deploys_refuse_and_lose_nothing() {
    // No shard waits longer than a handoff's swap — the old owner serving until the table
    // names the taker, the taker as soon as it has the last changes — a sample or two:
    // refusing nothing must not be luck. Draining 256 shards down to two pods, the last pod to
    // go hands nearly all of them to one taker at once, its last changes on one link behind
    // the copies of the rest: 1 to 4 ms over ten seeds.
    let swap = ms(2);
    for seed in SEEDS {
        for (report, longest) in [
            (scenario::rollout(seed, shape()), swap),
            (scenario::rollout(seed, five()), swap),
            (scenario::scale_up(seed, shape(), 5), swap),
            (scenario::scale_down(seed, five(), 3), swap),
            (scenario::scale_down(seed, five_wide(), 2), ms(5)),
            (scenario::scale_down_at_once(seed, five(), 2), swap),
            (scenario::scale_down_at_once(seed, five_wide(), 2), swap),
            (scenario::rollout_surge(seed, five(), 2), swap),
        ] {
            sound(&report);
            assert_eq!((report.refused, report.lost), (0, 0), "{report:?}");
            assert!(report.unserved_longest <= longest, "{report:?}");
            assert!(report.settled, "{report:?}");
        }
    }
}

/// Told to stop with no pod left to take their shards — every pod at once, as deleting the
/// service or scaling it to zero does, or the last of a scale-down one at a time — pods stop
/// drained, soon, rather than killed at the end of their grace period.
#[test]
fn pods_none_is_left_to_take_from_stop_drained() {
    let soon = ms(1_000);
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, five());
        for pod in &pods {
            world.drain(*pod);
        }
        let took = world.run_while_not(ms(10_000), |world| {
            pods.iter().all(|pod| !world.is_alive(*pod))
        });
        assert!(
            took.is_some_and(|took| took <= soon),
            "seed {seed}: {took:?}"
        );
        assert!(
            world.stopped.iter().all(|(_, _, drained)| *drained),
            "seed {seed}: {:?}",
            world.stopped
        );

        let (mut world, pods) = scenario::cluster(seed, five());
        for pod in pods.iter().rev() {
            world.drain(*pod);
            let _gone = world.run_while_not(ms(10_000), |world| !world.is_alive(*pod));
        }
        assert!(
            world.stopped.len() == pods.len()
                && world.stopped.iter().all(|(_, _, drained)| *drained),
            "seed {seed}: {:?}",
            world.stopped
        );
        let took = match world.stopped.as_slice() {
            [.., (_, before, _), (_, last, _)] => Some(*last - *before),
            _ => None,
        };
        assert!(
            took.is_some_and(|took| took <= soon),
            "seed {seed}: {took:?}"
        );
    }
}

/// Goal B: a pod or the leader cut off, heartbeats lost one way, pods cut apart from each
/// other, a zone of two pods, the leader among them, cut from the other three, and a pod cut
/// off as it drains: never two owners for longer than a view change spreading, and settled
/// again once the network is.
#[test]
fn network_faults_only_overshoot_and_not_without_end() {
    for seed in SEEDS {
        for report in [
            scenario::isolate_follower(seed, shape(), ms(3_000)),
            scenario::isolate_leader(seed, shape(), ms(3_000)),
            scenario::one_way(seed, shape(), ms(3_000)),
            scenario::pods_apart(seed, shape(), ms(1_000)),
            scenario::lossy(seed, shape(), 20_000, ms(2)),
            scenario::zone_cut(seed, five(), 2, ms(3_000)),
            scenario::drain_cut_off(seed, shape(), ms(3_000)),
        ] {
            sound(&report);
            assert!(report.settled, "{report:?}");
        }
    }
}

/// A pod frozen, as CPU throttling or a stalled node stops one: shorter than its lease, it
/// counts on thawing what queued meanwhile, an overshoot; longer, it is failed over as a crash
/// is. Either way, never two owners, and settled again.
#[test]
fn a_frozen_pod_only_overshoots() {
    for seed in SEEDS {
        for report in [
            scenario::frozen(seed, shape(), false, ms(300)),
            scenario::frozen(seed, shape(), true, ms(300)),
            scenario::frozen(seed, shape(), false, ms(2_000)),
            scenario::frozen(seed, shape(), true, ms(2_000)),
        ] {
            sound(&report);
            assert_eq!(report.overlap_total, 0, "{report:?}");
            assert!(report.settled, "{report:?}");
        }
    }
}

/// Messages lost now and then, 2% of them each way: a request whose ask or answer is lost is
/// left to `when_unreachable` — refused under `deny`, admitted under `allow`, an overshoot —
/// never asked again elsewhere, where it would be counted twice. About twice the loss is left
/// so, and nothing is counted twice.
#[test]
fn a_lossy_network_leaves_its_losses_to_when_unreachable_and_counts_nothing_twice() {
    for seed in SEEDS {
        let report = scenario::lossy(seed, shape(), 20_000, ms(2));
        sound(&report);
        assert!(report.refused * 100 <= report.sent * 6, "{report:?}");
        assert_eq!(report.counted_twice, 0, "{report:?}");
    }
}

/// The lease runs both ways: a pod nobody hears stops before the others take its shards, so
/// they never have two owners.
#[test]
fn a_pod_nobody_hears_stops_before_its_shards_are_taken() {
    for seed in SEEDS {
        let report = scenario::mute_follower(seed, shape(), ms(3_000));
        sound(&report);
        assert_eq!(report.overlap_total, 0, "{report:?}");
        assert!(report.settled, "{report:?}");
    }
}

/// A pod cut off from the others, the clients still reaching it: it stops serving, and a
/// request it answers "not serving" — not counted — is asked again of another pod, whose table
/// names the new owner, not of it again until its deadline.
#[test]
fn a_pod_cut_off_sends_its_keys_requests_to_the_new_owner() {
    for seed in SEEDS {
        let report = scenario::isolate_follower(seed, shape(), ms(3_000));
        sound(&report);
        assert!(report.refused * 50 <= report.sent, "{report:?}");
    }
}

/// The lease is timed from when the pod sent what the others answer: old heartbeats reaching a
/// pod taken out, late and in order, saying they hear it, do not keep it serving.
#[test]
fn old_heartbeats_arriving_late_never_extend_a_lease() {
    for seed in SEEDS {
        let report = scenario::late_heartbeats(seed, shape(), ms(2_000), ms(4_000));
        sound(&report);
        assert!(report.settled, "{report:?}");
    }
}

/// The pre-vote: a follower that stops hearing the leader, the others still hearing it, does
/// not unseat it.
#[test]
fn a_follower_that_does_not_hear_the_leader_does_not_unseat_it() {
    for seed in SEEDS {
        let report = scenario::deaf_follower(seed, shape(), ms(3_000));
        sound(&report);
        assert_eq!(report.terms, 1, "{report:?}");
        assert!(report.settled, "{report:?}");
    }
}

/// A leader that hears no majority both ways stands down while still cut off, a silence and
/// a heartbeat or so after: not only once a table it publishes goes unaccepted, later still.
#[test]
fn an_isolated_leader_stands_down() {
    for seed in SEEDS {
        assert!(
            scenario::isolated_leader_stands_down(seed, shape(), ms(900)),
            "seed {seed}"
        );
    }
}

/// Q1: most of the voting set lost at once, so the table can no longer change, and nothing
/// unsafe happens meanwhile; once the API shows them gone, the pod left is the whole service
/// running and is named to form the next generation alone.
#[test]
fn losing_most_of_the_voting_set_recovers_through_the_control_plane_alone() {
    for seed in SEEDS {
        let report = scenario::majority_lost(seed, shape());
        sound(&report);
        assert!(report.settled, "{report:?}");
    }
}

/// Q5: a request under two keys, its parts split by the client, pods forwarding a part whose
/// owner the client cannot reach: while clients are cut off from a pod, nothing left to
/// `when_unreachable` but the asks in flight to it as the cut came, their answers cut off
/// too; and gathered by a pod, the same. Split without forwarding is left so throughout.
#[test]
fn requests_under_two_keys_reach_their_owners_through_another_pod() {
    use edgerush_cluster_sim::client::Multi;
    use edgerush_cluster_sim::scenario::Trouble;
    let forwarding = Shape {
        forwarding: true,
        ..shape()
    };
    for seed in SEEDS {
        for report in [
            scenario::multi_key(seed, forwarding, Multi::Split, Trouble::ClientsCut),
            scenario::multi_key(seed, shape(), Multi::Gather, Trouble::ClientsCut),
            scenario::multi_key(seed, forwarding, Multi::Split, Trouble::None),
        ] {
            sound(&report);
            // At most a request's parts each client had in flight.
            assert!(report.refused <= forwarding.clients * 2, "{report:?}");
            assert_eq!(report.counted_twice, 0, "{report:?}");
        }
        let alone = scenario::multi_key(seed, shape(), Multi::Split, Trouble::ClientsCut);
        assert!(alone.refused > 0, "{alone:?}");
    }
}
