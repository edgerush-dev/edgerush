//! The rate limit itself: keys' owners holding every request to a rule — a fixed window, a
//! rolling window or a bucket — through the faults of `membership-design.md` §2. What matters
//! is what clients were told — admitted beyond the rule (an overshoot, which faults may cause,
//! bounded) and refused within it (a shave, which Goal B says no network fault causes) — each
//! judged by the rule's own reference.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::ms;
use edgerush_cluster_sim::client::Multi;
use edgerush_cluster_sim::scenario::{self, Report, Shape, Trouble};
use edgerush_cluster_sim::world::World;
use edgerush_cluster_synchronous::Rule;

const SEEDS: std::ops::RangeInclusive<u64> = 1..=5;

/// A fixed window, a rolling window and a bucket, each 3 a half second: a bucket's burst of 3
/// is its window's worth.
const RULES: [Rule; 3] = [
    Rule::Window {
        per: 3,
        window: ms(500),
    },
    Rule::Rolling {
        per: 3,
        window: ms(500),
    },
    Rule::Bucket {
        per: 3,
        period: ms(500),
        burst: 3,
    },
];

/// A fixed window of ten minutes, for tests that read what owners hold once clients stop: no
/// key's window ends, and the key is forgotten, before it is read.
const LONG: Rule = Rule::Window {
    per: 3,
    window: ms(600_000),
};

/// The most beyond any rule at once its owners may admit through a fault: two owners serving a
/// key for the time it takes one to stop, and the charges of a cycle not yet copied when the
/// other took over, each at most a limit's — or a burst's — worth.
const PEAK: u64 = 2 * 3;

/// Eight clients at 1 920 a second over 32 keys each, so 60 a second a key, held to 3 every
/// half second by `rule`: a key asks ten times its limit, room enough for an overshoot to show.
const fn limited(rule: Rule, allow: bool) -> Shape {
    Shape {
        shards: 16,
        rate: 1_920,
        limit: Some((rule, allow)),
        ..Shape::defaults()
    }
}

fn line(report: &Report) -> String {
    format!(
        "{}: limited {} shaved {} refused with room {} overshoot {} (peak {}) lost {} refused {}",
        report.name,
        report.limited,
        report.shaved,
        report.refused_with_room,
        report.overshoot,
        report.overshoot_peak,
        report.lost,
        report.refused
    )
}

/// Steady running, under every rule: it bites, and nothing is admitted beyond it or refused
/// within it.
#[test]
fn a_steady_cluster_admits_to_the_rule_exactly() {
    for rule in RULES {
        for seed in SEEDS {
            for allow in [false, true] {
                let report = scenario::steady(seed, limited(rule, allow));
                assert!(report.violations.is_empty(), "{report:?}");
                assert!(report.limited > 0, "{rule:?} never bit: {}", line(&report));
                assert_eq!(
                    (report.overshoot, report.shaved, report.refused_with_room),
                    (0, 0, 0),
                    "{rule:?} seed {seed}: {}",
                    line(&report)
                );
            }
        }
    }
}

/// Requests under one key through the faults, under every rule, under `when_unreachable` deny
/// and allow: the rule still bites; no key is admitted beyond it by more than [`PEAK`], what
/// `allow` admits without a count aside; and nothing is refused within it but on a network
/// losing messages: under `deny`, an answer lost after the owner charged leaves its request
/// charged, as decided; under either, a give-back lost holds allowance until it is sent again.
#[test]
fn faults_overshoot_boundedly_and_never_shave() {
    let mut wrong = Vec::new();
    for rule in RULES {
        for seed in 1..=3 {
            for allow in [false, true] {
                let shape = limited(rule, allow);
                let five = Shape { pods: 5, ..shape };
                for (report, may_shave) in [
                    (scenario::crash_follower(seed, shape), false),
                    (scenario::crash_leader(seed, shape), false),
                    (scenario::isolate_follower(seed, shape, ms(2_000)), false),
                    (scenario::one_way(seed, shape, ms(2_000)), false),
                    (
                        scenario::clients_cut_from_pod(seed, shape, ms(2_000)),
                        false,
                    ),
                    (scenario::pods_apart(seed, shape, ms(2_000)), false),
                    (scenario::rollout(seed, shape), false),
                    (scenario::scale_down(seed, five, 3), false),
                    (scenario::lossy(seed, shape, 20_000, ms(3)), true),
                ] {
                    assert!(report.violations.is_empty(), "{report:?}");
                    let shaves = report.shaved > 0 || (allow && report.refused_with_room > 0);
                    let beyond = report.overshoot_peak > PEAK;
                    if report.limited == 0 || beyond || (shaves && !may_shave) {
                        wrong.push(format!(
                            "{rule:?} seed {seed} allow {allow} {}",
                            line(&report)
                        ));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// An owner crashing, pods forwarding, under every rule: a replica deciding the requests it
/// would forward from its copy, once the owner has gone unheard, leaves fewer of them to
/// `when_unreachable` than one forwarding them to an owner gone, and still holds its keys to
/// the rule — no more beyond it than [`PEAK`], nothing refused within it.
#[test]
fn a_forwarder_deciding_from_its_copy_holds_a_crashed_owners_keys_to_the_rule() {
    let mut wrong = Vec::new();
    for rule in RULES {
        for seed in 1..=3 {
            let shape = Shape {
                forwarding: true,
                ..limited(rule, false)
            };
            let off = scenario::crash_follower(seed, shape);
            let decides = Shape {
                forwarder_decides: true,
                ..shape
            };
            let on = scenario::crash_follower(seed, decides);
            assert!(off.violations.is_empty(), "{off:?}");
            assert!(on.violations.is_empty(), "{on:?}");
            assert_eq!(off.decided_from_copy, 0, "{}", line(&off));
            if on.decided_from_copy == 0
                || on.refused >= off.refused
                || on.limited == 0
                || on.overshoot_peak > PEAK
                || on.shaved > 0
            {
                wrong.push(format!(
                    "{rule:?} seed {seed}: decided from copies {}, on {} | off {}",
                    on.decided_from_copy,
                    line(&on),
                    line(&off)
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Requests under two keys, split, a part refused under a window: the other part's charge is
/// given back, so the owners end holding what clients were told.
#[test]
fn split_requests_refused_for_a_limit_give_their_parts_back() {
    for seed in SEEDS {
        let shape = Shape {
            forwarding: true,
            ..limited(LONG, false)
        };
        let report = scenario::multi_key(seed, shape, Multi::Split, Trouble::None);
        assert!(report.limited > 0, "the limit never bit: {}", line(&report));
        assert_eq!(report.overcount, 0, "seed {seed}: {}", line(&report));
    }
}

/// A request refused under a window is charged nothing: once what was in flight is answered,
/// the owners hold just what clients were told was admitted.
#[test]
fn requests_refused_for_their_limit_are_charged_nothing() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, limited(LONG, false));
        world.run_for(ms(2_000));
        world.stop_clients();
        world.run_for(ms(1_000));
        assert!(world.stats.limited > 0, "seed {seed}: the limit never bit");
        assert_eq!(world.overcount(), 0, "seed {seed}");
    }
}

/// Every pod forgets a key once its time has come — its window ended, its newest slot out of
/// the window, its bucket full — by its own clock, the owner and the replicas alike, sending
/// nothing; keys counted under no rule are kept.
#[test]
fn every_pod_forgets_a_key_once_its_time_has_come() {
    let held = |world: &World| -> usize {
        world
            .alive()
            .filter_map(|pod| world.node(pod))
            .map(edgerush_cluster::Node::key_count)
            .sum()
    };
    for rule in RULES {
        let (mut world, _) = scenario::cluster(1, limited(rule, false));
        world.run_for(ms(2_000));
        assert!(held(&world) > 0, "{rule:?}");
        world.stop_clients();
        // Its time, half a second at most, then a sweep, once a second.
        world.run_for(ms(1_600));
        assert_eq!(held(&world), 0, "{rule:?}");
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
    }
    let (mut world, _) = scenario::cluster(1, scenario::Shape::defaults());
    world.run_for(ms(1_000));
    world.stop_clients();
    let before = held(&world);
    world.run_for(ms(3_000));
    assert!(before > 0);
    assert_eq!(held(&world), before, "keys under no rule forgotten");
}

/// Requests under two keys, split, under every rule, under `allow`, steady, a pod crashing or
/// clients cut from a pod: a part charged for a request then refused holds allowance until it
/// is given back, so a few requests are refused within the rule — never more than one in a
/// hundred of those refused under it. Under `deny` a crash may cost a window of the
/// key every client shares, the rule for answers lost after the owner charged.
#[test]
fn split_requests_shave_at_most_a_hundredth_under_allow() {
    for rule in RULES {
        for seed in 1..=3 {
            for trouble in [Trouble::None, Trouble::Crash, Trouble::ClientsCut] {
                let shape = Shape {
                    forwarding: true,
                    ..limited(rule, true)
                };
                let report = scenario::multi_key(seed, shape, Multi::Split, trouble);
                assert!(report.limited > 0, "{rule:?} never bit: {}", line(&report));
                assert!(
                    report.shaved * 100 <= report.limited,
                    "{rule:?} seed {seed}: {}",
                    line(&report)
                );
            }
        }
    }
}

/// One pod's wall clock off the rest by up to a fifth of a second, ahead or behind, the pod
/// then crashing, its keys failing over to pods whose clocks agree, under every rule: each
/// owner decides by its own wall clock, so its keys disagree only where they change owner —
/// never more than a limit's worth admitted beyond the rule at once, and fewer than one in a
/// hundred refusals within it, clocks taken to agree and skew measured. What clients
/// see in true time — the allowance a key lost moving onto a pod whose clock is behind, or the
/// windows a skewed clock shifts — is printed beside it, measured, not held to a bound.
#[test]
fn a_skewed_wall_clock_costs_little_where_keys_change_owner() {
    for rule in RULES {
        for skew in [0, ms(5), ms(50), ms(200)] {
            for ahead in [true, false] {
                let (mut world, pods) = scenario::cluster(1, limited(rule, true));
                let skew = i64::try_from(skew).unwrap();
                world.set_wall_skew(pods[1], if ahead { skew } else { -skew });
                world.run_for(ms(2_000));
                world.crash(pods[1]);
                world.run_for(ms(3_000));
                let limited = world.stats.limited;
                let truly = world.in_true_time().unwrap();
                let (lost, beyond) = truly.allowance();
                let case = format!(
                    "{rule:?} skew {} ms ahead {ahead}: shaved {} of {limited}, peak {}; \
                     in true time allowance lost {lost} beyond {beyond}, peak {}",
                    skew / i64::try_from(ms(1)).unwrap(),
                    world.shaved(),
                    world.overshoot_peak(),
                    truly.overshoot_peak()
                );
                println!("{case}");
                assert!(world.overshoot_peak() <= 3, "{case}");
                assert!(world.shaved() * 100 <= limited, "{case}");
                assert!(
                    world.stats.violations.is_empty(),
                    "{:?}",
                    world.stats.violations
                );
            }
        }
    }
}
