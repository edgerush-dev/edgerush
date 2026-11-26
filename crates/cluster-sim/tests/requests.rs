//! The request contract: what a data plane does at a request's deadline and after, so that a
//! fault between it and the pods only ever overshoots (`membership-design.md`, §6).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{NodeId, ms};
use edgerush_cluster_sim::scenario::{self, Shape};

const SEEDS: std::ops::RangeInclusive<u64> = 1..=5;

/// Sixteen shards: the same rules, measured faster.
fn shape() -> Shape {
    Shape {
        shards: 16,
        ..Shape::defaults()
    }
}

/// A follower crashing, pods forwarding: until clients learn a table that moves its shards,
/// they send its keys another way, and each key one way, as a data plane does — to a replica
/// of its shard chosen by the key, the same from every client — rather than spreading a key's
/// requests over the pods, which would have a replica deciding from its copy see a part of
/// them alone.
#[test]
fn a_key_whose_owner_is_out_of_reach_goes_one_way() {
    for seed in SEEDS {
        let forwarding = Shape {
            forwarding: true,
            ..shape()
        };
        let (mut world, _) = scenario::cluster(seed, forwarding);
        let victim = world
            .alive()
            .find(|pod| Some(*pod) != world.leader())
            .unwrap();
        world.crash(victim);
        world.run_for(ms(2_000));
        let long_way = world.long_way();
        assert!(!long_way.is_empty(), "seed {seed}: no key sent another way");
        let spread: Vec<_> = long_way.iter().filter(|(_, pods)| pods.len() > 1).collect();
        assert!(
            spread.is_empty(),
            "seed {seed}: {} of {} keys sent more than one way: {:?}",
            spread.len(),
            long_way.len(),
            spread.iter().take(5).collect::<Vec<_>>()
        );
    }
}

/// A pod's answers to the data planes lost, what they send it still arriving: a request the
/// owner had, its answer lost, is left to its deadline, never asked again elsewhere — where it
/// would be counted a second time.
#[test]
fn a_request_whose_answer_is_lost_is_never_counted_twice() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, shape());
        let pod = world.leader().unwrap();
        let clients: Vec<NodeId> = world.client_ids().collect();
        for client in clients {
            world.cut_one_way(pod, client);
        }
        world.run_for(ms(2_000));
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
        assert_eq!(world.stats.counted_twice, 0, "seed {seed}");
    }
}

/// Every answer to the data planes arriving after its request's deadline, while a pod drains
/// and its shards move: each request is left to `when_unreachable`, then given back when its
/// late "counted" arrives — to the key's owner now, which may not be the one that counted it.
/// The owners end holding what the clients were told, nothing more.
#[test]
fn late_answers_are_given_back_to_the_owner_now() {
    use edgerush_cluster_sim::world::Link;
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let late = Link {
            delay: ms(30),
            ..Link::lan()
        };
        let clients: Vec<NodeId> = world.client_ids().collect();
        let mut all: Vec<NodeId> = pods.clone();
        let new = world.add_pod();
        all.push(new);
        for pod in &all {
            for client in &clients {
                world.set_link(*pod, *client, late);
            }
        }
        let leaving = pods
            .iter()
            .copied()
            .find(|pod| Some(*pod) != world.leader())
            .unwrap();
        world.drain(leaving);
        world.run_for(ms(1_500));
        world.heal();
        world.run_for(ms(500));
        world.stop_clients();
        world.run_for(ms(1_000));
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
        assert_eq!(
            world.overcount(),
            0,
            "seed {seed}: counted beyond what clients were told"
        );
        assert_eq!(world.lost(), 0, "seed {seed}");
    }
}

/// A request under two keys split by the data plane, one part's owner crashing: the parts
/// counted for requests then refused are given back, so the owners end holding what the
/// clients were told.
#[test]
fn parts_counted_for_requests_refused_are_given_back() {
    use edgerush_cluster_sim::client::Multi;
    use edgerush_cluster_sim::scenario::Trouble;
    for seed in SEEDS {
        let forwarding = Shape {
            forwarding: true,
            ..shape()
        };
        let report = scenario::multi_key(seed, forwarding, Multi::Split, Trouble::Crash);
        assert!(
            report.orphaned > 0,
            "seed {seed}: nothing orphaned to give back"
        );
        assert!(report.violations.is_empty(), "{:?}", report.violations);
        assert_eq!(report.overcount, 0, "seed {seed}: {report:?}");
    }
}

/// Answers arriving over a second late: a request left to `when_unreachable` waits for its
/// answer for as long as its pod is there, not a fixed time, and its give-back is sent until
/// it is answered, three sends at most; the owners end holding what the clients were
/// told, less two give-backs too many at most for each, its answers late — not one a send
/// every 100 ms.
#[test]
fn answers_however_late_are_given_back() {
    use edgerush_cluster_sim::world::Link;
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let late = Link {
            delay: ms(1_200),
            ..Link::lan()
        };
        // The clients' checks wait longer than the answers are late, so that the pods stay up
        // for them.
        world.set_checks(edgerush_cluster_sim::client::Checks {
            timeout: ms(2_000),
            ..edgerush_cluster_sim::client::Checks::default()
        });
        let clients: Vec<NodeId> = world.client_ids().collect();
        for pod in &pods {
            for client in &clients {
                world.set_link(*pod, *client, late);
            }
        }
        world.run_for(ms(1_000));
        world.stop_clients();
        world.run_for(ms(4_000));
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
        assert_eq!(
            world.overcount(),
            0,
            "seed {seed}: counted beyond what clients were told"
        );
        let (lost, given) = (world.lost(), world.give_backs());
        assert!(
            given > 0 && lost <= 2 * given,
            "seed {seed}: given back {lost} beyond what clients were told, of {given} give-backs"
        );
    }
}

/// Answers arriving 5 s late, long after a give-back's last send: each is sent three times,
/// not once every interval until its answer comes, so that one charge is credited three
/// times at most however late its answers. The clients' checks wait longer than that, so
/// that their late answers keep the pods up.
#[test]
fn a_give_back_is_sent_three_times_at_most() {
    use edgerush_cluster_sim::client::Checks;
    use edgerush_cluster_sim::world::Link;
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        world.set_checks(Checks {
            timeout: ms(10_000),
            ..Checks::default()
        });
        let late = Link {
            delay: ms(5_000),
            ..Link::lan()
        };
        let clients: Vec<NodeId> = world.client_ids().collect();
        for pod in &pods {
            for client in &clients {
                world.set_link(*pod, *client, late);
            }
        }
        world.run_for(ms(1_000));
        world.stop_clients();
        world.run_for(ms(8_000));
        let (sends, given) = (world.stats.give_back_sends, world.give_backs());
        assert!(
            given > 0 && sends <= 3 * given,
            "seed {seed}: {sends} sends of {given} give-backs"
        );
    }
}

/// Answers arriving after their deadline, so that every request is left to `when_unreachable`
/// and given back once its "counted" comes, and a fifth of what clients send lost on the way:
/// a give-back lost is sent again, three sends in all, and the owners end holding what
/// the clients were told but for give-backs lost all three times — one in 125 at a fifth lost,
/// held here under one in fifty of the requests sent, each given back.
#[test]
fn give_backs_lost_are_sent_again() {
    use edgerush_cluster_sim::world::Link;
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        let late = Link {
            delay: ms(50),
            ..Link::lan()
        };
        let lossy = Link {
            loss_per_million: 200_000,
            ..Link::lan()
        };
        let clients: Vec<NodeId> = world.client_ids().collect();
        for pod in &pods {
            for client in &clients {
                world.set_link(*pod, *client, late);
                world.set_link(*client, *pod, lossy);
            }
        }
        world.run_for(ms(1_000));
        world.stop_clients();
        world.run_for(ms(4_000));
        assert!(
            world.stats.violations.is_empty(),
            "{:?}",
            world.stats.violations
        );
        let (standing, sent) = (world.overcount(), world.stats.sent);
        assert!(
            standing * 50 <= sent,
            "seed {seed}: {standing} counted beyond what clients were told, of {sent} sent"
        );
    }
}

/// A request under two keys gathered by a pod, one part's owner crashing: the parts the
/// gathering pod counted for requests it then reported not counted, and parts counted after,
/// are given back, so the owners end holding what the clients were told — but for parts the
/// crashed pod had counted for requests it was itself gathering: they went with it, and only
/// it knew of them, at most a request each client had in flight to it.
#[test]
fn parts_a_gathering_pod_counted_for_requests_refused_are_given_back() {
    use edgerush_cluster_sim::client::Multi;
    use edgerush_cluster_sim::scenario::Trouble;
    for seed in SEEDS {
        let report = scenario::multi_key(seed, shape(), Multi::Gather, Trouble::Crash);
        assert!(
            report.orphaned > 0,
            "seed {seed}: nothing orphaned to give back"
        );
        assert!(report.violations.is_empty(), "{:?}", report.violations);
        assert!(
            report.overcount <= shape().clients,
            "seed {seed}: {report:?}"
        );
    }
}

/// Clients cut off from a pod, both ways: their checks of it refused, they ask nothing of it,
/// by any path, while it stays cut off — sent there again and again, each coming back
/// unreachable, they would flood the others with their requests sent another way.
#[test]
fn a_pod_a_client_cannot_reach_is_left_alone_while_its_checks_are_refused() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, shape());
        let pod = world.leader().unwrap();
        world.cut_clients_from(pod);
        world.run_for(ms(3_000));
        // What each client sent it before its first check there was refused, a millisecond
        // or two, and whatever each had in flight to it then.
        let bound = shape().clients * 10;
        assert!(
            world.stats.unreachable <= bound,
            "seed {seed}: {}",
            world.stats.unreachable
        );
    }
}

/// A request that could not reach its key's owner is sent another way, it alone: the next
/// under the owner's keys goes to the owner again, as a data plane's worker marks nothing
/// down. A check of the owner refused marks it down for every request; one answered, up.
#[test]
fn a_request_that_cannot_reach_its_pod_marks_it_down_for_no_other() {
    use edgerush_cluster::table;
    use edgerush_cluster_sim::client::{Client, Message};
    let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
    let first = table::first(&members, 16, 2, 1);
    let Some(key) = (0..).find(|key| first.owner(table::shard_of(*key, 16)) == Some(1)) else {
        panic!("pod 1 owns nothing");
    };
    let mut client = Client::new(1_000_000, vec![key], ms(5), ms(20), 16);
    client.learn(first);
    let pods = [1, 2, 3];
    let id_of = |send: &(NodeId, Message)| match send.1 {
        Message::Request { id, .. } => id,
        _ => u64::MAX,
    };
    let cut = client.send(ms(1), &pods).unwrap();
    assert_eq!(cut.0, 1, "sent to the owner");
    let (elsewhere, _) = client.unreachable(id_of(&cut), 1, ms(2), &pods).unwrap();
    assert_ne!(elsewhere, 1, "sent another way");
    assert_eq!(
        client.send(ms(3), &pods).unwrap().0,
        1,
        "the next to the owner"
    );
    // Checked: the owner's refused, the others' answered.
    let checks = client.checks_due(ms(4), &pods);
    assert_eq!(checks.len(), 3, "each pod checked at once");
    for (pod, id) in checks {
        if pod == 1 {
            client.check_refused(pod, id, ms(5));
        } else {
            client.check_answered(pod, id, ms(5));
        }
    }
    assert!(client.is_down(1));
    assert_ne!(client.send(ms(6), &pods).unwrap().0, 1, "marked down");
    // Its next check, an interval after the last, answered: up.
    assert!(client.checks_due(ms(50), &pods).is_empty(), "not yet due");
    let again = client.checks_due(ms(105), &pods);
    let (_, id) = again.iter().find(|(pod, _)| *pod == 1).copied().unwrap();
    client.check_answered(1, id, ms(106));
    assert!(!client.is_down(1));
    assert_eq!(client.send(ms(107), &pods).unwrap().0, 1, "up again");
}

/// Requests unanswered mark no pod down, however many, as a data plane's timeouts decide only
/// their own requests: the owner is asked still. Its checks missed in a row do, each checked
/// again at once after a miss; an answer between starts the count again.
#[test]
fn requests_unanswered_mark_no_pod_down_and_checks_missed_do() {
    use edgerush_cluster::table;
    use edgerush_cluster_sim::client::{Checks, Client, Outcome};
    let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
    let first = table::first(&members, 16, 2, 1);
    let Some(key) = (0..).find(|key| first.owner(table::shard_of(*key, 16)) == Some(1)) else {
        panic!("pod 1 owns nothing");
    };
    let mut client = Client::new(1_000_000, vec![key], ms(5), ms(20), 16);
    client.checking = Checks {
        interval: ms(10),
        timeout: ms(20),
        misses: 2,
    };
    client.learn(first);
    let pods = [1, 2, 3];
    for at in 1..=5 {
        assert_eq!(client.send(ms(at), &pods).unwrap().0, 1);
    }
    let (ended, _) = client.check(ms(40), &pods);
    assert_eq!(ended.len(), 5);
    assert!(
        ended
            .iter()
            .all(|(_, outcome)| *outcome == Outcome::Refused)
    );
    assert_eq!(client.send(ms(41), &pods).unwrap().0, 1, "asked still");
    // Pod 1's checks go unanswered; the others' are answered.
    let checked = |client: &mut Client, at: u64| {
        let mut out = None;
        for (pod, id) in client.checks_due(at, &pods) {
            if pod == 1 {
                out = Some(id);
            } else {
                client.check_answered(pod, id, at);
            }
        }
        out
    };
    let first_check = checked(&mut client, ms(42)).unwrap();
    let second = checked(&mut client, ms(62)).expect("missed: checked again at once");
    assert_ne!(second, first_check);
    assert!(!client.is_down(1), "one miss");
    // Answered, the count starts again.
    client.check_answered(1, second, ms(63));
    assert_eq!(
        checked(&mut client, ms(70)),
        None,
        "an interval after an answer"
    );
    let third = checked(&mut client, ms(73)).unwrap();
    let _fourth = checked(&mut client, ms(93)).expect("missed: checked again at once");
    assert!(!client.is_down(1), "one miss since the answer");
    assert_eq!(
        checked(&mut client, ms(113)),
        None,
        "two in a row: an interval on"
    );
    assert!(client.is_down(1), "two in a row");
    assert_ne!(client.send(ms(113), &pods).unwrap().0, 1, "marked down");
    // A late answer to a check missed before brings nothing back.
    client.check_answered(1, third, ms(114));
    assert!(client.is_down(1));
}

/// A request left unanswered at its deadline has its pod checked at once, before the
/// interval is out; another soon after, no sooner than an interval after the check it asked
/// for, so that requests going wrong on a pod that answers do not have it checked back to back.
#[test]
fn a_request_gone_wrong_has_its_pod_checked_at_once_once_each_interval() {
    use edgerush_cluster::table;
    use edgerush_cluster_sim::client::{Checks, Client};
    let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
    let first = table::first(&members, 16, 2, 1);
    let Some(key) = (0..).find(|key| first.owner(table::shard_of(*key, 16)) == Some(1)) else {
        panic!("pod 1 owns nothing");
    };
    let mut client = Client::new(1_000_000, vec![key], ms(50), ms(20), 16);
    client.checking = Checks {
        interval: ms(100),
        timeout: ms(50),
        misses: 3,
    };
    client.learn(first);
    let pods = [1, 2, 3];
    let of_1 = |checks: Vec<(NodeId, u64)>| checks.into_iter().find(|(pod, _)| *pod == 1);
    for (pod, id) in client.checks_due(ms(1), &pods) {
        client.check_answered(pod, id, ms(2));
    }
    // Unanswered past its deadline: pod 1 checked at once, its interval not out.
    assert_eq!(client.send(ms(3), &pods).unwrap().0, 1);
    let _ = client.check(ms(23), &pods);
    let (_, asked) = of_1(client.checks_due(ms(23), &pods)).expect("checked at once");
    client.check_answered(1, asked, ms(24));
    // Another soon after: an interval after the one it asked for, not at once.
    assert_eq!(client.send(ms(25), &pods).unwrap().0, 1);
    let _ = client.check(ms(45), &pods);
    assert!(
        of_1(client.checks_due(ms(50), &pods)).is_none(),
        "not again so soon"
    );
    assert!(of_1(client.checks_due(ms(122), &pods)).is_none());
    assert!(
        of_1(client.checks_due(ms(123), &pods)).is_some(),
        "an interval after"
    );
}

/// Clients cut off from every pod: with every pod marked down by its checks, a request
/// waits — tried again each millisecond, left to `when_unreachable` at its deadline — rather
/// than sent at once to every pod in turn, each sending coming back unreachable.
#[test]
fn with_every_pod_out_of_reach_a_request_waits() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        for pod in pods {
            world.cut_clients_from(pod);
        }
        world.run_for(ms(1_000));
        // Each pod's requests unreachable until its first check is refused, about once a
        // client, with what each client had in flight to it.
        let bound = shape().clients * 3 * 10;
        assert!(
            world.stats.unreachable <= bound,
            "seed {seed}: {}",
            world.stats.unreachable
        );
    }
}
