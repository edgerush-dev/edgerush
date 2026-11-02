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
/// times at most however late its answers.
#[test]
fn a_give_back_is_sent_three_times_at_most() {
    use edgerush_cluster_sim::world::Link;
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
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

/// Clients cut off from a pod, both ways: a client whose request could not reach it asks
/// nothing of it while its stream is down, by any path — sent there again and again, each
/// coming back unreachable, it would flood the others with its requests sent another way.
#[test]
fn a_pod_a_client_cannot_reach_is_left_alone_while_its_stream_is_down() {
    for seed in SEEDS {
        let (mut world, _) = scenario::cluster(seed, shape());
        let pod = world.leader().unwrap();
        world.cut_clients_from(pod);
        world.run_for(ms(3_000));
        // About once a client each time its stream is taken to be down again, 500 ms on, and
        // whatever each had in flight to it then.
        let bound = shape().clients * 10;
        assert!(
            world.stats.unreachable <= bound,
            "seed {seed}: {}",
            world.stats.unreachable
        );
    }
}

/// A data plane whose stream to a pod is down — a request sent there could not reach it — and
/// a "counted" for an earlier request coming from that pod late: the breaker starts again, but
/// the stream is still down, and the next request under the pod's keys goes elsewhere.
#[test]
fn a_late_answer_does_not_open_a_stream_that_is_down() {
    use edgerush_cluster::table;
    use edgerush_cluster_sim::Rng;
    use edgerush_cluster_sim::client::Client;
    use edgerush_cluster_synchronous::{Answer, Message};
    let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
    let first = table::first(&members, 16, 2, 1);
    // A key of pod 1's, asked three times in turn.
    let Some(key) = (0..).find(|key| first.owner(table::shard_of(*key, 16)) == Some(1)) else {
        panic!("pod 1 owns nothing");
    };
    let mut client = Client::new(1_000_000, vec![key], ms(5), ms(20), 16);
    client.learn(first);
    let mut rng = Rng::new(1);
    let pods = [1, 2, 3];
    let id_of = |send: &(NodeId, Message)| match send.1 {
        Message::Request { id, .. } => id,
        _ => u64::MAX,
    };
    let early = client.send(ms(1), &pods, &mut rng).unwrap();
    let cut = client.send(ms(2), &pods, &mut rng).unwrap();
    assert_eq!((early.0, cut.0), (1, 1), "sent to the owner");
    let _ = client.unreachable(id_of(&cut), 1, ms(3), &pods, &mut rng);
    let _ = client.answered(id_of(&early), Answer::Counted(1), ms(4), &pods, &mut rng);
    let next = client.send(ms(5), &pods, &mut rng).unwrap();
    assert_ne!(next.0, 1, "sent to a pod its stream to is down");
}

/// Clients cut off from every pod: with no pod to send to, a request waits — tried again as
/// a stream comes back, left to `when_unreachable` at its deadline — rather than sent at once
/// to every pod in turn, each sending coming back unreachable.
#[test]
fn with_every_pod_out_of_reach_a_request_waits() {
    for seed in SEEDS {
        let (mut world, pods) = scenario::cluster(seed, shape());
        for pod in pods {
            world.cut_clients_from(pod);
        }
        world.run_for(ms(1_000));
        // Each pod found down about once a client, and again 500 ms on, with what each
        // client had in flight to it.
        let bound = shape().clients * 3 * 10;
        assert!(
            world.stats.unreachable <= bound,
            "seed {seed}: {}",
            world.stats.unreachable
        );
    }
}
