//! The asynchronous owner's applying run on the store of both modes' values: three pods formed
//! in-process, every message delivered at once, the key's owner applying batches on `Node::update` and its
//! replicas holding what it holds once its copies reach them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::table::{self, shard_of};
use edgerush_cluster::{Config, Kept, Kind, Message, NodeId, Time, ms};
use edgerush_cluster_asynchronous::{self as asynchronous, apply};
use edgerush_cluster_modes::{Store, Value};
use edgerush_ratelimit::service::{Contract, Report};
use std::collections::VecDeque;
use std::num::NonZeroU64;

const SHARDS: u16 = 4;
const CYCLE: Time = ms(10);

/// 100 a second, a bucket of ten.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(10),
    }
}

/// The asynchronous mode's value of a value of both modes'.
fn mine(value: &Value) -> &asynchronous::Value {
    match value {
        Value::Asynchronous(value) => value,
        other => panic!("{other:?} under an asynchronous key"),
    }
}

/// Pods 1, 2 and 3 given their first table, as the control plane forms them, and what they
/// send then delivered: the first copies among it.
fn pods() -> Vec<Store> {
    let mut config = Config::defaults();
    config.shards = SHARDS;
    let ids: [NodeId; 3] = [1, 2, 3];
    let members = ids.map(|id| (id, format!("pod-{id}:7000"))).into();
    let first = table::first(&members, SHARDS, 2, 1);
    let mut pods: Vec<Store> = ids
        .iter()
        .map(|&id| {
            let peers = ids.iter().copied().filter(|peer| *peer != id);
            Store::new(id, format!("pod-{id}:7000"), config, peers)
        })
        .collect();
    let mut going = VecDeque::new();
    for pod in &mut pods {
        let from = pod.id();
        let reply = Message::TableReply {
            table: first.clone(),
        };
        going.extend(
            pod.receive(1, reply, ms(1))
                .into_iter()
                .map(|(to, m)| (from, to, m)),
        );
    }
    deliver(&mut pods, going, ms(1));
    pods
}

/// Each pod ticks at `now`, and everything sent is delivered at once, until nothing more is.
fn step(pods: &mut [Store], now: Time) {
    let mut going: VecDeque<(NodeId, NodeId, Message<Value>)> = VecDeque::new();
    for pod in pods.iter_mut() {
        let from = pod.id();
        going.extend(pod.tick(now).into_iter().map(|(to, m)| (from, to, m)));
    }
    deliver(pods, going, now);
}

/// Delivers what is `going`, and what each delivery sends, at `now`, until nothing more is.
fn deliver(pods: &mut [Store], mut going: VecDeque<(NodeId, NodeId, Message<Value>)>, now: Time) {
    while let Some((from, to, message)) = going.pop_front() {
        let Some(pod) = pods.iter_mut().find(|pod| pod.id() == to) else {
            continue;
        };
        going.extend(
            pod.receive(from, message, now)
                .into_iter()
                .map(|(next, m)| (to, next, m)),
        );
    }
}

/// The owner applies a data plane's batches to a key on `Node::update`; its replicas, copies
/// delivered, hold the same value, and a replica taking over goes on from it: a batch it has
/// applied, sent again, is not counted twice.
#[test]
fn replicas_hold_the_owners_value_and_go_on_from_it() {
    let mut pods = pods();
    let mut now = ms(1);
    let key = Kind::Asynchronous.key(7);
    let shard = shard_of(key, SHARDS);
    let serving = |pods: &[Store], now| pods.iter().position(|pod| pod.serves(shard, now));
    let owner = loop {
        now += ms(5);
        step(&mut pods, now);
        if let Some(owner) = serving(&pods, now) {
            break owner;
        }
        assert!(now < ms(5_000), "no pod came to serve the key's shard");
    };
    for (sequence, count) in [(1, 3), (2, 4), (3, 2)] {
        let report = Report::Hits {
            sequence,
            count,
            seen: sequence - 1,
            age: 0,
        };
        let applied = pods[owner].update(key, now, |kept, wall| {
            apply(kept, 100, contract(), report, CYCLE, wall)
        });
        assert_eq!(applied.map(|answer| answer.applied), Some(sequence));
        now += CYCLE;
        step(&mut pods, now);
    }
    for _ in 0..10 {
        now += ms(5);
        step(&mut pods, now);
    }
    let held = pods[owner].value(key).expect("the owner holds the key");
    assert_eq!(mine(&held).counted(), 9);
    let replicas: Vec<usize> = pods[owner]
        .committed()
        .and_then(|table| table.shards.get(usize::from(shard)))
        .map(|entry| entry.replicas.clone())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|id| pods.iter().position(|pod| pod.id() == id))
        .collect();
    assert_eq!(replicas.len(), 2);
    for &replica in &replicas {
        assert_eq!(
            pods[replica].value(key).as_ref(),
            Some(&held),
            "pod {replica}"
        );
    }

    // A replica's copy, applied as its owner would: the last batch sent again is answered
    // as applied and counted once; the next is counted.
    let mut copy = Kept::new(pods[replicas[0]].value(key).unwrap_or_default());
    let again = Report::Hits {
        sequence: 3,
        count: 2,
        seen: 2,
        age: 0,
    };
    let (answer, _) = apply(&mut copy, 100, contract(), again, CYCLE, now);
    assert_eq!((answer.applied, mine(&copy.value).counted()), (3, 9));
    let next = Report::Hits {
        sequence: 4,
        count: 1,
        seen: 3,
        age: 0,
    };
    let (answer, _) = apply(&mut copy, 100, contract(), next, CYCLE, now);
    assert_eq!((answer.applied, mine(&copy.value).counted()), (4, 10));
}
