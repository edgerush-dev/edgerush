//! The asynchronous mode on a pod of both modes: a data plane's batch in, each entry applied by
//! its key's owner, the answers out; entries for keys another pod owns forwarded to it, once,
//! and its answers relayed back.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Config, Key, Kind, NodeId, ShardId, Stamp, Time, ms, table};
use edgerush_cluster_asynchronous::{
    Answers, Batch, Entry, Turned, TurnedAway, Unserved, Value, Why,
};
use edgerush_cluster_modes as modes;
use edgerush_ratelimit::service::{Contract, Report};
use std::num::NonZeroU64;

/// The asynchronous mode's messages, over the store of both modes' values.
type Message = edgerush_cluster_asynchronous::Message<modes::Value>;

/// What a pod sends, as the asynchronous mode's messages.
type Outbox = Vec<(NodeId, Message)>;

/// A pod of both modes as these tests drive it: the asynchronous mode's messages in and out,
/// the store's among them, and its values as the mode's.
#[derive(Clone)]
struct Pod(modes::Pod);

impl Pod {
    fn new(id: NodeId, address: String, config: Config, peers: [NodeId; 2]) -> Self {
        Self(modes::Pod::new(id, address, config, peers))
    }

    fn store(&self) -> &modes::Store {
        self.0.store()
    }

    fn store_mut(&mut self) -> &mut modes::Store {
        self.0.store_mut()
    }

    fn set_life(&mut self, life: u64) {
        self.0.set_life(life);
    }

    fn turned_away(&self) -> TurnedAway {
        self.0.turned_away()
    }

    /// The asynchronous mode's value it holds for `key`.
    fn value(&self, key: Key) -> Option<Value> {
        self.0.value(key).map(|value| match value {
            modes::Value::Asynchronous(value) => value,
            other => panic!("{other:?} under an asynchronous key"),
        })
    }

    fn tick(&mut self, now: Time) -> Outbox {
        asynchronous(self.0.tick(now))
    }

    fn receive(&mut self, from: NodeId, message: impl Into<Message>, now: Time) -> Outbox {
        let message: Message = message.into();
        asynchronous(self.0.receive(from, message, now))
    }

    fn batch(&mut self, batch: Batch, now: Time) -> Answers {
        self.0.batch(batch, now)
    }
}

/// What a pod of both modes sent, as the asynchronous mode's messages: none of the other
/// mode's.
fn asynchronous(out: modes::Outbox) -> Outbox {
    out.into_iter()
        .map(|(to, message)| {
            let message = match message {
                modes::Message::Store(message) => Message::Store(message),
                modes::Message::Asynchronous(message) => message,
                modes::Message::Synchronous(message) => {
                    panic!("the synchronous mode's {message:?} to {to}")
                }
            };
            (to, message)
        })
        .collect()
}

const CYCLE: Time = ms(10);
const PLANE: NodeId = 1_000_000;

fn config() -> Config {
    let mut config = Config::defaults();
    config.shards = 4;
    config
}

/// 100 a second, a bucket of ten.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(10),
    }
}

/// The first table of pods 1, 2 and 3.
fn first() -> edgerush_cluster::Table {
    let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
    table::first(&members, 4, 2, 1)
}

/// Pod 1 holding the first table, not yet leased.
fn member() -> Pod {
    let mut pod = Pod::new(1, "pod-1:7000".to_owned(), config(), [2, 3]);
    let _ = pod.receive(2, StoreMessage::TableReply { table: first() }, ms(1));
    pod
}

/// Pod 1's peers heard at `now`, its lease held.
fn lease(pod: &mut Pod, now: Time) {
    for peer in [2, 3] {
        let heard = StoreMessage::Heartbeat {
            term: pod.store().term(),
            leader: Some(2),
            committed: pod.store().committed_stamp(),
            fresh: false,
            sent: now,
            wall: 0,
            hears: vec![(1, now), (2, now), (3, now)],
            leased: true,
        };
        let _ = pod.receive(peer, heard, now);
    }
}

/// The first key of the asynchronous mode's of a shard the first table gives `owner`.
fn key_of(owner: NodeId) -> Key {
    let table = first();
    let shard: ShardId = (0..4)
        .find(|shard| table.owner(*shard) == Some(owner))
        .unwrap();
    (0..)
        .map(|key| Kind::Asynchronous.key(key))
        .find(|key| shard_of(*key, 4) == shard)
        .unwrap()
}

fn hits(key: Key, sequence: u64, count: u64) -> Entry {
    Entry {
        key,
        contract: contract(),
        report: Report::Hits {
            sequence,
            count,
            seen: sequence - 1,
            age: 0,
        },
    }
}

fn poll(key: Key) -> Entry {
    Entry {
        key,
        contract: contract(),
        report: Report::Poll,
    }
}

fn batch(stamp: Stamp, made: u64, entries: Vec<Entry>) -> Batch {
    Batch {
        from: PLANE,
        stamp,
        made,
        cycle: CYCLE,
        entries,
    }
}

/// The owner applies an entry of a key it serves and answers it; one of a key whose owner
/// is another pod it answers elsewhere, applying nothing, and makes no key for it.
#[test]
fn the_owner_applies_its_keys_and_answers_the_rest_elsewhere() {
    let mut pod = member();
    lease(&mut pod, ms(2));
    let (own, theirs) = (key_of(1), key_of(2));
    let stamp = first().stamp();
    let answers = pod.batch(
        batch(stamp, 5, vec![hits(own, 1, 3), hits(theirs, 1, 4)]),
        ms(3),
    );
    assert_eq!(answers.entries.len(), 1);
    assert_eq!(answers.entries[0].key, own);
    assert_eq!(answers.entries[0].answered.applied, 1);
    assert!(answers.entries[0].answered.behind > 0, "charged");
    assert_eq!(
        answers.unserved,
        vec![Unserved {
            key: theirs,
            why: Why::Elsewhere
        }]
    );
    assert_eq!(
        pod.value(own).as_ref().map(|value| value.counted()),
        Some(3)
    );
    assert_eq!(pod.value(theirs), None);
    assert_eq!(answers.made, 5, "said back");
    assert_eq!(answers.table, None, "the data plane's table is the pod's");
    let turned = Turned {
        entries: 1,
        hits: 4,
    };
    assert_eq!(
        pod.turned_away(),
        TurnedAway {
            elsewhere: turned,
            not_serving: Turned::default()
        }
    );
}

/// An owner without its lease applies nothing and says it is not serving.
#[test]
fn an_owner_without_its_lease_answers_not_serving() {
    let mut pod = member();
    let own = key_of(1);
    let answers = pod.batch(batch(first().stamp(), 0, vec![hits(own, 1, 3)]), ms(3));
    assert!(answers.entries.is_empty());
    assert_eq!(
        answers.unserved,
        vec![Unserved {
            key: own,
            why: Why::NotServing
        }]
    );
    assert_eq!(pod.value(own), None);
    // A poll turned away too counts an entry and no hits.
    let _ = pod.batch(batch(first().stamp(), 0, vec![poll(own)]), ms(4));
    let turned = Turned {
        entries: 2,
        hits: 3,
    };
    assert_eq!(
        pod.turned_away(),
        TurnedAway {
            elsewhere: Turned::default(),
            not_serving: turned
        }
    );
}

/// Those admitting lately are counted by the cycle the batch carries, its data planes': a
/// data plane that admitted 200 ms before another counts among the others within thirty of
/// their 10 ms cycles, and not within thirty of 5 ms ones.
#[test]
fn those_admitting_lately_are_counted_by_the_batchs_cycle() {
    for (cycle, others) in [(CYCLE, 1), (CYCLE / 2, 0)] {
        let mut pod = member();
        lease(&mut pod, ms(2));
        let own = key_of(1);
        let stamp = first().stamp();
        let one = Batch {
            cycle,
            ..batch(stamp, 0, vec![hits(own, 1, 1)])
        };
        let _ = pod.batch(one, ms(3));
        let other = Batch {
            from: PLANE + 1,
            cycle,
            ..batch(stamp, 0, vec![hits(own, 1, 1)])
        };
        lease(&mut pod, ms(203));
        let answers = pod.batch(other, ms(203));
        assert_eq!(answers.entries[0].answered.others, others, "{cycle}");
    }
}

/// A hit is charged, and its admission recorded, by the pod's wall clock, not its own.
#[test]
fn hits_are_applied_by_the_wall_clock() {
    let mut pod = member();
    pod.store_mut().set_wall_offset(5_000_000_000);
    lease(&mut pod, ms(2));
    let own = key_of(1);
    let _ = pod.batch(batch(first().stamp(), 0, vec![hits(own, 1, 1)]), ms(3));
    let value = pod.value(own).unwrap();
    assert_eq!(value.admitted(PLANE), Some(5_000_000_000 + ms(3)));
}

/// A data plane whose table is older, or that has none, is answered with the pod's.
#[test]
fn a_data_plane_behind_is_answered_with_the_table() {
    let mut pod = member();
    lease(&mut pod, ms(2));
    let answers = pod.batch(batch((0, 0), 0, vec![hits(key_of(2), 1, 1)]), ms(3));
    assert_eq!(answers.table, Some(first()));
}

/// Answers carry its life and are numbered in the order it gave them, wrapping.
#[test]
fn answers_are_numbered_in_the_pods_life() {
    let mut pod = member();
    pod.set_life(7);
    let one = pod.batch(batch((0, 0), 0, Vec::new()), ms(3));
    let two = pod.batch(batch((0, 0), 0, Vec::new()), ms(4));
    assert_eq!((one.life, one.number), (7, 1));
    assert_eq!((two.life, two.number), (7, 2));
}

/// A batch received is answered to whoever sent it, as the store's messages are taken by
/// the store.
#[test]
fn a_batch_received_is_answered_to_its_sender() {
    let mut pod = member();
    lease(&mut pod, ms(2));
    let out = pod.receive(
        PLANE,
        Message::Batch(batch(first().stamp(), 0, vec![hits(key_of(1), 1, 1)])),
        ms(3),
    );
    assert!(
        matches!(&out[..], [(to, Message::Answers(answers))] if *to == PLANE && answers.entries.len() == 1),
        "{out:?}"
    );
}

/// A data plane's batch reaching a pod with entries for a key another pod owns: those
/// forwarded to the owner, once, with the batch's cycle, to be answered through this pod;
/// the rest answered here, the forwarded ones neither applied nor answered unserved.
#[test]
fn entries_for_another_owner_are_forwarded_to_it() {
    let mut pod = member();
    lease(&mut pod, ms(2));
    let (own, theirs) = (key_of(1), key_of(2));
    // The data planes' cycle, not the default: it goes on with the entries.
    let cycle = ms(7);
    let sent = Batch {
        cycle,
        ..batch(
            first().stamp(),
            5,
            vec![hits(own, 1, 1), hits(theirs, 1, 2)],
        )
    };
    let out = pod.receive(PLANE, Message::Batch(sent), ms(3));
    let [
        (to, Message::Answers(answers)),
        (
            owner,
            Message::Forwarded {
                client,
                via,
                batch: onward,
            },
        ),
    ] = &out[..]
    else {
        panic!("{out:?}");
    };
    assert_eq!(*to, PLANE);
    assert_eq!(answers.entries.len(), 1);
    assert_eq!(answers.entries[0].key, own);
    assert!(answers.unserved.is_empty(), "{answers:?}");
    assert_eq!((*owner, *client, *via), (2, PLANE, 1));
    assert_eq!(
        onward,
        &Batch {
            cycle,
            ..batch(first().stamp(), 5, vec![hits(theirs, 1, 2)])
        }
    );
    assert_eq!(pod.value(theirs), None, "nothing applied here");
}

/// Entries forwarded to a pod are answered back through the pod that forwarded them, and
/// not forwarded again: one it does not own either is answered unserved.
#[test]
fn entries_forwarded_are_answered_through_the_forwarder_and_go_no_further() {
    let mut pod = member();
    lease(&mut pod, ms(2));
    let (own, theirs) = (key_of(1), key_of(2));
    let forwarded = Message::Forwarded {
        client: PLANE,
        via: 3,
        batch: batch(
            first().stamp(),
            5,
            vec![hits(own, 1, 1), hits(theirs, 1, 2)],
        ),
    };
    let out = pod.receive(3, forwarded, ms(3));
    let [
        (
            to,
            Message::Relayed {
                client,
                owner,
                answers,
            },
        ),
    ] = &out[..]
    else {
        panic!("{out:?}");
    };
    assert_eq!((*to, *client, *owner), (3, PLANE, pod.store().id()));
    assert_eq!(answers.entries.len(), 1);
    assert_eq!(answers.entries[0].key, own);
    assert_eq!(
        answers.unserved,
        vec![Unserved {
            key: theirs,
            why: Why::Elsewhere
        }]
    );
    assert_eq!(answers.made, 5);
}

/// Answers relayed through a pod are passed on to the data plane they are for.
#[test]
fn answers_relayed_are_passed_on_to_their_data_plane() {
    let mut pod = member();
    let answers = pod.batch(batch((0, 0), 0, Vec::new()), ms(3));
    let relayed = Message::Relayed {
        client: PLANE,
        owner: 2,
        answers: answers.clone(),
    };
    let out = pod.receive(2, relayed.clone(), ms(4));
    assert_eq!(out, vec![(PLANE, relayed)]);
}

/// A poll answers with the bucket and keeps the key, but is no change to copy.
#[test]
fn a_poll_is_answered_and_marks_nothing_to_copy() {
    let mut pod = member();
    lease(&mut pod, ms(2));
    let own = key_of(1);
    let answers = pod.batch(batch(first().stamp(), 0, vec![poll(own)]), ms(3));
    assert_eq!(answers.entries.len(), 1);
    assert_eq!(answers.entries[0].answered.behind, 0, "a full bucket");
    assert_eq!(
        pod.value(own).as_ref().map(|value| value.counted()),
        Some(0)
    );
    let copied = |out: &Outbox| {
        out.iter().any(|(_, message)| {
            matches!(message, Message::Store(StoreMessage::Replicate { entries, .. })
                if entries.iter().any(|(key, _)| *key == own))
        })
    };
    // Ticks through a few of the store's copying cycles, from `from` until `to`.
    let ticks = |pod: &mut Pod, from: Time, to: Time| {
        let mut out = Outbox::new();
        for at in (from..to).step_by(5) {
            lease(pod, ms(at));
            out.extend(pod.tick(ms(at)));
        }
        out
    };
    assert!(!copied(&ticks(&mut pod, 4, 50)), "a poll copied to no one");
    let _ = pod.batch(batch(first().stamp(), 0, vec![hits(own, 1, 1)]), ms(50));
    assert!(
        copied(&ticks(&mut pod, 51, 100)),
        "hits copied to the replicas"
    );
}
