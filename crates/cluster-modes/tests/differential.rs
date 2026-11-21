//! A pod of both modes against each mode's own pod (G1, docs/06 §4): the same messages to each,
//! at the same times, give the same answers, and leave the same values and the same bytes,
//! whatever a mode's requests or batches are. And a data plane's keys of the other mode's are
//! never served, nor held.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Config, Key, Message as StoreMessage, NodeId, Time, ms, table};
use edgerush_cluster_asynchronous::{self as asynchronous, Batch, Entry};
use edgerush_cluster_modes::{Kind, Message, Pod, Value};
use edgerush_cluster_synchronous::{self as synchronous, Answer, Rule};
use edgerush_ratelimit::service::{Contract, Report};
use proptest::prelude::*;
use std::num::NonZeroU64;

const CYCLE: Time = ms(10);
const CLIENT: NodeId = 1 << 63;

fn config() -> Config {
    let mut config = Config::defaults();
    config.shards = 4;
    config
}

/// The first table of a service of one pod, which owns every key.
fn first() -> edgerush_cluster::Table {
    let members = [(1, "pod-1:7000".to_owned())].into_iter().collect();
    table::first(&members, config().shards, 0, 1)
}

/// Each pod the only one of its service, its first table taken at 1 ms.
fn both() -> Pod {
    let mut pod = Pod::new(1, "pod-1:7000".to_owned(), config(), [], CYCLE);
    let _ = pod.receive(1, StoreMessage::TableReply { table: first() }, ms(1));
    let _ = pod.tick(ms(1));
    pod
}

fn synchronous_alone() -> synchronous::Pod {
    let mut pod = synchronous::Pod::new(1, "pod-1:7000".to_owned(), config(), []);
    let _ = pod.receive(1, StoreMessage::TableReply { table: first() }, ms(1));
    let _ = pod.tick(ms(1));
    pod
}

fn asynchronous_alone() -> asynchronous::Pod {
    let mut pod = asynchronous::Pod::new(1, "pod-1:7000".to_owned(), config(), [], CYCLE);
    let _ = pod.receive(1, StoreMessage::TableReply { table: first() }, ms(1));
    let _ = pod.tick(ms(1));
    pod
}

/// One synchronous request: under which of eight keys, what rule, and whether it gives back
/// the receipt its key's last admission was answered with.
#[derive(Clone, Copy, Debug)]
struct Asked {
    key: u8,
    rule: u8,
    give_back: bool,
    after: Time,
}

fn rule(which: u8) -> Option<Rule> {
    match which % 4 {
        0 => None,
        1 => Some(Rule::Window {
            per: 3,
            window: ms(100),
        }),
        2 => Some(Rule::Rolling {
            per: 3,
            window: ms(100),
            slots: 10,
        }),
        _ => Some(Rule::Bucket {
            per: 3,
            period: ms(100),
            burst: 2,
        }),
    }
}

/// The answers among `out`, by client and number, in order.
fn replies<M>(
    out: &[(NodeId, M)],
    reply: impl Fn(&M) -> Option<(NodeId, u64, Answer)>,
) -> Vec<(NodeId, u64, Answer)> {
    out.iter()
        .filter_map(|(_, message)| reply(message))
        .collect()
}

fn synchronous_reply(message: &synchronous::Message) -> Option<(NodeId, u64, Answer)> {
    match message {
        synchronous::Message::Reply { client, id, answer } => Some((*client, *id, answer.clone())),
        _ => None,
    }
}

fn both_reply(message: &Message) -> Option<(NodeId, u64, Answer)> {
    match message {
        Message::Synchronous(synchronous::Message::Reply { client, id, answer }) => {
            Some((*client, *id, answer.clone()))
        }
        _ => None,
    }
}

fn request<V>(
    id: u64,
    key: Key,
    rule: Option<Rule>,
    give_back: bool,
    receipt: u64,
) -> synchronous::Message<V> {
    synchronous::Message::Request {
        client: CLIENT,
        id,
        key,
        stamp: (1, 1),
        hops: 0,
        via: None,
        give_back,
        rule,
        receipt,
    }
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

/// One asynchronous batch entry: which of eight keys, from which of three data planes, hits
/// or a poll.
#[derive(Clone, Copy, Debug)]
struct Reported {
    key: u8,
    plane: u8,
    hits: u8,
    after: Time,
}

proptest! {
    /// The synchronous mode's requests, any of its rules, give-backs among them, at any times:
    /// a pod of both modes answers each as the mode's own pod does, its ticks send what the
    /// mode's pod's do, and it holds the same value under each key, of the same bytes.
    #[test]
    fn the_synchronous_mode_on_a_pod_of_both_is_its_own(
        asked in proptest::collection::vec(
            (0..8_u8, any::<u8>(), any::<bool>(), 0..ms(30)).prop_map(|(key, rule, give_back, after)| {
                Asked { key, rule, give_back, after }
            }),
            1..60,
        ),
    ) {
        let (mut own, mut both) = (synchronous_alone(), both());
        let mut now = ms(2);
        let mut receipts = [0_u64; 8];
        for (id, ask) in (1..).zip(asked) {
            now += ask.after;
            let key = Kind::Synchronous.key(Key::from(ask.key) * 7_919);
            let rule = rule(ask.rule);
            let receipt = receipts[usize::from(ask.key)];
            let mine = own.receive(CLIENT, request(id, key, rule, ask.give_back, receipt), now);
            let theirs = both.receive(CLIENT, request::<Value>(id, key, rule, ask.give_back, receipt), now);
            let answers = replies(&mine, synchronous_reply);
            prop_assert_eq!(&answers, &replies(&theirs, both_reply));
            if let Some((_, _, Answer::Admitted(receipt))) = answers.first() {
                receipts[usize::from(ask.key)] = *receipt;
            }
            let ticked = own.tick(now);
            prop_assert_eq!(replies(&ticked, synchronous_reply), replies(&both.tick(now), both_reply));
            prop_assert_eq!(
                own.store().value(key).map(Value::Synchronous),
                both.value(key)
            );
        }
        prop_assert_eq!(own.store().key_bytes(), both.store().key_bytes());
        prop_assert_eq!(own.store().key_count(), both.store().key_count());
        prop_assert_eq!(both.store().keys_by_kind()[1], (0, 0));
        prop_assert_eq!(both.wrong_kind(), 0);
    }

    /// The asynchronous mode's batches, hits and polls from three data planes, at any times: a
    /// pod of both modes answers each as the mode's own pod does, and holds the same value
    /// under each key, of the same bytes.
    #[test]
    fn the_asynchronous_mode_on_a_pod_of_both_is_its_own(
        reported in proptest::collection::vec(
            (0..8_u8, 0..3_u8, 0..4_u8, 0..ms(30)).prop_map(|(key, plane, hits, after)| {
                Reported { key, plane, hits, after }
            }),
            1..60,
        ),
    ) {
        let (mut own, mut both) = (asynchronous_alone(), both());
        let mut now = ms(2);
        let mut sequences = [0_u64; 3];
        for report in reported {
            now += report.after;
            let key = Kind::Asynchronous.key(Key::from(report.key) * 7_919);
            let plane = usize::from(report.plane);
            let report_of = if report.hits == 0 {
                Report::Poll
            } else {
                sequences[plane] += 1;
                Report::Hits {
                    sequence: sequences[plane],
                    count: u64::from(report.hits),
                    seen: sequences[plane] - 1,
                    age: 0,
                }
            };
            let batch = Batch {
                from: 100 + u64::from(report.plane),
                stamp: (1, 1),
                made: now,
                entries: vec![Entry { key, contract: contract(), report: report_of }],
            };
            let from = CLIENT + u64::from(report.plane);
            let mine = own.receive(from, asynchronous::Message::Batch(batch.clone()), now);
            let theirs = both.receive(from, Message::Asynchronous(asynchronous::Message::Batch(batch)), now);
            let answers: Vec<_> = mine.iter().filter_map(|(to, message)| match message {
                asynchronous::Message::Answers(answers) => Some((*to, answers.clone())),
                _ => None,
            }).collect();
            let answered: Vec<_> = theirs.iter().filter_map(|(to, message)| match message {
                Message::Asynchronous(asynchronous::Message::Answers(answers)) => Some((*to, answers.clone())),
                _ => None,
            }).collect();
            prop_assert_eq!(answers, answered);
            let _ = own.tick(now);
            let _ = both.tick(now);
            prop_assert_eq!(own.value(key).map(Value::Asynchronous), both.value(key));
        }
        prop_assert_eq!(own.store().key_bytes(), both.store().key_bytes());
        prop_assert_eq!(own.store().key_count(), both.store().key_count());
        prop_assert_eq!(both.store().keys_by_kind()[0], (0, 0));
        prop_assert_eq!(both.wrong_kind(), 0);
    }
}

/// A data plane's request under a key of the other mode's is not served, and counted; nothing
/// is made under it. Each mode's keys of its own beside it are served.
#[test]
fn a_key_of_the_other_modes_is_never_served_nor_held() {
    let mut pod = both();
    let now = ms(2);
    let theirs = Kind::Asynchronous.key(5);
    let out = pod.receive(CLIENT, request::<Value>(1, theirs, None, false, 0), now);
    assert_eq!(replies(&out, both_reply), [(CLIENT, 1, Answer::NotServing)]);
    let gather = synchronous::Message::Gather {
        client: CLIENT,
        id: 2,
        keys: vec![Kind::Synchronous.key(6), theirs],
    };
    let out = pod.receive(CLIENT, Message::Synchronous(gather), now);
    assert_eq!(replies(&out, both_reply), [(CLIENT, 2, Answer::NotServing)]);
    let mine = Kind::Synchronous.key(7);
    let out = pod.receive(CLIENT, request::<Value>(3, mine, None, false, 0), now);
    assert_eq!(replies(&out, both_reply), [(CLIENT, 3, Answer::Counted(1))]);

    let entry = |key| Entry {
        key,
        contract: contract(),
        report: Report::Hits {
            sequence: 1,
            count: 1,
            seen: 0,
            age: 0,
        },
    };
    let batch = Batch {
        from: 100,
        stamp: (1, 1),
        made: now,
        entries: vec![entry(Kind::Synchronous.key(8)), entry(theirs)],
    };
    let out = pod.receive(
        CLIENT,
        Message::Asynchronous(asynchronous::Message::Batch(batch)),
        now,
    );
    let [(to, Message::Asynchronous(asynchronous::Message::Answers(answers)))] = &out[..] else {
        panic!("one batch, one answer: {out:?}");
    };
    assert_eq!(*to, CLIENT);
    assert_eq!(
        answers
            .entries
            .iter()
            .map(|answer| answer.key)
            .collect::<Vec<_>>(),
        [theirs]
    );
    assert_eq!(
        answers.unserved,
        [asynchronous::Unserved {
            key: Kind::Synchronous.key(8),
            why: asynchronous::Why::NotServing,
        }]
    );
    assert_eq!(pod.wrong_kind(), 3);
    assert_eq!(
        pod.value(Kind::Synchronous.key(6)),
        None,
        "a gather refused counts nothing"
    );
    assert_eq!(pod.value(Kind::Synchronous.key(8)), None);
    assert!(matches!(pod.value(theirs), Some(Value::Asynchronous(_))));
    assert!(matches!(pod.value(mine), Some(Value::Synchronous(_))));
    assert_eq!(pod.store().keys_by_kind().map(|(count, _)| count), [1, 1]);
}
