//! A pod of the asynchronous mode, over its store: a data plane's batch in, each entry applied
//! by its key's owner ([`apply`]) on `Node::update`, the answers out; entries for
//! keys another pod owns forwarded to it, once, and its answers relayed back, for a data plane
//! cut off from that owner. The store holds the keys, copies them to the shard's replicas,
//! hands them on and forgets them, in place of today's pod's copies to every pod and handovers
//! by the view.
//!
//! Forwarding is safe here as it is not in the synchronous mode: a data plane's batches are
//! numbered, and the owner applies each once, so one that reaches it both directly and
//! forwarded counts once.

use crate::message::{Answer, Answers, Batch, Entry, Message, Outbox, Unserved, Why};
use crate::{Store, Value, apply};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Config, NodeId, Time};
use edgerush_ratelimit::service::Report;
use std::collections::BTreeMap;

/// One pod of the asynchronous mode in one life: its store, and the batches it answers.
#[derive(Clone, Debug)]
pub struct Pod {
    node: Store,
    /// The time between a data plane's batches, which tells how lately a data plane must have
    /// admitted a key to count among those admitting it.
    cycle: Time,
    /// Its life, which its answers carry; 0 leaves them unnumbered.
    life: u64,
    /// The answers it has given in this life.
    answered: u64,
    /// What it has answered unserved in this life, by why.
    turned_away: TurnedAway,
}

/// The entries a pod has answered unserved, nothing of them applied, and the hits they
/// carried, by why (#159): for an overshoot after a pod paused or a table moved to be read off
/// the pod that turned the hits away. Those hits are not lost: the data plane sends them again
/// to the owner its table names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurnedAway {
    /// Of keys its table names another pod the owner of, or none.
    pub elsewhere: Turned,
    /// Of keys its table names it the owner of, while it is not serving.
    pub not_serving: Turned,
}

/// Entries turned away, and their hits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Turned {
    /// Entries.
    pub entries: u64,
    /// The hits they reported.
    pub hits: u64,
}

impl Pod {
    /// Pod `id`, reached at `address`, new, knowing how to reach `peers`, its data planes
    /// sending every `cycle` nanoseconds.
    #[must_use]
    pub fn new(
        id: NodeId,
        address: String,
        config: Config,
        peers: impl IntoIterator<Item = NodeId>,
        cycle: Time,
    ) -> Self {
        Self {
            node: Store::new(id, address, config, peers),
            cycle: cycle.max(1),
            life: 0,
            answered: 0,
            turned_away: TurnedAway::default(),
        }
    }

    /// Its life, drawn at random when it starts and never 0, which its answers carry: a data
    /// plane takes an answer's buckets only if it is the pod's latest, and a restart starts the
    /// numbering again. 0, the default, leaves its answers unnumbered, each taken as it comes.
    pub const fn set_life(&mut self, life: u64) {
        self.life = life;
    }

    /// What it has answered unserved in this life, by why.
    #[must_use]
    pub const fn turned_away(&self) -> TurnedAway {
        self.turned_away
    }

    /// Its store.
    #[must_use]
    pub const fn store(&self) -> &Store {
        &self.node
    }

    /// Its store, to change: told to drain, its wall clock set, keys loaded.
    pub const fn store_mut(&mut self) -> &mut Store {
        &mut self.node
    }

    /// The value it holds for `key`, owned or a copy.
    #[must_use]
    pub fn value(&self, key: u64) -> Option<Value> {
        self.node.value(key)
    }

    /// Its timer at `now`: its store's, the mode keeping no timer of its own.
    pub fn tick(&mut self, now: Time) -> Outbox {
        from_store(self.node.tick(now))
    }

    /// `message` from `from` at `now`: a batch answered to `from`, its entries for keys
    /// another pod owns forwarded to that owner ([`forward`](Self::forward)); entries
    /// forwarded to it answered back through the pod that forwarded them, and answers relayed
    /// through it passed on to their data plane; the store's taken by its store. Answers are
    /// not a pod's to take.
    pub fn receive(&mut self, from: NodeId, message: impl Into<Message>, now: Time) -> Outbox {
        match message.into() {
            Message::Batch(batch) => self.forward(from, batch, now),
            Message::Forwarded { client, via, batch } => {
                let answers = self.batch(batch, now);
                vec![(via, Message::Relayed { client, answers })]
            }
            Message::Relayed { client, answers } => {
                vec![(client, Message::Relayed { client, answers })]
            }
            Message::Store(message) => from_store(self.node.receive(from, message, now)),
            Message::Answers(_) => Outbox::new(),
        }
    }

    /// A data plane's batch reaches it at `now`: each entry of a key it serves applied, in
    /// order, and answered; each of one it does not serve answered so, nothing of it applied,
    /// for the data plane to send again to the owner the table names; its table with them if
    /// later than the batch's. What changed goes to the shard's replicas with the store's next
    /// copy.
    pub fn batch(&mut self, batch: Batch, now: Time) -> Answers {
        let Batch {
            from,
            stamp,
            made,
            entries,
        } = batch;
        let cycle = self.cycle;
        let mut answers = Vec::new();
        let mut unserved = Vec::new();
        for entry in entries {
            let applied = self.node.update(entry.key, now, |kept, wall| {
                apply(kept, from, entry.contract, entry.report, cycle, wall)
            });
            match applied {
                Some(answered) => answers.push(Answer {
                    key: entry.key,
                    answered,
                }),
                None => {
                    let why = self.why_not(entry.key);
                    let turned = match why {
                        Why::Elsewhere => &mut self.turned_away.elsewhere,
                        Why::NotServing => &mut self.turned_away.not_serving,
                    };
                    turned.entries = turned.entries.saturating_add(1);
                    if let Report::Hits { count, .. } = entry.report {
                        turned.hits = turned.hits.saturating_add(count);
                    }
                    unserved.push(Unserved {
                        key: entry.key,
                        why,
                    });
                }
            }
        }
        let table = self
            .node
            .committed()
            .filter(|table| table.stamp() > stamp)
            .cloned();
        self.answered = self.answered.wrapping_add(1);
        Answers {
            life: self.life,
            number: self.answered,
            made,
            entries: answers,
            unserved,
            table,
        }
    }

    /// Data plane `from`'s batch reaching it at `now`: the entries of keys its table names
    /// another pod the owner of forwarded to that owner, once, to be answered through it; the
    /// rest answered here, as [`batch`](Self::batch) answers them.
    pub fn forward(&mut self, from: NodeId, batch: Batch, now: Time) -> Outbox {
        let Batch {
            from: plane,
            stamp,
            made,
            entries,
        } = batch;
        let shards = self.node.config().shards;
        let me = self.node.id();
        let mut kept = Vec::new();
        let mut onward: BTreeMap<NodeId, Vec<Entry>> = BTreeMap::new();
        for entry in entries {
            let owner = self
                .node
                .committed()
                .and_then(|table| table.owner(shard_of(entry.key, shards)));
            match owner {
                Some(owner) if owner != me => onward.entry(owner).or_default().push(entry),
                _ => kept.push(entry),
            }
        }
        let answers = self.batch(
            Batch {
                from: plane,
                stamp,
                made,
                entries: kept,
            },
            now,
        );
        let mut out = vec![(from, Message::Answers(answers))];
        for (owner, entries) in onward {
            out.push((
                owner,
                Message::Forwarded {
                    client: from,
                    via: me,
                    batch: Batch {
                        from: plane,
                        stamp,
                        made,
                        entries,
                    },
                },
            ));
        }
        out
    }

    /// Why it does not serve `key`: its table names it the owner, but it is not serving; or
    /// names another, or none.
    fn why_not(&self, key: u64) -> Why {
        let shard = shard_of(key, self.node.config().shards);
        let owner = self.node.committed().and_then(|table| table.owner(shard));
        if owner == Some(self.node.id()) {
            Why::NotServing
        } else {
            Why::Elsewhere
        }
    }
}

/// The store's messages as the mode's.
fn from_store(out: edgerush_cluster::Outbox<Value>) -> Outbox {
    out.into_iter()
        .map(|(to, message)| (to, Message::Store(message)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::Message as StoreMessage;
    use edgerush_cluster::{ShardId, Stamp, ms, table};
    use edgerush_ratelimit::service::{Contract, Report};
    use std::num::NonZeroU64;

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
        let mut pod = Pod::new(1, "pod-1:7000".to_owned(), config(), [2, 3], CYCLE);
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

    /// The first key of a shard the first table gives `owner`.
    fn key_of(owner: NodeId) -> u64 {
        let table = first();
        let shard: ShardId = (0..4)
            .find(|shard| table.owner(*shard) == Some(owner))
            .unwrap();
        (0..).find(|key| shard_of(*key, 4) == shard).unwrap()
    }

    fn hits(key: u64, sequence: u64, count: u64) -> Entry {
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

    fn poll(key: u64) -> Entry {
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
        assert_eq!(pod.value(own).as_ref().map(Value::counted), Some(3));
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

    /// Those admitting lately are counted by the pod's cycle: a data plane that admitted
    /// 200 ms before another, within thirty of its 10 ms cycles, counts among the others.
    #[test]
    fn those_admitting_lately_are_counted_by_the_pods_cycle() {
        let mut pod = member();
        lease(&mut pod, ms(2));
        let own = key_of(1);
        let stamp = first().stamp();
        let _ = pod.batch(batch(stamp, 0, vec![hits(own, 1, 1)]), ms(3));
        let other = Batch {
            from: PLANE + 1,
            ..batch(stamp, 0, vec![hits(own, 1, 1)])
        };
        lease(&mut pod, ms(203));
        let answers = pod.batch(other, ms(203));
        assert_eq!(answers.entries[0].answered.others, 1);
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
    /// forwarded to the owner, once, to be answered through this pod; the rest answered here,
    /// the forwarded ones neither applied nor answered unserved.
    #[test]
    fn entries_for_another_owner_are_forwarded_to_it() {
        let mut pod = member();
        lease(&mut pod, ms(2));
        let (own, theirs) = (key_of(1), key_of(2));
        let sent = batch(
            first().stamp(),
            5,
            vec![hits(own, 1, 1), hits(theirs, 1, 2)],
        );
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
        assert_eq!(onward, &batch(first().stamp(), 5, vec![hits(theirs, 1, 2)]));
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
        let [(to, Message::Relayed { client, answers })] = &out[..] else {
            panic!("{out:?}");
        };
        assert_eq!((*to, *client), (3, PLANE));
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
        assert_eq!(pod.value(own).as_ref().map(Value::counted), Some(0));
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
}
