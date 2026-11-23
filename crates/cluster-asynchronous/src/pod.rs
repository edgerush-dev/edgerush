//! The asynchronous mode's part of a pod, over its store ([`Applier`]): a data plane's batch
//! in, each entry applied by its key's owner ([`apply`]) on `Node::update`, the answers out;
//! entries for keys another pod owns forwarded to it, once, and its answers relayed back, for
//! a data plane cut off from that owner. The store holds the keys, copies them to the shard's
//! replicas, hands them on and forgets them.
//!
//! Forwarding is safe here as it is not in the synchronous mode: a data plane's batches are
//! numbered, and the owner applies each once, so one that reaches it both directly and
//! forwarded counts once.

use crate::message::{Answer, Answers, Batch, Entry, Message, Outbox, Unserved, Why};
use crate::{Value, apply};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Held, Node};
use edgerush_cluster::{Key, NodeId, Time};
use edgerush_ratelimit::service::Report;
use std::collections::BTreeMap;

/// A value the store may hold under a key that can hold this mode's: what the mode applies a
/// data plane's report to, changed in place.
pub trait Holds: Held {
    /// The mode's value it holds, to change: a key new to the store is made afresh as this
    /// mode's.
    fn asynchronous_mut(&mut self) -> &mut Value;
}

impl Holds for Value {
    fn asynchronous_mut(&mut self) -> &mut Value {
        self
    }
}

/// The mode's part of a pod, over a store it is handed each time: the batches it answers,
/// numbered by its life. Kept apart from the store so that one store can serve this mode
/// beside another.
#[derive(Clone, Debug, Default)]
pub struct Applier {
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

impl Applier {
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

    /// `message` from `from` at `now`, over `node`: a batch answered to `from`, its entries for
    /// keys another pod owns forwarded to that owner ([`forward`](Self::forward)); entries
    /// forwarded to it answered back through the pod that forwarded them, and answers relayed
    /// through it passed on to their data plane; the store's taken by the store. Answers are
    /// not a pod's to take.
    pub fn receive<V: Holds>(
        &mut self,
        node: &mut Node<V>,
        from: NodeId,
        message: Message<V>,
        now: Time,
    ) -> Outbox<V> {
        match message {
            Message::Batch(batch) => self.forward(node, from, batch, now),
            Message::Forwarded { client, via, batch } => {
                let answers = self.batch(node, batch, now);
                let owner = node.id();
                vec![(
                    via,
                    Message::Relayed {
                        client,
                        owner,
                        answers,
                    },
                )]
            }
            Message::Relayed {
                client,
                owner,
                answers,
            } => vec![(
                client,
                Message::Relayed {
                    client,
                    owner,
                    answers,
                },
            )],
            Message::Store(message) => from_store(node.receive(from, message, now)),
            Message::Answers(_) => Outbox::new(),
        }
    }

    /// A data plane's batch reaches it at `now`: each entry of a key it serves applied, in
    /// order, and answered; each of one it does not serve answered so, nothing of it applied,
    /// for the data plane to send again to the owner the table names; its table with them if
    /// later than the batch's. What changed goes to the shard's replicas with the store's next
    /// copy.
    pub fn batch<V: Holds>(&mut self, node: &mut Node<V>, batch: Batch, now: Time) -> Answers {
        let Batch {
            from,
            stamp,
            made,
            cycle,
            entries,
        } = batch;
        let mut answers = Vec::new();
        let mut unserved = Vec::new();
        for entry in entries {
            let applied = node.update(entry.key, now, |kept, wall| {
                apply(kept, from, entry.contract, entry.report, cycle, wall)
            });
            match applied {
                Some(answered) => answers.push(Answer {
                    key: entry.key,
                    answered,
                }),
                None => {
                    let why = why_not(node, entry.key);
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
        let table = node
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
    pub fn forward<V: Holds>(
        &mut self,
        node: &mut Node<V>,
        from: NodeId,
        batch: Batch,
        now: Time,
    ) -> Outbox<V> {
        let Batch {
            from: plane,
            stamp,
            made,
            cycle,
            entries,
        } = batch;
        let shards = node.config().shards;
        let me = node.id();
        let mut kept = Vec::new();
        let mut onward: BTreeMap<NodeId, Vec<Entry>> = BTreeMap::new();
        for entry in entries {
            let owner = node
                .committed()
                .and_then(|table| table.owner(shard_of(entry.key, shards)));
            match owner {
                Some(owner) if owner != me => onward.entry(owner).or_default().push(entry),
                _ => kept.push(entry),
            }
        }
        let answers = self.batch(
            node,
            Batch {
                from: plane,
                stamp,
                made,
                cycle,
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
                        cycle,
                        entries,
                    },
                },
            ));
        }
        out
    }
}

/// Why `node` does not serve `key`: its table names it the owner, but it is not serving; or
/// names another, or none.
fn why_not<V: Held>(node: &Node<V>, key: Key) -> Why {
    let shard = shard_of(key, node.config().shards);
    let owner = node.committed().and_then(|table| table.owner(shard));
    if owner == Some(node.id()) {
        Why::NotServing
    } else {
        Why::Elsewhere
    }
}

/// The store's messages as the mode's.
fn from_store<V>(out: edgerush_cluster::Outbox<V>) -> Outbox<V> {
    out.into_iter()
        .map(|(to, message)| (to, Message::Store(message)))
        .collect()
}
