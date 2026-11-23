//! The synchronous mode's part of a pod, over its store ([`Decider`]): each request decided by
//! its key's owner under its rule, a refused request's counted parts given back, and requests
//! under several keys gathered by the pod they reach. The store holds the keys, copies them
//! and hands them on; this decides what they hold.

use crate::message::{Answer, Message, Outbox};
use crate::{Rule, Value};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Changed, Held, Kept, Key, Node, NodeId, Stamp, Time};
use std::collections::BTreeMap;

/// A request under several keys this pod gathers: who asked, and each part still to count,
/// when last asked of its owner.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Gathering {
    started: Time,
    /// Each key not yet counted, with when its part was last sent and its number.
    waiting: BTreeMap<Key, (Time, u64)>,
    /// Keys counted: given back if the request is reported not counted.
    counted: Vec<Key>,
}

/// A part of a gathered request asked of a key's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Part {
    /// The request, by client and the client's number.
    request: (NodeId, u64),
    key: Key,
    at: Time,
}

/// How a request asks: to count one or give one back, under what rule, and for a give-back
/// under a rule the receipt its charge was answered with.
#[derive(Clone, Copy, Debug)]
struct Ask {
    give_back: bool,
    rule: Option<Rule>,
    receipt: u64,
}

/// A value the store may hold under a key that can hold this mode's: what the mode decides a
/// request by, read and changed in place.
pub trait Holds: Held {
    /// The mode's value it holds, to change: a key new to the store is made afresh as this
    /// mode's.
    fn synchronous_mut(&mut self) -> &mut Value;
}

impl Holds for Value {
    fn synchronous_mut(&mut self) -> &mut Value {
        self
    }
}

/// The mode's part of a pod, over a store it is handed each time: the requests it decides,
/// gathers and gives back. Kept apart from the store so that one store can serve this mode
/// beside another.
#[derive(Clone, Debug, Default)]
pub struct Decider {
    /// Whether it forwards a request for a shard not its own to its owner.
    forwarding: bool,
    /// Whether it decides a request it would forward from its copy, the owner unheard.
    forwarder_decides: bool,
    /// Requests it decided from its copy, their owner unheard.
    decided_from_copy: u64,
    /// Requests under several keys it gathers, by client and the client's number.
    gathering: BTreeMap<(NodeId, u64), Gathering>,
    /// Parts it has asked of other owners for them: the part's number, to its request and key,
    /// and when. Kept past the request, to give back a part counted late, for [`PART_FOR`]: a
    /// pod forgotten may only be cut off, its answer still to come.
    parts: BTreeMap<u64, Part>,
    /// Give-backs it has asked of owners, by number: the key, when last sent (0 for not yet, or
    /// to be sent again at once), and how many sends have gone unanswered.
    giving: BTreeMap<u64, (Key, Time, u8)>,
    next_part: u64,
    /// Parts counted for requests it then reported not counted.
    orphaned: u64,
}

impl Decider {
    /// Whether it forwards a request for a shard not its own to the owner its table names, for
    /// at most one hop, rather than answer [`Answer::Elsewhere`].
    pub const fn set_forwarding(&mut self, forwarding: bool) {
        self.forwarding = forwarding;
    }

    /// Whether a request from a data plane it would forward it decides itself from its copy,
    /// as the owner would, when its table names it a replica of the key's shard, it holds its
    /// lease and it has not heard the owner for `stop_serving` (`Node::update_copy`): so
    /// that an owner gone has its keys held to their limits while it is failed over.
    pub const fn set_forwarder_decides(&mut self, decides: bool) {
        self.forwarder_decides = decides;
    }

    /// Requests it decided from its copy, their owner unheard.
    #[must_use]
    pub const fn decided_from_copy(&self) -> u64 {
        self.decided_from_copy
    }

    /// Parts it counted, gathering requests under several keys, for requests it then reported
    /// not counted: what a client retrying the whole request may count twice.
    #[must_use]
    pub const fn orphaned(&self) -> u64 {
        self.orphaned
    }

    /// Give-backs it is still sending: asked of their owners, neither answered nor yet sent
    /// [`GIVE_BACK_SENDS`] times unanswered.
    #[must_use]
    pub fn giving_back(&self) -> usize {
        self.giving.len()
    }

    /// Parts it has asked of other owners and keeps, to give back one counted late: each for
    /// [`PART_FOR`] after it was last asked.
    #[must_use]
    pub fn parts_kept(&self) -> usize {
        self.parts.len()
    }

    /// Its timer at `now`, over `node`, whose own timer has run: the requests it gathers.
    pub fn tick<V: Holds>(&mut self, node: &mut Node<V>, now: Time) -> Outbox<V> {
        let mut out = Outbox::new();
        self.parts
            .retain(|_, part| now.saturating_sub(part.at) <= PART_FOR);
        self.gather(node, now, &mut out);
        out
    }

    /// `message` from `from` at `now`, over `node`: a request, an answer or a request to gather
    /// taken here, the store's by the store.
    pub fn receive<V: Holds>(
        &mut self,
        node: &mut Node<V>,
        from: NodeId,
        message: Message<V>,
        now: Time,
    ) -> Outbox<V> {
        let mut out = Outbox::new();
        match message {
            Message::Request {
                client,
                id,
                key,
                stamp,
                hops,
                via,
                give_back,
                rule,
                receipt,
            } => {
                let ask = Ask {
                    give_back,
                    rule,
                    receipt,
                };
                self.on_request(node, client, id, key, stamp, hops, ask, now, &mut out);
                // Answered through the pod that forwarded it, if one did.
                if let Some(via) = via {
                    for (to, message) in &mut out {
                        if matches!(message, Message::Reply { .. }) && *to == client {
                            *to = via;
                        }
                    }
                }
            }
            // An answer passed on by this pod, to the client it is for; or, for this pod, a part
            // of a request it gathers.
            Message::Reply { client, id, answer } => {
                if client == node.id() {
                    self.on_part(node, id, &answer, now, &mut out);
                } else {
                    out.push((client, Message::Reply { client, id, answer }));
                }
            }
            Message::Gather { client, id, keys } => {
                // Asked again while gathering it: the parts already under way stand.
                self.gathering
                    .entry((client, id))
                    .or_insert_with(|| Gathering {
                        started: now,
                        waiting: keys.iter().map(|key| (*key, (0, 0))).collect(),
                        counted: Vec::new(),
                    });
                self.gather(node, now, &mut out);
            }
            Message::Store(message) => out = from_store(node.receive(from, message, now)),
        }
        out
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "a request's fields, as they come"
    )]
    fn on_request<V: Holds>(
        &mut self,
        node: &mut Node<V>,
        client: NodeId,
        id: u64,
        key: Key,
        stamp: Stamp,
        hops: u8,
        ask: Ask,
        now: Time,
        out: &mut Outbox<V>,
    ) {
        let mut decided = node.update(key, now, |kept, wall| decide(key, kept, ask, wall));
        if decided.is_none() && self.forwarder_decides && self.forwarding && hops == 0 {
            // A request it would forward: decided from its copy, its owner unheard.
            decided = node.update_copy(key, now, |kept, wall| decide(key, kept, ask, wall));
            self.decided_from_copy += u64::from(decided.is_some());
        }
        let answer = if let Some(answer) = decided {
            answer
        } else {
            let shard = shard_of(key, node.config().shards);
            let table = node.committed();
            let owner = table.and_then(|table| table.owner(shard));
            if owner == Some(node.id()) {
                Answer::NotServing
            } else if let Some(owner) = owner.filter(|_| self.forwarding && hops == 0) {
                out.push((
                    owner,
                    Message::Request {
                        client,
                        id,
                        key,
                        stamp,
                        hops: hops + 1,
                        via: Some(node.id()),
                        give_back: ask.give_back,
                        rule: ask.rule,
                        receipt: ask.receipt,
                    },
                ));
                // And the client its later table, so that it asks the owner itself next time.
                if let Some(table) = table.filter(|table| table.stamp() > stamp) {
                    out.push((
                        client,
                        edgerush_cluster::Message::TableReply {
                            table: table.clone(),
                        }
                        .into(),
                    ));
                }
                return;
            } else {
                let newer = table.filter(|table| table.stamp() > stamp).cloned();
                Answer::Elsewhere(newer)
            }
        };
        out.push((client, Message::Reply { client, id, answer }));
    }

    /// Gives back one counted under `key` for a request it gathered and reported not counted:
    /// at once if it serves the key's shard, else asked of its owner by its table, and again
    /// every [`GIVE_BACK_AGAIN`] until answered.
    fn give_back<V: Holds>(
        &mut self,
        node: &mut Node<V>,
        key: Key,
        now: Time,
        out: &mut Outbox<V>,
    ) {
        if give_here(node, key, now) {
            return;
        }
        self.next_part += 1;
        self.giving.insert(self.next_part, (key, 0, 0));
        self.give_backs(node, now, out);
    }

    /// Sends each give-back not yet answered, due: never sent, or unanswered for
    /// [`GIVE_BACK_AGAIN`], then [`GIVE_BACK_LATER`] — dropped once [`GIVE_BACK_SENDS`] have
    /// gone unanswered; one whose shard it now serves it gives back itself.
    fn give_backs<V: Holds>(&mut self, node: &mut Node<V>, now: Time, out: &mut Outbox<V>) {
        let due: Vec<(u64, Key, u8)> = self
            .giving
            .iter()
            .filter(|(_, (_, sent, unanswered))| {
                *sent == 0 || now.saturating_sub(*sent) >= give_back_wait(*unanswered)
            })
            .map(|(id, (key, sent, unanswered))| (*id, *key, unanswered + u8::from(*sent != 0)))
            .collect();
        for (id, key, unanswered) in due {
            // Each send unanswered may have been taken, an answer only late: a give-back credited
            // once a send, so at most so many times, its charge standing should every one
            // be lost.
            if unanswered >= GIVE_BACK_SENDS {
                self.giving.remove(&id);
                continue;
            }
            if give_here(node, key, now) {
                self.giving.remove(&id);
                continue;
            }
            let Some(owner) = owner_elsewhere(node, key) else {
                continue;
            };
            self.giving.insert(id, (key, now.max(1), unanswered));
            out.push((
                owner,
                Message::Request {
                    client: node.id(),
                    id,
                    key,
                    stamp: node.committed_stamp(),
                    hops: 0,
                    via: None,
                    give_back: true,
                    rule: None,
                    receipt: 0,
                },
            ));
        }
    }

    /// Advances every request it gathers: parts it serves counted at once, the others asked of
    /// their owners, again if unanswered for [`PART_AGAIN`]; each request answered once every
    /// part is counted, or reported not counted after [`GATHER_FOR`], what was counted for it
    /// given back.
    fn gather<V: Holds>(&mut self, node: &mut Node<V>, now: Time, out: &mut Outbox<V>) {
        self.give_backs(node, now, out);
        let requests: Vec<(NodeId, u64)> = self.gathering.keys().copied().collect();
        for (client, id) in requests {
            let Some(gathering) = self.gathering.get(&(client, id)).cloned() else {
                continue;
            };
            if gathering.waiting.is_empty() {
                self.gathering.remove(&(client, id));
                out.push((
                    client,
                    Message::Reply {
                        client,
                        id,
                        answer: Answer::Counted(0),
                    },
                ));
                continue;
            }
            if now.saturating_sub(gathering.started) > GATHER_FOR {
                self.gathering.remove(&(client, id));
                self.orphaned += gathering.counted.len() as u64;
                for key in gathering.counted {
                    self.give_back(node, key, now, out);
                }
                out.push((
                    client,
                    Message::Reply {
                        client,
                        id,
                        answer: Answer::NotServing,
                    },
                ));
                continue;
            }
            for (key, (sent, _)) in gathering.waiting {
                if count_here(node, key, now) {
                    if let Some(gathering) = self.gathering.get_mut(&(client, id)) {
                        gathering.waiting.remove(&key);
                        gathering.counted.push(key);
                    }
                    continue;
                }
                if sent != 0 && now.saturating_sub(sent) < PART_AGAIN {
                    continue;
                }
                let Some(owner) = owner_elsewhere(node, key) else {
                    continue;
                };
                self.next_part += 1;
                let part = self.next_part;
                self.parts.insert(
                    part,
                    Part {
                        request: (client, id),
                        key,
                        at: now,
                    },
                );
                if let Some(gathering) = self.gathering.get_mut(&(client, id)) {
                    gathering.waiting.insert(key, (now.max(1), part));
                }
                out.push((
                    owner,
                    Message::Request {
                        client: node.id(),
                        id: part,
                        key,
                        stamp: node.committed_stamp(),
                        hops: 1,
                        via: None,
                        give_back: false,
                        rule: None,
                        receipt: 0,
                    },
                ));
            }
            if self
                .gathering
                .get(&(client, id))
                .is_some_and(|gathering| gathering.waiting.is_empty())
            {
                self.gathering.remove(&(client, id));
                out.push((
                    client,
                    Message::Reply {
                        client,
                        id,
                        answer: Answer::Counted(0),
                    },
                ));
            }
        }
    }

    /// An owner's answer to part `part` of a request it gathers, or to a give-back of its.
    fn on_part<V: Holds>(
        &mut self,
        node: &mut Node<V>,
        part: u64,
        answer: &Answer,
        now: Time,
        out: &mut Outbox<V>,
    ) {
        if let Some((key, _, unanswered)) = self.giving.remove(&part) {
            // Not given back — the pod not serving, or not the owner — sent again soon, an
            // answered send taking nothing of its tries.
            if !matches!(answer, Answer::Counted(_)) {
                self.giving.insert(part, (key, 0, unanswered));
            }
            return;
        }
        let Some(Part { request, key, .. }) = self.parts.remove(&part) else {
            return;
        };
        let Some(gathering) = self.gathering.get_mut(&request) else {
            // The request is answered already: a part counted now is an orphan, given back.
            if matches!(answer, Answer::Counted(_)) {
                self.orphaned += 1;
                self.give_back(node, key, now, out);
            }
            return;
        };
        match answer {
            Answer::Counted(_) => {
                // The first part counted for a key, whichever was asked, counts it done; any
                // counted after — asked again, both counted — is an orphan, given back.
                if gathering.waiting.remove(&key).is_some() {
                    gathering.counted.push(key);
                } else {
                    self.orphaned += 1;
                    self.give_back(node, key, now, out);
                }
            }
            Answer::Elsewhere(table) => {
                // Asked again at once, of the owner its table, the later of the two, names.
                if let Some((sent, _)) = gathering.waiting.get_mut(&key) {
                    *sent = 0;
                }
                if let Some(table) = table {
                    let from = node.id();
                    let table = edgerush_cluster::Message::TableReply {
                        table: table.clone(),
                    };
                    out.extend(from_store(node.receive(from, table, now)));
                }
            }
            // A gathering pod asks its parts under no rule: never answered so.
            Answer::NotServing | Answer::Admitted(_) | Answer::Limited { .. } => {}
        }
        self.gather(node, now, out);
    }
}
/// Counts one under `key` with no rule, as `node`, its owner, if it serves the key: whether it
/// did.
fn count_here<V: Holds>(node: &mut Node<V>, key: Key, now: Time) -> bool {
    node.update(key, now, |kept, _| {
        kept.value.synchronous_mut().count_one();
        kept.forget_at = Time::MAX;
        ((), Changed::Whole)
    })
    .is_some()
}

/// Gives one back under `key`, as `node`, its owner, if it serves the key: whether it did.
fn give_here<V: Holds>(node: &mut Node<V>, key: Key, now: Time) -> bool {
    node.update(key, now, |kept, _| {
        kept.value.synchronous_mut().give_one();
        kept.forget_at = Time::MAX;
        ((), Changed::Whole)
    })
    .is_some()
}

/// The owner `node`'s table names for `key`, if another pod.
fn owner_elsewhere<V: Held>(node: &Node<V>, key: Key) -> Option<NodeId> {
    let shard = shard_of(key, node.config().shards);
    node.committed()
        .and_then(|table| table.owner(shard))
        .filter(|owner| *owner != node.id())
}

/// A request decided by its key's owner, by the wall clock `wall`: its answer, and what it
/// changed of the key for its next copy.
fn decide<V: Holds>(key: Key, kept: &mut Kept<V>, ask: Ask, wall: Time) -> (Answer, Changed) {
    // A give-back is counted apart, a part of the value that only rises, so that copies merge
    // by keeping the larger of each part.
    let value = kept.value.synchronous_mut();
    let answer = match (ask.rule, ask.give_back) {
        (None, false) => {
            value.count_one();
            Answer::Counted(value.net())
        }
        (None, true) => {
            value.give_one();
            Answer::Counted(value.net())
        }
        (Some(rule), true) => {
            rule.give_back(value, ask.receipt);
            Answer::Counted(value.net())
        }
        // Charged only if admitted.
        (Some(rule), false) => rule.admit(key, value, wall).map_or_else(
            || Answer::Limited {
                wait: rule.wait(key, value, wall),
            },
            Answer::Admitted,
        ),
    };
    // Forgettable from when its rule says; under none, never.
    kept.forget_at = ask.rule.map_or(Time::MAX, |rule| {
        kept.forget_at.max(rule.forget_at(key, value))
    });
    // A refusal charges nothing: no change for a replica, which a saturated rolling window
    // would otherwise be sent whole of with every refusal. Of a rolling window, the slot
    // charged or given back alone.
    let slot = match answer {
        Answer::Admitted(slot) => Some(slot),
        _ if ask.give_back => Some(ask.receipt),
        _ => None,
    };
    let changed = if matches!(answer, Answer::Limited { .. }) {
        Changed::Nothing
    } else if let (Some(Rule::Rolling { .. }), Some(slot)) = (ask.rule, slot) {
        Changed::Part(slot)
    } else {
        Changed::Whole
    };
    (answer, changed)
}

/// The store's messages, as this mode's.
fn from_store<V>(out: edgerush_cluster::Outbox<V>) -> Outbox<V> {
    out.into_iter()
        .map(|(to, message)| (to, Message::Store(message)))
        .collect()
}

/// How long a pod gathering a request under several keys waits for every part before reporting
/// it not counted.
const GATHER_FOR: Time = 15_000_000;

/// How long a give-back a pod asked of an owner goes unanswered before it is sent again the
/// first time: far beyond a request's deadline, so that one whose answer is only late is not
/// given back twice, and soon enough that one lost holds its allowance little longer.
const GIVE_BACK_AGAIN: Time = 100_000_000;

/// How long it goes unanswered before each send after: past a crashed owner's failover and an
/// answer late, so that the last send reaches the owner then named.
const GIVE_BACK_LATER: Time = 1_000_000_000;

/// The sends of a give-back that may go unanswered before it is dropped: one whose
/// answers are late is credited at most this many times.
pub const GIVE_BACK_SENDS: u8 = 3;

/// How long a give-back sent `unanswered` times unanswered waits before it is sent again.
const fn give_back_wait(unanswered: u8) -> Time {
    if unanswered == 0 {
        GIVE_BACK_AGAIN
    } else {
        GIVE_BACK_LATER
    }
}

/// How long a part asked of its owner goes unanswered before it is asked again.
const PART_AGAIN: Time = 3_000_000;

/// How long a part goes unanswered before it is forgotten: a part counted and answered
/// later stays charged, for the rest of its window, refused or not.
pub const PART_FOR: Time = 10_000_000_000;
