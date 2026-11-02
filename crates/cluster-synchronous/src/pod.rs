//! A pod of the synchronous mode, over its store: each request decided by its key's
//! owner under its rule, a refused request's counted parts given back, and requests under
//! several keys gathered by the pod they reach. The store holds the keys, copies them and
//! hands them on; this decides what they hold.

use crate::message::{Answer, Message, Outbox};
use crate::{Rule, Store, Value};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Changed, Config, Kept, NodeId, Stamp, Time};
use std::collections::BTreeMap;

/// A request under several keys this pod gathers: who asked, and each part still to count,
/// when last asked of its owner.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Gathering {
    started: Time,
    /// Each key not yet counted, with when its part was last sent and its number.
    waiting: BTreeMap<u64, (Time, u64)>,
    /// Keys counted: given back if the request is reported not counted.
    counted: Vec<u64>,
}

/// A part of a gathered request asked of a key's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Part {
    /// The request, by client and the client's number.
    request: (NodeId, u64),
    key: u64,
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

/// One pod of the synchronous mode in one life: its store, and the requests it decides.
#[derive(Clone, Debug)]
pub struct Pod {
    node: Store,
    /// Whether it forwards a request for a shard not its own to its owner.
    forwarding: bool,
    /// Requests under several keys it gathers, by client and the client's number.
    gathering: BTreeMap<(NodeId, u64), Gathering>,
    /// Parts it has asked of other owners for them: the part's number, to its request and key,
    /// and when. Kept past the request, to give back a part counted late, for [`PART_FOR`]: a
    /// pod forgotten may only be cut off, its answer still to come.
    parts: BTreeMap<u64, Part>,
    /// Give-backs it has asked of owners, by number: the key, when last sent (0 for not yet, or
    /// to be sent again at once), and how many sends have gone unanswered.
    giving: BTreeMap<u64, (u64, Time, u8)>,
    next_part: u64,
    /// Parts counted for requests it then reported not counted.
    orphaned: u64,
}

impl Pod {
    /// Pod `id`, reached at `address`, new, knowing how to reach `peers`.
    #[must_use]
    pub fn new(
        id: NodeId,
        address: String,
        config: Config,
        peers: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self {
            node: Store::new(id, address, config, peers),
            forwarding: false,
            gathering: BTreeMap::new(),
            parts: BTreeMap::new(),
            giving: BTreeMap::new(),
            next_part: 0,
            orphaned: 0,
        }
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

    /// Whether it forwards a request for a shard not its own to the owner its table names, for
    /// at most one hop, rather than answer [`Answer::Elsewhere`].
    pub const fn set_forwarding(&mut self, forwarding: bool) {
        self.forwarding = forwarding;
    }

    /// Parts it counted, gathering requests under several keys, for requests it then reported
    /// not counted: what a client retrying the whole request may count twice.
    #[must_use]
    pub const fn orphaned(&self) -> u64 {
        self.orphaned
    }

    /// The count it holds for `key`, owned or a copy: what was counted under it less what was
    /// given back.
    #[must_use]
    pub fn count(&self, key: u64) -> Option<u64> {
        self.node.value(key).map(|value| value.net())
    }

    /// Its timer at `now`: its store's, then the requests it gathers.
    pub fn tick(&mut self, now: Time) -> Outbox {
        let mut out = from_store(self.node.tick(now));
        self.parts
            .retain(|_, part| now.saturating_sub(part.at) <= PART_FOR);
        self.gather(now, &mut out);
        out
    }

    /// `message` from `from` at `now`: a request, an answer or a request to gather taken here,
    /// the store's by its store.
    pub fn receive(&mut self, from: NodeId, message: impl Into<Message>, now: Time) -> Outbox {
        let mut out = Outbox::new();
        match message.into() {
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
                self.on_request(client, id, key, stamp, hops, ask, now, &mut out);
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
                if client == self.node.id() {
                    self.on_part(id, &answer, now, &mut out);
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
                self.gather(now, &mut out);
            }
            Message::Store(message) => out = from_store(self.node.receive(from, message, now)),
        }
        out
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "a request's fields, as they come"
    )]
    fn on_request(
        &mut self,
        client: NodeId,
        id: u64,
        key: u64,
        stamp: Stamp,
        hops: u8,
        ask: Ask,
        now: Time,
        out: &mut Outbox,
    ) {
        let decided = self
            .node
            .update(key, now, |kept, wall| decide(key, kept, ask, wall));
        let answer = if let Some(answer) = decided {
            answer
        } else {
            let shard = shard_of(key, self.node.config().shards);
            let table = self.node.committed();
            let owner = table.and_then(|table| table.owner(shard));
            if owner == Some(self.node.id()) {
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
                        via: Some(self.node.id()),
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

    /// Counts one under `key` with no rule, as its owner, if it serves the key: whether it did.
    fn count_here(&mut self, key: u64, now: Time) -> bool {
        self.node
            .update(key, now, |kept, _| {
                kept.value.count_one();
                kept.forget_at = Time::MAX;
                ((), Changed::Whole)
            })
            .is_some()
    }

    /// Gives one back under `key`, as its owner, if it serves the key: whether it did.
    fn give_here(&mut self, key: u64, now: Time) -> bool {
        self.node
            .update(key, now, |kept, _| {
                kept.value.give_one();
                kept.forget_at = Time::MAX;
                ((), Changed::Whole)
            })
            .is_some()
    }

    /// Gives back one counted under `key` for a request it gathered and reported not counted:
    /// at once if it serves the key's shard, else asked of its owner by its table, and again
    /// every [`GIVE_BACK_AGAIN`] until answered.
    fn give_back(&mut self, key: u64, now: Time, out: &mut Outbox) {
        if self.give_here(key, now) {
            return;
        }
        self.next_part += 1;
        self.giving.insert(self.next_part, (key, 0, 0));
        self.give_backs(now, out);
    }

    /// Sends each give-back not yet answered, due: never sent, or unanswered for
    /// [`GIVE_BACK_AGAIN`], then [`GIVE_BACK_LATER`] — dropped once [`GIVE_BACK_SENDS`] have
    /// gone unanswered; one whose shard it now serves it gives back itself.
    fn give_backs(&mut self, now: Time, out: &mut Outbox) {
        let due: Vec<(u64, u64, u8)> = self
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
            if self.give_here(key, now) {
                self.giving.remove(&id);
                continue;
            }
            let Some(owner) = self.owner_elsewhere(key) else {
                continue;
            };
            self.giving.insert(id, (key, now.max(1), unanswered));
            out.push((
                owner,
                Message::Request {
                    client: self.node.id(),
                    id,
                    key,
                    stamp: self.node.committed_stamp(),
                    hops: 0,
                    via: None,
                    give_back: true,
                    rule: None,
                    receipt: 0,
                },
            ));
        }
    }

    /// The owner its table names for `key`, if another pod.
    fn owner_elsewhere(&self, key: u64) -> Option<NodeId> {
        let shard = shard_of(key, self.node.config().shards);
        self.node
            .committed()
            .and_then(|table| table.owner(shard))
            .filter(|owner| *owner != self.node.id())
    }

    /// Advances every request it gathers: parts it serves counted at once, the others asked of
    /// their owners, again if unanswered for [`PART_AGAIN`]; each request answered once every
    /// part is counted, or reported not counted after [`GATHER_FOR`], what was counted for it
    /// given back.
    fn gather(&mut self, now: Time, out: &mut Outbox) {
        self.give_backs(now, out);
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
                    self.give_back(key, now, out);
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
                if self.count_here(key, now) {
                    if let Some(gathering) = self.gathering.get_mut(&(client, id)) {
                        gathering.waiting.remove(&key);
                        gathering.counted.push(key);
                    }
                    continue;
                }
                if sent != 0 && now.saturating_sub(sent) < PART_AGAIN {
                    continue;
                }
                let Some(owner) = self.owner_elsewhere(key) else {
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
                        client: self.node.id(),
                        id: part,
                        key,
                        stamp: self.node.committed_stamp(),
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
    fn on_part(&mut self, part: u64, answer: &Answer, now: Time, out: &mut Outbox) {
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
                self.give_back(key, now, out);
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
                    self.give_back(key, now, out);
                }
            }
            Answer::Elsewhere(table) => {
                // Asked again at once, of the owner its table, the later of the two, names.
                if let Some((sent, _)) = gathering.waiting.get_mut(&key) {
                    *sent = 0;
                }
                if let Some(table) = table {
                    let from = self.node.id();
                    let table = edgerush_cluster::Message::TableReply {
                        table: table.clone(),
                    };
                    out.extend(from_store(self.node.receive(from, table, now)));
                }
            }
            // A gathering pod asks its parts under no rule: never answered so.
            Answer::NotServing | Answer::Admitted(_) | Answer::Limited => {}
        }
        self.gather(now, out);
    }
}

/// A request decided by its key's owner, by the wall clock `wall`: its answer, and what it
/// changed of the key for its next copy.
fn decide(key: u64, kept: &mut Kept<Value>, ask: Ask, wall: Time) -> (Answer, Changed) {
    // A give-back is counted apart, a part of the value that only rises, so that copies merge
    // by keeping the larger of each part.
    let value = &mut kept.value;
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
        (Some(rule), false) => rule
            .admit(key, value, wall)
            .map_or(Answer::Limited, Answer::Admitted),
    };
    // Forgettable from when its rule says; under none, never.
    kept.forget_at = ask.rule.map_or(Time::MAX, |rule| {
        kept.forget_at.max(rule.forget_at(key, &kept.value))
    });
    // A refusal charges nothing: no change for a replica, which a saturated rolling window
    // would otherwise be sent whole of with every refusal. Of a rolling window, the slot
    // charged or given back alone.
    let slot = match answer {
        Answer::Admitted(slot) => Some(slot),
        _ if ask.give_back => Some(ask.receipt),
        _ => None,
    };
    let changed = if matches!(answer, Answer::Limited) {
        Changed::Nothing
    } else if let (Some(Rule::Rolling { .. }), Some(slot)) = (ask.rule, slot) {
        Changed::Part(slot)
    } else {
        Changed::Whole
    };
    (answer, changed)
}

/// The store's messages, as this mode's.
fn from_store(out: edgerush_cluster::Outbox<Value>) -> Outbox {
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
const GIVE_BACK_SENDS: u8 = 3;

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
const PART_FOR: Time = 10_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::Message as StoreMessage;
    use edgerush_cluster::{ShardId, ms, table};

    fn config() -> Config {
        let mut config = Config::defaults();
        config.shards = 4;
        config
    }

    /// Pod 2 holding the first table of pods 1, 2 and 3.
    fn member() -> Pod {
        let mut node = Pod::new(2, "pod-2:7000".to_owned(), config(), [1, 3]);
        let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
        let table = table::first(&members, 4, 2, 1);
        let _ = node.receive(1, StoreMessage::TableReply { table }, ms(1));
        node
    }

    /// The first key of `shard`.
    fn key_in(shard: ShardId) -> u64 {
        (0..).find(|key| shard_of(*key, 4) == shard).unwrap_or(0)
    }

    #[test]
    fn a_gathering_pod_gives_back_a_part_counted_late_until_given_back() {
        let mut node = Pod::new(1, "pod-1:7000".to_owned(), config(), [2, 3]);
        let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
        let first = table::first(&members, 4, 2, 1);
        let mine = (0..4)
            .find(|shard| first.owner(*shard) == Some(1))
            .unwrap_or(0);
        let theirs = (0..4)
            .find(|shard| first.owner(*shard) == Some(2))
            .unwrap_or(0);
        let _ = node.receive(2, StoreMessage::TableReply { table: first }, ms(1));
        let lease = |node: &mut Pod, now: Time| {
            for peer in [2, 3] {
                let heard = StoreMessage::Heartbeat {
                    term: node.store().term(),
                    leader: Some(2),
                    committed: node.store().committed_stamp(),
                    fresh: false,
                    sent: now,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = node.receive(peer, heard, now);
            }
        };
        lease(&mut node, ms(2));
        let (own, other) = (key_in(mine), key_in(theirs));
        let asked = node.receive(
            1_000_000,
            Message::Gather {
                client: 1_000_000,
                id: 7,
                keys: vec![own, other],
            },
            ms(3),
        );
        let part = asked.iter().find_map(|(to, message)| match message {
            Message::Request { id, key, .. } if *to == 2 && *key == other => Some(*id),
            _ => None,
        });
        let Some(part) = part else {
            panic!("pod 2's part never asked: {asked:?}");
        };
        // Past the gathering's time: reported not counted.
        for at in 1..10 {
            lease(&mut node, ms(3 + at * 5));
            let _ = node.tick(ms(3 + at * 5));
        }
        let back_of = |out: &Outbox| {
            out.iter().find_map(|(to, message)| match message {
                Message::Request {
                    id,
                    key,
                    give_back: true,
                    rule: None,
                    receipt: 0,
                    ..
                } if *to == 2 && *key == other => Some(*id),
                _ => None,
            })
        };
        let late = Message::Reply {
            client: 1,
            id: part,
            answer: Answer::Counted(1),
        };
        let out = node.receive(2, late, ms(60));
        let Some(back) = back_of(&out) else {
            panic!("the part counted late never given back: {out:?}");
        };
        let refused = Message::Reply {
            client: 1,
            id: back,
            answer: Answer::NotServing,
        };
        let _ = node.receive(2, refused, ms(61));
        lease(&mut node, ms(62));
        let again = node.tick(ms(62));
        assert!(
            back_of(&again).is_some(),
            "never given back again: {again:?}"
        );
        // Never answered from then on, its answers maybe only late: sent `GIVE_BACK_SENDS`
        // times unanswered in all, the answered one not counted, the second 100 ms after and the
        // last a second after that, past a failover, then dropped.
        let mut sends = vec![62];
        for at in 1..=300 {
            let now = ms(62 + at * 10);
            lease(&mut node, now);
            if back_of(&node.tick(now)).is_some() {
                sends.push(62 + at * 10);
            }
        }
        assert_eq!(sends, [62, 162, 1_162]);
        assert_eq!(sends.len(), usize::from(GIVE_BACK_SENDS));
        assert!(node.giving.is_empty(), "{:?}", node.giving);
    }

    /// A pod gathering a request under keys of pods 2 and 3 asks a part again after
    /// [`PART_AGAIN`] unanswered: both asks of pod 2 counted, the second answer arriving while
    /// pod 3's part is still out, one completes pod 2's key and the other is given back.
    #[test]
    fn a_gathering_pod_gives_back_a_part_counted_twice() {
        let mut node = Pod::new(1, "pod-1:7000".to_owned(), config(), [2, 3]);
        let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
        let first = table::first(&members, 4, 2, 1);
        let of = |pod| {
            (0..4)
                .find(|shard| first.owner(*shard) == Some(pod))
                .unwrap_or(0)
        };
        let (two, three) = (key_in(of(2)), key_in(of(3)));
        let _ = node.receive(
            2,
            StoreMessage::TableReply {
                table: first.clone(),
            },
            ms(1),
        );
        let lease = |node: &mut Pod, now: Time| {
            for peer in [2, 3] {
                let heard = StoreMessage::Heartbeat {
                    term: node.store().term(),
                    leader: Some(2),
                    committed: node.store().committed_stamp(),
                    fresh: false,
                    sent: now,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = node.receive(peer, heard, now);
            }
        };
        let asked = |out: &Outbox, pod: NodeId, wanted: u64| {
            out.iter()
                .filter_map(|(to, message)| match message {
                    Message::Request {
                        id, key, give_back, ..
                    } if *to == pod && *key == wanted => Some((*id, *give_back)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        lease(&mut node, 2_000_000);
        let gather = Message::Gather {
            client: 1_000_000,
            id: 7,
            keys: vec![two, three],
        };
        let out = node.receive(1_000_000, gather, 3_000_000);
        let mut asks = asked(&out, 2, two);
        let Some((part_of_three, _)) = asked(&out, 3, three).first().copied() else {
            panic!("pod 3's part never asked: {out:?}");
        };
        // Unanswered for a while: asked again.
        lease(&mut node, 7_000_000);
        asks.extend(asked(&node.tick(7_000_000), 2, two));
        let [(part, false), (again, false)] = asks[..] else {
            panic!("pod 2 not asked twice: {asks:?}");
        };
        let counted = |id| Message::Reply {
            client: 1,
            id,
            answer: Answer::Counted(1),
        };
        let mut out = node.receive(2, counted(part), 8_000_000);
        out.extend(node.receive(2, counted(again), 9_000_000));
        let given_back = asked(&out, 2, two).iter().filter(|(_, back)| *back).count();
        assert_eq!(given_back, 1, "{out:?}");
        let out = node.receive(3, counted(part_of_three), 10_000_000);
        let answered = out.iter().any(|(to, message)| {
            *to == 1_000_000
                && matches!(
                    message,
                    Message::Reply {
                        answer: Answer::Counted(_),
                        ..
                    }
                )
        });
        assert!(answered, "the request never answered counted: {out:?}");
    }

    /// A pod gathering a request under two keys, its own and pod 2's: the request timing out,
    /// pod 2's part counted after is given back to it, and given back again when pod 2 says it
    /// was not, until it is.
    /// A part never answered is kept, to give back should it be counted late, for `PART_FOR`
    /// and no longer, its pod forgotten or not: a request timed out leaves nothing kept for
    /// good, and a pod forgotten, which may only be cut off, still has its late answer taken.
    #[test]
    fn a_part_unanswered_is_kept_for_a_while_its_pod_forgotten_or_not() {
        let mut node = Pod::new(1, "pod-1:7000".to_owned(), config(), [2, 3]);
        let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
        let first = table::first(&members, 4, 2, 1);
        let theirs = (0..4)
            .find(|shard| first.owner(*shard) == Some(2))
            .unwrap_or(0);
        let _ = node.receive(
            2,
            StoreMessage::TableReply {
                table: first.clone(),
            },
            ms(1),
        );
        let heard = |node: &mut Pod, peers: &[NodeId], now: Time| {
            for peer in peers {
                let beat = StoreMessage::Heartbeat {
                    term: node.store().term(),
                    leader: Some(3),
                    committed: node.store().committed_stamp(),
                    fresh: false,
                    sent: now,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = node.receive(*peer, beat, now);
            }
        };
        let gather = |node: &mut Pod, id: u64, now: Time| {
            let ask = Message::Gather {
                client: 1_000_000,
                id,
                keys: vec![key_in(theirs)],
            };
            node.receive(1_000_000, ask, now)
                .into_iter()
                .find_map(|(to, message)| match message {
                    Message::Request { id, .. } if to == 2 => Some(id),
                    _ => None,
                })
        };
        // Pod 2 heard throughout, its answers never coming.
        let _ = gather(&mut node, 1, ms(2));
        let mut kept_at_5_s = 0;
        for at in 1..=210 {
            let now = ms(2 + at * 50);
            heard(&mut node, &[2, 3], now);
            let _ = node.tick(now);
            if at == 100 {
                kept_at_5_s = node.parts.len();
            }
        }
        assert_eq!(
            kept_at_5_s, 1,
            "a part counted late could not be given back"
        );
        assert!(node.parts.is_empty(), "{:?} kept past PART_FOR", node.parts);
        // Pod 2 cut off: heard no more, and the table leaves it out, a new pod having taken its
        // vote, so that it is forgotten; its answer, only late, comes 2 s on.
        let start = ms(10_600);
        let part = gather(&mut node, 2, start);
        let _ = node.tick(start + ms(20));
        assert_eq!(node.parts.len(), 1);
        let survivors = [1, 3, 4].into_iter().collect();
        let joining = [(4, "pod-4:7000".to_owned())].into();
        let mut without = table::repair(&first, &survivors, &survivors, &joining, 2)
            .unwrap_or_else(|| first.clone());
        assert!(!without.votes(2) && !without.has_member(2), "{without:?}");
        without.version = first.version + 1;
        let _ = node.receive(
            3,
            StoreMessage::TableReply { table: without },
            start + ms(30),
        );
        for at in 1..=39 {
            let now = start + ms(30 + at * 50);
            heard(&mut node, &[3], now);
            let _ = node.tick(now);
        }
        assert!(!node.store().peers().contains(&2), "pod 2 not forgotten");
        let late = Message::Reply {
            client: 1,
            id: part.unwrap_or(0),
            answer: Answer::Counted(1),
        };
        let out = node.receive(2, late, start + ms(2_000));
        assert!(
            out.iter().any(|(_, message)| matches!(message,
                Message::Request { key, give_back: true, .. } if *key == key_in(theirs))),
            "a part counted 2 s late, its pod forgotten, not given back: {out:?}"
        );
    }

    /// A request refused for the limit charges nothing, so nothing of its key is copied: a
    /// saturated rolling window would otherwise be sent whole with every refusal.
    #[test]
    fn a_refused_request_sends_its_replicas_nothing() {
        let mut node = member();
        let table = node.store().committed().cloned().unwrap();
        let shard = (0..4).find(|shard| table.owner(*shard) == Some(2)).unwrap();
        for peer in [1, 3] {
            let beat = StoreMessage::Heartbeat {
                term: 1,
                leader: Some(1),
                committed: table.stamp(),
                fresh: false,
                sent: ms(2),
                hears: vec![(1, ms(2)), (2, ms(2)), (3, ms(2))],
                leased: true,
            };
            let _ = node.receive(peer, beat, ms(2));
        }
        for (run, rule) in [
            Rule::Rolling {
                per: 2,
                window: ms(1_000),
            },
            Rule::Window {
                per: 2,
                window: ms(1_000),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let at = |offset: u64| ms(run as u64 * 100 + offset);
            // A key of its own for each rule.
            let key = (0..)
                .filter(|key| shard_of(*key, 4) == shard)
                .nth(run)
                .unwrap_or(0);
            let ask = |id| Message::Request {
                client: 1_000_000,
                id,
                key,
                stamp: table.stamp(),
                hops: 0,
                via: None,
                give_back: false,
                rule: Some(rule),
                receipt: 0,
            };
            let _ = node.receive(9, ask(1), at(3));
            let _ = node.receive(9, ask(2), at(4));
            let _: Outbox = (5..=15).flat_map(|offset| node.tick(at(offset))).collect();
            let refused = node.receive(9, ask(3), at(16));
            assert!(
                refused.iter().any(|(_, message)| matches!(
                    message,
                    Message::Reply {
                        answer: Answer::Limited,
                        ..
                    }
                )),
                "{refused:?}"
            );
            let next: Outbox = (17..=30).flat_map(|offset| node.tick(at(offset))).collect();
            let copied = next.iter().any(|(_, message)| {
                matches!(message, Message::Store(StoreMessage::Replicate { after: Some(_), entries, .. })
                    if entries.iter().any(|(copied, _)| *copied == key))
            });
            assert!(!copied, "{rule:?}: a refusal copied: {next:?}");
        }
    }

    /// A rolling window's copy of what changed carries only the slots changed since the last,
    /// not the up to a thousand the window holds: slots merge each by the larger, so a part is
    /// as safe as the whole.
    #[test]
    fn a_rolling_windows_copy_carries_only_the_slots_changed() {
        let mut node = member();
        let table = node.store().committed().cloned().unwrap();
        let shard = (0..4).find(|shard| table.owner(*shard) == Some(2)).unwrap();
        for peer in [1, 3] {
            let beat = StoreMessage::Heartbeat {
                term: 1,
                leader: Some(1),
                committed: table.stamp(),
                fresh: false,
                sent: ms(2),
                hears: vec![(1, ms(2)), (2, ms(2)), (3, ms(2))],
                leased: true,
            };
            let _ = node.receive(peer, beat, ms(2));
        }
        let key = key_in(shard);
        let rule = Rule::Rolling {
            per: 100,
            window: ms(1_000),
        };
        let ask = |id| Message::Request {
            client: 1_000_000,
            id,
            key,
            stamp: table.stamp(),
            hops: 0,
            via: None,
            give_back: false,
            rule: Some(rule),
            receipt: 0,
        };
        let slots_sent = |out: &Outbox| {
            out.iter()
                .find_map(|(_, message)| match message {
                    Message::Store(StoreMessage::Replicate {
                        after: Some(_),
                        entries,
                        ..
                    }) => entries
                        .iter()
                        .find(|(copied, _)| *copied == key)
                        .map(|(_, kept)| match &kept.value {
                            Value::Slots(slots) => slots.keys().copied().collect::<Vec<_>>(),
                            _ => Vec::new(),
                        }),
                    _ => None,
                })
                .unwrap_or_default()
        };
        let _ = node.receive(9, ask(1), ms(3));
        let _ = node.receive(9, ask(2), ms(4));
        let first: Outbox = (5..=15).flat_map(|at| node.tick(ms(at))).collect();
        assert_eq!(slots_sent(&first), vec![3, 4]);
        let _ = node.receive(9, ask(3), ms(20));
        let next: Outbox = (21..=35).flat_map(|at| node.tick(ms(at))).collect();
        assert_eq!(slots_sent(&next), vec![20]);
        // Raised by another's copy in the same cycle — an old owner's last changes — the key
        // is copied on whole, the slots the copy brought with the one charged.
        let _ = node.receive(9, ask(4), ms(40));
        let last = StoreMessage::Handoff {
            shard,
            term: 1,
            entries: vec![(
                key,
                Kept {
                    value: Value::Slots([(36, (1, 0))].into_iter().collect()),
                    forget_at: ms(10_000),
                },
            )],
            dropped: Vec::new(),
        };
        let _ = node.receive(1, last, ms(41));
        let whole: Outbox = (42..=55).flat_map(|at| node.tick(ms(at))).collect();
        assert_eq!(slots_sent(&whole), vec![3, 4, 20, 36, 40]);
    }

    /// An owner decides a request under a rule by its wall clock: admitted and charged with
    /// room, refused and charged nothing without; its wall clock a window on, admitted again.
    #[test]
    fn an_owner_decides_requests_under_a_rule_by_its_wall_clock() {
        let mut node = member();
        let table = node.store().committed().cloned().unwrap();
        let shard = (0..4).find(|shard| table.owner(*shard) == Some(2)).unwrap();
        for peer in [1, 3] {
            let beat = StoreMessage::Heartbeat {
                term: 1,
                leader: Some(1),
                committed: table.stamp(),
                fresh: false,
                sent: ms(2),
                hears: vec![(1, ms(2)), (2, ms(2)), (3, ms(2))],
                leased: true,
            };
            let _ = node.receive(peer, beat, ms(2));
        }
        assert!(node.store().serves(shard, ms(3)));
        let key = key_in(shard);
        let rule = Rule::Window {
            per: 1,
            window: ms(100),
        };
        let ask = |id| Message::Request {
            client: 1_000_000,
            id,
            key,
            stamp: table.stamp(),
            hops: 0,
            via: None,
            give_back: false,
            rule: Some(rule),
            receipt: 0,
        };
        let answer = |out: Outbox| {
            out.into_iter().find_map(|(_, message)| match message {
                Message::Reply { answer, .. } => Some(answer),
                _ => None,
            })
        };
        let window = rule.window_of(key, ms(3));
        assert_eq!(
            answer(node.receive(9, ask(1), ms(3))),
            Some(Answer::Admitted(window))
        );
        assert_eq!(
            answer(node.receive(9, ask(2), ms(3))),
            Some(Answer::Limited)
        );
        assert_eq!(
            node.store().value(key).map(|value| value.net()),
            Some(1),
            "a refusal charged"
        );
        node.store_mut()
            .set_wall_offset(i64::try_from(ms(100)).unwrap());
        assert_eq!(
            answer(node.receive(9, ask(3), ms(3))),
            Some(Answer::Admitted(window + 1))
        );
    }
}
