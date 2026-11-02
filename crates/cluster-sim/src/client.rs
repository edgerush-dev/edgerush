//! A client standing for a data plane: requests at a steady rate under keys of its own, each
//! sent to the owner its table names, sent another way only when it could not reach the pod
//! — never when its answer is late, as the owner may have counted it — and given up at its
//! deadline.

use crate::Rng;
use edgerush_cluster::table::{Table, shard_of};
use edgerush_cluster::{NodeId, Time};
use edgerush_cluster_synchronous::{Answer, Message, Rule};
use std::collections::BTreeMap;

/// How one request ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Counted by a pod, after this long.
    Counted(Time),
    /// Not counted by its deadline: what a data plane answers as `when_unreachable` says.
    Refused,
    /// Refused by a key's owner under its rule, its limit reached; what other parts of it
    /// were charged given back.
    Limited,
}

/// A request's keys, each with the number of the part asked under it, and how it ended.
pub type Ended = (Vec<(u64, u64)>, Outcome);

/// How a client asks under several keys, when it does (Q5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Multi {
    /// Each key asked of its owner apart, in parallel; counted once every part is.
    Split,
    /// The whole request sent to one pod, which gathers the parts from their owners.
    Gather,
}

/// A message to send, with whom it is for.
pub type Send = (NodeId, Message);

/// A request on its way.
#[derive(Clone, Debug)]
struct Inflight {
    key: u64,
    /// The request it is a part of, split by key, if it is one.
    group: Option<u64>,
    /// Every key, for a request sent whole to be gathered.
    keys: Vec<u64>,
    started: Time,
    /// When the attempt now in flight was sent.
    sent: Time,
    /// The pod it was sent to.
    to: NodeId,
    /// When to ask again, the pod having answered that it did not count it.
    again: Option<Time>,
    /// Whether the attempt in flight has been counted against its pod's breaker.
    missed: bool,
    /// Whether it gives back one counted before: sent again until answered, at most
    /// [`GIVE_BACK_SENDS`] sends unanswered, as each may be credited, its answer only late.
    give_back: bool,
    /// For a give-back, its sends gone unanswered.
    unanswered: u8,
    /// For a give-back under a rule, the receipt its charge was answered with.
    receipt: u64,
}

/// One client.
#[derive(Clone, Debug)]
pub struct Client {
    /// Its identity on the network, apart from every pod's.
    pub id: NodeId,
    keys: Vec<u64>,
    /// The time between its requests.
    pub interval: Time,
    /// How long a request may take before it is refused.
    pub deadline: Time,
    /// How long an attempt may go unanswered before it is tried elsewhere.
    pub attempt: Time,
    table: Option<Table>,
    next: u64,
    inflight: BTreeMap<u64, Inflight>,
    shards: u16,
    /// When it last sent a request.
    pub last_sent: Option<Time>,
    /// Whether each request is under a second key shared by every client, and how it is asked.
    multi: Option<(Multi, u64)>,
    /// Requests split by key: when each began, its parts still out, its keys counted, and
    /// whether a part was refused.
    groups: BTreeMap<u64, Group>,
    /// Parts counted for requests then refused.
    pub orphaned: u64,
    /// Pods it has stopped asking, each until when, as a data plane's breaker does: one left
    /// [`AVOID_AFTER`] attempts in a row unanswered. Their keys go to another pod meanwhile, to
    /// be forwarded.
    avoiding: BTreeMap<NodeId, Time>,
    /// Attempts each pod has left unanswered in a row.
    misses: BTreeMap<NodeId, u32>,
    /// Pods its stream to is down — a send could not reach them — each until when: asked by
    /// no path meanwhile, where one avoided for answers missing may still be.
    down: BTreeMap<NodeId, Time>,
    /// Its clock as of its last event, for whom to avoid.
    now: Time,
    /// Requests it has made, parts and give-backs aside.
    pub requests: u64,
    /// Requests left to `when_unreachable`, with their keys and the pod each was last sent
    /// to: waited on until answered — a "counted" given back — or its pod is gone, its stream
    /// with it, as an ask given up on stays on its stream in a data plane.
    left: BTreeMap<u64, (Vec<u64>, NodeId)>,
    /// The rule its requests are held to by their keys' owners, if any.
    rule: Option<Rule>,
}

/// How long a give-back goes unanswered before it is sent again the first time: far beyond a
/// request's deadline, as it has none, so that one whose answer is only late is not given back
/// twice, and soon enough that one lost holds its allowance little longer.
const GIVE_BACK_AGAIN: Time = 100_000_000;

/// How long it goes unanswered before each send after: past a crashed owner's failover and an
/// answer late, so that the last send reaches the owner then named.
const GIVE_BACK_LATER: Time = 1_000_000_000;

/// The sends of a give-back that may go unanswered before it is dropped: one whose
/// answers are late is credited at most this many times, its charge standing should every
/// one be lost.
const GIVE_BACK_SENDS: u8 = 3;

/// The pod a request waiting for a stream is sent to: none.
const NOBODY: NodeId = 0;

/// How long a request with no pod to go to waits before it is tried again.
const WAIT: Time = 1_000_000;

/// How long a client avoids a pod that left attempts unanswered.
const AVOID_FOR: Time = 500_000_000;

/// Attempts a pod leaves unanswered in a row before a client avoids it: one alone is as likely
/// a message lost, and avoiding the owner of a key over it sends its requests the long way.
const AVOID_AFTER: u32 = 2;

/// A request split by key.
#[derive(Clone, Debug, Default)]
struct Group {
    out: usize,
    /// Every key it is under, each with its part's number; and those of its parts charged,
    /// each with its receipt.
    keys: Vec<(u64, u64)>,
    counted: Vec<(u64, u64)>,
    refused: bool,
    /// Whether a part was refused under its rule.
    limited: bool,
    started: Time,
}

impl Client {
    /// Client `id`, requesting under `keys` in turn every `interval`, each given `deadline`.
    #[must_use]
    pub fn new(id: NodeId, keys: Vec<u64>, interval: Time, deadline: Time, shards: u16) -> Self {
        Self {
            id,
            keys,
            interval,
            deadline,
            attempt: deadline / 4,
            table: None,
            next: 0,
            inflight: BTreeMap::new(),
            shards,
            last_sent: None,
            multi: None,
            groups: BTreeMap::new(),
            orphaned: 0,
            avoiding: BTreeMap::new(),
            misses: BTreeMap::new(),
            down: BTreeMap::new(),
            now: 0,
            requests: 0,
            left: BTreeMap::new(),
            rule: None,
        }
    }

    /// Each request from now held to `rule` by its key's owner.
    pub const fn set_rule(&mut self, rule: Rule) {
        self.rule = Some(rule);
    }

    /// Each request from now also under `shared`, asked as `multi` says.
    pub fn set_multi(&mut self, multi: Multi, shared: u64) {
        self.multi = Some((multi, shared));
    }

    /// Its table's number.
    fn stamp(&self) -> (u64, u64) {
        self.table.as_ref().map_or((0, 0), Table::stamp)
    }

    /// Takes `table` if it is later than its own.
    pub fn learn(&mut self, table: Table) {
        if table.stamp() > self.stamp() {
            self.table = Some(table);
        }
    }

    /// Where it would send a request under `key`: the owner its table names if that pod is
    /// among `pods`, else one of `pods` at random.
    fn target(&self, key: u64, pods: &[NodeId], rng: &mut Rng) -> Option<NodeId> {
        let owner = self
            .table
            .as_ref()
            .and_then(|table| table.owner(shard_of(key, self.shards)));
        let avoided = |pod: &NodeId| {
            self.avoiding
                .get(pod)
                .is_some_and(|until| *until > self.now)
        };
        // Never a pod its stream to is down; none at all if every one's is.
        let reachable: Vec<NodeId> = pods
            .iter()
            .copied()
            .filter(|pod| !self.is_down(*pod))
            .collect();
        owner
            .filter(|owner| reachable.contains(owner) && !avoided(owner))
            .or_else(|| {
                let open: Vec<NodeId> = reachable
                    .iter()
                    .copied()
                    .filter(|pod| !avoided(pod))
                    .collect();
                let pool = if open.is_empty() { &reachable } else { &open };
                let index = usize::try_from(rng.below(pool.len() as u64)).ok()?;
                pool.get(index).copied()
            })
    }

    /// Request `id` with no pod to send it to, every stream down: waits, tried again in a
    /// millisecond, and left to `when_unreachable` at its deadline if none comes back.
    fn wait(&mut self, id: u64, now: Time) {
        if let Some(entry) = self.inflight.get_mut(&id) {
            entry.to = NOBODY;
            entry.sent = now;
            entry.again = Some(now + WAIT);
            entry.missed = true;
        }
    }

    fn request(&self, id: u64, key: u64) -> Message {
        if let Some(inflight) = self.inflight.get(&id)
            && !inflight.keys.is_empty()
        {
            return Message::Gather {
                client: self.id,
                id,
                keys: inflight.keys.clone(),
            };
        }
        let inflight = self.inflight.get(&id);
        Message::Request {
            client: self.id,
            id,
            key,
            stamp: self.stamp(),
            hops: 0,
            via: None,
            give_back: inflight.is_some_and(|inflight| inflight.give_back),
            rule: self.rule,
            receipt: inflight.map_or(0, |inflight| inflight.receipt),
        }
    }

    /// Gives back one charged under `key`, with `receipt`, for a request then refused: to the
    /// key's owner now, by its table, as a request goes.
    fn give_back(
        &mut self,
        key: u64,
        receipt: u64,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
    ) -> Option<Send> {
        let id = self.next;
        self.next += 1;
        let to = self.target(key, pods, rng);
        self.inflight.insert(
            id,
            Inflight {
                key,
                group: None,
                keys: Vec::new(),
                started: now,
                sent: now,
                to: to.unwrap_or(NOBODY),
                again: None,
                missed: false,
                give_back: true,
                unanswered: 0,
                receipt,
            },
        );
        let Some(to) = to else {
            self.wait(id, now);
            return None;
        };
        Some((to, self.request(id, key)))
    }

    /// Its next request, at `now`, to one of `pods`.
    pub fn send(&mut self, now: Time, pods: &[NodeId], rng: &mut Rng) -> Option<(NodeId, Message)> {
        self.now = now;
        self.last_sent = Some(now);
        let key = *self
            .keys
            .get(usize::try_from(self.next).ok()? % self.keys.len().max(1))?;
        let id = self.next;
        self.next += 1;
        self.requests += 1;
        let to = self.target(key, pods, rng);
        let keys = match self.multi {
            Some((Multi::Gather, shared)) => vec![key, shared],
            _ => Vec::new(),
        };
        self.inflight.insert(
            id,
            Inflight {
                key,
                group: None,
                keys,
                started: now,
                sent: now,
                to: to.unwrap_or(NOBODY),
                again: None,
                missed: false,
                give_back: false,
                unanswered: 0,
                receipt: 0,
            },
        );
        let Some(to) = to else {
            self.wait(id, now);
            return None;
        };
        Some((to, self.request(id, key)))
    }

    /// Its next request split by key, at `now`: a part to each key's owner.
    pub fn send_split(
        &mut self,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
    ) -> Vec<(NodeId, Message)> {
        self.now = now;
        self.last_sent = Some(now);
        let Some((Multi::Split, shared)) = self.multi else {
            return self.send(now, pods, rng).into_iter().collect();
        };
        let Some(key) = usize::try_from(self.next)
            .ok()
            .and_then(|next| self.keys.get(next % self.keys.len().max(1)))
            .copied()
        else {
            return Vec::new();
        };
        let group = self.next;
        self.next += 1;
        self.requests += 1;
        let parts = [(key, self.next), (shared, self.next + 1)];
        self.next += 2;
        self.groups.insert(
            group,
            Group {
                out: 2,
                keys: parts.to_vec(),
                started: now,
                ..Group::default()
            },
        );
        let mut sends = Vec::new();
        for (part, id) in parts {
            let to = self.target(part, pods, rng);
            self.inflight.insert(
                id,
                Inflight {
                    key: part,
                    group: Some(group),
                    keys: Vec::new(),
                    started: now,
                    sent: now,
                    to: to.unwrap_or(NOBODY),
                    again: None,
                    missed: false,
                    give_back: false,
                    unanswered: 0,
                    receipt: 0,
                },
            );
            match to {
                Some(to) => sends.push((to, self.request(id, part))),
                None => self.wait(id, now),
            }
        }
        sends
    }

    /// Whether requests are split by key.
    #[must_use]
    pub fn splits(&self) -> bool {
        matches!(self.multi, Some((Multi::Split, _)))
    }

    /// A part of a split request ended, charged with its receipt or not: the whole request's
    /// end, once every part has, and the keys of the parts charged for it, refused, to give
    /// back with their receipts.
    fn part_ended(
        &mut self,
        group: u64,
        key: u64,
        charged: Option<u64>,
        now: Time,
    ) -> (Option<Ended>, Vec<(u64, u64)>) {
        let Some(entry) = self.groups.get_mut(&group) else {
            return (None, Vec::new());
        };
        entry.out = entry.out.saturating_sub(1);
        match charged {
            Some(receipt) => entry.counted.push((key, receipt)),
            None => entry.refused = true,
        }
        if entry.out > 0 {
            return (None, Vec::new());
        }
        let Some(entry) = self.groups.remove(&group) else {
            return (None, Vec::new());
        };
        // Beyond a limit, refused whatever else: what it counted, given back, is no orphan.
        if entry.limited {
            return (Some((entry.keys, Outcome::Limited)), entry.counted);
        }
        if entry.refused {
            self.orphaned += entry.counted.len() as u64;
            return (Some((entry.keys, Outcome::Refused)), entry.counted);
        }
        let ended = (
            entry.keys,
            Outcome::Counted(now.saturating_sub(entry.started)),
        );
        (Some(ended), Vec::new())
    }

    /// Give-backs for each of `keys`, with its receipt.
    fn give_backs(
        &mut self,
        keys: Vec<(u64, u64)>,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
    ) -> Vec<Send> {
        keys.into_iter()
            .filter_map(|(key, receipt)| self.give_back(key, receipt, now, pods, rng))
            .collect()
    }

    /// A pod's `answer` to request `id`: done, or sent again where the answer points; and what
    /// to send, give-backs among it. The key of a request counted is given back with how it
    /// ended; a "counted" coming after its request was left to `when_unreachable` is given
    /// back to the keys' owners.
    pub fn answered(
        &mut self,
        id: u64,
        answer: Answer,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
    ) -> (Option<Ended>, Vec<Send>) {
        self.now = now;
        let Some(inflight) = self.inflight.get(&id).cloned() else {
            // A request left to `when_unreachable` answered at last: given back if charged,
            // settled either way.
            let receipt = match answer {
                Answer::Counted(_) => Some(0),
                Answer::Admitted(receipt) => Some(receipt),
                _ => None,
            };
            if let Some((keys, _)) = self.left.remove(&id)
                && let Some(receipt) = receipt
            {
                let back = keys.into_iter().map(|key| (key, receipt)).collect();
                return (None, self.give_backs(back, now, pods, rng));
            }
            return (None, Vec::new());
        };
        match answer {
            Answer::Counted(_) | Answer::Admitted(_) => {
                let receipt = if let Answer::Admitted(receipt) = answer {
                    receipt
                } else {
                    0
                };
                self.inflight.remove(&id);
                self.avoiding.remove(&inflight.to);
                self.misses.remove(&inflight.to);
                if inflight.give_back {
                    return (None, Vec::new());
                }
                if let Some(group) = inflight.group {
                    let (ended, back) = self.part_ended(group, inflight.key, Some(receipt), now);
                    return (ended, self.give_backs(back, now, pods, rng));
                }
                (
                    Some((
                        Self::parts(&inflight, id),
                        Outcome::Counted(now - inflight.started),
                    )),
                    Vec::new(),
                )
            }
            // Refused under its rule, charged nothing: what other parts were charged given back.
            Answer::Limited => {
                self.inflight.remove(&id);
                self.avoiding.remove(&inflight.to);
                self.misses.remove(&inflight.to);
                if let Some(group) = inflight.group {
                    if let Some(entry) = self.groups.get_mut(&group) {
                        entry.limited = true;
                    }
                    let (ended, back) = self.part_ended(group, inflight.key, None, now);
                    return (ended, self.give_backs(back, now, pods, rng));
                }
                (
                    Some((Self::parts(&inflight, id), Outcome::Limited)),
                    Vec::new(),
                )
            }
            Answer::Elsewhere(table) => {
                if let Some(table) = table {
                    self.learn(table);
                }
                let resend = self.retry(id, now, pods, rng, Some(inflight.to));
                (None, resend.into_iter().collect())
            }
            // Not serving: not counted. The pod may soon serve, or the table soon move the
            // shard; asked again in a millisecond.
            Answer::NotServing => {
                if let Some(entry) = self.inflight.get_mut(&id) {
                    entry.again = Some(now + 1_000_000);
                }
                (None, Vec::new())
            }
        }
    }

    /// The keys of request `id`, each with the request's number.
    fn parts(inflight: &Inflight, id: u64) -> Vec<(u64, u64)> {
        if inflight.keys.is_empty() {
            vec![(inflight.key, id)]
        } else {
            inflight.keys.iter().map(|key| (*key, id)).collect()
        }
    }

    /// Sends request `id` again, to the owner its table names or else a pod not `avoid`.
    fn retry(
        &mut self,
        id: u64,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
        avoid: Option<NodeId>,
    ) -> Option<(NodeId, Message)> {
        let inflight = self.inflight.get(&id)?.clone();
        let Some(mut to) = self.target(inflight.key, pods, rng) else {
            self.wait(id, now);
            return None;
        };
        if Some(to) == avoid
            && self
                .table
                .as_ref()
                .and_then(|table| table.owner(shard_of(inflight.key, self.shards)))
                != Some(to)
        {
            let others: Vec<NodeId> = pods
                .iter()
                .copied()
                .filter(|pod| Some(*pod) != avoid && !self.is_down(*pod))
                .collect();
            if let Some(other) = usize::try_from(rng.below(others.len() as u64))
                .ok()
                .and_then(|index| others.get(index))
            {
                to = *other;
            }
        }
        let entry = self.inflight.get_mut(&id)?;
        entry.sent = now;
        entry.to = to;
        entry.again = None;
        entry.missed = false;
        Some((to, self.request(id, inflight.key)))
    }

    /// Request `id` could not reach the pod it was sent to — its stream to it down, the pod
    /// gone — so that pod never had it: sent another way, to be forwarded, and the pod avoided
    /// as a data plane's breaker would.
    pub fn unreachable(
        &mut self,
        id: u64,
        pod: NodeId,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
    ) -> Option<(NodeId, Message)> {
        self.now = now;
        if self
            .inflight
            .get(&id)
            .is_none_or(|inflight| inflight.to != pod)
        {
            return None;
        }
        self.avoiding.insert(pod, now + AVOID_FOR);
        self.down.insert(pod, now + AVOID_FOR);
        self.retry_anywhere(id, now, pods, rng, pod)
    }

    /// At `now`: requests past their deadline refused, those a pod said it did not count asked
    /// again, and attempts unanswered for `attempt` counted against their pod's breaker — never
    /// sent again, as the owner may have counted them.
    pub fn check(&mut self, now: Time, pods: &[NodeId], rng: &mut Rng) -> (Vec<Ended>, Vec<Send>) {
        self.now = now;
        let mut ended = Vec::new();
        let mut sends = Vec::new();
        // Its pod gone, a request left so has its stream gone too: no answer is coming.
        self.left.retain(|_, (_, pod)| pods.contains(pod));
        let ids: Vec<u64> = self.inflight.keys().copied().collect();
        for id in ids {
            let Some(inflight) = self.inflight.get(&id).cloned() else {
                continue;
            };
            if inflight.give_back {
                let wait = if inflight.unanswered == 0 {
                    GIVE_BACK_AGAIN
                } else {
                    GIVE_BACK_LATER
                };
                if now.saturating_sub(inflight.sent) >= wait {
                    let unanswered = inflight.unanswered + 1;
                    if unanswered >= GIVE_BACK_SENDS {
                        self.inflight.remove(&id);
                        continue;
                    }
                    if let Some(entry) = self.inflight.get_mut(&id) {
                        entry.unanswered = unanswered;
                    }
                    sends.extend(self.retry_anywhere(id, now, pods, rng, inflight.to));
                }
                continue;
            }
            if now.saturating_sub(inflight.started) >= self.deadline {
                self.inflight.remove(&id);
                let keys = if inflight.keys.is_empty() {
                    vec![inflight.key]
                } else {
                    inflight.keys.clone()
                };
                self.left.insert(id, (keys.clone(), inflight.to));
                if let Some(group) = inflight.group {
                    let (whole, back) = self.part_ended(group, inflight.key, None, now);
                    ended.extend(whole);
                    sends.extend(self.give_backs(back, now, pods, rng));
                } else {
                    let parts = keys.into_iter().map(|key| (key, id)).collect();
                    ended.push((parts, Outcome::Refused));
                }
                continue;
            }
            // Not counted: asked again of another pod, which knows the owner if its table is
            // later — the pod asked may be cut off from the rest, its table behind.
            if inflight.again.is_some_and(|at| now >= at) {
                sends.extend(self.retry_anywhere(id, now, pods, rng, inflight.to));
                continue;
            }
            // A request sent whole is waited on for three quarters of its deadline: the pod
            // gathering it answers once its parts are in.
            let attempt = if inflight.keys.is_empty() {
                self.attempt
            } else {
                self.deadline / 4 * 3
            };
            if !inflight.missed && now.saturating_sub(inflight.sent) >= attempt {
                let misses = self.misses.entry(inflight.to).or_insert(0);
                *misses += 1;
                if *misses >= AVOID_AFTER {
                    self.avoiding.insert(inflight.to, now + AVOID_FOR);
                }
                // Counted once against it, and waited on to its deadline.
                if let Some(entry) = self.inflight.get_mut(&id) {
                    entry.missed = true;
                }
            }
        }
        (ended, sends)
    }

    fn retry_anywhere(
        &mut self,
        id: u64,
        now: Time,
        pods: &[NodeId],
        rng: &mut Rng,
        avoid: NodeId,
    ) -> Option<(NodeId, Message)> {
        let inflight = self.inflight.get(&id)?.clone();
        let others: Vec<NodeId> = pods
            .iter()
            .copied()
            .filter(|pod| *pod != avoid && !self.is_down(*pod))
            .collect();
        // Else the pod avoided, if its stream is not down; else it waits.
        let pool = if others.is_empty() {
            pods.iter()
                .copied()
                .filter(|pod| *pod == avoid && !self.is_down(*pod))
                .collect()
        } else {
            others
        };
        let Some(to) = usize::try_from(rng.below(pool.len().max(1) as u64))
            .ok()
            .and_then(|index| pool.get(index).copied())
        else {
            self.wait(id, now);
            return None;
        };
        let entry = self.inflight.get_mut(&id)?;
        entry.sent = now;
        entry.to = to;
        entry.again = None;
        entry.missed = false;
        Some((to, self.request(id, inflight.key)))
    }

    /// Whether its stream to `pod` is down now.
    fn is_down(&self, pod: NodeId) -> bool {
        self.down.get(&pod).is_some_and(|until| *until > self.now)
    }
}
