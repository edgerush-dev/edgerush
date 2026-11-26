//! A client standing for a data plane: requests at a steady rate under keys of its own, each
//! sent to the owner its table names, sent another way only when it could not reach the pod
//! — never when its answer is late, as the owner may have counted it — and given up at its
//! deadline. Another way is the pod a data plane would choose, by the key, so that every
//! client sends a key the same way.
//!
//! It checks every pod it may ask, as a data plane's checking thread does (docs/06 §4): one
//! check at a time, each an interval after the last ended, but at once after a miss, and at
//! once — at most once each interval — when a request to the pod goes unanswered in time or
//! cannot reach it. Checks missed in a row, or one refused, its link cut or its pod gone,
//! mark the pod down, and its keys go another way until a check is answered. Nothing else
//! marks a pod down: such a request decides only itself, and asks for a check.

use edgerush_cluster::table::{Table, shard_of};
use edgerush_cluster::{Key, NodeId, Time};
use edgerush_cluster_synchronous::{Answer, Rule};

/// The synchronous mode's messages, over the store of both modes' values.
pub type Message = edgerush_cluster_synchronous::Message<edgerush_cluster_modes::Value>;
use std::collections::{BTreeMap, BTreeSet};

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
pub type Ended = (Vec<(Key, u64)>, Outcome);

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
    key: Key,
    /// The request it is a part of, split by key, if it is one.
    group: Option<u64>,
    /// Every key, for a request sent whole to be gathered.
    keys: Vec<Key>,
    started: Time,
    /// When the attempt now in flight was sent.
    sent: Time,
    /// The pod it was sent to.
    to: NodeId,
    /// When to ask again, the pod having answered that it did not count it.
    again: Option<Time>,
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
    keys: Vec<Key>,
    /// The time between its requests.
    pub interval: Time,
    /// How long a request may take before it is refused.
    pub deadline: Time,
    table: Option<Table>,
    next: u64,
    inflight: BTreeMap<u64, Inflight>,
    shards: u16,
    /// When it last sent a request.
    pub last_sent: Option<Time>,
    /// Whether each request is under a second key shared by every client, and how it is asked.
    multi: Option<(Multi, Key)>,
    /// Requests split by key: when each began, its parts still out, its keys counted, and
    /// whether a part was refused.
    groups: BTreeMap<u64, Group>,
    /// Parts counted for requests then refused.
    pub orphaned: u64,
    /// Pods its checks have marked down: their keys go another way, to be forwarded.
    down: BTreeSet<NodeId>,
    /// Each pod's checks.
    checks: BTreeMap<NodeId, Checking>,
    /// How it checks the pods.
    pub checking: Checks,
    /// The number of its last check.
    checked: u64,
    /// Its clock as of its last event.
    now: Time,
    /// Requests it has made, parts and give-backs aside.
    pub requests: u64,
    /// Requests left to `when_unreachable`, with their keys and the pod each was last sent
    /// to: waited on until answered — a "counted" given back, unless `allow` admitted it — or
    /// its pod is gone, its stream with it, as an ask given up on stays on its stream in a data
    /// plane.
    left: BTreeMap<u64, (Vec<Key>, NodeId)>,
    /// The rule its requests are held to by their keys' owners, if any.
    rule: Option<Rule>,
    /// Whether `when_unreachable` admits a request left to it, under the rule: one that then
    /// goes on, what its owners charged standing, as a data plane's does.
    allow: bool,
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

/// How a client checks the pods, as a data plane's `health_check` says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checks {
    /// From the end of one check to the start of the next.
    pub interval: Time,
    /// How long a check waits for its answer before it is missed.
    pub timeout: Time,
    /// Checks missed in a row that mark a pod down.
    pub misses: u32,
}

impl Default for Checks {
    /// The bench's: every 100 ms, 200 ms each, three in a row.
    fn default() -> Self {
        Self {
            interval: 100_000_000,
            timeout: 200_000_000,
            misses: 3,
        }
    }
}

/// One pod's checks.
#[derive(Clone, Copy, Debug, Default)]
struct Checking {
    /// The check out, by its number, and when it was sent.
    out: Option<(u64, Time)>,
    /// When the next is due.
    due: Time,
    /// Checks missed in a row.
    misses: u32,
    /// A request to the pod went wrong since the last check began: a check asked for.
    wanted: bool,
    /// When the last check asked for so was due.
    asked_at: Option<Time>,
}

impl Checking {
    /// The next check brought forward for a request gone wrong, if one was: to `now`, but no
    /// sooner than an interval after the last asked for so.
    fn bring_forward(&mut self, now: Time, interval: Time) {
        if !self.wanted || self.out.is_some() {
            return;
        }
        let at = self
            .asked_at
            .map_or(now, |asked| asked.saturating_add(interval).max(now));
        if at < self.due {
            self.due = at;
            self.asked_at = Some(at);
        }
    }
}

/// A request split by key.
#[derive(Clone, Debug, Default)]
struct Group {
    out: usize,
    /// Every key it is under, each with its part's number; and those of its parts charged,
    /// each with its receipt.
    keys: Vec<(Key, u64)>,
    counted: Vec<(Key, u64)>,
    refused: bool,
    /// Whether a part was refused under its rule.
    limited: bool,
    started: Time,
}

impl Client {
    /// Client `id`, requesting under `keys` in turn every `interval`, each given `deadline`.
    #[must_use]
    pub fn new(id: NodeId, keys: Vec<Key>, interval: Time, deadline: Time, shards: u16) -> Self {
        Self {
            id,
            keys,
            interval,
            deadline,
            table: None,
            next: 0,
            inflight: BTreeMap::new(),
            shards,
            last_sent: None,
            multi: None,
            groups: BTreeMap::new(),
            orphaned: 0,
            down: BTreeSet::new(),
            checks: BTreeMap::new(),
            checking: Checks::default(),
            checked: 0,
            now: 0,
            requests: 0,
            left: BTreeMap::new(),
            rule: None,
            allow: false,
        }
    }

    /// Each request from now held to `rule` by its key's owner, one left to
    /// `when_unreachable` admitted if `allow`.
    pub const fn set_limit(&mut self, rule: Rule, allow: bool) {
        self.rule = Some(rule);
        self.allow = allow;
    }

    /// Each request from now also under `shared`, asked as `multi` says.
    pub fn set_multi(&mut self, multi: Multi, shared: Key) {
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

    /// Where it would send a request under `key`: of `pods`, those not marked down, as
    /// [`choose`](Self::choose) chooses; none if every one is, as a data plane's plane then has
    /// none to ask.
    fn target(&self, key: Key, pods: &[NodeId]) -> Option<NodeId> {
        let open: Vec<NodeId> = pods
            .iter()
            .copied()
            .filter(|pod| !self.is_down(*pod))
            .collect();
        self.choose(key, &open)
    }

    /// Of `open`, the pods `key` may be sent to now, the one a data plane's `Plane::through`
    /// chooses: the key's owner, as its table names it; else a replica of the key's shard,
    /// holding a copy, chosen by the key; else another member, chosen by the key; else, its
    /// table naming none of them, one of `open` chosen by the key. Chosen by the key, every
    /// client sends a key the same way, as every data plane does.
    fn choose(&self, key: Key, open: &[NodeId]) -> Option<NodeId> {
        let shard = self
            .table
            .as_ref()
            .and_then(|table| table.shards.get(usize::from(shard_of(key, self.shards))));
        if let Some(owner) = shard
            .and_then(|shard| shard.owner)
            .filter(|owner| open.contains(owner))
        {
            return Some(owner);
        }
        let replicas: Vec<NodeId> = shard
            .into_iter()
            .flat_map(|shard| &shard.replicas)
            .copied()
            .filter(|replica| open.contains(replica))
            .collect();
        let members: Vec<NodeId> = self
            .table
            .iter()
            .flat_map(|table| table.members.keys())
            .copied()
            .filter(|member| open.contains(member))
            .collect();
        chosen_by(key, &replicas)
            .or_else(|| chosen_by(key, &members))
            .or_else(|| chosen_by(key, open))
    }

    /// The pod its table names as `key`'s owner, if any.
    #[must_use]
    pub fn owner(&self, key: Key) -> Option<NodeId> {
        self.table
            .as_ref()
            .and_then(|table| table.owner(shard_of(key, self.shards)))
    }

    /// Request `id` with no pod to send it to, every stream down: waits, tried again in a
    /// millisecond, and left to `when_unreachable` at its deadline if none comes back.
    fn wait(&mut self, id: u64, now: Time) {
        if let Some(entry) = self.inflight.get_mut(&id) {
            entry.to = NOBODY;
            entry.sent = now;
            entry.again = Some(now + WAIT);
        }
    }

    fn request(&self, id: u64, key: Key) -> Message {
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
    fn give_back(&mut self, key: Key, receipt: u64, now: Time, pods: &[NodeId]) -> Option<Send> {
        let id = self.next;
        self.next += 1;
        let to = self.target(key, pods);
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
    pub fn send(&mut self, now: Time, pods: &[NodeId]) -> Option<(NodeId, Message)> {
        self.now = now;
        self.last_sent = Some(now);
        let key = *self
            .keys
            .get(usize::try_from(self.next).ok()? % self.keys.len().max(1))?;
        let id = self.next;
        self.next += 1;
        self.requests += 1;
        let to = self.target(key, pods);
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
    pub fn send_split(&mut self, now: Time, pods: &[NodeId]) -> Vec<(NodeId, Message)> {
        self.now = now;
        self.last_sent = Some(now);
        let Some((Multi::Split, shared)) = self.multi else {
            return self.send(now, pods).into_iter().collect();
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
            let to = self.target(part, pods);
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
        key: Key,
        charged: Option<u64>,
        now: Time,
    ) -> (Option<Ended>, Vec<(Key, u64)>) {
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
            // Admitted under `allow`, it went on: what its owners charged stands.
            if self.allow {
                return (Some((entry.keys, Outcome::Refused)), Vec::new());
            }
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
    fn give_backs(&mut self, keys: Vec<(Key, u64)>, now: Time, pods: &[NodeId]) -> Vec<Send> {
        keys.into_iter()
            .filter_map(|(key, receipt)| self.give_back(key, receipt, now, pods))
            .collect()
    }

    /// A pod's `answer` to request `id`: done, or sent again where the answer points; and what
    /// to send, give-backs among it. The key of a request counted is given back with how it
    /// ended; a "counted" coming after its request was left to `when_unreachable` is given
    /// back to the keys' owners, unless `allow` admitted it.
    pub fn answered(
        &mut self,
        id: u64,
        answer: Answer,
        now: Time,
        pods: &[NodeId],
    ) -> (Option<Ended>, Vec<Send>) {
        self.now = now;
        let Some(inflight) = self.inflight.get(&id).cloned() else {
            // A request left to `when_unreachable` answered at last: given back if charged and
            // refused, settled either way; admitted under `allow`, it went on, the charge
            // standing.
            let receipt = match answer {
                Answer::Counted(_) => Some(0),
                Answer::Admitted(receipt) => Some(receipt),
                _ => None,
            };
            if let Some((keys, _)) = self.left.remove(&id)
                && let Some(receipt) = receipt
                && !self.allow
            {
                let back = keys.into_iter().map(|key| (key, receipt)).collect();
                return (None, self.give_backs(back, now, pods));
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
                if inflight.give_back {
                    return (None, Vec::new());
                }
                if let Some(group) = inflight.group {
                    let (ended, back) = self.part_ended(group, inflight.key, Some(receipt), now);
                    return (ended, self.give_backs(back, now, pods));
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
            Answer::Limited { .. } => {
                self.inflight.remove(&id);
                if let Some(group) = inflight.group {
                    if let Some(entry) = self.groups.get_mut(&group) {
                        entry.limited = true;
                    }
                    let (ended, back) = self.part_ended(group, inflight.key, None, now);
                    return (ended, self.give_backs(back, now, pods));
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
                let resend = self.retry(id, now, pods, Some(inflight.to));
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
    fn parts(inflight: &Inflight, id: u64) -> Vec<(Key, u64)> {
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
        avoid: Option<NodeId>,
    ) -> Option<(NodeId, Message)> {
        let inflight = self.inflight.get(&id)?.clone();
        let Some(mut to) = self.target(inflight.key, pods) else {
            self.wait(id, now);
            return None;
        };
        if Some(to) == avoid && self.owner(inflight.key) != Some(to) {
            let others: Vec<NodeId> = pods
                .iter()
                .copied()
                .filter(|pod| Some(*pod) != avoid && !self.is_down(*pod))
                .collect();
            if let Some(other) = self.choose(inflight.key, &others) {
                to = other;
            }
        }
        let entry = self.inflight.get_mut(&id)?;
        entry.sent = now;
        entry.to = to;
        entry.again = None;
        Some((to, self.request(id, inflight.key)))
    }

    /// Request `id` could not reach the pod it was sent to — its stream to it down, the pod
    /// gone — so that pod never had it: sent another way, to be forwarded, never to that pod
    /// again. That marks the pod down for no other request: only its checks do.
    pub fn unreachable(
        &mut self,
        id: u64,
        pod: NodeId,
        now: Time,
        pods: &[NodeId],
    ) -> Option<(NodeId, Message)> {
        self.now = now;
        if self
            .inflight
            .get(&id)
            .is_none_or(|inflight| inflight.to != pod)
        {
            return None;
        }
        self.trouble(pod, now);
        self.retry_anywhere(id, now, pods, pod, false)
    }

    /// At `now`, the checks to send, each to its pod with its number: one to each of `pods`
    /// with none out and its interval since the last ended gone by, a pod first met checked at
    /// once. A check out past its timeout is missed first, the `misses`th in a row marking its
    /// pod down.
    pub fn checks_due(&mut self, now: Time, pods: &[NodeId]) -> Vec<(NodeId, u64)> {
        self.now = now;
        let mut due = Vec::new();
        for pod in pods {
            let checks = self.checking;
            let mut checking = self.checks.get(pod).copied().unwrap_or(Checking {
                due: now,
                ..Checking::default()
            });
            if let Some((_, sent)) = checking.out
                && now.saturating_sub(sent) >= checks.timeout
            {
                checking.out = None;
                checking.misses = checking.misses.saturating_add(1);
                // Missed: checked again at once, but an interval on once marked down.
                checking.due = if checking.misses >= checks.misses {
                    self.down.insert(*pod);
                    now + checks.interval
                } else {
                    now
                };
                checking.bring_forward(now, checks.interval);
            }
            if checking.out.is_none() && now >= checking.due {
                self.checked += 1;
                checking.out = Some((self.checked, now));
                checking.wanted = false;
                due.push((*pod, self.checked));
            }
            self.checks.insert(*pod, checking);
        }
        due
    }

    /// Pod `pod`'s answer to check `id`, at `now`: if it is the check out, the pod is up.
    pub fn check_answered(&mut self, pod: NodeId, id: u64, now: Time) {
        self.now = now;
        let interval = self.checking.interval;
        if let Some(checking) = self.checks.get_mut(&pod)
            && checking.out.is_some_and(|(out, _)| out == id)
        {
            checking.out = None;
            checking.due = now + interval;
            checking.misses = 0;
            checking.bring_forward(now, interval);
            self.down.remove(&pod);
        }
    }

    /// A request to `pod` went wrong at `now` — unanswered by its deadline, or unable to reach
    /// it — which asks for its pod to be checked at once, at most once each interval.
    fn trouble(&mut self, pod: NodeId, now: Time) {
        let interval = self.checking.interval;
        let checking = self.checks.entry(pod).or_insert(Checking {
            due: now,
            ..Checking::default()
        });
        checking.wanted = true;
        checking.bring_forward(now, interval);
    }

    /// Check `id` of `pod` refused at `now`, its link cut or the pod gone: if it is the check
    /// out, the pod is down at once.
    pub fn check_refused(&mut self, pod: NodeId, id: u64, now: Time) {
        self.now = now;
        let interval = self.checking.interval;
        if let Some(checking) = self.checks.get_mut(&pod)
            && checking.out.is_some_and(|(out, _)| out == id)
        {
            checking.out = None;
            checking.due = now + interval;
            checking.bring_forward(now, interval);
            self.down.insert(pod);
        }
    }

    /// At `now`: requests past their deadline refused, and those a pod said it did not count
    /// asked again. One unanswered is waited on to its deadline, never sent again, as the
    /// owner may have counted it, and marks its pod down for nothing.
    pub fn check(&mut self, now: Time, pods: &[NodeId]) -> (Vec<Ended>, Vec<Send>) {
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
                    sends.extend(self.retry_anywhere(id, now, pods, inflight.to, true));
                }
                continue;
            }
            if now.saturating_sub(inflight.started) >= self.deadline {
                self.inflight.remove(&id);
                if inflight.to != NOBODY {
                    self.trouble(inflight.to, now);
                }
                let keys = if inflight.keys.is_empty() {
                    vec![inflight.key]
                } else {
                    inflight.keys.clone()
                };
                self.left.insert(id, (keys.clone(), inflight.to));
                if let Some(group) = inflight.group {
                    let (whole, back) = self.part_ended(group, inflight.key, None, now);
                    ended.extend(whole);
                    sends.extend(self.give_backs(back, now, pods));
                } else {
                    let parts = keys.into_iter().map(|key| (key, id)).collect();
                    ended.push((parts, Outcome::Refused));
                }
                continue;
            }
            // Not counted: asked again of another pod, which knows the owner if its table is
            // later — the pod asked may be cut off from the rest, its table behind.
            if inflight.again.is_some_and(|at| now >= at) {
                sends.extend(self.retry_anywhere(id, now, pods, inflight.to, true));
            }
        }
        (ended, sends)
    }

    /// Sends request `id` again, to a pod not `avoid` and not marked down, as
    /// [`choose`](Self::choose) chooses; else, if `or_avoid`, to `avoid` if it is not marked
    /// down; else it waits.
    fn retry_anywhere(
        &mut self,
        id: u64,
        now: Time,
        pods: &[NodeId],
        avoid: NodeId,
        or_avoid: bool,
    ) -> Option<(NodeId, Message)> {
        let inflight = self.inflight.get(&id)?.clone();
        let others: Vec<NodeId> = pods
            .iter()
            .copied()
            .filter(|pod| *pod != avoid && !self.is_down(*pod))
            .collect();
        let pool = if others.is_empty() && or_avoid {
            pods.iter()
                .copied()
                .filter(|pod| *pod == avoid && !self.is_down(*pod))
                .collect()
        } else {
            others
        };
        let Some(to) = self.choose(inflight.key, &pool) else {
            self.wait(id, now);
            return None;
        };
        let entry = self.inflight.get_mut(&id)?;
        entry.sent = now;
        entry.to = to;
        entry.again = None;
        Some((to, self.request(id, inflight.key)))
    }

    /// Whether its checks have marked `pod` down.
    #[must_use]
    pub fn is_down(&self, pod: NodeId) -> bool {
        self.down.contains(&pod)
    }
}

/// The one of `among` that `key` chooses, as a data plane's `chosen_by` does, if there is any.
fn chosen_by(key: Key, among: &[NodeId]) -> Option<NodeId> {
    let count = Key::try_from(among.len()).ok().filter(|count| *count > 0)?;
    among.get(usize::try_from(key % count).ok()?).copied()
}
