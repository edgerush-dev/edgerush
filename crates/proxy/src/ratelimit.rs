//! The rate limits route rules are under (07 §2): each worker is a data plane of the
//! rate-limiting service's protocol (`edgerush_ratelimit::service`), deciding every request
//! under all its limits at once from its copies, asking a service pod about keys it holds no
//! fresh copy of, and sending the service, every cycle, what it admitted — over gRPC, from a
//! task of the worker's own.
//!
//! Nothing here runs unless a rule limits: the worker makes its limiter the first time one
//! does, and makes it again when the service's settings change, which starts every key
//! afresh, as any change to a limit does.
//!
//! Layered (experimental, 12 item 3), the process alone is a data plane, on a thread of its
//! own ([`hub`]), and each worker decides from slices of its room, syncing every millisecond.

mod hub;

pub(crate) use hub::Hub;

use crate::forwarding::Client as Requester;
use crate::request::NoTables;
use edgerush_config::{CompiledLimit, CompiledRateLimitService};
use edgerush_ratelimit::service::layer::{Local, Worker};
use edgerush_ratelimit::service::shared::Worker as Holder;
use edgerush_ratelimit::service::{
    Contract, DataPlane, Decided, Decision, Keyed, Ticket, View, longest,
};
use edgerush_ratelimit_service::grpc::Client;
use edgerush_rules::{Address, Context, KeyValue, Named, Outcome, Text};
use http::HeaderName;
use hub::SYNC;
pub(crate) use hub::{FromHub, ToHub};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc, oneshot};

/// How long a worker waits for the owner's answer to an ask before deciding what waits from
/// its copy: long enough for TCP to send a lost segment again (some 200 ms on Linux), as the
/// comparison harness found that the protocol's own four cycles, 40 ms, were not.
const ASK_TIMEOUT: u64 = 300_000_000;

/// Why a request was refused, as the worker's copies have it.
pub(crate) struct Refusal<'a> {
    /// The limits that refused it, bit `n` for the rule's `n`th: those out of room. None
    /// when the copies show room under every one, the service pod having refused it from
    /// counts the copies have yet to catch up with.
    pub(crate) by: u64,
    /// The limit that waits longest to admit it, the first of those that wait as long, and
    /// how long: when to come back.
    pub(crate) longest: Option<(&'a CompiledLimit, Duration)>,
}

/// The IETF `RateLimit` field (draft-ietf-httpapi-ratelimit-headers): on a refusal, the
/// limit that refused, none of it left, and for how long.
pub(crate) const RATELIMIT: HeaderName = HeaderName::from_static("ratelimit");

/// What a limit's key reads of a request: the client's address, until authentication
/// gives the rest (stage 9).
struct ByKey<'a> {
    client: &'a Requester,
}

impl Context for ByKey<'_> {
    fn text(&self, _: Text) -> Option<&str> {
        None
    }

    fn address(&self, field: Address) -> Option<IpAddr> {
        match field {
            Address::Client => Some(self.client.address()),
            Address::Peer => self.client.peer().map(|peer| peer.ip()),
        }
    }

    fn named(&self, _: Named, _: &str) -> Option<&str> {
        None
    }
}

/// Puts together in `buffer` the key a request is counted by under the limit `name`
/// defined as `definition` says: the name, a zero, the definition's digest, and the key's
/// value — an IPv6 client by its /64, not its whole address, which one client holds 2^64
/// of (06 §2). A limit changed has keys of its own, which start afresh.
fn key_into(buffer: &mut Vec<u8>, name: &str, definition: [u8; 8], key: KeyValue<'_>) {
    buffer.clear();
    buffer.extend_from_slice(name.as_bytes());
    buffer.push(0);
    buffer.extend_from_slice(&definition);
    match key {
        KeyValue::Text(text) => buffer.extend_from_slice(text.as_bytes()),
        KeyValue::Address(IpAddr::V4(v4)) => buffer.extend_from_slice(&v4.octets()),
        KeyValue::Address(IpAddr::V6(v6)) => buffer.extend_from_slice(&v6.octets()[..8]),
    }
}

/// How a request fared against the limits it is under.
pub(crate) enum Limited {
    /// Within it.
    Admit,
    /// Over it.
    Refuse,
    /// Asked about: the owner's decision comes, with which keys refused it if it did. A
    /// sender dropped (the limiter gone) refuses it.
    Wait(oneshot::Receiver<Decided>),
}

/// A worker's limiter: as a data plane of its own, or deciding from its process's slices.
pub(crate) struct Limiter {
    service: CompiledRateLimitService,
    mode: Mode,
    /// The requests waiting for a decision from elsewhere, by ticket or ask: dropped with
    /// the limiter, which refuses them.
    waiting: RefCell<HashMap<Ticket, oneshot::Sender<Decided>>>,
    /// Where each of a request's keys is put together, one for each limit it can be under,
    /// kept so that no request makes them afresh; and their contracts.
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<Contract>>,
    /// The clock: the worker's own, or, layered, its process's.
    start: Instant,
    /// Wakes the task to send what a decision asked for at once.
    asked: Notify,
    stopped: Cell<bool>,
}

/// How a worker's limiter decides.
#[expect(
    clippy::large_enum_variant,
    reason = "one limiter a worker, whose size matters not; a box would put a pointer between               every decision and its data plane"
)]
enum Mode {
    /// A data plane of its own, and the clients of the service's pods.
    Apart {
        plane: RefCell<DataPlane<Keyed>>,
        clients: Vec<Client>,
    },
    /// From the slices of its process's room (12 item 3).
    Layered {
        hub: Arc<Hub>,
        /// Which of the process's workers it is.
        index: usize,
        slices: RefCell<Worker<Keyed>>,
        /// The next ask's number.
        next: Cell<u64>,
    },
    /// From each key's one room, which every worker of its process takes from (12 item 3).
    Shared {
        hub: Arc<Hub>,
        /// Which of the process's workers it is.
        index: usize,
        rooms: RefCell<Holder<Keyed>>,
        /// The next ask's number.
        next: Cell<u64>,
    },
}

impl Limiter {
    /// A limiter for worker `worker` reaching `service`, its keys holding at most
    /// `max_key_bytes`, its task started; `None` if an address cannot be made into one to
    /// connect to.
    pub(crate) fn start(
        service: &CompiledRateLimitService,
        worker: u16,
        max_key_bytes: u64,
    ) -> Option<Rc<Self>> {
        let clients = service
            .pods
            .iter()
            .map(|address| Client::new(address))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        // A data plane is one worker in one life: the time it began, and its number.
        let began = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            });
        let id = began.wrapping_mul(1_024).wrapping_add(u64::from(worker));
        let pods = u32::try_from(clients.len()).unwrap_or(u32::MAX);
        let view = View {
            epoch: 0,
            pods: (0..pods).collect(),
        };
        let cycle = u64::try_from(service.cycle.as_nanos()).unwrap_or(u64::MAX);
        // Keys are what clients send: hashed under a key of the worker's own, drawn at random.
        let entropy = RandomState::new();
        let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
        let mut plane = DataPlane::with_hasher(id, cycle, view, hasher);
        plane.set_ask_timeout(ASK_TIMEOUT);
        plane.set_capacity(max_key_bytes);
        let limiter = Rc::new(Self {
            service: service.clone(),
            mode: Mode::Apart {
                plane: RefCell::new(plane),
                clients,
            },
            waiting: RefCell::new(HashMap::new()),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
            start: Instant::now(),
            asked: Notify::new(),
            stopped: Cell::new(false),
        });
        tokio::task::spawn_local(Rc::clone(&limiter).run());
        Some(limiter)
    }

    /// A limiter for worker `index` of the process `hub`, deciding from the slices it gives,
    /// its task started (12 item 3).
    pub(crate) fn layered(
        service: &CompiledRateLimitService,
        hub: Arc<Hub>,
        index: usize,
    ) -> Rc<Self> {
        let (answers, answered) = mpsc::unbounded_channel();
        let _gone = hub.to.send(ToHub::Join {
            worker: index,
            answers,
        });
        let entropy = RandomState::new();
        let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
        let start = hub.epoch();
        let limiter = Rc::new(Self {
            service: service.clone(),
            mode: Mode::Layered {
                hub,
                index,
                slices: RefCell::new(Worker::with_hasher(hasher)),
                next: Cell::new(0),
            },
            waiting: RefCell::new(HashMap::new()),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
            start,
            asked: Notify::new(),
            stopped: Cell::new(false),
        });
        tokio::task::spawn_local(Rc::clone(&limiter).run_layered(answered));
        limiter
    }

    /// A limiter for worker `index` of the process `hub`, taking from the rooms it hands
    /// out, its task started (12 item 3).
    pub(crate) fn shared(
        service: &CompiledRateLimitService,
        hub: Arc<Hub>,
        index: usize,
    ) -> Rc<Self> {
        let (answers, answered) = mpsc::unbounded_channel();
        let _gone = hub.to.send(ToHub::Join {
            worker: index,
            answers,
        });
        let entropy = RandomState::new();
        let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
        let start = hub.epoch();
        let limiter = Rc::new(Self {
            service: service.clone(),
            mode: Mode::Shared {
                hub,
                index,
                rooms: RefCell::new(Holder::with_hasher(hasher)),
                next: Cell::new(0),
            },
            waiting: RefCell::new(HashMap::new()),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
            start,
            asked: Notify::new(),
            stopped: Cell::new(false),
        });
        tokio::task::spawn_local(Rc::clone(&limiter).run_shared(answered));
        limiter
    }

    /// Whether it reaches `service` as it is set now: the same pods, the same cycle, the
    /// same layering, keep and sharing. A change of what its keys may hold is taken as it is
    /// ([`Self::set_capacity`]).
    pub(crate) fn serves(&self, service: &CompiledRateLimitService) -> bool {
        self.service.pods == service.pods
            && self.service.cycle == service.cycle
            && self.service.layered == service.layered
            && self.service.layered_keep == service.layered_keep
            && self.service.shared == service.shared
    }

    /// The bytes its keys may hold from now on: layered, the process's, which its first
    /// worker passes on.
    pub(crate) fn set_capacity(&self, max_key_bytes: u64) {
        match &self.mode {
            Mode::Apart { plane, .. } => plane.borrow_mut().set_capacity(max_key_bytes),
            Mode::Layered { hub, index: 0, .. } | Mode::Shared { hub, index: 0, .. } => {
                let _gone = hub.to.send(ToHub::Capacity(max_key_bytes));
            }
            Mode::Layered { .. } | Mode::Shared { .. } => {}
        }
    }

    /// What its keys hold, in bytes, how many there are, and how many it has evicted, all
    /// told: for the worker's sweep to say. Layered, the process's, said by its first worker
    /// alone, so that the workers' sum is the process's.
    pub(crate) fn usage(&self) -> (u64, usize, u64) {
        match &self.mode {
            Mode::Apart { plane, .. } => {
                let plane = plane.borrow();
                (plane.holding(), plane.key_count(), plane.evicted())
            }
            Mode::Layered { hub, index: 0, .. } | Mode::Shared { hub, index: 0, .. } => hub.usage(),
            Mode::Layered { .. } | Mode::Shared { .. } => (0, 0, 0),
        }
    }

    /// Stops its task: a limiter for other settings takes its place.
    pub(crate) fn stop(&self) {
        self.stopped.set(true);
        self.asked.notify_one();
    }

    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Decides a request from `client` under those of `limits` that `limited` marks, bit `n`
    /// for the `n`th: each counted by its key — the limit's name, a zero, the key's value —
    /// and all decided at once, admitted under all of them or none (06 §2). A limit the
    /// request has no key for lets it through, as one whose number came out below one.
    pub(crate) fn decide_all(
        &self,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: &Requester,
    ) -> Limited {
        let now = self.now();
        match &self.mode {
            Mode::Layered {
                hub,
                index,
                slices,
                next,
            } => {
                return self.with_keys(limits, limited, client, |parts| {
                    if parts.is_empty() {
                        return Limited::Admit;
                    }
                    let local = slices.borrow_mut().decide(parts, now);
                    self.or_ask(local, hub, *index, next, parts)
                });
            }
            Mode::Shared {
                hub,
                index,
                rooms,
                next,
            } => {
                return self.with_keys(limits, limited, client, |parts| {
                    if parts.is_empty() {
                        return Limited::Admit;
                    }
                    let local = rooms.borrow_mut().decide(parts, now);
                    self.or_ask(local, hub, *index, next, parts)
                });
            }
            Mode::Apart { .. } => {}
        }
        let Mode::Apart { plane, .. } = &self.mode else {
            return Limited::Admit;
        };
        let decision = self.with_keys(limits, limited, client, |parts| {
            let mut plane = plane.borrow_mut();
            match parts {
                [] => None,
                [(key, contract)] => Some(plane.decide(key, *contract, now)),
                parts => Some(plane.decide_all(parts, now)),
            }
        });
        match decision {
            None | Some(Decision::Admit) => Limited::Admit,
            Some(Decision::Refuse) => Limited::Refuse,
            Some(Decision::Wait(ticket)) => {
                let (decided, waited) = oneshot::channel();
                self.waiting.borrow_mut().insert(ticket, decided);
                self.asked.notify_one();
                Limited::Wait(waited)
            }
        }
    }

    /// A worker's own decision of a request under `parts`, layered or shared: as it is, or,
    /// one it cannot make, asked of its process `hub`, numbered from `next`.
    fn or_ask(
        &self,
        local: Local,
        hub: &Hub,
        index: usize,
        next: &Cell<u64>,
        parts: &[(&[u8], Contract)],
    ) -> Limited {
        match local {
            Local::Admit => Limited::Admit,
            Local::Refuse => Limited::Refuse,
            Local::Ask => {
                let id = next.get();
                next.set(id.wrapping_add(1));
                let keys = parts
                    .iter()
                    .map(|(key, contract)| ((*key).into(), *contract));
                let asked = ToHub::Ask {
                    worker: index,
                    id,
                    keys: keys.collect(),
                };
                if hub.to.send(asked).is_err() {
                    return Limited::Refuse;
                }
                let (decided, waited) = oneshot::channel();
                self.waiting.borrow_mut().insert(id, decided);
                Limited::Wait(waited)
            }
        }
    }

    /// Which of `limits` refused a request from `client` that was under those `limited`
    /// marks, and how long it should wait to be admitted under all of them (06 §2): as
    /// `told` says, by position among the request's keys, where a service pod decided it
    /// and said; otherwise as the worker's copies have them.
    pub(crate) fn refusal<'a>(
        &self,
        limits: &'a [Arc<CompiledLimit>],
        limited: u64,
        client: &Requester,
        told: &[(usize, u64)],
    ) -> Refusal<'a> {
        let now = self.now();
        self.with_keys(limits, limited, client, |parts| {
            // A key begins with its limit's name and a zero.
            let at = |part: usize| {
                let (key, _) = parts.get(part)?;
                let name = key.split(|&byte| byte == 0).next()?;
                limits
                    .iter()
                    .position(|limit| limit.name.as_bytes() == name)
            };
            let mut by = 0;
            // The copies only where no pod said; layered, the worker's slices.
            let from_copies = told.is_empty();
            let own: Vec<(usize, u64)> = match &self.mode {
                Mode::Apart { plane, .. } => plane.borrow().waits(parts, now).collect(),
                Mode::Layered { slices, .. } => slices.borrow().waits(parts, now).collect(),
                Mode::Shared { rooms, .. } => rooms.borrow().waits(parts, now).collect(),
            };
            let waits = own.into_iter().filter(|_| from_copies);
            let waits = waits.chain(told.iter().copied()).inspect(|(part, _)| {
                if let Some(bit) =
                    at(*part).and_then(|at| 1_u64.checked_shl(u32::try_from(at).ok()?))
                {
                    by |= bit;
                }
            });
            let longest = longest(waits).and_then(|(part, wait)| {
                let limit = limits.get(at(part)?)?;
                Some((&**limit, Duration::from_nanos(wait)))
            });
            Refusal { by, longest }
        })
    }

    /// Calls `then` with the keys of a request from `client` under
    /// those of `limits` that `limited` marks, bit `n` for the `n`th: each the limit's name,
    /// a zero and the key's value, with its contract, put together in the worker's buffers.
    /// A limit the request has no key for is left out, as one whose number came out below
    /// one: it lets the request through.
    fn with_keys<R>(
        &self,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: &Requester,
        then: impl FnOnce(&[(&[u8], Contract)]) -> R,
    ) -> R {
        let mut keys = self.keys.borrow_mut();
        let mut contracts = self.contracts.borrow_mut();
        contracts.clear();
        let context = ByKey { client };
        let mut used = 0;
        for (at, limit) in limits.iter().enumerate() {
            let bit = 1_u64.checked_shl(u32::try_from(at).unwrap_or(u32::MAX));
            if bit.is_none_or(|bit| limited & bit == 0) {
                continue;
            }
            let Outcome::Applies { key, limit: count } = limit.rule.counted(&context, &NoTables)
            else {
                continue;
            };
            let Some(count) = NonZeroU64::new(count) else {
                continue;
            };
            if keys.len() <= used {
                keys.push(Vec::new());
            }
            let Some(buffer) = keys.get_mut(used) else {
                continue;
            };
            key_into(buffer, &limit.name, limit.definition, key);
            contracts.push(Contract {
                limit: count,
                window: limit.window,
                burst: limit.burst,
            });
            used += 1;
        }
        // The usual few at once without a list made for them.
        match (&keys[..used], &contracts[..]) {
            ([], _) => then(&[]),
            ([a], [ca]) => then(&[(a, *ca)]),
            ([a, b], [ca, cb]) => then(&[(a, *ca), (b, *cb)]),
            ([a, b, c], [ca, cb, cc]) => then(&[(a, *ca), (b, *cb), (c, *cc)]),
            ([a, b, c, d], [ca, cb, cc, cd]) => then(&[(a, *ca), (b, *cb), (c, *cc), (d, *cd)]),
            (many, contracts) => {
                let parts: Vec<(&[u8], Contract)> = many
                    .iter()
                    .map(Vec::as_slice)
                    .zip(contracts.iter().copied())
                    .collect();
                then(&parts)
            }
        }
    }

    /// Hands the requests the data plane decided their decisions.
    fn settle(&self) {
        let Mode::Apart { plane, .. } = &self.mode else {
            return;
        };
        let decided = plane.borrow_mut().take_decisions();
        let mut waiting = self.waiting.borrow_mut();
        for decided in decided {
            if let Some(waiter) = waiting.remove(&decided.ticket) {
                let _gone = waiter.send(decided);
            }
        }
    }

    /// Sends what the data plane has to send, each batch on a task of its own, and hands it
    /// the answers as they come.
    fn send(self: &Rc<Self>) {
        let Mode::Apart { plane, clients } = &self.mode else {
            return;
        };
        let outgoing = plane.borrow_mut().take_outgoing();
        for (pod, batch) in outgoing {
            let Some(client) = clients.get(pod as usize).cloned() else {
                continue;
            };
            let me = Rc::clone(self);
            tokio::task::spawn_local(async move {
                // Unanswered, it is sent again or given up as the protocol says.
                if let Ok(answers) = client.exchange(batch).await
                    && let Mode::Apart { plane, .. } = &me.mode
                {
                    plane.borrow_mut().on_answers(answers, me.now());
                    me.settle();
                    me.send();
                }
            });
        }
    }

    async fn run(self: Rc<Self>) {
        let mut cycle = tokio::time::interval(self.service.cycle);
        cycle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cycle.tick() => {
                    let now = self.now();
                    if let Mode::Apart { plane, .. } = &self.mode {
                        plane.borrow_mut().on_cycle(now);
                    }
                    self.settle();
                }
                () = self.asked.notified() => {}
            }
            if self.stopped.get() {
                return;
            }
            self.send();
        }
    }
}

impl Limiter {
    /// A layered worker's loop: its sync every millisecond, and its process's answers.
    async fn run_layered(self: Rc<Self>, mut answered: mpsc::UnboundedReceiver<FromHub>) {
        let Mode::Layered {
            hub, index, slices, ..
        } = &self.mode
        else {
            return;
        };
        let mut sync = tokio::time::interval(SYNC);
        sync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = sync.tick() => {
                    let used = slices.borrow_mut().take_used(self.now());
                    if !used.is_empty() {
                        let _gone = hub.to.send(ToHub::Used { worker: *index, used });
                    }
                }
                message = answered.recv() => match message {
                    Some(FromHub::Slices(given)) => slices.borrow_mut().on_slices(given),
                    Some(FromHub::Rooms { .. }) => {}
                    Some(FromHub::Answer { id, answer }) => {
                        slices.borrow_mut().on_slices(answer.slices);
                        if let Some(waiter) = self.waiting.borrow_mut().remove(&id) {
                            let _gone = waiter.send(Decided {
                                ticket: id,
                                admitted: answer.admitted,
                                refused_by: answer.refused_by,
                            });
                        }
                    }
                    None => return,
                },
                () = self.asked.notified() => {}
            }
            if self.stopped.get() {
                return;
            }
        }
    }
}

impl Limiter {
    /// Shared, the worker's side of its process: the answers to its asks, with the rooms of
    /// their keys, and now and then the rooms its process has closed dropped.
    async fn run_shared(self: Rc<Self>, mut answered: mpsc::UnboundedReceiver<FromHub>) {
        let Mode::Shared { rooms, .. } = &self.mode else {
            return;
        };
        let mut sweep = tokio::time::interval(Duration::from_secs(1));
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = sweep.tick() => rooms.borrow_mut().sweep(),
                message = answered.recv() => match message {
                    Some(FromHub::Rooms { id, answer }) => {
                        rooms.borrow_mut().on_rooms(answer.rooms);
                        if let Some(waiter) = self.waiting.borrow_mut().remove(&id) {
                            let _gone = waiter.send(Decided {
                                ticket: id,
                                admitted: answer.admitted,
                                refused_by: answer.refused_by,
                            });
                        }
                    }
                    Some(_) => {}
                    None => return,
                },
                () = self.asked.notified() => {}
            }
            if self.stopped.get() {
                return;
            }
        }
    }
}

impl std::fmt::Debug for Limiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limiter")
            .field("service", &self.service)
            .field("waiting", &self.waiting.borrow().len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A definition's digest for the tests: what it is matters only in being the same.
    const DEFINED: [u8; 8] = [7; 8];

    fn key(name: &str, value: KeyValue<'_>) -> Vec<u8> {
        let mut buffer = b"left over".to_vec();
        key_into(&mut buffer, name, DEFINED, value);
        buffer
    }

    fn address(text: &str) -> KeyValue<'static> {
        KeyValue::Address(text.parse().unwrap())
    }

    /// A key is the limit's name, a zero, its definition's digest and the value: another
    /// limit's key for the same client is another key, and so is the same limit's once it
    /// is defined otherwise.
    #[test]
    fn a_key_is_the_limits_name_a_zero_its_definition_and_the_value() {
        let mut expected = b"per-client\0".to_vec();
        expected.extend_from_slice(&DEFINED);
        expected.extend_from_slice(&[192, 0, 2, 10]);
        assert_eq!(key("per-client", address("192.0.2.10")), expected);
        let mut expected = b"acct\0".to_vec();
        expected.extend_from_slice(&DEFINED);
        expected.extend_from_slice(b"acme");
        assert_eq!(key("acct", KeyValue::Text("acme")), expected);
        assert_ne!(
            key("a", address("192.0.2.10")),
            key("b", address("192.0.2.10"))
        );
        // The same limit defined otherwise: another key.
        let mut other = Vec::new();
        key_into(&mut other, "a", [8; 8], address("192.0.2.10"));
        assert_ne!(key("a", address("192.0.2.10")), other);
    }

    /// An IPv6 client is counted by its /64: addresses in it are one client, the next /64
    /// another.
    #[test]
    fn an_ipv6_client_is_counted_by_its_slash_64() {
        let one = key("per-client", address("2001:db8:1:2::1"));
        assert_eq!(one.len(), "per-client".len() + 1 + DEFINED.len() + 8);
        assert_eq!(
            one,
            key("per-client", address("2001:db8:1:2:ffff:ffff:ffff:ffff"))
        );
        assert_ne!(one, key("per-client", address("2001:db8:1:3::1")));
    }
}
