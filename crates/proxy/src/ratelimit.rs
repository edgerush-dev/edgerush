//! The rate limits route rules are under (07 §2). The process is one participant of the
//! rate-limiting service's protocol (`edgerush_ratelimit::service::pooled`), on a thread of
//! its own ([`hub`]); each worker decides a request under all its limits at once from the
//! entries of its keys that the participant keeps and every worker shares, one
//! compare-and-swap a key, and asks the process only about what it cannot decide itself.
//!
//! Nothing here runs unless a rule limits: the worker makes its limiter the first time one
//! does, and makes it again when the service's settings change, which starts every key
//! afresh, as any change to a limit does.

mod hub;

#[cfg(test)]
pub(crate) use hub::REFUSE_START;
pub(crate) use hub::{Failed, Hub};

use crate::forwarding::Requester;
use crate::request::NoTables;
use edgerush_config::{CompiledLimit, CompiledRateLimitService};
use edgerush_ratelimit::service::pooled::{
    self, Entry, Handle, Local as PooledLocal, Outcome as Pooled, Registry,
};
use edgerush_ratelimit::service::{Contract, Decided, Keyed, longest};
use edgerush_rules::{Address, Context, KeyValue, Named, Outcome, Text};
use http::HeaderName;
use hub::SYNC;
pub(crate) use hub::{FromHub, ToHub};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, oneshot};

/// How long the process waits for an owner's answer to an ask before deciding what waits
/// from its entries: long enough for TCP to send a lost segment again (some 200 ms on
/// Linux), as the comparison harness found that the protocol's own four cycles, 40 ms, were
/// not.
const ASK_TIMEOUT: u64 = 300_000_000;

/// Why a request was refused, as the worker's entries have it.
pub(crate) struct Refusal<'a> {
    /// The limits that refused it, bit `n` for the rule's `n`th: those out of room. None
    /// when the entries show room under every one, the service pod having refused it from
    /// counts the entries have yet to catch up with.
    pub(crate) by: u64,
    /// The limit that waits longest to admit it, the first of those that wait as long, and
    /// how long: when to come back.
    pub(crate) longest: Option<(&'a CompiledLimit, Duration)>,
}

/// The IETF `RateLimit` field (draft-ietf-httpapi-ratelimit-headers): on a refusal, the
/// limit that refused, none of it left, and for how long.
pub(crate) const RATELIMIT: HeaderName = HeaderName::from_static("ratelimit");

/// What a limit's key reads of a request: the client's address as forwarding worked it
/// out, until authentication gives the rest (stage 9).
struct ByKey {
    client: Requester,
}

impl Context for ByKey {
    fn text(&self, _: Text) -> Option<&str> {
        None
    }

    fn address(&self, field: Address) -> Option<IpAddr> {
        match field {
            Address::Client => Some(self.client.address),
            Address::Peer => self.client.peer,
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

/// How long a request may wait deferred: an ask's own time, which a hold it waits on may
/// take to settle, and a little more.
pub(crate) const DEFER_WAIT: Duration = Duration::from_millis(400);

/// The most requests a worker, or its process, holds deferred at once: past it, a request
/// is answered as the gateway's own overload.
pub(crate) const MOST_DEFERRED: usize = 1_024;

/// The most entries a worker keeps signalled between syncs: a key signals at most once each
/// time its count starts, so a millisecond's worth for any worker; past it, the worker asks
/// its process to visit every key at the next cycle rather than keep more.
const MOST_SIGNALLED: usize = 4_096;

/// How a request fared against the limits it is under.
pub(crate) enum Limited {
    /// Within it.
    Admit,
    /// Over it.
    Refuse,
    /// Asked about or deferred: its decision comes, or why there is none. A sender dropped
    /// unanswered is the limiter gone: [`Undecided::Unavailable`].
    Later(oneshot::Receiver<Result<Decided, Undecided>>),
    /// Not decided, and not to be: why.
    Undecided(Undecided),
}

/// Why a request's rate limits did not decide it: each the gateway's own trouble, answered
/// 503, never as over a limit (06 §4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Undecided {
    /// It could not take a place to wait in.
    Full,
    /// Its time to wait ran out, wherever it was waiting.
    Timeout,
    /// There is no thread to decide it: never started, or gone.
    Unavailable,
}

/// A worker's handles to its process's entries, found by key.
type Handles = HashSet<Handle, Keyed>;

/// A worker's limiter: deciding from the entries its process's participant keeps.
pub(crate) struct Limiter {
    service: CompiledRateLimitService,
    hub: Arc<Hub>,
    /// Which of the process's workers it is.
    index: usize,
    /// Where it finds an entry of the process it holds no handle to.
    registry: Arc<Registry>,
    /// Its handles to entries, by key: one lookup a key a request.
    handles: RefCell<Handles>,
    /// The next ask's number.
    next: Cell<u64>,
    /// Its asks waiting on its process, by number.
    asked: RefCell<HashMap<u64, oneshot::Sender<Result<Decided, Undecided>>>>,
    /// What its deferred requests wait on, and how many there are, and may be.
    settled: Arc<Notify>,
    deferring: Cell<usize>,
    most_deferred: Cell<usize>,
    /// With keys visited only when they need it, the entries its decisions signalled since
    /// it last handed them to its process: every sync, at most `MOST_SIGNALLED`.
    signalled: RefCell<Vec<Arc<Entry>>>,
    /// Where each of a request's keys is put together, one for each limit it can be under,
    /// kept so that no request makes them afresh; and their contracts.
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<Contract>>,
    /// The clock: its process's.
    start: Instant,
    /// Wakes its task to stop.
    stopping: Notify,
    stopped: Cell<bool>,
}

impl Limiter {
    /// A limiter for worker `index` of the process `hub`, deciding from the shared entries
    /// its participant keeps, its task started.
    pub(crate) fn new(service: &CompiledRateLimitService, hub: Arc<Hub>, index: usize) -> Rc<Self> {
        let registry = hub.registry();
        let settled = hub.settled();
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
            hub,
            index,
            registry,
            handles: RefCell::new(HashSet::with_hasher(hasher)),
            next: Cell::new(0),
            asked: RefCell::new(HashMap::new()),
            settled,
            deferring: Cell::new(0),
            most_deferred: Cell::new(MOST_DEFERRED),
            signalled: RefCell::new(Vec::new()),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
            start,
            stopping: Notify::new(),
            stopped: Cell::new(false),
        });
        tokio::task::spawn_local(Rc::clone(&limiter).run(answered));
        limiter
    }

    /// Whether it is a limiter of the process thread `hub`. The thread is kept while the
    /// service's pods, cycle and levers are; a change of what its keys may hold is taken as
    /// it is ([`Hub::set_capacity`]).
    pub(crate) fn is_on(&self, hub: &Arc<Hub>) -> bool {
        Arc::ptr_eq(&self.hub, hub)
    }

    /// What its process's keys hold, in bytes, how many there are, and how many it has
    /// evicted, all told: for the worker's sweep to say, by its first worker alone, so that
    /// the workers' sum is the process's.
    pub(crate) fn usage(&self) -> (u64, usize, u64) {
        if self.index == 0 {
            self.hub.usage()
        } else {
            (0, 0, 0)
        }
    }

    /// Stops its task: a limiter for other settings takes its place.
    pub(crate) fn stop(&self) {
        self.stopped.set(true);
        self.stopping.notify_one();
    }

    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Decides a request from `client` under those of `limits` that `limited` marks, bit `n`
    /// for the `n`th: each counted by its key — the limit's name, a zero, the key's value —
    /// and all decided at once, admitted under all of them or none (06 §2). A limit the
    /// request has no key for lets it through, as one whose number came out below one.
    pub(crate) fn decide_all(
        self: &Rc<Self>,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: Requester,
    ) -> Limited {
        let now = self.now();
        self.with_keys(limits, limited, client, |parts| {
            if parts.is_empty() {
                return Limited::Admit;
            }
            let outcome = decide(&self.handles, &self.registry, self.making(), parts, now);
            self.signal(&outcome, parts);
            match outcome.local {
                PooledLocal::Admit => Limited::Admit,
                PooledLocal::Refuse => Limited::Refuse,
                PooledLocal::Ask => self.ask(parts),
                PooledLocal::Defer => self.defer(parts),
            }
        })
    }

    /// Which of `limits` refused a request from `client` that was under those `limited`
    /// marks, and how long it should wait to be admitted under all of them (06 §2): as
    /// `told` says, by position among the request's keys, where a service pod decided it
    /// and said; otherwise as the worker's entries have them.
    pub(crate) fn refusal<'a>(
        &self,
        limits: &'a [Arc<CompiledLimit>],
        limited: u64,
        client: Requester,
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
            // The entries only where no pod said.
            let own: Vec<(usize, u64)> = if told.is_empty() {
                // The keys it holds entries of, each where it is among the request's.
                let handles = self.handles.borrow();
                let (at, held): (Vec<usize>, Vec<&Entry>) = parts
                    .iter()
                    .enumerate()
                    .filter_map(|(at, (key, contract))| {
                        let entry = handles.get(*key)?.entry();
                        (entry.contract() == *contract).then_some((at, &**entry))
                    })
                    .unzip();
                pooled::waits(&held, now)
                    .filter_map(|(part, wait)| Some((*at.get(part)?, wait)))
                    .collect()
            } else {
                Vec::new()
            };
            let waits = own
                .into_iter()
                .chain(told.iter().copied())
                .inspect(|(part, _)| {
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
        client: Requester,
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

    /// The worker's side of its process: the answers to its asks, with the entries of their
    /// keys; its signals handed over every sync; and now and then its handles to entries
    /// its process has closed dropped.
    async fn run(self: Rc<Self>, mut answered: mpsc::UnboundedReceiver<FromHub>) {
        let mut sweep = tokio::time::interval(Duration::from_secs(1));
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sync = tokio::time::interval(SYNC);
        sync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = sweep.tick() => {
                    self.handles.borrow_mut().retain(|held| !held.entry().closed());
                }
                // What its decisions signalled, handed over in one message a sync.
                _ = sync.tick(), if self.service.schedule_keys => {
                    let entries = std::mem::take(&mut *self.signalled.borrow_mut());
                    if !entries.is_empty() {
                        let _gone = self.hub.to.send(ToHub::Signals(entries));
                    }
                }
                message = answered.recv() => match message {
                    Some(FromHub { id, admitted, undecided, refused_by, entries }) => {
                        let mut handles = self.handles.borrow_mut();
                        for entry in entries {
                            handles.replace(Handle::new(entry));
                        }
                        drop(handles);
                        if let Some(waiter) = self.asked.borrow_mut().remove(&id) {
                            let decided = match undecided {
                                Some(why) => Err(why),
                                None => Ok(Decided {
                                    ticket: id,
                                    admitted,
                                    refused_by,
                                }),
                            };
                            let _gone = waiter.send(decided);
                        }
                    }
                    // The process is gone: what still waits on it is told so, rather than
                    // left waiting for an answer that cannot come.
                    None => {
                        for (_, waiter) in self.asked.borrow_mut().drain() {
                            let _gone = waiter.send(Err(Undecided::Unavailable));
                        }
                        return;
                    }
                },
                () = self.stopping.notified() => {}
            }
            if self.stopped.get() {
                return;
            }
        }
    }
}

/// A worker's own decision of a request under `parts` at `now`, from its handles to the
/// entries of its keys: those it holds none of under their contracts looked for in its
/// process's `registry` first — or, with cold keys decided locally, made there — and asked
/// about if not there.
fn decide(
    handles: &RefCell<Handles>,
    registry: &Registry,
    making: Option<(&Hub, u64)>,
    parts: &[(&[u8], Contract)],
    now: u64,
) -> Pooled {
    let ask = Pooled {
        local: PooledLocal::Ask,
        drain: false,
        wake: false,
        signal: 0,
    };
    let held = |handles: &Handles, key: &[u8], contract| {
        handles
            .get(key)
            .is_some_and(|held| held.entry().contract() == contract)
    };
    {
        let mut handles = handles.borrow_mut();
        for (key, contract) in parts {
            if held(&handles, key, *contract) {
                continue;
            }
            // Cold keys decided locally, a key the process has not met is made here, at
            // once, and handed to the process to keep.
            let found = match making {
                None => registry.find(key, *contract),
                Some((hub, cycle)) => {
                    registry
                        .find_or_make(key, *contract, cycle, now)
                        .map(|(entry, made)| {
                            if made {
                                let _gone = hub.to.send(ToHub::Adopt(Arc::clone(&entry)));
                            }
                            entry
                        })
                }
            };
            let Some(entry) = found else {
                return ask;
            };
            handles.replace(Handle::new(entry));
        }
    }
    let handles = handles.borrow();
    let entry = |at: usize| {
        let (key, _) = parts.get(at)?;
        handles.get(*key).map(|held| &**held.entry())
    };
    // The usual few at once without a list made for them.
    let Some(first) = entry(0) else {
        return ask;
    };
    if parts.len() <= 4 {
        let mut entries = [first; 4];
        for (at, slot) in entries.iter_mut().enumerate().take(parts.len()).skip(1) {
            let Some(entry) = entry(at) else {
                return ask;
            };
            *slot = entry;
        }
        return pooled::decide(&entries[..parts.len()], now);
    }
    let entries: Option<Vec<&Entry>> = (0..parts.len()).map(entry).collect();
    entries.map_or(ask, |entries| pooled::decide(&entries, now))
}

impl Limiter {
    /// With cold keys decided locally, where a worker makes a key's entry: its process, and
    /// the cycle, in nanoseconds.
    fn making(&self) -> Option<(&Hub, u64)> {
        let cycle = u64::try_from(self.service.cycle.as_nanos()).unwrap_or(u64::MAX);
        (!self.service.ask_cold_keys).then_some((&*self.hub, cycle))
    }

    /// What a decision asks of others: a key's count drained, waiters woken, and with keys
    /// visited only when they need it, the keys it signalled kept to hand over — or, with no
    /// room left to keep them, every key asked to be visited.
    fn signal(&self, outcome: &Pooled, parts: &[(&[u8], Contract)]) {
        if outcome.signal != 0 && self.service.schedule_keys {
            let handles = self.handles.borrow();
            let mut signalled = self.signalled.borrow_mut();
            for (at, (key, _)) in parts.iter().enumerate() {
                if outcome.signal & (1 << at) == 0 {
                    continue;
                }
                let Some(held) = handles.get(*key) else {
                    continue;
                };
                if signalled.len() < MOST_SIGNALLED {
                    signalled.push(Arc::clone(held.entry()));
                } else {
                    self.registry.require_scan();
                }
            }
        }
        if outcome.drain {
            for (key, _) in parts {
                let _gone = self.hub.to.send(ToHub::Drain((*key).into()));
            }
        }
        if outcome.wake {
            self.settled.notify_waiters();
        }
    }

    /// A request under `parts` asked of the process; undecided if it is gone.
    fn ask(&self, parts: &[(&[u8], Contract)]) -> Limited {
        match self.ask_process(parts) {
            Some(later) => Limited::Later(later),
            None => Limited::Undecided(Undecided::Unavailable),
        }
    }

    fn ask_process(
        &self,
        parts: &[(&[u8], Contract)],
    ) -> Option<oneshot::Receiver<Result<Decided, Undecided>>> {
        let id = self.next.get();
        self.next.set(id.wrapping_add(1));
        let keys = parts
            .iter()
            .map(|(key, contract)| ((*key).into(), *contract))
            .collect();
        let message = ToHub::Ask {
            worker: self.index,
            id,
            keys,
        };
        self.hub.to.send(message).ok()?;
        let (decided, later) = oneshot::channel();
        self.asked.borrow_mut().insert(id, decided);
        Some(later)
    }

    /// A request under `parts` deferred: decided again on a task of its own as its keys'
    /// holds settle and counts drain, until its time is up; or, with as many deferred as
    /// may be, undecided at once.
    fn defer(self: &Rc<Self>, parts: &[(&[u8], Contract)]) -> Limited {
        if self.deferring.get() >= self.most_deferred.get() {
            return Limited::Undecided(Undecided::Full);
        }
        self.deferring.set(self.deferring.get() + 1);
        let keys: Vec<(Box<[u8]>, Contract)> = parts
            .iter()
            .map(|(key, contract)| ((*key).into(), *contract))
            .collect();
        let (decided, later) = oneshot::channel();
        tokio::task::spawn_local(Rc::clone(self).deferred(keys, decided));
        Limited::Later(later)
    }

    /// A deferred request under `keys`, decided again whenever a hold settles or a count
    /// drains anywhere in the process, until it is decided, its time is up, or nobody waits
    /// for it any more.
    async fn deferred(
        self: Rc<Self>,
        keys: Vec<(Box<[u8]>, Contract)>,
        mut decided: oneshot::Sender<Result<Decided, Undecided>>,
    ) {
        let parts: Vec<(&[u8], Contract)> = keys
            .iter()
            .map(|(key, contract)| (&**key, *contract))
            .collect();
        let deadline = tokio::time::Instant::now() + DEFER_WAIT;
        let decision = |admitted| {
            Ok(Decided {
                ticket: 0,
                admitted,
                refused_by: Box::default(),
            })
        };
        let result = loop {
            // Waiting on its keys before it decides, and listening before it decides, so
            // that a hold settled between the two is not missed.
            let _waiting = Waiting::on(&self.handles, &self.registry, &parts);
            let woken = self.settled.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let outcome = decide(
                &self.handles,
                &self.registry,
                self.making(),
                &parts,
                self.now(),
            );
            self.signal(&outcome, &parts);
            match outcome.local {
                PooledLocal::Admit => break decision(true),
                PooledLocal::Refuse => break decision(false),
                PooledLocal::Ask => {
                    break match self.ask_process(&parts) {
                        Some(later) => later.await.unwrap_or(Err(Undecided::Unavailable)),
                        None => Err(Undecided::Unavailable),
                    };
                }
                PooledLocal::Defer => {}
            }
            tokio::select! {
                () = &mut woken => {}
                () = tokio::time::sleep_until(deadline) => break Err(Undecided::Timeout),
                () = decided.closed() => {
                    self.deferring.set(self.deferring.get().saturating_sub(1));
                    return;
                }
            }
        };
        self.deferring.set(self.deferring.get().saturating_sub(1));
        let _gone = decided.send(result);
    }

    /// The most requests it holds deferred, for a test to fill.
    #[cfg(test)]
    pub(crate) fn set_most_deferred(&self, most: usize) {
        self.most_deferred.set(most);
    }
}

/// A deferred request's wait on the entries of its keys, from before it decides again
/// until after: what settles or drains them wakes it.
struct Waiting(Vec<Arc<Entry>>);

impl Waiting {
    fn on(handles: &RefCell<Handles>, registry: &Registry, parts: &[(&[u8], Contract)]) -> Self {
        let mut handles = handles.borrow_mut();
        let entries: Vec<Arc<Entry>> = parts
            .iter()
            .filter_map(|(key, contract)| {
                let held = handles
                    .get(*key)
                    .filter(|held| held.entry().contract() == *contract)
                    .map(|held| Arc::clone(held.entry()));
                held.or_else(|| {
                    let found = registry.find(key, *contract)?;
                    handles.replace(Handle::new(Arc::clone(&found)));
                    Some(found)
                })
            })
            .collect();
        for entry in &entries {
            entry.add_waiter();
        }
        Self(entries)
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        for entry in &self.0 {
            entry.remove_waiter();
        }
    }
}

impl std::fmt::Debug for Limiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limiter")
            .field("service", &self.service)
            .field("asked", &self.asked.borrow().len())
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
