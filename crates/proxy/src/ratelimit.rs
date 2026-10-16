//! The rate limits route rules are under (07 §2): each worker is a data plane of the
//! rate-limiting service's protocol (`edgerush_ratelimit::service`), deciding every request
//! under all its limits at once from its copies, asking a service pod about keys it holds no
//! fresh copy of, and sending the service, every cycle, what it admitted — over gRPC, from a
//! task of the worker's own.
//!
//! Nothing here runs unless a rule limits: the worker makes its limiter the first time one
//! does, and makes it again when the service's settings change, which starts every key
//! afresh, as any change to a limit does.

use crate::forwarding::Client as Requester;
use crate::request::NoTables;
use edgerush_config::{CompiledLimit, CompiledRateLimitService};
use edgerush_ratelimit::service::{
    Contract, DataPlane, Decided, Decision, Keyed, Ticket, View, longest,
};
use edgerush_ratelimit_service::grpc::Client;
use edgerush_rules::{Address, Context, KeyValue, Named, Outcome, Text};
use http::HeaderName;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, oneshot};

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

/// Puts together in `buffer` the key a request is counted by under the limit `name`: the
/// name, a zero, and the key's value — an IPv6 client by its /64, not its whole address,
/// which one client holds 2^64 of (06 §2).
fn key_into(buffer: &mut Vec<u8>, name: &str, key: KeyValue<'_>) {
    buffer.clear();
    buffer.extend_from_slice(name.as_bytes());
    buffer.push(0);
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

/// A worker's limiter: its data plane and the clients of the service's pods.
pub(crate) struct Limiter {
    service: CompiledRateLimitService,
    plane: RefCell<DataPlane<Keyed>>,
    clients: Vec<Client>,
    /// The requests waiting for an owner's decision, by ticket: dropped with the limiter,
    /// which refuses them.
    waiting: RefCell<HashMap<Ticket, oneshot::Sender<Decided>>>,
    /// Where each of a request's keys is put together, one for each limit it can be under,
    /// kept so that no request makes them afresh; and their contracts.
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<Contract>>,
    start: Instant,
    /// Wakes the task to send what a decision asked for at once.
    asked: Notify,
    stopped: Cell<bool>,
}

impl Limiter {
    /// A limiter for worker `worker` reaching `service`, its task started; `None` if an
    /// address cannot be made into one to connect to.
    pub(crate) fn start(service: &CompiledRateLimitService, worker: u16) -> Option<Rc<Self>> {
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
        let limiter = Rc::new(Self {
            service: service.clone(),
            plane: RefCell::new(plane),
            clients,
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

    /// Whether it reaches `service` as it is set now.
    pub(crate) fn serves(&self, service: &CompiledRateLimitService) -> bool {
        self.service == *service
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
        let decision = self.with_keys(limits, limited, client, |plane, parts| match parts {
            [] => None,
            [(key, contract)] => Some(plane.decide(key, *contract, now)),
            parts => Some(plane.decide_all(parts, now)),
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
        self.with_keys(limits, limited, client, |plane, parts| {
            // A key begins with its limit's name and a zero.
            let at = |part: usize| {
                let (key, _) = parts.get(part)?;
                let name = key.split(|&byte| byte == 0).next()?;
                limits
                    .iter()
                    .position(|limit| limit.name.as_bytes() == name)
            };
            let mut by = 0;
            // The copies only where no pod said.
            let from_copies = told.is_empty();
            let waits = plane.waits(parts, now).filter(|_| from_copies);
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

    /// Calls `then` with the data plane and the keys of a request from `client` under
    /// those of `limits` that `limited` marks, bit `n` for the `n`th: each the limit's name,
    /// a zero and the key's value, with its contract, put together in the worker's buffers.
    /// A limit the request has no key for is left out, as one whose number came out below
    /// one: it lets the request through.
    fn with_keys<R>(
        &self,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: &Requester,
        then: impl FnOnce(&mut DataPlane<Keyed>, &[(&[u8], Contract)]) -> R,
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
            key_into(buffer, &limit.name, key);
            contracts.push(Contract {
                limit: count,
                window: limit.window,
                burst: limit.burst,
            });
            used += 1;
        }
        let mut plane = self.plane.borrow_mut();
        // The usual few at once without a list made for them.
        match (&keys[..used], &contracts[..]) {
            ([], _) => then(&mut plane, &[]),
            ([a], [ca]) => then(&mut plane, &[(a, *ca)]),
            ([a, b], [ca, cb]) => then(&mut plane, &[(a, *ca), (b, *cb)]),
            ([a, b, c], [ca, cb, cc]) => then(&mut plane, &[(a, *ca), (b, *cb), (c, *cc)]),
            ([a, b, c, d], [ca, cb, cc, cd]) => {
                then(&mut plane, &[(a, *ca), (b, *cb), (c, *cc), (d, *cd)])
            }
            (many, contracts) => {
                let parts: Vec<(&[u8], Contract)> = many
                    .iter()
                    .map(Vec::as_slice)
                    .zip(contracts.iter().copied())
                    .collect();
                then(&mut plane, &parts)
            }
        }
    }

    /// Hands the requests the data plane decided their decisions.
    fn settle(&self) {
        let decided = self.plane.borrow_mut().take_decisions();
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
        let outgoing = self.plane.borrow_mut().take_outgoing();
        for (pod, batch) in outgoing {
            let Some(client) = self.clients.get(pod as usize).cloned() else {
                continue;
            };
            let me = Rc::clone(self);
            tokio::task::spawn_local(async move {
                // Unanswered, it is sent again or given up as the protocol says.
                if let Ok(answers) = client.exchange(batch).await {
                    me.plane.borrow_mut().on_answers(answers, me.now());
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
                    self.plane.borrow_mut().on_cycle(now);
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

    fn key(name: &str, value: KeyValue<'_>) -> Vec<u8> {
        let mut buffer = b"left over".to_vec();
        key_into(&mut buffer, name, value);
        buffer
    }

    fn address(text: &str) -> KeyValue<'static> {
        KeyValue::Address(text.parse().unwrap())
    }

    /// A key is the limit's name, a zero and the value: another limit's key for the same
    /// client is another key.
    #[test]
    fn a_key_is_the_limits_name_a_zero_and_the_value() {
        let mut expected = b"per-client".to_vec();
        expected.extend_from_slice(&[0, 192, 0, 2, 10]);
        assert_eq!(key("per-client", address("192.0.2.10")), expected);
        assert_eq!(key("acct", KeyValue::Text("acme")), b"acct\0acme");
        assert_ne!(
            key("a", address("192.0.2.10")),
            key("b", address("192.0.2.10"))
        );
    }

    /// An IPv6 client is counted by its /64: addresses in it are one client, the next /64
    /// another.
    #[test]
    fn an_ipv6_client_is_counted_by_its_slash_64() {
        let one = key("per-client", address("2001:db8:1:2::1"));
        assert_eq!(one.len(), "per-client".len() + 1 + 8);
        assert_eq!(
            one,
            key("per-client", address("2001:db8:1:2:ffff:ffff:ffff:ffff"))
        );
        assert_ne!(one, key("per-client", address("2001:db8:1:3::1")));
    }
}
