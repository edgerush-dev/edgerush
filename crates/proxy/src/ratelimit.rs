//! Rules' rate limits (experimental): each worker is a data plane of the rate-limiting
//! service's protocol (`edgerush_ratelimit::service`), deciding every request from its
//! copies, asking the key's owner about a key it holds no fresh copy of, and sending the
//! service, every cycle, what it admitted — over gRPC, from a task of the worker's own.
//!
//! Nothing here runs unless a rule limits: the worker makes its limiter the first time one
//! does, and makes it again when the service's settings change, which starts every key
//! afresh, as any change to a limit does.

use edgerush_config::{CompiledRateLimit, CompiledRateLimitService};
use edgerush_ratelimit::service::{Contract, DataPlane, Decision, Keyed, Ticket, View};
use edgerush_ratelimit_service::grpc::Client;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, oneshot};

/// How long a worker waits for the owner's answer to an ask before deciding what waits from
/// its copy: long enough for TCP to send a lost segment again (some 200 ms on Linux), as the
/// comparison harness found that the protocol's own four cycles, 40 ms, were not.
const ASK_TIMEOUT: u64 = 300_000_000;

/// How a request fared against its rule's limit.
pub(crate) enum Limited {
    /// Within it.
    Admit,
    /// Over it.
    Refuse,
    /// Asked about: the owner's decision comes, `true` admitting it. A sender dropped (the
    /// limiter gone) refuses it.
    Wait(oneshot::Receiver<bool>),
}

/// A worker's limiter: its data plane and the clients of the service's pods.
pub(crate) struct Limiter {
    service: CompiledRateLimitService,
    plane: RefCell<DataPlane<Keyed>>,
    clients: Vec<Client>,
    /// The requests waiting for an owner's decision, by ticket: dropped with the limiter,
    /// which refuses them.
    waiting: RefCell<HashMap<Ticket, oneshot::Sender<bool>>>,
    /// Where a key is put together, kept so that no request makes one afresh.
    key: RefCell<Vec<u8>>,
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
            key: RefCell::new(Vec::new()),
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

    /// Decides a request whose key under `limit` is `value`.
    pub(crate) fn decide(&self, limit: &CompiledRateLimit, value: &[u8]) -> Limited {
        let mut key = self.key.borrow_mut();
        key.clear();
        key.extend_from_slice(&limit.name);
        key.push(0);
        key.extend_from_slice(value);
        let contract = Contract {
            limit: limit.limit,
            window: limit.window,
            burst: limit.burst,
        };
        let decision = self.plane.borrow_mut().decide(&key, contract, self.now());
        match decision {
            Decision::Admit => Limited::Admit,
            Decision::Refuse => Limited::Refuse,
            Decision::Wait(ticket) => {
                let (decided, waited) = oneshot::channel();
                self.waiting.borrow_mut().insert(ticket, decided);
                self.asked.notify_one();
                Limited::Wait(waited)
            }
        }
    }

    /// Hands the requests the data plane decided their decisions.
    fn settle(&self) {
        let decided = self.plane.borrow_mut().take_decided();
        let mut waiting = self.waiting.borrow_mut();
        for (ticket, admitted) in decided {
            if let Some(waiter) = waiting.remove(&ticket) {
                let _gone = waiter.send(admitted);
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
