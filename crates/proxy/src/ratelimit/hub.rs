//! The process layer of a layered limiter (12 item 3): a thread of its own, as Lucas chose,
//! holding the pod's one data plane of the rate-limiting service, the clients of its pods,
//! and the slices of its room it gives the workers. Its inputs are the workers' messages,
//! the service's answers and its cycle; nothing of it is shared with a worker but a channel
//! and the clock they read its times by.

use super::{ASK_TIMEOUT, DEFER_WAIT, MOST_DEFERRED};
use edgerush_config::CompiledRateLimitService;
use edgerush_ratelimit::service::layer::{Answer, Process, Slice, Used};
use edgerush_ratelimit::service::pooled::{
    Decision as Pooled, Entry, Levers, Participant, Registry,
};
use edgerush_ratelimit::service::shared::{Answer as RoomAnswer, Process as Rooms};
use edgerush_ratelimit::service::{
    Answers, Batch, Contract, DataPlane, Keyed, PodId, Ticket, View,
};
use edgerush_ratelimit_service::grpc::Client;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};

/// The time between a worker's syncs with its process (Lucas: every millisecond).
pub(crate) const SYNC: Duration = Duration::from_millis(1);

/// What a worker sends its process.
pub(crate) enum ToHub {
    /// Worker `worker` is there, to be answered on `answers`.
    Join {
        worker: usize,
        answers: mpsc::UnboundedSender<FromHub>,
    },
    /// Its sync: what it admitted since the last, by key.
    Used { worker: usize, used: Vec<Used> },
    /// A request it cannot decide itself, for the process to.
    Ask {
        worker: usize,
        id: u64,
        keys: Vec<(Box<[u8]>, Contract)>,
    },
    /// The bytes the keys may hold from now on.
    Capacity(u64),
    /// Pooled, a key whose unreported count passed half its most: to drain now.
    Drain(Box<[u8]>),
    /// Pooled, cold keys decided locally: an entry a worker made, for the process to keep.
    Adopt(Arc<Entry>),
    /// Pooled, keys visited only when they need it: those its decisions signalled since its
    /// last such message, for the next cycle to visit.
    Signals(Vec<Arc<Entry>>),
}

/// What the process sends a worker.
pub(crate) enum FromHub {
    /// New slices, for its sync.
    Slices(Vec<Slice>),
    /// The answer to a request it asked about.
    Answer { id: u64, answer: Answer },
    /// Shared, the answer to a request it asked about, with the rooms of its keys.
    Rooms { id: u64, answer: RoomAnswer },
    /// Pooled, the decision on a request it asked about — refused, which of its keys
    /// refused and how long each waits — with the entries of its keys, to hold; or, if
    /// `overloaded`, none: it could not be decided in the time allowed a deferred request,
    /// or too many were deferred.
    Pooled {
        id: u64,
        admitted: bool,
        overloaded: bool,
        refused_by: Box<[(usize, u64)]>,
        entries: Vec<Arc<Entry>>,
    },
}

/// What the process thread and the workers share: the clock its times are read by, and
/// what its keys hold.
struct Shared {
    epoch: Instant,
    /// Bytes, keys, and evictions all told, as of its last cycle.
    usage: [AtomicU64; 3],
    stats: Stats,
}

/// Pooled, what the process thread says of its own work, for the scrape: the asks it
/// takes and how long they wait, its queue, its exchanges, and the time its work takes.
/// The most-of figures are since the last scrape, which takes them.
#[derive(Default)]
pub(crate) struct Stats {
    asks: AtomicU64,
    answered: AtomicU64,
    ask_ns: AtomicU64,
    ask_ns_most: AtomicU64,
    queued_most: AtomicU64,
    in_flight: AtomicU64,
    in_flight_most: AtomicU64,
    entries_in_flight: AtomicU64,
    cycle_ns: AtomicU64,
    cycle_ns_most: AtomicU64,
    answers_ns: AtomicU64,
    busy_ns: AtomicU64,
    visits: AtomicU64,
}

impl Stats {
    fn answered(&self, waited: u64) {
        self.answered.fetch_add(1, Ordering::Relaxed);
        self.ask_ns.fetch_add(waited, Ordering::Relaxed);
        self.ask_ns_most.fetch_max(waited, Ordering::Relaxed);
    }

    /// The scrape's lines, the most-of figures taken.
    pub(crate) fn render(&self) -> String {
        let read = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let take = |a: &AtomicU64| a.swap(0, Ordering::Relaxed);
        let lines = [
            ("asks_total", "counter", read(&self.asks)),
            ("asks_answered_total", "counter", read(&self.answered)),
            ("ask_ns_total", "counter", read(&self.ask_ns)),
            ("ask_ns_most", "gauge", take(&self.ask_ns_most)),
            ("queued_most", "gauge", take(&self.queued_most)),
            ("exchanges_in_flight", "gauge", read(&self.in_flight)),
            (
                "exchanges_in_flight_most",
                "gauge",
                take(&self.in_flight_most),
            ),
            ("entries_in_flight", "gauge", read(&self.entries_in_flight)),
            ("cycle_ns_total", "counter", read(&self.cycle_ns)),
            ("cycle_ns_most", "gauge", take(&self.cycle_ns_most)),
            ("answers_ns_total", "counter", read(&self.answers_ns)),
            ("busy_ns_total", "counter", read(&self.busy_ns)),
            ("visits_total", "counter", read(&self.visits)),
        ];
        let mut out = String::new();
        for (name, kind, value) in lines {
            out.push_str(&format!(
                "# TYPE edgerush_rate_limit_hub_{name} {kind}\nedgerush_rate_limit_hub_{name} {value}\n"
            ));
        }
        out
    }
}

/// A handle on a running process layer. It stops once every handle and every worker's
/// sender is gone.
pub(crate) struct Hub {
    pub(crate) to: mpsc::UnboundedSender<ToHub>,
    shared: Arc<Shared>,
    service: CompiledRateLimitService,
    /// Pooled, its participant's registry of live entries, for workers meeting a key.
    registry: Option<Arc<Registry>>,
    /// Pooled, woken when a hold settles or a count drains on a key requests are deferred
    /// on, by whichever thread did it: the workers' deferred requests wait on it.
    settled: Arc<Notify>,
}

impl Hub {
    /// The process layer of `workers` reaching `service`, its keys holding at most
    /// `max_key_bytes`, its thread started; `None` if an address cannot be made into one to
    /// connect to, or no thread could be had.
    pub(crate) fn start(
        service: &CompiledRateLimitService,
        workers: usize,
        max_key_bytes: u64,
    ) -> Option<Arc<Self>> {
        let clients = service
            .pods
            .iter()
            .map(|address| Client::new(address))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let shared = Arc::new(Shared {
            epoch: Instant::now(),
            usage: Default::default(),
            stats: Stats::default(),
        });
        let (to, from) = mpsc::unbounded_channel();
        let running = Arc::clone(&shared);
        let set = service.clone();
        let settled = Arc::new(Notify::new());
        let waking = Arc::clone(&settled);
        // Pooled, the participant is made on the thread and its registry handed back.
        let (made, registry) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("edgerush-rate-limits".to_owned())
            .spawn(move || {
                let running = Running {
                    shared: running,
                    settled: waking,
                };
                run(clients, &set, workers, max_key_bytes, from, &running, &made);
            })
            .ok()?;
        let registry = if service.pooled {
            Some(registry.recv().ok()?)
        } else {
            None
        };
        Some(Arc::new(Self {
            to,
            shared,
            service: service.clone(),
            registry,
            settled,
        }))
    }

    /// Pooled, its participant's registry.
    pub(crate) fn registry(&self) -> Option<Arc<Registry>> {
        self.registry.clone()
    }

    /// Pooled, the process thread's own measures, as scrape lines.
    pub(crate) fn render_stats(&self) -> String {
        self.shared.stats.render()
    }

    /// Pooled, what deferred requests wait on.
    pub(crate) fn settled(&self) -> Arc<Notify> {
        Arc::clone(&self.settled)
    }

    /// Whether it reaches `service` as it is set now: the same pods, the same cycle, the
    /// same keep and sharing.
    pub(crate) fn serves(&self, service: &CompiledRateLimitService) -> bool {
        self.service.pods == service.pods
            && self.service.cycle == service.cycle
            && self.service.layered_keep == service.layered_keep
            && self.service.shared == service.shared
            && self.service.pooled == service.pooled
            && self.service.poll_quiet_keys == service.poll_quiet_keys
            && self.service.ask_cold_keys == service.ask_cold_keys
            && self.service.schedule_keys == service.schedule_keys
    }

    /// The clock its times are read by.
    pub(crate) fn epoch(&self) -> Instant {
        self.shared.epoch
    }

    /// What its keys hold, in bytes, how many there are, and how many it has evicted, all
    /// told, as of its last cycle.
    pub(crate) fn usage(&self) -> (u64, usize, u64) {
        let [bytes, keys, evicted] = &self.shared.usage;
        (
            bytes.load(Ordering::Relaxed),
            usize::try_from(keys.load(Ordering::Relaxed)).unwrap_or(usize::MAX),
            evicted.load(Ordering::Relaxed),
        )
    }
}

impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("service", &self.service)
            .finish_non_exhaustive()
    }
}

/// What the process thread shares with its handles.
struct Running {
    shared: Arc<Shared>,
    settled: Arc<Notify>,
}

/// The process thread: a runtime of its own, the process and its clients, until every
/// sender is gone, as `service` sets it: its cycle, and whether its workers share by slices
/// or by rooms.
fn run(
    clients: Vec<Client>,
    service: &CompiledRateLimitService,
    workers: usize,
    max_key_bytes: u64,
    from: mpsc::UnboundedReceiver<ToHub>,
    running: &Running,
    made: &std::sync::mpsc::SyncSender<Arc<Registry>>,
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    // The process is one data plane in one life: the time it began, numbered past any
    // worker's.
    let began = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    let id = began.wrapping_mul(1_024).wrapping_add(1_023);
    let pods = u32::try_from(clients.len()).unwrap_or(u32::MAX);
    let view = View {
        epoch: 0,
        pods: (0..pods).collect(),
    };
    // Keys are what clients send: hashed under a key of the process's own, drawn at random.
    let entropy = RandomState::new();
    let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
    let cycle = service.cycle;
    let engine = if service.pooled {
        // The proxy refuses a pooled service where 128-bit atomics are not native, so the
        // participant is made; if not, the thread ends, and the hub with it.
        let Some(mut participant) = Participant::new(id, nanos(cycle), view, hasher) else {
            return;
        };
        participant.set_ask_timeout(ASK_TIMEOUT);
        participant.set_capacity(max_key_bytes);
        participant.set_workers(workers as u64);
        // One exchange in flight to each service pod, what comes meanwhile gathered for
        // the next (Lucas).
        participant.set_one_in_flight(true);
        participant.set_levers(Levers {
            poll_quiet: service.poll_quiet_keys,
            ask_cold: service.ask_cold_keys,
        });
        participant.set_scheduled(service.schedule_keys);
        if made.send(participant.registry()).is_err() {
            return;
        }
        Engine::Pooled {
            participant: Box::new(participant),
            waiting: HashMap::new(),
            deferred: Vec::new(),
        }
    } else {
        let mut plane = DataPlane::with_hasher(id, nanos(cycle), view, hasher);
        plane.set_ask_timeout(ASK_TIMEOUT);
        plane.set_capacity(max_key_bytes);
        if service.shared {
            Engine::Shared(Rooms::new(plane, hasher))
        } else {
            Engine::Layered(
                Process::new(plane, workers, nanos(SYNC), hasher)
                    .keeping(nanos(service.layered_keep)),
            )
        }
    };
    let state = Rc::new(State {
        engine: RefCell::new(engine),
        clients,
        answers: RefCell::new(vec![None; workers]),
        shared: Arc::clone(&running.shared),
        settled: Arc::clone(&running.settled),
    });
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, serve(state, from, cycle));
}

/// Pooled, a request waiting on the service: the worker that asked, its number for it,
/// the request's keys, and when the process took it.
type Waiting = (usize, u64, Vec<(Box<[u8]>, Contract)>, u64);

/// Pooled, a request deferred here: as it waited, and until when it may.
struct Deferred {
    asked: Waiting,
    until: u64,
}

/// How the process shares its view among its workers: slices of each key's room, each
/// key's one room, or, pooled, the shared entries its participant keeps.
enum Engine {
    Layered(Process<Keyed>),
    Shared(Rooms<Keyed>),
    Pooled {
        participant: Box<Participant<Keyed>>,
        /// Requests that wait on the service, by ticket: the worker, its id, the keys.
        waiting: HashMap<Ticket, Waiting>,
        /// Requests deferred here, decided again at every turn of the loop until their
        /// time is up: as many as a worker may defer, at most.
        deferred: Vec<Deferred>,
    },
}

impl Engine {
    fn plane(&mut self) -> Option<&mut DataPlane<Keyed>> {
        match self {
            Self::Layered(process) => Some(process.plane()),
            Self::Shared(rooms) => Some(rooms.plane()),
            Self::Pooled { .. } => None,
        }
    }

    fn set_capacity(&mut self, bytes: u64) {
        match self {
            Self::Pooled { participant, .. } => participant.set_capacity(bytes),
            other => {
                if let Some(plane) = other.plane() {
                    plane.set_capacity(bytes);
                }
            }
        }
    }

    /// What its keys hold, in bytes, how many there are, and how many it has evicted.
    fn usage(&mut self) -> (u64, u64, u64) {
        match self {
            Self::Pooled { participant, .. } => (
                participant.holding(),
                participant.key_count() as u64,
                participant.evicted(),
            ),
            other => other.plane().map_or((0, 0, 0), |plane| {
                (plane.holding(), plane.key_count() as u64, plane.evicted())
            }),
        }
    }

    fn take_outgoing(&mut self) -> Vec<(PodId, Batch)> {
        match self {
            Self::Pooled { participant, .. } => participant.take_outgoing(),
            other => other
                .plane()
                .map(DataPlane::take_outgoing)
                .unwrap_or_default(),
        }
    }

    /// Pooled, the exchange in flight to `pod` failed: the next may go.
    fn exchanged(&mut self, pod: PodId) {
        if let Self::Pooled { participant, .. } = self {
            participant.exchanged(pod);
        }
    }

    fn on_answers(&mut self, answers: Answers, now: u64) {
        match self {
            Self::Pooled { participant, .. } => participant.on_answers(answers, now),
            other => {
                if let Some(plane) = other.plane() {
                    plane.on_answers(answers, now);
                }
            }
        }
    }

    /// Pooled, the keys its cycles visited, all told.
    fn visits(&self) -> u64 {
        match self {
            Self::Pooled { participant, .. } => participant.visits(),
            _ => 0,
        }
    }

    fn on_cycle(&mut self, now: u64) {
        match self {
            Self::Pooled { participant, .. } => participant.on_cycle(now),
            other => {
                if let Some(plane) = other.plane() {
                    plane.on_cycle(now);
                }
            }
        }
    }
}

/// The process thread's state.
struct State {
    engine: RefCell<Engine>,
    clients: Vec<Client>,
    /// By worker: where its answers go, once it has joined.
    answers: RefCell<Vec<Option<mpsc::UnboundedSender<FromHub>>>>,
    shared: Arc<Shared>,
    settled: Arc<Notify>,
}

impl State {
    fn now(&self) -> u64 {
        u64::try_from(self.shared.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Sends worker `worker` `message`, if it has joined and is there still.
    fn tell(&self, worker: usize, message: FromHub) {
        if let Some(Some(answers)) = self.answers.borrow().get(worker) {
            let _gone = answers.send(message);
        }
    }

    fn on(&self, message: ToHub) {
        let now = self.now();
        match message {
            ToHub::Join { worker, answers } => {
                if let Some(slot) = self.answers.borrow_mut().get_mut(worker) {
                    *slot = Some(answers);
                }
            }
            ToHub::Used { worker, used } => {
                let slices = match &mut *self.engine.borrow_mut() {
                    Engine::Layered(process) => process.on_used(worker, used, now),
                    Engine::Shared(_) | Engine::Pooled { .. } => return,
                };
                self.tell(worker, FromHub::Slices(slices));
                // An ask waiting for this sync may now be decided.
                self.answer();
            }
            ToHub::Ask { worker, id, keys } => {
                let keys: Vec<(&[u8], Contract)> = keys
                    .iter()
                    .map(|(key, contract)| (&**key, *contract))
                    .collect();
                let told = match &mut *self.engine.borrow_mut() {
                    Engine::Layered(process) => process
                        .on_ask(worker, id, &keys, now)
                        .map(|answer| FromHub::Answer { id, answer }),
                    Engine::Shared(rooms) => rooms
                        .on_ask(worker, id, &keys, now)
                        .map(|answer| FromHub::Rooms { id, answer }),
                    Engine::Pooled {
                        participant,
                        waiting,
                        deferred,
                    } => {
                        let owned = keys
                            .iter()
                            .map(|(key, contract)| ((*key).into(), *contract))
                            .collect();
                        self.shared.stats.asks.fetch_add(1, Ordering::Relaxed);
                        let told = pooled_decided(
                            participant,
                            waiting,
                            deferred,
                            (worker, id, owned, now),
                            now,
                        );
                        if told.is_some() {
                            self.shared.stats.answered(0);
                        }
                        told
                    }
                };
                if let Some(told) = told {
                    self.tell(worker, told);
                }
            }
            ToHub::Capacity(bytes) => self.engine.borrow_mut().set_capacity(bytes),
            ToHub::Drain(key) => {
                if let Engine::Pooled { participant, .. } = &mut *self.engine.borrow_mut() {
                    participant.drain(&key, now);
                }
            }
            ToHub::Adopt(entry) => {
                if let Engine::Pooled { participant, .. } = &mut *self.engine.borrow_mut() {
                    participant.adopt(entry);
                }
            }
            ToHub::Signals(entries) => {
                if let Engine::Pooled { participant, .. } = &mut *self.engine.borrow_mut() {
                    participant.on_signals(entries);
                }
            }
        }
    }

    /// Hands the requests the service decided their answers.
    fn answer(&self) {
        let now = self.now();
        let told: Vec<(usize, FromHub)> = match &mut *self.engine.borrow_mut() {
            Engine::Layered(process) => process
                .take_answers(now)
                .into_iter()
                .map(|(worker, id, answer)| (worker, FromHub::Answer { id, answer }))
                .collect(),
            Engine::Shared(rooms) => rooms
                .take_answers(now)
                .into_iter()
                .map(|(worker, id, answer)| (worker, FromHub::Rooms { id, answer }))
                .collect(),
            Engine::Pooled {
                participant,
                waiting,
                deferred,
            } => {
                let mut told: Vec<(usize, FromHub)> = participant
                    .take_decisions()
                    .into_iter()
                    .filter_map(|decided| {
                        let (worker, id, keys, since) = waiting.remove(&decided.ticket)?;
                        self.shared.stats.answered(now.saturating_sub(since));
                        let keys: Vec<(&[u8], Contract)> = keys
                            .iter()
                            .map(|(key, contract)| (&**key, *contract))
                            .collect();
                        Some((
                            worker,
                            FromHub::Pooled {
                                id,
                                admitted: decided.admitted,
                                overloaded: false,
                                refused_by: decided.refused_by,
                                entries: participant.entries(&keys),
                            },
                        ))
                    })
                    .collect();
                // Handed back at their settling: deferred here, as one deferred when asked.
                for ticket in participant.take_retries() {
                    if let Some(asked) = waiting.remove(&ticket) {
                        deferred.push(Deferred {
                            asked,
                            until: now.saturating_add(nanos(DEFER_WAIT)),
                        });
                    }
                }
                // Every request deferred here, decided again.
                for Deferred { asked, until } in std::mem::take(deferred) {
                    let worker = asked.0;
                    let since = asked.3;
                    if now >= until {
                        self.shared.stats.answered(now.saturating_sub(since));
                        told.push((worker, overloaded(asked.1)));
                        continue;
                    }
                    let mut again = Vec::new();
                    if let Some(answer) =
                        pooled_decided(participant, waiting, &mut again, asked, now)
                    {
                        self.shared.stats.answered(now.saturating_sub(since));
                        told.push((worker, answer));
                    }
                    deferred.extend(again.into_iter().map(|again| Deferred { until, ..again }));
                }
                if participant.take_wake() {
                    self.settled.notify_waiters();
                }
                told
            }
        };
        for (worker, message) in told {
            self.tell(worker, message);
        }
    }

    /// Says what its keys hold, for the workers' scrape.
    fn report(&self) {
        let (holding, count, gone) = self.engine.borrow_mut().usage();
        let [bytes, keys, evicted] = &self.shared.usage;
        bytes.store(holding, Ordering::Relaxed);
        keys.store(count, Ordering::Relaxed);
        evicted.store(gone, Ordering::Relaxed);
    }

    /// Sends what the data plane has to send, each batch on a task of its own, and hands it
    /// the answers as they come.
    fn send(self: &Rc<Self>) {
        let outgoing = self.engine.borrow_mut().take_outgoing();
        for (pod, batch) in outgoing {
            let Some(client) = self.clients.get(pod as usize).cloned() else {
                continue;
            };
            let me = Rc::clone(self);
            let entries = (batch.entries.len() + batch.asks.len()) as u64;
            let stats = &self.shared.stats;
            let flying = stats.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            stats.in_flight_most.fetch_max(flying, Ordering::Relaxed);
            stats
                .entries_in_flight
                .fetch_add(entries, Ordering::Relaxed);
            tokio::task::spawn_local(async move {
                // Unanswered, it is sent again or given up as the protocol says.
                let answered = client.exchange(batch).await;
                let stats = &me.shared.stats;
                stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                stats
                    .entries_in_flight
                    .fetch_sub(entries, Ordering::Relaxed);
                if answered.is_err() {
                    me.engine.borrow_mut().exchanged(pod);
                    me.send();
                }
                if let Ok(answers) = answered {
                    let started = Instant::now();
                    let now = me.now();
                    me.engine.borrow_mut().on_answers(answers, now);
                    me.answer();
                    me.send();
                    let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    me.shared
                        .stats
                        .answers_ns
                        .fetch_add(took, Ordering::Relaxed);
                }
            });
        }
    }
}

/// Pooled, what comes of request `asked` at `now`: told its decision now; kept waiting on
/// the service, by ticket; or deferred, to be decided again while its time lasts — or,
/// past as many deferred as a worker may hold, told it is overloaded.
fn pooled_decided(
    participant: &mut Participant<Keyed>,
    waiting: &mut HashMap<Ticket, Waiting>,
    deferred: &mut Vec<Deferred>,
    asked: Waiting,
    now: u64,
) -> Option<FromHub> {
    let keys: Vec<(&[u8], Contract)> = asked
        .2
        .iter()
        .map(|(key, contract)| (&**key, *contract))
        .collect();
    let id = asked.1;
    match participant.decide_all(&keys, now) {
        Pooled::Wait(ticket) => {
            waiting.insert(ticket, asked);
            None
        }
        Pooled::Defer if deferred.len() >= MOST_DEFERRED => Some(overloaded(id)),
        Pooled::Defer => {
            deferred.push(Deferred {
                asked,
                until: now.saturating_add(nanos(DEFER_WAIT)),
            });
            None
        }
        decision => {
            let admitted = decision == Pooled::Admit;
            let refused_by = if admitted {
                Box::default()
            } else {
                participant.waits(&keys, now).collect()
            };
            Some(FromHub::Pooled {
                id,
                admitted,
                overloaded: false,
                refused_by,
                entries: participant.entries(&keys),
            })
        }
    }
}

/// Pooled, the answer to ask `id` that could not be decided in time.
fn overloaded(id: u64) -> FromHub {
    FromHub::Pooled {
        id,
        admitted: false,
        overloaded: true,
        refused_by: Box::default(),
        entries: Vec::new(),
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// The most of the workers' messages the process takes at once before it sends.
const READY: usize = 1_024;

/// The process's loop: the workers' messages as they come, the cycle, and, shared, its sync
/// of the rooms every millisecond.
async fn serve(state: Rc<State>, mut from: mpsc::UnboundedReceiver<ToHub>, cycle: Duration) {
    let mut tick = tokio::time::interval(cycle);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sync = tokio::time::interval(SYNC);
    sync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shared = matches!(&*state.engine.borrow(), Engine::Shared(_));
    loop {
        tokio::select! {
            _ = sync.tick(), if shared => {
                let now = state.now();
                if let Engine::Shared(rooms) = &mut *state.engine.borrow_mut() {
                    rooms.on_sync(now);
                }
            }
            message = from.recv() => match message {
                Some(message) => {
                    let queued = from.len() as u64 + 1;
                    state.shared.stats.queued_most.fetch_max(queued, Ordering::Relaxed);
                    state.on(message);
                    // What else has come is taken too before sending, so that asks that
                    // come together go to the service in one batch rather than an exchange
                    // each; at most so many, so that the cycle is not held up.
                    for _ in 1..READY {
                        let Ok(message) = from.try_recv() else {
                            break;
                        };
                        state.on(message);
                    }
                }
                None => return,
            },
            _ = tick.tick() => {
                let started = Instant::now();
                let now = state.now();
                state.engine.borrow_mut().on_cycle(now);
                let visits = state.engine.borrow().visits();
                state.shared.stats.visits.store(visits, Ordering::Relaxed);
                state.report();
                let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                state.shared.stats.cycle_ns.fetch_add(took, Ordering::Relaxed);
                state.shared.stats.cycle_ns_most.fetch_max(took, Ordering::Relaxed);
            }
        }
        // Pooled, what was deferred decided again, and what settled woken, every turn.
        let started = Instant::now();
        state.answer();
        state.send();
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        state
            .shared
            .stats
            .busy_ns
            .fetch_add(took, Ordering::Relaxed);
    }
}
