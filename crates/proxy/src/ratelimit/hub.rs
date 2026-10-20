//! The process's rate-limiting thread (06 §4): one thread of its own holding the process's
//! one participant of the rate-limiting service and the clients of its pods. Its workers decide warm requests themselves from the entries the
//! participant keeps; the thread reports what they admitted, rebases the entries on the
//! owners' answers, decides what they ask about, and evicts idle keys. Its inputs are the
//! workers' messages, the service's answers and its cycle; nothing of it is shared with a
//! worker but the entries, a channel, and the clock they read its times by.

use super::{ASK_TIMEOUT, DEFER_WAIT, MOST_DEFERRED};
use edgerush_config::CompiledRateLimitService;
use edgerush_ratelimit::service::pooled::{
    Decision as Pooled, Entry, Levers, Participant, Registry,
};
use edgerush_ratelimit::service::{Contract, Keyed, Ticket, View};
use edgerush_ratelimit_service::grpc::Client;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};

/// The time between a worker's handing its signals to its process.
pub(crate) const SYNC: Duration = Duration::from_millis(1);

/// What the process thread calls if it fails (03 §2), for whoever runs the data plane to
/// fail the process: once, from the thread.
#[derive(Clone)]
pub(crate) struct Failed(pub(crate) Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Failed")
    }
}

/// What a worker sends its process.
pub(crate) enum ToHub {
    /// Worker `worker` is there, to be answered on `answers`.
    Join {
        worker: usize,
        answers: mpsc::UnboundedSender<FromHub>,
    },
    /// A request it cannot decide itself, for the process to.
    Ask {
        worker: usize,
        id: u64,
        keys: Vec<(Box<[u8]>, Contract)>,
    },
    /// The bytes the keys may hold from now on.
    Capacity(u64),
    /// A key whose unreported count passed half its most: to drain now.
    Drain(Box<[u8]>),
    /// Cold keys decided locally: an entry a worker made, for the process to keep.
    Adopt(Arc<Entry>),
    /// Keys visited only when they need it: those its decisions signalled since its last
    /// such message, for the next cycle to visit.
    Signals(Vec<Arc<Entry>>),
    /// For a test: to panic, on the thread itself or in the next exchange's task.
    #[cfg(test)]
    Panic { in_exchange: bool },
}

/// What the process sends a worker: the decision on a request it asked about — refused,
/// which of its keys refused and how long each waits — with the entries of its keys, to
/// hold; or, if `overloaded`, none: it could not be decided in the time allowed a deferred
/// request, or too many were deferred.
pub(crate) struct FromHub {
    pub(crate) id: u64,
    pub(crate) admitted: bool,
    pub(crate) overloaded: bool,
    pub(crate) refused_by: Box<[(usize, u64)]>,
    pub(crate) entries: Vec<Arc<Entry>>,
}

/// What the process thread and the workers share: the clock its times are read by, and
/// what its keys hold.
struct Shared {
    epoch: Instant,
    /// Bytes, keys, and evictions all told, as of its last cycle.
    usage: [AtomicU64; 3],
    stats: Stats,
}

/// What the process thread says of its own work, for the scrape: the asks it takes and how
/// long they wait, its queue, its exchanges and the answers whose buckets were passed over,
/// a later one from their pod taken first, the time its work takes, and the keys its cycles visit. The most-of
/// figures are since the last scrape, which takes them.
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
    answers_late: AtomicU64,
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
            ("answers_late_total", "counter", read(&self.answers_late)),
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

/// A handle on a running process thread. It stops once every handle and every worker's
/// sender is gone.
pub(crate) struct Hub {
    pub(crate) to: mpsc::UnboundedSender<ToHub>,
    shared: Arc<Shared>,
    service: CompiledRateLimitService,
    /// Its participant's registry of live entries, for workers meeting a key.
    registry: Arc<Registry>,
    /// Woken when a hold settles or a count drains on a key requests are deferred on, by
    /// whichever thread did it: the workers' deferred requests wait on it.
    settled: Arc<Notify>,
}

impl Hub {
    /// The process thread of `workers` reaching `service`, its keys holding at most
    /// `max_key_bytes`, started, calling `failed` if it fails once started; `None` if an
    /// address cannot be made into one to connect to, no thread could be had, or the
    /// machine has no native 128-bit compare-and-swap for the participant's entries (which
    /// the proxy refuses at start).
    pub(crate) fn start(
        service: &CompiledRateLimitService,
        workers: usize,
        max_key_bytes: u64,
        failed: Option<Failed>,
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
        // The participant is made on the thread and its registry handed back.
        let (made, registry) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("edgerush-rate-limits".to_owned())
            .spawn(move || {
                let running = Running {
                    shared: running,
                    settled: waking,
                    failed,
                };
                run(clients, &set, workers, max_key_bytes, from, &running, &made);
            })
            .ok()?;
        let registry = registry.recv().ok()?;
        Some(Arc::new(Self {
            to,
            shared,
            service: service.clone(),
            registry,
            settled,
        }))
    }

    /// Its participant's registry.
    pub(crate) fn registry(&self) -> Arc<Registry> {
        Arc::clone(&self.registry)
    }

    /// The process thread's own measures, as scrape lines.
    pub(crate) fn render_stats(&self) -> String {
        self.shared.stats.render()
    }

    /// What deferred requests wait on.
    pub(crate) fn settled(&self) -> Arc<Notify> {
        Arc::clone(&self.settled)
    }

    /// Whether it reaches `service` as it is set now: the same pods, the same cycle, the
    /// same levers.
    pub(crate) fn serves(&self, service: &CompiledRateLimitService) -> bool {
        self.service.pods == service.pods
            && self.service.cycle == service.cycle
            && self.service.poll_quiet_keys == service.poll_quiet_keys
            && self.service.ask_cold_keys == service.ask_cold_keys
            && self.service.schedule_keys == service.schedule_keys
    }

    /// The clock its times are read by.
    pub(crate) fn epoch(&self) -> Instant {
        self.shared.epoch
    }

    /// For a test that lets go of every handle: whether its thread has ended since, told
    /// by what the thread holds of it until it does.
    #[cfg(test)]
    pub(crate) fn ended(&self) -> impl Fn() -> bool + use<> {
        let shared = Arc::downgrade(&self.shared);
        move || shared.strong_count() == 0
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

/// What the process thread shares with its handles, and what it calls if it fails.
struct Running {
    shared: Arc<Shared>,
    settled: Arc<Notify>,
    failed: Option<Failed>,
}

/// The process thread's watch (03 §2): it tells once that the thread failed, if the thread
/// unwinds or a task of its is thrown away after a panic — an exchange's, whose pod would
/// otherwise wait on it for ever. It tells nothing once the thread's loop has ended, as it
/// does when the last handle goes: a reload replaced it, or the data plane went.
struct Watch {
    failed: Option<Failed>,
    /// The loop has ended, or the watch has told.
    over: Cell<bool>,
}

impl Watch {
    fn fail(&self) {
        if !self.over.replace(true)
            && let Some(Failed(failed)) = &self.failed
        {
            failed();
        }
    }
}

/// Carried by the thread and by each exchange's task: tells the watch if dropped before it
/// has finished.
struct Guard {
    watch: Rc<Watch>,
    finished: bool,
}

impl Guard {
    fn new(watch: &Rc<Watch>) -> Self {
        Self {
            watch: Rc::clone(watch),
            finished: false,
        }
    }

    /// What it guards has finished. A method, so that a task carrying it captures all of it
    /// rather than the one field.
    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.finished {
            self.watch.fail();
        }
    }
}

/// The process thread: a runtime of its own, the participant and its clients, until every
/// sender is gone, as `service` sets it: its cycle and its levers.
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
    // The proxy refuses rate limits where 128-bit atomics are not native, so the
    // participant is made; if not, the thread ends, and the hub with it.
    let Some(mut participant) = Participant::new(id, nanos(cycle), view, hasher) else {
        return;
    };
    participant.set_ask_timeout(ASK_TIMEOUT);
    participant.set_capacity(max_key_bytes);
    participant.set_workers(workers as u64);
    // One exchange in flight to each service pod, what comes meanwhile gathered for the
    // next.
    participant.set_one_in_flight(true);
    participant.set_levers(Levers {
        poll_quiet: service.poll_quiet_keys,
        ask_cold: service.ask_cold_keys,
    });
    participant.set_scheduled(service.schedule_keys);
    if made.send(participant.registry()).is_err() {
        return;
    }
    // Started: from here on, an end other than the loop's is a failure.
    let watch = Rc::new(Watch {
        failed: running.failed.clone(),
        over: Cell::new(false),
    });
    let mut thread = Guard::new(&watch);
    let state = Rc::new(State {
        participant: RefCell::new(participant),
        waiting: RefCell::new(HashMap::new()),
        deferred: RefCell::new(Vec::new()),
        clients,
        answers: RefCell::new(vec![None; workers]),
        shared: Arc::clone(&running.shared),
        settled: Arc::clone(&running.settled),
        watch: Rc::clone(&watch),
        #[cfg(test)]
        panic_in_exchange: Cell::new(false),
    });
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, serve(state, from, cycle));
    // Every handle gone: the exchanges still in flight go with the `LocalSet`, unfailed.
    watch.over.set(true);
    thread.finish();
}

/// A request waiting on the service: the worker that asked, its number for it, the
/// request's keys, and when the process took it.
type Waiting = (usize, u64, Vec<(Box<[u8]>, Contract)>, u64);

/// A request deferred here: as it waited, and until when it may.
struct Deferred {
    asked: Waiting,
    until: u64,
}

/// The process thread's state.
struct State {
    participant: RefCell<Participant<Keyed>>,
    /// Requests that wait on the service, by ticket: the worker, its id, the keys.
    waiting: RefCell<HashMap<Ticket, Waiting>>,
    /// Requests deferred here, decided again at every turn of the loop until their time is
    /// up: as many as a worker may defer, at most.
    deferred: RefCell<Vec<Deferred>>,
    clients: Vec<Client>,
    /// By worker: where its answers go, once it has joined.
    answers: RefCell<Vec<Option<mpsc::UnboundedSender<FromHub>>>>,
    shared: Arc<Shared>,
    settled: Arc<Notify>,
    watch: Rc<Watch>,
    /// For a test: the next exchange's task panics.
    #[cfg(test)]
    panic_in_exchange: Cell<bool>,
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
            ToHub::Ask { worker, id, keys } => {
                self.shared.stats.asks.fetch_add(1, Ordering::Relaxed);
                let told = decided(
                    &mut self.participant.borrow_mut(),
                    &mut self.waiting.borrow_mut(),
                    &mut self.deferred.borrow_mut(),
                    (worker, id, keys, now),
                    now,
                );
                if let Some(told) = told {
                    self.shared.stats.answered(0);
                    self.tell(worker, told);
                }
            }
            ToHub::Capacity(bytes) => self.participant.borrow_mut().set_capacity(bytes),
            ToHub::Drain(key) => self.participant.borrow_mut().drain(&key, now),
            ToHub::Adopt(entry) => self.participant.borrow_mut().adopt(entry),
            ToHub::Signals(entries) => self.participant.borrow_mut().on_signals(entries),
            #[cfg(test)]
            ToHub::Panic { in_exchange: false } => panic!("a panic the test makes"),
            #[cfg(test)]
            ToHub::Panic { in_exchange: true } => self.panic_in_exchange.set(true),
        }
    }

    /// Hands the requests the service decided their answers, decides again what is
    /// deferred here, and wakes what waits on a settled hold or a drained count.
    fn answer(&self) {
        let now = self.now();
        let mut participant = self.participant.borrow_mut();
        let mut waiting = self.waiting.borrow_mut();
        let mut deferred = self.deferred.borrow_mut();
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
                    FromHub {
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
        for Deferred { asked, until } in std::mem::take(&mut *deferred) {
            let worker = asked.0;
            let since = asked.3;
            if now >= until {
                self.shared.stats.answered(now.saturating_sub(since));
                told.push((worker, overloaded(asked.1)));
                continue;
            }
            let mut again = Vec::new();
            if let Some(answer) = decided(&mut participant, &mut waiting, &mut again, asked, now) {
                self.shared.stats.answered(now.saturating_sub(since));
                told.push((worker, answer));
            }
            deferred.extend(again.into_iter().map(|again| Deferred { until, ..again }));
        }
        if participant.take_wake() {
            self.settled.notify_waiters();
        }
        drop((participant, waiting, deferred));
        for (worker, message) in told {
            self.tell(worker, message);
        }
    }

    /// Says what its keys hold, and how many answers came late, for the workers' scrape.
    fn report(&self) {
        let participant = self.participant.borrow();
        let [bytes, keys, evicted] = &self.shared.usage;
        bytes.store(participant.holding(), Ordering::Relaxed);
        keys.store(participant.key_count() as u64, Ordering::Relaxed);
        evicted.store(participant.evicted(), Ordering::Relaxed);
        self.shared
            .stats
            .answers_late
            .store(participant.late_answers(), Ordering::Relaxed);
    }

    /// Sends what the participant has to send, each batch on a task of its own, and hands
    /// it the answers as they come.
    fn send(self: &Rc<Self>) {
        let outgoing = self.participant.borrow_mut().take_exchanges();
        for (pod, exchange, batch) in outgoing {
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
            let mut guard = Guard::new(&self.watch);
            tokio::task::spawn_local(async move {
                // Unanswered, it is sent again or given up as the protocol says.
                let answered = client.exchange(batch).await;
                #[cfg(test)]
                if me.panic_in_exchange.get() {
                    panic!("a panic the test makes");
                }
                let stats = &me.shared.stats;
                stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                stats
                    .entries_in_flight
                    .fetch_sub(entries, Ordering::Relaxed);
                match answered {
                    Err(_) => {
                        me.participant.borrow_mut().exchanged(pod, exchange);
                        me.send();
                    }
                    Ok(answers) => {
                        let started = Instant::now();
                        let now = me.now();
                        me.participant
                            .borrow_mut()
                            .on_answers_to(exchange, answers, now);
                        me.answer();
                        me.send();
                        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        me.shared
                            .stats
                            .answers_ns
                            .fetch_add(took, Ordering::Relaxed);
                    }
                }
                guard.finish();
            });
        }
    }
}

/// What comes of request `asked` at `now`: told its decision now; kept waiting on the
/// service, by ticket; or deferred, to be decided again while its time lasts — or, past as
/// many deferred as a worker may hold, told it is overloaded.
fn decided(
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
            Some(FromHub {
                id,
                admitted,
                overloaded: false,
                refused_by,
                entries: participant.entries(&keys),
            })
        }
    }
}

/// The answer to ask `id` that could not be decided in time.
fn overloaded(id: u64) -> FromHub {
    FromHub {
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

/// The process's loop: the workers' messages as they come, and the cycle.
async fn serve(state: Rc<State>, mut from: mpsc::UnboundedReceiver<ToHub>, cycle: Duration) {
    let mut tick = tokio::time::interval(cycle);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
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
                let visits = {
                    let mut participant = state.participant.borrow_mut();
                    participant.on_cycle(now);
                    participant.visits()
                };
                state.shared.stats.visits.store(visits, Ordering::Relaxed);
                state.report();
                let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                state.shared.stats.cycle_ns.fetch_add(took, Ordering::Relaxed);
                state.shared.stats.cycle_ns_most.fetch_max(took, Ordering::Relaxed);
            }
        }
        // What was deferred decided again, and what settled woken, every turn.
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
