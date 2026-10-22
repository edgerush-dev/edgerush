//! The process's rate-limiting thread (06 §4): one thread of its own holding the process's
//! one participant of the rate-limiting service and the clients of its pods. Its workers decide warm requests themselves from the entries the
//! participant keeps; the thread reports what they admitted, rebases the entries on the
//! owners' answers, decides what they ask about, and evicts idle keys. Its inputs are the
//! workers' messages, the service's answers and its cycle; nothing of it is shared with a
//! worker but the entries, a channel, and the clock they read its times by.

use super::{ASK_TIMEOUT, Decision, Made, Undecided};
use crate::ProxyError;
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

#[cfg(test)]
thread_local! {
    /// Set, the next start on this thread is refused as one that could have no thread: for
    /// tests of a data plane whose rate limits cannot be started.
    pub(crate) static REFUSE_START: Cell<bool> = const { Cell::new(false) };
}

/// What a worker sends its process.
pub(crate) enum ToHub {
    /// Worker `worker` is there, to be answered on `answers`.
    Join {
        worker: usize,
        answers: mpsc::UnboundedSender<FromHub>,
    },
    /// A request it cannot decide itself, for the process to, by `until` in the process's
    /// clock: its deadline, from when the worker first met it; its decision to go back in
    /// the answer, closed once the request has gone.
    Ask {
        worker: usize,
        id: u64,
        keys: Vec<(Box<[u8]>, Contract)>,
        until: u64,
        decided: Decision,
    },
    /// The bytes the keys may hold from now on.
    Capacity(u64),
    /// A key whose unreported count passed half its most: to drain now.
    Drain(Box<[u8]>),
    /// Cold keys decided locally: an entry a worker made, for the process to keep, with
    /// the room it holds among its worker's until the process takes it in.
    Adopt(Arc<Entry>, Made),
    /// Keys visited only when they need it: those its decisions signalled since its last
    /// such message, for the next cycle to visit.
    Signals(Vec<Arc<Entry>>),
    /// For a test: to panic, on the thread itself or in the next exchange's task.
    #[cfg(test)]
    Panic { in_exchange: bool },
    /// For a test: to stand still, as a thread held up does, until told to go on.
    #[cfg(test)]
    Stall(std::sync::mpsc::Receiver<()>),
}

/// What the process sends a worker: the decision on a request it asked about — refused,
/// which of its keys refused and how long each waits — with the entries of its keys, to
/// hold; or, if `undecided`, none, and why: its time ran out. Every ask is answered once,
/// which gives its worker back the place it holds; where the decision goes, `to`, is none
/// for a request gone, which only gives its place back.
pub(crate) struct FromHub {
    pub(crate) id: u64,
    pub(crate) to: Option<Decision>,
    pub(crate) admitted: bool,
    pub(crate) undecided: Option<Undecided>,
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
/// long they wait, those it let go undecided as their requests had gone, its queue, its exchanges and the answers whose buckets were passed over,
/// a later one from their pod taken first, the time its work takes, and the keys its cycles visit. The most-of
/// figures are since the last scrape, which takes them.
#[derive(Default)]
pub(crate) struct Stats {
    asks: AtomicU64,
    answered: AtomicU64,
    abandoned: AtomicU64,
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
            ("asks_abandoned_total", "counter", read(&self.abandoned)),
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
    /// `max_key_bytes`, started, calling `failed` if it fails once started.
    ///
    /// # Errors
    ///
    /// [`ProxyError::RateLimitThread`] if an address cannot be made into one to connect to
    /// (which the config refuses before), no thread or runtime could be had, or the machine
    /// has no native 128-bit compare-and-swap for the participant's entries (which the
    /// proxy refuses before).
    pub(crate) fn start(
        service: &CompiledRateLimitService,
        workers: usize,
        max_key_bytes: u64,
        failed: Option<Failed>,
    ) -> Result<Arc<Self>, ProxyError> {
        #[cfg(test)]
        if REFUSE_START.with(Cell::take) {
            return Err(ProxyError::RateLimitThread("refused for a test".to_owned()));
        }
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
        // The participant is made on the thread and its registry handed back, or why not.
        let (made, registry) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("edgerush-rate-limits".to_owned())
            .spawn(move || {
                let running = Running {
                    shared: running,
                    settled: waking,
                    failed,
                };
                run(&set, workers, max_key_bytes, from, &running, &made);
            })
            .map_err(|error| ProxyError::RateLimitThread(format!("no thread: {error}")))?;
        let registry = registry
            .recv()
            .unwrap_or_else(|_| Err("it ended before it began".to_owned()))
            .map_err(ProxyError::RateLimitThread)?;
        Ok(Arc::new(Self {
            to,
            shared,
            service: service.clone(),
            registry,
            settled,
        }))
    }

    /// The bytes its keys may hold from now on, the process's for all its workers: for a
    /// reload that keeps it.
    pub(crate) fn set_capacity(&self, max_key_bytes: u64) {
        let _gone = self.to.send(ToHub::Capacity(max_key_bytes));
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
/// sender is gone, as `service` sets it: its pods, its cycle and its levers.
fn run(
    service: &CompiledRateLimitService,
    workers: usize,
    max_key_bytes: u64,
    from: mpsc::UnboundedReceiver<ToHub>,
    running: &Running,
    made: &std::sync::mpsc::SyncSender<Result<Arc<Registry>, String>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _gone = made.send(Err(format!("no runtime: {error}")));
            return;
        }
    };
    // Made in the runtime, which a client's connection runs on, whoever starts the thread.
    let clients = {
        let _in = runtime.enter();
        service
            .pods
            .iter()
            .map(|address| {
                Client::new(address).map_err(|error| format!("pod `{address}`: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let clients = match clients {
        Ok(clients) => clients,
        Err(error) => {
            let _gone = made.send(Err(error));
            return;
        }
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
        let _gone = made.send(Err("no native 128-bit compare-and-swap".to_owned()));
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
    if made.send(Ok(participant.registry())).is_err() {
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

/// A request a worker asked about.
struct Asked {
    /// The worker, and its number for the request.
    worker: usize,
    id: u64,
    keys: Vec<(Box<[u8]>, Contract)>,
    /// When the process took it, and by when it must be decided: its deadline, set by its
    /// worker when it first met the request, whatever has become of it since.
    since: u64,
    until: u64,
    /// Where its decision goes: closed once the request has gone.
    decided: Decision,
}

impl Asked {
    /// What it is answered at `now` without being decided: nothing, if it may still be;
    /// let go, if its request has gone — counted as such, its place given back; or told it
    /// is out of time. Only before a decision starts or a retry: one its process's
    /// participant has taken on is left to settle, its place held till then.
    fn over(self, now: u64, stats: &Stats) -> Result<FromHub, Self> {
        if self.decided.is_closed() {
            stats.abandoned.fetch_add(1, Ordering::Relaxed);
            Ok(FromHub {
                to: None,
                ..undecided(self.id, Undecided::Timeout)
            })
        } else if now >= self.until {
            Ok(FromHub {
                to: Some(self.decided),
                ..undecided(self.id, Undecided::Timeout)
            })
        } else {
            Err(self)
        }
    }
}

/// The process thread's state.
struct State {
    participant: RefCell<Participant<Keyed>>,
    /// Requests that wait on the service, by ticket.
    waiting: RefCell<HashMap<Ticket, Asked>>,
    /// Requests deferred here, decided again at every turn of the loop until their time is
    /// up: no more than their workers have places for.
    deferred: RefCell<Vec<Asked>>,
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
            ToHub::Ask {
                worker,
                id,
                keys,
                until,
                decided: to,
            } => {
                self.shared.stats.asks.fetch_add(1, Ordering::Relaxed);
                let asked = Asked {
                    worker,
                    id,
                    keys,
                    since: now,
                    until,
                    decided: to,
                };
                // Gone or out of time while queued: answered undecided — never decided, or
                // charged, for a request no longer waiting for it.
                let told = match asked.over(now, &self.shared.stats) {
                    Ok(over) => Some(over),
                    Err(asked) => decided(
                        &mut self.participant.borrow_mut(),
                        &mut self.waiting.borrow_mut(),
                        &mut self.deferred.borrow_mut(),
                        asked,
                        now,
                    ),
                };
                if let Some(told) = told {
                    self.shared.stats.answered(0);
                    self.tell(worker, told);
                }
            }
            ToHub::Capacity(bytes) => self.participant.borrow_mut().set_capacity(bytes),
            ToHub::Drain(key) => self.participant.borrow_mut().drain(&key, now),
            // Its room given back once it is taken in.
            ToHub::Adopt(entry, _made) => self.participant.borrow_mut().adopt(entry),
            ToHub::Signals(entries) => self.participant.borrow_mut().on_signals(entries),
            #[cfg(test)]
            ToHub::Panic { in_exchange: false } => panic!("a panic the test makes"),
            #[cfg(test)]
            ToHub::Panic { in_exchange: true } => self.panic_in_exchange.set(true),
            #[cfg(test)]
            ToHub::Stall(until) => {
                let _told = until.recv();
            }
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
                // Gone or not, settled as the participant had it, its place given back.
                let Asked {
                    worker,
                    id,
                    keys,
                    since,
                    decided: to,
                    ..
                } = waiting.remove(&decided.ticket)?;
                self.shared.stats.answered(now.saturating_sub(since));
                let keys: Vec<(&[u8], Contract)> = keys
                    .iter()
                    .map(|(key, contract)| (&**key, *contract))
                    .collect();
                Some((
                    worker,
                    FromHub {
                        id,
                        to: Some(to),
                        admitted: decided.admitted,
                        undecided: None,
                        refused_by: decided.refused_by,
                        entries: participant.entries(&keys),
                    },
                ))
            })
            .collect();
        // Handed back at their settling: deferred here, as one deferred when asked, within
        // the time it had from the first — or, its request gone, let go rather than tried
        // again.
        for ticket in participant.take_retries() {
            if let Some(asked) = waiting.remove(&ticket) {
                deferred.push(asked);
            }
        }
        // Every request deferred here, decided again while its time lasts and it is there.
        for asked in std::mem::take(&mut *deferred) {
            let (worker, since) = (asked.worker, asked.since);
            let asked = match asked.over(now, &self.shared.stats) {
                Ok(over) => {
                    self.shared.stats.answered(now.saturating_sub(since));
                    told.push((worker, over));
                    continue;
                }
                Err(asked) => asked,
            };
            if let Some(answer) = decided(&mut participant, &mut waiting, &mut deferred, asked, now)
            {
                self.shared.stats.answered(now.saturating_sub(since));
                told.push((worker, answer));
            }
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
/// service, by ticket; or deferred, to be decided again while its time lasts. What is kept
/// needs no bound of its own: each holds one of its worker's places until it is answered.
fn decided(
    participant: &mut Participant<Keyed>,
    waiting: &mut HashMap<Ticket, Asked>,
    deferred: &mut Vec<Asked>,
    asked: Asked,
    now: u64,
) -> Option<FromHub> {
    let keys: Vec<(&[u8], Contract)> = asked
        .keys
        .iter()
        .map(|(key, contract)| (&**key, *contract))
        .collect();
    let id = asked.id;
    match participant.decide_all(&keys, now) {
        Pooled::Wait(ticket) => {
            waiting.insert(ticket, asked);
            None
        }
        Pooled::Defer => {
            deferred.push(asked);
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
                to: Some(asked.decided),
                admitted,
                undecided: None,
                refused_by,
                entries: participant.entries(&keys),
            })
        }
    }
}

/// The answer to ask `id` that was not decided, for `why`, going nowhere until it is told
/// where.
fn undecided(id: u64, why: Undecided) -> FromHub {
    FromHub {
        id,
        to: None,
        admitted: false,
        undecided: Some(why),
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
