//! The process's rate-limiting thread (06 §4): one thread of its own holding the process's
//! one participant of the rate-limiting service and its streams to the service's pods. Its
//! workers decide warm requests themselves from the entries the participant keeps; the thread
//! reports what they admitted to each key's owner, rebases the entries on the owners' answers,
//! decides what they ask about, and evicts idle keys. Its inputs are the workers' messages, the
//! pods' answers and tables, and its cycle; nothing of it is shared with a worker but the
//! entries, a channel, and the clock they read its times by.
//!
//! The thread first opens a stream to each address the service names, its seeds, which tell it
//! the store's table; and then one to each member it sends to, at the address the table gives
//! it, opened when first needed. A key's batches go to the owner the table names, or, the owner
//! unanswered for four cycles, through one of its replicas too, which forwards them to the
//! owner and relays its answers back: the participant takes those as the owner's.

use super::{Decision, EXCHANGE_TIMEOUT, Made, Undecided, Usage};
use crate::ProxyError;
use boring::ssl::SslConnector;
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster_asynchronous::{Message as AsyncMessage, Routed};
use edgerush_config::CompiledRateLimitService;
use edgerush_ratelimit::service::pooled::{
    Decision as Pooled, Entry, Levers, Participant, Registry,
};
use edgerush_ratelimit::service::{self, Keyed};
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::plane::NodeId;
use edgerush_ratelimit_service::cluster::pod::exchange_stream;
use edgerush_ratelimit_service::cluster::wire::asynchronous::{
    self, answered_from, batch_to_wire, too_long,
};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// The time between a worker's handing its signals to its process.
pub(crate) const SYNC: Duration = Duration::from_millis(1);

/// The participant, routed by the store's table.
type Plane = Participant<Keyed, Routed>;

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
        keys: Vec<(Box<[u8]>, service::Limit)>,
        until: u64,
        decided: Decision,
    },
    /// The bytes the keys may hold from now on.
    Capacity(u64),
    /// An entry a worker made, for the process to keep, with the room it holds among its
    /// worker's until the process takes it in.
    Adopt(Arc<Entry>, Made),
    /// Keys visited only when they need it: those its decisions signalled since its last
    /// such message, for the next cycle to visit.
    Signals(Vec<Arc<Entry>>),
    /// For a test: to panic, on the thread itself or in the task of the next answer.
    #[cfg(test)]
    Panic { in_exchange: bool },
    /// For a test: to stand still, as a thread held up does, until told to go on.
    #[cfg(test)]
    Stall(std::sync::mpsc::Receiver<()>),
    /// For a test: the table it routes by, told on the sender.
    #[cfg(test)]
    Table(tokio::sync::oneshot::Sender<Option<edgerush_cluster::Table>>),
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

/// The asynchronous mode's clock, which the process thread and its workers all read: the wall
/// clock as the process read it when the thread began, carried on by the monotonic clock. Every
/// data plane numbers a key's fixed windows by it alike (06 §4), as near as their wall clocks
/// agree, and no time it reads goes back.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Clock {
    epoch: Instant,
    /// The wall clock at `epoch`, in nanoseconds since 1970.
    wall: u64,
}

impl Clock {
    /// The clock from now.
    fn new() -> Self {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            });
        Self {
            epoch: Instant::now(),
            wall,
        }
    }

    /// The time, in nanoseconds since 1970.
    pub(crate) fn now(&self) -> u64 {
        let since = u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.wall.saturating_add(since)
    }

    /// The instant of time `at`, to wait until.
    pub(crate) fn instant(&self, at: u64) -> Instant {
        self.epoch + Duration::from_nanos(at.saturating_sub(self.wall))
    }
}

/// What the process thread and the workers share: the clock its times are read by, what its
/// keys hold, and the pods it waits on.
struct Shared {
    clock: Clock,
    /// Bytes, keys, and evictions and hits forgiven all told, as of its last cycle.
    usage: [AtomicU64; 4],
    stats: Stats,
    /// The table's members, by the address each is reached at, and whether each is
    /// unanswered, as of its last cycle: for the scrape, which takes the lock, never a worker.
    pods: Mutex<Vec<(String, bool)>>,
}

/// What the process thread says of its own work, for the scrape: the asks it takes and how
/// long they wait, those it let go undecided as their requests had gone, its queue, the
/// exchanges it sent and those in flight, and the answers whose buckets were passed over,
/// a later one from their pod taken first, the time its work takes, the keys its cycles visit and
/// those they left out of a full batch, and batches too long to send. The most-of figures are
/// since the last scrape, which takes them.
#[derive(Default)]
pub(crate) struct Stats {
    asks: AtomicU64,
    answered: AtomicU64,
    abandoned: AtomicU64,
    ask_ns: AtomicU64,
    ask_ns_most: AtomicU64,
    queued_most: AtomicU64,
    exchanges: AtomicU64,
    in_flight: AtomicU64,
    in_flight_most: AtomicU64,
    entries_in_flight: AtomicU64,
    cycle_ns: AtomicU64,
    cycle_ns_most: AtomicU64,
    answers_ns: AtomicU64,
    busy_ns: AtomicU64,
    visits: AtomicU64,
    answers_late: AtomicU64,
    keys_left_out: AtomicU64,
    batches_too_long: AtomicU64,
}

impl Stats {
    fn answered(&self, waited: u64) {
        self.answered.fetch_add(1, Ordering::Relaxed);
        self.ask_ns.fetch_add(waited, Ordering::Relaxed);
        self.ask_ns_most.fetch_max(waited, Ordering::Relaxed);
    }

    /// An exchange of `entries` sent.
    fn flying(&self, entries: u64) {
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        let flying = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_flight_most.fetch_max(flying, Ordering::Relaxed);
        self.entries_in_flight.fetch_add(entries, Ordering::Relaxed);
    }

    /// An exchange of `entries` answered, or failed.
    fn landed(&self, entries: u64) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.entries_in_flight.fetch_sub(entries, Ordering::Relaxed);
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
            ("exchanges_total", "counter", read(&self.exchanges)),
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
            ("keys_left_out_total", "counter", read(&self.keys_left_out)),
            (
                "batches_too_long_total",
                "counter",
                read(&self.batches_too_long),
            ),
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
    registry: Arc<Registry<Keyed>>,
    /// What its streams dial the pods with, if with TLS: replaced by a reload that keeps the
    /// thread, for the connections made after it.
    tls: Option<watch::Sender<SslConnector>>,
}

impl Hub {
    /// The process thread of `workers` reaching `service`, its keys holding at most
    /// `max_key_bytes`, started, calling `failed` if it fails once started.
    ///
    /// # Errors
    ///
    /// [`ProxyError::RateLimitThread`] if no thread or runtime could be had, or the machine
    /// has no native 128-bit compare-and-swap for the participant's entries (which the
    /// proxy refuses before).
    pub(crate) fn start(
        service: &CompiledRateLimitService,
        workers: usize,
        max_key_bytes: u64,
        failed: Option<Failed>,
        tls: Option<SslConnector>,
    ) -> Result<Arc<Self>, ProxyError> {
        #[cfg(test)]
        if REFUSE_START.with(Cell::take) {
            return Err(ProxyError::RateLimitThread("refused for a test".to_owned()));
        }
        let shared = Arc::new(Shared {
            clock: Clock::new(),
            usage: Default::default(),
            stats: Stats::default(),
            pods: Mutex::new(Vec::new()),
        });
        let (to, from) = mpsc::unbounded_channel();
        let running = Arc::clone(&shared);
        let set = service.clone();
        let (tls, dialling) = tls.map(watch::channel).unzip();
        // The participant is made on the thread and its registry handed back, or why not.
        let (made, registry) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("edgerush-rate-limits".to_owned())
            .spawn(move || {
                let running = Running {
                    shared: running,
                    failed,
                };
                let dial = dialling.map_or(Dial::Plain, Dial::Secured);
                run(&set, dial, workers, max_key_bytes, from, &running, &made);
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
            tls,
        }))
    }

    /// What its streams dial the pods with from now on: for a reload that keeps it.
    pub(crate) fn set_tls(&self, tls: Option<SslConnector>) {
        if let (Some(dialling), Some(tls)) = (&self.tls, tls) {
            dialling.send_replace(tls);
        }
    }

    /// The bytes its keys may hold from now on, the process's for all its workers: for a
    /// reload that keeps it.
    pub(crate) fn set_capacity(&self, max_key_bytes: u64) {
        let _gone = self.to.send(ToHub::Capacity(max_key_bytes));
    }

    /// Its participant's registry.
    pub(crate) fn registry(&self) -> Arc<Registry<Keyed>> {
        Arc::clone(&self.registry)
    }

    /// The process thread's own measures, and whether it waits on each pod of the table, as
    /// scrape lines: each pod labelled by its address, which a pod keeps for its life, rather
    /// than its member id, new each life.
    pub(crate) fn render_stats(&self) -> String {
        let mut out = self.shared.stats.render();
        out.push_str("# TYPE edgerush_rate_limit_hub_pod_unanswered gauge\n");
        let pods = self
            .shared
            .pods
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for (address, unanswered) in pods.iter() {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_hub_pod_unanswered{{pod=\"{address}\"}} {}",
                u8::from(*unanswered)
            );
        }
        out
    }

    /// Whether it reaches `service` as it is set now: the same seeds and secret, plain or
    /// with TLS as before, the same cycle, the same levers. Another certificate is no change:
    /// it is dialled with from then on ([`set_tls`](Self::set_tls)).
    pub(crate) fn serves(&self, service: &CompiledRateLimitService) -> bool {
        self.service.addresses == service.addresses
            && self.service.key_secret == service.key_secret
            && self.service.tls.is_some() == service.tls.is_some()
            && self.service.cycle == service.cycle
            && self.service.poll_quiet_keys == service.poll_quiet_keys
            && self.service.schedule_keys == service.schedule_keys
            && self.service.cold_share == service.cold_share
    }

    /// The clock its times are read by.
    pub(crate) fn clock(&self) -> Clock {
        self.shared.clock
    }

    /// For a test that lets go of every handle: whether its thread has ended since, told
    /// by what the thread holds of it until it does.
    #[cfg(test)]
    pub(crate) fn ended(&self) -> impl Fn() -> bool + use<> {
        let shared = Arc::downgrade(&self.shared);
        move || shared.strong_count() == 0
    }

    /// What its keys hold, as of its last cycle.
    pub(crate) fn usage(&self) -> Usage {
        let [bytes, keys, evicted, forgiven] = &self.shared.usage;
        Usage {
            bytes: bytes.load(Ordering::Relaxed),
            keys: usize::try_from(keys.load(Ordering::Relaxed)).unwrap_or(usize::MAX),
            evicted: evicted.load(Ordering::Relaxed),
            forgiven: forgiven.load(Ordering::Relaxed),
        }
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
    failed: Option<Failed>,
}

/// The process thread's watch (03 §2): it tells once that the thread failed, if the thread
/// unwinds or a task of its is thrown away after a panic — a stream's, whose answers would
/// otherwise go untaken for ever. It tells nothing once the thread's loop has ended, as it
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

/// Carried by the thread and by each stream's task: tells the watch if dropped before it
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

/// The process thread: a runtime of its own, the participant and its streams, until every
/// sender is gone, as `service` sets it: its seeds, its secret, its cycle and its levers; its
/// streams dialling as `dial` says.
fn run(
    service: &CompiledRateLimitService,
    dial: Dial,
    workers: usize,
    max_key_bytes: u64,
    from: mpsc::UnboundedReceiver<ToHub>,
    running: &Running,
    made: &std::sync::mpsc::SyncSender<Result<Arc<Registry<Keyed>>, String>>,
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
    // The process is one data plane in one life: the time it began, numbered past any
    // worker's.
    let began = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    let id = began.wrapping_mul(1_024).wrapping_add(1_023);
    // Keys are hashed under the secret every data plane shares, so that each sends a key to
    // the pod that owns it; no key has an owner until a pod tells the table.
    let hasher = Keyed::new(service.key_secret.0);
    let cycle = service.cycle;
    // The proxy refuses rate limits where 128-bit atomics are not native, so the
    // participant is made; if not, the thread ends, and the hub with it.
    let Some(mut participant) = Participant::new(id, nanos(cycle), Routed::new(None), hasher)
    else {
        let _gone = made.send(Err("no native 128-bit compare-and-swap".to_owned()));
        return;
    };
    participant.set_exchange_timeout(EXCHANGE_TIMEOUT);
    participant.set_capacity(max_key_bytes);
    participant.set_workers(workers as u64);
    // One exchange in flight to each service pod, what comes meanwhile gathered for the
    // next.
    participant.set_one_in_flight(true);
    participant.set_levers(Levers {
        poll_quiet: service.poll_quiet_keys,
    });
    participant.set_scheduled(service.schedule_keys);
    participant.set_cold_share(service.cold_share);
    if made.send(Ok(participant.registry())).is_err() {
        return;
    }
    // Started: from here on, an end other than the loop's is a failure.
    let watch = Rc::new(Watch {
        failed: running.failed.clone(),
        over: Cell::new(false),
    });
    let mut thread = Guard::new(&watch);
    let seeds = service
        .addresses
        .iter()
        .map(|address| Rc::new(PodStream::new(address.clone(), None)))
        .collect();
    let state = Rc::new(State {
        participant: RefCell::new(participant),
        dial,
        seeds,
        members: RefCell::new(BTreeMap::new()),
        answers: RefCell::new(vec![None; workers]),
        shared: Arc::clone(&running.shared),
        watch: Rc::clone(&watch),
        #[cfg(test)]
        panic_in_exchange: Cell::new(false),
    });
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, serve(state, from, cycle));
    // Every handle gone: the streams still open go with the `LocalSet`, unfailed.
    watch.over.set(true);
    thread.finish();
}

/// A request a worker asked about.
struct Asked {
    /// Its worker's number for the request.
    id: u64,
    keys: Vec<(Box<[u8]>, service::Limit)>,
    /// By when it must be decided: its deadline, set by its worker when it first met the
    /// request, whatever has become of it since.
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
    participant: RefCell<Plane>,
    /// What its streams dial with.
    dial: Dial,
    /// A stream to each seed, sent nothing: what it is told is the store's table.
    seeds: Vec<Rc<PodStream>>,
    /// A stream to each member it has sent to, by its member id, as long as the table
    /// names it.
    members: RefCell<BTreeMap<NodeId, Rc<PodStream>>>,
    /// By worker: where its answers go, once it has joined.
    answers: RefCell<Vec<Option<mpsc::UnboundedSender<FromHub>>>>,
    shared: Arc<Shared>,
    watch: Rc<Watch>,
    /// For a test: the next answer's task panics.
    #[cfg(test)]
    panic_in_exchange: Cell<bool>,
}

impl State {
    fn now(&self) -> u64 {
        self.shared.clock.now()
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
                    id,
                    keys,
                    until,
                    decided: to,
                };
                // Gone or out of time while queued: answered undecided — never decided, or
                // charged, for a request no longer waiting for it.
                let told = match asked.over(now, &self.shared.stats) {
                    Ok(over) => over,
                    Err(asked) => decided(&mut self.participant.borrow_mut(), asked, now),
                };
                self.shared.stats.answered(0);
                self.tell(worker, told);
            }
            ToHub::Capacity(bytes) => self.participant.borrow_mut().set_capacity(bytes),
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
            #[cfg(test)]
            ToHub::Table(told) => {
                let _gone = told.send(self.participant.borrow().routing().table().cloned());
            }
        }
    }

    /// Says what its keys hold, how many answers came late and keys were left out of full
    /// batches, for the workers' scrape, and which of the table's pods it waits on.
    fn report(&self) {
        let participant = self.participant.borrow();
        let [bytes, keys, evicted, forgiven] = &self.shared.usage;
        bytes.store(participant.holding(), Ordering::Relaxed);
        keys.store(participant.key_count() as u64, Ordering::Relaxed);
        evicted.store(participant.evicted(), Ordering::Relaxed);
        forgiven.store(participant.forgiven(), Ordering::Relaxed);
        let stats = &self.shared.stats;
        stats
            .answers_late
            .store(participant.late_answers(), Ordering::Relaxed);
        stats
            .keys_left_out
            .store(participant.keys_left_out(), Ordering::Relaxed);
        drop(participant);
        self.publish_pods();
    }

    /// Says which of the table's pods it waits on, for the scrape: every cycle, and as a
    /// table comes.
    fn publish_pods(&self) {
        let participant = self.participant.borrow();
        let unanswered: BTreeSet<NodeId> = participant.unanswered(self.now()).collect();
        let pods = participant
            .routing()
            .table()
            .map_or_else(Vec::new, |table| {
                table
                    .members
                    .iter()
                    .map(|(id, address)| (address.clone(), unanswered.contains(id)))
                    .collect()
            });
        *self
            .shared
            .pods
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = pods;
    }

    /// Sends what the participant has to send, each batch up the stream to its pod, opened
    /// if it is not; a batch that cannot go — too long, its pod's address unknown, its stream
    /// down or full — fails at once, and goes again as the protocol says.
    fn send(self: &Rc<Self>) {
        let outgoing = self.participant.borrow_mut().take_exchanges();
        for (pod, exchange, batch) in outgoing {
            let entries = batch.entries.len() as u64;
            let batch = batch_to_wire(batch);
            let sent = if too_long(&batch) {
                self.shared
                    .stats
                    .batches_too_long
                    .fetch_add(1, Ordering::Relaxed);
                false
            } else {
                self.member(pod).is_some_and(|stream| {
                    stream.send(
                        self,
                        Sending {
                            exchange,
                            entries,
                            batch,
                        },
                    )
                })
            };
            if !sent {
                self.participant.borrow_mut().exchanged(pod, exchange);
            }
        }
    }

    /// The stream to member `pod`, at the address the table gives it; none if the table
    /// names no such member.
    fn member(&self, pod: NodeId) -> Option<Rc<PodStream>> {
        if let Some(stream) = self.members.borrow().get(&pod) {
            return Some(Rc::clone(stream));
        }
        let address = self
            .participant
            .borrow()
            .routing()
            .table()?
            .members
            .get(&pod)?
            .clone();
        let stream = Rc::new(PodStream::new(address, Some(pod)));
        self.members.borrow_mut().insert(pod, Rc::clone(&stream));
        Some(stream)
    }

    /// Opens each seed's stream that is down, unless it failed to open within the exchange
    /// timeout: at the start, and every cycle after, so that a table comes whenever a seed
    /// answers.
    fn open_seeds(self: &Rc<Self>) {
        let now = self.now();
        for seed in &self.seeds {
            seed.open_if_down(self, now);
        }
    }

    /// Lets go of the streams to members the table no longer names, failing what is out on
    /// them.
    fn let_go(&self) {
        let named = |pod: &NodeId| {
            self.participant
                .borrow()
                .routing()
                .table()
                .is_some_and(|table| table.members.contains_key(pod))
        };
        let gone: Vec<Rc<PodStream>> = {
            let mut members = self.members.borrow_mut();
            let gone: Vec<NodeId> = members.keys().copied().filter(|pod| !named(pod)).collect();
            gone.iter().filter_map(|pod| members.remove(pod)).collect()
        };
        for stream in gone {
            stream.shut(self);
        }
    }

    /// What `stream` was told, at `now`: the table, its pod's
    /// answers to its oldest batch out, or an owner's relayed through it.
    fn told(&self, stream: &PodStream, told: AsyncMessage, now: u64) {
        match told {
            AsyncMessage::Store(StoreMessage::TableReply { table }) => {
                self.participant
                    .borrow_mut()
                    .on_routing(Routed::new(Some(table)), now);
                self.let_go();
                self.publish_pods();
            }
            AsyncMessage::Answers(answers) => {
                // A seed is sent nothing, and answers nothing.
                let Some(pod) = stream.member else {
                    return;
                };
                let Some(out) = stream.out.borrow_mut().pop_front() else {
                    return;
                };
                self.shared.stats.landed(out.entries);
                self.participant.borrow_mut().on_reply_to(
                    out.exchange,
                    Routed::reply(pod, None, answers),
                    now,
                );
            }
            AsyncMessage::Relayed { owner, answers, .. } => {
                let Some(pod) = stream.member else {
                    return;
                };
                self.participant
                    .borrow_mut()
                    .on_reply(Routed::reply(owner, Some(pod), answers), now);
            }
            AsyncMessage::Batch(_) | AsyncMessage::Forwarded { .. } | AsyncMessage::Store(_) => {}
        }
    }
}

/// A batch going up a stream, the exchange it starts, and its entries, for the stats.
struct Sending {
    exchange: u64,
    entries: u64,
    batch: asynchronous::Batch,
}

/// An exchange out on a stream: its number, and its entries.
struct Out {
    exchange: u64,
    entries: u64,
}

/// A stream's link, in one of three states.
enum Link {
    /// Not open: the next batch opens it.
    Down,
    /// Being opened, as connection `generation`, with what is to go up once it is.
    Opening(u64, Vec<Sending>),
    /// Open, as connection `generation`.
    Up(u64, mpsc::Sender<asynchronous::Batch>),
}

/// The process's stream to one pod: a seed, or a member the table names.
struct PodStream {
    address: String,
    /// The member the table names at the address; none for a seed, which is sent nothing.
    member: Option<NodeId>,
    link: RefCell<Link>,
    /// The generation the next connection opened gets.
    next: Cell<u64>,
    /// The exchanges out on the connection open, oldest first: the pod answers its batches
    /// in the order they came, each with an answer of its own.
    out: RefCell<VecDeque<Out>>,
    /// When its last open failed, if it did: no other is tried until the exchange timeout
    /// after, and what would go up fails at once meanwhile.
    failed: Cell<Option<u64>>,
}

impl PodStream {
    fn new(address: String, member: Option<NodeId>) -> Self {
        Self {
            address,
            member,
            link: RefCell::new(Link::Down),
            next: Cell::new(0),
            out: RefCell::new(VecDeque::new()),
            failed: Cell::new(None),
        }
    }

    /// Whether an open failed within the exchange timeout before `now`.
    fn backing_off(&self, now: u64) -> bool {
        self.failed
            .get()
            .is_some_and(|at| now < at.saturating_add(EXCHANGE_TIMEOUT))
    }

    /// The generation of a connection about to be opened.
    fn opening(&self) -> u64 {
        let generation = self.next.get();
        self.next.set(generation.wrapping_add(1));
        generation
    }

    /// Sends `sending` up, opening the stream first if it is down: whether it went, or will
    /// once the stream is open.
    fn send(self: &Rc<Self>, state: &Rc<State>, sending: Sending) -> bool {
        let mut link = self.link.borrow_mut();
        match &mut *link {
            Link::Up(generation, up) => match up.try_send(sending.batch) {
                Ok(()) => {
                    state.shared.stats.flying(sending.entries);
                    self.out.borrow_mut().push_back(Out {
                        exchange: sending.exchange,
                        entries: sending.entries,
                    });
                    true
                }
                Err(mpsc::error::TrySendError::Full(_)) => false,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    let generation = *generation;
                    drop(link);
                    self.ended(state, generation);
                    false
                }
            },
            // One batch out to a pod at a time: what waits here is one, or a few the
            // participant gave up on while the stream opens.
            Link::Opening(_, queued) => {
                queued.push(sending);
                true
            }
            Link::Down => {
                if self.backing_off(state.now()) {
                    return false;
                }
                let generation = self.opening();
                *link = Link::Opening(generation, vec![sending]);
                drop(link);
                spawn_open(state, self, generation);
                true
            }
        }
    }

    /// Opens it if it is down and not backing off: a seed's, to be told the table.
    fn open_if_down(self: &Rc<Self>, state: &Rc<State>, now: u64) {
        let mut link = self.link.borrow_mut();
        if matches!(*link, Link::Down) && !self.backing_off(now) {
            let generation = self.opening();
            *link = Link::Opening(generation, Vec::new());
            drop(link);
            spawn_open(state, self, generation);
        }
    }

    /// Whether connection `generation` is the one it has, opening or open.
    fn is(&self, generation: u64) -> bool {
        matches!(
            &*self.link.borrow(),
            Link::Opening(current, _) | Link::Up(current, _) if *current == generation
        )
    }

    /// Lets go of connection `generation`, which ended or never opened: the stream is down
    /// if it is still the one it has, and what was out on it, or waited for it, has failed.
    fn ended(&self, state: &State, generation: u64) {
        let was = {
            let mut link = self.link.borrow_mut();
            if !matches!(
                &*link,
                Link::Opening(current, _) | Link::Up(current, _) if *current == generation
            ) {
                return;
            }
            std::mem::replace(&mut *link, Link::Down)
        };
        self.failed_out(state, was);
    }

    /// Lets go of it, open or opening, and of what is out on it: the table no longer names
    /// its pod.
    fn shut(&self, state: &State) {
        let was = std::mem::replace(&mut *self.link.borrow_mut(), Link::Down);
        self.failed_out(state, was);
    }

    /// What was out on the connection that was `was`, or waited for it, failed.
    fn failed_out(&self, state: &State, was: Link) {
        let queued = match was {
            Link::Opening(_, queued) => {
                queued.into_iter().map(|sending| sending.exchange).collect()
            }
            Link::Up(..) | Link::Down => Vec::new(),
        };
        let out: Vec<Out> = self.out.borrow_mut().drain(..).collect();
        let Some(pod) = self.member else {
            return;
        };
        let mut participant = state.participant.borrow_mut();
        for out in out {
            state.shared.stats.landed(out.entries);
            participant.exchanged(pod, out.exchange);
        }
        for exchange in queued {
            participant.exchanged(pod, exchange);
        }
    }
}

/// Opens connection `generation` of `stream` on a task of its own, guarded: a panic in it
/// fails the thread.
fn spawn_open(state: &Rc<State>, stream: &Rc<PodStream>, generation: u64) {
    let mut guard = Guard::new(&state.watch);
    let (state, stream) = (Rc::clone(state), Rc::clone(stream));
    tokio::task::spawn_local(async move {
        open(&state, &stream, generation).await;
        guard.finish();
    });
}

/// Opens connection `generation` of `stream`, sends what waited for it, and hands the
/// participant what the pod tells it, until it ends or the process lets go of it.
async fn open(state: &Rc<State>, stream: &Rc<PodStream>, generation: u64) {
    let opened = exchange_stream(&stream.address, &state.dial).await;
    let mut down = match opened {
        Ok(opened) => {
            let queued = {
                let mut link = stream.link.borrow_mut();
                if !matches!(&*link, Link::Opening(current, _) if *current == generation) {
                    return;
                }
                match std::mem::replace(&mut *link, Link::Up(generation, opened.up.clone())) {
                    Link::Opening(_, queued) => queued,
                    Link::Up(..) | Link::Down => Vec::new(),
                }
            };
            stream.failed.set(None);
            for sending in queued {
                let exchange = sending.exchange;
                if opened.up.try_send(sending.batch).is_ok() {
                    state.shared.stats.flying(sending.entries);
                    stream.out.borrow_mut().push_back(Out {
                        exchange,
                        entries: sending.entries,
                    });
                } else if let Some(pod) = stream.member {
                    state.participant.borrow_mut().exchanged(pod, exchange);
                }
            }
            opened.down
        }
        Err(_) => {
            stream.failed.set(Some(state.now()));
            stream.ended(state, generation);
            state.send();
            return;
        }
    };
    loop {
        let message = down.message().await;
        if !stream.is(generation) {
            return;
        }
        let told = match message {
            Ok(Some(told)) => told,
            Ok(None) | Err(_) => {
                stream.ended(state, generation);
                state.send();
                return;
            }
        };
        #[cfg(test)]
        if state.panic_in_exchange.get() {
            panic!("a panic the test makes");
        }
        // A message it cannot use is passed over, as a pod's of another release might send.
        let Ok(told) = answered_from(told) else {
            continue;
        };
        let started = Instant::now();
        state.told(stream, told, state.now());
        state.send();
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        state
            .shared
            .stats
            .answers_ns
            .fetch_add(took, Ordering::Relaxed);
    }
}

/// The decision of request `asked` at `now`, for its worker.
fn decided(participant: &mut Plane, asked: Asked, now: u64) -> FromHub {
    let keys: Vec<(&[u8], service::Limit)> = asked
        .keys
        .iter()
        .map(|(key, contract)| (&**key, *contract))
        .collect();
    let admitted = participant.decide_all(&keys, now) == Pooled::Admit;
    let refused_by = if admitted {
        Box::default()
    } else {
        participant.waits(&keys, now).collect()
    };
    FromHub {
        id: asked.id,
        to: Some(asked.decided),
        admitted,
        undecided: None,
        refused_by,
        entries: participant.entries(&keys),
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

/// The process's loop: its seeds' streams opened, then the workers' messages as they come,
/// and the cycle.
async fn serve(state: Rc<State>, mut from: mpsc::UnboundedReceiver<ToHub>, cycle: Duration) {
    state.open_seeds();
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
                state.open_seeds();
                let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                state.shared.stats.cycle_ns.fetch_add(took, Ordering::Relaxed);
                state.shared.stats.cycle_ns_most.fetch_max(took, Ordering::Relaxed);
            }
        }
        // What is to go to the pods sent, every turn.
        let started = Instant::now();
        state.send();
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        state
            .shared
            .stats
            .busy_ns
            .fetch_add(took, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster_asynchronous::Answers;

    /// The asynchronous mode's clock reads the wall clock, so that data planes started at
    /// different times number a key's fixed windows alike (06 §4), and its times turn back
    /// into the instants they are.
    #[test]
    fn the_clock_reads_the_wall_clock() {
        let clock = Clock::new();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| u64::try_from(since.as_nanos()).unwrap())
            .unwrap();
        let now = clock.now();
        assert!(now.abs_diff(wall) < 1_000_000_000, "{now} against {wall}");
        let later = now + 5_000_000;
        let instant = clock.instant(later);
        assert_eq!(
            instant - clock.epoch,
            Duration::from_nanos(later - clock.wall)
        );
    }

    /// A thread's state, as `run` makes it, routed by a table of `members` (ids and addresses)
    /// rather than told one, its streams none.
    fn state(members: &[(NodeId, &str)]) -> Rc<State> {
        let members: BTreeMap<NodeId, String> = members
            .iter()
            .map(|(id, address)| (*id, (*address).to_owned()))
            .collect();
        let table = edgerush_cluster::table::first(&members, 16, 2, 1);
        let participant = Participant::new(
            1_023,
            10_000_000,
            Routed::new(Some(table)),
            Keyed::new([1, 2]),
        )
        .expect("native 128-bit atomics");
        Rc::new(State {
            participant: RefCell::new(participant),
            dial: Dial::Plain,
            seeds: Vec::new(),
            members: RefCell::new(BTreeMap::new()),
            answers: RefCell::new(Vec::new()),
            // A second old, so that its clock reads past the exchange timeout.
            shared: Arc::new(Shared {
                clock: Clock {
                    epoch: Instant::now() - Duration::from_secs(1),
                    wall: 0,
                },
                usage: Default::default(),
                stats: Stats::default(),
                pods: Mutex::new(Vec::new()),
            }),
            watch: Rc::new(Watch {
                failed: None,
                over: Cell::new(false),
            }),
            panic_in_exchange: Cell::new(false),
        })
    }

    /// A stream to member `pod`, open as connection 0, with `exchanges` out on it in that
    /// order, each sent as the thread counts them.
    fn open_with(state: &State, pod: NodeId, exchanges: &[u64]) -> Rc<PodStream> {
        let stream = Rc::new(PodStream::new("127.0.0.1:9".to_owned(), Some(pod)));
        let (up, _down) = mpsc::channel(1);
        *stream.link.borrow_mut() = Link::Up(0, up);
        stream.next.set(1);
        for exchange in exchanges {
            state.shared.stats.flying(1);
            stream.out.borrow_mut().push_back(Out {
                exchange: *exchange,
                entries: 1,
            });
        }
        stream
    }

    fn answers() -> Answers {
        Answers {
            life: 1,
            number: 1,
            made: 0,
            entries: Vec::new(),
            unserved: Vec::new(),
            table: None,
        }
    }

    /// A pod answers the batches on its stream in the order they came: its answer is the
    /// oldest batch's out, the later left waiting.
    #[test]
    fn a_pods_answer_is_its_oldest_batchs_out() {
        let state = state(&[(1, "pod-1:7000")]);
        let stream = open_with(&state, 1, &[7, 8]);
        state.told(&stream, AsyncMessage::Answers(answers()), 0);
        let left: Vec<u64> = stream.out.borrow().iter().map(|out| out.exchange).collect();
        assert_eq!(left, [8]);
        assert_eq!(state.shared.stats.in_flight.load(Ordering::Relaxed), 1);
    }

    /// A relayed answer is the owner's, through the stream's pod: it answers none of the
    /// batches out on the stream.
    #[test]
    fn a_relayed_answer_leaves_the_streams_batches_out() {
        let state = state(&[(1, "pod-1:7000"), (2, "pod-2:7000")]);
        let stream = open_with(&state, 2, &[7]);
        let relayed = AsyncMessage::Relayed {
            client: 5,
            owner: 1,
            answers: answers(),
        };
        state.told(&stream, relayed, 0);
        assert_eq!(stream.out.borrow().len(), 1);
    }

    /// A stream that ends fails every batch out on it, the thread counting none in flight,
    /// and is down for the next to open; an end of a connection it has replaced changes
    /// nothing.
    #[test]
    fn a_stream_that_ends_fails_its_batches_out() {
        let state = state(&[(1, "pod-1:7000")]);
        let stream = open_with(&state, 1, &[7, 8]);
        stream.ended(&state, 5);
        assert_eq!(stream.out.borrow().len(), 2, "another connection's end");
        stream.ended(&state, 0);
        assert!(stream.out.borrow().is_empty());
        assert_eq!(state.shared.stats.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(
            state.shared.stats.entries_in_flight.load(Ordering::Relaxed),
            0
        );
        assert!(matches!(*stream.link.borrow(), Link::Down));
    }

    /// A table that no longer names a member lets go of the stream to it; those it names are
    /// kept.
    #[test]
    fn a_member_the_table_no_longer_names_is_let_go_of() {
        let state = state(&[(1, "pod-1:7000"), (2, "pod-2:7000")]);
        for pod in [1, 2, 3] {
            let stream = open_with(&state, pod, &[]);
            state.members.borrow_mut().insert(pod, stream);
        }
        state.let_go();
        let kept: Vec<NodeId> = state.members.borrow().keys().copied().collect();
        assert_eq!(kept, [1, 2]);
    }

    /// An open that failed is not tried again within the exchange timeout: what would go
    /// up fails at once meanwhile, and once it has passed the next batch opens the stream.
    #[tokio::test]
    async fn a_stream_whose_open_failed_waits_the_exchange_timeout() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let state = state(&[(1, "pod-1:7000")]);
                let stream = Rc::new(PodStream::new("127.0.0.1:9".to_owned(), Some(1)));
                let sending = |exchange| Sending {
                    exchange,
                    entries: 0,
                    batch: asynchronous::Batch::default(),
                };
                let now = state.now();
                stream.failed.set(Some(now));
                assert!(!stream.send(&state, sending(1)), "backing off");
                assert!(matches!(*stream.link.borrow(), Link::Down));
                stream
                    .failed
                    .set(Some(now.saturating_sub(EXCHANGE_TIMEOUT)));
                assert!(stream.send(&state, sending(2)), "tried again");
                assert!(matches!(*stream.link.borrow(), Link::Opening(..)));
            })
            .await;
    }
}
