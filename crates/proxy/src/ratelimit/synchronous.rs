//! The synchronous mode's rate limits (06 §4): every request decided by the owners of its
//! keys in the rate-limiting service. Each worker holds its own stream to each service pod
//! (`edgerush_ratelimit_service::synchronous`): a request's keys go to their owners as the
//! pods' view has them, those of different owners asked in parallel, and the answers come
//! back to the worker that asked, with no thread between. Admitted under all, or refused,
//! the owners that admitted given their charges back; an owner that does not answer in
//! time, or whose stream breaks, leaves the request to `when_unreachable`.
//!
//! The process keeps a breaker for each owner, which its workers share: after `failures`
//! in a row it opens, and every worker decides that owner's keys by `when_unreachable` at
//! once, asking nothing. A thread of the process's own probes it after `probe_after`,
//! opening a stream of its own: the pod's view, sent first on every stream, closes it.
//!
//! With `when_unreachable: local`, the keys of the owners out of reach are decided from the
//! process's own buckets, as local mode decides (`super::local`): each made full when first
//! met without its owner and kept from one outage to the next until the sweep, on the probe
//! thread, forgets it full and idle. Nothing is charged there while the owners answer.

use super::local::{LocalLimiter, Process as LocalKeys, SWEEP};
use super::{Awaited, Failed, KeyedBy, Later, Limited, Refusal, Undecided, Usage, with_keys};
use edgerush_config::{CompiledLimit, CompiledSynchronous, WhenUnreachable};
use edgerush_ratelimit::local::Decision;
use edgerush_ratelimit::service::{Contract, Decided, PodId, View, longest};
use edgerush_ratelimit_service::grpc::Error;
use edgerush_ratelimit_service::synchronous::wire;
use edgerush_ratelimit_service::synchronous::{decide_stream, is_not_serving};
use edgerush_telemetry::{Counter, Sharded};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// How many messages may wait for a stream while it is opened: past them, a request is
/// left to `when_unreachable`, as if the owner did not answer.
const QUEUED: usize = 1_024;

/// How long a probe waits for an owner's view: to connect and to hear from it.
const PROBE_WAIT: Duration = Duration::from_secs(1);

/// How often the probe thread looks for breakers due a probe, at most: probes come no
/// later than this after their time.
const PROBE_LOOK: Duration = Duration::from_millis(50);

/// The process's side of the synchronous mode: the config its workers ask by, a breaker
/// for each owner, and what is counted of them.
#[derive(Debug)]
pub(crate) struct Process {
    config: CompiledSynchronous,
    /// Each owner's, by its place among the addresses.
    breakers: Box<[Arc<Breaker>]>,
    /// What each worker's asks of each owner came to: answered, timed out, stream broken.
    asks: Box<[Arc<OwnerAsks>]>,
    /// Requests left to `when_unreachable`: let on, refused, and decided locally.
    unreachable: Arc<Sharded<[Counter; 3]>>,
    /// The process's own buckets, with `when_unreachable: local`.
    local: Option<Arc<LocalKeys>>,
    /// The clock its breakers' times are read by, kept across a reload with them.
    start: Instant,
}

/// What a process's workers' asks of one owner came to.
type OwnerAsks = Sharded<[Counter; 4]>;

/// How an ask of an owner ended.
#[derive(Clone, Copy)]
enum Ended {
    Answered = 0,
    TimedOut = 1,
    Broken = 2,
    /// Refused as not serving: not ready, not failing.
    NotServing = 3,
}

impl Process {
    /// The process's synchronous mode for `config`, with `workers` workers, and the thread
    /// that probes its owners' open breakers, which calls `failed` if it panics.
    ///
    /// A reload carries an outage's state across from the `running` one: the breaker of
    /// each owner whose address is still listed, whatever else changed, and the process's
    /// own buckets while `when_unreachable` stays `local`, within the new cap; so that
    /// tuning a setting mid-outage neither closes the breakers nor hands out fresh buckets.
    pub(crate) fn start(
        config: CompiledSynchronous,
        workers: NonZeroUsize,
        failed: Option<Failed>,
        running: Option<&Self>,
    ) -> std::io::Result<Arc<Self>> {
        let kept = |address: &String| {
            let running = running?;
            let at = running.config.addresses.iter().position(|a| a == address)?;
            Some((
                Arc::clone(running.breakers.get(at)?),
                Arc::clone(running.asks.get(at)?),
            ))
        };
        let (breakers, asks): (Vec<Arc<Breaker>>, Vec<Arc<OwnerAsks>>) = config
            .addresses
            .iter()
            .map(|address| {
                kept(address).unwrap_or_else(|| {
                    (
                        Arc::new(Breaker::default()),
                        Arc::new(Sharded::new(workers)),
                    )
                })
            })
            .unzip();
        let local = (config.when_unreachable == WhenUnreachable::Local).then(|| {
            match running.and_then(|running| running.local.as_ref()) {
                Some(local) => {
                    local.set_capacity(config.max_key_bytes);
                    Arc::clone(local)
                }
                None => LocalKeys::unswept(workers.get(), config.max_key_bytes),
            }
        });
        let process = Arc::new(Self {
            breakers: breakers.into_boxed_slice(),
            asks: asks.into_boxed_slice(),
            unreachable: running.map_or_else(
                || Arc::new(Sharded::new(workers)),
                |running| Arc::clone(&running.unreachable),
            ),
            local,
            start: running.map_or_else(Instant::now, |running| running.start),
            config,
        });
        let probed = Arc::downgrade(&process);
        std::thread::Builder::new()
            .name("edgerush-rate-limit-probe".to_owned())
            .spawn(move || probe(&probed, failed))?;
        Ok(process)
    }

    /// Whether it asks as `config` says, so that a reload saying the same keeps it.
    pub(crate) fn is_for(&self, config: &CompiledSynchronous) -> bool {
        self.config == *config
    }

    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Counts an ask of the owner at `pod` that ended as `how`, and tells its breaker.
    fn asked(&self, pod: usize, how: Ended) {
        if let Some(asks) = self.asks.get(pod) {
            asks.local()[how as usize].inc();
        }
        if let Some(breaker) = self.breakers.get(pod) {
            match how {
                Ended::Answered => breaker.answered(),
                Ended::TimedOut | Ended::Broken => {
                    breaker.failed(self.now(), self.config.breaker_failures);
                }
                Ended::NotServing => {}
            }
        }
    }

    /// Whether the owner at `pod` is not to be asked: its breaker open.
    fn shut_out(&self, pod: usize) -> bool {
        self.breakers
            .get(pod)
            .is_some_and(|breaker| breaker.is_open())
    }

    /// What its own buckets hold, in bytes, how many there are, and how many were evicted;
    /// nothing without `when_unreachable: local`.
    pub(crate) fn usage(&self) -> Usage {
        self.local
            .as_ref()
            .map_or_else(Usage::default, |local| local.usage())
    }

    /// What its owners' asks came to, their breakers, and what was left to
    /// `when_unreachable`, as metrics.
    pub(crate) fn render_stats(&self) -> String {
        let mut out = String::from("# TYPE edgerush_rate_limit_owner_asks_total counter\n");
        let outcomes = ["answered", "timed_out", "broken", "not_serving"];
        for (pod, asks) in self.asks.iter().enumerate() {
            for (at, outcome) in outcomes.iter().enumerate() {
                let count: u64 = asks.sum(|counters| counters[at].get());
                let _ = writeln!(
                    out,
                    "edgerush_rate_limit_owner_asks_total{{pod=\"{pod}\",outcome=\"{outcome}\"}} {count}"
                );
            }
        }
        out.push_str("# TYPE edgerush_rate_limit_owner_breaker_open gauge\n");
        for (pod, breaker) in self.breakers.iter().enumerate() {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_owner_breaker_open{{pod=\"{pod}\"}} {}",
                u8::from(breaker.is_open())
            );
        }
        out.push_str("# TYPE edgerush_rate_limit_owner_breaker_trips_total counter\n");
        for (pod, breaker) in self.breakers.iter().enumerate() {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_owner_breaker_trips_total{{pod=\"{pod}\"}} {}",
                breaker.trips.load(Ordering::Relaxed)
            );
        }
        out.push_str("# TYPE edgerush_rate_limit_unreachable_total counter\n");
        for (at, action) in ["allow", "deny", "local"].iter().enumerate() {
            let count: u64 = self.unreachable.sum(|counters| counters[at].get());
            let _ = writeln!(
                out,
                "edgerush_rate_limit_unreachable_total{{action=\"{action}\"}} {count}"
            );
        }
        out
    }
}

/// An owner's breaker, which a process's workers share: a few atomics, read on every ask
/// and written only on failures and on the first answer after them.
#[derive(Debug, Default)]
struct Breaker {
    /// Failures in a row.
    failures: AtomicU32,
    /// Closed, 0; open, when it opened or was last probed, in the process's nanoseconds, one
    /// more, so that a time of 0 is not taken for closed.
    opened: AtomicU64,
    /// How many times it opened.
    trips: AtomicU64,
}

impl Breaker {
    fn is_open(&self) -> bool {
        self.opened.load(Ordering::Relaxed) != 0
    }

    /// An answer: the count of failures starts again. Read first, so that a healthy owner's
    /// answers write nothing every worker would share.
    fn answered(&self) {
        if self.failures.load(Ordering::Relaxed) != 0 {
            self.failures.store(0, Ordering::Relaxed);
        }
    }

    /// A failure at `now`: the `threshold`th in a row opens it, once.
    fn failed(&self, now: u64, threshold: u32) {
        let failures = self
            .failures
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if failures >= threshold
            && self
                .opened
                .compare_exchange(
                    0,
                    now.saturating_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            self.trips.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Probed at `now`, if it is still as `seen`, opened at that time: due again after
    /// another period. False if it changed meanwhile — closed, or probed by another thread
    /// sharing it across a reload — so that a stale look never opens it again.
    fn rearm(&self, seen: u64, now: u64) -> bool {
        self.opened
            .compare_exchange(
                seen,
                now.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// The owner heard from: closed.
    fn close(&self) {
        self.failures.store(0, Ordering::Relaxed);
        self.opened.store(0, Ordering::Relaxed);
    }
}

/// Probes the open breakers of `process`'s owners, each `probe_after` after it opened or
/// was last probed, for as long as the process is kept; tells `failed` if it panics, as
/// the other rate-limiting threads do (03 §2). A probe opens a stream to the owner and
/// waits for its view: heard, the breaker closes; not, it waits another `probe_after`.
/// With `when_unreachable: local`, it sweeps the process's own buckets every [`SWEEP`]
/// too, as local mode's thread does.
fn probe(process: &std::sync::Weak<Process>, failed: Option<Failed>) {
    struct Watch(Option<Failed>);
    impl Drop for Watch {
        fn drop(&mut self) {
            if std::thread::panicking()
                && let Some(Failed(failed)) = &self.0
            {
                failed();
            }
        }
    }
    let _watch = Watch(failed);
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    runtime.block_on(async {
        let mut look = tokio::time::interval(PROBE_LOOK);
        look.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut swept = Instant::now();
        loop {
            look.tick().await;
            let Some(held) = process.upgrade() else {
                return;
            };
            if let Some(local) = &held.local
                && swept.elapsed() >= SWEEP
            {
                local.sweep_once();
                swept = Instant::now();
            }
            let probe_after = u64::try_from(held.config.probe_after.as_nanos()).unwrap_or(u64::MAX);
            let now = held.now();
            for (pod, breaker) in held.breakers.iter().enumerate() {
                let opened = breaker.opened.load(Ordering::Relaxed);
                if opened == 0 || now.saturating_sub(opened - 1) < probe_after {
                    continue;
                }
                // Due again only after another period, heard from or not; not this thread's to
                // probe if another closed or probed it meanwhile.
                if !breaker.rearm(opened, now) {
                    continue;
                }
                let Some(address) = held.config.addresses.get(pod).cloned() else {
                    continue;
                };
                let probed = std::sync::Arc::downgrade(&held);
                tokio::spawn(async move {
                    let heard = tokio::time::timeout(PROBE_WAIT, async {
                        let mut stream = decide_stream(&address).await.ok()?;
                        let first = stream.down.message().await.ok()??;
                        matches!(first.message, Some(wire::FromOwner::View(_))).then_some(())
                    })
                    .await;
                    if let (Ok(Some(())), Some(process)) = (heard, probed.upgrade())
                        && let Some(breaker) = process.breakers.get(pod)
                    {
                        breaker.close();
                    }
                });
            }
        }
    });
}

/// Keys with their contracts, kept where they may be needed again.
type Kept = Vec<(Vec<u8>, Contract)>;

/// An owner's answer, or why there is none.
type Answer = Result<wire::Decided, Gone>;

/// Why an owner will not answer an ask.
#[derive(Clone, Copy, Debug)]
enum Gone {
    /// Its stream broke, or could not be opened.
    Broken,
    /// It refused the stream as not serving: the view has not taken it in, in its present
    /// life, and its keys are for their next owner meanwhile.
    NotServing,
}

/// How long a worker waits before trying again a pod that refused it as not serving.
const NOT_SERVING_RETRY: Duration = Duration::from_millis(250);

/// A worker's connection to a pod, as it stands: each one opened has a number of its own,
/// its generation, so that an old connection let go of, whose end comes later, touches
/// only what is its own and never the one that replaced it.
enum Link {
    /// Not open: the next message opens it.
    Down,
    /// Being opened, with what is to go up once it is.
    Opening(u64, Vec<wire::Upstream>),
    /// Open.
    Up(u64, mpsc::Sender<wire::Upstream>),
}

/// What waits on an owner's answer, by the number of the ask.
enum Waiter {
    /// A request.
    Asked(oneshot::Sender<Answer>),
    /// A request that gave up on it and was refused: an admission that comes late is
    /// given back, under these keys.
    GivenUp(Kept),
}

/// A worker's stream to one service pod, and what waits on its answers, each with the
/// generation of the connection it was sent on.
struct Stream {
    address: String,
    link: RefCell<Link>,
    /// The generation the next connection opened gets.
    next: Cell<u64>,
    waiting: RefCell<HashMap<u64, (u64, Waiter)>>,
    /// The pod refused it as not serving, and has not opened a stream since: its keys go
    /// to their next owner.
    not_serving: Cell<bool>,
}

impl Stream {
    /// Sends `message` up, opening the stream first if it is not open: the generation of
    /// the connection it went on, or none if it cannot go — the stream broke, or too much
    /// waits for it.
    fn send(self: &Rc<Self>, message: wire::Upstream, view: &Rc<RefCell<View>>) -> Option<u64> {
        let mut link = self.link.borrow_mut();
        match &mut *link {
            Link::Up(generation, up) => match up.try_send(message) {
                Ok(()) => Some(*generation),
                Err(mpsc::error::TrySendError::Full(_)) => None,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    let generation = *generation;
                    *link = Link::Down;
                    drop(link);
                    self.broke(generation, Gone::Broken);
                    None
                }
            },
            Link::Opening(generation, queued) => {
                if queued.len() >= QUEUED {
                    return None;
                }
                queued.push(message);
                Some(*generation)
            }
            Link::Down => {
                let generation = self.opening();
                *link = Link::Opening(generation, vec![message]);
                tokio::task::spawn_local(open(
                    Rc::downgrade(self),
                    Rc::downgrade(view),
                    generation,
                ));
                Some(generation)
            }
        }
    }

    /// The generation of a connection about to be opened.
    fn opening(&self) -> u64 {
        let generation = self.next.get();
        self.next.set(generation.wrapping_add(1));
        generation
    }

    /// Lets go of the stream, if it is open or opening, and of what waits on it: its owner's
    /// breaker is open.
    fn shut(&self) {
        let was = std::mem::replace(&mut *self.link.borrow_mut(), Link::Down);
        if let Link::Opening(generation, _) | Link::Up(generation, _) = was {
            self.broke(generation, Gone::Broken);
        }
    }

    /// Hands every request waiting on the connection of `generation` the news that its
    /// owner will not answer on it, and `why`; those that gave up on it hear no more.
    fn broke(&self, generation: u64, why: Gone) {
        let mut waiting = self.waiting.borrow_mut();
        let gone: Vec<u64> = waiting
            .iter()
            .filter(|(_, (on, _))| *on == generation)
            .map(|(id, _)| *id)
            .collect();
        let broken: Vec<Waiter> = gone
            .iter()
            .filter_map(|id| waiting.remove(id).map(|(_, waiter)| waiter))
            .collect();
        drop(waiting);
        for waiter in broken {
            if let Waiter::Asked(asked) = waiter {
                let _gone = asked.send(Err(why));
            }
        }
    }

    /// Lets go of the connection of `generation`, which ended: the stream is down if it is
    /// still the one it has, and what waited on it is told `why`.
    fn ended(&self, generation: u64, why: Gone) {
        let mut link = self.link.borrow_mut();
        if let Link::Opening(current, _) | Link::Up(current, _) = &*link
            && *current == generation
        {
            *link = Link::Down;
        }
        drop(link);
        self.broke(generation, why);
    }

    /// The pod refused the connection of `generation` as not serving: marked so, its keys
    /// go to their next owner, and what waited on it is asked of them; tried again in the
    /// background a little later, a stream opening there clearing the mark.
    fn refused(self: &Rc<Self>, generation: u64, view: &Weak<RefCell<View>>) {
        self.not_serving.set(true);
        self.ended(generation, Gone::NotServing);
        let (stream, view) = (Rc::downgrade(self), view.clone());
        tokio::task::spawn_local(async move {
            tokio::time::sleep(NOT_SERVING_RETRY).await;
            let Some(held) = stream.upgrade() else {
                return;
            };
            let mut link = held.link.borrow_mut();
            if !matches!(*link, Link::Down) {
                return;
            }
            let generation = held.opening();
            *link = Link::Opening(generation, Vec::new());
            drop(link);
            tokio::task::spawn_local(open(stream, view, generation));
        });
    }
}

/// Opens the connection of `generation` for `stream`, sends what waited for it, and hands
/// out its answers and the pods' view until it ends, or the worker lets go of it. A
/// connection the worker let go of while it was being opened is closed at once.
async fn open(stream: Weak<Stream>, view: Weak<RefCell<View>>, generation: u64) {
    let Some(address) = stream.upgrade().map(|stream| stream.address.clone()) else {
        return;
    };
    let opened = decide_stream(&address).await;
    let Some(held) = stream.upgrade() else {
        return;
    };
    let mut down = match opened {
        Ok(opened) => {
            let mut link = held.link.borrow_mut();
            let Link::Opening(current, _) = &*link else {
                return;
            };
            if *current != generation {
                return;
            }
            let Link::Opening(_, queued) =
                std::mem::replace(&mut *link, Link::Up(generation, opened.up.clone()))
            else {
                return;
            };
            drop(link);
            held.not_serving.set(false);
            for message in queued {
                if opened.up.try_send(message).is_err() {
                    break;
                }
            }
            opened.down
        }
        Err(Error::Status(status)) if is_not_serving(&status) => {
            held.refused(generation, &view);
            return;
        }
        Err(_) => {
            held.ended(generation, Gone::Broken);
            return;
        }
    };
    drop(held);
    loop {
        let message = down.message().await;
        let Some(held) = stream.upgrade() else {
            return;
        };
        match message {
            Ok(Some(wire::Downstream {
                message: Some(wire::FromOwner::Decided(decided)),
            })) => {
                let waiter = held
                    .waiting
                    .borrow_mut()
                    .remove(&decided.id)
                    .map(|(_, waiter)| waiter);
                match waiter {
                    Some(Waiter::Asked(asked)) => {
                        let _gone = asked.send(Ok(decided));
                    }
                    // Admitted after its request gave up and was refused: given back.
                    Some(Waiter::GivenUp(keys)) if decided.admitted => {
                        if let Some(view) = view.upgrade() {
                            let _lost = held.send(give_back_of(keys), &view);
                        }
                    }
                    Some(Waiter::GivenUp(_)) | None => {}
                }
            }
            Ok(Some(wire::Downstream {
                message: Some(wire::FromOwner::View(said)),
            })) => {
                // The latest the pods agreed, whichever stream says it first.
                if let Some(view) = view.upgrade() {
                    let said = View::from(said);
                    let mut view = view.borrow_mut();
                    if said.epoch > view.epoch {
                        *view = said;
                    }
                }
            }
            Ok(Some(wire::Downstream { message: None })) => {}
            Err(status) if is_not_serving(&status) => {
                held.refused(generation, &view);
                return;
            }
            Ok(None) | Err(_) => {
                held.ended(generation, Gone::Broken);
                return;
            }
        }
    }
}

/// A worker's limiter in the synchronous mode: its streams to the service's pods, the view
/// it reads owners from, and its buffers for a request's keys.
pub(crate) struct SyncLimiter {
    process: Arc<Process>,
    streams: Vec<Rc<Stream>>,
    view: Rc<RefCell<View>>,
    next: Cell<u64>,
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<Contract>>,
    /// Its handles on the process's own buckets, with `when_unreachable: local`.
    local: Option<Rc<LocalLimiter>>,
}

impl std::fmt::Debug for SyncLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncLimiter").finish_non_exhaustive()
    }
}

impl SyncLimiter {
    /// A worker's limiter on `process`: a stream to each pod, all of them opened now, and
    /// every pod taken to serve until the pods say otherwise. Every pod's, not only those a
    /// key has needed: the pods' view comes down each, so that a worker whose owners go
    /// still hears from the rest where their keys went.
    pub(crate) fn new(process: Arc<Process>, index: usize) -> Rc<Self> {
        let streams: Vec<Rc<Stream>> = process
            .config
            .addresses
            .iter()
            .map(|address| {
                Rc::new(Stream {
                    address: address.clone(),
                    link: RefCell::new(Link::Opening(0, Vec::new())),
                    next: Cell::new(1),
                    waiting: RefCell::new(HashMap::new()),
                    not_serving: Cell::new(false),
                })
            })
            .collect();
        let pods = (0..process.config.addresses.len())
            .filter_map(|pod| PodId::try_from(pod).ok())
            .collect();
        let view = Rc::new(RefCell::new(View { epoch: 0, pods }));
        for stream in &streams {
            tokio::task::spawn_local(open(Rc::downgrade(stream), Rc::downgrade(&view), 0));
        }
        let local = process
            .local
            .as_ref()
            .map(|local| LocalLimiter::new(Arc::clone(local), index));
        Rc::new(Self {
            process,
            streams,
            view,
            next: Cell::new(0),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
            local,
        })
    }

    /// What its process's own buckets hold, as [`Process::usage`] says.
    pub(crate) fn usage(&self) -> Usage {
        self.process.usage()
    }

    /// Whether it asks for `process`.
    pub(crate) fn is_on(&self, process: &Arc<Process>) -> bool {
        Arc::ptr_eq(&self.process, process)
    }

    /// Asks the owners about a request from `client` under those of `limits` that
    /// `limited` marks: each owner about its keys, all at once, the decision to come.
    pub(crate) fn decide_all<'f>(
        self: &Rc<Self>,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: impl Into<KeyedBy<'f>>,
    ) -> Limited {
        let until = tokio::time::Instant::now() + self.process.config.timeout;
        with_keys(
            &self.keys,
            &self.contracts,
            limits,
            limited,
            client.into(),
            |keys| {
                if keys.is_empty() {
                    return Limited::Admit;
                }
                Limited::Later(Later(Awaited::Owners(self.ask(keys, until))))
            },
        )
    }

    /// Sends each owner its part of a request under `keys`, to be answered by `until`.
    fn ask(self: &Rc<Self>, keys: &[(&[u8], Contract)], until: tokio::time::Instant) -> Asked {
        // Which owner each key has, as the view has it, around one not serving.
        let owners: Vec<Option<usize>> =
            keys.iter().map(|(key, _)| self.route(key, None)).collect();
        let mut parts: Vec<Part> = Vec::new();
        for (at, owner) in owners.iter().enumerate() {
            match parts.iter_mut().find(|part| part.pod == *owner) {
                Some(part) => part.positions.push(at),
                None => parts.push(Part {
                    pod: *owner,
                    positions: vec![at],
                    kept: Vec::new(),
                    answer: None,
                }),
            }
        }
        for part in &mut parts {
            // Kept for as long as they may be needed again: given back to an owner when
            // another refuses, or when it admits too late; asked of the next owner when it
            // is not serving; decided locally when it is out of reach.
            part.kept = part
                .positions
                .iter()
                .filter_map(|at| keys.get(*at))
                .map(|(key, contract)| (key.to_vec(), *contract))
                .collect();
            self.send_part(part);
        }
        Asked {
            limiter: Rc::clone(self),
            parts: parts.into(),
            until,
        }
    }

    /// The pod that decides `key`: its owner in the view or, the owner not serving, the next,
    /// if that one is; never `refusing`, the pod that has just refused it as not serving.
    /// None when the view names no pod this data plane has an address for.
    fn route(&self, key: &[u8], refusing: Option<usize>) -> Option<usize> {
        let usable = |pod: Option<PodId>| {
            pod.and_then(|pod| usize::try_from(pod).ok())
                .filter(|pod| *pod < self.streams.len())
        };
        let ready = |pod: &usize| !self.streams[*pod].not_serving.get() && Some(*pod) != refusing;
        let (owner, next) = self.view.borrow().owners(key);
        let (owner, next) = (usable(owner), usable(next));
        match owner {
            Some(pod) if ready(&pod) => Some(pod),
            Some(pod) => next
                .filter(ready)
                .or((Some(pod) != refusing).then_some(pod)),
            None => None,
        }
    }

    /// Asks the part's pod about its keys, registering the answer to come on the connection
    /// the ask went on; no answer when it could not be asked.
    fn send_part(self: &Rc<Self>, part: &mut Part) {
        let Some(pod) = part.pod else {
            return;
        };
        // Its breaker open, its keys are decided at once by `when_unreachable`, and the
        // worker lets go of its stream, which may be hung: the next is opened afresh.
        if self.process.shut_out(pod) {
            self.streams[pod].shut();
            return;
        }
        let id = self.next.get();
        self.next.set(id.wrapping_add(1));
        let sent: Vec<wire::Key> = part
            .kept
            .iter()
            .map(|(key, contract)| wire::Key {
                key: key.clone(),
                contract: Some((*contract).into()),
            })
            .collect();
        let (answered, answer) = oneshot::channel();
        let stream = &self.streams[pod];
        let decide = wire::Upstream {
            message: Some(wire::ToOwner::Decide(wire::Decide { id, keys: sent })),
        };
        // Registered once sent, on the connection it went on: nothing else runs on the
        // worker in between, so its answer cannot come first.
        match stream.send(decide, &self.view) {
            Some(generation) => {
                stream
                    .waiting
                    .borrow_mut()
                    .insert(id, (generation, Waiter::Asked(answered)));
                part.answer = Some((id, generation, answer));
            }
            None => self.process.asked(pod, Ended::Broken),
        }
    }

    /// The keys of `part`, refused as not serving by `refusing`, asked again of their next
    /// owners: a part for each, and one with no pod for those that have none.
    fn ask_again(self: &Rc<Self>, part: Part, refusing: usize) -> Vec<Part> {
        let mut parts: Vec<Part> = Vec::new();
        for (position, kept) in part.positions.into_iter().zip(part.kept) {
            let pod = self.route(&kept.0, Some(refusing));
            match parts.iter_mut().find(|again| again.pod == pod) {
                Some(again) => {
                    again.positions.push(position);
                    again.kept.push(kept);
                }
                None => parts.push(Part {
                    pod,
                    positions: vec![position],
                    kept: vec![kept],
                    answer: None,
                }),
            }
        }
        for again in &mut parts {
            self.send_part(again);
        }
        parts
    }

    /// Which of `limits` refused a request from `client` under those `limited` marks, and
    /// how long it should wait, as its owners `told`: by position among its keys, with how
    /// long each waits.
    pub(crate) fn refusal<'a, 'f>(
        &self,
        limits: &'a [Arc<CompiledLimit>],
        limited: u64,
        client: impl Into<KeyedBy<'f>>,
        told: &[(usize, u64)],
    ) -> Refusal<'a> {
        with_keys(
            &self.keys,
            &self.contracts,
            limits,
            limited,
            client.into(),
            |keys| {
                // A key begins with its limit's name and a zero.
                let at = |part: usize| {
                    let (key, _) = keys.get(part)?;
                    let name = key.split(|&byte| byte == 0).next()?;
                    limits
                        .iter()
                        .position(|limit| limit.name.as_bytes() == name)
                };
                let by = told
                    .iter()
                    .filter_map(|(part, _)| at(*part))
                    .filter_map(|at| 1_u64.checked_shl(u32::try_from(at).ok()?))
                    .fold(0, |by, bit| by | bit);
                let longest = longest(told.iter().copied()).and_then(|(part, wait)| {
                    let limit = limits.get(at(part)?)?;
                    Some((&**limit, Duration::from_nanos(wait)))
                });
                Refusal { by, longest }
            },
        )
    }

    /// Gives one request's charge under `keys` back to the owner at `pod`.
    fn give_back(&self, pod: usize, keys: Kept) {
        if let Some(stream) = self.streams.get(pod) {
            let _lost = stream.send(give_back_of(keys), &self.view);
        }
    }
}

/// The message that gives one request's charge under `keys` back.
fn give_back_of(keys: Kept) -> wire::Upstream {
    let keys = keys
        .into_iter()
        .map(|(key, contract)| wire::Key {
            key,
            contract: Some(contract.into()),
        })
        .collect();
    wire::Upstream {
        message: Some(wire::ToOwner::GiveBack(wire::GiveBack { keys })),
    }
}

/// One owner's part of a request: its keys' places among the request's, the keys with
/// their contracts where they may be needed again, and its answer to come; no pod, or no
/// answer, for a part that could not be asked.
struct Part {
    pod: Option<usize>,
    positions: Vec<usize>,
    kept: Kept,
    /// The ask's number, the generation of the connection it went on, and its answer.
    answer: Option<(u64, u64, oneshot::Receiver<Answer>)>,
}

/// A request asked of its owners, waiting on their answers.
pub(crate) struct Asked {
    limiter: Rc<SyncLimiter>,
    parts: std::collections::VecDeque<Part>,
    until: tokio::time::Instant,
}

impl Asked {
    /// The decision: refused if any owner refused, the others' charges given back;
    /// admitted if all admitted; otherwise, some owner out of reach, as `when_unreachable`
    /// says.
    pub(crate) async fn decided(mut self) -> Result<Decided, Undecided> {
        let mut admitted = Vec::new();
        let mut refused_by = Vec::new();
        let mut unreached = None;
        // The keys of the owners out of reach, with their places among the request's.
        let mut out_of_reach: Vec<(Vec<usize>, Kept)> = Vec::new();
        // The asks given up on: their answers may come yet.
        let mut given_up = Vec::new();
        let process = Arc::clone(&self.limiter.process);
        while let Some(mut part) = self.parts.pop_front() {
            let (Some(pod), Some((id, generation, mut answer))) = (part.pod, part.answer.take())
            else {
                unreached = Some(Undecided::Unavailable);
                out_of_reach.push((part.positions, part.kept));
                continue;
            };
            match tokio::time::timeout_at(self.until, &mut answer).await {
                Ok(Ok(Ok(decided))) if decided.admitted => {
                    process.asked(pod, Ended::Answered);
                    admitted.push((part.pod, part.kept));
                }
                Ok(Ok(Ok(decided))) => {
                    process.asked(pod, Ended::Answered);
                    // A refusal stays one: a place outside the keys asked, as only a faulty
                    // pod sends, is taken for the first of them.
                    let at = usize::try_from(decided.position).unwrap_or(usize::MAX);
                    if let Some(&position) =
                        part.positions.get(at).or_else(|| part.positions.first())
                    {
                        refused_by.push((position, decided.wait_ns));
                    }
                }
                // Not ready: its keys asked of their next owners, as if it had gone.
                Ok(Ok(Err(Gone::NotServing))) => {
                    process.asked(pod, Ended::NotServing);
                    let again = self.limiter.ask_again(part, pod);
                    self.parts.extend(again);
                }
                Ok(Ok(Err(Gone::Broken))) | Ok(Err(_)) => {
                    process.asked(pod, Ended::Broken);
                    unreached = Some(Undecided::Unavailable);
                    out_of_reach.push((part.positions, part.kept));
                }
                Err(_) => {
                    process.asked(pod, Ended::TimedOut);
                    unreached = Some(Undecided::Timeout);
                    given_up.push((pod, id, generation, answer, part.kept.clone()));
                    out_of_reach.push((part.positions, part.kept));
                }
            }
        }
        let decision = self.decide(admitted, refused_by, unreached, out_of_reach);
        // What its owners admit too late is given back if the request was refused; if it
        // went on, the owner's charge counts it. Nothing else runs on the worker between
        // the last of them giving up and this, so an answer already in is found here and
        // any later one by the stream.
        let refused = !matches!(&decision, Ok(decided) if decided.admitted);
        for (pod, id, generation, mut answer, keys) in given_up {
            let Some(stream) = self.limiter.streams.get(pod) else {
                continue;
            };
            match answer.try_recv() {
                Ok(Ok(decided)) => {
                    stream.waiting.borrow_mut().remove(&id);
                    if refused && decided.admitted {
                        self.limiter.give_back(pod, keys);
                    }
                }
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => {}
                Err(oneshot::error::TryRecvError::Empty) => {
                    let mut waiting = stream.waiting.borrow_mut();
                    if refused {
                        waiting.insert(id, (generation, Waiter::GivenUp(keys)));
                    } else {
                        waiting.remove(&id);
                    }
                }
            }
        }
        decision
    }

    /// The decision from what the owners said: `admitted` by those that admitted, with
    /// their keys; `refused_by`, the refusals; and `unreached`, why some did not answer, the
    /// keys of those `out_of_reach`.
    fn decide(
        &self,
        admitted: Vec<(Option<usize>, Kept)>,
        refused_by: Vec<(usize, u64)>,
        unreached: Option<Undecided>,
        out_of_reach: Vec<(Vec<usize>, Kept)>,
    ) -> Result<Decided, Undecided> {
        let process = &self.limiter.process;
        let give_back = |admitted: Vec<(Option<usize>, Kept)>| {
            for (pod, keys) in admitted {
                if let Some(pod) = pod
                    && !keys.is_empty()
                {
                    self.limiter.give_back(pod, keys);
                }
            }
        };
        if !refused_by.is_empty() {
            give_back(admitted);
            return Ok(Decided {
                admitted: false,
                refused_by: refused_by.into_boxed_slice(),
            });
        }
        let Some(why) = unreached else {
            return Ok(Decided {
                admitted: true,
                refused_by: Box::default(),
            });
        };
        match process.config.when_unreachable {
            WhenUnreachable::Allow => {
                process.unreachable.local()[0].inc();
                Ok(Decided {
                    admitted: true,
                    refused_by: Box::default(),
                })
            }
            WhenUnreachable::Local if let Some(local) = &self.limiter.local => {
                process.unreachable.local()[2].inc();
                let (positions, keys): (Vec<usize>, Kept) = out_of_reach.into_iter().fold(
                    (Vec::new(), Vec::new()),
                    |(mut positions, mut keys), (more_positions, more_keys)| {
                        positions.extend(more_positions);
                        keys.extend(more_keys);
                        (positions, keys)
                    },
                );
                match local.decide_keys(&keys) {
                    Decision::Admit => Ok(Decided {
                        admitted: true,
                        refused_by: Box::default(),
                    }),
                    Decision::Refuse { position, wait } => {
                        give_back(admitted);
                        Ok(Decided {
                            admitted: false,
                            refused_by: positions
                                .get(position)
                                .map(|at| (*at, wait))
                                .into_iter()
                                .collect(),
                        })
                    }
                }
            }
            // `local` with no buckets of its own is not made: refused as `deny` is.
            WhenUnreachable::Deny | WhenUnreachable::Local => {
                process.unreachable.local()[1].inc();
                give_back(admitted);
                Err(why)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Breaker, Process};
    use edgerush_config::{CompiledSynchronous, WhenUnreachable};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn config(timeout_ms: u64) -> CompiledSynchronous {
        CompiledSynchronous {
            addresses: vec!["127.0.0.1:1".to_owned()],
            timeout: Duration::from_millis(timeout_ms),
            when_unreachable: WhenUnreachable::Deny,
            breaker_failures: 1,
            probe_after: Duration::from_secs(60),
            max_key_bytes: 1 << 20,
        }
    }

    /// A refusal is never taken for an admission: one naming a place outside the keys its
    /// owner was asked about, as only a faulty pod sends, refuses by the first of them.
    #[tokio::test]
    async fn a_refusal_at_a_place_out_of_range_still_refuses() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let process = Process::start(config(100), NonZeroUsize::MIN, None, None).unwrap();
                let limiter = super::SyncLimiter::new(process, 0);
                let (answered, answer) = tokio::sync::oneshot::channel();
                let asked = super::Asked {
                    limiter,
                    parts: vec![super::Part {
                        pod: Some(0),
                        positions: vec![3],
                        kept: Vec::new(),
                        answer: Some((1, 0, answer)),
                    }]
                    .into(),
                    until: tokio::time::Instant::now() + Duration::from_secs(5),
                };
                let decided = edgerush_ratelimit_service::synchronous::wire::Decided {
                    id: 1,
                    admitted: false,
                    position: 9,
                    wait_ns: 7,
                };
                answered.send(Ok(decided)).unwrap();
                let decided = asked.decided().await.unwrap();
                assert!(!decided.admitted);
                assert_eq!(&*decided.refused_by, [(3, 7)]);
            })
            .await;
    }

    /// A process that carries another's breakers carries the clock their times were read
    /// by: a breaker opened before the reload is probed when it is due, not as late again
    /// as the old process had run.
    #[test]
    fn breakers_carried_across_keep_their_clock() {
        let one = NonZeroUsize::MIN;
        let before = Process::start(config(20), one, None, None).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        before.breakers[0].failed(before.now(), 1);
        let after = Process::start(config(30), one, None, Some(&before)).unwrap();
        assert!(after.breakers[0].is_open(), "carried");
        assert!(
            after.now() >= 50_000_000,
            "read by the same clock: {}",
            after.now()
        );
    }

    /// Failures in a row open it, once however many follow; an answer between them starts
    /// the count again; closing it starts it again too.
    #[test]
    fn a_breaker_opens_on_failures_in_a_row_only() {
        let breaker = Breaker::default();
        breaker.failed(10, 3);
        breaker.failed(20, 3);
        breaker.answered();
        breaker.failed(30, 3);
        breaker.failed(40, 3);
        assert!(!breaker.is_open(), "an answer between");
        breaker.failed(50, 3);
        assert!(breaker.is_open());
        breaker.failed(60, 3);
        assert_eq!(breaker.trips.load(std::sync::atomic::Ordering::Relaxed), 1);
        breaker.close();
        assert!(!breaker.is_open());
        breaker.failed(70, 3);
        assert!(!breaker.is_open(), "counted afresh");
    }

    /// A probe that looked before the breaker was closed does not open it again.
    #[test]
    fn a_stale_probe_does_not_open_a_closed_breaker() {
        let breaker = Breaker::default();
        breaker.failed(10, 1);
        let seen = breaker.opened.load(std::sync::atomic::Ordering::Relaxed);
        breaker.close();
        assert!(!breaker.rearm(seen, 20));
        assert!(!breaker.is_open());
        breaker.failed(30, 1);
        let seen = breaker.opened.load(std::sync::atomic::Ordering::Relaxed);
        assert!(breaker.rearm(seen, 40), "as it was seen");
        assert!(breaker.is_open());
    }

    /// A failure at time 0 still opens it: a time of 0 is not taken for closed.
    #[test]
    fn a_breaker_opened_at_the_start_is_open() {
        let breaker = Breaker::default();
        breaker.failed(0, 1);
        assert!(breaker.is_open());
    }
}
