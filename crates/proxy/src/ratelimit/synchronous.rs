//! The synchronous mode's rate limits (06 §4): every request decided by the owners of its
//! keys in the rate-limiting service's store (`edgerush_ratelimit_service::cluster`). Each
//! worker holds its own streams to the service's pods and its own copy of the store's table
//! (`cluster::plane::Plane`): a request's keys are each hashed under the secret every data
//! plane shares and asked of the member the table names their owner, all at once, and the
//! answers come back to the worker that asked, with no thread between. Admitted under all, or
//! refused, the owners that admitted given their charges back.
//!
//! A worker first asks the pods at `service.addresses`, its seeds, for the table, and then
//! reaches each member at the address the table gives it. A key whose owner says it is
//! elsewhere is asked of the owner the newer table names; one whose owner is not serving, or
//! that never reached its owner — the stream to it down, or its breaker open — goes through
//! another member, chosen by the key, which forwards it. One unanswered in time leaves the
//! request to `when_unreachable`, and is never asked again: it may have been counted.
//!
//! The process keeps a breaker for each member, which its workers share: after `failures` in
//! a row it opens, and every worker sends that member's keys through another at once. A
//! thread of the process's own probes it after `probe_after`, opening a stream of its own: the
//! member's table, told first on every stream, closes it.
//!
//! With `when_unreachable: local`, the keys of the owners out of reach are decided from the
//! process's own keys, as local mode decides (`super::local`): each made full when first met
//! without its owner and kept from one outage to the next until the sweep, on the probe
//! thread, forgets it full and idle. Nothing is charged there while the owners answer.

use super::local::{LocalLimiter, Process as LocalKeys, SWEEP};
use super::{Awaited, Failed, KeyedBy, Later, Limited, Refusal, Undecided, Usage, with_keys};
use boring::ssl::SslConnector;
use edgerush_config::{CompiledLimit, CompiledModel, CompiledSynchronous, WhenUnreachable};
use edgerush_ratelimit::local::{Decision, Limit};
use edgerush_ratelimit::service::{Decided, longest};
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::plane::{
    Answer, Due, GIVE_BACK_AGAIN, GIVE_BACK_LATER, Giving, NodeId, Plane, Rule, Told,
};
use edgerush_ratelimit_service::cluster::pod::decide_stream;
use edgerush_ratelimit_service::cluster::wire::{self, Asking, Telling};
use edgerush_telemetry::{Counter, Sharded};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Write as _;
use std::num::{NonZeroU64, NonZeroUsize};
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

/// How many messages may wait for a stream while it is opened: past them, a key is sent
/// another way, as if its stream were down.
const QUEUED: usize = 1_024;

/// How long a probe waits for a member's table: to connect and to hear from it.
const PROBE_WAIT: Duration = Duration::from_secs(1);

/// How often the probe thread looks for breakers due a probe, at most: probes come no
/// later than this after their time.
const PROBE_LOOK: Duration = Duration::from_millis(50);

/// How many times one key of a request is sent at most: its owner, then the owner a newer
/// table names, or another member for one not serving or out of reach. Past it, the key is
/// out of reach.
const SENDS_A_KEY: u8 = 4;

/// The process's side of the synchronous mode: the config its workers ask by, each seed and
/// member of the store they have met with its breaker, and what is counted of them.
#[derive(Debug)]
pub(crate) struct Process {
    config: CompiledSynchronous,
    /// Each seed and member its workers have met, by the address it is reached at: a worker
    /// looks here only when it first meets one, and holds its own handle from then on, so
    /// that an ask takes no lock.
    members: Mutex<BTreeMap<String, Arc<Member>>>,
    /// Requests left to `when_unreachable`: let on, refused, and decided locally.
    unreachable: Arc<Sharded<[Counter; 3]>>,
    /// The process's own keys, with `when_unreachable: local`.
    local: Option<Arc<LocalKeys>>,
    /// The clock its breakers' times are read by, kept across a reload with them.
    start: Instant,
    /// What its streams dial the pods with, if with TLS.
    tls: Option<watch::Receiver<SslConnector>>,
    workers: NonZeroUsize,
}

/// A seed or a member of the store, as a process's workers share it: where it is reached,
/// the member there if a table has named it, its breaker, and what was asked of it.
#[derive(Debug)]
struct Member {
    address: String,
    id: Option<NodeId>,
    breaker: Arc<Breaker>,
    asks: Arc<OwnerAsks>,
}

/// What a process's workers' asks of one member came to.
type OwnerAsks = Sharded<[Counter; 4]>;

/// How an ask of a member ended.
#[derive(Clone, Copy)]
enum Ended {
    Answered = 0,
    TimedOut = 1,
    Broken = 2,
    /// Answered as not serving: not ready, not failing.
    NotServing = 3,
    /// Timed out, sent to a pod only to be forwarded: counted as timed out, but no failure of
    /// that pod's.
    ForwardedTimedOut = 4,
}

impl Ended {
    /// Its place among an owner's counts: a forwarded ask's timeout is a timeout.
    const fn counted(self) -> usize {
        match self {
            Self::ForwardedTimedOut => Self::TimedOut as usize,
            how => how as usize,
        }
    }
}

impl Process {
    /// The process's synchronous mode for `config`, with `workers` workers, and the thread
    /// that probes its members' open breakers, which calls `failed` if it panics.
    ///
    /// A reload carries an outage's state across from the `running` one: every member met,
    /// its breaker with it, whatever else changed, and the process's own keys while
    /// `when_unreachable` stays `local`, within the new cap; so that tuning a setting
    /// mid-outage neither closes the breakers nor hands out fresh buckets.
    pub(crate) fn start(
        config: CompiledSynchronous,
        workers: NonZeroUsize,
        failed: Option<Failed>,
        running: Option<&Self>,
        tls: Option<SslConnector>,
    ) -> std::io::Result<Arc<Self>> {
        let members = running.map_or_else(BTreeMap::new, |running| running.members().clone());
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
            members: Mutex::new(members),
            unreachable: running.map_or_else(
                || Arc::new(Sharded::new(workers)),
                |running| Arc::clone(&running.unreachable),
            ),
            local,
            start: running.map_or_else(Instant::now, |running| running.start),
            config,
            // Fixed for its life: a reload that changes it starts another.
            tls: tls.map(|tls| watch::channel(tls).1),
            workers,
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

    /// The seeds and members met, a lock poisoned taken as it was: what it guards is a map
    /// whose every change is one insert.
    fn members(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Arc<Member>>> {
        self.members.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whom `address` reaches, as the workers share it — a seed, with no `id`, or member `id`
    /// — met now if not before. A seed's pod named a member keeps the seed's breaker; another
    /// member at an address, the pod there restarted, starts with a breaker of its own
    /// (decided: one a member).
    fn member_at(&self, address: &str, id: Option<NodeId>) -> Arc<Member> {
        let mut members = self.members();
        let (breaker, asks) = match members.get(address) {
            Some(found) if id.is_none() || found.id == id => return Arc::clone(found),
            Some(found) if found.id.is_none() => {
                (Arc::clone(&found.breaker), Arc::clone(&found.asks))
            }
            _ => (
                Arc::new(Breaker::default()),
                Arc::new(Sharded::new(self.workers)),
            ),
        };
        let member = Arc::new(Member {
            address: address.to_owned(),
            id,
            breaker,
            asks,
        });
        members.insert(address.to_owned(), Arc::clone(&member));
        member
    }

    /// What its streams dial with.
    fn dial(&self) -> Dial {
        self.tls
            .as_ref()
            .map_or(Dial::Plain, |tls| Dial::Secured(tls.clone()))
    }

    /// Counts an ask of `member` that ended as `how`, and tells its breaker.
    fn asked(&self, member: &Member, how: Ended) {
        member.asks.local()[how.counted()].inc();
        match how {
            Ended::Answered => member.breaker.answered(),
            Ended::TimedOut | Ended::Broken => {
                member
                    .breaker
                    .failed(self.now(), self.config.breaker_failures);
            }
            Ended::NotServing | Ended::ForwardedTimedOut => {}
        }
    }

    /// What its own keys hold, in bytes, how many there are, and how many were evicted;
    /// nothing without `when_unreachable: local`.
    pub(crate) fn usage(&self) -> Usage {
        self.local
            .as_ref()
            .map_or_else(Usage::default, |local| local.usage())
    }

    /// What its members' asks came to, their breakers, and what was left to
    /// `when_unreachable`, as metrics: each member labelled by its address, which a pod keeps
    /// for its life, rather than its id, new each life.
    pub(crate) fn render_stats(&self) -> String {
        let members: Vec<Arc<Member>> = self.members().values().cloned().collect();
        let mut out = String::from("# TYPE edgerush_rate_limit_owner_asks_total counter\n");
        let outcomes = ["answered", "timed_out", "broken", "not_serving"];
        for member in &members {
            for (at, outcome) in outcomes.iter().enumerate() {
                let count: u64 = member.asks.sum(|counters| counters[at].get());
                let _ = writeln!(
                    out,
                    "edgerush_rate_limit_owner_asks_total{{pod=\"{}\",outcome=\"{outcome}\"}} {count}",
                    member.address
                );
            }
        }
        out.push_str("# TYPE edgerush_rate_limit_owner_breaker_open gauge\n");
        for member in &members {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_owner_breaker_open{{pod=\"{}\"}} {}",
                member.address,
                u8::from(member.breaker.is_open())
            );
        }
        out.push_str("# TYPE edgerush_rate_limit_owner_breaker_trips_total counter\n");
        for member in &members {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_owner_breaker_trips_total{{pod=\"{}\"}} {}",
                member.address,
                member.breaker.trips.load(Ordering::Relaxed)
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

/// A member's breaker, which a process's workers share: a few atomics, read on every ask
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

    /// An answer: the count of failures starts again. Read first, so that a healthy member's
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

    /// The member heard from: closed.
    fn close(&self) {
        self.failures.store(0, Ordering::Relaxed);
        self.opened.store(0, Ordering::Relaxed);
    }
}

/// Probes the open breakers of `process`'s members, each `probe_after` after it opened or
/// was last probed, for as long as the process is kept; tells `failed` if it panics, as
/// the other rate-limiting threads do (03 §2). A probe opens a stream to the member and
/// waits for its table: heard, the breaker closes; not, it waits another `probe_after`.
/// With `when_unreachable: local`, it sweeps the process's own keys every [`SWEEP`] too, as
/// local mode's thread does.
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
            let members: Vec<Arc<Member>> = held.members().values().cloned().collect();
            for member in members {
                let opened = member.breaker.opened.load(Ordering::Relaxed);
                if opened == 0 || now.saturating_sub(opened - 1) < probe_after {
                    continue;
                }
                // Due again only after another period, heard from or not; not this thread's to
                // probe if another closed or probed it meanwhile.
                if !member.breaker.rearm(opened, now) {
                    continue;
                }
                let dial = held.dial();
                tokio::spawn(async move {
                    let heard = tokio::time::timeout(PROBE_WAIT, async {
                        let mut stream = decide_stream(&member.address, &dial).await.ok()?;
                        let first = stream.down.message().await.ok()??;
                        matches!(first.told, Some(wire::Told::Table(_))).then_some(())
                    })
                    .await;
                    if let Ok(Some(())) = heard {
                        member.breaker.close();
                    }
                });
            }
        }
    });
}

/// An owner's answer, or why there is none.
type Answered = Result<Answer, Gone>;

/// Why a pod will not answer an ask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Gone {
    /// Its connection broke with the ask on it, which may have reached the pod.
    Broken,
    /// Its connection never opened, the ask still waiting for it: it never reached the pod.
    Unreached,
}

impl Gone {
    /// Why the asks on a connection that was `link` are gone, the connection let go of.
    const fn of(link: &Link) -> Self {
        match link {
            Link::Opening(..) => Self::Unreached,
            Link::Down | Link::Up(..) => Self::Broken,
        }
    }
}

/// A worker's stream to one pod, in one of three states.
enum Link {
    /// Not open: the next message opens it.
    Down,
    /// Being opened, with what is to go up once it is.
    Opening(u64, Vec<Asking>),
    /// Open.
    Up(u64, mpsc::Sender<Asking>),
}

/// What waits on a pod's answer, by the number of the ask.
enum Waiter {
    /// An ask: a request's key, or a give-back.
    Asked(oneshot::Sender<Answered>),
    /// A request's key that gave up on it and was refused: an admission that comes late is
    /// given back, under this rule.
    GivenUp(u64, Rule),
}

/// A worker's stream to one pod, by its address, and what waits on its answers, each with the
/// generation of the connection it was sent on.
struct Stream {
    address: String,
    dial: Dial,
    link: RefCell<Link>,
    /// The generation the next connection opened gets.
    next: Cell<u64>,
    waiting: RefCell<HashMap<u64, (u64, Waiter)>>,
}

impl Stream {
    /// Sends `message` up, opening the stream first if it is not open: the generation of
    /// the connection it went on, or none if it cannot go — the stream broke, or too much
    /// waits for it — so that it never reached the pod.
    fn send(self: &Rc<Self>, message: Asking, limiter: &Weak<SyncLimiter>) -> Option<u64> {
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
                tokio::task::spawn_local(open(Rc::downgrade(self), limiter.clone(), generation));
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

    /// Hands every ask waiting on the connection of `generation` the news that its pod will
    /// not answer on it, and `why`; those that gave up on it hear no more.
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

    /// Lets go of the stream, if it is open or opening, and of what waits on it: its pod's
    /// breaker is open.
    fn shut(&self) {
        let was = std::mem::replace(&mut *self.link.borrow_mut(), Link::Down);
        if let Link::Opening(generation, _) | Link::Up(generation, _) = was {
            self.broke(generation, Gone::of(&was));
        }
    }

    /// Lets go of the connection of `generation`, which ended, or never opened as `why` says:
    /// the stream is down if it is still the one it has, and what waited on it is told.
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
}

/// Opens the connection of `generation` for `stream`, sends what waited for it, and hands
/// out its answers and the tables it is told, through `limiter`'s plane, until it ends or the
/// worker lets go of it.
async fn open(stream: Weak<Stream>, limiter: Weak<SyncLimiter>, generation: u64) {
    let Some((address, dial)) = stream
        .upgrade()
        .map(|stream| (stream.address.clone(), stream.dial.clone()))
    else {
        return;
    };
    let opened = decide_stream(&address, &dial).await;
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
            for message in queued {
                if opened.up.try_send(message).is_err() {
                    break;
                }
            }
            opened.down
        }
        Err(_) => {
            held.ended(generation, Gone::Unreached);
            return;
        }
    };
    drop(held);
    loop {
        let message = down.message().await;
        let (Some(held), Some(worker)) = (stream.upgrade(), limiter.upgrade()) else {
            return;
        };
        let told: Telling = match message {
            Ok(Some(told)) => told,
            Ok(None) | Err(_) => {
                held.ended(generation, Gone::Broken);
                return;
            }
        };
        // A message it cannot use is passed over, as a pod's of another release might send.
        let Ok(Told::Answer { id, answer }) = worker.plane.borrow_mut().told(told) else {
            continue;
        };
        let waiter = held
            .waiting
            .borrow_mut()
            .remove(&id)
            .map(|(_, waiter)| waiter);
        match (waiter, answer) {
            (Some(Waiter::Asked(asked)), answer) => {
                let _gone = asked.send(Ok(answer));
            }
            // Admitted after its request gave up and was refused: given back.
            (Some(Waiter::GivenUp(key, rule)), Answer::Admitted(receipt)) => {
                worker.give_back(key, rule, receipt);
            }
            (Some(Waiter::GivenUp(..)) | None, _) => {}
        }
    }
}

/// What a key is held to in the synchronous mode: the store's rule, which its owner decides
/// by, and the same limit as local mode keeps it, for `when_unreachable: local`; none for a
/// rolling window, which local mode does not keep and its config refuses with it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HeldTo {
    rule: Rule,
    local: Option<Limit>,
}

impl super::Held for HeldTo {
    fn of(limit: &CompiledLimit, count: NonZeroU64) -> Option<Self> {
        let (per, window) = (count.get(), limit.window.get());
        let rule = match limit.model {
            CompiledModel::Bucket { burst, .. } => Rule::Bucket {
                per,
                period: window,
                burst: burst.get(),
            },
            CompiledModel::FixedWindow { .. } => Rule::Window { per, window },
            CompiledModel::RollingWindow { .. } => Rule::Rolling { per, window },
        };
        Some(Self {
            rule,
            local: <Limit as super::Held>::of(limit, count),
        })
    }
}

/// Whom a key is sent to: a member the table names, or, with no table yet, a seed, by its
/// place among them, which forwards it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum To {
    Member(NodeId),
    Seed(usize),
}

/// A worker's limiter in the synchronous mode: its streams to the service's pods, the table
/// it routes by, the seeds and members it has met, and its buffers for a request's keys.
pub(crate) struct SyncLimiter {
    process: Arc<Process>,
    plane: RefCell<Plane>,
    /// By address: a seed's, and each member's the table gives.
    streams: RefCell<HashMap<String, Rc<Stream>>>,
    /// Each seed, as the process shares it, in the order the config gives them.
    seeds: Vec<Arc<Member>>,
    /// Each member met, as the process shares it.
    members: RefCell<HashMap<NodeId, Arc<Member>>>,
    next: Cell<u64>,
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<HeldTo>>,
    /// Its handles on the process's own keys, with `when_unreachable: local`.
    local: Option<Rc<LocalLimiter>>,
}

impl std::fmt::Debug for SyncLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncLimiter").finish_non_exhaustive()
    }
}

impl SyncLimiter {
    /// A worker's limiter on `process`: a stream to each seed opened now, for the table, which
    /// each of them tells as the stream opens.
    pub(crate) fn new(process: Arc<Process>, index: usize) -> Rc<Self> {
        let local = process
            .local
            .as_ref()
            .map(|local| LocalLimiter::new(Arc::clone(local), index));
        let seeds = process
            .config
            .addresses
            .iter()
            .map(|seed| process.member_at(seed, None))
            .collect();
        let limiter = Rc::new(Self {
            plane: RefCell::new(Plane::new(process.config.key_secret.0)),
            process,
            streams: RefCell::new(HashMap::new()),
            seeds,
            members: RefCell::new(HashMap::new()),
            next: Cell::new(0),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
            local,
        });
        for seed in &limiter.process.config.addresses {
            let stream = limiter.stream(seed);
            let generation = stream.opening();
            *stream.link.borrow_mut() = Link::Opening(generation, Vec::new());
            tokio::task::spawn_local(open(
                Rc::downgrade(&stream),
                Rc::downgrade(&limiter),
                generation,
            ));
        }
        limiter
    }

    /// What its process's own keys hold, as [`Process::usage`] says.
    pub(crate) fn usage(&self) -> Usage {
        self.process.usage()
    }

    /// Whether it asks for `process`.
    pub(crate) fn is_on(&self, process: &Arc<Process>) -> bool {
        Arc::ptr_eq(&self.process, process)
    }

    /// Its stream to the pod at `address`, made, not opened, if it has none.
    fn stream(&self, address: &str) -> Rc<Stream> {
        Rc::clone(
            self.streams
                .borrow_mut()
                .entry(address.to_owned())
                .or_insert_with(|| {
                    Rc::new(Stream {
                        address: address.to_owned(),
                        dial: self.process.dial(),
                        link: RefCell::new(Link::Down),
                        next: Cell::new(0),
                        waiting: RefCell::new(HashMap::new()),
                    })
                }),
        )
    }

    /// Member `id`, as the process shares it, if its table gives it an address: looked up in
    /// the process the first time only.
    fn member(&self, id: NodeId) -> Option<Arc<Member>> {
        if let Some(member) = self.members.borrow().get(&id) {
            return Some(Arc::clone(member));
        }
        let address = self.plane.borrow().address(id)?.to_owned();
        let member = self.process.member_at(&address, Some(id));
        self.members.borrow_mut().insert(id, Arc::clone(&member));
        Some(member)
    }

    /// Whether member `id`'s breaker is open: its keys go another way.
    fn shut_out(&self, id: NodeId) -> bool {
        self.member(id)
            .is_some_and(|member| self.passed_over(&member))
    }

    /// Whether `member`'s breaker is open: its keys go another way, and the worker lets go of
    /// its stream to it, which may be hung, so that the next it opens there is afresh.
    fn passed_over(&self, member: &Member) -> bool {
        let open = member.breaker.is_open();
        if open && let Some(stream) = self.streams.borrow().get(&member.address) {
            stream.shut();
        }
        open
    }

    /// Whom to send `key` to and where it is reached: its owner unless `avoid` names it or its
    /// breaker is open, else another member chosen by the key; with no table yet, a seed chosen
    /// by the key among those not in `avoid_seeds` and with their breakers closed. None when
    /// none is left.
    fn route(&self, key: u64, avoid: &[NodeId], avoid_seeds: &[usize]) -> Option<(To, String)> {
        let plane = self.plane.borrow();
        if plane.table().is_none() {
            let open = || {
                self.seeds
                    .iter()
                    .enumerate()
                    .filter(|(at, seed)| !avoid_seeds.contains(at) && !self.passed_over(seed))
            };
            let count = u64::try_from(open().count())
                .ok()
                .filter(|count| *count > 0)?;
            let (at, seed) = open().nth(usize::try_from(key % count).ok()?)?;
            return Some((To::Seed(at), seed.address.clone()));
        }
        let to = plane.through(key, |member| {
            avoid.contains(&member) || self.shut_out(member)
        })?;
        Some((To::Member(to), plane.address(to)?.to_owned()))
    }

    /// Counts an ask of `to` that ended as `how`.
    fn asked(&self, to: To, how: Ended) {
        let member = match to {
            To::Member(id) => self.member(id),
            To::Seed(at) => self.seeds.get(at).cloned(),
        };
        if let Some(member) = member {
            self.process.asked(&member, how);
        }
    }

    /// The next number to ask by.
    fn number(&self) -> u64 {
        let id = self.next.get();
        self.next.set(id.wrapping_add(1));
        id
    }

    /// Asks the owners about a request from `client` under those of `limits` that
    /// `limited` marks: each key of its owner, all at once, the decision to come.
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

    /// Sends each of `keys` to its owner, to be answered by `until`.
    fn ask(self: &Rc<Self>, keys: &[(&[u8], HeldTo)], until: tokio::time::Instant) -> Asked {
        let parts = keys
            .iter()
            .enumerate()
            .map(|(position, (bytes, held))| {
                let mut part = Part {
                    position,
                    key: self.plane.borrow().key(bytes),
                    held: *held,
                    bytes: bytes.to_vec(),
                    avoid: Vec::new(),
                    avoid_seeds: Vec::new(),
                    sends: 0,
                    sent: None,
                };
                self.send_part(&mut part);
                part
            })
            .collect();
        Asked {
            limiter: Rc::clone(self),
            parts,
            until,
        }
    }

    /// Sends `part` to whom its key routes to, and again another way each time that cannot
    /// reach its pod — its stream down or too full — counting that against the member: sent,
    /// or out of reach, every member passed over or [`SENDS_A_KEY`] spent.
    fn send_part(self: &Rc<Self>, part: &mut Part) {
        part.sent = None;
        while part.sends < SENDS_A_KEY {
            let Some((to, address)) = self.route(part.key, &part.avoid, &part.avoid_seeds) else {
                return;
            };
            part.sends += 1;
            let id = self.number();
            let Some(asking) = self
                .plane
                .borrow()
                .request(id, part.key, Some(part.held.rule))
            else {
                return;
            };
            let (answered, answer) = oneshot::channel();
            let stream = self.stream(&address);
            // Registered once sent, on the connection it went on: nothing else runs on the
            // worker in between, so its answer cannot come first.
            if let Some(generation) = stream.send(asking, &Rc::downgrade(self)) {
                stream
                    .waiting
                    .borrow_mut()
                    .insert(id, (generation, Waiter::Asked(answered)));
                let forwarded = matches!(
                    to,
                    To::Member(member) if self.plane.borrow().owner(part.key) != Some(member)
                );
                part.sent = Some(Sent {
                    to,
                    forwarded,
                    stream,
                    id,
                    generation,
                    answer,
                });
                return;
            }
            // It never reached the pod: sent another way, to be forwarded (membership-design).
            self.asked(to, Ended::Broken);
            match to {
                To::Member(member) => part.avoid.push(member),
                To::Seed(at) => part.avoid_seeds.push(at),
            }
        }
    }

    /// Gives back one charged under `key` and `rule`, answered with `receipt`, to the key's
    /// owner as the table names it then: sent again at [`GIVE_BACK_AGAIN`] and [`GIVE_BACK_LATER`]
    /// while unanswered, three sends at most (T6a), on a task of the worker's own.
    fn give_back(self: &Rc<Self>, key: u64, rule: Rule, receipt: u64) {
        tokio::task::spawn_local(give_back(Rc::downgrade(self), key, rule, receipt));
    }

    /// Sends the give-back of `key` once: its answer to come, if it could be sent.
    fn send_give_back(
        self: &Rc<Self>,
        key: u64,
        rule: Rule,
        receipt: u64,
    ) -> Option<oneshot::Receiver<Answered>> {
        let (_, address) = self.route(key, &[], &[])?;
        let id = self.number();
        let asking = self
            .plane
            .borrow()
            .give_back(id, key, Some(rule), receipt)?;
        let stream = self.stream(&address);
        let generation = stream.send(asking, &Rc::downgrade(self))?;
        let (answered, answer) = oneshot::channel();
        stream
            .waiting
            .borrow_mut()
            .insert(id, (generation, Waiter::Asked(answered)));
        Some(answer)
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
}

/// Gives back one charged under `key` and `rule` with `receipt`, for `limiter`'s worker:
/// sent, then sent again while unanswered as [`Giving`] says, until answered or dropped.
async fn give_back(limiter: Weak<SyncLimiter>, key: u64, rule: Rule, receipt: u64) {
    let Some(held) = limiter.upgrade() else {
        return;
    };
    let mut giving = Giving::sent(held.process.now());
    let mut answer = held.send_give_back(key, rule, receipt);
    let mut wait = GIVE_BACK_AGAIN;
    drop(held);
    loop {
        let waited = Duration::from_nanos(wait);
        let answered = match &mut answer {
            Some(answer) => matches!(tokio::time::timeout(waited, answer).await, Ok(Ok(Ok(_)))),
            None => {
                tokio::time::sleep(waited).await;
                false
            }
        };
        if answered {
            return;
        }
        let Some(held) = limiter.upgrade() else {
            return;
        };
        match giving.due(held.process.now()) {
            Due::Again => {
                answer = held.send_give_back(key, rule, receipt);
                wait = GIVE_BACK_LATER;
            }
            Due::Waiting => {}
            Due::Dropped => return,
        }
    }
}

/// One key of a request: its place among the request's keys, the store's key, what it is held
/// to and its bytes, for the process's own keys; the members and seeds passed over for it, its
/// sends, and where it went last, its answer to come.
struct Part {
    position: usize,
    key: u64,
    held: HeldTo,
    bytes: Vec<u8>,
    avoid: Vec<NodeId>,
    avoid_seeds: Vec<usize>,
    sends: u8,
    sent: Option<Sent>,
}

/// Where a key was sent, and its answer to come.
struct Sent {
    to: To,
    /// Whether it went to a member that is not the key's owner, only to be forwarded.
    forwarded: bool,
    stream: Rc<Stream>,
    id: u64,
    generation: u64,
    answer: oneshot::Receiver<Answered>,
}

/// A request asked of its owners, waiting on their answers.
pub(crate) struct Asked {
    limiter: Rc<SyncLimiter>,
    parts: VecDeque<Part>,
    until: tokio::time::Instant,
}

impl Asked {
    /// The decision: refused if any owner refused, the others' charges given back;
    /// admitted if all admitted; otherwise, some owner out of reach, as `when_unreachable`
    /// says.
    pub(crate) async fn decided(mut self) -> Result<Decided, Undecided> {
        let mut admitted: Vec<(u64, Rule, u64)> = Vec::new();
        let mut refused_by = Vec::new();
        let mut unreached = None;
        // The keys out of reach, with their places among the request's.
        let mut out_of_reach: Vec<Part> = Vec::new();
        // The asks given up on: their answers may come yet.
        let mut given_up: Vec<(Sent, u64, Rule)> = Vec::new();
        while let Some(mut part) = self.parts.pop_front() {
            let Some(mut sent) = part.sent.take() else {
                unreached = Some(Undecided::Unavailable);
                out_of_reach.push(part);
                continue;
            };
            match tokio::time::timeout_at(self.until, &mut sent.answer).await {
                Ok(Ok(Ok(answer))) => {
                    self.limiter.asked(sent.to, ended_by(&answer));
                    match answer {
                        Answer::Admitted(receipt) => {
                            admitted.push((part.key, part.held.rule, receipt));
                        }
                        // Asked under a rule, an owner charges or refuses; a plain count is
                        // not asked by data planes, and is taken as charged, nothing to give
                        // back by.
                        Answer::Counted(_) => admitted.push((part.key, part.held.rule, 0)),
                        Answer::Limited { wait } => refused_by.push((part.position, wait)),
                        // Its table taken in as it was told: asked of the owner it names.
                        Answer::Elsewhere(_) => self.again(part, &mut unreached, &mut out_of_reach),
                        Answer::NotServing => {
                            match sent.to {
                                To::Member(member) => part.avoid.push(member),
                                To::Seed(at) => part.avoid_seeds.push(at),
                            }
                            self.again(part, &mut unreached, &mut out_of_reach);
                        }
                    }
                }
                // Its stream never opened: it never reached the pod, and goes another way, to be
                // forwarded (membership-design).
                Ok(Ok(Err(Gone::Unreached))) => {
                    self.limiter.asked(sent.to, Ended::Broken);
                    match sent.to {
                        To::Member(member) => part.avoid.push(member),
                        To::Seed(at) => part.avoid_seeds.push(at),
                    }
                    self.again(part, &mut unreached, &mut out_of_reach);
                }
                // Its stream broke with the ask on it: it may have reached the pod, and is
                // not asked again.
                Ok(Ok(Err(Gone::Broken)) | Err(_)) => {
                    self.limiter.asked(sent.to, Ended::Broken);
                    unreached = Some(Undecided::Unavailable);
                    out_of_reach.push(part);
                }
                // A timeout says nothing of a pod that was only to forward the ask: its owner
                // may be the one gone, failing over (decided). Counted, but not against its
                // breaker; a pod truly hung opens its breaker by its own keys' timeouts. A
                // seed's counts: before a table, nothing says it is not the owner.
                Err(_) => {
                    let how = if sent.forwarded {
                        Ended::ForwardedTimedOut
                    } else {
                        Ended::TimedOut
                    };
                    self.limiter.asked(sent.to, how);
                    unreached = Some(Undecided::Timeout);
                    given_up.push((sent, part.key, part.held.rule));
                    out_of_reach.push(part);
                }
            }
        }
        let decision = self.decide(admitted, refused_by, unreached, out_of_reach);
        // What its owners admit too late is given back if the request was refused; if it
        // went on, the owner's charge counts it. Nothing else runs on the worker between
        // the last of them giving up and this, so an answer already in is found here and
        // any later one by the stream.
        let refused = !matches!(&decision, Ok(decided) if decided.admitted);
        for (mut sent, key, rule) in given_up {
            match sent.answer.try_recv() {
                Ok(Ok(Answer::Admitted(receipt))) => {
                    if refused {
                        self.limiter.give_back(key, rule, receipt);
                    }
                }
                Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {}
                Err(oneshot::error::TryRecvError::Empty) => {
                    let mut waiting = sent.stream.waiting.borrow_mut();
                    if refused {
                        waiting.insert(sent.id, (sent.generation, Waiter::GivenUp(key, rule)));
                    } else {
                        waiting.remove(&sent.id);
                    }
                }
            }
        }
        decision
    }

    /// Sends `part` again, to be waited on with the rest; out of reach if it cannot be.
    fn again(
        &mut self,
        mut part: Part,
        unreached: &mut Option<Undecided>,
        out_of_reach: &mut Vec<Part>,
    ) {
        self.limiter.send_part(&mut part);
        if part.sent.is_some() {
            self.parts.push_back(part);
        } else {
            *unreached = Some(Undecided::Unavailable);
            out_of_reach.push(part);
        }
    }

    /// The decision from what the owners said: `admitted` by those that admitted, each key
    /// with its rule and receipt; `refused_by`, the refusals; and `unreached`, why some did not
    /// answer, those keys `out_of_reach`.
    fn decide(
        &self,
        admitted: Vec<(u64, Rule, u64)>,
        refused_by: Vec<(usize, u64)>,
        unreached: Option<Undecided>,
        out_of_reach: Vec<Part>,
    ) -> Result<Decided, Undecided> {
        let process = &self.limiter.process;
        let give_back = |admitted: Vec<(u64, Rule, u64)>| {
            for (key, rule, receipt) in admitted {
                self.limiter.give_back(key, rule, receipt);
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
                // Each with local mode's limit: a rolling window has none, and its config is
                // refused with `local`.
                let (positions, keys): (Vec<usize>, Vec<(Vec<u8>, Limit)>) = out_of_reach
                    .into_iter()
                    .filter_map(|part| Some((part.position, (part.bytes, part.held.local?))))
                    .unzip();
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
            // `local` with no keys of its own is not made: refused as `deny` is.
            WhenUnreachable::Deny | WhenUnreachable::Local => {
                process.unreachable.local()[1].inc();
                give_back(admitted);
                Err(why)
            }
        }
    }
}

/// How an answer ends an ask, for its member's breaker.
const fn ended_by(answer: &Answer) -> Ended {
    match answer {
        Answer::NotServing => Ended::NotServing,
        _ => Ended::Answered,
    }
}

#[cfg(test)]
mod tests {
    use super::{Breaker, Process};
    use edgerush_config::{CompiledSynchronous, KeySecret, WhenUnreachable};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn config(timeout_ms: u64) -> CompiledSynchronous {
        CompiledSynchronous {
            addresses: vec!["127.0.0.1:1".to_owned()],
            key_secret: KeySecret([1, 2]),
            timeout: Duration::from_millis(timeout_ms),
            when_unreachable: WhenUnreachable::Deny,
            breaker_failures: 1,
            probe_after: Duration::from_secs(60),
            max_key_bytes: 1 << 20,
            tls: None,
        }
    }

    /// A process that carries another's breakers carries the clock their times were read
    /// by, and every member met: a breaker opened before the reload is probed when it is
    /// due, not as late again as the old process had run.
    #[test]
    fn breakers_carried_across_keep_their_clock() {
        let one = NonZeroUsize::MIN;
        let before = Process::start(config(20), one, None, None, None).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let member = before.member_at("rl-0:7000", Some(7));
        member.breaker.failed(before.now(), 1);
        let after = Process::start(config(30), one, None, Some(&before), None).unwrap();
        assert!(
            after.member_at("rl-0:7000", Some(7)).breaker.is_open(),
            "carried"
        );
        assert!(
            after.now() >= 50_000_000,
            "read by the same clock: {}",
            after.now()
        );
    }

    /// A member met is the same handle for every worker, and its metrics are labelled by its
    /// address. A seed's pod, once a table names it, keeps the seed's breaker; another member
    /// at the address, its pod restarted, starts with a breaker of its own.
    #[test]
    fn a_member_is_shared_and_shown_by_its_address() {
        let process = Process::start(config(20), NonZeroUsize::MIN, None, None, None).unwrap();
        let seed = process.member_at("rl-0:7000", None);
        seed.breaker.failed(process.now(), 1);
        let named = process.member_at("rl-0:7000", Some(7));
        assert!(named.breaker.is_open(), "the seed's breaker, its pod named");
        let again = process.member_at("rl-0:7000", Some(7));
        assert!(std::sync::Arc::ptr_eq(&named, &again));
        assert!(std::sync::Arc::ptr_eq(
            &named,
            &process.member_at("rl-0:7000", None)
        ));
        let stats = process.render_stats();
        assert!(
            stats.contains("edgerush_rate_limit_owner_breaker_open{pod=\"rl-0:7000\"} 1\n"),
            "{stats}"
        );
        let restarted = process.member_at("rl-0:7000", Some(8));
        assert!(!restarted.breaker.is_open(), "a breaker of its own");
        let stats = process.render_stats();
        assert!(
            stats.contains("edgerush_rate_limit_owner_breaker_open{pod=\"rl-0:7000\"} 0\n"),
            "{stats}"
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

    /// Each model goes to the store as its own rule, at the limit's numbers, with local
    /// mode's limit beside it for `when_unreachable: local`: none for a rolling window.
    #[test]
    fn a_limit_is_held_at_the_store_by_its_own_model() {
        use super::{HeldTo, Rule};
        let mut config: edgerush_config::Config = serde_saphyr::from_str(
            r#"
listeners: { web: { address: "127.0.0.1:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate } }
routes:
  - name: api
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], forward: { backends: [{ upstream: u, weight: 1 }] }, rate_limits: [b, f, r] }]
upstreams: { u: { load_balancer: p2c, endpoints: { listed: [] } } }
"#,
        )
        .unwrap();
        config.limits = Some(
            serde_saphyr::from_str(
                "limits:
  - { name: b, key: client, model: { bucket: { rate: 10/s, burst: 20 } } }
  - { name: f, key: client, model: { fixed_window: { limit: 3, window: 1m } } }
  - { name: r, key: client, model: { rolling_window: { limit: 7, window: 1h } } }
",
            )
            .unwrap(),
        );
        let compiled = edgerush_config::compile(&config).unwrap();
        let rule = compiled
            .rule(edgerush_config::RuleId { route: 0, rule: 0 })
            .unwrap();
        let held = |name: &str| {
            let limit = rule.limits.iter().find(|limit| limit.name == name).unwrap();
            <HeldTo as crate::ratelimit::Held>::of(limit, limit.count).unwrap()
        };
        assert_eq!(
            held("b").rule,
            Rule::Bucket {
                per: 10,
                period: 1_000_000_000,
                burst: 20
            }
        );
        assert_eq!(
            held("f").rule,
            Rule::Window {
                per: 3,
                window: 60_000_000_000
            }
        );
        assert_eq!(
            held("r").rule,
            Rule::Rolling {
                per: 7,
                window: 3_600_000_000_000
            }
        );
        assert!(held("b").local.is_some() && held("f").local.is_some());
        assert!(
            held("r").local.is_none(),
            "local mode keeps no rolling window"
        );
    }
}
