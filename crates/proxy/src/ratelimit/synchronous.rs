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
//! that never reached its owner — the stream to it down, or the pod marked down — goes through
//! another member, chosen by the key, which forwards it. One unanswered in time leaves the
//! request to `when_unreachable`, and is never asked again: it may have been counted.
//!
//! A worker whose asks time out `timeouts_before_reconnect` times in a row on its connection
//! to a pod, no answer read on it between, asks the pod on a new connection, so that a
//! connection stuck on its own is not kept: the old one is still read until its asks' waits
//! are over and a while past, so that an admission that comes late is given back, and one
//! connection drains at a time. That marks the pod down for no one.
//!
//! A thread of the process's own checks each seed and member its workers have met, on a
//! stream of its own: a check every `check_interval`, which the pod's task answers in its
//! turn, behind what it was asked before, counting nothing. `check_misses` missed in a row
//! mark the pod down, as does its stream broken and not opened again, and one answered marks
//! it up; a pod starts up. Marked down, every worker sends its keys another way at once,
//! letting go of its own stream to it. Only the checks mark a pod down: a worker's timeouts,
//! the pod slow or the worker behind, decide only its own requests.
//!
//! With `when_unreachable: local`, the keys of the owners out of reach are decided from the
//! process's own keys, as local mode decides (`super::local`): each made full when first met
//! without its owner and kept from one outage to the next until the sweep, on the checking
//! thread, forgets it full and idle. Nothing is charged there while the owners answer.

use super::local::{LocalLimiter, Process as LocalKeys};
use super::{Awaited, Failed, KeyedBy, Later, Limited, Refusal, Undecided, Usage, with_keys};
use boring::ssl::SslConnector;
use edgerush_cluster::Key;
use edgerush_config::{CompiledLimit, CompiledModel, CompiledSynchronous, WhenUnreachable};
use edgerush_ratelimit::local::{Decision, Limit};
use edgerush_ratelimit::service::{Decided, longest};
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::plane::{
    Answer, Due, GIVE_BACK_AGAIN, GIVE_BACK_LATER, Giving, NodeId, Plane, Rule, Told,
};
use edgerush_ratelimit_service::cluster::pod::{DecideStream, decide_stream};
use edgerush_ratelimit_service::cluster::wire::{self, Asking, Telling};
use edgerush_telemetry::{Counter, Sharded};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::num::{NonZeroU64, NonZeroUsize};
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

/// How many messages may wait for a stream while it is opened: past them, a key is sent
/// another way, as if its stream were down.
const QUEUED: usize = 1_024;

/// How often the checking thread looks for seeds and members met and not yet checked, and
/// takes the process's own keys' turn with `when_unreachable: local`.
const LOOK: Duration = Duration::from_millis(50);

/// How long a connection given up for a new one is still read past the longest any ask on
/// it may wait: the asks on it have all given up by then, and an admission of theirs that
/// comes this late is not given back.
const DRAINING: Duration = Duration::from_secs(1);

/// How many times one key of a request is sent at most: its owner, then the owner a newer
/// table names, or another member for one not serving or out of reach. Past it, the key is
/// out of reach.
const SENDS_A_KEY: u8 = 4;

/// The process's side of the synchronous mode: the config its workers ask by, each seed and
/// member of the store they have met with what its checks say of it, and what is counted of
/// them.
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
    /// What its streams dial the pods with, if with TLS.
    tls: Option<watch::Receiver<SslConnector>>,
    workers: NonZeroUsize,
}

/// A seed or a member of the store, as a process's workers share it: where it is reached,
/// the member there if a table has named it, what its checks say of it, what was asked of
/// it, and how often its workers gave a connection to it up for a new one.
#[derive(Debug)]
struct Member {
    address: String,
    id: Option<NodeId>,
    health: Arc<Health>,
    asks: Arc<PodAsks>,
    reconnects: Arc<Sharded<Counter>>,
}

/// What a process's workers' asks of one pod came to.
type PodAsks = Sharded<[Counter; 4]>;

/// How an ask of a pod ended.
#[derive(Clone, Copy)]
enum Ended {
    Answered = 0,
    TimedOut = 1,
    Broken = 2,
    /// Answered as not serving: not ready, not failing.
    NotServing = 3,
}

impl Process {
    /// The process's synchronous mode for `config`, with `workers` workers, and the thread
    /// that checks its seeds and members, which calls `failed` if it panics.
    ///
    /// A reload carries an outage's state across from the `running` one: every member met,
    /// marked down or up as its checks last said, whatever else changed, and the process's
    /// own keys while `when_unreachable` stays `local`, within the new cap; so that tuning a
    /// setting mid-outage neither asks a pod down again nor hands out fresh buckets.
    pub(crate) fn start(
        config: CompiledSynchronous,
        workers: NonZeroUsize,
        failed: Option<Failed>,
        running: Option<&Self>,
        tls: Option<SslConnector>,
    ) -> std::io::Result<Arc<Self>> {
        let process = Self::new(config, workers, running, tls);
        let checked = Arc::downgrade(&process);
        std::thread::Builder::new()
            .name("edgerush-rate-limit-check".to_owned())
            .spawn(move || check(&checked, failed))?;
        Ok(process)
    }

    /// The same, without the thread that checks: nothing marks a pod down or up. Its seeds
    /// are met from the start, so that they are checked before any worker asks them.
    fn new(
        config: CompiledSynchronous,
        workers: NonZeroUsize,
        running: Option<&Self>,
        tls: Option<SslConnector>,
    ) -> Arc<Self> {
        let members = running.map_or_else(BTreeMap::new, |running| running.members().clone());
        let local = (config.when_unreachable == WhenUnreachable::Local).then(|| {
            match running.and_then(|running| running.local.as_ref()) {
                Some(local) => {
                    local.set_capacity(config.max_key_bytes);
                    Arc::clone(local)
                }
                None => LocalKeys::unswept(config.max_key_bytes),
            }
        });
        let process = Arc::new(Self {
            members: Mutex::new(members),
            unreachable: running.map_or_else(
                || Arc::new(Sharded::new(workers)),
                |running| Arc::clone(&running.unreachable),
            ),
            local,
            config,
            // Fixed for its life: a reload that changes it starts another.
            tls: tls.map(|tls| watch::channel(tls).1),
            workers,
        });
        for seed in &process.config.addresses {
            process.member_at(seed, None);
        }
        process
    }

    /// Whether it asks as `config` says, so that a reload saying the same keeps it.
    pub(crate) fn is_for(&self, config: &CompiledSynchronous) -> bool {
        self.config == *config
    }

    /// The seeds and members met, a lock poisoned taken as it was: what it guards is a map
    /// whose every change is one insert.
    fn members(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Arc<Member>>> {
        self.members.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whom `address` reaches, as the workers share it — a seed, with no `id`, or member `id`
    /// — met now if not before. A seed's pod named a member keeps what the seed's checks
    /// said; another member at an address, the pod there restarted, starts up, with counts of
    /// its own (decided: one a member).
    fn member_at(&self, address: &str, id: Option<NodeId>) -> Arc<Member> {
        let mut members = self.members();
        let (health, asks, reconnects) = match members.get(address) {
            Some(found) if id.is_none() || found.id == id => return Arc::clone(found),
            Some(found) if found.id.is_none() => (
                Arc::clone(&found.health),
                Arc::clone(&found.asks),
                Arc::clone(&found.reconnects),
            ),
            _ => (
                Arc::new(Health::default()),
                Arc::new(Sharded::new(self.workers)),
                Arc::new(Sharded::new(self.workers)),
            ),
        };
        let member = Arc::new(Member {
            address: address.to_owned(),
            id,
            health,
            asks,
            reconnects,
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

    /// Counts an ask of `member` that ended as `how`: a count, which nothing is decided by.
    fn asked(member: &Member, how: Ended) {
        member.asks.local()[how as usize].inc();
    }

    /// What its own keys hold, in bytes, how many there are, and how many were evicted;
    /// nothing without `when_unreachable: local`.
    pub(crate) fn usage(&self) -> Usage {
        self.local
            .as_ref()
            .map_or_else(Usage::default, |local| local.usage())
    }

    /// What its pods' asks came to, what their checks said, their workers' reconnects, and
    /// what was left to `when_unreachable`, as metrics: each pod labelled by its address,
    /// which a pod keeps for its life, rather than its id, new each life.
    pub(crate) fn render_stats(&self) -> String {
        let members: Vec<Arc<Member>> = self.members().values().cloned().collect();
        let mut out = String::from("# TYPE edgerush_rate_limit_pod_asks_total counter\n");
        let outcomes = ["answered", "timed_out", "broken", "not_serving"];
        for member in &members {
            for (at, outcome) in outcomes.iter().enumerate() {
                let count: u64 = member.asks.sum(|counters| counters[at].get());
                let _ = writeln!(
                    out,
                    "edgerush_rate_limit_pod_asks_total{{pod=\"{}\",outcome=\"{outcome}\"}} {count}",
                    member.address
                );
            }
        }
        out.push_str("# TYPE edgerush_rate_limit_pod_down gauge\n");
        for member in &members {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_pod_down{{pod=\"{}\"}} {}",
                member.address,
                u8::from(member.health.is_down())
            );
        }
        out.push_str("# TYPE edgerush_rate_limit_pod_marked_down_total counter\n");
        for member in &members {
            let _ = writeln!(
                out,
                "edgerush_rate_limit_pod_marked_down_total{{pod=\"{}\"}} {}",
                member.address,
                member.health.marked_down.load(Ordering::Relaxed)
            );
        }
        out.push_str("# TYPE edgerush_rate_limit_pod_checks_total counter\n");
        let outcomes = ["answered", "missed", "refused"];
        for member in &members {
            for (at, outcome) in outcomes.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "edgerush_rate_limit_pod_checks_total{{pod=\"{}\",outcome=\"{outcome}\"}} {}",
                    member.address,
                    member.health.checks[at].load(Ordering::Relaxed)
                );
            }
        }
        out.push_str("# TYPE edgerush_rate_limit_pod_reconnects_total counter\n");
        for member in &members {
            let count: u64 = member.reconnects.sum(Counter::get);
            let _ = writeln!(
                out,
                "edgerush_rate_limit_pod_reconnects_total{{pod=\"{}\"}} {count}",
                member.address
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

/// What a process's checks say of a pod, which its workers read before every ask of it,
/// and only its checks write.
#[derive(Debug, Default)]
struct Health {
    /// Marked down: its keys go another way.
    down: AtomicBool,
    /// How many times it was marked down.
    marked_down: AtomicU64,
    /// Its checks, answered, missed and refused.
    checks: [AtomicU64; 3],
}

impl Health {
    fn is_down(&self) -> bool {
        self.down.load(Ordering::Relaxed)
    }

    /// A check came to `checked`.
    fn count(&self, checked: Checked) {
        self.checks[checked as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Marked down or up, as `down` says; a pod marked down from up counts once.
    fn mark(&self, down: bool) {
        if self.down.swap(down, Ordering::Relaxed) != down && down {
            self.marked_down.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// How a check of a pod came out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Checked {
    /// Its answer came within the check's timeout.
    Answered = 0,
    /// None came within it: the stream is kept, and the answer, if it comes, passed over.
    Missed = 1,
    /// Its stream could not be opened, or broke and could not be opened again: the pod
    /// refuses it, or is gone.
    Refused = 2,
}

/// The checks missed in a row of one pod, as the checking thread counts them.
#[derive(Debug, Default)]
struct Misses(u32);

impl Misses {
    /// What `checked` makes of the pod, `limit` misses in a row marking it down: down, up,
    /// or as it was. A refusal marks it down at once; an answer, up.
    fn after(&mut self, checked: Checked, limit: u32) -> Option<bool> {
        match checked {
            Checked::Answered => {
                self.0 = 0;
                Some(false)
            }
            Checked::Missed => {
                self.0 = self.0.saturating_add(1);
                (self.0 >= limit).then_some(true)
            }
            Checked::Refused => Some(true),
        }
    }
}

/// Checks every seed and member `process`'s workers have met, each on a task and a stream of
/// its own, for as long as the process is kept; tells `failed` if it panics, as the other
/// rate-limiting threads do (03 §2). With `when_unreachable: local`, it takes the process's
/// own keys' turn at every look too ([`LocalKeys::turn`]), sweeping and evicting as local
/// mode's thread does.
fn check(process: &std::sync::Weak<Process>, failed: Option<Failed>) {
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
        let mut look = tokio::time::interval(LOOK);
        look.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut swept = Instant::now();
        let mut checking = HashSet::new();
        loop {
            look.tick().await;
            let Some(held) = process.upgrade() else {
                return;
            };
            if let Some(local) = &held.local {
                swept = local.turn(swept, Instant::now());
            }
            let met: Vec<String> = held.members().keys().cloned().collect();
            drop(held);
            for address in met {
                if checking.insert(address.clone()) {
                    tokio::spawn(check_pod(process.clone(), address));
                }
            }
        }
    });
}

/// Checks the pod at `address` every `check_interval` for as long as `process` is kept,
/// marking the one there down or up as [`Misses`] says. A check waits `check_timeout` at
/// most, opening the stream first if there is none, or again at once if it broke.
async fn check_pod(process: std::sync::Weak<Process>, address: String) {
    let Some(every) = process.upgrade().map(|held| held.config.check_interval) else {
        return;
    };
    let mut every = tokio::time::interval(every);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stream = None;
    let mut misses = Misses::default();
    let mut next = 0_u64;
    // The pod whose misses these are: another at the address, restarted, counts afresh.
    let mut of: Option<Arc<Health>> = None;
    loop {
        every.tick().await;
        let Some((dial, timeout, limit)) = process.upgrade().map(|held| {
            (
                held.dial(),
                held.config.check_timeout,
                held.config.check_misses,
            )
        }) else {
            return;
        };
        next = next.wrapping_add(1);
        let checked = check_once(&mut stream, &address, &dial, timeout, next).await;
        let Some(held) = process.upgrade() else {
            return;
        };
        let Some(member) = held.members().get(&address).cloned() else {
            continue;
        };
        if !of
            .as_ref()
            .is_some_and(|health| Arc::ptr_eq(health, &member.health))
        {
            misses = Misses::default();
            of = Some(Arc::clone(&member.health));
        }
        member.health.count(checked);
        if let Some(down) = misses.after(checked, limit) {
            member.health.mark(down);
        }
    }
}

/// One check, `id`, of the pod at `address` on `stream`, opened first if there is none and
/// again at once if it breaks, all within `timeout`.
async fn check_once(
    stream: &mut Option<DecideStream>,
    address: &str,
    dial: &Dial,
    timeout: Duration,
    id: u64,
) -> Checked {
    let until = tokio::time::Instant::now() + timeout;
    // A stream that broke is opened again once, at once: broken again, the check is refused.
    for _ in 0..2 {
        if stream.is_none() {
            match tokio::time::timeout_at(until, decide_stream(address, dial)).await {
                Ok(Ok(opened)) => *stream = Some(opened),
                Ok(Err(_)) => return Checked::Refused,
                Err(_) => return Checked::Missed,
            }
        }
        let Some(open) = stream.as_mut() else {
            return Checked::Refused;
        };
        match tokio::time::timeout_at(until, answered(open, id)).await {
            Ok(true) => return Checked::Answered,
            Ok(false) => *stream = None,
            Err(_) => return Checked::Missed,
        }
    }
    Checked::Refused
}

/// Sends check `id` up `stream` and reads until its answer: true once it comes, false if the
/// stream breaks first. Tables, and answers to checks missed before, are passed over.
async fn answered(stream: &mut DecideStream, id: u64) -> bool {
    if stream.up.send(wire::asking_check(id)).await.is_err() {
        return false;
    }
    loop {
        match stream.down.message().await {
            Ok(Some(told)) if wire::told_checked(&told) == Some(id) => return true,
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return false,
        }
    }
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
    Up(Connection),
}

/// An open connection to a pod, of its generation: dropped, its way up closes and the task
/// reading its answers stops.
struct Connection {
    generation: u64,
    up: mpsc::Sender<Asking>,
    _reading: oneshot::Sender<()>,
}

/// What waits on a pod's answer, by the number of the ask.
enum Waiter {
    /// An ask: a request's key, or a give-back.
    Asked(oneshot::Sender<Answered>),
    /// A request's key that gave up on it and was refused: an admission that comes late is
    /// given back, under this rule.
    GivenUp(Key, Rule),
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
    /// Asks timed out in a row on the open connection, with no answer read on it between.
    timeouts: Cell<u32>,
    /// A connection given up for a new one, still read for answers that come late, so that
    /// a late admission is given back: one at a time.
    draining: RefCell<Option<Connection>>,
}

impl Stream {
    /// Sends `message` up, opening the stream first if it is not open: the generation of
    /// the connection it went on, or none if it cannot go — the stream broke, or too much
    /// waits for it — so that it never reached the pod.
    fn send(self: &Rc<Self>, message: Asking, limiter: &Weak<SyncLimiter>) -> Option<u64> {
        let mut link = self.link.borrow_mut();
        match &mut *link {
            Link::Up(connection) => match connection.up.try_send(message) {
                Ok(()) => Some(connection.generation),
                Err(mpsc::error::TrySendError::Full(_)) => None,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    let generation = connection.generation;
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

    /// Lets go of the stream, if it is open or opening, and of what waits on it: its pod is
    /// marked down.
    fn shut(&self) {
        let was = std::mem::replace(&mut *self.link.borrow_mut(), Link::Down);
        let generation = match &was {
            Link::Opening(generation, _) => Some(*generation),
            Link::Up(connection) => Some(connection.generation),
            Link::Down => None,
        };
        if let Some(generation) = generation {
            self.broke(generation, Gone::of(&was));
        }
    }

    /// Lets go of the connection of `generation`, which ended, or never opened as `why` says:
    /// the stream is down if it is still the one it has, and what waited on it is told.
    fn ended(&self, generation: u64, why: Gone) {
        let mut link = self.link.borrow_mut();
        let current = match &*link {
            Link::Opening(current, _) => Some(*current),
            Link::Up(connection) => Some(connection.generation),
            Link::Down => None,
        };
        if current == Some(generation) {
            *link = Link::Down;
        }
        drop(link);
        if self.is_draining(generation) {
            *self.draining.borrow_mut() = None;
        }
        self.broke(generation, why);
    }

    /// The generation of the connection open now, if one is.
    fn open_now(&self) -> Option<u64> {
        match &*self.link.borrow() {
            Link::Up(connection) => Some(connection.generation),
            Link::Opening(..) | Link::Down => None,
        }
    }

    /// An answer read on the connection of `generation`: if it is the one open, its
    /// timeouts in a row start again.
    fn answered(&self, generation: u64) {
        if self.open_now() == Some(generation) {
            self.timeouts.set(0);
        }
    }

    /// An ask on the connection of `generation` timed out: whether that gave it up. The
    /// `after`th in a row on the one open gives it up for a new one, which the next ask opens, unless another is still
    /// draining: the one given up is read on for `draining` more, for answers that come
    /// late, then let go of. The pod is marked down for no one: a worker too far behind to
    /// read its answers in time reconnects too, its asks answered as before. A timeout on a
    /// connection still opening counts for nothing, as one is new already (one whose pod never
    /// answers is dropped by its keepalive), nor does one on a connection no longer open.
    fn timed_out(self: &Rc<Self>, generation: u64, after: u32, draining: Duration) -> bool {
        if self.open_now() != Some(generation) {
            return false;
        }
        let timeouts = self.timeouts.get().saturating_add(1);
        self.timeouts.set(timeouts);
        if timeouts < after || self.draining.borrow().is_some() {
            return false;
        }
        let Link::Up(connection) = std::mem::replace(&mut *self.link.borrow_mut(), Link::Down)
        else {
            return false;
        };
        *self.draining.borrow_mut() = Some(connection);
        self.timeouts.set(0);
        let stream = Rc::downgrade(self);
        tokio::task::spawn_local(async move {
            tokio::time::sleep(draining).await;
            if let Some(stream) = stream.upgrade() {
                stream.drained(generation);
            }
        });
        true
    }

    /// Whether the connection of `generation` is the one draining.
    fn is_draining(&self, generation: u64) -> bool {
        self.draining
            .borrow()
            .as_ref()
            .is_some_and(|connection| connection.generation == generation)
    }

    /// Lets go of the connection of `generation` if it is the one draining: what still waits
    /// on it hears that it broke, and late answers to asks given up on are no longer read.
    fn drained(&self, generation: u64) {
        if !self.is_draining(generation) {
            return;
        }
        *self.draining.borrow_mut() = None;
        self.broke(generation, Gone::Broken);
    }

    /// Whether nothing waits on the connection of `generation`.
    fn idle(&self, generation: u64) -> bool {
        !self
            .waiting
            .borrow()
            .values()
            .any(|(on, _)| *on == generation)
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
    let (mut down, mut let_go) = match opened {
        Ok(opened) => {
            let mut link = held.link.borrow_mut();
            let Link::Opening(current, _) = &*link else {
                return;
            };
            if *current != generation {
                return;
            }
            let (reading, let_go) = oneshot::channel();
            let connection = Connection {
                generation,
                up: opened.up.clone(),
                _reading: reading,
            };
            let Link::Opening(_, queued) = std::mem::replace(&mut *link, Link::Up(connection))
            else {
                return;
            };
            drop(link);
            for message in queued {
                if opened.up.try_send(message).is_err() {
                    break;
                }
            }
            (opened.down, let_go)
        }
        Err(_) => {
            held.ended(generation, Gone::Unreached);
            return;
        }
    };
    drop(held);
    loop {
        let message = tokio::select! {
            message = down.message() => message,
            // Let go of: what waited on it was told, and nothing more is read.
            _ = &mut let_go => return,
        };
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
        held.answered(generation);
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
        // Draining, and its last answer read: let go of at once. Only a connection draining
        // looks through what waits, never the one open.
        if held.is_draining(generation) && held.idle(generation) {
            held.drained(generation);
        }
    }
}

/// What a key is held to in the synchronous mode: the store's rule, which its owner decides
/// by, and the same limit as local mode keeps it, for `when_unreachable: local`.
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
            CompiledModel::RollingWindow { slots, .. } => Rule::Rolling {
                per,
                window,
                slots: slots.get(),
            },
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
                        timeouts: Cell::new(0),
                        draining: RefCell::new(None),
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

    /// Whether member `id` is marked down: its keys go another way.
    fn shut_out(&self, id: NodeId) -> bool {
        self.member(id)
            .is_some_and(|member| self.passed_over(&member))
    }

    /// Whether `member` is marked down: its keys go another way, and the worker lets go of
    /// its stream to it, which may be hung, so that the next it opens there is afresh.
    fn passed_over(&self, member: &Member) -> bool {
        let down = member.health.is_down();
        if down && let Some(stream) = self.streams.borrow().get(&member.address) {
            stream.shut();
        }
        down
    }

    /// Whom to send `key` to and where it is reached: its owner unless `avoid` names it or it
    /// is marked down, else another member chosen by the key; with no table yet, a seed chosen
    /// by the key among those not in `avoid_seeds` and not marked down. None when none is
    /// left.
    fn route(&self, key: Key, avoid: &[NodeId], avoid_seeds: &[usize]) -> Option<(To, String)> {
        let plane = self.plane.borrow();
        if plane.table().is_none() {
            let open = || {
                self.seeds
                    .iter()
                    .enumerate()
                    .filter(|(at, seed)| !avoid_seeds.contains(at) && !self.passed_over(seed))
            };
            let count = Key::try_from(open().count())
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
            Process::asked(&member, how);
        }
    }

    /// Counts a connection to `to` given up for a new one.
    fn reconnected(&self, to: To) {
        let member = match to {
            To::Member(id) => self.member(id),
            To::Seed(at) => self.seeds.get(at).cloned(),
        };
        if let Some(member) = member {
            member.reconnects.local().inc();
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
                part.sent = Some(Sent {
                    to,
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
    fn give_back(self: &Rc<Self>, key: Key, rule: Rule, receipt: u64) {
        tokio::task::spawn_local(give_back(Rc::downgrade(self), key, rule, receipt));
    }

    /// Sends the give-back of `key` once: its answer to come, if it could be sent.
    fn send_give_back(
        self: &Rc<Self>,
        key: Key,
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
async fn give_back(limiter: Weak<SyncLimiter>, key: Key, rule: Rule, receipt: u64) {
    giving_back(|| {
        let held = limiter.upgrade()?;
        Some(held.send_give_back(key, rule, receipt))
    })
    .await;
}

/// Sends a give-back with `send`, then sends it again while unanswered as [`Giving`] says,
/// until answered, dropped, or `send` finds its worker gone (`None`). `send` gives the answer
/// to come, if the give-back could be sent. An answer that comes as a failure (its stream
/// broken or never opened) counts as none: the give-back is sent again when it is due, not
/// at once, so that a failing stream does not use up its sends.
async fn giving_back(mut send: impl FnMut() -> Option<Option<oneshot::Receiver<Answered>>>) {
    let start = tokio::time::Instant::now();
    // Read by the clock the waits run on, so that a wait over finds the give-back due.
    let since = |at: tokio::time::Instant| {
        u64::try_from(at.duration_since(start).as_nanos()).unwrap_or(u64::MAX)
    };
    let mut giving = Giving::sent(since(start));
    let Some(mut answer) = send() else {
        return;
    };
    let mut wait = Duration::from_nanos(GIVE_BACK_AGAIN);
    let mut until = start + wait;
    loop {
        match &mut answer {
            Some(waiting) => match tokio::time::timeout_at(until, waiting).await {
                Ok(Ok(Ok(_))) => return,
                // An answer come is never awaited again (tokio panics): the rest of the wait
                // goes by without it.
                Ok(_) => {
                    answer = None;
                    continue;
                }
                Err(_) => {}
            },
            None => tokio::time::sleep_until(until).await,
        }
        let now = tokio::time::Instant::now();
        match giving.due(since(now)) {
            Due::Again => {
                let Some(sent) = send() else {
                    return;
                };
                answer = sent;
                wait = Duration::from_nanos(GIVE_BACK_LATER);
            }
            Due::Waiting => {}
            Due::Dropped => return,
        }
        until = now + wait;
    }
}

/// One key of a request: its place among the request's keys, the store's key, what it is held
/// to and its bytes, for the process's own keys; the members and seeds passed over for it, its
/// sends, and where it went last, its answer to come.
struct Part {
    position: usize,
    key: Key,
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
        let mut admitted: Vec<(Key, Rule, u64)> = Vec::new();
        let mut refused_by = Vec::new();
        let mut unreached = None;
        // The keys out of reach, with their places among the request's.
        let mut out_of_reach: Vec<Part> = Vec::new();
        // The asks given up on: their answers may come yet.
        let mut given_up: Vec<(Sent, Key, Rule)> = Vec::new();
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
                // Counted, and decided by `when_unreachable` alone: only the checks mark a pod
                // down, as a timeout may be the worker's own, too far behind to read the answer.
                Err(_) => {
                    self.limiter.asked(sent.to, Ended::TimedOut);
                    let config = &self.limiter.process.config;
                    if sent.stream.timed_out(
                        sent.generation,
                        config.timeouts_before_reconnect,
                        config.timeout.saturating_add(DRAINING),
                    ) {
                        self.limiter.reconnected(sent.to);
                    }
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
        admitted: Vec<(Key, Rule, u64)>,
        refused_by: Vec<(usize, u64)>,
        unreached: Option<Undecided>,
        out_of_reach: Vec<Part>,
    ) -> Result<Decided, Undecided> {
        let process = &self.limiter.process;
        let give_back = |admitted: Vec<(Key, Rule, u64)>| {
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

/// How an answer ends an ask, as its pod's asks are counted.
const fn ended_by(answer: &Answer) -> Ended {
    match answer {
        Answer::NotServing => Ended::NotServing,
        _ => Ended::Answered,
    }
}

#[cfg(test)]
mod tests {
    use super::{Answered, Checked, Gone, Health, Misses, Process, giving_back};
    use edgerush_config::{CompiledSynchronous, KeySecret, WhenUnreachable};
    use edgerush_ratelimit_service::cluster::plane::{
        Answer, GIVE_BACK_AGAIN, GIVE_BACK_LATER, GIVE_BACK_SENDS,
    };
    use std::num::NonZeroUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn config(timeout_ms: u64) -> CompiledSynchronous {
        CompiledSynchronous {
            addresses: vec!["127.0.0.1:1".to_owned()],
            key_secret: KeySecret([1, 2]),
            timeout: Duration::from_millis(timeout_ms),
            when_unreachable: WhenUnreachable::Deny,
            timeouts_before_reconnect: 2,
            check_interval: Duration::from_millis(100),
            check_timeout: Duration::from_millis(100),
            check_misses: 3,
            max_key_bytes: 1 << 20,
            tls: None,
        }
    }

    /// A reload carries every member met across, marked down or up as its checks last said.
    #[test]
    fn a_reload_carries_what_the_checks_said() {
        let one = NonZeroUsize::MIN;
        let before = Process::new(config(20), one, None, None);
        before.member_at("rl-0:7000", Some(7)).health.mark(true);
        before.member_at("rl-1:7000", Some(8));
        let after = Process::new(config(30), one, Some(&before), None);
        assert!(after.member_at("rl-0:7000", Some(7)).health.is_down());
        assert!(!after.member_at("rl-1:7000", Some(8)).health.is_down());
    }

    /// A member met is the same handle for every worker, and its metrics are labelled by its
    /// address. A seed's pod, once a table names it, keeps what the seed's checks said;
    /// another member at the address, its pod restarted, starts up.
    #[test]
    fn a_member_is_shared_and_shown_by_its_address() {
        let process = Process::new(config(20), NonZeroUsize::MIN, None, None);
        let seed = process.member_at("rl-0:7000", None);
        seed.health.mark(true);
        let named = process.member_at("rl-0:7000", Some(7));
        assert!(named.health.is_down(), "the seed's, its pod named");
        let again = process.member_at("rl-0:7000", Some(7));
        assert!(std::sync::Arc::ptr_eq(&named, &again));
        assert!(std::sync::Arc::ptr_eq(
            &named,
            &process.member_at("rl-0:7000", None)
        ));
        let stats = process.render_stats();
        assert!(
            stats.contains(
                "edgerush_rate_limit_pod_down{pod=\"rl-0:7000\"} 1
"
            ),
            "{stats}"
        );
        let restarted = process.member_at("rl-0:7000", Some(8));
        assert!(!restarted.health.is_down(), "starts up");
        let stats = process.render_stats();
        assert!(
            stats.contains(
                "edgerush_rate_limit_pod_down{pod=\"rl-0:7000\"} 0
"
            ),
            "{stats}"
        );
    }

    /// Misses in a row mark a pod down, an answer between them starting the count again; a
    /// refusal marks it down at once; one answer marks it up.
    #[test]
    fn misses_in_a_row_mark_a_pod_down_and_an_answer_up() {
        let mut misses = Misses::default();
        assert_eq!(misses.after(Checked::Missed, 3), None);
        assert_eq!(misses.after(Checked::Missed, 3), None);
        assert_eq!(misses.after(Checked::Answered, 3), Some(false));
        assert_eq!(misses.after(Checked::Missed, 3), None);
        assert_eq!(misses.after(Checked::Missed, 3), None);
        assert_eq!(
            misses.after(Checked::Missed, 3),
            Some(true),
            "three in a row"
        );
        assert_eq!(misses.after(Checked::Missed, 3), Some(true), "still");
        assert_eq!(
            misses.after(Checked::Answered, 3),
            Some(false),
            "one answer"
        );
        assert_eq!(misses.after(Checked::Refused, 3), Some(true), "at once");
        assert_eq!(misses.after(Checked::Missed, 1), Some(true));
    }

    /// A pod marked down counts once each time it goes down from up, and every check counts
    /// by how it came out.
    #[test]
    fn a_pod_marked_down_counts_once_a_time() {
        let health = Health::default();
        health.mark(false);
        health.mark(true);
        health.mark(true);
        assert!(health.is_down());
        health.mark(false);
        assert!(!health.is_down());
        health.mark(true);
        assert_eq!(health.marked_down.load(Ordering::Relaxed), 2);
        for checked in [
            Checked::Answered,
            Checked::Missed,
            Checked::Missed,
            Checked::Refused,
        ] {
            health.count(checked);
        }
        let checks: Vec<u64> = health
            .checks
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect();
        assert_eq!(checks, [1, 2, 1]);
    }

    /// A stream to a pod, open on a connection of generation `generation`, with what that
    /// connection's reader would hear when it is let go of.
    fn stream_up(generation: u64) -> (std::rc::Rc<super::Stream>, oneshot::Receiver<()>) {
        use std::cell::{Cell, RefCell};
        let stream = std::rc::Rc::new(super::Stream {
            address: "rl-0:7000".to_owned(),
            dial: edgerush_ratelimit_service::cluster::link::Dial::Plain,
            link: RefCell::new(super::Link::Down),
            next: Cell::new(generation.wrapping_add(1)),
            waiting: RefCell::new(std::collections::HashMap::new()),
            timeouts: Cell::new(0),
            draining: RefCell::new(None),
        });
        let let_go = reopen(&stream, generation);
        (stream, let_go)
    }

    /// Opens connection `generation` on `stream`, as the next ask would.
    fn reopen(stream: &super::Stream, generation: u64) -> oneshot::Receiver<()> {
        let (up, _) = tokio::sync::mpsc::channel(1);
        let (reading, let_go) = oneshot::channel();
        *stream.link.borrow_mut() = super::Link::Up(super::Connection {
            generation,
            up,
            _reading: reading,
        });
        let_go
    }

    /// The second timeout in a row on the connection open gives it up: the next ask opens
    /// another, and the one given up is read until its bound, then let go of, what still
    /// waited on it told it broke. While it drains, timeouts on the next give up nothing; a
    /// timeout on a connection no longer open counts for nothing, and an answer starts the
    /// count again.
    #[tokio::test(start_paused = true)]
    async fn a_connection_given_up_is_drained_until_its_bound_one_at_a_time() {
        use super::{Link, Waiter};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let bound = Duration::from_secs(1);
                let (stream, mut first) = stream_up(0);
                assert!(!stream.timed_out(0, 2, bound));
                stream.answered(0);
                assert!(!stream.timed_out(0, 2, bound));
                assert_eq!(stream.open_now(), Some(0), "an answer between");
                let (asked, mut answer) = oneshot::channel();
                stream
                    .waiting
                    .borrow_mut()
                    .insert(1, (0, Waiter::Asked(asked)));
                assert!(stream.timed_out(0, 2, bound));
                assert!(matches!(*stream.link.borrow(), Link::Down), "given up");
                assert!(stream.is_draining(0));
                let empty = Err(oneshot::error::TryRecvError::Empty);
                assert_eq!(first.try_recv(), empty, "still read");
                assert!(matches!(
                    answer.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                let mut second = reopen(&stream, 1);
                assert!(!stream.timed_out(0, 2, bound));
                assert!(!stream.timed_out(1, 2, bound));
                assert!(!stream.timed_out(1, 2, bound));
                assert!(!stream.timed_out(1, 2, bound));
                assert_eq!(stream.open_now(), Some(1), "one draining at a time");
                tokio::time::sleep(bound - Duration::from_millis(1)).await;
                assert!(stream.is_draining(0), "until its bound");
                tokio::time::sleep(Duration::from_millis(2)).await;
                assert!(!stream.is_draining(0), "let go of at its bound");
                assert!(matches!(
                    first.try_recv(),
                    Err(oneshot::error::TryRecvError::Closed)
                ));
                assert!(matches!(answer.try_recv(), Ok(Err(Gone::Broken))));
                assert!(stream.waiting.borrow().is_empty());
                assert!(stream.timed_out(1, 2, bound));
                assert!(stream.is_draining(1), "none draining: given up now");
                assert_eq!(second.try_recv(), empty, "and read");
            })
            .await;
    }

    /// Each model goes to the store as its own rule, at the limit's numbers, with local
    /// mode's limit beside it for `when_unreachable: local`, a rolling window's in the same
    /// slots.
    #[test]
    fn a_limit_is_held_at_the_store_by_its_own_model() {
        use super::{HeldTo, Rule};
        use edgerush_ratelimit::local::{Limit, Rolling};
        use std::num::NonZeroU64;
        let mut config: edgerush_config::Config = serde_saphyr::from_str(
            r#"
listeners: { web: { address: "127.0.0.1:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate } }
routes:
  - name: api
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], forward: { backends: [{ upstream: u, weight: 1 }] }, rate_limits: [b, f, r, s] }]
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
  - { name: s, key: client, model: { rolling_window: { limit: 7, window: 1h, slots: 60 } } }
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
                window: 3_600_000_000_000,
                slots: 1_000
            }
        );
        assert_eq!(
            held("s").rule,
            Rule::Rolling {
                per: 7,
                window: 3_600_000_000_000,
                slots: 60
            }
        );
        assert!(held("b").local.is_some() && held("f").local.is_some());
        // The fallback in the same slots as the store, so that both decide alike.
        let rolling = |slots| {
            Some(Limit::RollingWindow(Rolling {
                limit: NonZeroU64::new(7).unwrap(),
                window: NonZeroU64::new(3_600_000_000_000).unwrap(),
                slots: NonZeroU64::new(slots).unwrap(),
            }))
        };
        assert_eq!(held("r").local, rolling(1_000));
        assert_eq!(held("s").local, rolling(60));
    }

    /// A give-back's answer that comes 10 ms after it is sent, other than answered: its
    /// stream broken with the give-back on it, never opened, or the stream's end of the answer
    /// dropped. Coming partway through the wait, not at once, it tells a wait carried on from a
    /// wait begun again.
    fn failing(how: usize) -> oneshot::Receiver<Answered> {
        let (answered, answer) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            match how {
                0 => assert!(answered.send(Err(Gone::Broken)).is_ok()),
                1 => assert!(answered.send(Err(Gone::Unreached)).is_ok()),
                _ => drop(answered),
            }
        });
        answer
    }

    /// A give-back whose answer comes as a failure before it is due again is not waited on
    /// again, which panics its task (#248), and is sent again when it is due, not before.
    #[tokio::test(start_paused = true)]
    async fn a_give_back_whose_stream_breaks_is_sent_again_when_due() {
        for how in 0..3 {
            let start = tokio::time::Instant::now();
            let mut sends = Vec::new();
            giving_back(|| {
                sends.push(start.elapsed());
                if sends.len() == 1 {
                    return Some(Some(failing(how)));
                }
                let (answered, answer) = oneshot::channel();
                answered.send(Ok(Answer::Counted(0))).unwrap();
                Some(Some(answer))
            })
            .await;
            assert_eq!(
                sends,
                [Duration::ZERO, Duration::from_nanos(GIVE_BACK_AGAIN)],
                "failure {how}"
            );
        }
    }

    /// A give-back whose every answer fails at once is still sent only when due, and dropped
    /// after its last send has waited as long as one unanswered would.
    #[tokio::test(start_paused = true)]
    async fn a_give_back_whose_every_stream_breaks_is_dropped_when_due() {
        let start = tokio::time::Instant::now();
        let mut sends = Vec::new();
        giving_back(|| {
            sends.push(start.elapsed());
            Some(Some(failing(0)))
        })
        .await;
        let again = Duration::from_nanos(GIVE_BACK_AGAIN);
        let later = Duration::from_nanos(GIVE_BACK_LATER);
        assert_eq!(sends, [Duration::ZERO, again, again + later]);
        assert_eq!(GIVE_BACK_SENDS, 3);
        assert_eq!(start.elapsed(), again + later * 2, "dropped when due");
    }
}
