//! The process layer of a layered limiter (12 item 3): a thread of its own, as Lucas chose,
//! holding the pod's one data plane of the rate-limiting service, the clients of its pods,
//! and the slices of its room it gives the workers. Its inputs are the workers' messages,
//! the service's answers and its cycle; nothing of it is shared with a worker but a channel
//! and the clock they read its times by.

use super::ASK_TIMEOUT;
use edgerush_config::CompiledRateLimitService;
use edgerush_ratelimit::service::layer::{Answer, Process, Slice, Used};
use edgerush_ratelimit::service::{Contract, DataPlane, Keyed, View};
use edgerush_ratelimit_service::grpc::Client;
use std::cell::RefCell;
use std::hash::{BuildHasher, RandomState};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

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
}

/// What the process sends a worker.
pub(crate) enum FromHub {
    /// New slices, for its sync.
    Slices(Vec<Slice>),
    /// The answer to a request it asked about.
    Answer { id: u64, answer: Answer },
}

/// What the process thread and the workers share: the clock its times are read by, and
/// what its keys hold.
struct Shared {
    epoch: Instant,
    /// Bytes, keys, and evictions all told, as of its last cycle.
    usage: [AtomicU64; 3],
}

/// A handle on a running process layer. It stops once every handle and every worker's
/// sender is gone.
pub(crate) struct Hub {
    pub(crate) to: mpsc::UnboundedSender<ToHub>,
    shared: Arc<Shared>,
    service: CompiledRateLimitService,
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
        });
        let (to, from) = mpsc::unbounded_channel();
        let running = Arc::clone(&shared);
        let cycle = service.cycle;
        std::thread::Builder::new()
            .name("edgerush-rate-limits".to_owned())
            .spawn(move || run(clients, cycle, workers, max_key_bytes, from, &running))
            .ok()?;
        Some(Arc::new(Self {
            to,
            shared,
            service: service.clone(),
        }))
    }

    /// Whether it reaches `service` as it is set now: the same pods, the same cycle.
    pub(crate) fn serves(&self, service: &CompiledRateLimitService) -> bool {
        self.service.pods == service.pods && self.service.cycle == service.cycle
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

/// The process thread: a runtime of its own, the process and its clients, until every
/// sender is gone.
fn run(
    clients: Vec<Client>,
    cycle: Duration,
    workers: usize,
    max_key_bytes: u64,
    from: mpsc::UnboundedReceiver<ToHub>,
    shared: &Arc<Shared>,
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
    let nanos = |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    // Keys are what clients send: hashed under a key of the process's own, drawn at random.
    let entropy = RandomState::new();
    let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
    let mut plane = DataPlane::with_hasher(id, nanos(cycle), view, hasher);
    plane.set_ask_timeout(ASK_TIMEOUT);
    plane.set_capacity(max_key_bytes);
    let state = Rc::new(State {
        process: RefCell::new(Process::new(plane, workers, nanos(SYNC), hasher)),
        clients,
        answers: RefCell::new(vec![None; workers]),
        shared: Arc::clone(shared),
    });
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, serve(state, from, cycle));
}

/// The process thread's state.
struct State {
    process: RefCell<Process<Keyed>>,
    clients: Vec<Client>,
    /// By worker: where its answers go, once it has joined.
    answers: RefCell<Vec<Option<mpsc::UnboundedSender<FromHub>>>>,
    shared: Arc<Shared>,
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
                let slices = self.process.borrow_mut().on_used(worker, used, now);
                self.tell(worker, FromHub::Slices(slices));
                // An ask waiting for this sync may now be decided.
                self.answer();
            }
            ToHub::Ask { worker, id, keys } => {
                let keys: Vec<(&[u8], Contract)> = keys
                    .iter()
                    .map(|(key, contract)| (&**key, *contract))
                    .collect();
                let answer = self.process.borrow_mut().on_ask(worker, id, &keys, now);
                if let Some(answer) = answer {
                    self.tell(worker, FromHub::Answer { id, answer });
                }
            }
            ToHub::Capacity(bytes) => self.process.borrow_mut().plane().set_capacity(bytes),
        }
    }

    /// Hands the requests the service decided their answers.
    fn answer(&self) {
        let now = self.now();
        let answers = self.process.borrow_mut().take_answers(now);
        for (worker, id, answer) in answers {
            self.tell(worker, FromHub::Answer { id, answer });
        }
    }

    /// Says what its keys hold, for the workers' scrape.
    fn report(&self) {
        let mut process = self.process.borrow_mut();
        let plane = process.plane();
        let [bytes, keys, evicted] = &self.shared.usage;
        bytes.store(plane.holding(), Ordering::Relaxed);
        keys.store(plane.key_count() as u64, Ordering::Relaxed);
        evicted.store(plane.evicted(), Ordering::Relaxed);
    }

    /// Sends what the data plane has to send, each batch on a task of its own, and hands it
    /// the answers as they come.
    fn send(self: &Rc<Self>) {
        let outgoing = self.process.borrow_mut().plane().take_outgoing();
        for (pod, batch) in outgoing {
            let Some(client) = self.clients.get(pod as usize).cloned() else {
                continue;
            };
            let me = Rc::clone(self);
            tokio::task::spawn_local(async move {
                // Unanswered, it is sent again or given up as the protocol says.
                if let Ok(answers) = client.exchange(batch).await {
                    let now = me.now();
                    me.process.borrow_mut().plane().on_answers(answers, now);
                    me.answer();
                    me.send();
                }
            });
        }
    }
}

/// The process's loop: the workers' messages as they come, and the cycle.
async fn serve(state: Rc<State>, mut from: mpsc::UnboundedReceiver<ToHub>, cycle: Duration) {
    let mut tick = tokio::time::interval(cycle);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            message = from.recv() => match message {
                Some(message) => state.on(message),
                None => return,
            },
            _ = tick.tick() => {
                let now = state.now();
                state.process.borrow_mut().plane().on_cycle(now);
                state.answer();
                state.report();
            }
        }
        state.send();
    }
}
