//! Local mode's rate limits (06 §4): the process's keys, shared by its workers, with no
//! rate-limiting service and no thread on the request path. Each worker decides a request
//! from its own handles to the keys, one compare-and-swap a key
//! (`edgerush_ratelimit::local`); a thread of the process's sweeps the keys once a second,
//! forgetting those full and quiet and, past the cap, evicting.

use super::{Failed, KeyedBy, Limited, Refusal, with_keys};
use edgerush_config::CompiledLimit;
use edgerush_ratelimit::local::{Decision, Handles, Local};
use edgerush_ratelimit::service::{Contract, Keyed, longest};
use std::cell::RefCell;
use std::hash::{BuildHasher, RandomState};
use std::rc::{Rc, Weak as RcWeak};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// How often the keys are swept, and how long a full key goes unused before it is
/// forgotten: a second, so that a key in steady use is not made again and again.
const SWEEP: Duration = Duration::from_secs(1);

/// The process's keys and the clock every worker decides them by.
#[derive(Debug)]
pub(crate) struct Process {
    local: Local<Keyed>,
    hasher: Keyed,
    start: Instant,
}

impl Process {
    /// The keys of a process of `workers` workers, within `max_key_bytes`, and the thread
    /// that sweeps them, which calls `failed` if it panics.
    pub(crate) fn start(
        workers: usize,
        max_key_bytes: u64,
        failed: Option<Failed>,
    ) -> std::io::Result<Arc<Self>> {
        let entropy = RandomState::new();
        let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
        let local = Local::new(hasher, u64::try_from(workers).unwrap_or(u64::MAX));
        local.set_capacity(max_key_bytes);
        let process = Arc::new(Self {
            local,
            hasher,
            start: Instant::now(),
        });
        let swept = Arc::downgrade(&process);
        std::thread::Builder::new()
            .name("edgerush-rate-limit-sweep".to_owned())
            .spawn(move || sweep(&swept, failed))?;
        Ok(process)
    }

    /// The bytes its keys may hold, from now on: a reload that changes it keeps the keys.
    pub(crate) fn set_capacity(&self, max_key_bytes: u64) {
        self.local.set_capacity(max_key_bytes);
    }

    /// What its keys hold, in bytes, how many there are, and how many were evicted.
    pub(crate) fn usage(&self) -> (u64, usize, u64) {
        (
            self.local.holding(),
            self.local.key_count(),
            self.local.evicted(),
        )
    }

    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Sweeps the keys every [`SWEEP`] for as long as the process keeps them; tells `failed` if
/// it panics, as the asynchronous mode's thread does (03 §2).
fn sweep(process: &Weak<Process>, failed: Option<Failed>) {
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
    loop {
        std::thread::sleep(SWEEP);
        let Some(process) = process.upgrade() else {
            return;
        };
        let quiet = u64::try_from(SWEEP.as_nanos()).unwrap_or(u64::MAX);
        process.local.sweep(process.now(), quiet);
    }
}

/// A worker's limiter in local mode: its handles to the process's keys, and its buffers for
/// a request's keys.
#[derive(Debug)]
pub(crate) struct LocalLimiter {
    process: Arc<Process>,
    index: usize,
    handles: RefCell<Handles<Keyed>>,
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<Contract>>,
}

impl LocalLimiter {
    /// Worker `index`'s limiter on `process`'s keys, letting go of its handles to keys
    /// forgotten once a second, for as long as the worker keeps it.
    pub(crate) fn new(process: Arc<Process>, index: usize) -> Rc<Self> {
        let handles = RefCell::new(Handles::new(process.hasher));
        let limiter = Rc::new(Self {
            process,
            index,
            handles,
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
        });
        let held: RcWeak<Self> = Rc::downgrade(&limiter);
        tokio::task::spawn_local(async move {
            let mut every = tokio::time::interval(SWEEP);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                every.tick().await;
                let Some(limiter) = held.upgrade() else {
                    return;
                };
                limiter.handles.borrow_mut().prune();
            }
        });
        limiter
    }

    /// Whether it decides from `process`'s keys.
    pub(crate) fn is_on(&self, process: &Arc<Process>) -> bool {
        Arc::ptr_eq(&self.process, process)
    }

    /// What its process's keys hold, said by its first worker alone, so that the workers'
    /// sum is the process's.
    pub(crate) fn usage(&self) -> (u64, usize, u64) {
        if self.index == 0 {
            self.process.usage()
        } else {
            (0, 0, 0)
        }
    }

    /// Decides a request from `client` under those of `limits` that `limited` marks, all at
    /// once: admitted under all, or refused and charged under none. Never later: nothing is
    /// waited for.
    pub(crate) fn decide_all<'f>(
        &self,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: impl Into<KeyedBy<'f>>,
    ) -> Limited {
        let now = self.process.now();
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
                match self
                    .process
                    .local
                    .decide(&mut self.handles.borrow_mut(), keys, now)
                {
                    Decision::Admit => Limited::Admit,
                    Decision::Refuse { .. } => Limited::Refuse,
                }
            },
        )
    }

    /// Which of `limits` refused a request from `client` under those `limited` marks, and
    /// how long it should wait to be admitted under all of them, as the keys have it now.
    pub(crate) fn refusal<'a, 'f>(
        &self,
        limits: &'a [Arc<CompiledLimit>],
        limited: u64,
        client: impl Into<KeyedBy<'f>>,
    ) -> Refusal<'a> {
        let now = self.process.now();
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
                let refused =
                    self.process
                        .local
                        .refused_by(&mut self.handles.borrow_mut(), keys, now);
                let by = refused
                    .iter()
                    .filter_map(|(part, _)| at(*part))
                    .filter_map(|at| 1_u64.checked_shl(u32::try_from(at).ok()?))
                    .fold(0, |by, bit| by | bit);
                let longest = longest(refused.into_iter()).and_then(|(part, wait)| {
                    let limit = limits.get(at(part)?)?;
                    Some((&**limit, Duration::from_nanos(wait)))
                });
                Refusal { by, longest }
            },
        )
    }
}
