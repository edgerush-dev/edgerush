//! Local mode's rate limits (06 §4): the process's keys, shared by its workers, with no
//! rate-limiting service and no thread on the request path. Each worker decides a request
//! from its own handles to the keys, one compare-and-swap a key
//! (`edgerush_ratelimit::local`); a thread of the process's sweeps the keys once a second,
//! forgetting those full and quiet and, past the cap, evicting.

use super::{Failed, KeyedBy, Limited, Refused, with_keys};
use edgerush_config::CompiledLimit;
use edgerush_ratelimit::local::{Decision, HANDLE_PARTS, Handles, Local};
use edgerush_ratelimit::service::{Contract, Keyed};
use std::cell::RefCell;
use std::hash::{BuildHasher, RandomState};
use std::rc::{Rc, Weak as RcWeak};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// How often the keys are swept, and how long a full key goes unused before it is
/// forgotten: a second, so that a key in steady use is not made again and again.
pub(crate) const SWEEP: Duration = Duration::from_secs(1);

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
        let process = Self::unswept(workers, max_key_bytes);
        let swept = Arc::downgrade(&process);
        std::thread::Builder::new()
            .name("edgerush-rate-limit-sweep".to_owned())
            .spawn(move || sweep(&swept, failed))?;
        Ok(process)
    }

    /// The keys of a process of `workers` workers, within `max_key_bytes`, with no thread of
    /// their own: whoever keeps them sweeps them ([`sweep_once`](Self::sweep_once)) every
    /// [`SWEEP`].
    pub(crate) fn unswept(workers: usize, max_key_bytes: u64) -> Arc<Self> {
        let entropy = RandomState::new();
        let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
        let local = Local::new(hasher, u64::try_from(workers).unwrap_or(u64::MAX));
        local.set_capacity(max_key_bytes);
        Arc::new(Self {
            local,
            hasher,
            start: Instant::now(),
        })
    }

    /// Forgets the keys full and unused for a [`SWEEP`] and, past the cap, evicts.
    pub(crate) fn sweep_once(&self) {
        let quiet = u64::try_from(SWEEP.as_nanos()).unwrap_or(u64::MAX);
        self.local.sweep(self.now(), quiet);
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
        process.sweep_once();
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
    /// forgotten a part at a time, every part once a second, for as long as the worker keeps
    /// it: no one pass holds the worker up reading every handle.
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
            let mut every = tokio::time::interval(SWEEP / HANDLE_PARTS as u32);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                every.tick().await;
                let Some(limiter) = held.upgrade() else {
                    return;
                };
                limiter.handles.borrow_mut().prune_next();
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
    /// once: admitted under all, or refused and charged under none, saying which refused it
    /// and how long it should wait to be admitted under all of them. Never later: nothing
    /// is waited for.
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
                let decided =
                    self.process
                        .local
                        .decide_saying(&mut self.handles.borrow_mut(), keys, now);
                let (Decision::Refuse { position, wait }, mut parts) = decided else {
                    return Limited::Admit;
                };
                // A key begins with its limit's name and a zero.
                let at = |part: usize| {
                    let (key, _) = keys.get(part)?;
                    let name = key.split(|&byte| byte == 0).next()?;
                    limits
                        .iter()
                        .position(|limit| limit.name.as_bytes() == name)
                };
                let mut by = 0;
                while parts != 0 {
                    let part = parts.trailing_zeros() as usize;
                    parts &= parts - 1;
                    if let Some(bit) =
                        at(part).and_then(|at| 1_u64.checked_shl(u32::try_from(at).ok()?))
                    {
                        by |= bit;
                    }
                }
                Limited::Refuse(Some(Refused {
                    by,
                    longest: at(position).map(|at| (at, Duration::from_nanos(wait))),
                }))
            },
        )
    }

    /// Decides a request under `keys`, each with its contract, all at once: admitted under
    /// all, or refused by the key that waits longest, by its place among them, and charged
    /// under none.
    pub(crate) fn decide_keys(&self, keys: &[(Vec<u8>, Contract)]) -> Decision {
        let now = self.process.now();
        self.process
            .local
            .decide(&mut self.handles.borrow_mut(), keys, now)
    }
}
