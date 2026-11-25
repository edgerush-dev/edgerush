//! Local mode's rate limits (06 §4): the process's keys, shared by its workers, with no
//! rate-limiting service and no thread on the request path. Each worker decides a request
//! from its own handles to the keys, one compare-and-swap a key
//! (`edgerush_ratelimit::local`); a thread of the process's sweeps the keys once a second,
//! forgetting those full and quiet, and evicts past the cap every [`CAP_LOOK`].

use super::{Failed, KeyedBy, Limited, Refused, Usage, with_keys};
use edgerush_config::CompiledLimit;
use edgerush_ratelimit::local::{Decision, HANDLE_PARTS, Handles, Limit, Local};
use edgerush_ratelimit::service::Keyed;
use std::cell::RefCell;
use std::hash::{BuildHasher, RandomState};
use std::rc::{Rc, Weak as RcWeak};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// How often the keys are swept, and how long a full key goes unused before it is
/// forgotten: a second, so that a key in steady use is not made again and again.
pub(crate) const SWEEP: Duration = Duration::from_secs(1);

/// How often the sweep's thread looks, between sweeps, whether the keys hold more than the
/// cap, evicting if they do: keys made faster than the cap allows pass it by at most what
/// is made in this time, not in a [`SWEEP`] (GitHub #410). Within the cap a look reads one
/// count.
pub(crate) const CAP_LOOK: Duration = Duration::from_millis(100);

/// The process's keys and the clock every worker decides them by.
#[derive(Debug)]
pub(crate) struct Process {
    local: Local<Keyed>,
    hasher: Keyed,
    start: Instant,
}

impl Process {
    /// The keys of a process, within `max_key_bytes`, and the thread that sweeps them, which
    /// calls `failed` if it panics.
    pub(crate) fn start(max_key_bytes: u64, failed: Option<Failed>) -> std::io::Result<Arc<Self>> {
        let process = Self::unswept(max_key_bytes);
        let swept = Arc::downgrade(&process);
        std::thread::Builder::new()
            .name("edgerush-rate-limit-sweep".to_owned())
            .spawn(move || sweep(&swept, failed))?;
        Ok(process)
    }

    /// The keys of a process, within `max_key_bytes`, with no thread of their own: whoever keeps them takes their [`turn`](Self::turn) every [`CAP_LOOK`] or
    /// sooner.
    pub(crate) fn unswept(max_key_bytes: u64) -> Arc<Self> {
        let entropy = RandomState::new();
        let hasher = Keyed::new([entropy.hash_one(0_u8), entropy.hash_one(1_u8)]);
        let local = Local::new(hasher);
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

    /// The sweep's thread's turn at `at`, its last sweep at `swept`: a sweep once a [`SWEEP`]
    /// has passed since, else eviction past the cap. When its last sweep is now.
    pub(crate) fn turn(&self, swept: Instant, at: Instant) -> Instant {
        if at.saturating_duration_since(swept) >= SWEEP {
            self.sweep_once();
            at
        } else {
            self.local.evict(self.now());
            swept
        }
    }

    /// The bytes its keys may hold, from now on: a reload that changes it keeps the keys.
    pub(crate) fn set_capacity(&self, max_key_bytes: u64) {
        self.local.set_capacity(max_key_bytes);
    }

    /// What its keys hold: nothing is forgiven, as nothing is reported.
    pub(crate) fn usage(&self) -> Usage {
        Usage {
            bytes: self.local.holding(),
            keys: self.local.key_count(),
            evicted: self.local.evicted(),
            forgiven: 0,
        }
    }

    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Sweeps the keys every [`SWEEP`], and evicts past the cap every [`CAP_LOOK`], for as long as
/// the process keeps them; tells `failed` if it panics, as the asynchronous mode's thread
/// does (03 §2).
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
    let mut swept = Instant::now();
    loop {
        std::thread::sleep(CAP_LOOK);
        let Some(process) = process.upgrade() else {
            return;
        };
        swept = process.turn(swept, Instant::now());
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
    contracts: RefCell<Vec<Limit>>,
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
            let mut every = tokio::time::interval(
                Duration::from_nanos(edgerush_ratelimit::HANDLES_PRUNED) / HANDLE_PARTS as u32,
            );
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
    pub(crate) fn usage(&self) -> Usage {
        if self.index == 0 {
            self.process.usage()
        } else {
            Usage::default()
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

    /// Decides a request under `keys`, each with its limit, all at once: admitted under
    /// all, or refused by the key that waits longest, by its place among them, and charged
    /// under none.
    pub(crate) fn decide_keys(&self, keys: &[(Vec<u8>, Limit)]) -> Decision {
        let now = self.process.now();
        self.process
            .local
            .decide(&mut self.handles.borrow_mut(), keys, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_ratelimit::local::Contract;
    use std::num::NonZeroU64;

    /// Keys `from` up to `to` made now, each charged once under a bucket of one an hour: none
    /// full again for an hour, so none a sweep could forget.
    fn spent(process: &Process, from: u64, to: u64) {
        let hour = NonZeroU64::new(3_600_000_000_000).unwrap();
        let one = NonZeroU64::MIN;
        let bucket = Limit::Bucket(Contract {
            limit: one,
            window: hour,
            burst: one,
        });
        let mut handles = Handles::new(process.hasher);
        for n in from..to {
            let key = n.to_be_bytes();
            let decided = process
                .local
                .decide(&mut handles, &[(key, bucket)], process.now());
            assert_eq!(decided, Decision::Admit);
        }
    }

    /// Between sweeps the thread's turn evicts the keys past the cap at once, not at the next
    /// sweep, so keys made faster than the sweep runs pass it only by a [`CAP_LOOK`]'s worth
    /// (GitHub #410); a [`SWEEP`] after the last, the turn sweeps.
    #[test]
    fn a_turn_between_sweeps_evicts_past_the_cap() {
        let process = Process::unswept(u64::MAX);
        let swept = Instant::now();
        spent(&process, 0, 20);
        let each = process.usage().bytes - process.local.tables();
        assert_eq!(each % 20, 0, "every key of the same bytes");
        let (each, tables) = (each / 20, process.local.tables());
        // Nine tenths of the cap holds the tables, which keep their room, and nine and a half
        // keys.
        process.set_capacity((tables + 9 * each + each / 2) * 10 / 9);
        assert_eq!(process.turn(swept, swept + CAP_LOOK), swept, "no sweep yet");
        let usage = process.usage();
        assert_eq!(usage.evicted, 11, "down to nine tenths of the cap");
        assert_eq!(usage.bytes, tables + 9 * each);
        assert_eq!(process.turn(swept, swept + SWEEP), swept + SWEEP, "a sweep");
    }
}
