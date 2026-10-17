//! A lock a key, as Redis serializes a script: a worker takes the key's lock and decides
//! the whole request under it — bucket and share together — and lets it go. Keys are in a
//! map split into shards, each behind a read-write lock: read on every request, or, cached,
//! only the first time a worker meets the key, the worker keeping the key's handle. The
//! background thread is the cycle: every 10 ms it takes what each key admitted, for the
//! service, and gives the key its share again.

use crate::model::{CYCLE, Clock, Contract, IdMap, Sharded};
use crate::{Decide, Design};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    tat: u64,
    /// Admitted in the cycle under way: what the cycle reports.
    spent: u64,
}

type Handle = Arc<Mutex<State>>;

pub(crate) struct Locked {
    contract: Contract,
    keys: Sharded<Handle>,
    cached: bool,
    reported: AtomicU64,
}

impl Locked {
    pub(crate) fn new(contract: Contract, cached: bool) -> Self {
        Self {
            contract,
            keys: Sharded::new(),
            cached,
            reported: AtomicU64::new(0),
        }
    }

    /// What its cycles reported, all told.
    pub(crate) fn reported(&self) -> u64 {
        self.reported.load(Ordering::Acquire)
    }

    fn cycle(&self) {
        let mut reported = 0;
        self.keys.each(|_, state| {
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reported += state.spent;
            state.spent = 0;
        });
        self.reported.fetch_add(reported, Ordering::AcqRel);
    }
}

fn decide_in(state: &Mutex<State>, contract: Contract, now: u64) -> bool {
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if state.spent >= contract.share {
        return false;
    }
    match contract.admit(state.tat, now) {
        Some(next) => {
            state.tat = next;
            state.spent += 1;
            true
        }
        None => false,
    }
}

pub(crate) struct Taker<'a> {
    design: &'a Locked,
    cache: IdMap<Handle>,
}

impl Decide for Taker<'_> {
    fn decide(&mut self, key: u64, now: u64) -> bool {
        let design = self.design;
        if design.cached {
            let handle = self
                .cache
                .entry(key)
                .or_insert_with(|| design.keys.get_or_insert(key, Handle::default));
            return decide_in(handle, design.contract, now);
        }
        let decided = design.keys.with(key, |state| {
            state.map(|state| decide_in(state, design.contract, now))
        });
        match decided {
            Some(admitted) => admitted,
            None => decide_in(
                &design.keys.get_or_insert(key, Handle::default),
                design.contract,
                now,
            ),
        }
    }
}

impl Design for Locked {
    type Worker<'a> = Taker<'a>;

    fn worker(&self, _index: usize) -> Taker<'_> {
        Taker {
            design: self,
            cache: IdMap::default(),
        }
    }

    fn background(&self, clock: Clock, stop: &AtomicBool, pause: &AtomicBool) {
        let mut next = clock.now() + CYCLE;
        while !stop.load(Ordering::Acquire) {
            crate::sleep_until(clock, next, pause);
            next += CYCLE;
            self.cycle();
        }
        self.cycle();
    }
}
