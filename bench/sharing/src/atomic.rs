//! Atomics: a key's whole state in one word — its bucket's theoretical arrival time and
//! what it admitted this cycle, packed — that a worker moves by compare-and-swap, bucket
//! and share checked and charged together with no lock. Keys are found as in the locked
//! design: a sharded map read on every request, or, cached, a handle kept by the worker.
//! The background thread is the cycle, taking each key's admissions by compare-and-swap.

use crate::model::{CYCLE, Clock, Contract, IdMap, Sharded};
use crate::{Decide, Design};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Bits of the word for what a key admitted this cycle; the rest, 40, for its arrival time
/// in nanoseconds since the run began: 18 minutes, longer than any run here.
const SPENT_BITS: u32 = 24;
const MOST_SPENT: u64 = (1 << SPENT_BITS) - 1;
const LAST_TAT: u64 = (1 << (64 - SPENT_BITS)) - 1;

fn unpack(word: u64) -> (u64, u64) {
    (word >> SPENT_BITS, word & MOST_SPENT)
}

fn pack(tat: u64, spent: u64) -> u64 {
    (tat << SPENT_BITS) | spent
}

/// A key's word, on a cache line of its own.
#[derive(Default)]
#[repr(align(128))]
struct Key {
    word: AtomicU64,
}

pub(crate) struct Atomics {
    contract: Contract,
    keys: Sharded<Arc<Key>>,
    cached: bool,
    reported: AtomicU64,
}

impl Atomics {
    pub(crate) fn new(contract: Contract, cached: bool) -> Self {
        Self {
            contract,
            keys: Sharded::new(),
            cached,
            reported: AtomicU64::new(0),
        }
    }

    pub(crate) fn reported(&self) -> u64 {
        self.reported.load(Ordering::Acquire)
    }

    fn cycle(&self) {
        let mut reported = 0;
        self.keys.each(|_, key| {
            let before = key
                .word
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                    let (tat, spent) = unpack(word);
                    (spent > 0).then(|| pack(tat, 0))
                });
            if let Ok(word) = before {
                reported += unpack(word).1;
            }
        });
        self.reported.fetch_add(reported, Ordering::AcqRel);
    }
}

fn decide_in(key: &Key, contract: Contract, now: u64) -> bool {
    let share = contract.share.min(MOST_SPENT);
    let mut word = key.word.load(Ordering::Acquire);
    loop {
        let (tat, spent) = unpack(word);
        if spent >= share {
            return false;
        }
        let Some(next) = contract.admit(tat, now).filter(|next| *next <= LAST_TAT) else {
            return false;
        };
        match key.word.compare_exchange_weak(
            word,
            pack(next, spent + 1),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(seen) => word = seen,
        }
    }
}

pub(crate) struct Taker<'a> {
    design: &'a Atomics,
    cache: IdMap<Arc<Key>>,
}

impl Decide for Taker<'_> {
    fn decide(&mut self, key: u64, now: u64) -> bool {
        let design = self.design;
        if design.cached {
            let handle = self
                .cache
                .entry(key)
                .or_insert_with(|| design.keys.get_or_insert(key, Arc::default));
            return decide_in(handle, design.contract, now);
        }
        let decided = design.keys.with(key, |state| {
            state.map(|state| decide_in(state, design.contract, now))
        });
        match decided {
            Some(admitted) => admitted,
            None => decide_in(
                &design.keys.get_or_insert(key, Arc::default),
                design.contract,
                now,
            ),
        }
    }
}

impl Design for Atomics {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_word_holds_the_arrival_time_and_the_spent() {
        let word = pack(LAST_TAT, MOST_SPENT);
        assert_eq!(unpack(word), (LAST_TAT, MOST_SPENT));
        assert_eq!(unpack(pack(12_345, 7)), (12_345, 7));
    }
}
