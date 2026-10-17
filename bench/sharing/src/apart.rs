//! No limit, the floor every design is measured from; and each worker apart, as EdgeRush
//! decides today: a worker holds its own state of every key it sees and shares nothing,
//! held here to its part of the contract, as a worker would be were nothing shared. A
//! worker's cycle resets its share as it comes to a key, so nothing runs beside it.

use crate::model::{CYCLE, Clock, Contract, IdMap};
use crate::{Decide, Design};
use std::sync::atomic::AtomicBool;

/// Every request admitted, nothing kept.
pub(crate) struct Nothing;

pub(crate) struct Admit;

impl Decide for Admit {
    fn decide(&mut self, _key: u64, _now: u64) -> bool {
        true
    }
}

impl Design for Nothing {
    type Worker<'a> = Admit;

    fn worker(&self, _index: usize) -> Admit {
        Admit
    }

    fn background(&self, _clock: Clock, _stop: &AtomicBool, _pause: &AtomicBool) {}
}

/// Each worker apart, `ways` of them, each held to its part of `contract`.
pub(crate) struct Apart {
    contract: Contract,
}

impl Apart {
    pub(crate) fn new(contract: Contract, ways: u64) -> Self {
        Self {
            contract: contract.split(ways),
        }
    }
}

struct Held {
    tat: u64,
    spent: u64,
    cycle: u64,
}

pub(crate) struct Own {
    contract: Contract,
    keys: IdMap<Held>,
}

impl Decide for Own {
    fn decide(&mut self, key: u64, now: u64) -> bool {
        let cycle = now / CYCLE;
        let held = self.keys.entry(key).or_insert(Held {
            tat: 0,
            spent: 0,
            cycle,
        });
        if held.cycle != cycle {
            held.cycle = cycle;
            held.spent = 0;
        }
        if held.spent >= self.contract.share {
            return false;
        }
        match self.contract.admit(held.tat, now) {
            Some(next) => {
                held.tat = next;
                held.spent += 1;
                true
            }
            None => false,
        }
    }
}

impl Design for Apart {
    type Worker<'a> = Own;

    fn worker(&self, _index: usize) -> Own {
        Own {
            contract: self.contract,
            keys: IdMap::default(),
        }
    }

    fn background(&self, _clock: Clock, _stop: &AtomicBool, _pause: &AtomicBool) {}
}
