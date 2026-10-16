//! A pod's view of its keys shared among its workers (12 item 3): the process alone a
//! [`DataPlane`] of the protocol, its workers deciding from slices of its room.
//!
//! Every sync, about every millisecond, a [`Worker`] tells its [`Process`] what it admitted
//! under each key since the last, and the process answers with a new slice of each key's
//! room: the room divided equally among the workers that came to the key lately, never more
//! than the room less the slices other workers still hold. A slice not used lapses at the
//! worker's next sync. A request every key of which has slice left is admitted by the worker
//! on the spot — no message, no shared memory; one it cannot decide goes to the process,
//! which decides it from its view as a data plane decides a request, waiting on the service
//! for a key it holds no fresh copy of. A key with no room comes back with until when, and
//! the worker refuses on its own till then.
//!
//! No I/O: what carries the messages between worker and process — channels between
//! threads, or the simulator — is the caller's.

use super::{Contract, DataPlane, Decision, Ticket};
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;

/// How a worker's process counts it among those that came to a key lately: within two
/// syncs.
const LATELY_SYNCS: u64 = 2;

/// What a worker admitted under a key since its last sync, or held a slice of unused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Used {
    /// The key.
    pub key: Box<[u8]>,
    /// Its contract.
    pub contract: Contract,
    /// Requests admitted under it.
    pub count: u64,
}

/// A slice of a key's room, for one worker until its next sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    /// The key.
    pub key: Box<[u8]>,
    /// Its contract.
    pub contract: Contract,
    /// Requests the worker may admit under it before its next sync.
    pub allowance: u64,
    /// With no room left, until when the worker refuses requests under it on its own.
    pub refuse_until: Option<u64>,
}

/// How a worker decides a request on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Local {
    /// Every key had slice left: admitted, and taken from each.
    Admit,
    /// A key has no room until a time not yet come: refused.
    Refuse,
    /// Neither: for the process to decide.
    Ask,
}

/// What a worker holds of a key.
#[derive(Clone, Debug)]
struct Held {
    contract: Contract,
    allowance: u64,
    used: u64,
    refuse_until: Option<u64>,
}

/// A worker's side: the slices its process gave it.
#[derive(Debug)]
pub struct Worker<S> {
    keys: HashMap<Box<[u8]>, Held, S>,
}

impl<S: BuildHasher> Worker<S> {
    /// A worker holding nothing, its keys hashed by `hasher`.
    #[must_use]
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            keys: HashMap::with_hasher(hasher),
        }
    }

    /// Decides a request under `keys` at `now` if it can: admitted if every key has slice
    /// left, taken from all of them; refused if a key has no room until a time to come;
    /// otherwise for the process.
    pub fn decide(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Local {
        let mut room = true;
        for (key, contract) in keys {
            match self
                .keys
                .get(*key)
                .filter(|held| held.contract == *contract)
            {
                Some(held) if held.refuse_until.is_some_and(|until| until > now) => {
                    return Local::Refuse;
                }
                Some(held) if held.allowance > 0 => {}
                _ => room = false,
            }
        }
        if !room {
            return Local::Ask;
        }
        for (key, _) in keys {
            if let Some(held) = self.keys.get_mut(*key) {
                held.allowance -= 1;
                held.used += 1;
            }
        }
        Local::Admit
    }

    /// What it holds for the next sync: every key it admitted under or held a slice of
    /// since the last, its slices lapsing now, as they do at a sync; keys it did neither for
    /// are forgotten.
    pub fn take_used(&mut self, now: u64) -> Vec<Used> {
        let mut used = Vec::new();
        self.keys.retain(|key, held| {
            let sent = held.used > 0 || held.allowance > 0;
            if sent {
                used.push(Used {
                    key: key.clone(),
                    contract: held.contract,
                    count: held.used,
                });
            }
            held.used = 0;
            held.allowance = 0;
            sent || held.refuse_until.is_some_and(|until| until > now)
        });
        used
    }

    /// The slices its process gave it, each in place of what it held of the key.
    pub fn on_slices(&mut self, slices: Vec<Slice>) {
        for slice in slices {
            let held = self.keys.entry(slice.key).or_insert(Held {
                contract: slice.contract,
                allowance: 0,
                used: 0,
                refuse_until: None,
            });
            if held.contract != slice.contract {
                // Another contract: the key starts again, as everywhere (06 §2).
                held.used = 0;
            }
            held.contract = slice.contract;
            held.allowance = slice.allowance;
            held.refuse_until = slice.refuse_until;
        }
    }
}

/// The process's answer to a request a worker asked it to decide.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// Whether it was admitted.
    pub admitted: bool,
    /// Refused, those of its keys that refused it, by position in its list, each with how
    /// long from the decision it waits to admit one.
    pub refused_by: Box<[(usize, u64)]>,
    /// New slices of its keys for the worker.
    pub slices: Vec<Slice>,
}

/// What the process has given out of a key's room, by worker.
#[derive(Clone, Debug)]
struct Given {
    contract: Contract,
    /// By worker: the slice it holds until its next sync.
    slices: Vec<u64>,
    /// By worker: when it last came to the key, by a sync or an ask.
    seen: Vec<Option<u64>>,
}

/// A request a worker asked about, waiting on the service.
#[derive(Clone, Debug)]
struct Asked {
    worker: usize,
    id: u64,
    keys: Vec<(Box<[u8]>, Contract)>,
}

/// The process's side: the pod's one data plane, and what it has given its workers.
pub struct Process<S> {
    plane: DataPlane<S>,
    workers: usize,
    /// The time between a worker's syncs.
    sync: u64,
    given: HashMap<Box<[u8]>, Given, S>,
    asked: BTreeMap<Ticket, Asked>,
    answers: Vec<(usize, u64, Answer)>,
}

impl<S: BuildHasher + Clone> Process<S> {
    /// The process of `workers`, which sync every `sync` nanoseconds, sharing `plane`'s
    /// view; its own maps hashed as the plane's are, by `hasher`.
    #[must_use]
    pub fn new(plane: DataPlane<S>, workers: usize, sync: u64, hasher: S) -> Self {
        Self {
            plane,
            workers: workers.max(1),
            sync: sync.max(1),
            given: HashMap::with_hasher(hasher),
            asked: BTreeMap::new(),
            answers: Vec::new(),
        }
    }

    /// Its data plane, for whatever carries its messages to the service and back.
    pub fn plane(&mut self) -> &mut DataPlane<S> {
        &mut self.plane
    }

    /// Worker `worker`'s sync at `now`: what it admitted charged, its slices lapsed, and new
    /// ones given for every key it sent.
    pub fn on_used(&mut self, worker: usize, used: Vec<Used>, now: u64) -> Vec<Slice> {
        let mut slices = Vec::with_capacity(used.len());
        for Used {
            key,
            contract,
            count,
        } in used
        {
            if count > 0 {
                self.plane.charge(&key, contract, count, now);
            }
            self.lapse(&key, contract, worker, now);
            slices.push(self.slice(key, contract, worker, now));
        }
        slices
    }

    /// Worker `worker` asks about request `id` under `keys` at `now`: decided past what is
    /// given out, the answer now, or, a key held no fresh copy of, once the service answers
    /// ([`take_answers`](Self::take_answers)).
    pub fn on_ask(
        &mut self,
        worker: usize,
        id: u64,
        keys: &[(&[u8], Contract)],
        now: u64,
    ) -> Option<Answer> {
        for (key, contract) in keys {
            self.lapse(key, *contract, worker, now);
        }
        // A key with room, all of it given out: refused here, the room the workers'.
        let short: Vec<(usize, u64)> = keys
            .iter()
            .enumerate()
            .filter(|(_, (key, contract))| self.available(key, *contract, now) == Some(0))
            .map(|(at, (key, contract))| (at, self.wait(key, *contract, now)))
            .collect();
        if !short.is_empty() {
            return Some(Answer {
                admitted: false,
                refused_by: short.into(),
                slices: self.slices(keys, worker, now),
            });
        }
        match self.plane.decide_all(keys, now) {
            Decision::Admit => Some(Answer {
                admitted: true,
                refused_by: Box::default(),
                slices: self.slices(keys, worker, now),
            }),
            Decision::Refuse => Some(Answer {
                admitted: false,
                refused_by: self.plane.waits(keys, now).collect(),
                slices: self.slices(keys, worker, now),
            }),
            Decision::Wait(ticket) => {
                let keys = keys
                    .iter()
                    .map(|(key, contract)| ((*key).into(), *contract));
                self.asked.insert(
                    ticket,
                    Asked {
                        worker,
                        id,
                        keys: keys.collect(),
                    },
                );
                None
            }
        }
    }

    /// The answers to requests that waited on the service, decided since last asked: the
    /// worker, the request's id, and the answer.
    pub fn take_answers(&mut self, now: u64) -> Vec<(usize, u64, Answer)> {
        for decided in self.plane.take_decisions() {
            let Some(asked) = self.asked.remove(&decided.ticket) else {
                continue;
            };
            let keys: Vec<(&[u8], Contract)> = asked
                .keys
                .iter()
                .map(|(key, contract)| (&**key, *contract))
                .collect();
            let slices = self.slices(&keys, asked.worker, now);
            self.answers.push((
                asked.worker,
                asked.id,
                Answer {
                    admitted: decided.admitted,
                    refused_by: decided.refused_by,
                    slices,
                },
            ));
        }
        std::mem::take(&mut self.answers)
    }

    /// Worker `worker`'s slice of `key` lapsed at `now`: the room it held is the process's
    /// again, and the worker counted as having come to the key.
    fn lapse(&mut self, key: &[u8], contract: Contract, worker: usize, now: u64) {
        let workers = self.workers;
        let given = self
            .given
            .entry(key.into())
            .and_modify(|given| {
                if given.contract != contract {
                    *given = Given::new(contract, workers);
                }
            })
            .or_insert_with(|| Given::new(contract, workers));
        if let Some(slice) = given.slices.get_mut(worker) {
            *slice = 0;
        }
        if let Some(seen) = given.seen.get_mut(worker) {
            *seen = Some(now);
        }
    }

    /// What of `key`'s room is not given out at `now`; `None` while it is cold.
    fn available(&self, key: &[u8], contract: Contract, now: u64) -> Option<u64> {
        let room = self.plane.room(key, contract, now)?;
        let out: u64 = self
            .given
            .get(key)
            .filter(|given| given.contract == contract)
            .map_or(0, |given| given.slices.iter().sum());
        Some(room.saturating_sub(out))
    }

    /// How long from `now` `key` waits to admit a request, as the copy has it; if it would
    /// admit one now but its room is given out, until the workers' next syncs.
    fn wait(&self, key: &[u8], contract: Contract, now: u64) -> u64 {
        self.plane
            .waits(&[(key, contract)], now)
            .next()
            .map_or(self.sync, |(_, wait)| wait)
    }

    /// A new slice of each of `keys` for `worker`.
    fn slices(&mut self, keys: &[(&[u8], Contract)], worker: usize, now: u64) -> Vec<Slice> {
        keys.iter()
            .map(|(key, contract)| self.slice((*key).into(), *contract, worker, now))
            .collect()
    }

    /// A new slice of `key` for `worker`, whose old one has lapsed: its room divided equally
    /// among the workers that came to it lately, no more than the room not given out.
    fn slice(&mut self, key: Box<[u8]>, contract: Contract, worker: usize, now: u64) -> Slice {
        let Some(room) = self.plane.room(&key, contract, now) else {
            // Cold: nothing to give until the service answers; asks decide it meanwhile.
            return Slice {
                key,
                contract,
                allowance: 0,
                refuse_until: None,
            };
        };
        let available = self.available(&key, contract, now).unwrap_or(0);
        let lately = self.sync.saturating_mul(LATELY_SYNCS);
        let given = self
            .given
            .entry(key.clone())
            .or_insert_with(|| Given::new(contract, self.workers));
        let active = given
            .seen
            .iter()
            .filter(|seen| seen.is_some_and(|seen| now.saturating_sub(seen) <= lately))
            .count()
            .max(1) as u64;
        let allowance = available.min(room / active);
        if let Some(slice) = given.slices.get_mut(worker) {
            *slice = allowance;
        }
        let refuse_until = (available == 0).then(|| now + self.wait(&key, contract, now));
        Slice {
            key,
            contract,
            allowance,
            refuse_until,
        }
    }
}

impl Given {
    fn new(contract: Contract, workers: usize) -> Self {
        Self {
            contract,
            slices: vec![0; workers],
            seen: vec![None; workers],
        }
    }
}

#[cfg(test)]
mod tests;
