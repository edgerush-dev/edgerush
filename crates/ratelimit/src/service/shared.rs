//! A pod's view of its keys shared among its workers through one room a key (12 item 3; a
//! spike, beside [`layer`](super::layer)): the process alone a [`DataPlane`] of the
//! protocol, and for each key it holds a [`Room`] of atomics that every worker takes from
//! directly — no message, no slice of its own — and that the process folds into its data
//! plane and fills again at every sync.
//!
//! A sync swaps what is left of a room for nothing, charges its data plane what was taken
//! since the last, and puts the plane's room for the key back: between the two a worker
//! finds the room empty and asks, so nothing is taken twice and never more than the room
//! of one data plane. The process finds the rooms taken from without looking at the others:
//! the first take from a room since its last fold marks it and puts it on the process's
//! queue, so a sync costs the keys in use, not those held. A worker that finds a room
//! empty asks the process, which folds the
//! key at once and decides from its data plane, waiting on the service for a key it holds
//! no fresh copy of. A key with no room at all comes back with until when, and every worker
//! refuses on its own till then.
//!
//! No I/O: a room is memory the threads share, and what carries the asks between worker and
//! process is the caller's.

use super::layer::Local;
use super::{Contract, DataPlane, Decision, Ticket};
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

/// How long a key no worker took from or asked about is kept: then its room is closed, and
/// a worker still holding it asks.
const IDLE: u64 = 60_000_000_000;

/// How often idle keys are looked for among all held: once a second, not at every sync.
const SWEEP: u64 = 1_000_000_000;

/// How many times an ask folds its keys and takes before it is refused, the workers having
/// emptied a room between the fold and the take each time.
const FOLDS: usize = 3;

/// Takes a request's worth from each of `rooms` at `now`, all or none: what was taken
/// given back when one of them has nothing, and which one, with what it came to.
fn take_all(rooms: &[Handed], now: u64) -> Result<(), (usize, Take)> {
    for (at, handed) in rooms.iter().enumerate() {
        let take = handed.room.take(now);
        if take != Take::Taken {
            for taken in &rooms[..at] {
                taken.room.give_back();
            }
            return Err((at, take));
        }
    }
    Ok(())
}

/// A key's room, shared by a process's workers: on a cache line of its own, so that one
/// key's takes do not slow another's.
#[derive(Debug)]
#[repr(align(128))]
pub struct Room {
    /// What may be taken before the process folds the key again.
    left: AtomicU64,
    /// Until when the bucket has no room at all; 0 for no such time.
    refuse_until: AtomicU64,
    /// The process no longer holds the key: whoever holds the room asks instead.
    closed: AtomicBool,
    /// Taken from since the process last folded it, and so on its queue.
    marked: AtomicBool,
    /// The key it is the room of, and its contract, for the process to fold it by.
    key: Box<[u8]>,
    contract: Contract,
    /// The process's queue of rooms taken from.
    marks: Sender<Arc<Room>>,
}

/// What taking from a room came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Take {
    /// One request's worth taken.
    Taken,
    /// No room until a time not yet come.
    Refuse,
    /// Nothing left before the process's next sync, or the room closed: for the process.
    Ask,
}

impl Room {
    fn new(key: Box<[u8]>, contract: Contract, marks: Sender<Arc<Room>>) -> Self {
        Self {
            left: AtomicU64::new(0),
            refuse_until: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            marked: AtomicBool::new(false),
            key,
            contract,
            marks,
        }
    }

    /// Takes a request's worth at `now`, if there is any left; the first since the process
    /// last folded the room puts it on the process's queue.
    pub fn take(self: &Arc<Self>, now: u64) -> Take {
        if self.closed.load(Ordering::Acquire) {
            return Take::Ask;
        }
        if self.refuse_until.load(Ordering::Acquire) > now {
            return Take::Refuse;
        }
        // A failed take writes nothing: a room run dry costs its workers a read.
        match self
            .left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            }) {
            Ok(_) => {
                // Read, not written, by every take but the first: the line is the take's.
                if !self.marked.load(Ordering::Acquire) && !self.marked.swap(true, Ordering::AcqRel)
                {
                    // The process gone, nothing is folded any more: the room is closed too.
                    let _gone = self.marks.send(Arc::clone(self));
                }
                Take::Taken
            }
            Err(_) => Take::Ask,
        }
    }

    /// Gives back what a [`take`](Self::take) took, for a request another key refused.
    pub fn give_back(&self) {
        self.left.fetch_add(1, Ordering::AcqRel);
    }

    /// Whether the process has closed it.
    #[must_use]
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// How long from `now` it has no room, if it has none.
    #[must_use]
    pub fn wait(&self, now: u64) -> Option<u64> {
        let until = self.refuse_until.load(Ordering::Acquire);
        (until > now).then(|| until - now)
    }
}

/// A key's room as a worker holds it.
#[derive(Clone, Debug)]
pub struct Handed {
    /// The key.
    pub key: Box<[u8]>,
    /// Its contract.
    pub contract: Contract,
    /// Its room.
    pub room: Arc<Room>,
}

/// A worker's side: the rooms its process handed it.
#[derive(Debug)]
pub struct Worker<S> {
    rooms: HashMap<Box<[u8]>, (Contract, Arc<Room>), S>,
}

impl<S: BuildHasher> Worker<S> {
    /// A worker holding no room, its keys hashed by `hasher`.
    #[must_use]
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            rooms: HashMap::with_hasher(hasher),
        }
    }

    /// Decides a request under `keys` at `now` if it can: admitted if every key's room had
    /// some left, taken from all of them; refused if a key has no room until a time to
    /// come; otherwise for the process, what was taken given back.
    pub fn decide(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Local {
        for (at, (key, contract)) in keys.iter().enumerate() {
            let take = match self.rooms.get(*key) {
                Some((held, room)) if held == contract => room.take(now),
                _ => Take::Ask,
            };
            if take == Take::Taken {
                continue;
            }
            for (key, _) in &keys[..at] {
                if let Some((_, room)) = self.rooms.get(*key) {
                    room.give_back();
                }
            }
            if take == Take::Refuse {
                return Local::Refuse;
            }
            if self.rooms.get(*key).is_some_and(|(_, room)| room.closed()) {
                self.rooms.remove(*key);
            }
            return Local::Ask;
        }
        Local::Admit
    }

    /// Those of `keys` it refuses on its own at `now`, each by its position in `keys` with
    /// how long it waits.
    pub fn waits<'a>(
        &'a self,
        keys: &'a [(&[u8], Contract)],
        now: u64,
    ) -> impl Iterator<Item = (usize, u64)> + 'a {
        keys.iter()
            .enumerate()
            .filter_map(move |(position, (key, contract))| {
                let (held, room) = self.rooms.get(*key)?;
                (held == contract)
                    .then(|| room.wait(now))?
                    .map(|wait| (position, wait))
            })
    }

    /// The rooms its process handed it, each in place of what it held of the key.
    pub fn on_rooms(&mut self, rooms: Vec<Handed>) {
        for handed in rooms {
            self.rooms
                .insert(handed.key, (handed.contract, handed.room));
        }
    }

    /// Drops the rooms the process has closed.
    pub fn sweep(&mut self) {
        self.rooms.retain(|_, (_, room)| !room.closed());
    }

    /// How many rooms it holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rooms.len()
    }

    /// Whether it holds none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }
}

/// The process's answer to a request a worker asked about.
#[derive(Clone, Debug)]
pub struct Answer {
    /// Whether it was admitted.
    pub admitted: bool,
    /// Refused, the keys that refused it, by position, with how long each waits.
    pub refused_by: Box<[(usize, u64)]>,
    /// The rooms of its keys, for the worker to take from itself from now on.
    pub rooms: Vec<Handed>,
}

/// What the process holds of a key.
#[derive(Debug)]
struct Held {
    contract: Contract,
    room: Arc<Room>,
    /// What it put in the room at the last fold.
    given: u64,
    /// When a worker last took from it or asked about it.
    used: u64,
}

/// A request a worker asked about, waiting on the service.
#[derive(Clone, Debug)]
struct Asked {
    worker: usize,
    id: u64,
    keys: Vec<(Box<[u8]>, Contract)>,
}

/// The process's side: the pod's one data plane, and a room for each key.
pub struct Process<S> {
    plane: DataPlane<S>,
    keys: HashMap<Box<[u8]>, Held, S>,
    asked: BTreeMap<Ticket, Asked>,
    answers: Vec<(usize, u64, Answer)>,
    /// The rooms' queue: those taken from since their last fold.
    marks: (Sender<Arc<Room>>, Receiver<Arc<Room>>),
    /// The rooms with no room until a time, to fold when it comes.
    refusing: Vec<(u64, Arc<Room>)>,
    /// When idle keys were last looked for.
    swept: u64,
}

impl<S: BuildHasher + Clone> Process<S> {
    /// The process deciding from `plane`, its own map hashed as the plane's is, by `hasher`.
    #[must_use]
    pub fn new(plane: DataPlane<S>, hasher: S) -> Self {
        Self {
            plane,
            keys: HashMap::with_hasher(hasher),
            asked: BTreeMap::new(),
            answers: Vec::new(),
            marks: channel(),
            refusing: Vec::new(),
            swept: 0,
        }
    }

    /// Its data plane, for whatever carries its messages to the service and back.
    pub fn plane(&mut self) -> &mut DataPlane<S> {
        &mut self.plane
    }

    /// Its sync at `now`: every room taken from since its last fold folded, and one whose
    /// bucket was empty filled if it has room again; now and then, a key idle long closed
    /// and forgotten.
    pub fn on_sync(&mut self, now: u64) {
        let marked: Vec<Arc<Room>> = self.marks.1.try_iter().collect();
        for room in marked {
            // Unmarked first: a take after this marks it again, for the next sync.
            room.marked.store(false, Ordering::Release);
            self.fold_room(&room, now);
        }
        let (due, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.refusing)
            .into_iter()
            .partition(|(until, _)| *until <= now);
        self.refusing = waiting;
        for (_, room) in due {
            self.fold_room(&room, now);
        }
        if now.saturating_sub(self.swept) >= SWEEP {
            self.swept = now;
            self.keys.retain(|_, held| {
                let idle = now.saturating_sub(held.used) > IDLE;
                if idle {
                    held.room.closed.store(true, Ordering::Release);
                }
                !idle
            });
        }
    }

    /// Folds `room` at `now` if it is still its key's: a room closed since is not.
    fn fold_room(&mut self, room: &Arc<Room>, now: u64) {
        let current = self
            .keys
            .get(&room.key)
            .is_some_and(|held| Arc::ptr_eq(&held.room, room));
        if current {
            self.fold(&room.key, room.contract, now);
        }
    }

    /// Worker `worker` asks about request `id` under `keys` at `now`: each key folded, then
    /// decided from the rooms as a worker would, or, a key held no fresh copy of, from the
    /// data plane, the answer once the service gives it ([`take_answers`](Self::take_answers)).
    pub fn on_ask(
        &mut self,
        worker: usize,
        id: u64,
        keys: &[(&[u8], Contract)],
        now: u64,
    ) -> Option<Answer> {
        for (key, _) in keys {
            if let Some(held) = self.keys.get_mut(*key) {
                held.used = now;
            }
        }
        // Workers may empty a room between its fold and the take here: folded again, what
        // they took is charged, and the room is the plane's again. A few times, then refused.
        let mut rooms = Vec::new();
        for _ in 0..FOLDS {
            for (key, contract) in keys {
                self.fold(key, *contract, now);
            }
            rooms = self.rooms(keys);
            match take_all(&rooms, now) {
                Ok(()) => {
                    return Some(Answer {
                        admitted: true,
                        refused_by: Box::default(),
                        rooms,
                    });
                }
                Err((at, Take::Refuse)) => {
                    let wait = rooms[at].room.wait(now).unwrap_or(0);
                    return Some(Answer {
                        admitted: false,
                        refused_by: Box::new([(at, wait)]),
                        rooms,
                    });
                }
                Err((at, _)) => {
                    let (key, contract) = keys[at];
                    if self.plane.room(key, contract, now).is_none() {
                        break;
                    }
                }
            }
        }
        if keys
            .iter()
            .all(|(key, contract)| self.plane.room(key, *contract, now).is_some())
        {
            return Some(Answer {
                admitted: false,
                refused_by: Box::default(),
                rooms,
            });
        }
        // A key with no fresh copy: the data plane decides it, as a data plane decides any.
        match self.plane.decide_all(keys, now) {
            Decision::Admit => Some(Answer {
                admitted: true,
                refused_by: Box::default(),
                rooms,
            }),
            Decision::Refuse => Some(Answer {
                admitted: false,
                refused_by: self.plane.waits(keys, now).collect(),
                rooms,
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
    /// worker, the request's id, and the answer, its keys folded.
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
            for (key, contract) in &keys {
                self.fold(key, *contract, now);
            }
            let rooms = self.rooms(&keys);
            self.answers.push((
                asked.worker,
                asked.id,
                Answer {
                    admitted: decided.admitted,
                    refused_by: decided.refused_by,
                    rooms,
                },
            ));
        }
        std::mem::take(&mut self.answers)
    }

    /// How many keys it holds rooms for.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether it holds none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The rooms of `keys`, as they are handed to a worker.
    fn rooms(&self, keys: &[(&[u8], Contract)]) -> Vec<Handed> {
        keys.iter()
            .filter_map(|(key, contract)| {
                let held = self.keys.get(*key)?;
                Some(Handed {
                    key: (*key).into(),
                    contract: *contract,
                    room: Arc::clone(&held.room),
                })
            })
            .collect()
    }

    /// Folds `key`'s room at `now`: what was taken since its last fold charged on the data
    /// plane, and the plane's room put back, or, the bucket empty, until when it has none.
    fn fold(&mut self, key: &[u8], contract: Contract, now: u64) {
        let marks = &self.marks.0;
        let room = || Arc::new(Room::new(key.into(), contract, marks.clone()));
        let held = self.keys.entry(key.into()).or_insert_with(|| Held {
            contract,
            room: room(),
            given: 0,
            used: now,
        });
        if held.contract != contract {
            // Another contract: the key starts again, as everywhere (06 §2); whoever holds
            // the old room asks.
            held.room.closed.store(true, Ordering::Release);
            *held = Held {
                contract,
                room: room(),
                given: 0,
                used: now,
            };
        }
        let left = held.room.left.swap(0, Ordering::AcqRel);
        let taken = held.given.saturating_sub(left);
        if taken > 0 {
            held.used = now;
            self.plane.charge(key, contract, taken, now);
        }
        let (given, until) = match self.plane.room(key, contract, now) {
            None => (0, 0),
            Some(0) => {
                let wait = self
                    .plane
                    .waits(&[(key, contract)], now)
                    .next()
                    .map_or(1, |(_, wait)| wait.max(1));
                (0, now.saturating_add(wait))
            }
            Some(room) => (room, 0),
        };
        if let Some(held) = self.keys.get_mut(key) {
            held.given = given;
            held.room.refuse_until.store(until, Ordering::Release);
            held.room.left.store(given, Ordering::Release);
            if until > 0 {
                self.refusing.push((until, Arc::clone(&held.room)));
            }
        }
    }
}

impl<S> Drop for Process<S> {
    fn drop(&mut self) {
        // A room on the queue holds a sender of it: emptied, the queue goes with the last.
        while self.marks.1.try_recv().is_ok() {}
    }
}

#[cfg(test)]
mod tests;
