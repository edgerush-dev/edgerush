//! Rooms with a sync, as EdgeRush's spike (`service::shared`) has them, narrowed down: a
//! key's room is what workers may take before the process looks again, taken by
//! compare-and-swap; the first take since a fold puts the room on the process's queue.
//! The process thread holds every key's real state, single-threaded: every millisecond it
//! folds the rooms taken from — what was taken charged, the room put back — and it answers
//! a worker that found a room empty, or none, which waits for the answer. Every 10 ms the
//! cycle takes what each key admitted, for the service, and gives it its share again.

use crate::model::{CYCLE, Clock, Contract, IdMap, SYNC, Sharded};
use crate::{Decide, Design};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[repr(align(128))]
pub(crate) struct Room {
    left: AtomicU64,
    /// Until when the bucket has no room at all; 0 for no such time.
    refuse_until: AtomicU64,
    marked: AtomicBool,
    key: u64,
}

enum Take {
    Taken,
    Refuse,
    Ask,
}

impl Room {
    fn new(key: u64) -> Self {
        Self {
            left: AtomicU64::new(0),
            refuse_until: AtomicU64::new(0),
            marked: AtomicBool::new(false),
            key,
        }
    }

    fn take(self: &Arc<Self>, now: u64, marks: &Sender<Arc<Room>>) -> Take {
        if self.refuse_until.load(Ordering::Acquire) > now {
            return Take::Refuse;
        }
        let taken = self
            .left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            });
        if taken.is_err() {
            return Take::Ask;
        }
        if !self.marked.load(Ordering::Acquire) && !self.marked.swap(true, Ordering::AcqRel) {
            let _gone = marks.send(Arc::clone(self));
        }
        Take::Taken
    }
}

/// A worker's ask: the key, and where to answer — admitted or not, and the key's room.
type Ask = (u64, Sender<(bool, Arc<Room>)>);

/// The process's side, kept between its thread's runs: the ends of its queues — the rooms
/// taken from, and the asks — and every key's real state.
struct Ends {
    marked: Receiver<Arc<Room>>,
    asked: Receiver<Ask>,
    keys: IdMap<Real>,
    refusing: Vec<(u64, u64)>,
}

pub(crate) struct Rooms {
    contract: Contract,
    /// Rooms made already, for a worker that meets a key to find without asking.
    registry: Sharded<Arc<Room>>,
    marks: Sender<Arc<Room>>,
    asks: Sender<Ask>,
    /// The process's ends of the queues, taken by its thread.
    process: Mutex<Option<Ends>>,
    reported: AtomicU64,
}

impl Rooms {
    pub(crate) fn new(contract: Contract) -> Self {
        let (marks, marked) = channel();
        let (asks, asked) = channel();
        Self {
            contract,
            registry: Sharded::new(),
            marks,
            asks,
            process: Mutex::new(Some(Ends {
                marked,
                asked,
                keys: IdMap::default(),
                refusing: Vec::new(),
            })),
            reported: AtomicU64::new(0),
        }
    }

    pub(crate) fn reported(&self) -> u64 {
        self.reported.load(Ordering::Acquire)
    }
}

pub(crate) struct Taker<'a> {
    design: &'a Rooms,
    cache: IdMap<Arc<Room>>,
    asked: u64,
}

impl Taker<'_> {
    fn ask(&mut self, key: u64) -> bool {
        self.asked += 1;
        // A channel of its own for each ask: an ask the process never reads drops its end,
        // and the worker waiting for it is let go.
        let (answer, answered) = channel();
        if self.design.asks.send((key, answer)).is_err() {
            return false;
        }
        match answered.recv() {
            Ok((admitted, room)) => {
                self.cache.insert(key, room);
                admitted
            }
            Err(_) => false,
        }
    }
}

impl Decide for Taker<'_> {
    fn decide(&mut self, key: u64, now: u64) -> bool {
        let room = match self.cache.get(&key) {
            Some(room) => Some(Arc::clone(room)),
            None => self.design.registry.get(key).inspect(|room| {
                self.cache.insert(key, Arc::clone(room));
            }),
        };
        let Some(room) = room else {
            return self.ask(key);
        };
        match room.take(now, &self.design.marks) {
            Take::Taken => true,
            Take::Refuse => false,
            Take::Ask => self.ask(key),
        }
    }

    fn asks(&self) -> u64 {
        self.asked
    }
}

/// What the process holds of a key.
struct Real {
    room: Arc<Room>,
    tat: u64,
    spent: u64,
    given: u64,
}

/// The process thread's state.
struct Process<'a> {
    design: &'a Rooms,
    keys: IdMap<Real>,
    refusing: Vec<(u64, u64)>,
}

impl Process<'_> {
    /// Folds `key` at `now`: what its room gave out charged, and its room put back.
    fn fold(&mut self, key: u64, now: u64) -> &Arc<Room> {
        let contract = self.design.contract;
        let design = self.design;
        let real = self.keys.entry(key).or_insert_with(|| Real {
            room: design
                .registry
                .get_or_insert(key, || Arc::new(Room::new(key))),
            tat: 0,
            spent: 0,
            given: 0,
        });
        let left = real.room.left.swap(0, Ordering::AcqRel);
        let taken = real.given.saturating_sub(left);
        real.tat = contract.charge(real.tat, now, taken);
        real.spent += taken;
        let bucket = contract.room(real.tat, now);
        let room = bucket.min(contract.share.saturating_sub(real.spent));
        real.given = room;
        let until = if bucket == 0 {
            now + contract.wait(real.tat, now).max(1)
        } else {
            0
        };
        real.room.refuse_until.store(until, Ordering::Release);
        real.room.left.store(room, Ordering::Release);
        if until > 0 {
            self.refusing.push((until, key));
        }
        &real.room
    }

    fn answer(&mut self, (key, answers): Ask, now: u64) {
        let room = Arc::clone(self.fold(key, now));
        // Taken here as a worker would: what the fold just put back, or nothing.
        let admitted = room
            .left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if admitted && !room.marked.swap(true, Ordering::AcqRel) {
            let _gone = self.design.marks.send(Arc::clone(&room));
        }
        let _gone = answers.send((admitted, room));
    }

    fn sync(&mut self, marked: &Receiver<Arc<Room>>, now: u64) {
        let rooms: Vec<Arc<Room>> = marked.try_iter().collect();
        for room in rooms {
            room.marked.store(false, Ordering::Release);
            self.fold(room.key, now);
        }
        let (due, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.refusing)
            .into_iter()
            .partition(|(until, _)| *until <= now);
        self.refusing = waiting;
        for (_, key) in due {
            self.fold(key, now);
        }
    }

    fn cycle(&mut self, now: u64) {
        let mut reported = 0;
        let used: Vec<u64> = self
            .keys
            .iter()
            .filter(|(_, real)| {
                real.spent > 0 || real.given > real.room.left.load(Ordering::Acquire)
            })
            .map(|(key, _)| *key)
            .collect();
        for key in used {
            // What the room gave out since its last fold counted first.
            self.fold(key, now);
            if let Some(real) = self.keys.get_mut(&key) {
                reported += real.spent;
                real.spent = 0;
            }
            self.fold(key, now);
        }
        self.design.reported.fetch_add(reported, Ordering::AcqRel);
    }
}

impl Design for Rooms {
    type Worker<'a> = Taker<'a>;

    fn worker(&self, _index: usize) -> Taker<'_> {
        Taker {
            design: self,
            cache: IdMap::default(),
            asked: 0,
        }
    }

    fn background(&self, clock: Clock, stop: &AtomicBool, pause: &AtomicBool) {
        let Some(Ends {
            marked,
            asked,
            keys,
            refusing,
        }) = self
            .process
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        let mut process = Process {
            design: self,
            keys,
            refusing,
        };
        let mut next_sync = clock.now() + SYNC;
        let mut next_cycle = clock.now() + CYCLE;
        while !stop.load(Ordering::Acquire) {
            while pause.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            let now = clock.now();
            if now >= next_sync {
                process.sync(&marked, now);
                next_sync = now + SYNC;
            }
            if now >= next_cycle {
                process.cycle(now);
                next_cycle = now + CYCLE;
            }
            let wait = Duration::from_nanos(next_sync.saturating_sub(clock.now()));
            match asked.recv_timeout(wait) {
                Ok(ask) => process.answer(ask, clock.now()),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        // Whoever still waits is answered before the end, and the last admissions counted.
        while let Ok(ask) = asked.try_recv() {
            process.answer(ask, clock.now());
        }
        process.sync(&marked, clock.now());
        process.cycle(clock.now());
        *self
            .process
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Ends {
            marked,
            asked,
            keys: process.keys,
            refusing: process.refusing,
        });
    }
}
