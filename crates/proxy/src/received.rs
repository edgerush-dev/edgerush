//! What a worker's HTTP/2 connections hold of what their peers sent, charged to the worker's
//! storage ([15 §3](../../docs/15-http2-and-grpc.md)).
//!
//! h2 reads every frame as it arrives, whichever stream it is for, and holds a stream's DATA
//! until it is read: up to the stream's window, and on a connection up to the connection's.
//! A request's body is read only as fast as its upstream takes it, and an answer only as
//! fast as its client does, so what a slow one was sent stays in h2 — 4 MiB a stream and
//! 16 MiB a connection on our server, 1 MiB and 16 MiB on our client. That is the worker's
//! memory as much as anything the storage account counts, so after each turn of its
//! driver a connection is charged what h2 says it holds: what it received and has not yet
//! given back as credit, and on a server what else h2 holds of what its client sent.
//!
//! The charge follows what came, since h2 takes a frame in before anyone can refuse it: a
//! turn reads no more than tokio's budget lets it, and never past the windows. So a charge
//! that does not fit is made all the same, past the limit, and the worker is short, as it is
//! when h2 refuses a server's allocation for want of room (`reclaim`). Then the connection
//! charged the most that could make the room is closed — the one asking, if it is that one
//! — and stays charged until it goes, for marking it frees nothing. One at a time: a
//! shortage while one is going closes nobody else, and once it has gone the next is closed
//! only for a shortage seen after, so that nothing is closed that nobody is waiting for.
//! Where no connection could make the room, the shortage is held elsewhere and nobody is
//! closed.
//!
//! One per worker, and it never leaves it. Nothing here does I/O or reads a clock.

#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::storage::{Charge, Storage};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::task::Waker;

/// A worker's HTTP/2 connections, server and client alike, and what each is charged.
pub struct Received {
    storage: Rc<Storage>,
    /// By a number of the table's own, for as long as each connection is driven.
    open: RefCell<HashMap<u64, Rc<Holding>>>,
    next: Cell<u64>,
    /// The connection closed to make room and not yet gone, by its number.
    closing: Cell<Option<u64>>,
}

/// What one connection is charged, and whether it was closed to make room.
#[derive(Default)]
struct Holding {
    charge: RefCell<Option<Charge>>,
    charged: Cell<usize>,
    shed: Cell<bool>,
    driver: RefCell<Option<Waker>>,
}

/// One connection's place in the table, for as long as it is driven; dropping it takes the
/// connection out, and its charge with it.
pub struct Account {
    table: Rc<Received>,
    number: u64,
    holding: Rc<Holding>,
}

impl std::fmt::Debug for Received {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Received")
            .field("connections", &self.open.borrow().len())
            .finish_non_exhaustive()
    }
}

impl Received {
    /// A worker's table, charging `storage`.
    pub fn new(storage: Rc<Storage>) -> Rc<Self> {
        Rc::new(Self {
            storage,
            open: RefCell::new(HashMap::new()),
            next: Cell::new(0),
            closing: Cell::new(None),
        })
    }

    /// The worker is `bytes` short: unless a connection is already going, the one charged
    /// the most of those whose charge, with the room left, covers the shortage is closed.
    /// With none, the shortage is held elsewhere, and nobody is.
    fn short_by(&self, bytes: usize) {
        if self.closing.get().is_some() {
            return;
        }
        let room = self.storage.room();
        let heaviest = self
            .open
            .borrow()
            .iter()
            .filter(|(_, holding)| !holding.shed.get() && holding.charged.get() > 0)
            .filter(|(_, holding)| holding.charged.get().saturating_add(room) >= bytes)
            .max_by_key(|(_, holding)| holding.charged.get())
            .map(|(number, holding)| (*number, Rc::clone(holding)));
        if let Some((number, holding)) = heaviest {
            self.closing.set(Some(number));
            holding.shed();
        }
    }

    /// A connection's account, charged nothing yet.
    pub fn open(self: &Rc<Self>) -> Account {
        let number = self.next.get();
        self.next.set(number.wrapping_add(1));
        let holding = Rc::new(Holding::default());
        self.open.borrow_mut().insert(number, Rc::clone(&holding));
        Account {
            table: Rc::clone(self),
            number,
            holding,
        }
    }
}

impl Account {
    /// Charges the connection what h2 says it holds now, `held`. What does not fit is
    /// charged past the limit all the same, since it is held already, and the worker is
    /// short by that much. A connection closed to make room keeps what it is charged until
    /// it goes.
    pub fn settle(&self, held: usize) {
        let holding = &self.holding;
        if holding.shed.get() {
            return;
        }
        let storage = &self.table.storage;
        let charged = holding.charged.get();
        if holding.charge_to(storage, held).is_err() {
            // Refused, so it is more than was charged and more than the room left.
            let short = held - charged - storage.room();
            {
                let mut charge = holding.charge.borrow_mut();
                match charge.as_mut() {
                    Some(charge) => charge.grow_past(held - charged),
                    None => *charge = Some(storage.reserve_past(held)),
                }
            }
            holding.charged.set(held);
            self.table.short_by(short);
        }
    }

    /// h2 refused an allocation of `wanted` bytes for want of room in the worker: the
    /// worker is short by that much, as when a charge does not fit.
    pub fn reclaim(&self, wanted: usize) {
        self.table.short_by(wanted);
    }

    /// Adds what `charge` reserves to the connection's charge, without letting go of it or
    /// reserving it again: a reservation made for what the connection may come to hold, to
    /// be settled to what it does hold.
    pub fn absorb(&self, charge: Charge) {
        let bytes = charge.bytes();
        let mut held = self.holding.charge.borrow_mut();
        match held.as_mut() {
            Some(held) => held.absorb(charge),
            None => *held = Some(charge),
        }
        self.holding.charged.set(self.holding.charged.get() + bytes);
    }

    /// Splits `bytes` of the connection's charge off into a charge of their own, for memory
    /// the connection hands over to whatever takes it on: what is held does not change, and
    /// nothing is let go of and reserved again. Never more than the connection is charged.
    pub fn split(&self, bytes: usize) -> Option<Charge> {
        let mut held = self.holding.charge.borrow_mut();
        let split = held.as_mut()?.split(bytes);
        self.holding
            .charged
            .set(self.holding.charged.get() - split.bytes());
        Some(split)
    }

    /// Whether the connection was closed to make room: its driver closes it.
    pub fn is_shed(&self) -> bool {
        self.holding.shed.get()
    }

    /// The connection's driver is the one polling with `waker`: woken if it is shed.
    pub fn drive_with(&self, waker: &Waker) {
        let mut driver = self.holding.driver.borrow_mut();
        if !driver.as_ref().is_some_and(|kept| kept.will_wake(waker)) {
            *driver = Some(waker.clone());
        }
    }

    /// What the connection is charged.
    #[cfg(test)]
    pub fn charged(&self) -> usize {
        self.holding.charged.get()
    }
}

impl Drop for Account {
    fn drop(&mut self) {
        self.table.open.borrow_mut().remove(&self.number);
        // Its memory goes with it, and only now its charge.
        self.holding.charge.borrow_mut().take();
        self.holding.charged.set(0);
        if self.table.closing.get() == Some(self.number) {
            self.table.closing.set(None);
        }
    }
}

impl Holding {
    fn charge_to(
        &self,
        storage: &Rc<Storage>,
        held: usize,
    ) -> Result<(), crate::storage::Exhausted> {
        let charged = self.charged.get();
        let mut charge = self.charge.borrow_mut();
        match charge.as_mut() {
            _ if held == charged => {}
            Some(charge) if held > charged => charge.grow(held - charged)?,
            Some(charge) => charge.shrink(charged - held),
            None => *charge = Some(storage.reserve(held)?),
        }
        self.charged.set(held);
        Ok(())
    }

    fn shed(&self) {
        self.shed.set(true);
        if let Some(driver) = self.driver.borrow_mut().take() {
            driver.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    /// A waker that notes it was woken.
    struct Noted(AtomicBool);

    impl Wake for Noted {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn noted() -> (Arc<Noted>, Waker) {
        let noted = Arc::new(Noted(AtomicBool::new(false)));
        (Arc::clone(&noted), Waker::from(Arc::clone(&noted)))
    }

    /// A connection is charged what it holds now, up and down, and nothing once it goes.
    #[test]
    fn a_connection_is_charged_what_it_holds_and_nothing_once_it_goes() {
        let storage = Storage::with_provision(1 << 20, 0);
        let table = Received::new(Rc::clone(&storage));
        let account = table.open();
        account.settle(4096);
        assert_eq!((storage.used(), account.charged()), (4096, 4096));
        account.settle(100);
        assert_eq!(storage.used(), 100);
        account.settle(0);
        assert_eq!(storage.used(), 0);
        account.settle(5000);
        drop(account);
        assert_eq!(storage.used(), 0);
        assert!(table.open.borrow().is_empty());
    }

    /// What does not fit is charged past the limit all the same, since it is held already,
    /// and the connection charged the most is closed and woken. It keeps its charge until it
    /// goes, for closing it frees nothing; then the charge goes, and only then.
    #[test]
    fn at_the_limit_the_heaviest_is_closed_and_stays_charged_until_it_goes() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let (light, heavy, asking) = (table.open(), table.open(), table.open());
        let (woken, waker) = noted();
        heavy.drive_with(&waker);
        light.settle(200);
        heavy.settle(600);
        asking.settle(150);
        assert!(!heavy.is_shed());
        asking.settle(400);
        assert!(heavy.is_shed() && !light.is_shed() && !asking.is_shed());
        assert!(
            woken.0.load(Ordering::SeqCst),
            "the closed one was not woken"
        );
        assert_eq!((heavy.charged(), asking.charged()), (600, 400));
        assert_eq!(storage.used(), 1200);
        // Closing, it is charged no more and no less.
        heavy.settle(900);
        heavy.settle(0);
        assert_eq!(heavy.charged(), 600);
        drop(heavy);
        assert_eq!(storage.used(), 600);
    }

    /// One at a time: a shortage while a closed connection has not yet gone closes nobody
    /// else, however many come, so that a burst cannot close many before any room is made.
    #[test]
    fn a_shortage_while_one_is_going_closes_nobody_else() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let accounts: Vec<Account> = (0..5).map(|_| table.open()).collect();
        for (n, account) in accounts.iter().enumerate() {
            account.settle(100 + 50 * n);
        }
        // 1000 held of 1000: every further byte is a shortage.
        accounts[0].settle(250);
        let closed = || accounts.iter().filter(|account| account.is_shed()).count();
        assert_eq!(closed(), 1);
        assert!(accounts[4].is_shed(), "the heaviest goes first");
        for account in &accounts[..4] {
            account.reclaim(64);
        }
        accounts[1].settle(500);
        assert_eq!(closed(), 1, "a second closed before the first had gone");
    }

    /// Once the closed connection has gone, nobody more is closed until the worker is found
    /// short again: then the next charged the most is.
    #[test]
    fn once_the_closed_one_has_gone_the_next_is_closed_only_for_a_new_shortage() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let (a, b, c, asking) = (table.open(), table.open(), table.open(), table.open());
        a.settle(400);
        b.settle(300);
        c.settle(200);
        // 100 left; 250 asked: 150 short.
        asking.settle(250);
        assert!(a.is_shed() && !b.is_shed());
        drop(a);
        assert!(!b.is_shed() && !c.is_shed() && !asking.is_shed());
        assert_eq!(storage.used(), 750);
        // Short again: the next goes.
        asking.reclaim(300);
        assert!(b.is_shed() && !c.is_shed() && !asking.is_shed());
    }

    /// The one asking is closed when it is itself the heaviest: it may be the one holding
    /// what the worker is short of.
    #[test]
    fn the_one_asking_is_closed_when_it_is_the_heaviest() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let (other, asking) = (table.open(), table.open());
        other.settle(100);
        asking.settle(800);
        asking.settle(1200);
        assert!(asking.is_shed() && !other.is_shed());
        assert_eq!(storage.used(), 1300);
        drop(asking);
        assert_eq!(storage.used(), 100);
    }

    /// Where no connection's charge, with the room left, could make the room the worker is
    /// short of, the shortage is held elsewhere and nobody is closed.
    #[test]
    fn nobody_is_closed_when_no_connection_could_make_the_room() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let (small, smaller) = (table.open(), table.open());
        small.settle(50);
        smaller.settle(20);
        let elsewhere = storage.reserve(900).unwrap();
        small.reclaim(500);
        assert!(!small.is_shed() && !smaller.is_shed());
        // Nothing of it lingers: a connection that could have made the room, opened after,
        // is closed only for a shortage of its own.
        drop(elsewhere);
        let heavy = table.open();
        heavy.settle(600);
        assert!(!heavy.is_shed());
    }

    /// h2 refusing an allocation for want of room makes the worker as short as a charge that
    /// does not fit: the connection charged the most that could make the room is closed.
    #[test]
    fn a_refusal_for_want_of_room_closes_the_heaviest() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let (heavy, light, asking) = (table.open(), table.open(), table.open());
        heavy.settle(700);
        light.settle(250);
        asking.reclaim(300);
        assert!(heavy.is_shed() && !light.is_shed() && !asking.is_shed());
        assert_eq!(storage.used(), 950, "a refusal charges nothing");
    }
}
