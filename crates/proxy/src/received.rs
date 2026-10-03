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
//! given back as credit. A charge that does not fit closes the connection charged the
//! most, which lets go of its charge at once, and is tried again, as the HTTP/3 server
//! does with what quiche holds (16 §6); if no other holds anything, the storage is other
//! work's, and the connection that cannot be paid for is the one closed.
//!
//! The charge follows what came, since h2 takes a frame in before anyone can refuse it: a
//! turn reads no more than tokio's budget lets it, and never past the windows.
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
        })
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
    /// Charges the connection what h2 says it holds now, `held`. Where the worker cannot
    /// pay for it, the connection charged the most is closed and the charge tried again,
    /// until it fits or this one is closed.
    pub fn settle(&self, held: usize) {
        let holding = &self.holding;
        while !holding.shed.get() && holding.charge_to(&self.table.storage, held).is_err() {
            let heaviest = self
                .table
                .open
                .borrow()
                .values()
                .filter(|other| !other.shed.get())
                .max_by_key(|other| other.charged.get())
                .filter(|other| other.charged.get() > 0)
                .cloned();
            heaviest.as_ref().unwrap_or(holding).shed();
        }
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
        self.charge.borrow_mut().take();
        self.charged.set(0);
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

    /// Where the worker cannot pay, the connection charged the most is closed, lets go of its
    /// charge and is woken, and the one asking is charged.
    #[test]
    fn at_the_limit_the_heaviest_other_is_closed() {
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
        assert_eq!(heavy.charged(), 0);
        assert_eq!(storage.used(), 600);
        // Closed, it is charged nothing more.
        heavy.settle(900);
        assert_eq!(storage.used(), 600);
    }

    /// The one asking is closed where it is itself the heaviest, or where nothing else holds
    /// anything here and the storage is other work's.
    #[test]
    fn the_one_asking_is_closed_when_it_is_the_heaviest_or_alone() {
        let storage = Storage::with_provision(1000, 0);
        let table = Received::new(Rc::clone(&storage));
        let (other, asking) = (table.open(), table.open());
        other.settle(100);
        asking.settle(800);
        asking.settle(1200);
        assert!(asking.is_shed() && !other.is_shed());
        assert_eq!(storage.used(), 100);

        // Nothing held here any more; everything held by other work.
        other.settle(0);
        let elsewhere = storage.reserve(1000).unwrap();
        let alone = table.open();
        alone.settle(50);
        assert!(alone.is_shed(), "charged past the limit");
        assert!(!other.is_shed(), "closed for storage it does not hold");
        drop(elsewhere);
    }
}
