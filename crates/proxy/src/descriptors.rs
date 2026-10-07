//! A worker's open files: one count of its client connections and its sockets to
//! upstreams, held to its share of the process's limit ([03 §9](../../docs/03-data-plane.md)).
//!
//! A worker's connection cap is sized from the open-file limit at two files a connection,
//! its own and one to its upstream; but its upstream sockets are bounded by other things —
//! its places for exchanges, its idle sockets, its HTTP/2 connections, its tunnels — so
//! within its cap it could still run the process out of files, and then accepting fails,
//! and connecting. So, as NGINX keeps one count of a worker's connections either way, every
//! client connection and every socket it opens to an upstream count against its share of
//! the files, the limit less the process's reserve, divided among the workers. A client
//! connection is counted whatever: its cap already holds it to half the share. An upstream
//! socket is opened only within the share: past it, one of the worker's idle sockets is
//! closed to make room, and with none to close the socket is not opened, the worker's own
//! shortage (14 §8) rather than the endpoint's. A worker with no limit to keep to counts
//! nothing.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

/// What makes room among a worker's files: closes one of its idle sockets, and says
/// whether it did.
type Evict = Box<dyn Fn() -> bool>;

/// A worker's open files, counted against its share.
pub(crate) struct Descriptors {
    /// Its share; none where the process has no limit, and nothing is counted.
    share: Option<usize>,
    held: Cell<usize>,
    evict: RefCell<Option<Evict>>,
}

impl std::fmt::Debug for Descriptors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Descriptors")
            .field("share", &self.share)
            .field("held", &self.held.get())
            .finish_non_exhaustive()
    }
}

impl Descriptors {
    /// A worker's files, held to `share` if it has one.
    #[must_use]
    pub(crate) fn new(share: Option<usize>) -> Rc<Self> {
        Rc::new(Self {
            share,
            held: Cell::new(0),
            evict: RefCell::new(None),
        })
    }

    /// What closes one of the worker's idle sockets when an upstream socket would go past
    /// its share.
    pub(crate) fn evicting(&self, evict: impl Fn() -> bool + 'static) {
        *self.evict.borrow_mut() = Some(Box::new(evict));
    }

    /// A client connection's file, counted whatever: the connection cap bounds them.
    pub(crate) fn count(self: &Rc<Self>) -> Descriptor {
        if self.share.is_none() {
            return Descriptor(None);
        }
        self.held.set(self.held.get() + 1);
        Descriptor(Some(Rc::clone(self)))
    }

    /// An upstream socket's file, if the share has room for it, one of the worker's idle
    /// sockets closed first where it has none.
    pub(crate) fn take(self: &Rc<Self>) -> Option<Descriptor> {
        let Some(share) = self.share else {
            return Some(Descriptor(None));
        };
        if self.held.get() >= share {
            // Closed outside the borrow: a socket closed gives its file back here.
            let evicted = self
                .evict
                .try_borrow()
                .is_ok_and(|evict| evict.as_ref().is_some_and(|evict| evict()));
            if !evicted || self.held.get() >= share {
                return None;
            }
        }
        self.held.set(self.held.get() + 1);
        Some(Descriptor(Some(Rc::clone(self))))
    }

    /// How many files the worker holds, as counted.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.held.get()
    }
}

/// A file counted against a worker's share, given back when this is dropped.
#[derive(Debug)]
pub(crate) struct Descriptor(Option<Rc<Descriptors>>);

impl Descriptor {
    /// A file counted against nothing, for what is not a worker's.
    pub(crate) fn uncounted() -> Self {
        Self(None)
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        if let Some(descriptors) = &self.0 {
            descriptors
                .held
                .set(descriptors.held.get().saturating_sub(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_share_counts_nothing_and_refuses_nothing() {
        let files = Descriptors::new(None);
        let held: Vec<_> = (0..10).map(|_| files.take().unwrap()).collect();
        assert_eq!(files.held(), 0);
        drop(held);
    }

    /// Clients are counted whatever; upstream sockets only within the share, an idle one
    /// closed to make room, and none opened when there is none to close.
    #[test]
    fn upstream_sockets_are_held_to_the_share_clients_and_all() {
        let files = Descriptors::new(Some(3));
        let client = files.count();
        let first = files.take().unwrap();
        let idle = Rc::new(RefCell::new(Some(files.take().unwrap())));
        assert_eq!(files.held(), 3);
        let evicting = Rc::clone(&idle);
        files.evicting(move || evicting.borrow_mut().take().is_some());
        // Past the share: the idle socket is closed to make room.
        let second = files.take().expect("room made");
        assert!(idle.borrow().is_none());
        // Nothing idle left: refused.
        assert!(files.take().is_none());
        assert_eq!(files.held(), 3);
        // Clients are counted past it: their cap bounds them.
        let another = files.count();
        assert_eq!(files.held(), 4);
        drop((client, first, second, another));
        assert_eq!(files.held(), 0);
    }
}
