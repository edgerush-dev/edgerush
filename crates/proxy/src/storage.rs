//! What one worker holds for requests, and how much it may
//! ([14 §8](../../../docs/14-downstream-server.md)).
//!
//! Every allocation a worker makes for requests on either hop — the blocks heads and bodies
//! are read into, what is staged to be written, frames copied out — is paid for here before
//! it is made, and stays paid for as long as it lives. A payment is a [`Charge`], which gives
//! its bytes back when it is dropped, so that storage and what it is charged cannot part: a
//! charge held beside the allocation it pays for goes when the allocation does. Growth
//! reserves the new size before the old charge goes, because for a moment both are live.
//!
//! A reservation that would pass the limit fails at once, once memory freed since the last
//! sweep has been looked for. Nothing here waits for memory to come free, and what a
//! refusal means — closing a connection, cancelling an exchange — is the caller's to decide
//! (14 §8).
//!
//! Memory its owner lets go of is not always gone: a frame cut from a block shares the
//! block's memory, and holds all of it for as long as the frame lives. So an owner that lets
//! go of memory something else still holds a piece of hands it here with its charge
//! ([`Charge::outlive`]), and the charge goes only when a [`Storage::sweep`] finds nothing
//! else holding it — the one moment this end can learn that the last piece has gone
//! (14 §3). The worker sweeps once a second, and a reservation that would not fit sweeps
//! first: between two sweeps a worker streaming fast frees far more than its limit.
//!
//! One per worker, and it never leaves it. Nothing here does I/O or reads a clock.

#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use bytes::BytesMut;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

/// What a worker may hold for requests unless it is told otherwise: 14 §8's proposal, to
/// be checked against measurement before it is adopted.
pub const LIMIT: usize = 256 * 1024 * 1024;

/// What a worker's own answers and refusals may draw on above the limit, and nothing else:
/// enough for a worker that has run out to say so rather than close (14 §8).
pub const PROVISION: usize = 1024 * 1024;

/// One worker's account of the storage it holds for requests.
#[derive(Debug)]
pub struct Storage {
    limit: usize,
    /// How far past `limit` a charge for the worker's own answers may go.
    provision: usize,
    used: Cell<usize>,
    /// Memory its owner let go of while something else still held a piece of it, and what
    /// it is charged.
    outlived: RefCell<Vec<(BytesMut, usize)>>,
}

/// A reservation refused: it would have taken the worker past its limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{wanted} bytes more would take the worker past its limit of {limit}, with {used} held")]
pub struct Exhausted {
    /// What was asked for.
    pub wanted: usize,
    /// What was held when it was asked.
    pub used: usize,
    /// What the worker may hold.
    pub limit: usize,
}

impl Storage {
    /// An account that may hold up to `limit` bytes, and [`PROVISION`] more for the worker's
    /// own answers, holding none.
    pub fn new(limit: usize) -> Rc<Self> {
        Self::with_provision(limit, PROVISION)
    }

    /// The same, with `provision` for the worker's own answers.
    pub fn with_provision(limit: usize, provision: usize) -> Rc<Self> {
        Rc::new(Self {
            limit,
            provision,
            used: Cell::new(0),
            outlived: RefCell::new(Vec::new()),
        })
    }

    /// Releases the charges of memory handed over by [`Charge::outlive`] that nothing else
    /// holds any longer, and drops that memory. For the worker's once-a-second sweep, and
    /// for a reservation that would otherwise be refused.
    pub fn sweep(&self) {
        let mut released = 0;
        self.outlived.borrow_mut().retain_mut(|(memory, bytes)| {
            let alone = held_by_nothing_else(memory, *bytes);
            if alone {
                released += *bytes;
            }
            !alone
        });
        self.used.set(self.used.get().saturating_sub(released));
    }

    /// How many pieces of memory are waiting on something else to let go of them.
    #[cfg(test)]
    pub fn outlived(&self) -> usize {
        self.outlived.borrow().len()
    }

    /// What it holds: the bytes of every charge not yet dropped, and of memory outlived.
    pub fn used(&self) -> usize {
        self.used.get()
    }

    /// Whether a byte more would fit under the limit, memory freed since the last sweep
    /// included. Asking charges nothing.
    pub fn has_room(&self) -> bool {
        let room = self.take(1, Ceiling::Limit).is_ok();
        if room {
            self.give(1);
        }
        room
    }

    /// Reserves `bytes`, if they fit under the limit with everything already held.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if they do not, or if the count would overflow; nothing is charged.
    pub fn reserve(self: &Rc<Self>, bytes: usize) -> Result<Charge, Exhausted> {
        self.charge(bytes, Ceiling::Limit)
    }

    /// Reserves `bytes` for the worker's own answer or refusal, which may go past the limit
    /// as far as the provision allows. Nothing else may use it: while such a charge takes
    /// the account past its limit, [`Storage::reserve`] refuses everything.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if they do not fit under the limit and the provision together.
    pub fn reserve_answer(self: &Rc<Self>, bytes: usize) -> Result<Charge, Exhausted> {
        self.charge(bytes, Ceiling::Provision)
    }

    fn charge(self: &Rc<Self>, bytes: usize, ceiling: Ceiling) -> Result<Charge, Exhausted> {
        self.take(bytes, ceiling)?;
        Ok(Charge {
            storage: Rc::clone(self),
            bytes,
            ceiling,
        })
    }

    /// Counts `bytes` more as held, if they fit under `ceiling`.
    fn take(&self, bytes: usize, ceiling: Ceiling) -> Result<(), Exhausted> {
        let limit = match ceiling {
            Ceiling::Limit => self.limit,
            Ceiling::Provision => self.limit.saturating_add(self.provision),
        };
        let fits = |used: usize| used.checked_add(bytes).filter(|&total| total <= limit);
        let mut used = self.used.get();
        if fits(used).is_none() && !self.outlived.borrow().is_empty() {
            // Memory outlived may have been freed since the last sweep: a worker streaming
            // fast frees far more than its limit in the time between two. Looked for only
            // here, where the answer would otherwise be a refusal.
            self.sweep();
            used = self.used.get();
        }
        match fits(used) {
            Some(total) => {
                self.used.set(total);
                Ok(())
            }
            None => Err(Exhausted {
                wanted: bytes,
                used,
                limit,
            }),
        }
    }

    /// Counts `bytes` fewer as held. Never short: only a charge gives back, and only what
    /// it took.
    fn give(&self, bytes: usize) {
        let used = self.used.get();
        debug_assert!(bytes <= used, "{bytes} released of {used}");
        self.used.set(used.saturating_sub(bytes));
    }
}

/// Bytes reserved against a worker's [`Storage`], held until this is dropped.
#[derive(Debug)]
#[must_use = "a charge dropped at once pays for nothing"]
pub struct Charge {
    storage: Rc<Storage>,
    bytes: usize,
    /// How far it may grow.
    ceiling: Ceiling,
}

/// Which of an account's two ceilings a charge is made under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ceiling {
    /// The limit, which everything is held to.
    Limit,
    /// The limit and the provision above it, for the worker's own answers alone.
    Provision,
}

impl Charge {
    /// Reserves `more` besides, for storage that is growing; on a refusal nothing changes.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if the worker cannot pay for it.
    pub fn grow(&mut self, more: usize) -> Result<(), Exhausted> {
        self.storage.take(more, self.ceiling)?;
        self.bytes += more;
        Ok(())
    }

    /// Gives back `less` of what it reserves, for storage that has shrunk, and no more than
    /// it reserves.
    pub fn shrink(&mut self, less: usize) {
        let less = less.min(self.bytes);
        self.storage.give(less);
        self.bytes -= less;
    }

    /// What it reserves.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Lets go of `memory`, which this pays for, keeping its charge if anything else still
    /// holds a piece of it. The charge is then left with the account until a
    /// [`Storage::sweep`] finds the memory held by nothing else, and this charges nothing
    /// from now on.
    ///
    /// `memory` must be the whole of what this pays for, or what is left of it after pieces
    /// were split off: whether anything else holds it is learned by trying to take back as
    /// much of it as this was charged.
    pub fn outlive(&mut self, mut memory: BytesMut) {
        if !held_by_nothing_else(&mut memory, self.bytes) {
            let bytes = std::mem::take(&mut self.bytes);
            self.storage.outlived.borrow_mut().push((memory, bytes));
        }
    }
}

/// Whether `memory` is all its owner's: whether all `size` bytes of it can be taken back.
/// Empties it, as nothing in memory being let go of is anyone's.
fn held_by_nothing_else(memory: &mut BytesMut, size: usize) -> bool {
    memory.clear();
    memory.try_reclaim(size)
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.storage.give(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_charge_holds_its_bytes_until_it_is_dropped() {
        let storage = Storage::new(100);
        let first = storage.reserve(30).unwrap();
        let second = storage.reserve(50).unwrap();
        assert_eq!((first.bytes(), second.bytes()), (30, 50));
        assert_eq!(storage.used(), 80);
        drop(first);
        assert_eq!(storage.used(), 50);
        drop(second);
        assert_eq!(storage.used(), 0);
    }

    /// Up to the limit exactly, and not a byte past it; a refusal charges nothing and says
    /// what was asked against what.
    #[test]
    fn a_reservation_past_the_limit_is_refused_and_charges_nothing() {
        let storage = Storage::new(100);
        let _held = storage.reserve(60).unwrap();
        assert_eq!(
            storage.reserve(41).unwrap_err(),
            Exhausted {
                wanted: 41,
                used: 60,
                limit: 100
            }
        );
        assert_eq!(storage.used(), 60);
        let _rest = storage.reserve(40).unwrap();
        assert_eq!(storage.used(), 100);
        assert!(storage.reserve(1).is_err());
    }

    /// A full account has no room for a byte more, and one below its limit has; asking
    /// charges nothing.
    #[test]
    fn a_full_account_has_no_room() {
        let storage = Storage::new(100);
        assert!(storage.has_room());
        let held = storage.reserve(100).unwrap();
        assert!(!storage.has_room());
        assert_eq!(storage.used(), 100);
        drop(held);
        assert!(storage.has_room());
        assert_eq!(storage.used(), 0);
    }

    /// A count that would wrap is refused, not wrapped into a small number that fits.
    #[test]
    fn a_reservation_that_would_overflow_the_count_is_refused() {
        let storage = Storage::new(usize::MAX);
        let _held = storage.reserve(usize::MAX - 1).unwrap();
        assert!(storage.reserve(2).is_err());
        assert_eq!(storage.used(), usize::MAX - 1);
    }

    /// Growing reserves the new size while the old is still held, because both allocations
    /// are live until the copy is made: a worker near its limit cannot grow into it.
    #[test]
    fn growth_is_paid_for_before_the_old_charge_goes() {
        let storage = Storage::new(100);
        let old = storage.reserve(40).unwrap();
        assert!(storage.reserve(64).is_err(), "grew past the limit");
        let new = storage.reserve(60).unwrap();
        drop(old);
        assert_eq!(storage.used(), new.bytes());
    }

    /// The provision above the limit is for the worker's own answers alone: an ordinary
    /// reservation stops at the limit, one for an answer goes on to the provision's end,
    /// and while it is out nothing ordinary fits. Each charge grows only as far as the
    /// ceiling it was made under.
    #[test]
    fn the_provision_is_for_the_workers_own_answers_alone() {
        let storage = Storage::with_provision(100, 50);
        let mut ordinary = storage.reserve(100).unwrap();
        assert!(storage.reserve(1).is_err());
        assert!(
            ordinary.grow(1).is_err(),
            "an ordinary charge grew past the limit"
        );
        let mut answer = storage.reserve_answer(40).unwrap();
        assert_eq!(storage.used(), 140);
        assert!(answer.grow(10).is_ok());
        assert!(answer.grow(1).is_err(), "grew past the provision");
        assert!(storage.reserve_answer(1).is_err());
        drop(ordinary);
        assert!(
            storage.reserve(49).is_ok(),
            "the limit counts the answer's charge"
        );
        assert!(
            storage.reserve(51).is_err(),
            "the limit counts the answer's charge"
        );
        drop(answer);
        assert_eq!(storage.used(), 0);
    }

    /// Memory let go of while a piece of it is held elsewhere keeps its charge, through any
    /// number of sweeps, until the piece has gone; memory let go of whole releases it at
    /// once.
    #[test]
    fn memory_outlived_by_a_piece_of_it_stays_charged_until_a_sweep_finds_it_free() {
        let storage = Storage::new(1024);
        let mut charge = storage.reserve(64).unwrap();
        let mut memory = BytesMut::zeroed(64);
        let piece = memory.split_to(16).freeze();
        charge.outlive(memory);
        drop(charge);
        assert_eq!((storage.used(), storage.outlived()), (64, 1));
        storage.sweep();
        assert_eq!(storage.used(), 64, "released while a piece was held");
        drop(piece);
        storage.sweep();
        assert_eq!((storage.used(), storage.outlived()), (0, 0));

        let mut whole = storage.reserve(64).unwrap();
        whole.outlive(BytesMut::zeroed(64));
        drop(whole);
        assert_eq!((storage.used(), storage.outlived()), (0, 0));
    }

    /// Memory freed since the last sweep is not held against a reservation: one that would
    /// not fit looks again before it is refused. Between sweeps a worker streaming fast can
    /// free far more than its limit, and refusing then fails answers for memory nothing
    /// holds.
    #[test]
    fn a_reservation_that_does_not_fit_sweeps_before_it_is_refused() {
        let storage = Storage::new(64);
        let mut charge = storage.reserve(64).unwrap();
        let mut memory = BytesMut::zeroed(64);
        let piece = memory.split_to(16).freeze();
        charge.outlive(memory);
        drop(charge);
        assert!(storage.reserve(1).is_err(), "refused while a piece is held");
        let mut growing = storage.reserve_answer(0).unwrap();
        assert!(
            growing.grow(1).is_ok(),
            "the provision is there all the same"
        );
        drop(growing);

        drop(piece);
        let again = storage
            .reserve(64)
            .expect("nothing holds the memory any more");
        assert_eq!((storage.used(), storage.outlived()), (64, 0));
        drop(again);

        // And growing looks again too.
        let mut charge = storage.reserve(48).unwrap();
        let mut memory = BytesMut::zeroed(48);
        let piece = memory.split_to(16).freeze();
        charge.outlive(memory);
        drop(charge);
        let mut growing = storage.reserve(16).unwrap();
        drop(piece);
        assert!(
            growing.grow(48).is_ok(),
            "grown into memory freed since the sweep"
        );
    }

    /// What a test does to an account.
    #[derive(Debug, Clone)]
    enum Step {
        Reserve(usize),
        /// Drops the live charge at this position, counted around those there are.
        Drop(usize),
        /// Grows the live charge at this position by this much.
        Grow(usize, usize),
        /// Shrinks the live charge at this position by this much.
        Shrink(usize, usize),
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            (0usize..=64).prop_map(Step::Reserve),
            any::<usize>().prop_map(Step::Drop),
            (any::<usize>(), 0usize..=64).prop_map(|(at, more)| Step::Grow(at, more)),
            (any::<usize>(), 0usize..=80).prop_map(|(at, less)| Step::Shrink(at, less)),
        ]
    }

    proptest! {
        /// Against the plainest account there is — a list of what is held — whatever is
        /// reserved and dropped in whatever order: a reservation succeeds exactly when it
        /// fits, what is held is the sum of the charges alive, and it never passes the
        /// limit.
        #[test]
        fn the_account_is_the_sum_of_the_charges_alive(
            limit in 0usize..=256,
            steps in proptest::collection::vec(step(), 0..64),
        ) {
            let storage = Storage::new(limit);
            let mut alive: Vec<Charge> = Vec::new();
            for step in steps {
                match step {
                    Step::Reserve(bytes) => {
                        let held: usize = alive.iter().map(Charge::bytes).sum();
                        let fits = held + bytes <= limit;
                        match storage.reserve(bytes) {
                            Ok(charge) => {
                                prop_assert!(fits, "{bytes} granted with {held} of {limit}");
                                alive.push(charge);
                            }
                            Err(refused) => {
                                prop_assert!(!fits, "{bytes} refused with {held} of {limit}");
                                prop_assert_eq!(refused.used, held);
                            }
                        }
                    }
                    Step::Drop(at) if !alive.is_empty() => {
                        let at = at % alive.len();
                        drop(alive.swap_remove(at));
                    }
                    Step::Grow(at, more) if !alive.is_empty() => {
                        let held: usize = alive.iter().map(Charge::bytes).sum();
                        let at = at % alive.len();
                        let before = alive[at].bytes();
                        let fits = held + more <= limit;
                        prop_assert_eq!(alive[at].grow(more).is_ok(), fits);
                        let after = if fits { before + more } else { before };
                        prop_assert_eq!(alive[at].bytes(), after);
                    }
                    Step::Shrink(at, less) if !alive.is_empty() => {
                        let at = at % alive.len();
                        let before = alive[at].bytes();
                        alive[at].shrink(less);
                        prop_assert_eq!(alive[at].bytes(), before - less.min(before));
                    }
                    Step::Drop(_) | Step::Grow(..) | Step::Shrink(..) => {}
                }
                let held: usize = alive.iter().map(Charge::bytes).sum();
                prop_assert_eq!(storage.used(), held);
                prop_assert!(storage.used() <= limit);
            }
            drop(alive);
            prop_assert_eq!(storage.used(), 0);
        }
    }
}
