//! Every interleaving of the pooled entries' atomics, under loom
//! (`RUSTFLAGS="--cfg loom" cargo test --release -p edgerush-ratelimit --lib loom`).
//!
//! Loom has no 128-bit atomic, so the word is behind a loom mutex here ([`super::sync`]):
//! these check how the waiters, the cycle record and the signals order around the word,
//! not the word's own orderings, which the threaded tests in `tests` exercise.

#![cfg(loom)]

use super::*;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;

/// `limit` a second, a bucket of `burst`.
fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(1_000 * MS),
        burst: n(burst),
    }
}

/// The handshake between a request deferred on a key and whoever changes the key's word
/// next ([`wait_on`], [`waited_on`]), with a loom atomic in the word's place used as the
/// word is: the request's look at it a plain acquire load, as a refusal is; the change a
/// compare-and-swap. Either the request sees the change, or the changer sees it waiting
/// and wakes it — never neither. (The word itself is a lock under loom, which would order
/// the two however the handshake was written.)
#[test]
fn loom_a_change_to_the_word_wakes_or_reaches_its_waiter() {
    loom::model(|| {
        let word = Arc::new(AtomicU64::new(0));
        let waiters = Arc::new(AtomicU64::new(0));
        let waiter = {
            let (word, waiters) = (Arc::clone(&word), Arc::clone(&waiters));
            loom::thread::spawn(move || {
                wait_on(&waiters);
                word.load(Ordering::Acquire)
            })
        };
        let changed = word
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        let wake = waited_on(&waiters);
        let seen = waiter.join().unwrap();
        assert!(changed);
        assert!(seen == 1 || wake, "the change unseen and nobody woken");
    });
}

/// A request deferred on a key's outstanding hold, the hold given back at once: found or
/// woken, through the entry's own calls.
#[test]
fn loom_a_hold_given_back_wakes_or_admits_its_waiter() {
    loom::model(|| {
        let entry = Arc::new(Entry::new(b"k"[..].into(), contract(1, 1), CYCLE, false, 0));
        assert!(entry.take(Op::Hold, 0).0.done(), "the last token held");
        let waiter = {
            let entry = Arc::clone(&entry);
            loom::thread::spawn(move || {
                entry.add_waiter();
                entry.take(Op::Admit, 0).0
            })
        };
        let (wake, _) = entry.settle(false, 0);
        let taken = waiter.join().unwrap();
        assert!(taken.done() || wake, "{taken:?}, nobody woken");
    });
}

/// Two admissions and a drain at once: every admission is drained once or left in the
/// count, and a count left behind was started by an admission that signalled it, so that
/// a scheduled transport visits the key.
#[test]
fn loom_admissions_racing_a_drain_are_counted_once_and_a_count_left_is_signalled() {
    loom::model(|| {
        let entry = Arc::new(Entry::new(
            b"k"[..].into(),
            contract(1_000, 1_000),
            CYCLE,
            false,
            0,
        ));
        let admit = |entry: &Arc<Entry>| {
            let entry = Arc::clone(entry);
            loom::thread::spawn(move || {
                let (taken, started) = entry.take(Op::Admit, 0);
                (u64::from(taken.done()), started)
            })
        };
        let (a, b) = (admit(&entry), admit(&entry));
        let (drained, _) = entry.drain(0, Cycle::AsItIs);
        let (a, b) = (a.join().unwrap(), b.join().unwrap());
        let left = entry.snapshot().unreported;
        assert_eq!(drained + left, a.0 + b.0, "each admission counted once");
        assert!(left == 0 || a.1 || b.1, "{left} left, nothing signalled");
    });
}

/// Two requests whose clocks read either side of a cycle boundary, at once, the later in a
/// new 256-cycle step of the record: the word ends in the later cycle, never moved back by
/// the earlier, and the record matches the cycle the word holds — never ahead of it.
#[test]
fn loom_two_clocks_leave_the_record_with_the_words_cycle() {
    loom::model(|| {
        let entry = Arc::new(Entry::new(
            b"k"[..].into(),
            contract(1_000, 1_000),
            CYCLE,
            false,
            0,
        ));
        let (early, late) = (255 * CYCLE, 256 * CYCLE);
        let at = |entry: &Arc<Entry>, now| {
            let entry = Arc::clone(entry);
            loom::thread::spawn(move || entry.take(Op::Admit, now).0.done())
        };
        let (a, b) = (at(&entry, early), at(&entry, late));
        assert!(a.join().unwrap() && b.join().unwrap());
        let fields = entry.snapshot();
        assert_eq!(fields.cycle, word::cycle_of(late, CYCLE), "moved back");
        assert_eq!(
            entry.record.load(Ordering::Acquire),
            word::record_of(late, CYCLE)
        );
    });
}
