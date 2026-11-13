//! Every interleaving of the pooled entries' atomics, under loom
//! (`RUSTFLAGS="--cfg loom" cargo test --release -p edgerush-ratelimit --lib loom`).
//!
//! Loom has no 128-bit atomic, so the word is behind a loom mutex here ([`super::sync`]):
//! these check how a request's reads and charges, the cycle record and the signals order
//! around the word, not the word's own orderings, which the threaded tests in `tests`
//! exercise.

#![cfg(loom)]

use super::*;
use crate::service::{Contract, FixedWindow};
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

/// Two requests for a key's last room at once, each under it and another key with room:
/// read then charged, so both may be admitted, one past the limit; never both refused,
/// which would refuse one within it. Each admission is counted once on each of its keys.
#[test]
fn loom_requests_racing_for_the_last_room_over_admit_only() {
    loom::model(|| {
        let tight = Arc::new(Entry::new(b"t"[..].into(), contract(1, 1), CYCLE, 0));
        let roomy = Arc::new(Entry::new(
            b"r"[..].into(),
            contract(1_000, 1_000),
            CYCLE,
            0,
        ));
        let request = || {
            let (tight, roomy) = (Arc::clone(&tight), Arc::clone(&roomy));
            loom::thread::spawn(move || {
                u64::from(decide(&[&roomy, &tight], 0).local == Local::Admit)
            })
        };
        let (a, b) = (request(), request());
        let admitted = a.join().unwrap() + b.join().unwrap();
        assert!(admitted >= 1, "both refused, one within the limit");
        assert_eq!(tight.snapshot().unreported, admitted);
        assert_eq!(roomy.snapshot().unreported, admitted);
    });
}

/// One request under the last room's key alone, one under it and another: the one key's
/// compare-and-swap and the other's read then charge, at once. One at least is admitted.
#[test]
fn loom_one_key_and_two_racing_for_the_last_room_over_admit_only() {
    loom::model(|| {
        let tight = Arc::new(Entry::new(b"t"[..].into(), contract(1, 1), CYCLE, 0));
        let roomy = Arc::new(Entry::new(
            b"r"[..].into(),
            contract(1_000, 1_000),
            CYCLE,
            0,
        ));
        let alone = {
            let tight = Arc::clone(&tight);
            loom::thread::spawn(move || u64::from(decide(&[&tight], 0).local == Local::Admit))
        };
        let both = u64::from(decide(&[&roomy, &tight], 0).local == Local::Admit);
        let admitted = alone.join().unwrap() + both;
        assert!(admitted >= 1, "both refused, one within the limit");
        assert_eq!(tight.snapshot().unreported, admitted);
        assert_eq!(roomy.snapshot().unreported, both);
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
            0,
        ));
        let admit = |entry: &Arc<Entry>| {
            let entry = Arc::clone(entry);
            loom::thread::spawn(move || {
                let (taken, started) = entry.take(0);
                (u64::from(taken == Taken::Done), started)
            })
        };
        let (a, b) = (admit(&entry), admit(&entry));
        let (drained, _) = entry.drain(0);
        let (a, b) = (a.join().unwrap(), b.join().unwrap());
        let left = entry.snapshot().unreported;
        assert_eq!(drained + left, a.0 + b.0, "each admission counted once");
        assert!(left == 0 || a.1 || b.1, "{left} left, nothing signalled");
    });
}

/// Two workers starting a key's count, each claiming its signal for the queue to the
/// transport, while the transport takes what is queued — the signal first, then a visit
/// that drains the key: every admission is drained once or left in the count; a count left
/// has a signal queued for it, never one carried by a signal already taken; the key is
/// queued once at most; and it is claimed just while it is queued. It checks the order of
/// the steps, not the claim's orderings: what those keep out — a worker's change to the
/// word ordered after a drain it found the claim taken before — needs read-modify-writes
/// to take effect out of turn, which loom never tries, so it passes them relaxed too, as
/// it does with a loom atomic in the word's place ([`Entry::claim_signal`] says why they
/// are as they are).
#[test]
fn loom_a_signal_carried_by_one_on_its_way_reaches_the_transport() {
    loom::model(|| {
        let entry = Arc::new(Entry::new(
            b"k"[..].into(),
            contract(1_000, 1_000),
            CYCLE,
            0,
        ));
        let queue = Arc::new(loom::sync::Mutex::new(Vec::<Arc<Entry>>::new()));
        let admit = |entry: &Arc<Entry>, queue: &Arc<loom::sync::Mutex<Vec<Arc<Entry>>>>| {
            let (entry, queue) = (Arc::clone(entry), Arc::clone(queue));
            loom::thread::spawn(move || {
                let (taken, started) = entry.take(0);
                if started && entry.claim_signal() {
                    queue.lock().unwrap().push(Arc::clone(&entry));
                }
                u64::from(taken == Taken::Done)
            })
        };
        let (a, b) = (admit(&entry, &queue), admit(&entry, &queue));
        let taken = std::mem::take(&mut *queue.lock().unwrap());
        let mut drained = 0;
        for signalled in &taken {
            signalled.unclaim_signal();
            drained += signalled.drain(0).0;
        }
        let admitted = a.join().unwrap() + b.join().unwrap();
        let left = entry.snapshot().unreported;
        let queued = queue.lock().unwrap().len();
        assert_eq!(drained + left, admitted, "each admission counted once");
        assert!(left == 0 || queued == 1, "{left} left, no signal queued");
        assert!(queued <= 1, "queued {queued} times");
        assert_eq!(
            entry.signalled.load(Ordering::Acquire),
            queued == 1,
            "claimed while not queued, or queued unclaimed"
        );
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
            0,
        ));
        let (early, late) = (255 * CYCLE, 256 * CYCLE);
        let at = |entry: &Arc<Entry>, now| {
            let entry = Arc::clone(entry);
            loom::thread::spawn(move || entry.take(now).0 == Taken::Done)
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

/// A request on a window's entry racing the transport closing it for the next window (06 §4):
/// either admitted and drained once into the old window's report, or finding the entry closed
/// and asking; never admitted and left uncounted.
#[test]
fn loom_a_request_racing_a_windows_switch_is_counted_once_or_asks() {
    loom::model(|| {
        let n = |n| NonZeroU64::new(n).unwrap();
        let limit = Limit::FixedWindow(FixedWindow {
            limit: n(10),
            length: n(60_000 * MS),
        });
        let terms = Arc::new(Terms::new(limit, CYCLE, WHOLE_SHARE));
        let old = Arc::new(Entry::with_terms(
            b"w"[..].into(),
            terms,
            0,
            Some((1, 60_000 * MS)),
        ));
        let request = {
            let old = Arc::clone(&old);
            loom::thread::spawn(move || decide(&[&old], MS).local)
        };
        close(&old);
        let (drained, _) = old.drain(MS);
        let local = request.join().unwrap();
        match local {
            Local::Admit => assert_eq!(drained, 1, "admitted, drained once"),
            Local::Ask => assert_eq!(drained, 0),
            Local::Refuse => panic!("refused within the limit"),
        }
        assert_eq!(old.snapshot().unreported, 0, "nothing left behind");
    });
}
