//! Every interleaving of local mode's limiter, under loom
//! (`RUSTFLAGS="--cfg loom" cargo test --release -p edgerush-ratelimit --lib loom`): workers
//! racing on a key's last room, requests under two limits, and the sweep closing a key a
//! request is using; each for a bucket and for a fixed window.

#![cfg(loom)]

use super::*;
use crate::service::FixedState;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;

/// Every interleaving of `body` with at most three preemptions: enough for the races
/// below, two threads and a handful of steps each, and done in seconds rather than hours.
fn model(body: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(body);
}

/// `limit` a second, a bucket of `burst`.
fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(1_000 * MS),
        burst: n(burst),
    }
}

/// `limit` in each second's window.
fn window(limit: u64) -> Window {
    Window {
        limit: NonZeroU64::new(limit).unwrap(),
        window: NonZeroU64::new(1_000 * MS).unwrap(),
    }
}

/// Decides, on a thread of its own with handles of its own, a request under `keys`.
fn request<L: core::marker::Copy + Into<Limit> + Send + 'static>(
    local: &Arc<Local<FixedState>>,
    keys: &'static [&'static [u8]],
    c: L,
    now: u64,
) -> loom::thread::JoinHandle<bool> {
    let local = Arc::clone(local);
    loom::thread::spawn(move || {
        let mut handles = Handles::new(FixedState::default());
        let keys: Vec<(&[u8], L)> = keys.iter().map(|key| (*key, c)).collect();
        local.decide(&mut handles, &keys, now) == Decision::Admit
    })
}

/// Two workers asking at once for a key's one request of room: exactly one gets it.
#[test]
fn loom_the_last_room_goes_to_one_of_two() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 2));
        let one = request(&local, &[b"k"], contract(1, 1), 0);
        let other = request(&local, &[b"k"], contract(1, 1), 0);
        let admitted = [one.join().unwrap(), other.join().unwrap()];
        assert_eq!(admitted.iter().filter(|a| **a).count(), 1, "{admitted:?}");
    });
}

/// Two workers asking at once for the one request of room of a key gone idle, its bucket
/// full again: exactly one gets it. Neither may take the key for idle from its last use
/// while the other's charge is already in its word.
#[test]
fn loom_an_idle_keys_one_room_goes_to_one_of_two() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 2));
        let c = contract(1, 1);
        let mut handles = Handles::new(FixedState::default());
        assert_eq!(local.decide(&mut handles, &[(b"k", c)], 0), Decision::Admit);
        let one = request(&local, &[b"k"], c, 2_000 * MS);
        let other = request(&local, &[b"k"], c, 2_000 * MS);
        let admitted = [one.join().unwrap(), other.join().unwrap()];
        assert_eq!(admitted.iter().filter(|a| **a).count(), 1, "{admitted:?}");
    });
}

/// Two requests under the same two limits, each with room for one, decided in opposite
/// orders: one admitted, or both, a request too many — never neither, which would refuse one
/// within its limits, as charging each limit in turn and giving back on a refusal could.
#[test]
fn loom_two_limits_racing_never_refuse_both() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 2));
        let one = request(&local, &[b"a", b"b"], contract(1, 1), 0);
        let other = request(&local, &[b"b", b"a"], contract(1, 1), 0);
        let admitted = [one.join().unwrap(), other.join().unwrap()];
        assert!(admitted.iter().any(|a| *a), "both refused");
    });
}

/// The sweep forgetting a full, quiet key while a worker that holds a handle to it decides
/// a request: the request is admitted, on the old key before it closed or on its
/// successor, and what the registry counts is what it holds.
#[test]
fn loom_a_key_forgotten_under_a_request_admits_it() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 1));
        let c = contract(1, 1);
        let mut handles = Handles::new(FixedState::default());
        assert_eq!(local.decide(&mut handles, &[(b"k", c)], 0), Decision::Admit);
        let worker = {
            let local = Arc::clone(&local);
            loom::thread::spawn(move || {
                local.decide(&mut handles, &[(b"k", c)], 2_000 * MS) == Decision::Admit
            })
        };
        local.sweep(2_000 * MS, 500 * MS);
        assert!(worker.join().unwrap(), "refused though full");
        let held = u64::try_from(local.key_count()).unwrap() * key_bytes(b"k", 1);
        assert_eq!(local.holding(), held);
    });
}

/// A window's last room asked for by two workers at once: exactly one gets it.
#[test]
fn loom_a_windows_last_room_goes_to_one_of_two() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 2));
        let one = request(&local, &[b"k"], window(1), 0);
        let other = request(&local, &[b"k"], window(1), 0);
        let admitted = [one.join().unwrap(), other.join().unwrap()];
        assert_eq!(admitted.iter().filter(|a| **a).count(), 1, "{admitted:?}");
    });
}

/// The same for a window gone idle, a fresh one's room: neither worker may take the key for
/// idle from its last use while the other's charge is already in its word.
#[test]
fn loom_an_idle_windows_one_room_goes_to_one_of_two() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 2));
        let mut handles = Handles::new(FixedState::default());
        let w = window(1);
        assert_eq!(local.decide(&mut handles, &[(b"k", w)], 0), Decision::Admit);
        let one = request(&local, &[b"k"], w, 2_000 * MS);
        let other = request(&local, &[b"k"], w, 2_000 * MS);
        let admitted = [one.join().unwrap(), other.join().unwrap()];
        assert_eq!(admitted.iter().filter(|a| **a).count(), 1, "{admitted:?}");
    });
}

/// Two requests under the same two windows, decided in opposite orders: never both refused.
#[test]
fn loom_two_windows_racing_never_refuse_both() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 2));
        let one = request(&local, &[b"a", b"b"], window(1), 0);
        let other = request(&local, &[b"b", b"a"], window(1), 0);
        let admitted = [one.join().unwrap(), other.join().unwrap()];
        assert!(admitted.iter().any(|a| *a), "both refused");
    });
}

/// The sweep forgetting a window that counts none while a worker decides a request under
/// it: the request is admitted, and what the registry counts is what it holds.
#[test]
fn loom_a_window_forgotten_under_a_request_admits_it() {
    model(|| {
        let local = Arc::new(Local::new(FixedState::default(), 1));
        let w = window(1);
        let mut handles = Handles::new(FixedState::default());
        assert_eq!(local.decide(&mut handles, &[(b"k", w)], 0), Decision::Admit);
        let worker = {
            let local = Arc::clone(&local);
            loom::thread::spawn(move || {
                local.decide(&mut handles, &[(b"k", w)], 2_000 * MS) == Decision::Admit
            })
        };
        local.sweep(2_000 * MS, 500 * MS);
        assert!(worker.join().unwrap(), "refused though counting none");
        let held = u64::try_from(local.key_count()).unwrap() * key_bytes(b"k", 1);
        assert_eq!(local.holding(), held);
    });
}
