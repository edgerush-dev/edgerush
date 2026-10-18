//! The atomics the pooled entries are made of: the standard library's and
//! `portable-atomic`'s, or under `--cfg loom` loom's, so that its tests try every
//! interleaving of them (`loom_tests`).
//!
//! Loom has no 128-bit atomic. Under it the word is a value behind a loom mutex, so every
//! access to it is ordered as a lock orders it, which is stronger than the real word's
//! acquire and release: loom checks how the entries' other atomics — the waiters, the drain
//! flag, the cycle record, the request times — order around the word, not the word's own
//! orderings.

#[cfg(not(loom))]
pub(super) use portable_atomic::AtomicU128;
#[cfg(not(loom))]
pub(super) use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering, fence};

#[cfg(loom)]
pub(super) use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering, fence};

/// Under loom, the word: what the entries use of `AtomicU128`, behind a loom mutex.
#[cfg(loom)]
#[derive(Debug)]
pub(super) struct AtomicU128(loom::sync::Mutex<u128>);

#[cfg(loom)]
impl AtomicU128 {
    pub(super) fn new(value: u128) -> Self {
        Self(loom::sync::Mutex::new(value))
    }

    pub(super) fn is_lock_free() -> bool {
        true
    }

    pub(super) fn load(&self, _: Ordering) -> u128 {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn compare_exchange_weak(
        &self,
        current: u128,
        new: u128,
        _: Ordering,
        _: Ordering,
    ) -> Result<u128, u128> {
        let mut word = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *word == current {
            *word = new;
            Ok(current)
        } else {
            Err(*word)
        }
    }
}
