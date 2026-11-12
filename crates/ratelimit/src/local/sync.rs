//! What local mode's limiter is made of: the standard library's atomics, lock and `Arc`, and
//! `portable-atomic`'s 128-bit atomic for a key's word, or under `--cfg loom` loom's, so that
//! its tests try every interleaving of them (`loom_tests`). Loom has no 128-bit atomic: under
//! it a key's word is a value behind a loom mutex, as the pooled entries' is, so loom checks
//! how the registry and the handles order around the word, not the word's own orderings.

#[cfg(not(loom))]
pub(super) use portable_atomic::AtomicU128;
#[cfg(not(loom))]
pub(super) use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
pub(super) use std::sync::{Arc, RwLock};

#[cfg(loom)]
pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(loom)]
pub(super) use loom::sync::{Arc, RwLock};

/// Under loom, a key's word: what local mode uses of `AtomicU128`, behind a loom mutex.
#[cfg(loom)]
#[derive(Debug)]
pub(super) struct AtomicU128(loom::sync::Mutex<u128>);

#[cfg(loom)]
impl AtomicU128 {
    pub(super) fn new(value: u128) -> Self {
        Self(loom::sync::Mutex::new(value))
    }

    fn lock(&self) -> loom::sync::MutexGuard<'_, u128> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn load(&self, _: Ordering) -> u128 {
        *self.lock()
    }

    pub(super) fn compare_exchange(
        &self,
        current: u128,
        new: u128,
        _: Ordering,
        _: Ordering,
    ) -> Result<u128, u128> {
        let mut word = self.lock();
        if *word == current {
            *word = new;
            Ok(current)
        } else {
            Err(*word)
        }
    }

    pub(super) fn fetch_or(&self, value: u128, _: Ordering) -> u128 {
        let mut word = self.lock();
        let was = *word;
        *word |= value;
        was
    }
}
