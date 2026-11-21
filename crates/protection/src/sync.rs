//! What a pod's count and its backend's queue are made of: the standard library's atomics,
//! lock and `Arc`, or under `--cfg loom` loom's, so that their tests try every interleaving
//! of them (`loom_tests`).

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering, fence};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU32, AtomicUsize, Ordering, fence};
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};

/// `mutex`, locked. A thread that panicked holding one left nothing half done: what is
/// changed under these locks is changed whole before anything that can panic.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
