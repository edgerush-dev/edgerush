//! What local mode's limiter is made of: the standard library's atomics, lock and `Arc`, or
//! under `--cfg loom` loom's, so that its tests try every interleaving of them
//! (`loom_tests`).

#[cfg(not(loom))]
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(loom))]
pub(super) use std::sync::{Arc, RwLock};

#[cfg(loom)]
pub(super) use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(loom)]
pub(super) use loom::sync::{Arc, RwLock};
