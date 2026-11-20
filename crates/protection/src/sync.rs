//! What a pod's count is made of: the standard library's atomic and `Arc`, or under
//! `--cfg loom` loom's, so that its tests try every interleaving of them (`loom_tests`).

#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU32, Ordering};
