//! The EdgeRush request router.
//!
//! A pure crate: it decides which route rule a request belongs to and does nothing else —
//! no sockets, clocks, randomness or async runtime. Everything it needs is passed in, which
//! keeps it deterministic and easy to test, fuzz and benchmark.
//!
//! Only the host stage exists so far: hostname patterns and the index that finds the
//! candidates for a request's host. Path matching, predicates and the rest of Gateway API
//! precedence build on them.

pub mod host;
pub mod host_index;
#[cfg(test)]
mod strategies;

pub use host::{HostPattern, HostPatternError, WildcardLabels};
pub use host_index::{HostClaim, HostIndex};
