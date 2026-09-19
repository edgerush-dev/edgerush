//! The EdgeRush request router.
//!
//! A pure crate: it decides which route rule a request belongs to and does nothing else —
//! no sockets, clocks, randomness or async runtime. Everything it needs is passed in, which
//! keeps it deterministic and easy to test, fuzz and benchmark.
//!
//! Only hostname patterns exist so far; path matching, predicates and Gateway API
//! precedence build on them.

pub mod host;

pub use host::{HostPattern, HostPatternError, WildcardLabels};
