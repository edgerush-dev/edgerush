//! The EdgeRush request router.
//!
//! A pure crate: it decides which route rule a request belongs to and does nothing else —
//! no sockets, clocks, randomness or async runtime. Everything it needs is passed in, which
//! keeps it deterministic and easy to test, fuzz and benchmark.
//!
//! So far: the host stage (hostname patterns and the index that finds the candidates for a
//! request's host), the path stage for exact and prefix patterns, and the normaliser that
//! request paths go through before they are matched. Regex paths,
//! predicates, the rest of Gateway API precedence and the router that joins the stages
//! build on them.

mod hash;
pub mod host;
pub mod host_index;
pub mod normalise;
pub mod path;
pub mod path_index;
#[cfg(any(test, feature = "reference"))]
pub mod reference;
#[cfg(test)]
mod strategies;

pub use host::{HostPattern, HostPatternError, WildcardLabels};
pub use host_index::{HostClaim, HostIndex};
pub use normalise::{NormaliseError, normalise_path};
pub use path::{PathPattern, PathPatternError};
pub use path_index::{PathCandidates, PathIndex};
