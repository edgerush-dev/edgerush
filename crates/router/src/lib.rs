//! The EdgeRush request router.
//!
//! A pure crate: it decides which route rule a request belongs to and does nothing else —
//! no sockets, clocks, randomness or async runtime. Everything it needs is passed in, which
//! keeps it deterministic and easy to test, fuzz and benchmark.
//!
//! [`Router`] is the whole: it joins the host stage (hostname patterns and the index that
//! finds the groups of candidates for a request's host), the path stage (exact, prefix and
//! regex patterns and their index) and the header, query parameter and method predicates in
//! Gateway API's order of precedence. [`normalise_path`] is what request paths go through
//! before they are routed.

mod hash;
pub mod header;
pub mod host;
pub mod host_index;
pub mod normalise;
pub mod path;
pub mod path_index;
pub mod query;
#[cfg(any(test, feature = "reference"))]
pub mod reference;
pub mod router;
#[cfg(test)]
mod strategies;
mod whole_regex;

pub use header::{Fields, HeaderPredicate, HeaderPredicateError, HeaderPredicates};
pub use host::{HostPattern, HostPatternError, WildcardLabels};
pub use host_index::{HostClaim, HostIndex};
pub use normalise::{NormaliseError, normalise_path};
pub use path::{PathPattern, PathPatternError};
pub use path_index::{PathCandidates, PathIndex};
pub use query::{QueryPredicate, QueryPredicateError, QueryPredicates};
pub use router::{RequestParts, RouteMatch, Router};
pub use whole_regex::RegexError;
