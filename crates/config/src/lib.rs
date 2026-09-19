//! The EdgeRush config model: everything a data plane runs, in one provider-neutral form.
//! Ingress, Gateway API and policies all translate into it; it is what the control plane
//! streams, what a data plane compiles, and what the development harness reads from a file.
//!
//! A pure crate. The model derives `serde`'s traits and knows no format: YAML, the wire
//! encoding and whatever comes next belong to whoever reads or writes a file or a stream.
//!
//! **One spelling, nothing implied.** Whatever affects routing is written out, in a single
//! canonical form — no shorthand, and no defaults that choose behaviour. What is missing is
//! an error, not "everything".
//!
//! So far: routes ([`Route`]) and their compilation into a router ([`compile_routes`]).
//! Listeners, upstreams, filters and backends build on them.

mod compile;
mod route;

pub use compile::{Place, Problem, RouteError, RuleId, compile_routes};
pub use route::{Hostname, Match, PathMatch, Route, Rule, ValueMatch, ValuePredicate, Wildcard};
