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
//! So far: listeners, with the TLS they terminate, routes with their rules, header filters and weighted backends, and
//! upstreams ([`Config`]), and their compilation ([`compile`]) into a router per listener
//! and what each rule does and leads to ([`Compiled`]).

mod backends;
mod compile;
mod config;
mod route;

pub use backends::{UpstreamId, WeightedBackends};
pub use compile::{
    Compiled, CompiledListener, CompiledRetry, CompiledRule, CompiledUpstream, ConfigError, Object,
    Place, Problem, RuleId, compile,
};
pub use config::{
    Certificate, Config, HealthCheck, Keepalive, Listener, Probe, Protocol, Tls, Upstream,
    UpstreamProtocol, UpstreamTls,
};
pub use route::{
    Backend, Filter, GrpcMethod, Header, HeaderChanges, Hostname, Match, PathMatch, Retry, Route,
    Rule, ValueMatch, ValuePredicate, Wildcard,
};
