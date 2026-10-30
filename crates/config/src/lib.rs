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
//! So far: listeners, with the TLS they terminate and what they tell upstreams of a client, routes with their rules, header filters and weighted backends, and
//! upstreams ([`Config`]), and their compilation ([`compile`]) into a router per listener
//! and what each rule does and leads to ([`Compiled`]); and what the development harness
//! reads from its file ([`HarnessFile`]).

mod backends;
mod compile;
mod config;
mod dns;
mod harness_file;
mod kubernetes;
mod limits;
mod route;

pub use backends::{Destination, UpstreamId, WeightedBackends};
pub use compile::{
    Compiled, CompiledDataPlane, CompiledForwarding, CompiledListener, CompiledMirror,
    CompiledRateLimitService, CompiledRateLimiting, CompiledRetry, CompiledRule,
    CompiledServiceTls, CompiledSynchronous, CompiledTimeouts, CompiledTls, CompiledUpstream,
    CompiledUpstreamTls, ConfigError, L4, L4Route, MOST_ATTEMPTS, MatchId, Matches,
    NamedCertificate, Object, Outcome, Place, PresentedCertificate, Problem, RuleId, SniRouter,
    Step, Timeout, compile, compile_with_matches,
};
pub use config::{
    AccessLog, Certificate, ClientValidation, Config, DataPlane, Endpoints, Forwarding,
    HealthCheck, Http3, Keepalive, Listener, ListenerCertificate, ListenerProxyProtocol,
    LoadBalancer, Probe, Protocol, ProxyProtocolVersion, RateLimitBreaker, RateLimitMode,
    RateLimitService, RateLimitServiceTls, RateLimitSynchronous, RateLimiting, RequestId,
    SlowStart, StatedNames, Tls, Upstream, UpstreamProtocol, UpstreamTls, WhenUnreachable,
};
pub use dns::{DnsName, DnsNameError};
pub use harness_file::{CertificateFiles, HarnessFile};
pub use kubernetes::{KubernetesService, ServiceName, ServiceNameError, ServicePort};
pub use limits::{Bucket, CompiledLimit, Global, Limit, LimitMatch, Limits, Model};
pub use route::{
    Backend, EncodedSlash, Filter, Forward, Fraction, GrpcMethod, Header, HeaderChanges, Hostname,
    Match, Mirror, PathChange, PathMatch, Query, Redirect, Retry, Route, Rule, RulePaths, Scheme,
    TcpRoute, Timeouts, TlsRoute, UrlRewrite, ValueMatch, ValuePredicate, Wildcard,
};
