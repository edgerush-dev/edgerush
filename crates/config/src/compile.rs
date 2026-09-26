//! From a config as the model states it to what a data plane runs: everything is checked,
//! names are resolved to positions, and every problem is reported with its place, not only
//! the first.

use crate::backends::{UpstreamId, WeightedBackends};
use crate::route::{
    Filter, Fraction, GrpcMethod, HeaderChanges, Hostname, Match, PathMatch, Route, ValueMatch,
    ValuePredicate, Wildcard,
};
use crate::{
    Backend, Config, HealthCheck, Http3, Keepalive, Probe, Protocol, Rule, Tls, UpstreamProtocol,
    UpstreamTls,
};
use edgerush_filters::{HeaderModifier, HeaderModifierError};
use edgerush_router::{
    HeaderPredicate, HeaderPredicateError, HeaderPredicates, HostClaim, HostIndex, HostPattern,
    HostPatternError, PathPattern, PathPatternError, QueryPredicate, QueryPredicateError,
    QueryPredicates, RouteMatch, Router, WildcardLabels,
};
use http::Method;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Which rule a request was routed to: positions in the list of routes and in the route's
/// list of rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuleId {
    /// The route's position in [`Config::routes`].
    pub route: usize,
    /// The rule's position in the route.
    pub rule: usize,
}

/// A config compiled: fully resolved and immutable, what a snapshot is made of.
#[derive(Debug)]
pub struct Compiled {
    /// The listeners, in the order of their names, each with the routes that are for it.
    pub listeners: Vec<CompiledListener>,
    /// The upstreams, in the order of their names; an [`UpstreamId`] is a position here.
    pub upstreams: Vec<CompiledUpstream>,
    /// By position of the route, then of the rule. Each is shared on its own, so that a
    /// request can hold on to its rule without holding on to the whole config.
    rules: Vec<Vec<Arc<CompiledRule>>>,
}

impl Compiled {
    /// What to do with a request that was routed to `id`.
    #[must_use]
    pub fn rule(&self, id: RuleId) -> Option<&Arc<CompiledRule>> {
        self.rules.get(id.route)?.get(id.rule)
    }

    /// The upstream a rule's backend stands for.
    #[must_use]
    pub fn upstream(&self, id: UpstreamId) -> Option<&CompiledUpstream> {
        self.upstreams.get(id.0)
    }
}

/// A listener and the router for the requests that come in there.
#[derive(Debug)]
pub struct CompiledListener {
    /// Its name in the config.
    pub name: String,
    /// The address to listen on.
    pub address: SocketAddr,
    /// What is spoken there.
    pub protocol: Protocol,
    /// What an `https` listener presents; `None` for any other.
    pub tls: Option<Tls>,
    /// Whether it serves HTTP/3 as well, and how it says so; `https` listeners only.
    pub http3: Option<Http3>,
    /// Finds the rule a request belongs to, among the routes that are for this listener.
    pub router: Router<RuleId>,
    /// Where a `tcp` or `tls` listener's connections go; `None` for an HTTP listener.
    pub l4: Option<L4>,
    /// How long a tunnel of this listener's may carry nothing before it is closed: an hour
    /// unless its config says otherwise. Only a `tcp` or `tls` listener has tunnels.
    pub tunnel_idle: Duration,
}

/// How a passthrough listener's connections find their backends (17 in the docs).
#[derive(Debug)]
pub enum L4 {
    /// Every connection to the listener's one TCP route.
    Tcp(L4Route),
    /// By the name the ClientHello asks for.
    Tls(SniRouter),
}

/// A TCP or TLS route, compiled.
#[derive(Debug, Clone)]
pub struct L4Route {
    /// Its name in the config.
    pub name: String,
    /// Where its connections go.
    pub backends: WeightedBackends,
}

/// A `tls` listener's routes, by the hostnames they claim.
#[derive(Debug)]
pub struct SniRouter {
    routes: Vec<L4Route>,
    hosts: HostIndex<Vec<usize>>,
}

impl SniRouter {
    /// The route for a ClientHello that asks for `name`, in lower case: the most specific
    /// hostname that covers it, and among equally specific ones the route that came first.
    #[must_use]
    pub fn route(&self, name: &str) -> Option<&L4Route> {
        let group = self.hosts.lookup(name).next()?;
        self.routes.get(*group.first()?)
    }
}

/// What a rule does with its requests.
#[derive(Debug)]
pub struct CompiledRule {
    /// Changes to the request's headers before it goes to the upstream, if there are any:
    /// a rule that asks for none carries nothing to skip over.
    pub request_headers: Option<HeaderModifier>,
    /// Changes to the response's headers before it goes to the client, if there are any.
    pub response_headers: Option<HeaderModifier>,
    /// Where they go.
    pub backends: WeightedBackends,
    /// When a request is sent again, if ever.
    pub retry: Option<CompiledRetry>,
    /// Where copies of its requests go, if anywhere.
    pub mirrors: Vec<CompiledMirror>,
}

/// A rule's mirror, checked: where the copies go, and how many of the requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompiledMirror {
    /// The upstream the copies go to.
    pub upstream: UpstreamId,
    numerator: u32,
    denominator: u32,
}

impl CompiledMirror {
    /// Whether the request `random` was drawn for is one of the share copied.
    #[must_use]
    pub fn takes(&self, random: u64) -> bool {
        random % u64::from(self.denominator) < u64::from(self.numerator)
    }
}

/// A rule's retries, checked and in the form an answer is compared with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledRetry {
    /// Times a request may be sent again, beyond the first.
    pub attempts: u32,
    /// HTTP statuses that send it again.
    pub http_statuses: Vec<u16>,
    /// gRPC statuses that send a call again, one bit for each by its number.
    pub grpc_statuses: u32,
    /// The wait before the first retry.
    pub backoff_base: std::time::Duration,
    /// The most a wait may double to.
    pub backoff_max: std::time::Duration,
}

impl CompiledRetry {
    /// Whether an answer with this HTTP status is one to send the request again for.
    #[must_use]
    pub fn on_status(&self, status: u16) -> bool {
        self.http_statuses.contains(&status)
    }

    /// Whether a call ended with this gRPC status, by number, is one to send again.
    #[must_use]
    pub fn on_grpc(&self, code: usize) -> bool {
        code < 32 && self.grpc_statuses & (1 << code) != 0
    }
}

/// gRPC's names for its codes, by number.
const GRPC_CODES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

/// The most times a request may be sent again: beyond it a retry policy is a load
/// multiplier more than a remedy.
const MOST_ATTEMPTS: u32 = 5;

fn retry(rule: &Rule, place: &Place, errors: &mut Vec<ConfigError>) -> Option<CompiledRetry> {
    let retry = rule.retry.as_ref()?;
    let mut problems = Vec::new();
    if retry.attempts == 0 || retry.attempts > MOST_ATTEMPTS {
        problems.push(Problem::RetryAttempts(retry.attempts));
    }
    if retry.http_statuses.is_empty() && retry.grpc_statuses.is_empty() {
        problems.push(Problem::RetryOnNothing);
    }
    for status in &retry.http_statuses {
        // An answer that succeeded, or one still to come, is nothing to try again for.
        if !(400..=599).contains(status) {
            problems.push(Problem::RetryStatus(*status));
        }
    }
    let mut grpc = 0_u32;
    for name in &retry.grpc_statuses {
        match GRPC_CODES.iter().position(|code| code == name) {
            Some(0) | None => problems.push(Problem::RetryGrpcStatus(name.clone())),
            Some(code) => grpc |= 1 << code,
        }
    }
    if retry.backoff_base_ms == 0 || retry.backoff_base_ms > retry.backoff_max_ms {
        problems.push(Problem::RetryBackoff);
    }
    let compiled = problems.is_empty().then(|| CompiledRetry {
        attempts: retry.attempts,
        http_statuses: retry.http_statuses.clone(),
        grpc_statuses: grpc,
        backoff_base: std::time::Duration::from_millis(retry.backoff_base_ms),
        backoff_max: std::time::Duration::from_millis(retry.backoff_max_ms),
    });
    errors.extend(problems.into_iter().map(|problem| place.problem(problem)));
    compiled
}

/// An upstream, with the name it had in the config for logs and metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledUpstream {
    /// Its name in the config.
    pub name: String,
    /// Where to connect.
    pub endpoints: Vec<SocketAddr>,
    /// What to speak there.
    pub protocol: UpstreamProtocol,
    /// TLS to its endpoints, if any.
    pub tls: Option<UpstreamTls>,
    /// PINGs on its HTTP/2 connections, if any.
    pub keepalive: Option<Keepalive>,
    /// Probes of its endpoints, if any.
    pub health_check: Option<HealthCheck>,
}

/// Compiles a config. The order of the routes, then of the rules, then of a rule's matches
/// is the last tie-breaker of precedence.
///
/// # Errors
///
/// Returns every problem found, each with its place. Nothing is compiled if there is any:
/// a data plane keeps running what it has, and a harness does not start.
pub fn compile(config: &Config) -> Result<Compiled, Vec<ConfigError>> {
    let mut errors = Vec::new();

    // A `BTreeMap` hands out its names in order, so positions are the same on every pod.
    let upstream_ids: BTreeMap<&str, UpstreamId> = config
        .upstreams
        .keys()
        .enumerate()
        .map(|(position, name)| (name.as_str(), UpstreamId(position)))
        .collect();
    let upstreams = config
        .upstreams
        .iter()
        .map(|(name, upstream)| CompiledUpstream {
            name: name.clone(),
            endpoints: upstream.endpoints.clone(),
            protocol: upstream.protocol,
            tls: upstream.tls.clone(),
            keepalive: upstream.keepalive,
            health_check: upstream.health_check.clone(),
        })
        .collect();
    for (name, upstream) in &config.upstreams {
        if let Some(check) = &upstream.health_check {
            let problem = if check.interval_seconds == 0
                || check.timeout_seconds == 0
                || check.healthy_threshold == 0
                || check.unhealthy_threshold == 0
            {
                Some(Problem::HealthCheckZero)
            } else if check.timeout_seconds > check.interval_seconds {
                Some(Problem::HealthCheckOverlaps)
            } else {
                match &check.probe {
                    Probe::Http { path } if !path.starts_with('/') => {
                        Some(Problem::HealthCheckPath(path.clone()))
                    }
                    Probe::Grpc { .. } if upstream.protocol != UpstreamProtocol::Http2 => {
                        Some(Problem::GrpcCheckNeedsHttp2)
                    }
                    _ => None,
                }
            };
            if let Some(problem) = problem {
                errors.push(Place::upstream(name).problem(problem));
            }
        }
        if let Some(keepalive) = &upstream.keepalive {
            let problem = if upstream.protocol != UpstreamProtocol::Http2 {
                Some(Problem::KeepaliveNeedsHttp2)
            } else if keepalive.interval_seconds < KEEPALIVE_FLOOR_SECONDS
                && !keepalive.backend_allows_short_intervals
            {
                Some(Problem::KeepaliveTooOften(keepalive.interval_seconds))
            } else if keepalive.interval_seconds == 0 || keepalive.timeout_seconds == 0 {
                Some(Problem::KeepaliveZero)
            } else {
                None
            };
            if let Some(problem) = problem {
                errors.push(Place::upstream(name).problem(problem));
            }
        }
        if let Some(tls) = &upstream.tls {
            if tls.authorities.is_empty() {
                errors.push(Place::upstream(name).problem(Problem::NoAuthority));
            }
            if !is_host_name(&tls.server_name) {
                let problem = Problem::ServerName(tls.server_name.clone());
                errors.push(Place::upstream(name).problem(problem));
            }
        }
    }

    // Until listeners can share a socket, two on one address cannot both be served.
    let mut addresses: BTreeMap<SocketAddr, &str> = BTreeMap::new();
    for (name, listener) in &config.listeners {
        if let Some(other) = addresses.insert(listener.address, name) {
            let problem = Problem::AddressTaken {
                address: listener.address,
                other: other.to_owned(),
            };
            errors.push(Place::listener(name).problem(problem));
        }
        let certificates = listener.tls.as_ref().map(|tls| tls.certificates.len());
        match (listener.protocol, certificates) {
            (Protocol::Https, None | Some(0)) => {
                errors.push(Place::listener(name).problem(Problem::NoCertificate));
            }
            // A passthrough listener presents nothing: the backend terminates TLS.
            (Protocol::Http | Protocol::Tcp | Protocol::Tls, Some(_)) => {
                errors.push(Place::listener(name).problem(Problem::TlsUnwanted));
            }
            (Protocol::Https, Some(_)) | (Protocol::Http | Protocol::Tcp | Protocol::Tls, None) => {
            }
        }
        // QUIC is always TLS: there is no HTTP/3 in the clear (RFC 9114 §3.1).
        if listener.http3.is_some() && listener.protocol != Protocol::Https {
            errors.push(Place::listener(name).problem(Problem::Http3NeedsTls));
        }
        match (listener.protocol, listener.tunnel_idle_seconds) {
            (Protocol::Http | Protocol::Https, Some(_)) => {
                errors.push(Place::listener(name).problem(Problem::TunnelIdleUnwanted));
            }
            (_, Some(0)) => {
                errors.push(Place::listener(name).problem(Problem::TunnelIdleZero));
            }
            _ => {}
        }
        let validation = listener
            .tls
            .as_ref()
            .and_then(|tls| tls.client_validation.as_ref());
        if validation.is_some_and(|validation| validation.authorities.is_empty()) {
            errors.push(Place::listener(name).problem(Problem::NoAuthority));
        }
    }

    // The matches of every listener, by the listener's name.
    let mut matches: BTreeMap<&str, Vec<RouteMatch<RuleId>>> = config
        .listeners
        .keys()
        .map(|name| (name.as_str(), Vec::new()))
        .collect();
    let mut rules = Vec::new();
    let mut names = HashSet::new();
    for (route_at, route) in config.routes.iter().enumerate() {
        let place = Place::route(&route.name);
        if !names.insert(route.name.as_str()) {
            errors.push(place.problem(Problem::DuplicateName));
        }
        let listeners = listeners_of(route, &matches, &place, &mut errors);
        for listener in &listeners {
            let protocol = config.listeners.get(*listener).map(|l| l.protocol);
            if let Some(protocol @ (Protocol::Tcp | Protocol::Tls)) = protocol {
                errors.push(place.problem(Problem::WrongListener {
                    kind: "HTTP",
                    listener: (*listener).to_owned(),
                    protocol: protocol_name(protocol),
                }));
            }
        }
        let hosts = host_claims(route, &place, &mut errors);

        let mut compiled_rules = Vec::new();
        for (rule_at, rule) in route.rules.iter().enumerate() {
            let place = Place {
                rule: Some(rule_at),
                ..place.clone()
            };
            let id = RuleId {
                route: route_at,
                rule: rule_at,
            };
            if rule.matches.is_empty() {
                errors.push(place.problem(Problem::NoMatches));
            }
            for (match_at, matching) in rule.matches.iter().enumerate() {
                let place = Place {
                    matching: Some(match_at),
                    ..place.clone()
                };
                match route_match(matching, &hosts, id) {
                    Ok(route_match) => {
                        for listener in &listeners {
                            if let Some(matches) = matches.get_mut(listener) {
                                matches.push(route_match.clone());
                            }
                        }
                    }
                    Err(problems) => {
                        errors.extend(problems.into_iter().map(|problem| place.problem(problem)));
                    }
                }
            }
            let (request_headers, response_headers, mirrors) =
                filters(rule, &upstream_ids, &place, &mut errors);
            compiled_rules.push(Arc::new(CompiledRule {
                request_headers,
                response_headers,
                backends: backends(rule, &upstream_ids, &place, &mut errors),
                retry: retry(rule, &place, &mut errors),
                mirrors,
            }));
        }
        rules.push(compiled_rules);
    }

    let mut l4 = l4_routes(config, &upstream_ids, &mut names, &mut errors);

    if errors.is_empty() {
        let listeners = config
            .listeners
            .iter()
            .map(|(name, listener)| CompiledListener {
                name: name.clone(),
                address: listener.address,
                protocol: listener.protocol,
                tls: listener.tls.clone(),
                http3: listener.http3,
                router: Router::new(matches.remove(name.as_str()).unwrap_or_default()),
                l4: l4.remove(name.as_str()),
                tunnel_idle: Duration::from_secs(
                    listener.tunnel_idle_seconds.unwrap_or(TUNNEL_IDLE_SECONDS),
                ),
            })
            .collect();
        Ok(Compiled {
            listeners,
            upstreams,
            rules,
        })
    } else {
        Err(errors)
    }
}

/// The most backends a passthrough route has, as TCPRoute and TLSRoute allow.
const MOST_BACKENDS: usize = 16;

/// A tunnel's idle bound unless its listener's config says otherwise: an hour.
const TUNNEL_IDLE_SECONDS: u64 = 3_600;

/// How a protocol is written in a config.
fn protocol_name(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Http => "http",
        Protocol::Https => "https",
        Protocol::Tcp => "tcp",
        Protocol::Tls => "tls",
    }
}

/// The TCP and TLS routes, compiled, by the listener they are for; every problem with
/// them, and with the passthrough listeners they leave without a route, into `errors`.
fn l4_routes<'a>(
    config: &'a Config,
    upstream_ids: &BTreeMap<&str, UpstreamId>,
    names: &mut HashSet<&'a str>,
    errors: &mut Vec<ConfigError>,
) -> BTreeMap<&'a str, L4> {
    let mut tcp: BTreeMap<&str, Vec<L4Route>> = BTreeMap::new();
    for route in &config.tcp_routes {
        let place = Place::route(&route.name);
        if !names.insert(route.name.as_str()) {
            errors.push(place.problem(Problem::DuplicateName));
        }
        let listeners = l4_listeners(config, &route.listeners, Protocol::Tcp, &place, errors);
        let compiled = L4Route {
            name: route.name.clone(),
            backends: l4_backends(config, &route.backends, upstream_ids, &place, errors),
        };
        for listener in listeners {
            tcp.entry(listener).or_default().push(compiled.clone());
        }
    }
    let mut tls: BTreeMap<&str, (Vec<L4Route>, Vec<HostClaim<usize>>)> = BTreeMap::new();
    for route in &config.tls_routes {
        let place = Place::route(&route.name);
        if !names.insert(route.name.as_str()) {
            errors.push(place.problem(Problem::DuplicateName));
        }
        let listeners = l4_listeners(config, &route.listeners, Protocol::Tls, &place, errors);
        if route.hostnames.is_empty() {
            errors.push(place.problem(Problem::NoHostnames));
        }
        let claims: Vec<HostClaim<()>> = route
            .hostnames
            .iter()
            .filter_map(|hostname| {
                host_claim(hostname)
                    .map_err(|problem| errors.push(place.problem(problem)))
                    .ok()
            })
            .collect();
        let compiled = L4Route {
            name: route.name.clone(),
            backends: l4_backends(config, &route.backends, upstream_ids, &place, errors),
        };
        for listener in listeners {
            let (routes, hosts) = tls.entry(listener).or_default();
            let at = routes.len();
            routes.push(compiled.clone());
            hosts.extend(claims.iter().map(|claim| HostClaim {
                pattern: claim.pattern.clone(),
                falls_through: claim.falls_through,
                value: at,
            }));
        }
    }

    let mut compiled = BTreeMap::new();
    for (name, listener) in &config.listeners {
        match listener.protocol {
            Protocol::Tcp => {
                let mut routes = tcp.remove(name.as_str()).unwrap_or_default();
                if routes.len() == 1 {
                    if let Some(route) = routes.pop() {
                        compiled.insert(name.as_str(), L4::Tcp(route));
                    }
                } else {
                    errors.push(Place::listener(name).problem(Problem::TcpRoutes(routes.len())));
                }
            }
            Protocol::Tls => {
                let (routes, hosts) = tls.remove(name.as_str()).unwrap_or_default();
                let hosts = HostIndex::new(hosts, |members: Vec<usize>| members);
                compiled.insert(name.as_str(), L4::Tls(SniRouter { routes, hosts }));
            }
            Protocol::Http | Protocol::Https => {}
        }
    }
    compiled
}

/// The listeners a passthrough route is for, each once, as far as they exist and are of
/// the route's `kind`.
fn l4_listeners<'a>(
    config: &Config,
    names: &'a [String],
    kind: Protocol,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> Vec<&'a str> {
    if names.is_empty() {
        errors.push(place.problem(Problem::NoListeners));
    }
    let mut listeners: Vec<&str> = Vec::new();
    for name in names {
        match config.listeners.get(name) {
            None => errors.push(place.problem(Problem::UnknownListener(name.clone()))),
            Some(_) if listeners.contains(&name.as_str()) => {
                errors.push(place.problem(Problem::ListenerTwice(name.clone())));
            }
            Some(listener) if listener.protocol != kind => {
                errors.push(place.problem(Problem::WrongListener {
                    kind: if kind == Protocol::Tcp { "TCP" } else { "TLS" },
                    listener: name.clone(),
                    protocol: protocol_name(listener.protocol),
                }));
            }
            Some(_) => listeners.push(name),
        }
    }
    listeners
}

/// A passthrough route's backends: 1 to 16, of upstreams that take plain bytes.
fn l4_backends(
    config: &Config,
    backends: &[Backend],
    upstream_ids: &BTreeMap<&str, UpstreamId>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> WeightedBackends {
    if backends.is_empty() {
        errors.push(place.problem(Problem::NoBackends));
    }
    if backends.len() > MOST_BACKENDS {
        errors.push(place.problem(Problem::TooManyBackends));
    }
    let mut resolved = Vec::with_capacity(backends.len());
    for (at, backend) in backends.iter().enumerate() {
        let place = Place {
            backend: Some(at),
            ..place.clone()
        };
        let Some(&id) = upstream_ids.get(backend.upstream.as_str()) else {
            errors.push(place.problem(Problem::UnknownUpstream(backend.upstream.clone())));
            continue;
        };
        let speaks_more = config
            .upstreams
            .get(&backend.upstream)
            .is_some_and(|upstream| {
                upstream.protocol != UpstreamProtocol::Http1 || upstream.tls.is_some()
            });
        if speaks_more {
            errors.push(place.problem(Problem::PassthroughUpstream(backend.upstream.clone())));
        }
        resolved.push((id, backend.weight));
    }
    WeightedBackends::new(resolved)
}

/// What gRPC servers enforce by default between a client's PINGs: five minutes.
const KEEPALIVE_FLOOR_SECONDS: u64 = 300;

/// Whether `name` is a host name as a certificate names one: labels of letters, digits and
/// hyphens, none empty, none starting or ending with a hyphen. No port, no wildcard, and
/// not an address.
fn is_host_name(name: &str) -> bool {
    let label = |label: &str| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    };
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(label)
        && name.parse::<std::net::IpAddr>().is_err()
}

/// The listeners a route is for, each once, as far as they exist.
fn listeners_of<'a>(
    route: &'a Route,
    known: &BTreeMap<&str, Vec<RouteMatch<RuleId>>>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> Vec<&'a str> {
    if route.listeners.is_empty() {
        errors.push(place.problem(Problem::NoListeners));
    }
    let mut listeners: Vec<&str> = Vec::new();
    for name in &route.listeners {
        if !known.contains_key(name.as_str()) {
            errors.push(place.problem(Problem::UnknownListener(name.clone())));
        } else if listeners.contains(&name.as_str()) {
            errors.push(place.problem(Problem::ListenerTwice(name.clone())));
        } else {
            listeners.push(name);
        }
    }
    listeners
}

fn host_claims(route: &Route, place: &Place, errors: &mut Vec<ConfigError>) -> Vec<HostClaim<()>> {
    if route.hostnames.is_empty() {
        errors.push(place.problem(Problem::NoHostnames));
    }
    if route.rules.is_empty() {
        errors.push(place.problem(Problem::NoRules));
    }
    let mut hosts = Vec::new();
    for hostname in &route.hostnames {
        match host_claim(hostname) {
            Ok(claim) => hosts.push(claim),
            Err(problem) => errors.push(place.problem(problem)),
        }
    }
    hosts
}

fn host_claim(hostname: &Hostname) -> Result<HostClaim<()>, Problem> {
    let pattern = if hostname.name == "*" {
        None
    } else {
        let is_wildcard = hostname.name.starts_with('*');
        let labels = match hostname.wildcard {
            Some(Wildcard::OneLabel) => WildcardLabels::One,
            Some(Wildcard::AnyLabels) => WildcardLabels::OneOrMore,
            None if is_wildcard => {
                return Err(Problem::WildcardUnstated {
                    name: hostname.name.clone(),
                });
            }
            // An exact name: the kind has no meaning, so any will do.
            None => WildcardLabels::One,
        };
        let pattern =
            HostPattern::parse(&hostname.name, labels).map_err(|reason| Problem::Hostname {
                name: hostname.name.clone(),
                reason,
            })?;
        Some(pattern)
    };
    Ok(HostClaim {
        pattern,
        falls_through: hostname.falls_through,
        value: (),
    })
}

fn route_match(
    matching: &Match,
    hosts: &[HostClaim<()>],
    value: RuleId,
) -> Result<RouteMatch<RuleId>, Vec<Problem>> {
    let mut problems = Vec::new();

    let path = match (&matching.path, &matching.grpc) {
        (Some(path), None) => match path {
            PathMatch::Exact(path) => PathPattern::exact(path),
            PathMatch::Prefix(path) => PathPattern::prefix(path),
            PathMatch::Regex(pattern) => PathPattern::regex(pattern),
        }
        .map_err(|reason| problems.push(Problem::Path(reason)))
        .ok(),
        (None, Some(grpc)) => grpc_path(grpc)
            .map_err(|problem| problems.push(problem))
            .ok(),
        (None, None) => {
            problems.push(Problem::NoPath);
            None
        }
        (Some(_), Some(_)) => {
            problems.push(Problem::PathAndGrpc);
            None
        }
    };

    // Any token is a method to the `http` crate; one in lower case would be a method of its
    // own that no request has.
    let method = matching.method.as_ref().and_then(|method| {
        let parsed = Method::from_bytes(method.as_bytes())
            .ok()
            .filter(|_| !method.bytes().any(|byte| byte.is_ascii_lowercase()));
        if parsed.is_none() {
            problems.push(Problem::Method(method.clone()));
        }
        parsed
    });

    let mut headers = Vec::new();
    for ValuePredicate { name, value } in &matching.headers {
        let predicate = match value {
            ValueMatch::Exact(value) => HeaderPredicate::exact(name, value),
            ValueMatch::Regex(pattern) => HeaderPredicate::regex(name, pattern),
        };
        match predicate {
            Ok(predicate) => headers.push(predicate),
            Err(reason) => problems.push(Problem::Header {
                name: name.clone(),
                reason,
            }),
        }
    }

    let mut query = Vec::new();
    for ValuePredicate { name, value } in &matching.query {
        let predicate = match value {
            ValueMatch::Exact(value) => QueryPredicate::exact(name, value),
            ValueMatch::Regex(pattern) => QueryPredicate::regex(name, pattern),
        };
        match predicate {
            Ok(predicate) => query.push(predicate),
            Err(reason) => problems.push(Problem::Query {
                name: name.clone(),
                reason,
            }),
        }
    }

    match path {
        Some(path) if problems.is_empty() => Ok(RouteMatch {
            hosts: hosts.to_vec(),
            path,
            method,
            headers: HeaderPredicates::new(headers),
            query: QueryPredicates::new(query),
            value,
        }),
        _ => Err(problems),
    }
}

/// The path a gRPC method match stands for. Names are checked as GRPCRoute checks them:
/// a service is dot-separated identifiers, a method one identifier.
fn grpc_path(grpc: &GrpcMethod) -> Result<PathPattern, Problem> {
    fn identifier(name: &str) -> bool {
        let mut bytes = name.bytes();
        bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }
    let service_ok = |service: &str| {
        let service = service.strip_prefix('.').unwrap_or(service);
        service.split('.').all(identifier)
    };
    let bad = || Problem::GrpcMethod {
        service: grpc.service.clone().unwrap_or_default(),
        method: grpc.method.clone().unwrap_or_default(),
    };
    if grpc
        .service
        .as_deref()
        .is_some_and(|service| !service_ok(service))
        || grpc
            .method
            .as_deref()
            .is_some_and(|method| !identifier(method))
    {
        return Err(bad());
    }
    let path = match (&grpc.service, &grpc.method) {
        (Some(service), Some(method)) => PathPattern::exact(&format!("/{service}/{method}")),
        (Some(service), None) => PathPattern::prefix(&format!("/{service}")),
        // Checked to be an identifier: nothing in it means anything to a pattern.
        (None, Some(method)) => PathPattern::regex(&format!("/[^/]+/{method}")),
        (None, None) => return Err(bad()),
    };
    path.map_err(Problem::Path)
}

/// The rule's header modifiers, for the request and for the response, and its mirrors.
fn filters(
    rule: &Rule,
    upstream_ids: &BTreeMap<&str, UpstreamId>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> (
    Option<HeaderModifier>,
    Option<HeaderModifier>,
    Vec<CompiledMirror>,
) {
    let mut request = None;
    let mut response = None;
    let mut mirrors = Vec::new();
    for (at, filter) in rule.filters.iter().enumerate() {
        let place = Place {
            filter: Some(at),
            ..place.clone()
        };
        let (slot, changes, kind) = match filter {
            Filter::RequestMirror(mirror) => {
                let upstream = upstream_ids.get(mirror.upstream.as_str()).copied();
                if upstream.is_none() {
                    errors.push(place.problem(Problem::UnknownUpstream(mirror.upstream.clone())));
                }
                let Fraction {
                    numerator,
                    denominator,
                } = mirror.fraction;
                if denominator == 0 || numerator > denominator {
                    errors.push(place.problem(Problem::MirrorFraction {
                        numerator,
                        denominator,
                    }));
                }
                // A share of none is valid, as Gateway API has it, and costs nothing.
                if let Some(upstream) = upstream
                    && numerator > 0
                {
                    mirrors.push(CompiledMirror {
                        upstream,
                        numerator,
                        denominator,
                    });
                }
                continue;
            }
            Filter::RequestHeaderModifier(changes) => {
                (&mut request, changes, "request_header_modifier")
            }
            Filter::ResponseHeaderModifier(changes) => {
                (&mut response, changes, "response_header_modifier")
            }
        };
        if slot.is_some() {
            errors.push(place.problem(Problem::FilterTwice(kind)));
        }
        match header_modifier(changes) {
            Ok(modifier) => *slot = Some(modifier),
            Err(reason) => errors.push(place.problem(Problem::HeaderModifier(reason))),
        }
    }
    let kept = |modifier: Option<HeaderModifier>| modifier.filter(|modifier| !modifier.is_empty());
    (kept(request), kept(response), mirrors)
}

fn header_modifier(changes: &HeaderChanges) -> Result<HeaderModifier, HeaderModifierError> {
    fn pairs(headers: &[crate::Header]) -> impl Iterator<Item = (&str, &str)> {
        headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_str()))
    }
    HeaderModifier::new(
        pairs(&changes.set),
        pairs(&changes.add),
        changes.remove.iter().map(String::as_str),
    )
}

fn backends(
    rule: &Rule,
    upstream_ids: &BTreeMap<&str, UpstreamId>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> WeightedBackends {
    if rule.backends.is_empty() {
        errors.push(place.problem(Problem::NoBackends));
    }
    let resolved = rule
        .backends
        .iter()
        .enumerate()
        .filter_map(|(at, backend)| {
            let id = upstream_ids.get(backend.upstream.as_str()).copied();
            if id.is_none() {
                let place = Place {
                    backend: Some(at),
                    ..place.clone()
                };
                errors.push(place.problem(Problem::UnknownUpstream(backend.upstream.clone())));
            }
            Some((id?, backend.weight))
        });
    WeightedBackends::new(resolved.collect::<Vec<_>>())
}

/// A problem in a config, and where it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{place}: {problem}")]
pub struct ConfigError {
    /// Where the problem is.
    pub place: Place,
    /// What it is.
    pub problem: Problem,
}

/// A named thing in a config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    /// The listener of this name.
    Listener(String),
    /// The route of this name.
    Route(String),
    /// The upstream of this name.
    Upstream(String),
}

/// A place in a config: a listener, or a route, a rule in it, a match or a backend in the
/// rule. Positions count from zero, as the lists in a file do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    /// The listener or route.
    pub object: Object,
    /// The rule's position in the route, if the problem is in a rule.
    pub rule: Option<usize>,
    /// The match's position in the rule, if the problem is in a match.
    pub matching: Option<usize>,
    /// The filter's position in the rule, if the problem is in a filter.
    pub filter: Option<usize>,
    /// The backend's position in the rule, if the problem is in a backend.
    pub backend: Option<usize>,
}

impl Place {
    fn route(name: &str) -> Self {
        Self::of(Object::Route(name.to_owned()))
    }

    fn listener(name: &str) -> Self {
        Self::of(Object::Listener(name.to_owned()))
    }

    fn upstream(name: &str) -> Self {
        Self::of(Object::Upstream(name.to_owned()))
    }

    fn of(object: Object) -> Self {
        Self {
            object,
            rule: None,
            matching: None,
            filter: None,
            backend: None,
        }
    }

    fn problem(&self, problem: Problem) -> ConfigError {
        ConfigError {
            place: self.clone(),
            problem,
        }
    }
}

impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.object {
            Object::Listener(name) => write!(f, "listener `{name}`")?,
            Object::Route(name) => write!(f, "route `{name}`")?,
            Object::Upstream(name) => write!(f, "upstream `{name}`")?,
        }
        if let Some(rule) = self.rule {
            write!(f, ", rules[{rule}]")?;
        }
        if let Some(matching) = self.matching {
            write!(f, ", matches[{matching}]")?;
        }
        if let Some(filter) = self.filter {
            write!(f, ", filters[{filter}]")?;
        }
        if let Some(backend) = self.backend {
            write!(f, ", backends[{backend}]")?;
        }
        Ok(())
    }
}

/// What is wrong.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// Another listener has the same address.
    #[error("address {address} is already taken by listener `{other}`")]
    AddressTaken {
        /// The address both want.
        address: SocketAddr,
        /// The listener that has it.
        other: String,
    },
    /// An `https` listener with nothing to present.
    #[error("protocol `https` needs `tls` with a certificate")]
    NoCertificate,
    /// TLS on a listener that does not speak it.
    #[error("`tls` is for protocol `https`")]
    TlsUnwanted,
    /// HTTP/3 on a listener without TLS.
    #[error("`http3` is for protocol `https`")]
    Http3NeedsTls,
    /// A tunnel's idle bound on a listener that makes no tunnels.
    #[error("`tunnel_idle_seconds` is for protocols `tcp` and `tls`")]
    TunnelIdleUnwanted,
    /// A tunnel that could never carry anything.
    #[error("`tunnel_idle_seconds` must be at least 1")]
    TunnelIdleZero,
    /// A route for a listener that does not take its kind.
    #[error("a {kind} route cannot be for listener `{listener}`, which is `{protocol}`")]
    WrongListener {
        /// The route's kind.
        kind: &'static str,
        /// The listener named.
        listener: String,
        /// What the listener speaks.
        protocol: &'static str,
    },
    /// A `tcp` listener without exactly one TCP route.
    #[error("a `tcp` listener needs exactly one TCP route; it has {0}")]
    TcpRoutes(usize),
    /// More backends than a passthrough route may have.
    #[error("a TCP or TLS route has at most 16 backends")]
    TooManyBackends,
    /// A passthrough route to an upstream spoken to in HTTP/2 or over TLS, which it cannot
    /// be: a passthrough route carries bytes as they come.
    #[error(
        "upstream `{0}` is spoken to in HTTP/2 or over TLS, which a TCP or TLS route does not do"
    )]
    PassthroughUpstream(String),
    /// TLS to an upstream that trusts nobody.
    #[error("`tls` needs an authority to trust")]
    NoAuthority,
    /// PINGs for an upstream that is not spoken to in HTTP/2.
    #[error("`keepalive` is for an upstream spoken to in HTTP/2")]
    KeepaliveNeedsHttp2,
    /// PINGs more often than gRPC servers take, without saying the backend takes them.
    #[error(
        "a PING every {0} s is more often than the 300 s gRPC servers take; say \
         `backend_allows_short_intervals` if this backend takes it"
    )]
    KeepaliveTooOften(u64),
    /// A keepalive interval or timeout of nothing.
    #[error("`keepalive` needs an interval and a timeout of at least a second")]
    KeepaliveZero,
    /// A health check with an interval, timeout or threshold of nothing.
    #[error("`health_check` needs an interval, a timeout and thresholds of at least one")]
    HealthCheckZero,
    /// A probe that could still be running when the next is due.
    #[error("a health check's timeout is longer than its interval")]
    HealthCheckOverlaps,
    /// An HTTP probe of something that is not a path.
    #[error("health check path `{0}` does not start with `/`")]
    HealthCheckPath(String),
    /// A retry count of nothing, or more than a retry is worth.
    #[error("`retry.attempts` is {0}: from 1 to 5")]
    RetryAttempts(u32),
    /// A retry for no answer at all.
    #[error("`retry` names no status to send a request again for")]
    RetryOnNothing,
    /// A retry for an HTTP status that is not a failure.
    #[error("`retry` on status {0}: only 4xx and 5xx")]
    RetryStatus(u16),
    /// A retry for a gRPC status that is not one, or is success.
    #[error("`retry` on gRPC status `{0}`: not one of gRPC's failures")]
    RetryGrpcStatus(String),
    /// A backoff of nothing, or one whose most is less than its first.
    #[error("`retry` needs a backoff of at least a millisecond, no more than its most")]
    RetryBackoff,
    /// A mirror's share that is not one: out of nothing, or more than all.
    #[error("a mirror's fraction {numerator}/{denominator} is not a share of the requests")]
    MirrorFraction {
        /// How many.
        numerator: u32,
        /// Out of how many.
        denominator: u32,
    },
    /// A gRPC probe of an upstream not spoken to in HTTP/2.
    #[error("a gRPC health check is for an upstream spoken to in HTTP/2")]
    GrpcCheckNeedsHttp2,
    /// TLS to an upstream whose server is not named by a host name.
    #[error("server name `{0}` is not a host name")]
    ServerName(String),
    /// Another route has the same name.
    #[error("another route has the same name")]
    DuplicateName,
    /// The route is for no listener.
    #[error("no listeners")]
    NoListeners,
    /// The route names a listener the config does not have.
    #[error("there is no listener `{0}`")]
    UnknownListener(String),
    /// The route names a listener twice.
    #[error("listener `{0}` is listed twice")]
    ListenerTwice(String),
    /// The route serves no host.
    #[error("no hostnames; every host is the name `*`")]
    NoHostnames,
    /// The route has no rules.
    #[error("no rules")]
    NoRules,
    /// The rule is for no request.
    #[error("no matches; any path is `{{ prefix: / }}`")]
    NoMatches,
    /// The match says neither a path nor a gRPC method.
    #[error("a match needs `path` or `grpc`; any path is `{{ prefix: / }}`")]
    NoPath,
    /// The match says both.
    #[error("a match has `path` or `grpc`, not both")]
    PathAndGrpc,
    /// The gRPC method match names no service or method, or not in their form.
    #[error("gRPC service `{service}` and method `{method}` are not names a call could have")]
    GrpcMethod {
        /// The service as written, if any.
        service: String,
        /// The method as written, if any.
        method: String,
    },
    /// The rule has two filters of a kind that it may have once.
    #[error("a rule may have one `{0}`")]
    FilterTwice(&'static str),
    /// The header modifier is not valid.
    #[error("{0}")]
    HeaderModifier(HeaderModifierError),
    /// The rule sends its requests nowhere.
    #[error("no backends")]
    NoBackends,
    /// The backend names an upstream the config does not have.
    #[error("there is no upstream `{0}`")]
    UnknownUpstream(String),
    /// The hostname is not valid.
    #[error("hostname `{name}`: {reason}")]
    Hostname {
        /// The hostname as written.
        name: String,
        /// What is wrong with it.
        reason: HostPatternError,
    },
    /// A wildcard hostname that does not say what its `*` stands for.
    #[error("hostname `{name}` is a wildcard; `wildcard` must say `one_label` or `any_labels`")]
    WildcardUnstated {
        /// The hostname as written.
        name: String,
    },
    /// The path is not valid.
    #[error("path: {0}")]
    Path(PathPatternError),
    /// The method is not an HTTP method in upper case.
    #[error("method `{0}` is not an HTTP method in upper case")]
    Method(String),
    /// The header condition is not valid.
    #[error("header `{name}`: {reason}")]
    Header {
        /// The header name as written.
        name: String,
        /// What is wrong with the condition.
        reason: HeaderPredicateError,
    },
    /// The query parameter condition is not valid.
    #[error("query parameter `{name}`: {reason}")]
    Query {
        /// The parameter name as written.
        name: String,
        /// What is wrong with the condition.
        reason: QueryPredicateError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_router::RequestParts;
    use http::HeaderMap;

    fn config(yaml: &str) -> Config {
        serde_saphyr::from_str(yaml).unwrap()
    }

    fn route(compiled: &Compiled, host: &str, target: &str) -> Option<(usize, usize)> {
        route_with(compiled, "GET", host, target, &[])
    }

    fn route_on(compiled: &Compiled, listener: &str, host: &str) -> Option<(usize, usize)> {
        let listener = compiled
            .listeners
            .iter()
            .find(|l| l.name == listener)
            .unwrap();
        listener
            .router
            .route(&RequestParts {
                host,
                path: "/",
                query: "",
                method: &Method::GET,
                headers: &HeaderMap::new(),
            })
            .map(|id| (id.route, id.rule))
    }

    fn route_with(
        compiled: &Compiled,
        method: &str,
        host: &str,
        target: &str,
        fields: &[(&'static str, &'static str)],
    ) -> Option<(usize, usize)> {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(*name, value.parse().unwrap());
        }
        let web = compiled.listeners.iter().find(|l| l.name == "web").unwrap();
        web.router
            .route(&RequestParts {
                host,
                path,
                query,
                method: &method.parse().unwrap(),
                headers: &headers,
            })
            .map(|id| (id.route, id.rule))
    }

    const SHOP: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http }
routes:
  - name: shop
    listeners: [web]
    hostnames:
      - { name: shop.example.com, falls_through: true }
      - { name: "*.shop.example.com", wildcard: any_labels, falls_through: true }
    rules:
      - matches:
          - path: { exact: /checkout }
            method: POST
          - path: { prefix: /cart }
            headers: [{ name: X-Beta, value: { exact: "on" } }]
            query: [{ name: tenant, value: { regex: "[a-z]+" } }]
        backends:
          - { upstream: checkout, weight: 9 }
          - { upstream: checkout-canary, weight: 1 }
      - matches:
          - path: { regex: "/orders/[0-9]+" }
          - path: { prefix: / }
        filters:
          - type: request_header_modifier
            set: [{ name: X-Gateway, value: edgerush }]
            remove: [x-debug]
          - type: response_header_modifier
            add: [{ name: cache-control, value: no-store }]
        backends: [{ upstream: web, weight: 1 }]
  - name: everything-else
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: / } }]
        backends: [{ upstream: web, weight: 1 }]
upstreams:
  web: { endpoints: ["127.0.0.1:9000"] }
  checkout: { endpoints: ["127.0.0.1:9001", "[::1]:9001"] }
  checkout-canary: { endpoints: [] }
"#;

    #[test]
    fn a_config_compiles_into_a_router_that_finds_the_rule() {
        let compiled = compile(&config(SHOP)).unwrap();
        let shop = "shop.example.com";
        assert_eq!(
            route_with(&compiled, "POST", shop, "/checkout", &[]),
            Some((0, 0))
        );
        assert_eq!(route(&compiled, shop, "/checkout"), Some((0, 1)));
        assert_eq!(
            route_with(
                &compiled,
                "GET",
                shop,
                "/cart/1?tenant=acme",
                &[("x-beta", "on")]
            ),
            Some((0, 0))
        );
        assert_eq!(route(&compiled, shop, "/cart/1?tenant=acme"), Some((0, 1)));
        assert_eq!(route(&compiled, shop, "/orders/42"), Some((0, 1)));
        assert_eq!(
            route(&compiled, "eu.west.shop.example.com", "/x"),
            Some((0, 1))
        );
        assert_eq!(route(&compiled, "example.org", "/x"), Some((1, 0)));
    }

    #[test]
    fn a_rule_leads_to_its_upstreams_in_proportion_to_the_weights() {
        let compiled = compile(&config(SHOP)).unwrap();
        let rule = compiled.rule(RuleId { route: 0, rule: 0 }).unwrap();
        let picked: Vec<&str> = (0..10)
            .map(|point| {
                let upstream = rule.backends.pick(point).unwrap();
                compiled.upstream(upstream).unwrap().name.as_str()
            })
            .collect();
        assert_eq!(picked[..9], ["checkout"; 9]);
        assert_eq!(picked[9], "checkout-canary");

        let checkout = compiled
            .upstreams
            .iter()
            .find(|u| u.name == "checkout")
            .unwrap();
        let endpoints: Vec<String> = checkout.endpoints.iter().map(ToString::to_string).collect();
        assert_eq!(endpoints, ["127.0.0.1:9001", "[::1]:9001"]);
        // An upstream may have nowhere to connect: that is a state, not a mistake.
        let canary = compiled
            .upstreams
            .iter()
            .find(|u| u.name == "checkout-canary")
            .unwrap();
        assert!(canary.endpoints.is_empty());

        assert!(compiled.rule(RuleId { route: 0, rule: 2 }).is_none());
        assert!(compiled.rule(RuleId { route: 2, rule: 0 }).is_none());
    }

    #[test]
    fn a_rule_carries_its_header_modifiers_and_nothing_for_none() {
        let compiled = compile(&config(SHOP)).unwrap();
        let plain = compiled.rule(RuleId { route: 0, rule: 0 }).unwrap();
        assert!(plain.request_headers.is_none());
        assert!(plain.response_headers.is_none());

        let filtered = compiled.rule(RuleId { route: 0, rule: 1 }).unwrap();
        let mut request = HeaderMap::new();
        request.insert("x-debug", "1".parse().unwrap());
        request.insert("x-gateway", "spoofed".parse().unwrap());
        filtered
            .request_headers
            .as_ref()
            .unwrap()
            .apply(&mut request);
        assert_eq!(request.get("x-gateway").unwrap(), "edgerush");
        assert!(!request.contains_key("x-debug"));

        let mut response = HeaderMap::new();
        filtered
            .response_headers
            .as_ref()
            .unwrap()
            .apply(&mut response);
        assert_eq!(response.get("cache-control").unwrap(), "no-store");

        // A modifier with nothing in it is not kept.
        let empty = SHOP.replace("add: [{ name: cache-control, value: no-store }]", "add: []");
        let compiled = compile(&config(&empty)).unwrap();
        let filtered = compiled.rule(RuleId { route: 0, rule: 1 }).unwrap();
        assert!(filtered.request_headers.is_some());
        assert!(filtered.response_headers.is_none());
    }

    #[test]
    fn a_modifier_may_not_name_the_gateways_own_headers() {
        let reserved = SHOP.replace("remove: [x-debug]", "remove: [Host]").replace(
            "add: [{ name: cache-control, value: no-store }]",
            "add: [{ name: Transfer-Encoding, value: chunked }]",
        );
        let errors: Vec<String> = compile(&config(&reserved))
            .err()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            errors,
            [
                "route `shop`, rules[1], filters[0]: header `host` is the gateway's own and \
                 cannot be modified",
                "route `shop`, rules[1], filters[1]: header `transfer-encoding` is the \
                 gateway's own and cannot be modified",
            ]
        );
    }

    #[test]
    fn the_order_of_routes_is_the_last_tie_breaker() {
        let twins = r#"
listeners: { web: { address: "[::]:8080", protocol: http } }
routes:
  - name: older
    listeners: [web]
    hostnames: [{ name: a.example.com, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: /api } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: younger
    listeners: [web]
    hostnames: [{ name: a.example.com, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: /api } }], backends: [{ upstream: u, weight: 1 }] }]
upstreams: { u: { endpoints: [] } }
"#;
        let compiled = compile(&config(twins)).unwrap();
        assert_eq!(route(&compiled, "a.example.com", "/api/x"), Some((0, 0)));
    }

    #[test]
    fn wildcard_kind_and_fall_through_are_what_the_hostname_says() {
        let ingress_style = r#"
listeners: { web: { address: "[::]:8080", protocol: http } }
routes:
  - name: wildcard
    listeners: [web]
    hostnames: [{ name: "*.example.com", wildcard: one_label, falls_through: false }]
    rules: [{ matches: [{ path: { prefix: /shared } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: exact
    listeners: [web]
    hostnames: [{ name: a.example.com, wildcard: any_labels, falls_through: false }]
    rules: [{ matches: [{ path: { prefix: /own } }], backends: [{ upstream: u, weight: 1 }] }]
upstreams: { u: { endpoints: [] } }
"#;
        let compiled = compile(&config(ingress_style)).unwrap();
        assert_eq!(route(&compiled, "b.example.com", "/shared"), Some((0, 0)));
        assert_eq!(route(&compiled, "x.b.example.com", "/shared"), None);
        assert_eq!(route(&compiled, "a.example.com", "/own"), Some((1, 0)));
        assert_eq!(route(&compiled, "a.example.com", "/shared"), None);

        let gateway_style = ingress_style
            .replace("one_label", "any_labels")
            .replace("falls_through: false", "falls_through: true");
        let compiled = compile(&config(&gateway_style)).unwrap();
        assert_eq!(route(&compiled, "x.b.example.com", "/shared"), Some((0, 0)));
        assert_eq!(route(&compiled, "a.example.com", "/shared"), Some((0, 0)));
    }

    #[test]
    fn every_problem_is_reported_with_its_place() {
        let broken = r#"
listeners:
  web: { address: "[::]:8080", protocol: http }
  web-again: { address: "[::]:8080", protocol: http }
routes:
  - name: a
    listeners: []
    hostnames: []
    rules: []
  - name: a
    listeners: [web, wbe, web]
    hostnames:
      - { name: "*.example.com", falls_through: true }
      - { name: "exa mple.com", falls_through: true }
    rules:
      - matches: []
        filters:
          - { type: request_header_modifier, set: [{ name: x-a, value: "1" }] }
          - { type: response_header_modifier, set: [{ name: x-a, value: "1" }], remove: [X-A] }
          - { type: request_header_modifier, add: [{ name: "x y", value: "1" }] }
        backends: []
      - matches:
          - path: { prefix: /ok }
          - path: { exact: no-slash }
            method: get
            headers: [{ name: "x y", value: { exact: "1" } }]
            query: [{ name: "", value: { regex: "(" } }]
        backends:
          - { upstream: web, weight: 1 }
          - { upstream: wbe, weight: 1 }
upstreams:
  web: { endpoints: [] }
"#;
        let errors: Vec<String> = compile(&config(broken))
            .err()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            errors,
            [
                "listener `web-again`: address [::]:8080 is already taken by listener `web`",
                "route `a`: no listeners",
                "route `a`: no hostnames; every host is the name `*`",
                "route `a`: no rules",
                "route `a`: another route has the same name",
                "route `a`: there is no listener `wbe`",
                "route `a`: listener `web` is listed twice",
                "route `a`: hostname `*.example.com` is a wildcard; `wildcard` must say \
                 `one_label` or `any_labels`",
                "route `a`: hostname `exa mple.com`: hostname contains invalid character ' '",
                "route `a`, rules[0]: no matches; any path is `{ prefix: / }`",
                "route `a`, rules[0], filters[1]: header `x-a` is named more than once",
                "route `a`, rules[0], filters[2]: a rule may have one `request_header_modifier`",
                "route `a`, rules[0], filters[2]: `x y` is not a header name",
                "route `a`, rules[0]: no backends",
                "route `a`, rules[1], matches[1]: path: path does not start with `/`",
                "route `a`, rules[1], matches[1]: method `get` is not an HTTP method in upper \
                 case",
                "route `a`, rules[1], matches[1]: header `x y`: invalid header name",
                "route `a`, rules[1], matches[1]: query parameter ``: query parameter name is \
                 empty",
                "route `a`, rules[1], backends[1]: there is no upstream `wbe`",
            ]
        );
    }

    #[test]
    fn there_is_no_shorthand_and_nothing_unknown_is_let_through() {
        let parse = |routes: &str| {
            let yaml = format!("listeners: {{}}\nroutes: {routes}\nupstreams: {{}}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(parse("[{ name: a, listeners: [], hostnames: [], rules: [] }]").is_ok());
        // A hostname is always the full statement.
        assert!(
            parse("[{ name: a, listeners: [], hostnames: [a.example.com], rules: [] }]").is_err()
        );
        assert!(
            parse("[{ name: a, listeners: [], hostnames: [{ name: a.example.com }], rules: [] }]")
                .is_err()
        );
        // A backend always states its weight; a match its path or its gRPC method, which
        // compiling holds it to (`a_match_says_its_path_or_its_grpc_method`).
        let rule =
            |rule: &str| format!("[{{ name: a, listeners: [], hostnames: [], rules: [{rule}] }}]");
        assert!(parse(&rule("{ matches: [], backends: [] }")).is_ok());
        assert!(parse(&rule("{ matches: [], backends: [{ upstream: u }] }")).is_err());
        assert!(parse(&rule("{ matches: [] }")).is_err());
        // Misspelt keys are errors, not silence.
        assert!(
            parse("[{ name: a, listeners: [], hostnames: [], rules: [], rulez: [] }]").is_err()
        );
        assert!(parse(&rule("{ matches: [], backends: [], filter: [] }")).is_err());
        // A filter is of a kind the model knows and has no keys it does not.
        let filter = |filter: &str| {
            rule(&format!(
                "{{ matches: [], filters: [{filter}], backends: [] }}"
            ))
        };
        assert!(parse(&filter("{ type: request_header_modifier }")).is_ok());
        assert!(parse(&filter("{ type: request_header_modifier, remove: [x] }")).is_ok());
        assert!(parse(&filter("{ type: request_header_modifier, removes: [x] }")).is_err());
        assert!(parse(&filter("{ type: header_modifier }")).is_err());
        assert!(parse(&filter("{ set: [] }")).is_err());
        assert!(
            parse(&filter(
                "{ type: request_header_modifier, set: [{ name: x }] }"
            ))
            .is_err()
        );
        assert!(parse(&rule("{ matches: [{ path: { glob: /a } }], backends: [] }")).is_err());
        // An endpoint is an address, not a name.
        let upstream = |endpoint: &str| {
            let yaml = format!(
                "listeners: {{}}\nroutes: []\nupstreams: {{ u: {{ endpoints: [\"{endpoint}\"] }} }}\n"
            );
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(upstream("10.0.0.1:80").is_ok());
        // Spoken to in HTTP/1.1 unless it says otherwise, as a Kubernetes Service port
        // without an application protocol is.
        let spoken = |upstream: &str| {
            let yaml = format!("listeners: {{}}\nroutes: []\nupstreams: {{ u: {upstream} }}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|config| config.upstreams["u"].protocol)
        };
        assert_eq!(
            spoken("{ endpoints: [] }").unwrap(),
            UpstreamProtocol::Http1
        );
        assert_eq!(
            spoken("{ endpoints: [], protocol: http1 }").unwrap(),
            UpstreamProtocol::Http1
        );
        assert_eq!(
            spoken("{ endpoints: [], protocol: http2 }").unwrap(),
            UpstreamProtocol::Http2
        );
        assert!(spoken("{ endpoints: [], protocol: h2c }").is_err());
        assert!(upstream("localhost:80").is_err());
        assert!(upstream("10.0.0.1").is_err());

        // A listener speaks a protocol the model knows, on an address.
        let listener = |listener: &str| {
            let yaml = format!("listeners: {{ l: {listener} }}\nroutes: []\nupstreams: {{}}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(listener(r#"{ address: "[::]:80", protocol: http }"#).is_ok());
        assert!(listener(r#"{ address: "[::]:80", protocol: gopher }"#).is_err());
        assert!(listener(r#"{ address: "[::]:80" }"#).is_err());
        assert!(listener(r#"{ address: ":80", protocol: http }"#).is_err());
    }

    /// An `https` listener has certificates, at least one; nothing else has any. What is in
    /// them is for the data plane to read: the model carries them as they came.
    #[test]
    fn tls_is_for_https_listeners_and_every_one_has_it() {
        let with = |listener: &str| {
            config(&format!(
                "listeners: {{ l: {listener} }}\nroutes: []\nupstreams: {{}}\n"
            ))
        };
        let one = r#"{ certificates: [{ chain: "C", key: "K" }] }"#;

        let compiled = compile(&with(&format!(
            r#"{{ address: "[::]:443", protocol: https, tls: {one} }}"#
        )))
        .unwrap();
        assert_eq!(compiled.listeners[0].protocol, Protocol::Https);
        let tls = compiled.listeners[0].tls.as_ref().unwrap();
        assert_eq!(tls.certificates.len(), 1);
        assert_eq!(tls.certificates[0].chain, "C");
        assert_eq!(tls.certificates[0].key, "K");
        assert_eq!(tls.client_validation, None);
        let validated = compile(&with(
            r#"{ address: "[::]:443", protocol: https, tls: { certificates: [{ chain: "C", key: "K" }], client_validation: { authorities: ["CA"] } } }"#,
        ))
        .unwrap();
        let validation = validated.listeners[0].tls.as_ref().unwrap();
        assert_eq!(
            validation.client_validation.as_ref().unwrap().authorities,
            ["CA"]
        );
        assert!(
            compile(&with(
                r#"{ address: "[::]:443", protocol: https, tls: { certificates: [{ chain: "C", key: "K" }], client_validation: { authorities: [] } } }"#,
            ))
            .unwrap_err()[0]
                .to_string()
                .ends_with("`tls` needs an authority to trust")
        );

        let problems = |listener: String| -> Vec<String> {
            compile(&with(&listener))
                .unwrap_err()
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        assert_eq!(
            problems(r#"{ address: "[::]:443", protocol: https }"#.to_owned()),
            ["listener `l`: protocol `https` needs `tls` with a certificate"]
        );
        assert_eq!(
            problems(
                r#"{ address: "[::]:443", protocol: https, tls: { certificates: [] } }"#.to_owned()
            ),
            ["listener `l`: protocol `https` needs `tls` with a certificate"]
        );
        assert_eq!(
            problems(format!(
                r#"{{ address: "[::]:80", protocol: http, tls: {one} }}"#
            )),
            ["listener `l`: `tls` is for protocol `https`"]
        );
        // A certificate is its chain and its key, both said.
        let yaml = r#"listeners: { l: { address: "[::]:443", protocol: https, tls: { certificates: [{ chain: "C" }] } } }
routes: []
upstreams: {}
"#;
        assert!(serde_saphyr::from_str::<Config>(yaml).is_err());
    }

    /// An `https` listener may serve HTTP/3 as well, on the same port over UDP, and says so
    /// to its TCP clients for a day unless it gives a lifetime of its own; no other may.
    #[test]
    fn http3_is_for_https_listeners() {
        let with = |listener: &str| {
            config(&format!(
                "listeners: {{ l: {listener} }}
routes: []
upstreams: {{}}
"
            ))
        };
        let https = r#"address: "[::]:443", protocol: https, tls: { certificates: [{ chain: "C", key: "K" }] }"#;

        let compiled = compile(&with(&format!("{{ {https}, http3: {{}} }}"))).unwrap();
        assert_eq!(
            compiled.listeners[0].http3,
            Some(Http3 {
                alt_svc_max_age: 86_400
            })
        );
        let compiled = compile(&with(&format!(
            "{{ {https}, http3: {{ alt_svc_max_age: 60 }} }}"
        )))
        .unwrap();
        assert_eq!(compiled.listeners[0].http3.unwrap().alt_svc_max_age, 60);
        let compiled = compile(&with(&format!("{{ {https} }}"))).unwrap();
        assert_eq!(compiled.listeners[0].http3, None);

        let problems: Vec<String> = compile(&with(
            r#"{ address: "[::]:80", protocol: http, http3: {} }"#,
        ))
        .unwrap_err()
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(problems, ["listener `l`: `http3` is for protocol `https`"]);
        let yaml = format!(
            "listeners: {{ l: {{ {https}, http3: {{ max_age: 60 }} }} }}
routes: []
upstreams: {{}}
"
        );
        assert!(serde_saphyr::from_str::<Config>(&yaml).is_err());
    }

    fn listener<'a>(compiled: &'a Compiled, name: &str) -> &'a CompiledListener {
        compiled.listeners.iter().find(|l| l.name == name).unwrap()
    }

    /// A `tcp` listener's connections all go to its one route; a `tls` listener's by the
    /// name asked for, the most specific hostname first and the first route among equals;
    /// an HTTP listener has neither.
    #[test]
    fn passthrough_listeners_route_by_listener_and_by_name() {
        let compiled = compile(&config(
            r#"
listeners:
  web: { address: "[::]:80", protocol: http }
  db: { address: "[::]:5432", protocol: tcp, tunnel_idle_seconds: 90 }
  sni: { address: "[::]:8443", protocol: tls }
routes: []
tcp_routes:
  - { name: db, listeners: [db], backends: [{ upstream: postgres, weight: 1 }] }
tls_routes:
  - name: api
    listeners: [sni]
    hostnames: [{ name: api.example.com, falls_through: true }]
    backends: [{ upstream: api, weight: 1 }]
  - name: rest
    listeners: [sni]
    hostnames: [{ name: "*.example.com", wildcard: any_labels, falls_through: true }]
    backends: [{ upstream: api, weight: 1 }]
  - name: also-api
    listeners: [sni]
    hostnames: [{ name: api.example.com, falls_through: true }]
    backends: [{ upstream: postgres, weight: 1 }]
upstreams:
  api: { endpoints: ["10.0.0.2:443"] }
  postgres: { endpoints: ["10.0.0.1:5432"] }
"#,
        ))
        .unwrap();
        let Some(L4::Tcp(db)) = &listener(&compiled, "db").l4 else {
            panic!("db is a tcp listener");
        };
        assert_eq!(db.name, "db");
        assert_eq!(db.backends.pick(0), Some(UpstreamId(1)));
        let Some(L4::Tls(sni)) = &listener(&compiled, "sni").l4 else {
            panic!("sni is a tls listener");
        };
        let route = |name: &str| sni.route(name).map(|route| route.name.as_str());
        assert_eq!(route("api.example.com"), Some("api"));
        assert_eq!(route("www.example.com"), Some("rest"));
        assert_eq!(route("a.b.example.com"), Some("rest"));
        assert_eq!(route("example.com"), None);
        assert_eq!(route("elsewhere.test"), None);
        assert!(listener(&compiled, "web").l4.is_none());
        // A tunnel's idle bound, as said, or an hour.
        assert_eq!(
            listener(&compiled, "db").tunnel_idle,
            Duration::from_secs(90)
        );
        assert_eq!(
            listener(&compiled, "sni").tunnel_idle,
            Duration::from_secs(3_600)
        );
    }

    /// A route goes only to listeners of its kind, a `tcp` listener has exactly one, a
    /// passthrough listener presents no certificate, and a passthrough route has 1 to 16
    /// backends that take plain bytes and a name no other route has.
    #[test]
    fn passthrough_routes_are_held_to_their_listeners_and_backends() {
        let refused = |listeners: &str, routes: &str| -> Vec<String> {
            let yaml = format!(
                "listeners: {{ {listeners} }}\n{routes}\nupstreams: {{ up: {{ endpoints: [] }}, h2: {{ endpoints: [], protocol: http2 }} }}\n"
            );
            compile(&config(&yaml))
                .err()
                .unwrap_or_default()
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        let tcp = r#"t: { address: "[::]:1", protocol: tcp }"#;
        let tls = r#"s: { address: "[::]:2", protocol: tls }"#;
        let http = r#"h: { address: "[::]:3", protocol: http }"#;
        let tcp_route = |name: &str, listeners: &str, backends: &str| {
            format!("{{ name: {name}, listeners: [{listeners}], backends: [{backends}] }}")
        };
        let tls_route = |name: &str, listeners: &str, hostnames: &str| {
            format!(
                "{{ name: {name}, listeners: [{listeners}], hostnames: [{hostnames}], backends: [{{ upstream: up, weight: 1 }}] }}"
            )
        };
        let up = "{ upstream: up, weight: 1 }";
        let a_test = "{ name: a.test, falls_through: true }";

        assert_eq!(
            refused(tcp, "routes: []"),
            ["listener `t`: a `tcp` listener needs exactly one TCP route; it has 0"]
        );
        assert_eq!(
            refused(
                tcp,
                &format!(
                    "routes: []\ntcp_routes: [{}, {}]",
                    tcp_route("a", "t", up),
                    tcp_route("b", "t", up)
                )
            ),
            ["listener `t`: a `tcp` listener needs exactly one TCP route; it has 2"]
        );
        assert_eq!(
            refused(
                &format!("{tcp}, {tls}"),
                &format!("routes: []\ntcp_routes: [{}]", tcp_route("a", "t, s", up))
            ),
            ["route `a`: a TCP route cannot be for listener `s`, which is `tls`"]
        );
        assert_eq!(
            refused(
                &format!("{tls}, {http}"),
                &format!("routes: []\ntls_routes: [{}]", tls_route("a", "h", a_test))
            ),
            ["route `a`: a TLS route cannot be for listener `h`, which is `http`"]
        );
        assert_eq!(
            refused(
                tcp,
                &format!(
                    "routes:\n  - {{ name: web, listeners: [t], hostnames: [{{ name: \"*\", falls_through: true }}], rules: [{{ matches: [{{ path: {{ prefix: / }} }}], backends: [{up}] }}] }}\ntcp_routes: [{}]",
                    tcp_route("a", "t", up)
                )
            ),
            ["route `web`: a HTTP route cannot be for listener `t`, which is `tcp`"]
        );
        assert_eq!(
            refused(
                tls,
                &format!("routes: []\ntls_routes: [{}]", tls_route("a", "s", ""))
            ),
            ["route `a`: no hostnames; every host is the name `*`"]
        );
        assert_eq!(
            refused(
                tcp,
                &format!("routes: []\ntcp_routes: [{}]", tcp_route("a", "t", ""))
            ),
            ["route `a`: no backends"]
        );
        let seventeen = vec![up; 17].join(", ");
        assert_eq!(
            refused(
                tcp,
                &format!(
                    "routes: []\ntcp_routes: [{}]",
                    tcp_route("a", "t", &seventeen)
                )
            ),
            ["route `a`: a TCP or TLS route has at most 16 backends"]
        );
        assert_eq!(
            refused(
                tcp,
                &format!(
                    "routes: []\ntcp_routes: [{}]",
                    tcp_route("a", "t", "{ upstream: h2, weight: 1 }")
                )
            ),
            [
                "route `a`, backends[0]: upstream `h2` is spoken to in HTTP/2 or over TLS, which a TCP or TLS route does not do"
            ]
        );
        assert_eq!(
            refused(
                r#"t: { address: "[::]:1", protocol: tcp, tls: { certificates: [{ chain: C, key: K }] } }"#,
                &format!("routes: []\ntcp_routes: [{}]", tcp_route("a", "t", up))
            ),
            ["listener `t`: `tls` is for protocol `https`"]
        );
        assert_eq!(
            refused(
                &format!("{tcp}, {tls}"),
                &format!(
                    "routes: []\ntcp_routes: [{}]\ntls_routes: [{}]",
                    tcp_route("a", "t", up),
                    tls_route("a", "s", a_test)
                )
            ),
            ["route `a`: another route has the same name"]
        );
        // An idle bound for tunnels only a passthrough listener has, and never of nothing.
        assert_eq!(
            refused(
                &format!(
                    r#"{http}, w: {{ address: "[::]:4", protocol: http, tunnel_idle_seconds: 60 }}"#
                ),
                "routes: []"
            ),
            ["listener `w`: `tunnel_idle_seconds` is for protocols `tcp` and `tls`"]
        );
        assert_eq!(
            refused(
                r#"t: { address: "[::]:1", protocol: tcp, tunnel_idle_seconds: 0 }"#,
                &format!("routes: []\ntcp_routes: [{}]", tcp_route("a", "t", up))
            ),
            ["listener `t`: `tunnel_idle_seconds` must be at least 1"]
        );
    }

    /// TLS to an upstream names the server it expects and trusts at least one authority to
    /// vouch for it; the server's name is a host name, nothing more.
    #[test]
    fn tls_to_an_upstream_names_its_server_and_whom_to_trust() {
        let with = |tls: &str| {
            config(&format!(
                "listeners: {{}}\nroutes: []\nupstreams: {{ u: {{ endpoints: [], tls: {tls} }} }}\n"
            ))
        };
        let compiled = compile(&with(
            r#"{ server_name: backend.internal, authorities: ["CA"] }"#,
        ))
        .unwrap();
        let tls = compiled.upstreams[0].tls.as_ref().unwrap();
        assert_eq!(tls.server_name, "backend.internal");
        assert_eq!(tls.authorities, ["CA"]);

        let problems = |tls: &str| -> Vec<String> {
            compile(&with(tls))
                .unwrap_err()
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        assert_eq!(
            problems(r#"{ server_name: backend.internal, authorities: [] }"#),
            ["upstream `u`: `tls` needs an authority to trust"]
        );
        for name in [
            "\"\"",
            "\"backend.internal:443\"",
            "\"a b\"",
            "\"*.internal\"",
        ] {
            assert_eq!(
                problems(&format!(
                    r#"{{ server_name: {name}, authorities: ["CA"] }}"#
                )),
                [format!(
                    "upstream `u`: server name `{}` is not a host name",
                    name.trim_matches('"')
                )]
            );
        }
        // Both are said.
        let yaml = "listeners: {}\nroutes: []\nupstreams: { u: { endpoints: [], tls: { server_name: a.b } } }\n";
        assert!(serde_saphyr::from_str::<Config>(yaml).is_err());
    }

    /// A match states its path or its gRPC method: one of them, and not both.
    #[test]
    fn a_match_says_its_path_or_its_grpc_method() {
        let with = |matching: &str| {
            let yaml = format!(
                r#"
listeners: {{ web: {{ address: "[::]:80", protocol: http }} }}
routes:
  - name: r
    listeners: [web]
    hostnames: [{{ name: "*", falls_through: true }}]
    rules: [{{ matches: [{matching}], backends: [{{ upstream: u, weight: 1 }}] }}]
upstreams: {{ u: {{ endpoints: [] }} }}
"#
            );
            compile(&config(&yaml))
                .map(|_| ())
                .map_err(|errors| errors.iter().map(ToString::to_string).collect::<Vec<_>>())
        };
        assert!(with("{ path: { prefix: / } }").is_ok());
        assert!(with("{ grpc: { service: pkg.Svc } }").is_ok());
        let problem = |matching: &str| with(matching).unwrap_err()[0].clone();
        assert!(
            problem("{ method: GET }")
                .ends_with("a match needs `path` or `grpc`; any path is `{ prefix: / }`")
        );
        assert!(
            problem("{ path: { prefix: / }, grpc: { service: pkg.Svc } }")
                .ends_with("a match has `path` or `grpc`, not both")
        );
        for bad in [
            "{ grpc: {} }",
            "{ grpc: { service: \"pkg/Svc\" } }",
            "{ grpc: { service: \"pkg..Svc\" } }",
            "{ grpc: { method: \"1Do\" } }",
            "{ grpc: { method: \"Do.It\" } }",
            "{ grpc: { service: \"pkg.Svc\", method: \"Do it\" } }",
        ] {
            assert!(
                problem(bad).contains("are not names a call could have"),
                "{bad}"
            );
        }
    }

    /// A gRPC method match is the path the call would have: both names as that path, a
    /// service alone as its methods, a method alone in any service. Precedence follows
    /// from it as GRPCRoute has it: service before method, exact before either.
    #[test]
    fn a_grpc_method_matches_as_the_path_its_calls_have() {
        let compiled = compile(&config(
            r#"
listeners: { web: { address: "[::]:80", protocol: http } }
routes:
  - name: r
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ grpc: { method: Do } }]
        backends: [{ upstream: u, weight: 1 }]
      - matches: [{ grpc: { service: pkg.Svc } }]
        backends: [{ upstream: u, weight: 1 }]
      - matches: [{ grpc: { service: pkg.Svc, method: Do } }]
        backends: [{ upstream: u, weight: 1 }]
upstreams: { u: { endpoints: [] } }
"#,
        ))
        .unwrap();
        let rule = |target: &str| route(&compiled, "a.test", target).map(|(_, rule)| rule);
        // Listed least specific first: which wins is precedence's doing, not the order's.
        assert_eq!(rule("/pkg.Svc/Do"), Some(2));
        assert_eq!(rule("/pkg.Svc/Other"), Some(1));
        assert_eq!(rule("/other.Svc/Do"), Some(0));
        assert_eq!(rule("/other.Svc/Other"), None);
        assert_eq!(rule("/pkg.SvcX/Other"), None);
    }

    /// PINGs are for HTTP/2 upstreams, and no more often than gRPC servers take unless the
    /// backend is said to take more.
    #[test]
    fn keepalive_is_for_http2_upstreams_and_no_more_often_than_they_take() {
        let with = |upstream: &str| {
            let yaml = format!("listeners: {{}}\nroutes: []\nupstreams: {{ u: {upstream} }}\n");
            compile(&config(&yaml))
                .map(|compiled| compiled.upstreams[0].keepalive)
                .map_err(|errors| errors.iter().map(ToString::to_string).collect::<Vec<_>>())
        };
        let ok = with("{ endpoints: [], protocol: http2, keepalive: { interval_seconds: 300, timeout_seconds: 20, without_calls: false } }")
            .unwrap()
            .unwrap();
        assert_eq!(ok.interval_seconds, 300);
        assert!(with("{ endpoints: [], protocol: http2, keepalive: { interval_seconds: 10, timeout_seconds: 5, without_calls: true, backend_allows_short_intervals: true } }").is_ok());
        let problem = |upstream: &str| with(upstream).unwrap_err()[0].clone();
        assert!(problem("{ endpoints: [], keepalive: { interval_seconds: 300, timeout_seconds: 20, without_calls: false } }")
            .ends_with("`keepalive` is for an upstream spoken to in HTTP/2"));
        assert!(problem("{ endpoints: [], protocol: http2, keepalive: { interval_seconds: 60, timeout_seconds: 20, without_calls: false } }")
            .contains("more often than the 300 s gRPC servers take"));
        assert!(problem("{ endpoints: [], protocol: http2, keepalive: { interval_seconds: 0, timeout_seconds: 20, without_calls: false, backend_allows_short_intervals: true } }")
            .ends_with("needs an interval and a timeout of at least a second"));
    }

    /// A health check says everything it does, and nothing that cannot work.
    #[test]
    fn a_health_check_says_everything_and_nothing_that_cannot_work() {
        let with = |upstream: &str| {
            let yaml = format!("listeners: {{}}\nroutes: []\nupstreams: {{ u: {upstream} }}\n");
            compile(&config(&yaml))
                .map(|compiled| compiled.upstreams[0].health_check.clone())
                .map_err(|errors| errors.iter().map(ToString::to_string).collect::<Vec<_>>())
        };
        let check = |fields: &str| {
            format!(
                "{{ endpoints: [], protocol: http1, health_check: {{ interval_seconds: 5, timeout_seconds: 2, healthy_threshold: 2, unhealthy_threshold: 3, {fields} }} }}"
            )
        };
        let http = with(&check("probe: { http: { path: /healthz } }"))
            .unwrap()
            .unwrap();
        assert_eq!(
            http.probe,
            Probe::Http {
                path: "/healthz".to_owned()
            }
        );
        let grpc = check("probe: { grpc: { service: \"\" } }").replace("http1", "http2");
        assert!(with(&grpc).is_ok());

        let problem = |upstream: &str| with(upstream).unwrap_err()[0].clone();
        assert!(
            problem(&check("probe: { grpc: { service: x } }"))
                .ends_with("a gRPC health check is for an upstream spoken to in HTTP/2")
        );
        assert!(
            problem(&check("probe: { http: { path: healthz } }"))
                .ends_with("health check path `healthz` does not start with `/`")
        );
        assert!(
            problem(
                &check("probe: { http: { path: / } }")
                    .replace("timeout_seconds: 2", "timeout_seconds: 9")
            )
            .ends_with("a health check's timeout is longer than its interval")
        );
        assert!(
            problem(
                &check("probe: { http: { path: / } }")
                    .replace("healthy_threshold: 2", "healthy_threshold: 0")
            )
            .ends_with("thresholds of at least one")
        );
        // Nothing is left to a default.
        let missing = "{ endpoints: [], health_check: { interval_seconds: 5, timeout_seconds: 2, probe: { http: { path: / } } } }";
        let yaml = format!("listeners: {{}}\nroutes: []\nupstreams: {{ u: {missing} }}\n");
        assert!(serde_saphyr::from_str::<Config>(&yaml).is_err());
    }

    /// A rule's retries say when and how often, and nothing that could not work.
    #[test]
    fn a_mirror_goes_to_an_upstream_there_is_for_a_share_there_can_be() {
        let with = |mirror: &str| {
            let yaml = format!(
                r#"
listeners: {{ web: {{ address: "[::]:80", protocol: http }} }}
routes:
  - name: r
    listeners: [web]
    hostnames: [{{ name: "*", falls_through: true }}]
    rules: [{{ matches: [{{ path: {{ prefix: / }} }}], backends: [{{ upstream: u, weight: 1 }}], filters: [{mirror}] }}]
upstreams: {{ u: {{ endpoints: [] }}, shadow: {{ endpoints: [] }} }}
"#
            );
            compile(&config(&yaml))
                .map(|compiled| {
                    compiled
                        .rule(RuleId { route: 0, rule: 0 })
                        .unwrap()
                        .mirrors
                        .clone()
                })
                .map_err(|errors| errors.iter().map(ToString::to_string).collect::<Vec<_>>())
        };
        let mirror = |upstream: &str, numerator: u32, denominator: u32| {
            format!(
                "{{ type: request_mirror, upstream: {upstream}, fraction: {{ numerator: {numerator}, denominator: {denominator} }} }}"
            )
        };
        let all = with(&mirror("shadow", 1, 1)).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(
            all[0].upstream,
            UpstreamId(0),
            "upstreams are numbered by name"
        );
        assert!((0..100).all(|random| all[0].takes(random)));
        let quarter = with(&mirror("shadow", 1, 4)).unwrap();
        assert_eq!(
            (0..400).filter(|&random| quarter[0].takes(random)).count(),
            100
        );
        // Two mirrors, both kept, in order.
        let two = with(&format!(
            "{}, {}",
            mirror("shadow", 1, 1),
            mirror("u", 1, 2)
        ))
        .unwrap();
        assert_eq!(two.len(), 2);
        assert_eq!(two[1].upstream, UpstreamId(1));
        // None of them: nothing to do.
        assert!(with(&mirror("shadow", 0, 1)).unwrap().is_empty());

        let problems = |mirror: &str| with(mirror).unwrap_err();
        assert!(problems(&mirror("elsewhere", 1, 1))[0].contains("`elsewhere`"));
        assert!(problems(&mirror("shadow", 1, 0))[0].contains("1/0"));
        assert!(problems(&mirror("shadow", 2, 1))[0].contains("2/1"));
        assert!(
            serde_saphyr::from_str::<Filter>("{ type: request_mirror, upstream: shadow }").is_err(),
            "a share is stated"
        );
    }

    #[test]
    fn a_retry_says_when_and_how_often_and_nothing_that_cannot_work() {
        let with = |retry: &str| {
            let yaml = format!(
                r#"
listeners: {{ web: {{ address: "[::]:80", protocol: http }} }}
routes:
  - name: r
    listeners: [web]
    hostnames: [{{ name: "*", falls_through: true }}]
    rules: [{{ matches: [{{ path: {{ prefix: / }} }}], backends: [{{ upstream: u, weight: 1 }}], retry: {retry} }}]
upstreams: {{ u: {{ endpoints: [] }} }}
"#
            );
            compile(&config(&yaml))
                .map(|compiled| {
                    compiled
                        .rule(RuleId { route: 0, rule: 0 })
                        .unwrap()
                        .retry
                        .clone()
                })
                .map_err(|errors| errors.iter().map(ToString::to_string).collect::<Vec<_>>())
        };
        let retry = with("{ attempts: 2, http_statuses: [502, 503], grpc_statuses: [UNAVAILABLE], backoff_base_ms: 25, backoff_max_ms: 250 }")
            .unwrap()
            .unwrap();
        assert!(retry.on_status(503) && !retry.on_status(500));
        assert!(retry.on_grpc(14) && !retry.on_grpc(13) && !retry.on_grpc(99));
        assert_eq!(retry.backoff_base, std::time::Duration::from_millis(25));

        let problems = |retry: &str| with(retry).unwrap_err();
        let base = "backoff_base_ms: 25, backoff_max_ms: 250";
        assert!(
            problems(&format!("{{ attempts: 0, http_statuses: [503], {base} }}"))[0]
                .ends_with("`retry.attempts` is 0: from 1 to 5")
        );
        assert!(
            problems(&format!("{{ attempts: 6, http_statuses: [503], {base} }}"))[0]
                .contains("from 1 to 5")
        );
        assert!(
            problems(&format!("{{ attempts: 1, {base} }}"))[0]
                .ends_with("names no status to send a request again for")
        );
        assert!(
            problems(&format!("{{ attempts: 1, http_statuses: [200], {base} }}"))[0]
                .ends_with("only 4xx and 5xx")
        );
        assert!(
            problems(&format!("{{ attempts: 1, grpc_statuses: [OK], {base} }}"))[0]
                .contains("`OK`")
        );
        assert!(
            problems(&format!(
                "{{ attempts: 1, grpc_statuses: [unavailable], {base} }}"
            ))[0]
                .contains("`unavailable`")
        );
        assert!(
            problems(
                "{ attempts: 1, http_statuses: [503], backoff_base_ms: 300, backoff_max_ms: 250 }"
            )[0]
            .ends_with("no more than its most")
        );
    }

    /// A private key is never printed, whatever prints the config.
    #[test]
    fn a_private_key_is_not_shown() {
        let certificate = crate::Certificate {
            chain: "the chain".to_owned(),
            key: "the secret".to_owned(),
        };
        let shown = format!("{certificate:?}");
        assert!(shown.contains("the chain"), "{shown}");
        assert!(!shown.contains("the secret"), "{shown}");
    }

    #[test]
    fn every_listener_has_a_router_of_the_routes_that_are_for_it() {
        let two = r#"
listeners:
  public: { address: "[::]:8080", protocol: http }
  internal: { address: "127.0.0.1:9090", protocol: http }
  idle: { address: "127.0.0.1:9091", protocol: http }
routes:
  - name: site
    listeners: [public]
    hostnames: [{ name: "*", falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: metrics
    listeners: [internal, public]
    hostnames: [{ name: metrics.internal, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], backends: [{ upstream: u, weight: 1 }] }]
upstreams: { u: { endpoints: [] } }
"#;
        let compiled = compile(&config(two)).unwrap();
        let names: Vec<&str> = compiled.listeners.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["idle", "internal", "public"]);
        assert_eq!(compiled.listeners[2].address.to_string(), "[::]:8080");
        assert_eq!(compiled.listeners[2].protocol, Protocol::Http);

        assert_eq!(route_on(&compiled, "public", "example.com"), Some((0, 0)));
        assert_eq!(
            route_on(&compiled, "public", "metrics.internal"),
            Some((1, 0))
        );
        assert_eq!(
            route_on(&compiled, "internal", "metrics.internal"),
            Some((1, 0))
        );
        assert_eq!(route_on(&compiled, "internal", "example.com"), None);
        // A listener no route is for answers nothing, which is valid.
        assert_eq!(route_on(&compiled, "idle", "example.com"), None);
    }
}
