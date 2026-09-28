//! Routes as the model states them. These types say what was written; whether it makes
//! sense is for [`compile_routes`](crate::compile_routes) to find out.

use serde::Deserialize;

/// A route: the hosts it serves and its rules.
///
/// Routes come as an ordered list, and the order matters: it is the last tie-breaker of
/// precedence. The control plane emits them oldest first, then by name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// Unique among the routes; what errors, status and metrics refer to.
    pub name: String,
    /// The names of the listeners whose requests this route is for. At least one.
    pub listeners: Vec<String>,
    /// The hosts served. At least one: every host has to be asked for by the name `*`.
    pub hostnames: Vec<Hostname>,
    /// The rules, in order. At least one.
    pub rules: Vec<Rule>,
}

/// A route for a `tcp` listener: every connection it takes goes to these backends, as
/// Gateway API's TCPRoute has it (17 in the docs).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpRoute {
    /// Unique among every kind of route.
    pub name: String,
    /// The names of the `tcp` listeners it is for. At least one.
    pub listeners: Vec<String>,
    /// Where connections go, in proportion to the weights: 1 to 16.
    pub backends: Vec<Backend>,
}

/// A route for a `tls` listener: the connections whose ClientHello asks for a name its
/// hostnames cover, as Gateway API's TLSRoute has it in passthrough mode.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsRoute {
    /// Unique among every kind of route.
    pub name: String,
    /// The names of the `tls` listeners it is for. At least one.
    pub listeners: Vec<String>,
    /// The names served, matched against the SNI as a request's host is against an HTTP
    /// route's. At least one.
    pub hostnames: Vec<Hostname>,
    /// Where connections go, in proportion to the weights: 1 to 16.
    pub backends: Vec<Backend>,
}

/// A claim on hosts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hostname {
    /// An exact name, a wildcard (`*.example.com`), or `*` for every host.
    pub name: String,
    /// What the `*` of a wildcard name stands for. Required for a wildcard name; without
    /// meaning, and ignored, for any other.
    #[serde(default)]
    pub wildcard: Option<Wildcard>,
    /// Whether this claim stays a candidate on hosts that a more specific claim also
    /// covers: Gateway API requires it, nginx-style Ingress does not want it.
    pub falls_through: bool,
}

/// What the `*` of a wildcard hostname stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Wildcard {
    /// Exactly one label, as in Ingress.
    OneLabel,
    /// One or more labels, as in Gateway API.
    AnyLabels,
}

/// A rule: the requests it is for, what is done to them on the way, and what becomes of
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// The rule is for a request that satisfies any one of these. At least one.
    pub matches: Vec<Match>,
    /// What is done to a request and its response. Each kind of filter at most once.
    #[serde(default)]
    pub filters: Vec<Filter>,
    /// Its requests sent on to its backends. A rule says this or `redirect`: one of the
    /// two, never both.
    #[serde(default)]
    pub forward: Option<Forward>,
    /// Its requests answered with a redirect, said in place of `forward`.
    #[serde(default)]
    pub redirect: Option<Redirect>,
}

/// A redirect (Gateway API's `RequestRedirect`): the client is sent elsewhere, and nothing
/// goes upstream. The `Location` holds what is said here and only what it needs besides:
/// relative when none of scheme, host and port is said, and otherwise absolute, the scheme
/// unsaid taken from the listener and the host from the request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Redirect {
    /// 301, 302, 303, 307 or 308.
    pub status: u16,
    /// The scheme to send the client to.
    #[serde(default)]
    pub scheme: Option<Scheme>,
    /// The host to send the client to: a DNS name in lower case.
    #[serde(default)]
    pub host: Option<String>,
    /// The port to send the client to; written only if it is not its scheme's default.
    #[serde(default)]
    pub port: Option<u16>,
    /// The path to send the client to, made from the request's.
    #[serde(default)]
    pub path: Option<PathChange>,
    /// Whether the request's query goes with it.
    pub query: Query,
}

/// A scheme a redirect sends a client to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scheme {
    /// `http`.
    Http,
    /// `https`.
    Https,
}

/// What becomes of a request's query when it is redirected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    /// It goes with it, byte for byte.
    Keep,
    /// It is left out.
    Drop,
}

/// A change to a request's path.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathChange {
    /// The whole path becomes this.
    ReplaceFull(String),
    /// The rule's one match, a prefix, is replaced by this, by whole segments; it may be
    /// empty.
    ReplacePrefix(String),
}

/// A rule's requests sent on to its backends.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Forward {
    /// Where requests go, in proportion to the weights. At least one.
    pub backends: Vec<Backend>,
    /// How long its requests may take; none leaves them to the data plane's fixed clocks.
    #[serde(default)]
    pub timeouts: Option<Timeouts>,
    /// Sending a request again when its answer says to; none is never.
    #[serde(default)]
    pub retry: Option<Retry>,
}

/// How long a rule's requests may take (Gateway API's HTTPRoute `timeouts`). A timeout
/// stated takes the place of the data plane's fixed clocks for an answer's head; the idle
/// clocks, which find data that stopped flowing, run whatever is stated. At least one.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timeouts {
    /// Milliseconds from the request's head arriving to its answer's end, upload, retries
    /// and all; `0` for no limit at all.
    #[serde(default)]
    pub request_ms: Option<u64>,
    /// Milliseconds a try may take, from its start — connecting, and waiting for a place on
    /// an HTTP/2 connection, included — to its answer's head; `0` for no limit at all. No
    /// more than `request_ms`. A try that runs out of it is sent again under a retry's
    /// `on_timeout`. It stops at the head, not the answer's end: the answer streams to the
    /// client as the client takes it, and a slow client is not the backend running late.
    #[serde(default)]
    pub backend_request_ms: Option<u64>,
}

/// When a request is sent again, and how often (Gateway API's HTTPRoute retry, with
/// gRPC's statuses beside HTTP's). Decided on an answer's head alone: once a head has gone
/// to the client, the request is not sent again. Within a budget of the upstream's, and
/// only for a body small enough to be kept.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    /// Times a request may be sent again, beyond the first.
    pub attempts: u32,
    /// HTTP statuses that send it again. `502` also stands for an upstream that could not
    /// be reached or answered nothing; a try that ran out of time is `on_timeout`'s, not
    /// `502`'s.
    #[serde(default)]
    pub http_statuses: Vec<u16>,
    /// gRPC statuses, by gRPC's names (`UNAVAILABLE`), that send a call again when the
    /// answer's head carries them.
    #[serde(default)]
    pub grpc_statuses: Vec<String>,
    /// Whether a try that ran out of time before its answer's head is sent again: the
    /// upstream's fixed clocks for a head, or for the request's upload or the answer to
    /// go on, running out. Stated whichever way, so that a config says what it does.
    pub on_timeout: bool,
    /// Milliseconds before the first retry; each after it waits twice as long, and as much
    /// again at random.
    pub backoff_base_ms: u64,
    /// The most milliseconds a wait may double to.
    pub backoff_max_ms: u64,
}

/// Something done to a request or its response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Filter {
    /// Changes the headers of the request before it goes to the upstream.
    RequestHeaderModifier(HeaderChanges),
    /// Changes the headers of the response before it goes to the client.
    ResponseHeaderModifier(HeaderChanges),
    /// Sends a copy of the request to another upstream too, and throws its answer away.
    RequestMirror(Mirror),
    /// Sends the upstream another host, another path or both.
    UrlRewrite(UrlRewrite),
}

/// A rewrite (Gateway API's `URLRewrite`): what the upstream is sent in place of what the
/// request was routed on. At least one of the two.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UrlRewrite {
    /// The `Host` the upstream is sent: a DNS name in lower case, with no port.
    #[serde(default)]
    pub host: Option<String>,
    /// The path the upstream is sent, made from the request's.
    #[serde(default)]
    pub path: Option<PathChange>,
}

/// A copy of some share of a rule's requests, sent to another upstream (Gateway API's
/// `RequestMirror`). The copy never holds the request up: one that falls behind is given
/// up on.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mirror {
    /// Where the copies go.
    pub upstream: String,
    /// The share of requests copied: every one is `{ numerator: 1, denominator: 1 }`.
    pub fraction: Fraction,
}

/// A share: `numerator` out of every `denominator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fraction {
    /// How many.
    pub numerator: u32,
    /// Out of how many.
    pub denominator: u32,
}

/// Changes to headers. A header may be named once, in one of the three.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderChanges {
    /// Headers to give one value, whatever they had.
    #[serde(default)]
    pub set: Vec<Header>,
    /// Values to append to whatever the headers had.
    #[serde(default)]
    pub add: Vec<Header>,
    /// Headers to take away.
    #[serde(default)]
    pub remove: Vec<String>,
}

/// A header with a value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    /// The header name; case does not matter.
    pub name: String,
    /// The value.
    pub value: String,
}

/// One destination of a rule.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    /// The name of an upstream in the same config.
    pub upstream: String,
    /// This backend's share of the rule's requests, relative to the other weights. Zero
    /// is no share; if all are zero the rule has nowhere to send a request.
    pub weight: u32,
}

/// One way for a request to belong to a rule: all of what is stated here must hold.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Match {
    /// The path; `{ prefix: / }` is how to say any path. A match says this or `grpc`:
    /// one of the two, never both.
    #[serde(default)]
    pub path: Option<PathMatch>,
    /// A gRPC method, as a GRPCRoute matches one: said in place of `path`.
    #[serde(default)]
    pub grpc: Option<GrpcMethod>,
    /// The method, in upper case, if it matters.
    #[serde(default)]
    pub method: Option<String>,
    /// Conditions on headers; names are case-insensitive.
    #[serde(default)]
    pub headers: Vec<ValuePredicate>,
    /// Conditions on decoded query parameters; names are case-sensitive.
    #[serde(default)]
    pub query: Vec<ValuePredicate>,
}

/// A gRPC method, matched exactly — Gateway API's Core matching for GRPCRoute: its
/// service, its method, or both; at least one. A gRPC call's path is `/service/method`,
/// and this is matched as that path would be: both as the exact path, a service alone as
/// the prefix `/service`, a method alone in any service.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcMethod {
    /// The fully qualified service, as `package.Service`.
    #[serde(default)]
    pub service: Option<String>,
    /// The method.
    #[serde(default)]
    pub method: Option<String>,
}

/// How a path is matched.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathMatch {
    /// This path and no other.
    Exact(String),
    /// This path and everything below it, by whole segments.
    Prefix(String),
    /// The paths this regular expression describes as a whole.
    Regex(String),
}

/// A condition on a named value: a header or a query parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValuePredicate {
    /// The header or parameter name.
    pub name: String,
    /// What its value must be.
    pub value: ValueMatch,
}

/// How a value is matched.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueMatch {
    /// This value and no other.
    Exact(String),
    /// The values this regular expression describes as a whole.
    Regex(String),
}
