//! The whole of what a data plane is given to run.

use crate::{Route, TcpRoute, TlsRoute};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

/// A data plane's configuration. Filters are to come.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where requests come in, by name. They have no order.
    pub listeners: BTreeMap<String, Listener>,
    /// The routes, in order of precedence among otherwise equal matches.
    pub routes: Vec<Route>,
    /// The routes of `tcp` listeners.
    #[serde(default)]
    pub tcp_routes: Vec<TcpRoute>,
    /// The routes of `tls` listeners, in order of precedence among equally specific
    /// hostnames.
    #[serde(default)]
    pub tls_routes: Vec<TlsRoute>,
    /// The upstreams that backends refer to, by name. They have no order.
    pub upstreams: BTreeMap<String, Upstream>,
    /// The data plane's own settings; left out, each has its value.
    #[serde(default)]
    pub data_plane: DataPlane,
    /// How the data plane enforces the rate limits its rules are under
    /// ([06 §4](../../../docs/06-rate-limiting.md)); left out, it enforces none, though it
    /// checks them.
    #[serde(default)]
    pub rate_limiting: Option<RateLimiting>,
    /// The certificates listeners and upstreams name, by name: a resource of their own
    /// ([07 §1](../../../docs/07-config-and-dsl.md)), which reaches a data plane apart from
    /// the rest of its config and is never read with it, so that no file a config is read
    /// from can carry a key.
    #[serde(skip)]
    pub certificates: BTreeMap<String, Certificate>,
    /// The rate-limiting engine's configuration, if rate limiting is on: a resource of its
    /// own as certificates are ([07 §2](../../../docs/07-config-and-dsl.md)), whose limits
    /// routes and route rules name.
    #[serde(skip)]
    pub limits: Option<crate::Limits>,
}

/// The whole data plane's own settings ([07 §1](../../../docs/07-config-and-dsl.md)):
/// bounds rather than behaviour, each with its value when left out. The control plane fills
/// them from the `DataPlane` resource; they reload with the rest of the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DataPlane {
    /// Milliseconds an endpoint that could not be connected to is set aside before a
    /// connect probe may bring it back ([03 §6](../../../docs/03-data-plane.md)); 5,000
    /// when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub set_aside_ms: Option<u64>,
    /// Whether addresses discovered or resolved are sent to whatever they are, loopback,
    /// link-local, the pod's own and the gateway's other pods among them: experimental, for
    /// development, until a development profile takes it over
    /// ([03 §6](../../../docs/03-data-plane.md)). Off when left out.
    #[serde(default)]
    pub experimental_unrestricted_egress: Option<bool>,
}

/// How the data plane enforces its rate limits: the mode, and what that mode needs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RateLimiting {
    /// Which mode.
    pub mode: RateLimitMode,
    /// Bytes the rate limits' keys may hold on this data plane, which its process holds once
    /// for all its workers, as an estimate of their own data: past it, keys are evicted, and
    /// start again full ([06 §8](../../../docs/06-rate-limiting.md)). 268,435,456 (256 MiB)
    /// when left out. Not for `off`, which keeps no keys.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub max_key_bytes: Option<u64>,
    /// The rate-limiting service, which `synchronous` and `asynchronous` need and no other
    /// mode takes.
    #[serde(default)]
    pub service: Option<RateLimitService>,
    /// How a request waits for its owner, which `synchronous` needs and no other mode
    /// takes.
    #[serde(default)]
    pub synchronous: Option<RateLimitSynchronous>,
    /// Experimental: whether a limit may be keyed by a request header,
    /// `key: 'headers["x-api-key"]'` — what the client says rather than what the gateway
    /// established — so that limits keyed by something besides the client can be tried
    /// ([07 §2](../../../docs/07-config-and-dsl.md)). Off when left out.
    #[serde(default)]
    pub experimental_header_keys: Option<bool>,
    /// Experimental: whether the synchronous mode may run with fewer than three service
    /// pods, which cannot fail over ([06 §4](../../../docs/06-rate-limiting.md)) — for
    /// development and tests on one pod. Off when left out; only the synchronous mode
    /// takes it.
    #[serde(default)]
    pub experimental_fewer_pods: Option<bool>,
}

/// How a data plane enforces its rate limits ([06 §4](../../../docs/06-rate-limiting.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RateLimitMode {
    /// None enforced: the limits are checked as in any mode, and requests go on as if under
    /// none. What a data plane does with no `rate_limiting`.
    Off,
    /// Each process on what it sees alone, with no service: a client spread over several
    /// processes gets each one's limit.
    Local,
    /// The key's owner in the rate-limiting service decides every request.
    Synchronous,
    /// Each process decides from its copies of the keys and shares what it admitted through
    /// the rate-limiting service every cycle (experimental).
    Asynchronous,
}

/// How a request waits for its key's owner in the synchronous mode, and what decides it
/// when the owner does not answer ([06 §4](../../../docs/06-rate-limiting.md)). Every
/// field is required: how a data plane behaves when its owners are out of reach is the
/// operator's to say.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RateLimitSynchronous {
    /// Milliseconds a request waits for its owner's answer before `when_unreachable`
    /// decides it.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub timeout_ms: u64,
    /// What decides a request whose owner does not answer in time, or whose breaker is
    /// open.
    pub when_unreachable: WhenUnreachable,
    /// When the data plane stops asking an owner that does not answer, and when it tries
    /// it again.
    pub breaker: RateLimitBreaker,
}

/// What decides a request whose owner does not answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WhenUnreachable {
    /// It goes on, as under no limit.
    Allow,
    /// It is refused, 503 with `Retry-After: 1`: the service failed, not the client.
    Deny,
    /// The data plane's own buckets, as in the local mode, until the owner answers again.
    Local,
}

/// A data plane's breaker for each owner, which its workers share: open, nothing is asked
/// of the owner and `when_unreachable` decides at once.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RateLimitBreaker {
    /// Timeouts and broken streams in a row, on any worker, that open it; any answer
    /// starts the count again.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub failures: u32,
    /// Milliseconds open before a stream is opened to the owner again, without a request:
    /// the owner's first word on it closes the breaker, and none leaves it open as long
    /// again.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub probe_after_ms: u64,
}

/// The rate-limiting service: its pods, and in the asynchronous mode how often each process
/// sends them what it admitted and its levers, which the synchronous mode does not take.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RateLimitService {
    /// The address (`host:port`) of each of its pods, the first being pod 0.
    #[cfg_attr(feature = "schema", schemars(length(min = 1)))]
    pub addresses: Vec<String>,
    /// Milliseconds between the process's messages to the service; 10 when left out. Every
    /// limit's bucket holds at least two cycles of its rate, so a longer cycle asks for
    /// larger buckets: at 20 ms, a burst of 40 at `1000/s`.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub cycle_ms: Option<u64>,
    /// Whether a key with nothing to report is polled every cycle while in use, to keep its
    /// copy fresh. Off, a quiet key's copy is as fresh as its last answer until it has
    /// something to report again: messages grow with the keys admitting, not with all those
    /// in use. On,
    /// a key counts as in use for its refill time after its last request, so under a limit
    /// refilling over an hour or a day every key of every client seen in that time is polled
    /// every cycle: each cycle's messages and work grow with them, millions of keys every
    /// 10 ms. Off when left out, the faster.
    #[serde(default)]
    pub poll_quiet_keys: Option<bool>,
    /// Whether each cycle the process visits only the keys that need it — those its workers
    /// signal, and those with work outstanding — rather than every key it holds, so that
    /// quiet keys cost nothing between their uses. A key woken after cycles without a visit
    /// gets one share of its bucket refilled from the time gone by. Off, every cycle visits
    /// every key held, and a key is held for twice its refill time after its last request:
    /// under a limit refilling over an hour or a day, each cycle's work grows with every
    /// client seen in that time. On when left out, the faster.
    #[serde(default)]
    pub schedule_keys: Option<bool>,
}

/// A place where requests come in. Hostnames are to come; until a listener can be told
/// from another by hostname, each needs an address of its own.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Listener {
    /// The address to listen on; `[::]:8080` is every address, IPv4 included. No two
    /// listeners have the same one.
    pub address: SocketAddr,
    /// What is spoken there.
    pub protocol: Protocol,
    /// What an `https` listener presents, which it must have; no other may.
    #[serde(default)]
    pub tls: Option<Tls>,
    /// HTTP/3 as well, on the same port over UDP: for an `https` listener only.
    #[serde(default)]
    pub http3: Option<Http3>,
    /// How long, in seconds, a `tcp` or `tls` listener's tunnel may carry nothing either
    /// way before both its ends are closed (17 in the docs). An hour unless said otherwise,
    /// as HAProxy's `timeout tunnel` is set in practice: what a tunnel carries (a database's
    /// connection, a long poll) can be quiet for long. For those listeners only.
    #[serde(default)]
    #[cfg_attr(
        feature = "schema",
        schemars(range(min = 1, max = crate::compile::MOST_SECONDS))
    )]
    pub tunnel_idle_seconds: Option<u64>,
    /// What an `http` or `https` listener, which must have it, tells its upstreams of a
    /// request's client; no other may.
    #[serde(default)]
    pub forwarding: Option<Forwarding>,
    /// Whether an `http` or `https` listener, which must say, gives each request an ID of
    /// its own; no other may.
    #[serde(default)]
    pub request_id: Option<RequestId>,
    /// Whether every connection starts with a PROXY protocol header, and whose to believe
    /// (20 in the docs). Every listener states it, `off` included: it decides who a
    /// connection is taken to come from.
    #[serde(default)]
    pub proxy_protocol: Option<ListenerProxyProtocol>,
    /// Where a record of each of its requests, or each of its connections for a `tcp` or
    /// `tls` listener, is written (08 §2, 21 in the docs). Left out, nothing is logged.
    #[serde(default)]
    pub access_log: Option<AccessLog>,
}

/// A listener's PROXY protocol ([20 §2](../../../docs/20-proxy-protocol.md)).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ListenerProxyProtocol {
    /// No header is read: a connection's first bytes are its protocol's.
    Off,
    /// Every connection must start with a header, v1 or v2. Its addresses are believed
    /// when the connection comes from these ranges (`10.0.0.0/16`, `192.0.2.1/32` for one;
    /// written as `trusted_proxies` are), the load balancer's; from anywhere else, the
    /// header is read and dropped, and whoever connected is the client.
    Senders(
        #[cfg_attr(feature = "schema", schemars(length(min = 1, max = edgerush_filters::forwarding::MOST_ENTRIES)))]
         Vec<String>,
    ),
}

/// The PROXY protocol version an upstream is sent ([20 §4](../../../docs/20-proxy-protocol.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ProxyProtocolVersion {
    /// The text line.
    V1,
    /// The binary header.
    V2,
}

/// What an HTTP listener does with `X-Request-ID` (08 §3 in the docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RequestId {
    /// Every request gets an ID the gateway makes, in place of any it came with, whoever
    /// sent it; the answer tells the client the same ID, in place of any the upstream gave.
    Generate,
    /// The header is left as it is, both ways: a proxy in front can give the ID.
    Pass,
}

/// Where a listener's access log goes ([08 §2](../../../docs/08-observability.md)): one JSON
/// line a record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AccessLog {
    /// The process's standard output, which a container runtime collects; the process's own
    /// messages go to standard error.
    Stdout,
    /// A file, appended to; listeners that name the same path share it. A relative path is
    /// taken from the process's working directory. SIGUSR1 has it opened again, for
    /// rotation.
    File(PathBuf),
}

/// What an HTTP listener tells its upstreams of a request's client (03 §11 in the docs):
/// who it trusts to say who their clients are, and which headers only they may send. Both
/// are always written out, an empty list included, since both decide who a request is
/// taken to come from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Forwarding {
    /// The proxies whose `X-Forwarded-For` is believed, as ranges of their addresses:
    /// `10.0.4.0/24`, `192.0.2.1/32` for one. Empty, no one's is. Each is written one
    /// way: no bits set past its prefix, and an IPv4 range not as IPv6. List the proxies
    /// alone: whatever connects from a range listed can name any client, and the client it
    /// names is what upstreams are told, what logs record and what per-client rate limits
    /// count, so a range wider than the proxies lets anything inside it take a fresh limit
    /// with each request.
    #[cfg_attr(feature = "schema", schemars(length(max = edgerush_filters::forwarding::MOST_ENTRIES)))]
    pub trusted_proxies: Vec<String>,
    /// Headers taken off a request that did not come from a trusted proxy: names, or the
    /// front of one followed by `*` (`X-Forwarded-*`), matched whatever their case. The
    /// control plane's default is `Forwarded`, `X-Real-IP` and `X-Forwarded-*`. One that
    /// takes in a header the gateway reads itself (`Host`, `Con*`) is refused.
    #[cfg_attr(feature = "schema", schemars(length(max = edgerush_filters::forwarding::MOST_ENTRIES)))]
    pub trusted_only_headers: Vec<String>,
}

/// An `https` listener's HTTP/3 (16 in the docs): the same routes and TLS, over QUIC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Http3 {
    /// How long, in seconds, a client told over TCP that the listener serves HTTP/3 may
    /// remember it (the `ma` of `Alt-Svc`, RFC 7838 §3.1). A day unless said otherwise, as
    /// Envoy advertises it.
    #[serde(default = "Http3::day")]
    pub alt_svc_max_age: u32,
    /// Whether every client is to prove its address with a Retry before its handshake
    /// (RFC 9000 §8.1.2), as NGINX's `quic_retry` and HAProxy's `quic-force-retry` ask.
    /// Otherwise only past a threshold of handshakes under way, since a Retry costs a round
    /// trip. Off unless said.
    #[serde(default)]
    pub force_retry: bool,
}

impl Http3 {
    const fn day() -> u32 {
        86_400
    }
}

/// What a listener speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// HTTP/1.1 and HTTP/2 without TLS.
    Http,
    /// HTTP/1.1 and HTTP/2 over TLS, told apart by ALPN.
    Https,
    /// Bytes, as they come, to the listener's one TCP route (17 in the docs).
    Tcp,
    /// TLS read as far as its ClientHello and no further, to the TLS route whose
    /// hostnames cover the name the client asks for (SNI).
    Tls,
}

/// The TLS a listener terminates.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// The certificates it can present. A client is given the one that answers for the
    /// name it asked for (SNI): by the names it carries, or by the names stated for it
    /// alone; a client that asked for a name none answers for is refused in the handshake
    /// with an `unrecognized_name` alert. No certificate twice. None at all, and every
    /// client is refused so, as NGINX's `ssl_reject_handshake`: a translated listener
    /// waits so for its first Ingress with TLS.
    pub certificates: Vec<ListenerCertificate>,
    /// The certificate for a client that asks for no name (no SNI): one of
    /// `certificates`, or `none` to refuse such a client in the handshake. Said on every
    /// listener with TLS, so that which one is never left to their order. It holds for the
    /// listener's HTTP/3 socket too.
    pub without_sni: String,
    /// Host names refused in the handshake with an `unrecognized_name` alert before any
    /// certificate is looked at: translated, those an Ingress serves without TLS, so that
    /// another's wildcard never answers for them over HTTPS when that Ingress's own route
    /// is not there to take them.
    #[serde(default)]
    pub refused: Vec<String>,
    /// Clients must show a certificate these authorities vouch for (mTLS); none, and any
    /// client is served.
    #[serde(default)]
    pub client_validation: Option<ClientValidation>,
}

/// A certificate a listener presents, by its name: for every DNS name it carries, or for
/// the names stated for it alone.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum ListenerCertificate {
    /// Its name: it answers for every DNS name in its subject alternative names.
    Carried(String),
    /// Its name, and the only names it answers for, whatever else it carries: a
    /// translated Ingress's certificate answers for the hosts that Ingress lists and no
    /// other, so one team's certificate never answers for another team's host.
    Stated(StatedNames),
}

impl ListenerCertificate {
    /// The certificate's name, among the config's.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Carried(name) => name,
            Self::Stated(stated) => &stated.certificate,
        }
    }

    /// The names stated for it, if any are.
    #[must_use]
    pub fn stated(&self) -> Option<&[String]> {
        match self {
            Self::Carried(_) => None,
            Self::Stated(stated) => Some(&stated.names),
        }
    }
}

/// A certificate and the names it answers for.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct StatedNames {
    /// The certificate's name, among the config's.
    pub certificate: String,
    /// Host names, or `*.` and a host name for every name one label longer, as a
    /// certificate's own wildcard stands for one whole label.
    #[cfg_attr(feature = "schema", schemars(length(min = 1)))]
    pub names: Vec<String>,
}

/// Whom a listener trusts to vouch for its clients (Gateway API's frontend validation, in
/// its default mode: a client without a valid certificate is refused in the handshake).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClientValidation {
    /// The certificates, in PEM, of the authorities trusted.
    #[cfg_attr(feature = "schema", schemars(length(min = 1)))]
    pub authorities: Vec<String>,
}

/// A certificate and its private key, in PEM, as they came: reading them is the data
/// plane's, which refuses a config whose certificates it cannot use. Not read from a
/// config file ([`Config::certificates`]).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Certificate {
    /// The certificate first, then the intermediates that lead from it towards a root.
    pub chain: String,
    /// Its private key.
    pub key: String,
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Whatever prints a config — an error, a log — must not print a secret.
        f.debug_struct("Certificate")
            .field("chain", &self.chain)
            .field("key", &"(not shown)")
            .finish()
    }
}

/// A set of endpoints that serve the same thing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Where its endpoints come from.
    pub endpoints: Endpoints,
    /// What its endpoints are spoken to in, whatever the client spoke. An upstream of TCP
    /// and TLS routes is sent their bytes as they come: neither `http2` nor `tls`.
    #[serde(default)]
    pub protocol: UpstreamProtocol,
    /// TLS to its endpoints; none is plain TCP.
    #[serde(default)]
    pub tls: Option<UpstreamTls>,
    /// HTTP/2 PINGs to find a dead connection before a request does; none is no PINGs. For
    /// an upstream spoken to in HTTP/2 only.
    #[serde(default)]
    pub keepalive: Option<Keepalive>,
    /// Probes of each endpoint, which keep one that fails them out of load balancing;
    /// none is every endpoint taken as healthy.
    #[serde(default)]
    pub health_check: Option<HealthCheck>,
    /// Which endpoint takes each exchange. Always stated: it chooses behaviour.
    pub load_balancer: LoadBalancer,
    /// A new or recovered endpoint's ramp to its full share; none is every endpoint at its
    /// full share from the start.
    #[serde(default)]
    pub slow_start: Option<SlowStart>,
    /// A PROXY protocol header ahead of each tunnel's bytes, telling the backend who the
    /// client is; none is no header. For an upstream of TCP and TLS routes only: an HTTP
    /// route's connections carry many clients' requests.
    #[serde(default)]
    pub proxy_protocol: Option<ProxyProtocolVersion>,
}

/// Where an upstream's endpoints come from, one way and stated
/// ([03 §6](../../../docs/03-data-plane.md)).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Endpoints {
    /// Addresses, not names, each once. None is allowed, and means there is nothing to
    /// send a request to — a state a running system passes through, not a mistake.
    Listed(#[cfg_attr(feature = "schema", schemars(extend("uniqueItems" = true)))] Vec<SocketAddr>),
    /// A Kubernetes Service's endpoints, followed through its EndpointSlices:
    /// `<service>.<namespace>.svc.<cluster domain>:<port>`, the port named, or a number for
    /// the Service's only, unnamed port. A data plane follows them from the API server of
    /// the cluster it is in, and refuses it outside one.
    Kubernetes(String),
    /// What a name resolves to, each address on the port given: `<name>:<port>`, looked
    /// up as an absolute name, again as each answer's time to live runs out. A Service's
    /// own name, so that kube-proxy or a mesh chooses the pod, or a headless Service's,
    /// whose answer is its pods.
    Dns(String),
}

/// How an upstream's endpoint is chosen for an exchange ([03 §6](../../../docs/03-data-plane.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancer {
    /// Two different endpoints drawn at random, and the one with fewer exchanges in flight:
    /// the power of two choices. Steers away from an endpoint that stalls.
    P2c,
    /// Each endpoint in turn.
    RoundRobin,
}

/// Slow start ([03 §6](../../../docs/03-data-plane.md)): an endpoint added to an upstream
/// that keeps an endpoint it had, or passing its checks again after failing them, takes a
/// share rising linearly from a tenth of a full one to all of it over the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SlowStart {
    /// How long the ramp takes, in milliseconds.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub window_ms: u64,
}

/// An active check of an upstream's endpoints. Everything is stated: how often, how long a
/// probe may take, and how many results in a row change an endpoint's state (HAProxy's
/// `rise` and `fall`; Envoy's thresholds).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct HealthCheck {
    /// Seconds between probes of an endpoint.
    #[cfg_attr(
        feature = "schema",
        schemars(range(min = 1, max = crate::compile::MOST_SECONDS))
    )]
    pub interval_seconds: u64,
    /// Seconds a probe may take, connection and handshake included; no more than the
    /// interval.
    #[cfg_attr(
        feature = "schema",
        schemars(range(min = 1, max = crate::compile::MOST_SECONDS))
    )]
    pub timeout_seconds: u64,
    /// Passes in a row that make an unhealthy endpoint healthy.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub healthy_threshold: u32,
    /// Failures in a row that make a healthy endpoint unhealthy.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub unhealthy_threshold: u32,
    /// What a probe asks.
    pub probe: Probe,
}

/// What a health check asks an endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Probe {
    /// `GET` this path, in the upstream's protocol: a 2xx answer passes. Not for an upstream
    /// of TLS routes, whose endpoints speak TLS to the client, not to the data plane.
    Http {
        /// The path asked for.
        #[cfg_attr(feature = "schema", schemars(regex(pattern = "^/")))]
        path: String,
    },
    /// gRPC's `grpc.health.v1.Health/Check` for this service (empty for the server as a
    /// whole): `SERVING` passes, anything else fails. For an upstream spoken to in HTTP/2.
    Grpc {
        /// The service asked about.
        service: String,
    },
    /// A connection made, and its handshake finished where the upstream has `tls`: nothing
    /// is asked (20 in the docs). A plain one is closed so that, as a rule, the backend
    /// never sees it.
    Tcp,
}

/// PINGs on an HTTP/2 upstream's idle-looking connections, as gRPC's keepalive has them.
/// A gRPC server takes a PING more often than every five minutes as abuse, and says so
/// with GOAWAY(ENHANCE_YOUR_CALM), so nothing shorter is allowed unless the backend is
/// said to take it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Keepalive {
    /// Seconds between PINGs: at least 300 unless `backend_allows_short_intervals` is said.
    #[cfg_attr(
        feature = "schema",
        schemars(range(min = 1, max = crate::compile::MOST_SECONDS))
    )]
    pub interval_seconds: u64,
    /// Seconds a PING's answer is waited for before the connection is taken for dead.
    #[cfg_attr(
        feature = "schema",
        schemars(range(min = 1, max = crate::compile::MOST_SECONDS))
    )]
    pub timeout_seconds: u64,
    /// Whether to PING a connection with no call on it too; gRPC asks this be chosen on
    /// purpose.
    pub without_calls: bool,
    /// That the backend takes PINGs more often than every five minutes.
    #[serde(default)]
    pub backend_allows_short_intervals: bool,
}

/// TLS to an upstream's endpoints: whom they are expected to be, and whom to trust to say
/// so (Gateway API's BackendTLSPolicy: a hostname and CA certificates).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct UpstreamTls {
    /// The name asked for (SNI), and the one an endpoint's certificate must carry: a host
    /// name, not an address, and with no wildcard or port.
    pub server_name: String,
    /// The certificates, in PEM, of the authorities trusted to vouch for it.
    #[cfg_attr(feature = "schema", schemars(length(min = 1)))]
    pub authorities: Vec<String>,
    /// The name of the certificate the data plane shows an endpoint that asks who it is
    /// (mTLS); none, and it shows nothing.
    #[serde(default)]
    pub client_certificate: Option<String>,
}

/// What an upstream is spoken to in. Unsaid, it is HTTP/1.1, as for a Kubernetes Service
/// port that names no application protocol.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum UpstreamProtocol {
    /// HTTP/1.1, a request at a time on each connection.
    #[default]
    Http1,
    /// HTTP/2, many requests at once on each connection. Without TLS, by prior knowledge
    /// (RFC 9113 §3.3): the upstream is expected to speak it from the first byte.
    Http2,
}
