//! The whole of what a data plane is given to run.

use crate::{Route, TcpRoute, TlsRoute};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

/// A data plane's configuration. Filters are to come.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    /// The certificates listeners and upstreams name, by name: a resource of their own
    /// ([07 §1](../../../docs/07-config-and-dsl.md)), which reaches a data plane apart from
    /// the rest of its config and is never read with it, so that no file a config is read
    /// from can carry a key.
    #[serde(skip)]
    pub certificates: BTreeMap<String, Certificate>,
}

/// The whole data plane's own settings ([07 §1](../../../docs/07-config-and-dsl.md)):
/// bounds rather than behaviour, each with its value when left out. The control plane fills
/// them from the `DataPlane` resource; they reload with the rest of the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataPlane {
    /// Milliseconds an endpoint that could not be connected to is set aside before a
    /// connect probe may bring it back ([03 §6](../../../docs/03-data-plane.md)); 5,000
    /// when left out, and at least 1.
    #[serde(default)]
    pub set_aside_ms: Option<u64>,
}

/// A place where requests come in. Hostnames are to come; until a listener can be told
/// from another by hostname, each needs an address of its own.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    /// The address to listen on; `[::]:8080` is every address, IPv4 included.
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenerProxyProtocol {
    /// No header is read: a connection's first bytes are its protocol's.
    Off,
    /// Every connection must start with a header, v1 or v2. Its addresses are believed
    /// when the connection comes from these ranges (`10.0.0.0/16`, `192.0.2.1/32` for one;
    /// at least one), the load balancer's; from anywhere else, the header is read and
    /// dropped, and whoever connected is the client.
    Senders(Vec<String>),
}

/// The PROXY protocol version an upstream is sent ([20 §4](../../../docs/20-proxy-protocol.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyProtocolVersion {
    /// The text line.
    V1,
    /// The binary header.
    V2,
}

/// What an HTTP listener does with `X-Request-ID` (08 §3 in the docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessLog {
    /// The process's standard output, which a container runtime collects; the process's own
    /// messages go to standard error.
    Stdout,
    /// A file, appended to; listeners that name the same path share it.
    File(PathBuf),
}

/// What an HTTP listener tells its upstreams of a request's client (03 §11 in the docs):
/// who it trusts to say who their clients are, and which headers only they may send. Both
/// are always written out, an empty list included, since both decide who a request is
/// taken to come from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Forwarding {
    /// The proxies whose `X-Forwarded-For` is believed, as ranges of their addresses:
    /// `10.0.0.0/8`, `192.0.2.1/32` for one. Empty, no one's is.
    pub trusted_proxies: Vec<String>,
    /// Headers taken off a request that did not come from a trusted proxy: names, or the
    /// front of one followed by `*` (`X-Forwarded-*`), matched whatever their case. The
    /// control plane's default is `Forwarded`, `X-Real-IP` and `X-Forwarded-*`.
    pub trusted_only_headers: Vec<String>,
}

/// An `https` listener's HTTP/3 (16 in the docs): the same routes and TLS, over QUIC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// The names of the certificates it can present, at least one. A client is given the
    /// one whose names cover the name it asked for (SNI), and the first when none does or
    /// it asked for none.
    pub certificates: Vec<String>,
    /// Clients must show a certificate these authorities vouch for (mTLS); none, and any
    /// client is served.
    #[serde(default)]
    pub client_validation: Option<ClientValidation>,
}

/// Whom a listener trusts to vouch for its clients (Gateway API's frontend validation, in
/// its default mode: a client without a valid certificate is refused in the handshake).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientValidation {
    /// The certificates, in PEM, of the authorities trusted. At least one.
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Where to connect: addresses, not names. None is allowed, and means there is nothing
    /// to send a request to — a state a running system passes through, not a mistake.
    pub endpoints: Vec<SocketAddr>,
    /// What its endpoints are spoken to in, whatever the client spoke.
    #[serde(default)]
    pub protocol: UpstreamProtocol,
    /// TLS to its endpoints; none is plain TCP.
    #[serde(default)]
    pub tls: Option<UpstreamTls>,
    /// HTTP/2 PINGs to find a dead connection before a request does; none is no PINGs.
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

/// How an upstream's endpoint is chosen for an exchange ([03 §6](../../../docs/03-data-plane.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlowStart {
    /// How long the ramp takes, in milliseconds; at least 1.
    pub window_ms: u64,
}

/// An active check of an upstream's endpoints. Everything is stated: how often, how long a
/// probe may take, and how many results in a row change an endpoint's state (HAProxy's
/// `rise` and `fall`; Envoy's thresholds).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthCheck {
    /// Seconds between probes of an endpoint.
    pub interval_seconds: u64,
    /// Seconds a probe may take, connection and handshake included.
    pub timeout_seconds: u64,
    /// Passes in a row that make an unhealthy endpoint healthy.
    pub healthy_threshold: u32,
    /// Failures in a row that make a healthy endpoint unhealthy.
    pub unhealthy_threshold: u32,
    /// What a probe asks.
    pub probe: Probe,
}

/// What a health check asks an endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Probe {
    /// `GET` this path, in the upstream's protocol: a 2xx answer passes.
    Http {
        /// The path asked for.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Keepalive {
    /// Seconds between PINGs.
    pub interval_seconds: u64,
    /// Seconds a PING's answer is waited for before the connection is taken for dead.
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamTls {
    /// The name asked for (SNI), and the one an endpoint's certificate must carry.
    pub server_name: String,
    /// The certificates, in PEM, of the authorities trusted to vouch for it. At least one.
    pub authorities: Vec<String>,
    /// The name of the certificate the data plane shows an endpoint that asks who it is
    /// (mTLS); none, and it shows nothing.
    #[serde(default)]
    pub client_certificate: Option<String>,
}

/// What an upstream is spoken to in. Unsaid, it is HTTP/1.1, as for a Kubernetes Service
/// port that names no application protocol.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamProtocol {
    /// HTTP/1.1, a request at a time on each connection.
    #[default]
    Http1,
    /// HTTP/2, many requests at once on each connection. Without TLS, by prior knowledge
    /// (RFC 9113 §3.3): the upstream is expected to speak it from the first byte.
    Http2,
}
