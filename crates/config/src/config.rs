//! The whole of what a data plane is given to run.

use crate::Route;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;

/// A data plane's configuration. Filters are to come.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where requests come in, by name. They have no order.
    pub listeners: BTreeMap<String, Listener>,
    /// The routes, in order of precedence among otherwise equal matches.
    pub routes: Vec<Route>,
    /// The upstreams that backends refer to, by name. They have no order.
    pub upstreams: BTreeMap<String, Upstream>,
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
}

/// What a listener speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// HTTP/1.1 and HTTP/2 without TLS.
    Http,
    /// HTTP/1.1 and HTTP/2 over TLS, told apart by ALPN.
    Https,
}

/// The TLS a listener terminates.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// What it can present, at least one. A client is given the one whose names cover the
    /// name it asked for (SNI), and the first when none does or it asked for none.
    pub certificates: Vec<Certificate>,
}

/// A certificate and its private key, in PEM, as they came: reading them is the data
/// plane's, which refuses a config whose certificates it cannot use.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
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
