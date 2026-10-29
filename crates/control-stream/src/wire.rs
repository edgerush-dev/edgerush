//! The stream's messages on the wire: protobuf, derived from these structs.
//!
//! Protobuf is for version skew: a field a reader does not know is skipped, one it misses
//! takes its default, and a resource of a kind it does not know arrives as none, and is
//! passed over. The config itself is the model's JSON, which a data plane refuses whole if
//! it holds a field the data plane does not know.

use std::collections::BTreeMap;
use std::fmt;

/// What the control plane sends: one resource.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ToDataPlane {
    /// The resource; none is one of a kind this data plane does not know.
    #[prost(oneof = "Resource", tags = "1, 2, 3, 4")]
    pub resource: Option<Resource>,
}

/// A resource the control plane sends.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Resource {
    /// A certificate, at a version the data plane does not hold.
    #[prost(message, tag = "1")]
    Certificate(Certificate),
    /// The latest config.
    #[prost(message, tag = "2")]
    Config(Config),
    /// Certificates no longer offered.
    #[prost(message, tag = "3")]
    Removed(Removed),
    /// The addresses of the gateway's own pods, which no route may send to.
    #[prost(message, tag = "4")]
    Gateway(Gateway),
}

/// The addresses of the gateway's own pods: the data planes', the control plane's and the
/// rate-limiting service's, whole, each time they change (03 §6).
#[derive(Clone, PartialEq, prost::Message)]
pub struct Gateway {
    /// Each address, IPv4 or IPv6, as text.
    #[prost(string, repeated, tag = "1")]
    pub addresses: Vec<String>,
}

/// A certificate and its key, by the name configs give it.
#[derive(Clone, PartialEq, prost::Message)]
#[prost(skip_debug)]
pub struct Certificate {
    /// The name configs give it.
    #[prost(string, tag = "1")]
    pub name: String,
    /// Its version.
    #[prost(string, tag = "2")]
    pub version: String,
    /// The certificate, then the intermediates, in PEM.
    #[prost(string, tag = "3")]
    pub chain: String,
    /// Its private key, in PEM.
    #[prost(string, tag = "4")]
    pub key: String,
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Whatever prints a message — an error, a log — must not print a secret.
        f.debug_struct("Certificate")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("chain", &self.chain)
            .field("key", &"(not shown)")
            .finish()
    }
}

/// A config, and the certificates it names, which a data plane must hold to run it.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Config {
    /// Its version.
    #[prost(string, tag = "1")]
    pub version: String,
    /// The config model's JSON, without its certificates.
    #[prost(bytes = "vec", tag = "2")]
    pub json: Vec<u8>,
    /// The certificates it needs, each at the version it needs, by name.
    #[prost(btree_map = "string, string", tag = "3")]
    pub requires: BTreeMap<String, String>,
}

/// Certificates no longer offered, by name.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Removed {
    /// Their names.
    #[prost(string, repeated, tag = "1")]
    pub names: Vec<String>,
}

/// What a data plane sends.
#[derive(Clone, PartialEq, prost::Message)]
pub struct FromDataPlane {
    /// The message; none is one of a kind this control plane does not know.
    #[prost(oneof = "Said", tags = "1, 2")]
    pub said: Option<Said>,
}

/// What a data plane says.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Said {
    /// What it holds, and the config version it reads: the stream's first message, and
    /// only that.
    #[prost(message, tag = "1")]
    Hello(Hello),
    /// What it runs, after a config it was sent.
    #[prost(message, tag = "2")]
    Report(Report),
}

/// What a data plane holds as it opens a stream, and the config version it reads.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Hello {
    /// What it holds.
    #[prost(message, optional, tag = "1")]
    pub held: Option<Held>,
    /// Its config version; 0, from one that does not say, is none written.
    #[prost(uint32, tag = "2")]
    pub config_version: u32,
    /// The oldest config version it still reads.
    #[prost(uint32, tag = "3")]
    pub oldest_config_version: u32,
}

/// Resources' versions: a config's and certificates'.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Held {
    /// The config's; empty is none.
    #[prost(string, tag = "1")]
    pub config: String,
    /// Each certificate's, by name.
    #[prost(btree_map = "string, string", tag = "2")]
    pub certificates: BTreeMap<String, String>,
}

/// What a data plane runs, and the config it refused, if it refused the latest.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Report {
    /// The config it runs and the certificates that config uses.
    #[prost(message, optional, tag = "1")]
    pub running: Option<Held>,
    /// The config refused.
    #[prost(message, optional, tag = "2")]
    pub refused: Option<Refusal>,
}

/// A config a data plane refused, and why.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Refusal {
    /// Its version.
    #[prost(string, tag = "1")]
    pub config: String,
    /// Why.
    #[prost(string, tag = "2")]
    pub why: String,
}
