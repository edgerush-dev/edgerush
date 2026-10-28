//! A name and a port, for an upstream whose endpoints are what the name resolves to
//! ([03 §6](../../../docs/03-data-plane.md)): every address its answer holds, on that port.
//!
//! The name is looked up as it is written, as an absolute name, never with the search
//! domains a resolver is given, as NGINX's and HAProxy's resolvers look names up: in a pod,
//! whose resolver tries five search domains before a name of fewer than five dots, a
//! Service's own name would otherwise be asked for wrongly first, each time.

use std::fmt;
use std::net::IpAddr;
use std::num::NonZeroU16;

/// A name and a port: `<name>:<port>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DnsName {
    /// The name, in one spelling: lower case, without a trailing dot.
    pub host: String,
    /// The port each address it resolves to is reached on.
    pub port: NonZeroU16,
}

/// Why a name and a port cannot be looked up.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DnsNameError {
    /// No `:` and port after the name.
    #[error("no port: `<name>:<port>`")]
    NoPort,
    /// An address, which needs no lookup.
    #[error("`{0}` is an address, which `listed` holds, not a name")]
    Address(String),
    /// A label that is not a DNS label: 1 to 63 of `a-z`, `0-9` and `-`, neither first nor
    /// last a `-`.
    #[error("`{0}` is not a DNS label: 1 to 63 of a-z, 0-9 and -, not starting or ending in -")]
    Label(String),
    /// A name longer than DNS takes.
    #[error("the name is longer than 253 characters")]
    TooLong,
    /// A port that is not a number from 1 to 65535, written without a leading zero.
    #[error("port `{0}` is not a number from 1 to 65535")]
    Port(String),
}

impl DnsName {
    /// Reads `<name>:<port>`, the name in any case and with a trailing dot or without.
    ///
    /// # Errors
    ///
    /// Returns a [`DnsNameError`] saying what is wrong with it.
    pub fn parse(text: &str) -> Result<Self, DnsNameError> {
        let (name, port) = text.rsplit_once(':').ok_or(DnsNameError::NoPort)?;
        // An IPv6 address has colons of its own: `[::1]:80`, or bare.
        let unbracketed = name.trim_start_matches('[').trim_end_matches(']');
        if unbracketed.parse::<IpAddr>().is_ok() || name.contains(':') {
            return Err(DnsNameError::Address(name.to_owned()));
        }
        let host = name.strip_suffix('.').unwrap_or(name).to_ascii_lowercase();
        if host.len() > 253 {
            return Err(DnsNameError::TooLong);
        }
        for label in host.split('.') {
            if !is_label(label) {
                return Err(DnsNameError::Label(label.to_owned()));
            }
        }
        if port.starts_with('0') || !port.bytes().all(|b| b.is_ascii_digit()) {
            return Err(DnsNameError::Port(port.to_owned()));
        }
        let port = port
            .parse()
            .map_err(|_| DnsNameError::Port(port.to_owned()))?;
        Ok(Self { host, port })
    }
}

impl fmt::Display for DnsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// An RFC 1123 label, in lower case.
fn is_label(label: &str) -> bool {
    (1..=63).contains(&label.len())
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !label.starts_with('-')
        && !label.ends_with('-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(host: &str, port: u16) -> DnsName {
        DnsName {
            host: host.to_owned(),
            port: NonZeroU16::new(port).unwrap(),
        }
    }

    #[test]
    fn a_name_and_a_port_are_read_in_one_spelling() {
        for (text, host, port) in [
            (
                "web.shop.svc.cluster.local:80",
                "web.shop.svc.cluster.local",
                80,
            ),
            (
                "Web.Shop.SVC.cluster.local.:8080",
                "web.shop.svc.cluster.local",
                8080,
            ),
            ("api.example.net:443", "api.example.net", 443),
            ("localhost:1", "localhost", 1),
            ("a-b.c9:65535", "a-b.c9", 65535),
        ] {
            assert_eq!(DnsName::parse(text), Ok(name(host, port)), "{text}");
        }
        assert_eq!(
            name("web.shop.svc.cluster.local", 80).to_string(),
            "web.shop.svc.cluster.local:80"
        );
    }

    #[test]
    fn what_is_not_a_name_and_a_port_is_refused_with_why() {
        for (text, error) in [
            ("web.shop", DnsNameError::NoPort),
            (
                "192.0.2.1:80",
                DnsNameError::Address("192.0.2.1".to_owned()),
            ),
            ("[::1]:80", DnsNameError::Address("[::1]".to_owned())),
            ("::1:80", DnsNameError::Address("::1".to_owned())),
            ("-web.shop:80", DnsNameError::Label("-web".to_owned())),
            ("web-.shop:80", DnsNameError::Label("web-".to_owned())),
            ("web..shop:80", DnsNameError::Label(String::new())),
            (":80", DnsNameError::Label(String::new())),
            ("web_shop:80", DnsNameError::Label("web_shop".to_owned())),
            (
                &format!("{}:80", "a".repeat(64)),
                DnsNameError::Label("a".repeat(64)),
            ),
            (
                &format!("{}:80", vec!["a".repeat(63); 4].join(".")),
                DnsNameError::TooLong,
            ),
            ("web:", DnsNameError::Port(String::new())),
            ("web:0", DnsNameError::Port("0".to_owned())),
            ("web:080", DnsNameError::Port("080".to_owned())),
            ("web:65536", DnsNameError::Port("65536".to_owned())),
            ("web:http", DnsNameError::Port("http".to_owned())),
            ("web:+80", DnsNameError::Port("+80".to_owned())),
        ] {
            assert_eq!(DnsName::parse(text), Err(error), "{text}");
        }
    }
}
