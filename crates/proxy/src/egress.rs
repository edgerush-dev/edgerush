//! Default-deny egress ([03 §6](../../../docs/03-data-plane.md), 09 §2): the addresses a
//! data plane never sends to, of those it discovers or resolves. Addresses a config lists
//! are the operator's own, and are not looked at.

use std::fmt;
use std::net::IpAddr;

/// Why an address is not sent to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Refusal {
    /// `0.0.0.0` or `::`, which reaches this host.
    Unspecified,
    /// This host's own.
    Loopback,
    /// On the link alone, a cloud's metadata service among them.
    LinkLocal,
    /// A group's, not one host's.
    Multicast,
    /// One of the pod's own.
    Own,
    /// The API server's.
    ApiServer,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Loopback => "loopback",
            Self::LinkLocal => "link-local",
            Self::Multicast => "multicast",
            Self::Own => "one of the data plane's own",
            Self::ApiServer => "the API server's",
        })
    }
}

impl Refusal {
    /// The name a metric's label gives it.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Loopback => "loopback",
            Self::LinkLocal => "link_local",
            Self::Multicast => "multicast",
            Self::Own => "own",
            Self::ApiServer => "api_server",
        }
    }
}

/// The addresses that are this data plane's in particular, beside the ranges no data plane
/// sends to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Denied {
    /// The pod's own addresses.
    pub own: Vec<IpAddr>,
    /// The API server's, as the pod is given it.
    pub api_server: Option<IpAddr>,
}

impl Denied {
    /// Why `address` is not sent to, if it is not: an IPv4 address written as IPv6 is
    /// looked at as the IPv4 address it is.
    #[must_use]
    pub fn refusal(&self, address: IpAddr) -> Option<Refusal> {
        let address = address.to_canonical();
        let special = match address {
            IpAddr::V4(v4) if v4.is_unspecified() => Some(Refusal::Unspecified),
            IpAddr::V4(v4) if v4.is_loopback() => Some(Refusal::Loopback),
            IpAddr::V4(v4) if v4.is_link_local() => Some(Refusal::LinkLocal),
            IpAddr::V4(v4) if v4.is_multicast() || v4.is_broadcast() => Some(Refusal::Multicast),
            IpAddr::V6(v6) if v6.is_unspecified() => Some(Refusal::Unspecified),
            IpAddr::V6(v6) if v6.is_loopback() => Some(Refusal::Loopback),
            IpAddr::V6(v6) if v6.is_unicast_link_local() => Some(Refusal::LinkLocal),
            IpAddr::V6(v6) if v6.is_multicast() => Some(Refusal::Multicast),
            _ => None,
        };
        let canonical = |ours: &IpAddr| ours.to_canonical() == address;
        special
            .or_else(|| self.own.iter().any(canonical).then_some(Refusal::Own))
            .or_else(|| {
                self.api_server
                    .as_ref()
                    .is_some_and(canonical)
                    .then_some(Refusal::ApiServer)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(address: &str) -> IpAddr {
        address.parse().unwrap()
    }

    #[test]
    fn the_special_ranges_are_refused_whichever_way_they_are_written() {
        let denied = Denied::default();
        for (address, why) in [
            ("0.0.0.0", Refusal::Unspecified),
            ("::", Refusal::Unspecified),
            ("127.0.0.1", Refusal::Loopback),
            ("127.255.0.9", Refusal::Loopback),
            ("::1", Refusal::Loopback),
            ("::ffff:127.0.0.1", Refusal::Loopback),
            ("169.254.169.254", Refusal::LinkLocal),
            ("::ffff:169.254.169.254", Refusal::LinkLocal),
            ("fe80::1", Refusal::LinkLocal),
            ("febf::1", Refusal::LinkLocal),
            ("224.0.0.1", Refusal::Multicast),
            ("239.255.255.250", Refusal::Multicast),
            ("255.255.255.255", Refusal::Multicast),
            ("ff02::1", Refusal::Multicast),
        ] {
            assert_eq!(denied.refusal(ip(address)), Some(why), "{address}");
        }
        for address in [
            "10.0.0.1",
            "192.0.2.1",
            "169.253.255.255",
            "169.255.0.1",
            "128.0.0.1",
            "fd00::1",
            "fec0::1",
            "2001:db8::1",
            "::ffff:10.0.0.1",
        ] {
            assert_eq!(denied.refusal(ip(address)), None, "{address}");
        }
    }

    #[test]
    fn the_pods_own_and_the_api_servers_are_refused() {
        let denied = Denied {
            own: vec![ip("10.244.0.5"), ip("fd00:10:244::5")],
            api_server: Some(ip("10.96.0.1")),
        };
        assert_eq!(denied.refusal(ip("10.244.0.5")), Some(Refusal::Own));
        assert_eq!(denied.refusal(ip("::ffff:10.244.0.5")), Some(Refusal::Own));
        assert_eq!(denied.refusal(ip("fd00:10:244::5")), Some(Refusal::Own));
        assert_eq!(denied.refusal(ip("10.96.0.1")), Some(Refusal::ApiServer));
        assert_eq!(denied.refusal(ip("10.244.0.6")), None);
        // A range is said before whose the address is.
        let looped = Denied {
            own: vec![ip("127.0.0.1")],
            api_server: None,
        };
        assert_eq!(looped.refusal(ip("127.0.0.1")), Some(Refusal::Loopback));
    }
}
