//! Forwarding: who the client is, as the gateway tells an upstream (03 §11 in the docs).
//!
//! The upstream is sent one address in `X-Forwarded-For`: the client, as the gateway works
//! it out. A peer that is not a trusted proxy is the client itself, and whatever it said
//! about forwarding is replaced. A trusted proxy's `X-Forwarded-For` is walked from the
//! right, past the addresses of trusted proxies, and the first one that is not trusted is
//! the client ([`client_address`]). Trust is by address range only ([`TrustedProxies`]):
//! a count of hops would trust entries without looking at who connected, so a client that
//! reached the gateway directly could name its own address.
//!
//! Headers only a trusted proxy may send ([`HeaderNames`]) are taken off a request from
//! anyone else, before anything reads them.
//!
//! Every address is compared, and written, in one form: an IPv4 client of a dual-stack
//! socket (`::ffff:192.0.2.1`) is `192.0.2.1`.

use http::Version;
use http::header::{HeaderName, HeaderValue};
use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// The most entries a listener's list of address ranges, or of header names, may have: what
/// a request pays for them grows with the list.
pub const MOST_ENTRIES: usize = 64;

/// A range of addresses: `10.0.0.0/8`, `2001:db8::/32`, and `192.0.2.1/32` for one address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddressRange {
    network: IpAddr,
    prefix: u8,
}

impl AddressRange {
    /// Reads a range written as an address, `/`, and how many of its leading bits are the
    /// network's.
    ///
    /// # Errors
    ///
    /// [`ForwardingError::NotARange`] for anything else, a prefix longer than the address
    /// included; [`ForwardingError::HostBits`] for an address with bits set past the prefix,
    /// which would be a second way to write the same range; [`ForwardingError::MappedRange`]
    /// for an IPv4 range written as IPv6, which no address is compared with.
    pub fn new(text: &str) -> Result<Self, ForwardingError> {
        let not_a_range = || ForwardingError::NotARange(text.to_owned());
        let (address, prefix) = text.split_once('/').ok_or_else(not_a_range)?;
        let network: IpAddr = address.parse().map_err(|_| not_a_range())?;
        // Digits only: `u8`'s parser would take a `+` too.
        if prefix.is_empty() || !prefix.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(not_a_range());
        }
        let prefix: u8 = prefix.parse().map_err(|_| not_a_range())?;
        let bits = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > bits {
            return Err(not_a_range());
        }
        if let IpAddr::V6(v6) = network
            && v6.to_ipv4_mapped().is_some()
        {
            return Err(ForwardingError::MappedRange(text.to_owned()));
        }
        let range = Self { network, prefix };
        if range.masked() != network {
            return Err(ForwardingError::HostBits {
                written: text.to_owned(),
                meant: format!("{}/{prefix}", range.masked()),
            });
        }
        Ok(range)
    }

    /// Whether `address`, in its one form, is in the range.
    #[must_use]
    pub fn contains(&self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                mask32(u32::from(address), self.prefix) == u32::from(network)
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                mask128(u128::from(address), self.prefix) == u128::from(network)
            }
            _ => false,
        }
    }

    /// The network with every bit past the prefix cleared.
    fn masked(&self) -> IpAddr {
        match self.network {
            IpAddr::V4(v4) => IpAddr::V4(Ipv4Addr::from(mask32(u32::from(v4), self.prefix))),
            IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(mask128(u128::from(v6), self.prefix))),
        }
    }
}

fn mask32(bits: u32, prefix: u8) -> u32 {
    // A shift by the whole width is no shift at all, so a prefix of 0 is its own case.
    u32::MAX
        .checked_shl(32 - u32::from(prefix))
        .map_or(0, |mask| bits & mask)
}

fn mask128(bits: u128, prefix: u8) -> u128 {
    u128::MAX
        .checked_shl(128 - u32::from(prefix))
        .map_or(0, |mask| bits & mask)
}

/// The proxies a listener trusts to say who their clients are: ranges of their addresses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrustedProxies {
    ranges: Box<[AddressRange]>,
}

impl TrustedProxies {
    /// The proxies in these ranges ([`AddressRange::new`]); none for an empty list.
    ///
    /// # Errors
    ///
    /// The first range that is not one, or [`ForwardingError::TooMany`] for more than
    /// [`MOST_ENTRIES`].
    pub fn new<'a>(ranges: impl IntoIterator<Item = &'a str>) -> Result<Self, ForwardingError> {
        let ranges = ranges
            .into_iter()
            .map(AddressRange::new)
            .collect::<Result<Box<[_]>, _>>()?;
        if ranges.len() > MOST_ENTRIES {
            return Err(ForwardingError::TooMany(ranges.len()));
        }
        Ok(Self { ranges })
    }

    /// Whether `address` is a trusted proxy's.
    #[must_use]
    pub fn trusts(&self, address: IpAddr) -> bool {
        let address = address.to_canonical();
        self.ranges.iter().any(|range| range.contains(address))
    }

    /// Whether no proxy is trusted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

/// Header names, each whole or as the front of a name (`X-Forwarded-*`): those only a
/// trusted proxy may send. Matched whatever the case, and nothing else: `X_Forwarded_For`
/// is another name, which a backend that folds `_` into `-` must be told of by listing it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderNames {
    /// Lower case; a front ends where its `*` was.
    entries: Box<[Entry]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Whole(Box<[u8]>),
    Front(Box<[u8]>),
}

impl Entry {
    fn matches(&self, name: &[u8]) -> bool {
        // The first byte first: every request's every name is asked about, and that alone
        // rules out nearly all of them. An entry is never empty, and is in lower case.
        let (Self::Whole(entry) | Self::Front(entry)) = self;
        if name.first().map(u8::to_ascii_lowercase) != entry.first().copied() {
            return false;
        }
        match self {
            Self::Whole(whole) => name.eq_ignore_ascii_case(whole),
            Self::Front(front) => name
                .get(..front.len())
                .is_some_and(|start| start.eq_ignore_ascii_case(front)),
        }
    }

    /// Whether every name `other` matches, this matches too.
    fn covers(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Whole(whole), Self::Whole(other)) => whole == other,
            (Self::Whole(_), Self::Front(_)) => false,
            (Self::Front(front), Self::Whole(other) | Self::Front(other)) => {
                other.starts_with(front)
            }
        }
    }

    fn written(&self) -> String {
        match self {
            Self::Whole(whole) => String::from_utf8_lossy(whole).into_owned(),
            Self::Front(front) => format!("{}*", String::from_utf8_lossy(front)),
        }
    }
}

impl HeaderNames {
    /// The names in `entries`: a header name, or the front of one followed by `*`.
    ///
    /// # Errors
    ///
    /// [`ForwardingError::NotAName`] for an entry that is neither — a bare `*` included;
    /// [`ForwardingError::Reserved`] for one that takes in a header the gateway keeps
    /// ([`crate::RESERVED`]), without which it could not read the request;
    /// [`ForwardingError::Covered`] for one another entry already takes in, which would be
    /// a second way to say the same; [`ForwardingError::TooMany`] for more than
    /// [`MOST_ENTRIES`].
    pub fn new<'a>(entries: impl IntoIterator<Item = &'a str>) -> Result<Self, ForwardingError> {
        let mut read: Vec<(Entry, &str)> = Vec::new();
        for text in entries {
            let entry = match text.strip_suffix('*') {
                Some(front) => Entry::Front(name_bytes(front, text)?),
                None => Entry::Whole(name_bytes(text, text)?),
            };
            if let Some(reserved) = crate::RESERVED
                .iter()
                .find(|reserved| entry.matches(reserved.as_str().as_bytes()))
            {
                return Err(ForwardingError::Reserved {
                    entry: text.to_owned(),
                    header: reserved.as_str().to_owned(),
                });
            }
            for (other, other_text) in &read {
                let (wider, narrower) = if other.covers(&entry) {
                    (*other_text, text)
                } else if entry.covers(other) {
                    (text, *other_text)
                } else {
                    continue;
                };
                return Err(ForwardingError::Covered {
                    entry: narrower.to_owned(),
                    by: wider.to_owned(),
                });
            }
            read.push((entry, text));
        }
        if read.len() > MOST_ENTRIES {
            return Err(ForwardingError::TooMany(read.len()));
        }
        Ok(Self {
            entries: read.into_iter().map(|(entry, _)| entry).collect(),
        })
    }

    /// Whether `name`, in whatever case, is one of these.
    #[must_use]
    pub fn matches(&self, name: &[u8]) -> bool {
        self.entries.iter().any(|entry| entry.matches(name))
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries as they would be written, in lower case.
    #[must_use]
    pub fn written(&self) -> Vec<String> {
        self.entries.iter().map(Entry::written).collect()
    }
}

/// `name` in lower case, if it is a header name. `*` is one of the characters a name may
/// have, but here it only ever ends an entry, so that a name that has one elsewhere cannot
/// be mistaken for a front.
fn name_bytes(name: &str, entry: &str) -> Result<Box<[u8]>, ForwardingError> {
    if name.contains('*') {
        return Err(ForwardingError::NotAName(entry.to_owned()));
    }
    HeaderName::from_bytes(name.as_bytes())
        .map(|name| name.as_str().as_bytes().into())
        .map_err(|_| ForwardingError::NotAName(entry.to_owned()))
}

/// The client of a request that came from `peer`, a trusted proxy, with `forwarded_for`
/// the values of its `X-Forwarded-For` fields in the order they came.
///
/// The entries are walked from the right — the one `peer` added last — past the addresses of
/// trusted proxies; the first one that is not trusted is the client. An entry that is no
/// address ends the walk at the last address that was: the nearest proxy that can be
/// vouched for, as nginx has it. Every entry trusted, the client is the leftmost. No entry
/// at all, the client is `peer`.
///
/// An entry is an address, IPv6 as it is or in brackets, and may have a port after it, as
/// some load balancers write it; empty entries are skipped, as HTTP's lists allow. One pass
/// from the left does it, holding nothing but the answer so far.
#[must_use]
pub fn client_address<'a>(
    peer: IpAddr,
    forwarded_for: impl IntoIterator<Item = &'a [u8]>,
    trusted: &TrustedProxies,
) -> IpAddr {
    // What the walk from the right would come to if it reached this far and no farther.
    let mut client = None;
    // An entry that is no address was the last one seen: the next address the walk would
    // have passed is the answer, or `peer` if none comes.
    let mut after_junk = false;
    for value in forwarded_for {
        for entry in value.split(|&byte| byte == b',') {
            let entry = entry.trim_ascii();
            if entry.is_empty() {
                continue;
            }
            match entry_address(entry) {
                None => {
                    client = None;
                    after_junk = true;
                }
                Some(address) if !trusted.trusts(address) => {
                    client = Some(address);
                    after_junk = false;
                }
                Some(address) => {
                    // With no answer so far — first of all, or just after junk — a trusted
                    // address is where the walk would stop: the leftmost, should every
                    // entry be trusted, or the nearest one vouched for past the junk. After
                    // a client it is passed over.
                    if client.is_none() {
                        client = Some(address);
                        after_junk = false;
                    }
                }
            }
        }
    }
    if after_junk {
        return peer.to_canonical();
    }
    client.unwrap_or(peer).to_canonical()
}

/// The address an `X-Forwarded-For` entry names, in its one form.
fn entry_address(entry: &[u8]) -> Option<IpAddr> {
    let text = std::str::from_utf8(entry).ok()?;
    if let Ok(address) = text.parse::<IpAddr>() {
        return Some(address.to_canonical());
    }
    let (address, port) = if let Some(bracketed) = text.strip_prefix('[') {
        let (inside, rest) = bracketed.split_once(']')?;
        let port = match rest.strip_prefix(':') {
            Some(port) => Some(port),
            None if rest.is_empty() => None,
            None => return None,
        };
        (IpAddr::V6(inside.parse().ok()?), port)
    } else {
        let (address, port) = text.split_once(':')?;
        (IpAddr::V4(address.parse().ok()?), Some(port))
    };
    if let Some(port) = port
        && (port.is_empty() || port.len() > 5 || !port.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    Some(address.to_canonical())
}

/// `address` as `X-Forwarded-For` gives it: in its one form, with no port and no brackets,
/// as every proxy surveyed writes it.
#[must_use]
pub fn address_value(address: IpAddr) -> HeaderValue {
    // The longest address, IPv6 with an IPv4 tail, is 45 characters.
    let mut text = String::with_capacity(45);
    // Writing to a `String` does not fail.
    let _written = write!(text, "{}", address.to_canonical());
    HeaderValue::try_from(text).unwrap_or_else(|_| HeaderValue::from_static("unknown"))
}

/// The scheme a request came in by, as `X-Forwarded-Proto` gives it: over TLS, or QUIC,
/// `https`.
#[must_use]
pub fn proto_value(secure: bool) -> HeaderValue {
    HeaderValue::from_static(if secure { "https" } else { "http" })
}

/// What the gateway adds to `Via` for a request that came in with `version`
/// (RFC 9110 §7.6.3): the protocol's version, the name left out as it is HTTP, and the
/// gateway's pseudonym.
#[must_use]
pub fn via_value(version: Version) -> HeaderValue {
    HeaderValue::from_static(match version {
        Version::HTTP_09 => "0.9 edgerush",
        Version::HTTP_10 => "1.0 edgerush",
        Version::HTTP_2 => "2 edgerush",
        Version::HTTP_3 => "3 edgerush",
        _ => "1.1 edgerush",
    })
}

/// Why a listener's forwarding cannot be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ForwardingError {
    /// Not an address range.
    #[error("`{0}` is not an address range such as `10.0.0.0/8` or `2001:db8::/32`")]
    NotARange(String),
    /// A range whose address has bits set past its prefix.
    #[error("`{written}` has bits set past its prefix; the range is `{meant}`")]
    HostBits {
        /// As written.
        written: String,
        /// The range it would stand for.
        meant: String,
    },
    /// An IPv4 range written as IPv6.
    #[error("`{0}` is IPv4 written as IPv6; write it as IPv4, which is how clients are compared")]
    MappedRange(String),
    /// Not a header name, nor the front of one followed by `*`.
    #[error("`{0}` is not a header name, nor the front of one followed by `*`")]
    NotAName(String),
    /// An entry that takes in a header the gateway keeps.
    #[error("`{entry}` takes in `{header}`, which the gateway needs to read the request")]
    Reserved {
        /// As written.
        entry: String,
        /// The header it takes in.
        header: String,
    },
    /// An entry another already takes in.
    #[error("`{entry}` is already taken in by `{by}`")]
    Covered {
        /// The narrower entry.
        entry: String,
        /// The entry that takes it in.
        by: String,
    },
    /// More entries than a list may have.
    #[error("{0} entries; a list has at most 64")]
    TooMany(usize),
}

/// The walk as it is described, for the fuzz target to hold [`client_address`] to: every
/// entry split out and kept, then read from the right.
#[cfg(any(test, feature = "reference"))]
pub mod reference {
    use super::{TrustedProxies, entry_address};
    use std::net::IpAddr;

    /// What [`super::client_address`] comes to, the slow way.
    #[must_use]
    pub fn client_address<'a>(
        peer: IpAddr,
        forwarded_for: impl IntoIterator<Item = &'a [u8]>,
        trusted: &TrustedProxies,
    ) -> IpAddr {
        let entries: Vec<&[u8]> = forwarded_for
            .into_iter()
            .flat_map(|value| value.split(|&byte| byte == b','))
            .map(<[u8]>::trim_ascii)
            .filter(|entry| !entry.is_empty())
            .collect();
        let mut verified = peer.to_canonical();
        for entry in entries.iter().rev() {
            match entry_address(entry) {
                None => return verified,
                Some(address) if !trusted.trusts(address) => return address,
                Some(address) => verified = address,
            }
        }
        verified
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn trusting(ranges: &[&str]) -> TrustedProxies {
        TrustedProxies::new(ranges.iter().copied()).unwrap()
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn a_range_is_an_address_and_a_prefix_written_one_way() {
        for good in [
            "10.0.0.0/8",
            "192.0.2.1/32",
            "0.0.0.0/0",
            "2001:db8::/32",
            "::/0",
        ] {
            assert!(AddressRange::new(good).is_ok(), "{good}");
        }
        for bad in [
            "10.0.0.0",
            "10.0.0.0/",
            "10.0.0.0/33",
            "2001:db8::/129",
            "10.0.0.0/+8",
            "10.0.0/8",
            "10.0.0.0/8/8",
            "/8",
            "any",
            "",
        ] {
            assert_eq!(
                AddressRange::new(bad),
                Err(ForwardingError::NotARange(bad.to_owned())),
                "{bad}"
            );
        }
        assert_eq!(
            AddressRange::new("10.1.2.3/8"),
            Err(ForwardingError::HostBits {
                written: "10.1.2.3/8".to_owned(),
                meant: "10.0.0.0/8".to_owned(),
            })
        );
        assert_eq!(
            AddressRange::new("::ffff:10.0.0.0/104"),
            Err(ForwardingError::MappedRange(
                "::ffff:10.0.0.0/104".to_owned()
            ))
        );
    }

    #[test]
    fn a_range_holds_what_its_prefix_says() {
        let range = AddressRange::new("10.0.0.0/8").unwrap();
        assert!(range.contains(ip("10.255.0.1")));
        assert!(!range.contains(ip("11.0.0.0")));
        assert!(
            !range.contains(ip("::a00:1")),
            "IPv6 is never in an IPv4 range"
        );
        let everything = AddressRange::new("0.0.0.0/0").unwrap();
        assert!(everything.contains(ip("203.0.113.7")));
        assert!(!everything.contains(ip("2001:db8::1")));
        let one = AddressRange::new("2001:db8::1/128").unwrap();
        assert!(one.contains(ip("2001:db8::1")));
        assert!(!one.contains(ip("2001:db8::2")));
    }

    #[test]
    fn an_ipv4_client_of_a_dual_stack_socket_is_trusted_as_ipv4() {
        let trusted = trusting(&["10.0.0.0/8"]);
        assert!(trusted.trusts(ip("::ffff:10.1.0.5")));
        assert!(trusted.trusts(ip("10.1.0.5")));
        assert!(!trusted.trusts(ip("::ffff:11.1.0.5")));
        assert!(!TrustedProxies::default().trusts(ip("10.1.0.5")));
    }

    #[test]
    fn a_list_has_a_bound() {
        let many: Vec<String> = (0..=MOST_ENTRIES)
            .map(|n| format!("10.0.{n}.0/24"))
            .collect();
        assert_eq!(
            TrustedProxies::new(many.iter().map(String::as_str)),
            Err(ForwardingError::TooMany(MOST_ENTRIES + 1))
        );
        let names: Vec<String> = (0..=MOST_ENTRIES).map(|n| format!("x-{n}-")).collect();
        assert_eq!(
            HeaderNames::new(names.iter().map(String::as_str)),
            Err(ForwardingError::TooMany(MOST_ENTRIES + 1))
        );
    }

    #[test]
    fn names_are_whole_or_a_front_and_match_in_any_case() {
        let names = HeaderNames::new(["Forwarded", "X-Real-IP", "X-Forwarded-*"]).unwrap();
        for name in [
            "forwarded",
            "FORWARDED",
            "x-real-ip",
            "X-Forwarded-Port",
            "x-forwarded-",
        ] {
            assert!(names.matches(name.as_bytes()), "{name}");
        }
        for name in [
            "forwarded-for",
            "x-real-ip2",
            "x_forwarded_for",
            "x-forwarded",
            "via",
            "",
        ] {
            assert!(!names.matches(name.as_bytes()), "{name}");
        }
        assert_eq!(names.written(), ["forwarded", "x-real-ip", "x-forwarded-*"]);
        assert!(HeaderNames::default().is_empty());
    }

    #[test]
    fn a_name_that_is_none_or_the_gateways_own_or_said_twice_is_refused() {
        for bad in ["*", "x-*-for", "x y", ":authority", "", "x-forwarded-**"] {
            assert_eq!(
                HeaderNames::new([bad]),
                Err(ForwardingError::NotAName(bad.to_owned())),
                "{bad}"
            );
        }
        for (bad, header) in [
            ("Host", "host"),
            ("con*", "connection"),
            ("T*", "te"),
            ("transfer-*", "transfer-encoding"),
        ] {
            assert_eq!(
                HeaderNames::new([bad]),
                Err(ForwardingError::Reserved {
                    entry: bad.to_owned(),
                    header: header.to_owned(),
                }),
                "{bad}"
            );
        }
        assert_eq!(
            HeaderNames::new(["x-forwarded-*", "X-Forwarded-For"]),
            Err(ForwardingError::Covered {
                entry: "X-Forwarded-For".to_owned(),
                by: "x-forwarded-*".to_owned(),
            })
        );
        assert_eq!(
            HeaderNames::new(["x-forwarded-for", "x-*"]),
            Err(ForwardingError::Covered {
                entry: "x-forwarded-for".to_owned(),
                by: "x-*".to_owned(),
            })
        );
        assert_eq!(
            HeaderNames::new(["forwarded", "Forwarded"]),
            Err(ForwardingError::Covered {
                entry: "Forwarded".to_owned(),
                by: "forwarded".to_owned(),
            })
        );
    }

    fn client(peer: &str, forwarded_for: &[&str], trusted: &[&str]) -> IpAddr {
        client_address(
            ip(peer),
            forwarded_for.iter().map(|value| value.as_bytes()),
            &trusting(trusted),
        )
    }

    #[test]
    fn the_client_is_the_first_address_from_the_right_that_is_not_trusted() {
        let lb = ["10.0.0.0/8"];
        // The load balancer example: a forged entry, then the one the balancer added.
        assert_eq!(
            client("10.1.0.5", &["1.2.3.4, 198.51.100.9"], &lb),
            ip("198.51.100.9")
        );
        // Past a chain of trusted proxies.
        assert_eq!(
            client(
                "10.1.0.5",
                &["1.2.3.4, 198.51.100.9, 10.2.0.1, 10.3.0.1"],
                &lb
            ),
            ip("198.51.100.9")
        );
        // Several fields are one list, in their order.
        assert_eq!(
            client("10.1.0.5", &["1.2.3.4", "198.51.100.9", "10.2.0.1"], &lb),
            ip("198.51.100.9")
        );
        // Every entry trusted: the leftmost.
        assert_eq!(
            client("10.1.0.5", &["10.9.0.1, 10.2.0.1"], &lb),
            ip("10.9.0.1")
        );
        // Nothing said: the peer.
        assert_eq!(client("10.1.0.5", &[], &lb), ip("10.1.0.5"));
        assert_eq!(client("10.1.0.5", &[" , ,"], &lb), ip("10.1.0.5"));
    }

    #[test]
    fn junk_ends_the_walk_at_the_last_address_that_was_one() {
        let lb = ["10.0.0.0/8"];
        assert_eq!(
            client("10.1.0.5", &["198.51.100.9, unknown"], &lb),
            ip("10.1.0.5")
        );
        assert_eq!(
            client("10.1.0.5", &["198.51.100.9, unknown, 10.2.0.1"], &lb),
            ip("10.2.0.1")
        );
        assert_eq!(
            client("10.1.0.5", &["junk, 198.51.100.9, 10.2.0.1"], &lb),
            ip("198.51.100.9")
        );
        assert_eq!(client("10.1.0.5", &["[::1"], &lb), ip("10.1.0.5"));
    }

    #[test]
    fn an_entry_may_come_bracketed_with_a_port_and_is_read_in_one_form() {
        let lb = ["10.0.0.0/8"];
        for (entry, meant) in [
            ("198.51.100.9:4711", "198.51.100.9"),
            ("[2001:db8::17]", "2001:db8::17"),
            ("[2001:db8::17]:4711", "2001:db8::17"),
            ("2001:DB8::17", "2001:db8::17"),
            ("::ffff:198.51.100.9", "198.51.100.9"),
        ] {
            assert_eq!(client("10.1.0.5", &[entry], &lb), ip(meant), "{entry}");
        }
        for junk in [
            "198.51.100.9:",
            "198.51.100.9:123456",
            "198.51.100.9:x",
            "[2001:db8::17]x",
            "[198.51.100.9]",
            "2001:db8::17::1",
            "fe80::1%eth0",
            "_hidden",
        ] {
            assert_eq!(client("10.1.0.5", &[junk], &lb), ip("10.1.0.5"), "{junk}");
        }
    }

    #[test]
    fn values_are_given_in_one_form() {
        assert_eq!(address_value(ip("::ffff:192.0.2.1")), "192.0.2.1");
        assert_eq!(address_value(ip("2001:DB8::1")), "2001:db8::1");
        assert_eq!(proto_value(true), "https");
        assert_eq!(proto_value(false), "http");
        assert_eq!(via_value(Version::HTTP_10), "1.0 edgerush");
        assert_eq!(via_value(Version::HTTP_11), "1.1 edgerush");
        assert_eq!(via_value(Version::HTTP_2), "2 edgerush");
        assert_eq!(via_value(Version::HTTP_3), "3 edgerush");
    }

    fn an_entry() -> impl Strategy<Value = String> {
        prop_oneof![
            (0u8..4, any::<u8>()).prop_map(|(net, host)| format!("10.{net}.0.{host}")),
            (0u8..4, any::<u8>()).prop_map(|(net, host)| format!("198.51.{net}.{host}")),
            any::<u8>().prop_map(|host| format!("[2001:db8::{host:x}]:80")),
            Just("unknown".to_owned()),
            Just(String::new()),
        ]
    }

    proptest! {
        /// The one pass from the left comes to what the walk from the right does.
        #[test]
        fn one_pass_agrees_with_the_walk_from_the_right(
            peer in (0u8..4).prop_map(|net| IpAddr::V4(Ipv4Addr::new(10, net, 0, 1))),
            values in proptest::collection::vec(
                proptest::collection::vec(an_entry(), 0..5).prop_map(|entries| entries.join(", ")),
                0..4,
            ),
            trust_2001 in any::<bool>(),
        ) {
            let mut ranges = vec!["10.0.0.0/9"];
            if trust_2001 {
                ranges.push("2001:db8::/32");
            }
            let trusted = trusting(&ranges);
            prop_assert_eq!(
                client_address(peer, values.iter().map(String::as_bytes), &trusted),
                reference::client_address(peer, values.iter().map(String::as_bytes), &trusted)
            );
        }

        /// Whatever a client writes, the answer is an address it wrote or the peer.
        #[test]
        fn the_client_is_never_made_up(value in ".{0,64}") {
            let peer = ip("10.1.0.5");
            let found = client_address(peer, [value.as_bytes()], &trusting(&["10.0.0.0/8"]));
            prop_assert!(found == peer || value.contains(&found.to_string()) || value.contains(':'));
        }
    }
}
