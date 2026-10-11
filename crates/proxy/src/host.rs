//! The host a request is for: what routing looks up, taken from what the request says.
//!
//! A request names its host in its target (HTTP/2's `:authority`, HTTP/1.1's absolute
//! form) or in its `Host` field. Both are a `host[:port]`, and both are held to the same
//! narrow reading: a DNS name or an IP address, nothing a name could not be. What is left
//! out on purpose — user information, percent-encoding, the punctuation RFC 3986 allows in
//! a registered name — has no honest use in a request and is how two readers of one
//! request come to see two hosts.

use http::header::HOST;
use http::uri::Scheme;
use http::{HeaderMap, Uri};
use std::net::Ipv6Addr;

/// The only `Host` field of a request, as text.
///
/// # Errors
///
/// Returns a [`HostError`] if there is none, more than one, or one that is not ASCII.
pub fn host_field(headers: &HeaderMap) -> Result<&str, HostError> {
    let mut fields = headers.get_all(HOST).iter();
    let field = fields.next().ok_or(HostError::Missing)?;
    if fields.next().is_some() {
        return Err(HostError::Repeated);
    }
    field.to_str().map_err(|_| HostError::Invalid)
}

/// The bare host of a `host[:port]`, as the router wants it: no port, no trailing dot. An
/// IPv6 address keeps its brackets. Letter case is left alone; the router does not mind it.
/// Never allocates.
///
/// # Errors
///
/// Returns [`HostError::Invalid`] for anything but a DNS name or an IP address with an
/// optional port.
pub fn bare_host(authority: &str) -> Result<&str, HostError> {
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (address, rest) = rest.split_once(']').ok_or(HostError::Invalid)?;
        if address.parse::<Ipv6Addr>().is_err() {
            return Err(HostError::Invalid);
        }
        let port = match rest {
            "" => None,
            _ => Some(rest.strip_prefix(':').ok_or(HostError::Invalid)?),
        };
        // The address and its two brackets.
        let host = authority
            .get(..address.len() + 2)
            .ok_or(HostError::Invalid)?;
        (host, port)
    } else {
        let (name, port) = match authority.split_once(':') {
            Some((name, port)) => (name, Some(port)),
            None => (authority, None),
        };
        let is_name_byte = |byte: u8| byte.is_ascii_alphanumeric() || b"-._".contains(&byte);
        if !name.bytes().all(is_name_byte) {
            return Err(HostError::Invalid);
        }
        (name.strip_suffix('.').unwrap_or(name), port)
    };

    // RFC 3986 allows a port of no digits, as in `example.com:`.
    let is_port = |port: &str| {
        port.is_empty() || (port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok())
    };
    if host.is_empty() || !port.is_none_or(is_port) {
        return Err(HostError::Invalid);
    }
    Ok(host)
}

/// Whether `host`, a `Host` field, names the same entity as `authority`, a request's
/// `:authority`, once both are normalised as their scheme says (RFC 9113 §8.3.1, RFC 3986
/// §6.2.3): the same host without regard to case, and the same port, an empty one or
/// `default` — the scheme's, if it is known — being none. Nothing else is taken away.
///
/// # Errors
///
/// [`HostError::Invalid`] if `host` is not a `host[:port]` ([`bare_host`]).
pub fn same_entity(authority: &str, host: &str, default: Option<u16>) -> Result<bool, HostError> {
    bare_host(host)?;
    let (one, one_port) = split_port(authority);
    let (other, other_port) = split_port(host);
    let port = |port: Option<&str>| match port {
        None | Some("") => default,
        Some(digits) => digits.parse::<u16>().ok(),
    };
    Ok(one.eq_ignore_ascii_case(other) && port(one_port) == port(other_port))
}

/// A `host[:port]` as its host, an IPv6 address with its brackets, and what follows the
/// colon, if there is one.
fn split_port(authority: &str) -> (&str, Option<&str>) {
    let end = if authority.starts_with('[') {
        authority.find(']').map_or(authority.len(), |at| at + 1)
    } else {
        authority.find(':').unwrap_or(authority.len())
    };
    let (host, rest) = authority.split_at(end);
    (host, rest.strip_prefix(':'))
}

/// Holds an HTTP/2 request's `Host` fields to its target: each must name the entity the
/// target's authority names, as [`same_entity`] compares them with the scheme's default
/// port (RFC 9113 §8.3.1). A target without an authority holds them to nothing.
///
/// # Errors
///
/// [`HostError::Differs`] for a field naming another entity, [`HostError::Invalid`] for
/// one that names none.
pub(crate) fn hold_to_target<'a>(
    target: &Uri,
    fields: impl IntoIterator<Item = &'a [u8]>,
) -> Result<(), HostError> {
    let Some(authority) = target.authority() else {
        return Ok(());
    };
    let default = match target.scheme() {
        Some(scheme) if *scheme == Scheme::HTTPS => Some(443),
        Some(scheme) if *scheme == Scheme::HTTP => Some(80),
        _ => None,
    };
    for field in fields {
        let field = std::str::from_utf8(field).map_err(|_| HostError::Invalid)?;
        if !same_entity(authority.as_str(), field, default)? {
            return Err(HostError::Differs);
        }
    }
    Ok(())
}

/// Why a request's host cannot be told.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostError {
    /// Neither the request target nor a `Host` field names a host.
    #[error("request names no host")]
    Missing,
    /// More than one `Host` field.
    #[error("request has more than one Host field")]
    Repeated,
    /// Not a DNS name or an IP address with an optional port.
    #[error("host is not a name or an address with an optional port")]
    Invalid,
    /// An HTTP/2 request's `Host` field names another entity than its `:authority`.
    #[error("the Host field names another host than :authority")]
    Differs,
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use http::uri::Authority;
    use proptest::prelude::*;

    #[test]
    fn a_name_is_its_own_bare_host() {
        assert_eq!(bare_host("example.com"), Ok("example.com"));
        assert_eq!(bare_host("localhost"), Ok("localhost"));
        assert_eq!(bare_host("my_service.internal"), Ok("my_service.internal"));
        assert_eq!(bare_host("xn--caf-dma.example"), Ok("xn--caf-dma.example"));
        assert_eq!(bare_host("127.0.0.1"), Ok("127.0.0.1"));
    }

    /// RFC 9113 §8.3.1 has `Host` and `:authority` compared after scheme-based normalisation
    /// (RFC 3986 §6.2.3): the host without regard to case, an empty port or the scheme's
    /// default the same as none. Nothing else is taken away: a trailing dot stays.
    #[test]
    fn the_same_entity_is_told_from_another_by_its_schemes_normal_form() {
        let https = Some(443);
        for (authority, host) in [
            ("a.test", "A.Test"),
            ("a.test", "a.test:443"),
            ("a.test:443", "a.test"),
            ("a.test", "a.test:"),
            ("a.test", "a.test:0443"),
            ("[::1]", "[::1]:443"),
            ("[::A]", "[::a]"),
            ("10.0.0.1:8443", "10.0.0.1:8443"),
        ] {
            assert_eq!(same_entity(authority, host, https), Ok(true), "{host}");
        }
        for (authority, host) in [
            ("a.test", "a.test:80"),
            ("a.test", "b.test"),
            ("a.test", "a.test."),
            ("a.test:8443", "a.test"),
            ("[::1]", "[::2]"),
            ("a.test", "a.testx"),
        ] {
            assert_eq!(same_entity(authority, host, https), Ok(false), "{host}");
        }
        // With no scheme known, no port stands for none but none.
        assert_eq!(same_entity("a.test", "a.test:80", None), Ok(false));
        assert_eq!(same_entity("a.test", "a.test:", None), Ok(true));
        // A field that is no host is not compared.
        assert_eq!(
            same_entity("a.test", "user@a.test", https),
            Err(HostError::Invalid)
        );
    }

    #[test]
    fn the_port_is_left_out() {
        assert_eq!(bare_host("example.com:8080"), Ok("example.com"));
        assert_eq!(bare_host("example.com:0"), Ok("example.com"));
        assert_eq!(bare_host("example.com:65535"), Ok("example.com"));
        assert_eq!(bare_host("example.com:0080"), Ok("example.com"));
        assert_eq!(bare_host("example.com:"), Ok("example.com"));
        assert_eq!(bare_host("127.0.0.1:80"), Ok("127.0.0.1"));
    }

    #[test]
    fn one_trailing_dot_is_left_out() {
        assert_eq!(bare_host("example.com."), Ok("example.com"));
        assert_eq!(bare_host("example.com.:443"), Ok("example.com"));
        assert_eq!(bare_host("example.com.."), Ok("example.com."));
    }

    #[test]
    fn letter_case_is_left_alone() {
        assert_eq!(bare_host("Example.COM:80"), Ok("Example.COM"));
    }

    #[test]
    fn an_ipv6_address_keeps_its_brackets() {
        assert_eq!(bare_host("[::1]"), Ok("[::1]"));
        assert_eq!(bare_host("[::1]:8080"), Ok("[::1]"));
        assert_eq!(bare_host("[2001:db8::7]:"), Ok("[2001:db8::7]"));
        assert_eq!(bare_host("[::ffff:192.0.2.1]:80"), Ok("[::ffff:192.0.2.1]"));
    }

    #[test]
    fn what_is_not_a_name_or_an_address_is_rejected() {
        for authority in [
            "",
            ".",
            ":80",
            "example.com:80:80",
            "example.com:http",
            "example.com:+80",
            "example.com:65536",
            "user@example.com",
            "user:secret@example.com",
            "example.com/path",
            "example.com?query",
            "example.com#fragment",
            "exa mple.com",
            " example.com",
            "example.com ",
            "example.com,example.org",
            "ex%61mple.com",
            "café.example",
            "*.example.com",
            "::1",
            "[::1",
            "[]",
            "[::1]x",
            "[::1]80",
            "[::1]:http",
            "[example.com]",
            "[v7.fe80::1]",
            "[fe80::1%25eth0]",
            "[::1]]",
        ] {
            assert_eq!(
                bare_host(authority),
                Err(HostError::Invalid),
                "{authority:?}"
            );
        }
    }

    fn headers(hosts: &[&'static [u8]]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for host in hosts {
            headers.append(HOST, HeaderValue::from_bytes(host).unwrap());
        }
        headers
    }

    #[test]
    fn the_host_field_is_read_as_text() {
        assert_eq!(
            host_field(&headers(&[b"example.com:80"])),
            Ok("example.com:80")
        );
    }

    #[test]
    fn no_host_field_is_no_host() {
        assert_eq!(host_field(&headers(&[])), Err(HostError::Missing));
    }

    #[test]
    fn host_fields_that_disagree_or_agree_are_one_too_many() {
        let twice = headers(&[b"example.com", b"example.org"]);
        assert_eq!(host_field(&twice), Err(HostError::Repeated));
        let same = headers(&[b"example.com", b"example.com"]);
        assert_eq!(host_field(&same), Err(HostError::Repeated));
    }

    #[test]
    fn a_host_field_that_is_not_ascii_is_rejected() {
        assert_eq!(
            host_field(&headers(&[b"caf\xE9.example"])),
            Err(HostError::Invalid)
        );
    }

    /// Text that is often nearly a `host[:port]`.
    fn nearly_an_authority() -> impl Strategy<Value = String> {
        let piece = prop_oneof![
            4 => "[a-zA-Z0-9]{1,4}",
            2 => Just(".".to_owned()),
            2 => Just(":".to_owned()),
            1 => Just("-".to_owned()),
            1 => Just("_".to_owned()),
            1 => Just("[".to_owned()),
            1 => Just("]".to_owned()),
            1 => Just("::".to_owned()),
            1 => Just("@".to_owned()),
            1 => "[0-9]{1,6}",
            1 => "[ -~]",
        ];
        prop::collection::vec(piece, 0..8).prop_map(|pieces| pieces.concat())
    }

    proptest! {
        /// What is accepted here, the `http` crate reads in the same way — so the host that
        /// is routed on is the host a request built from the same text is sent to.
        #[test]
        fn the_http_crate_agrees_on_every_host_that_is_accepted(text in nearly_an_authority()) {
            if let Ok(host) = bare_host(&text) {
                let authority = Authority::try_from(text.as_str());
                prop_assert!(authority.is_ok(), "{text:?}");
                let theirs = authority.unwrap();
                let theirs = theirs.host();
                prop_assert_eq!(host, theirs.strip_suffix('.').unwrap_or(theirs), "{:?}", text);
            }
        }

        /// The same entity both ways, and every host the same as itself.
        #[test]
        fn the_same_entity_is_the_same_whichever_way_it_is_asked(
            one in nearly_an_authority(),
            other in nearly_an_authority(),
            default in prop::option::of(any::<u16>()),
        ) {
            if bare_host(&one).is_ok() && bare_host(&other).is_ok() {
                prop_assert_eq!(same_entity(&one, &other, default), same_entity(&other, &one, default));
                prop_assert_eq!(same_entity(&one, &one, default), Ok(true));
            }
        }

        #[test]
        fn a_bare_host_is_part_of_the_text_and_stays_what_it_is(text in nearly_an_authority()) {
            if let Ok(host) = bare_host(&text) {
                prop_assert!(text.starts_with(host));
                prop_assert!(!host.is_empty());
                if !host.ends_with('.') {
                    prop_assert_eq!(bare_host(host), Ok(host));
                }
            }
        }
    }
}
