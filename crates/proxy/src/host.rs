//! The host a request is for: what routing looks up, taken from what the request says.
//!
//! A request names its host in its target (HTTP/2's `:authority`, HTTP/1.1's absolute
//! form) or in its `Host` field. Both are a `host[:port]`, and both are held to the same
//! narrow reading: a DNS name or an IP address, nothing a name could not be. What is left
//! out on purpose — user information, percent-encoding, the punctuation RFC 3986 allows in
//! a registered name — has no honest use in a request and is how two readers of one
//! request come to see two hosts.

use http::HeaderMap;
use http::header::HOST;
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
