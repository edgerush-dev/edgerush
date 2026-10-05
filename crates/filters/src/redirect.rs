//! Redirects: Gateway API's `RequestRedirect`, answered by the gateway itself.
//!
//! **`Location` holds what the redirect states, and only what it needs besides to be a
//! valid reference** (18 §3 in the docs). A redirect that states none of scheme, host and
//! port stays on the request's origin, and its `Location` is relative: the client resolves
//! it against the URI it asked for (RFC 9110 §10.2.2), keeping its scheme, host and port.
//! Any other is absolute, and what it does not state comes from the request's own URI as
//! RFC 9112 §3.3 has a server reconstruct it: the scheme from the listener, the host from
//! the request. A port is written only when stated, and not when it is its scheme's
//! default. A redirect to another host is never `//host`, which leaves the scheme to the
//! client.
//!
//! Nothing in a `Location` is checked per request, because nothing needs to be: the path is
//! the normalised one — a single `/` first, never a `\` — so a relative `Location` cannot be
//! read as another host (RFC 3986 §4.2, and the WHATWG URL parser's backslash rules); what
//! the redirect states was checked when it was built; the query is one the server accepted.

use crate::path_modifier::PathModifier;
use http::{HeaderValue, StatusCode};
use std::net::Ipv4Addr;

/// A scheme a redirect can send a client to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `http`, port 80 unless said.
    Http,
    /// `https`, port 443 unless said.
    Https,
}

impl Scheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }
}

/// What becomes of the request's query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Query {
    /// Carried over byte for byte.
    Keep,
    /// Left out.
    Drop,
}

/// The request a redirect answers, as far as its `Location` needs it.
#[derive(Debug, Clone, Copy)]
pub struct Requested<'a> {
    /// The scheme of the listener it came in on.
    pub scheme: Scheme,
    /// The host it was routed on: no port, no trailing dot, in whatever case it came.
    pub host: &'a str,
    /// Its normalised path.
    pub path: &'a str,
    /// Its query, as it came, if it had one.
    pub query: Option<&'a str>,
}

/// A redirect, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redirect {
    status: StatusCode,
    origin: Origin,
    path: Option<PathModifier>,
    query: Query,
}

/// Where a redirect sends the client, as far as it says.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Origin {
    /// Nowhere else: the `Location` is relative.
    Same,
    /// A scheme and a host stated, and perhaps a port: all of it made once.
    Fixed(Box<str>),
    /// Some of it stated; the rest from the request.
    Partial {
        scheme: Option<Scheme>,
        host: Option<Box<str>>,
        /// The port and its `:digits`, made once.
        port: Option<(u16, Box<str>)>,
    },
}

/// The statuses a redirect may have: Gateway API's five.
pub const STATUSES: [u16; 5] = [301, 302, 303, 307, 308];

impl Redirect {
    /// A redirect with this status (one of [`STATUSES`]), sending the client to what
    /// it states of scheme, host, port and path, and keeping or dropping the query.
    ///
    /// # Errors
    ///
    /// Returns a [`RedirectError`] for a status that is none of the five, a host that is
    /// not a DNS name in lower case (Gateway API's `PreciseHostname`: no wildcard, port,
    /// trailing dot or address), or a port of 0.
    pub fn new(
        status: u16,
        scheme: Option<Scheme>,
        host: Option<&str>,
        port: Option<u16>,
        path: Option<PathModifier>,
        query: Query,
    ) -> Result<Self, RedirectError> {
        if !STATUSES.contains(&status) {
            return Err(RedirectError::Status(status));
        }
        let status = StatusCode::from_u16(status).map_err(|_| RedirectError::Status(status))?;
        if let Some(host) = host
            && !is_precise_host(host)
        {
            return Err(RedirectError::Host(host.to_owned()));
        }
        if port == Some(0) {
            return Err(RedirectError::Port);
        }
        let origin = match (scheme, host, port) {
            (None, None, None) => Origin::Same,
            (Some(scheme), Some(host), port) => {
                let mut fixed = format!("{}://{host}", scheme.as_str());
                if let Some(port) = port.filter(|port| *port != scheme.default_port()) {
                    fixed.push_str(&format!(":{port}"));
                }
                Origin::Fixed(fixed.into())
            }
            (scheme, host, port) => Origin::Partial {
                scheme,
                host: host.map(Into::into),
                port: port.map(|port| (port, format!(":{port}").into())),
            },
        };
        Ok(Self {
            status,
            origin,
            path,
            query,
        })
    }

    /// The status to answer with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The path change, if the redirect makes one.
    #[must_use]
    pub fn path(&self) -> Option<&PathModifier> {
        self.path.as_ref()
    }

    /// The `Location` to send the client of `request` to. One allocation, and no
    /// formatting.
    ///
    /// # Errors
    ///
    /// None is known to happen: what goes in is all characters a field value may hold.
    pub fn location(
        &self,
        request: &Requested<'_>,
    ) -> Result<HeaderValue, http::header::InvalidHeaderValue> {
        let query = match (self.query, request.query) {
            (Query::Keep, Some(query)) => Some(query),
            _ => None,
        };
        let mut location = String::with_capacity(
            self.origin_len(request)
                + request.path.len()
                + query.map_or(0, |query| query.len() + 1),
        );
        match &self.origin {
            Origin::Same => {}
            Origin::Fixed(origin) => location.push_str(origin),
            Origin::Partial { scheme, host, port } => {
                let scheme = scheme.unwrap_or(request.scheme);
                location.push_str(scheme.as_str());
                location.push_str("://");
                match host {
                    Some(host) => location.push_str(host),
                    // Letter case means nothing in a host; lower case is its normal form
                    // (RFC 3986 §6.2.2.1).
                    None => {
                        let start = location.len();
                        location.push_str(request.host);
                        if let Some(host) = location.get_mut(start..) {
                            host.make_ascii_lowercase();
                        }
                    }
                }
                if let Some((port, digits)) = port
                    && *port != scheme.default_port()
                {
                    location.push_str(digits);
                }
            }
        }
        match &self.path {
            Some(path) => path.write(request.path, &mut location),
            None => location.push_str(request.path),
        }
        if let Some(query) = query {
            location.push('?');
            location.push_str(query);
        }
        HeaderValue::try_from(location)
    }

    /// Room for what goes before the path: enough, not exact.
    fn origin_len(&self, request: &Requested<'_>) -> usize {
        match &self.origin {
            Origin::Same => 0,
            Origin::Fixed(origin) => origin.len(),
            // `https://`, the host, and `:65535`.
            Origin::Partial { host, .. } => {
                8 + host.as_deref().map_or(request.host.len(), str::len) + 6
            }
        }
    }
}

/// A DNS name as Gateway API's `PreciseHostname` has it: labels of lower-case letters,
/// digits and inner hyphens, 63 bytes at most, 253 in all; and not an IPv4 address, which
/// the pattern alone would let through.
pub(crate) fn is_precise_host(host: &str) -> bool {
    let label = |label: &str| {
        let bytes = label.as_bytes();
        (1..=63).contains(&bytes.len())
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            && bytes.first() != Some(&b'-')
            && bytes.last() != Some(&b'-')
    };
    host.len() <= 253 && host.split('.').all(label) && host.parse::<Ipv4Addr>().is_err()
}

/// Why a redirect was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedirectError {
    /// A status that is not a redirect Gateway API has.
    #[error("redirect status {0}: 301, 302, 303, 307 or 308")]
    Status(u16),
    /// A host that is not a DNS name in lower case.
    #[error("redirect host `{0}` is not a DNS name in lower case")]
    Host(String),
    /// A port of nothing.
    #[error("redirect port 0: from 1 to 65535")]
    Port,
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_router::PathPattern;
    use proptest::prelude::*;

    const PLAIN: Scheme = Scheme::Http;
    const SECURE: Scheme = Scheme::Https;

    fn request<'a>(scheme: Scheme, path: &'a str, query: Option<&'a str>) -> Requested<'a> {
        Requested {
            scheme,
            host: "shop.example.com",
            path,
            query,
        }
    }

    fn redirect(scheme: Option<Scheme>, host: Option<&str>, port: Option<u16>) -> Redirect {
        Redirect::new(302, scheme, host, port, None, Query::Keep).unwrap()
    }

    fn location(redirect: &Redirect, request: &Requested<'_>) -> String {
        redirect
            .location(request)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()
    }

    /// The table of 18 §3: what is stated, and what the `Location` is.
    #[test]
    fn location_holds_what_is_stated_and_what_it_needs_besides() {
        let on_plain = request(PLAIN, "/new", Some("q=1"));
        let table = [
            (redirect(None, None, None), "/new?q=1"),
            (
                redirect(Some(SECURE), None, None),
                "https://shop.example.com/new?q=1",
            ),
            (
                redirect(None, Some("www.example.org"), None),
                "http://www.example.org/new?q=1",
            ),
            (
                redirect(None, None, Some(8443)),
                "http://shop.example.com:8443/new?q=1",
            ),
            (
                redirect(Some(SECURE), Some("www.example.org"), None),
                "https://www.example.org/new?q=1",
            ),
            (
                redirect(Some(SECURE), Some("www.example.org"), Some(8443)),
                "https://www.example.org:8443/new?q=1",
            ),
        ];
        for (redirect, expected) in table {
            assert_eq!(location(&redirect, &on_plain), expected, "{redirect:?}");
        }
        // The listener's scheme is what an unstated one is.
        let on_secure = request(SECURE, "/new", None);
        assert_eq!(
            location(&redirect(None, Some("www.example.org"), None), &on_secure),
            "https://www.example.org/new"
        );
    }

    /// Gateway API's `httproute-redirect-port-and-scheme` cases that the data plane
    /// decides: a stated port is left out where it is its scheme's default, and a stated
    /// scheme alone means its default port. (Where neither is stated the `Location` is
    /// relative, and the client keeps the listener's port.)
    #[test]
    fn a_default_port_is_left_out_whichever_side_the_scheme_comes_from() {
        let cases = [
            (PLAIN, None, Some(80), "http://shop.example.com/p"),
            (PLAIN, None, Some(8080), "http://shop.example.com:8080/p"),
            (PLAIN, Some(SECURE), None, "https://shop.example.com/p"),
            (PLAIN, Some(SECURE), Some(443), "https://shop.example.com/p"),
            (
                PLAIN,
                Some(SECURE),
                Some(8443),
                "https://shop.example.com:8443/p",
            ),
            (SECURE, None, Some(443), "https://shop.example.com/p"),
            (SECURE, None, Some(8443), "https://shop.example.com:8443/p"),
            (SECURE, Some(PLAIN), None, "http://shop.example.com/p"),
            (SECURE, Some(PLAIN), Some(80), "http://shop.example.com/p"),
            (
                SECURE,
                Some(PLAIN),
                Some(8080),
                "http://shop.example.com:8080/p",
            ),
            (SECURE, None, Some(80), "https://shop.example.com:80/p"),
            (PLAIN, None, None, "/p"),
            (SECURE, None, None, "/p"),
        ];
        for (listener, scheme, port, expected) in cases {
            let got = location(
                &redirect(scheme, None, port),
                &request(listener, "/p", None),
            );
            assert_eq!(got, expected, "{listener:?} {scheme:?} {port:?}");
        }
    }

    #[test]
    fn the_query_is_kept_byte_for_byte_or_dropped() {
        let keep = redirect(None, None, None);
        let drop = Redirect::new(301, None, None, None, None, Query::Drop).unwrap();
        let odd = "a=%2f&b=%E2%82%AC&c&=d;e";
        assert_eq!(
            location(&keep, &request(PLAIN, "/p", Some(odd))),
            format!("/p?{odd}")
        );
        assert_eq!(location(&keep, &request(PLAIN, "/p", Some(""))), "/p?");
        assert_eq!(location(&keep, &request(PLAIN, "/p", None)), "/p");
        assert_eq!(location(&drop, &request(PLAIN, "/p", Some(odd))), "/p");
    }

    #[test]
    fn the_requests_host_goes_in_lower_case_an_address_in_its_brackets() {
        let to_secure = redirect(Some(SECURE), None, None);
        let shouting = Requested {
            host: "Shop.Example.COM",
            ..request(PLAIN, "/", None)
        };
        assert_eq!(location(&to_secure, &shouting), "https://shop.example.com/");
        let address = Requested {
            host: "[2001:db8::1]",
            ..request(PLAIN, "/", None)
        };
        assert_eq!(location(&to_secure, &address), "https://[2001:db8::1]/");
    }

    #[test]
    fn the_path_is_changed_as_the_redirect_says() {
        let prefix = PathPattern::prefix("/old").unwrap();
        let moved = Redirect::new(
            301,
            None,
            None,
            None,
            Some(PathModifier::prefix(&prefix, "/new").unwrap()),
            Query::Keep,
        )
        .unwrap();
        assert_eq!(
            location(&moved, &request(PLAIN, "/old/a", Some("b"))),
            "/new/a?b"
        );
        assert_eq!(moved.status(), StatusCode::MOVED_PERMANENTLY);
    }

    #[test]
    fn a_status_host_or_port_a_redirect_cannot_have_is_refused() {
        for status in [301, 302, 303, 307, 308] {
            assert!(Redirect::new(status, None, None, None, None, Query::Keep).is_ok());
        }
        for status in [200, 300, 304, 305, 306, 309, 400] {
            assert_eq!(
                Redirect::new(status, None, None, None, None, Query::Keep),
                Err(RedirectError::Status(status))
            );
        }
        let host = |host: &str| Redirect::new(302, None, Some(host), None, None, Query::Keep);
        for good in [
            "example.org",
            "a-b.example",
            "x",
            "1.example",
            &"a".repeat(63),
        ] {
            assert!(host(good).is_ok(), "{good}");
        }
        for bad in [
            "Example.org",
            "*.example.org",
            "example.org.",
            "example.org:8080",
            "a_b.example",
            "-a.example",
            "a-.example",
            "a..example",
            "",
            "192.0.2.1",
            "[2001:db8::1]",
            "ex ample",
            "/evil",
            &"a".repeat(64),
            &format!("{}.example", "a.".repeat(124)),
        ] {
            assert_eq!(host(bad), Err(RedirectError::Host(bad.to_owned())), "{bad}");
        }
        assert_eq!(
            Redirect::new(302, None, None, Some(0), None, Query::Keep),
            Err(RedirectError::Port)
        );
    }

    /// A normalised path: segments of plain path characters and upper-case encodings.
    fn normal_path() -> impl Strategy<Value = String> {
        let segment = prop::collection::vec(
            prop_oneof![
                Just("a"),
                Just("Z9"),
                Just("%C3%A9"),
                Just("%25"),
                Just(":"),
                Just("@"),
                Just("~"),
                Just("!$&'()*+,;="),
            ],
            1..4,
        )
        .prop_map(|parts| parts.concat());
        (prop::collection::vec(segment, 0..4), any::<bool>()).prop_map(|(segments, slash)| {
            let mut path = format!("/{}", segments.join("/"));
            if slash && !segments.is_empty() {
                path.push('/');
            }
            path
        })
    }

    /// A query as a server accepts one: RFC 3986's query characters.
    fn query() -> impl Strategy<Value = Option<String>> {
        prop::option::of("[a-zA-Z0-9=&%/?:@!$'()*+,;._~-]{0,12}")
    }

    fn scheme() -> impl Strategy<Value = Scheme> {
        prop_oneof![Just(PLAIN), Just(SECURE)]
    }

    proptest! {
        /// Whatever a redirect states and whatever the request, the `Location` is one
        /// reference that the `http` crate reads back into exactly the parts meant, and
        /// never one a browser could read another host into.
        #[test]
        fn a_location_is_one_reference_with_the_parts_meant(
            listener in scheme(),
            stated_scheme in prop::option::of(scheme()),
            stated_host in prop::option::of(prop_oneof![Just("www.example.org"), Just("a-1.test")]),
            stated_port in prop::option::of(1_u16..),
            host in prop_oneof![Just("Shop.Example.com"), Just("[2001:db8::1]"), Just("10.0.0.1")],
            path in normal_path(),
            query in query(),
            keep in any::<bool>(),
        ) {
            let redirect = Redirect::new(
                308,
                stated_scheme,
                stated_host,
                stated_port,
                None,
                if keep { Query::Keep } else { Query::Drop },
            ).unwrap();
            let requested = Requested { scheme: listener, host, path: &path, query: query.as_deref() };
            let got = location(&redirect, &requested);

            // What it should be, put together the plain way.
            let mut expected = String::new();
            if stated_scheme.is_some() || stated_host.is_some() || stated_port.is_some() {
                let scheme = stated_scheme.unwrap_or(listener);
                let host = stated_host.map_or_else(|| host.to_ascii_lowercase(), str::to_owned);
                expected = format!("{}://{host}", scheme.as_str());
                if let Some(port) = stated_port.filter(|port| *port != scheme.default_port()) {
                    expected += &format!(":{port}");
                }
            }
            expected += &path;
            if let (true, Some(query)) = (keep, &query) {
                expected += &format!("?{query}");
            }
            prop_assert_eq!(&got, &expected);

            // Read back by an independent parser.
            let uri: http::Uri = got.parse().unwrap();
            prop_assert_eq!(uri.path(), path.as_str());
            prop_assert_eq!(uri.query(), if keep { query.as_deref() } else { None });
            if expected.starts_with('/') {
                prop_assert!(uri.authority().is_none());
            } else {
                prop_assert!(uri.host().is_some());
            }
            // Nothing a browser reads as a second host.
            prop_assert!(!got.starts_with("//"));
            prop_assert!(!got.contains('\\'));
            prop_assert!(!got.bytes().any(|byte| byte.is_ascii_control() || byte == b' '));
        }
    }
}
