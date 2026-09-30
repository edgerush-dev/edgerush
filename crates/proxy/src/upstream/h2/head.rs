//! The head of a request as it goes to an HTTP/2 upstream (RFC 9113 §8.2–8.3).
//!
//! The request arrives in whatever version the client spoke, already routed and with what
//! was about the client's connection taken off. What is left to do is HTTP/2's own:
//! the authority goes in `:authority` and not in `Host`; nothing that is about a
//! connection may appear at all, since HTTP/2 has none of those fields and a peer must
//! treat one as a malformed request; `TE` may say `trailers` and nothing else; and the
//! body's length is said as it will be sent, not as it was said on the way in.

use crate::upstream::h1::codec::{HeldField, OutgoingFields, Sending};
use http::header::{CONTENT_LENGTH, TE};
use http::uri::{Authority, PathAndQuery, Scheme};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Uri, Version};

/// Why a request cannot be sent over HTTP/2.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HeadError {
    /// It names no host, which the core does not let through.
    #[error("the request names no host")]
    NoAuthority,
    /// Its host is not an authority: what the core let through as a `Host` is always one.
    #[error("the request's host is not an authority")]
    BadAuthority,
    /// A field that is not a field. What reaches here was checked on the way in, so this is
    /// not known to happen.
    #[error("a field cannot be sent over HTTP/2")]
    BadField,
}

/// The head of a request for `target` to be sent over HTTP/2 to a destination whose
/// scheme is `scheme`, its fields `fields` and its body sent as `sending` says.
///
/// # Errors
///
/// A [`HeadError`] for a request that names no host that could be an authority.
pub(crate) fn request<F: OutgoingFields + ?Sized>(
    method: &Method,
    target: &Uri,
    fields: &F,
    sending: Sending,
    scheme: Scheme,
) -> Result<Request<()>, HeadError> {
    // What the client asked for, in `:authority`: an intermediary builds it from the
    // request it forwards (RFC 9113 §8.3.1), and the core has made sure there is exactly
    // one host to build it from.
    let host = fields
        .values(&http::header::HOST)
        .next()
        .ok_or(HeadError::NoAuthority)?;
    let authority = Authority::try_from(host).map_err(|_| HeadError::BadAuthority)?;
    let path = target
        .path_and_query()
        .cloned()
        .unwrap_or_else(|| PathAndQuery::from_static("/"));
    let uri = Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path)
        .build()
        .map_err(|_| HeadError::BadAuthority)?;

    let mut headers = match fields.shared_count() {
        // Each name and value shared, not read and checked again, and room made once for
        // all of them (and a length) rather than as they come.
        Some(count) => {
            let mut headers = HeaderMap::with_capacity(count + 1);
            fields.each_shared(|name, value| {
                if travels(name.as_str().as_bytes(), value.as_bytes()) {
                    headers.append(name.clone(), value.clone());
                }
            });
            headers
        }
        // The lines read and checked again; what was added on the way shared, as it was
        // added.
        None => {
            let mut headers = HeaderMap::new();
            let mut failed = false;
            fields.each_outgoing(|field| {
                if failed {
                    return;
                }
                match field {
                    HeldField::Line(name, value) if travels(name, value) => {
                        match (HeaderName::from_bytes(name), HeaderValue::from_bytes(value)) {
                            (Ok(name), Ok(value)) => {
                                headers.append(name, value);
                            }
                            _ => failed = true,
                        }
                    }
                    HeldField::Added(name, value)
                        if travels(name.as_str().as_bytes(), value.as_bytes()) =>
                    {
                        headers.append(name.clone(), value.clone());
                    }
                    HeldField::Line(..) | HeldField::Added(..) => {}
                }
            });
            if failed {
                return Err(HeadError::BadField);
            }
            headers
        }
    };
    if let Sending::Length(length) = sending {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    }

    let mut request = Request::new(());
    *request.method_mut() = method.clone();
    *request.uri_mut() = uri;
    *request.version_mut() = Version::HTTP_2;
    *request.headers_mut() = headers;
    Ok(request)
}

/// Whether a field goes over HTTP/2 as it is. Names are compared as they arrived, in
/// whatever case.
fn travels(name: &[u8], value: &[u8]) -> bool {
    let is = |other: &[u8]| name.eq_ignore_ascii_case(other);
    // `TE` alone may appear, and only as `trailers` (RFC 9113 §8.2.2): what gRPC clients
    // send, and what gRPC servers look for.
    if is(TE.as_str().as_bytes()) {
        return value.trim_ascii().eq_ignore_ascii_case(b"trailers");
    }
    // About a connection, which HTTP/2 has none of (RFC 9113 §8.2.2); `Host`, which
    // `:authority` says instead (§8.3.1); HTTP/2's own upgrade setting; and the body's
    // framing, which is said again from how it is sent.
    !(is(b"connection")
        || is(b"keep-alive")
        || is(b"proxy-connection")
        || is(b"transfer-encoding")
        || is(b"upgrade")
        || is(b"host")
        || is(b"http2-settings")
        || is(b"content-length"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The head for a destination reached without TLS.
    fn head<F: OutgoingFields + ?Sized>(
        method: &Method,
        target: &Uri,
        fields: &F,
        sending: Sending,
    ) -> Result<Request<()>, HeadError> {
        request(method, target, fields, sending, Scheme::HTTP)
    }

    #[test]
    fn the_scheme_is_the_destinations() {
        let fields = fields(&[("host", "shop.example.com")]);
        let target = target("http://10.0.0.1:8443/a");
        for scheme in [Scheme::HTTP, Scheme::HTTPS] {
            let sent_as = request(
                &Method::GET,
                &target,
                &fields,
                Sending::None,
                scheme.clone(),
            );
            assert_eq!(sent_as.unwrap().uri().scheme(), Some(&scheme));
        }
    }

    fn fields(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    fn target(uri: &str) -> Uri {
        uri.parse().unwrap()
    }

    /// Fields that are not held as a header map, as a raw head's are not: read one by one.
    struct Lines(HeaderMap);

    impl edgerush_router::Fields for Lines {
        fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
            self.0.get_all(name).iter().map(HeaderValue::as_bytes)
        }
    }

    impl OutgoingFields for Lines {
        fn written_len(&self) -> usize {
            self.0.written_len()
        }

        fn write_fields(&self, out: &mut Vec<u8>) {
            self.0.write_fields(out);
        }

        fn each_field(&self, visit: impl FnMut(&[u8], &[u8])) {
            self.0.each_field(visit);
        }
    }

    /// Fields held as lines with others added on the way, as a raw head holds them.
    struct LinesAndAdded(HeaderMap, Vec<(HeaderName, HeaderValue)>);

    impl edgerush_router::Fields for LinesAndAdded {
        fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
            let added = self.1.iter().filter(move |(added, _)| added == name);
            self.0
                .get_all(name)
                .iter()
                .chain(added.map(|(_, value)| value))
                .map(HeaderValue::as_bytes)
        }
    }

    impl OutgoingFields for LinesAndAdded {
        fn written_len(&self) -> usize {
            self.0.written_len()
        }

        fn write_fields(&self, out: &mut Vec<u8>) {
            self.0.write_fields(out);
        }

        fn each_field(&self, mut visit: impl FnMut(&[u8], &[u8])) {
            self.each_outgoing(|field| match field {
                HeldField::Line(name, value) => visit(name, value),
                HeldField::Added(name, value) => visit(name.as_str().as_bytes(), value.as_bytes()),
            });
        }

        fn each_outgoing(&self, mut visit: impl FnMut(HeldField<'_>)) {
            for (name, value) in &self.0 {
                visit(HeldField::Line(name.as_str().as_bytes(), value.as_bytes()));
            }
            for (name, value) in &self.1 {
                visit(HeldField::Added(name, value));
            }
        }
    }

    /// What was added on the way goes as it was added, its flags kept — a request's ID
    /// never indexed — and held to what HTTP/2 lets through as the lines are.
    #[test]
    fn what_was_added_goes_as_it_was_added() {
        let mut id = HeaderValue::from_static("0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f");
        id.set_sensitive(true);
        let fields = LinesAndAdded(
            fields(&[("host", "a.test"), ("accept", "*/*")]),
            vec![
                (HeaderName::from_static("x-request-id"), id.clone()),
                (http::header::CONNECTION, HeaderValue::from_static("close")),
            ],
        );
        let sent_as = head(&Method::GET, &target("/"), &fields, Sending::None).unwrap();
        assert_eq!(sent_as.headers()["x-request-id"], id);
        assert!(sent_as.headers()["x-request-id"].is_sensitive());
        assert_eq!(sent_as.headers()["accept"], "*/*");
        assert!(!sent_as.headers().contains_key("connection"));
    }

    #[test]
    fn the_host_becomes_the_authority_and_the_target_the_path() {
        let sent_as = head(
            &Method::GET,
            &target("http://10.0.0.1:8080/a/b?c=d"),
            &fields(&[("host", "shop.example.com"), ("accept", "*/*")]),
            Sending::None,
        )
        .unwrap();
        assert_eq!(sent_as.version(), Version::HTTP_2);
        assert_eq!(sent_as.method(), Method::GET);
        assert_eq!(sent_as.uri().to_string(), "http://shop.example.com/a/b?c=d");
        assert!(sent_as.headers().get("host").is_none());
        assert_eq!(sent_as.headers()["accept"], "*/*");

        // A host with a port keeps it; an asterisk-form target stays one.
        let sent_as = head(
            &Method::OPTIONS,
            &target("*"),
            &fields(&[("host", "shop.example.com:8443")]),
            Sending::None,
        )
        .unwrap();
        assert_eq!(sent_as.uri().authority().unwrap(), "shop.example.com:8443");
        assert_eq!(sent_as.uri().path(), "*");
    }

    #[test]
    fn a_request_without_a_host_that_could_be_an_authority_is_not_sent() {
        let without = head(&Method::GET, &target("/"), &fields(&[]), Sending::None);
        assert_eq!(without.unwrap_err(), HeadError::NoAuthority);
        let bad = head(
            &Method::GET,
            &target("/"),
            &fields(&[("host", "a b")]),
            Sending::None,
        );
        assert_eq!(bad.unwrap_err(), HeadError::BadAuthority);
    }

    #[test]
    fn nothing_about_a_connection_goes_and_te_goes_only_as_trailers() {
        let sent = fields(&[
            ("host", "a.test"),
            ("connection", "keep-alive"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("http2-settings", "AAMAAABkAAQAAP__"),
            ("te", "trailers"),
            ("x-kept", "1"),
            ("proxy-authorization", "Basic added-by-a-rule"),
        ]);
        let sent_as = head(&Method::POST, &target("/"), &sent, Sending::Chunked).unwrap();
        let names: Vec<&str> = sent_as.headers().keys().map(HeaderName::as_str).collect();
        assert_eq!(names, ["te", "x-kept", "proxy-authorization"]);
        assert_eq!(sent_as.headers()["te"], "trailers");

        let other_te = fields(&[("host", "a.test"), ("te", "gzip")]);
        let sent_as = head(&Method::GET, &target("/"), &other_te, Sending::None).unwrap();
        assert!(sent_as.headers().get("te").is_none());
    }

    #[test]
    fn the_length_is_said_as_the_body_will_be_sent() {
        let said = fields(&[("host", "a.test"), ("content-length", "999")]);
        let length = head(&Method::POST, &target("/"), &said, Sending::Length(5)).unwrap();
        assert_eq!(length.headers().get_all("content-length").iter().count(), 1);
        assert_eq!(length.headers()["content-length"], "5");
        let empty = head(&Method::POST, &target("/"), &said, Sending::Length(0)).unwrap();
        assert_eq!(empty.headers()["content-length"], "0");
        let chunked = head(&Method::POST, &target("/"), &said, Sending::Chunked).unwrap();
        assert!(chunked.headers().get("content-length").is_none());
        let none = head(&Method::GET, &target("/"), &said, Sending::None).unwrap();
        assert!(none.headers().get("content-length").is_none());
    }

    /// Repeated fields keep their order, and so do fields of different names relative to
    /// one another as far as a header map can say.
    #[test]
    fn repeated_fields_keep_their_order() {
        let sent = fields(&[
            ("host", "a.test"),
            ("cookie", "a=1"),
            ("x-one", "1"),
            ("cookie", "b=2"),
        ]);
        let sent_as = head(&Method::GET, &target("/"), &sent, Sending::None).unwrap();
        let cookies: Vec<&[u8]> = sent_as
            .headers()
            .get_all("cookie")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect();
        assert_eq!(cookies, [&b"a=1"[..], b"b=2"]);
    }

    proptest! {
        /// Whatever a request carries, nothing that HTTP/2 forbids reaches h2 — which
        /// would refuse the request as malformed — and everything else does, as it was.
        #[test]
        fn only_what_http2_forbids_is_left_out(
            names in prop::collection::vec(
                prop::sample::select(vec![
                    "Connection", "keep-alive", "Proxy-Connection", "transfer-encoding",
                    "Upgrade", "host", "HTTP2-Settings", "te", "TE", "content-length",
                    "accept", "x-custom", "cookie", "authorization",
                ]),
                0..12,
            ),
            te_is_trailers in any::<bool>(),
        ) {
            let mut sent = HeaderMap::new();
            sent.insert("host", HeaderValue::from_static("a.test"));
            for name in &names {
                let value = if name.eq_ignore_ascii_case("te") && te_is_trailers {
                    "trailers"
                } else {
                    "v"
                };
                sent.append(
                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_static(value),
                );
            }
            let sent_as = head(&Method::GET, &target("/"), &sent, Sending::None).unwrap();
            // A map's fields are shared, where other fields are read one by one: the same
            // head either way, order and all.
            let read = head(&Method::GET, &target("/"), &Lines(sent.clone()), Sending::None)
                .unwrap();
            prop_assert!(sent_as.headers().iter().eq(read.headers().iter()));
            for (name, value) in sent_as.headers() {
                prop_assert!(travels(name.as_str().as_bytes(), value.as_bytes()), "{name}");
            }
            let expected = sent
                .iter()
                .filter(|(name, value)| travels(name.as_str().as_bytes(), value.as_bytes()))
                .count();
            prop_assert_eq!(sent_as.headers().len(), expected);
        }
    }
}
