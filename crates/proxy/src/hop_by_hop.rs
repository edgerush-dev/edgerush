//! Hop-by-hop headers: what is said about one connection and must not travel to the next
//! (RFC 9110 §7.6.1). Taken off a message before it is forwarded, in both directions:
//! `Connection` with every header it names, and `Keep-Alive`, `Proxy-Connection`,
//! `Transfer-Encoding` and `Upgrade` whether they are named or not. The HTTP engine frames
//! the message anew on the other side.
//!
//! `TE` is between this hop and the next as well, but a request that accepted trailers
//! goes on saying so — `TE: trailers` and nothing else — as it does with Envoy, HAProxy and
//! Go's proxy: trailers are passed through here, and gRPC servers insist on being told.
//! `Trailer` and the `Proxy-Auth*` headers stay; RFC 9110 no longer counts them.
//!
//! A client must not be able to have the gateway take away what the upstream relies on. A
//! request whose `Connection` names `Host` or an `X-Forwarded-*` header is rejected, as
//! Envoy and Pingora do, and so is one whose `Connection` is not a list of tokens.

use http::HeaderMap;
use http::header::{CONNECTION, HeaderName, HeaderValue, TE, TRANSFER_ENCODING, UPGRADE};

/// The headers that go whether `Connection` names them or not.
const HOP_BY_HOP: [HeaderName; 6] = [
    CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    TE,
    TRANSFER_ENCODING,
    UPGRADE,
];

/// Whether there is anything to check or to take off. Most messages say nothing about their
/// connection, and one pass over their headers is all they pay: looking each of the names
/// up costs several times as much. Without `Connection` no other header is named either.
pub(crate) fn is_present(headers: &HeaderMap) -> bool {
    headers.keys().any(is_hop_by_hop)
}

/// Whether the header is one of [`HOP_BY_HOP`]. Header names are held in lower case, and
/// matching their text (the length first) is cheaper than comparing names one by one.
fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection" | "keep-alive" | "proxy-connection" | "te" | "transfer-encoding" | "upgrade"
    )
}

/// Checks what a request's `Connection` names, before anything is taken off on its word.
pub(crate) fn check_connection(headers: &HeaderMap) -> Result<(), ConnectionError> {
    for value in headers.get_all(CONNECTION) {
        for option in options(value) {
            if !option.iter().copied().all(is_token_byte) {
                return Err(ConnectionError::Malformed);
            }
            if is_protected(option) {
                return Err(ConnectionError::Protected);
            }
        }
    }
    Ok(())
}

/// Takes the hop-by-hop headers off a request; `TE: trailers` stays if it was said.
/// Allocates only for a request with more than one `Connection` field.
pub(crate) fn strip_request(headers: &mut HeaderMap) {
    let accepts_trailers = headers
        .get_all(TE)
        .iter()
        .flat_map(options)
        // A `TE` option may carry a weight (`trailers;q=1`), and means the same with it.
        .filter_map(|option| option.split(|byte| *byte == b';').next())
        .any(|name| name.trim_ascii().eq_ignore_ascii_case(b"trailers"));
    strip(headers);
    if accepts_trailers {
        headers.insert(TE, HeaderValue::from_static("trailers"));
    }
}

/// Takes the hop-by-hop headers off a response.
pub(crate) fn strip_response(headers: &mut HeaderMap) {
    if is_present(headers) {
        strip(headers);
    }
}

fn strip(headers: &mut HeaderMap) {
    // The values are shared, not copied; the map cannot be read while it is changed.
    let mut fields = headers.get_all(CONNECTION).iter();
    if let Some(first) = fields.next().cloned() {
        let others: Vec<HeaderValue> = fields.cloned().collect();
        for value in std::iter::once(&first).chain(&others) {
            for option in options(value) {
                // An option that is no header name names no header.
                if let Ok(name) = str::from_utf8(option) {
                    headers.remove(name);
                }
            }
        }
    }
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
}

/// The members of a comma-separated list, without the white space around them and without
/// the empty ones that the list syntax allows.
fn options(value: &HeaderValue) -> impl Iterator<Item = &[u8]> {
    value
        .as_bytes()
        .split(|byte| *byte == b',')
        .map(<[u8]>::trim_ascii)
        .filter(|option| !option.is_empty())
}

/// RFC 9110's `tchar`: what connection options and header names are made of.
fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn is_protected(option: &[u8]) -> bool {
    const FORWARDED: &[u8] = b"x-forwarded-";
    option.eq_ignore_ascii_case(b"host")
        || option
            .get(..FORWARDED.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(FORWARDED))
}

/// Why a request's `Connection` header is not acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionError {
    /// A connection option that is not a token.
    #[error("Connection header is not a list of tokens")]
    Malformed,
    /// A connection option that names `Host` or an `X-Forwarded-*` header.
    #[error("Connection header names a header that must reach the upstream")]
    Protected,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(fields: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(value.as_bytes()).unwrap(),
            );
        }
        headers
    }

    /// The fields that are left, sorted.
    fn left(headers: &HeaderMap) -> Vec<(&str, &str)> {
        let mut fields: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.to_str().unwrap()))
            .collect();
        fields.sort_unstable();
        fields
    }

    fn request_without_hop_by_hop(fields: &[(&str, &str)]) -> HeaderMap {
        let mut headers = headers(fields);
        strip_request(&mut headers);
        headers
    }

    #[test]
    fn headers_about_the_connection_are_taken_off() {
        let stripped = request_without_hop_by_hop(&[
            ("connection", "keep-alive"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("te", "gzip"),
            ("accept", "*/*"),
        ]);
        assert_eq!(left(&stripped), [("accept", "*/*")]);
    }

    #[test]
    fn they_are_taken_off_whether_connection_names_them_or_not() {
        let stripped = request_without_hop_by_hop(&[("upgrade", "h2c"), ("keep-alive", "x")]);
        assert_eq!(left(&stripped), []);
    }

    #[test]
    fn what_connection_names_is_taken_off_with_all_its_values() {
        let stripped = request_without_hop_by_hop(&[
            ("connection", "X-Hop, close"),
            ("connection", " ,x-other ,"),
            ("x-hop", "1"),
            ("X-Hop", "2"),
            ("x-other", "3"),
            ("x-kept", "4"),
        ]);
        assert_eq!(left(&stripped), [("x-kept", "4")]);
    }

    #[test]
    fn a_message_is_asked_once_whether_it_says_anything_about_its_connection() {
        for name in [
            "connection",
            "Keep-Alive",
            "proxy-connection",
            "te",
            "transfer-encoding",
            "upgrade",
        ] {
            assert!(
                is_present(&headers(&[("accept", "*/*"), (name, "x")])),
                "{name}"
            );
        }
        assert!(!is_present(&headers(&[
            ("accept", "*/*"),
            ("trailer", "x"),
            ("host", "x")
        ])));
        assert!(!is_present(&HeaderMap::new()));
    }

    #[test]
    fn the_two_ways_of_saying_which_headers_go_agree() {
        for name in HOP_BY_HOP {
            assert!(is_hop_by_hop(&name), "{name}");
        }
        assert_eq!(HOP_BY_HOP.len(), 6);
        for name in ["host", "trailer", "tea", "connections", "x-upgrade"] {
            assert!(!is_hop_by_hop(&HeaderName::from_static(name)), "{name}");
        }
    }

    #[test]
    fn headers_that_are_end_to_end_stay() {
        let end_to_end = [
            ("authorization", "Bearer x"),
            ("content-length", "12"),
            ("content-type", "text/plain"),
            ("host", "example.com"),
            ("proxy-authorization", "Basic x"),
            ("trailer", "x-checksum"),
            ("x-forwarded-for", "192.0.2.1"),
        ];
        assert_eq!(left(&request_without_hop_by_hop(&end_to_end)), end_to_end);
    }

    #[test]
    fn a_request_that_accepts_trailers_goes_on_saying_so_and_nothing_else() {
        for te in [
            "trailers",
            "Trailers",
            "gzip, trailers",
            "trailers;q=0.5",
            "deflate,TRAILERS ;q=1",
        ] {
            let stripped = request_without_hop_by_hop(&[("connection", "TE"), ("te", te)]);
            assert_eq!(left(&stripped), [("te", "trailers")], "{te}");
        }
        let twice = request_without_hop_by_hop(&[("te", "gzip"), ("te", "trailers")]);
        assert_eq!(left(&twice), [("te", "trailers")]);

        for te in ["gzip", "trailersx", "x-trailers", ""] {
            let stripped = request_without_hop_by_hop(&[("te", te)]);
            assert_eq!(left(&stripped), [], "{te}");
        }
    }

    #[test]
    fn a_response_loses_the_same_headers_and_te_altogether() {
        let mut response = headers(&[
            ("connection", "close, x-hop"),
            ("keep-alive", "timeout=5"),
            ("transfer-encoding", "chunked"),
            ("te", "trailers"),
            ("x-hop", "1"),
            ("content-type", "text/plain"),
            ("trailer", "x-checksum"),
        ]);
        strip_response(&mut response);
        assert_eq!(
            left(&response),
            [("content-type", "text/plain"), ("trailer", "x-checksum")]
        );
    }

    #[test]
    fn options_that_are_no_header_names_name_nothing() {
        let mut response = headers(&[("connection", "x y, \"quoted\", caf\u{e9}"), ("x", "1")]);
        strip_response(&mut response);
        assert_eq!(left(&response), [("x", "1")]);
    }

    #[test]
    fn an_ordinary_connection_header_is_acted_on() {
        for connection in [
            "close",
            "keep-alive",
            "Upgrade, HTTP2-Settings",
            "TE, x-hop",
            " , ",
            "",
        ] {
            let headers = headers(&[("connection", connection)]);
            assert_eq!(check_connection(&headers), Ok(()), "{connection:?}");
        }
        assert_eq!(check_connection(&HeaderMap::new()), Ok(()));
    }

    #[test]
    fn a_connection_header_that_names_what_the_upstream_relies_on_is_not() {
        for connection in [
            "host",
            "close, Host",
            "x-forwarded-for",
            "X-Forwarded-Host",
            "x-forwarded-",
        ] {
            let headers = headers(&[("connection", connection)]);
            assert_eq!(
                check_connection(&headers),
                Err(ConnectionError::Protected),
                "{connection}"
            );
        }
        let second = headers(&[("connection", "close"), ("connection", "x-forwarded-proto")]);
        assert_eq!(check_connection(&second), Err(ConnectionError::Protected));

        // Near misses name other headers.
        for connection in ["hosts", "x-forwarded", "forwarded"] {
            let headers = headers(&[("connection", connection)]);
            assert_eq!(check_connection(&headers), Ok(()), "{connection}");
        }
    }

    #[test]
    fn a_connection_header_that_is_not_a_list_of_tokens_is_not_acted_on() {
        for connection in [
            ":authority",
            "x y",
            "close; q=1",
            "\"close\"",
            "caf\u{e9}",
            "a=b",
        ] {
            let headers = headers(&[("connection", connection)]);
            assert_eq!(
                check_connection(&headers),
                Err(ConnectionError::Malformed),
                "{connection}"
            );
        }
    }
}
