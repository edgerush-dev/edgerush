//! Heads between HTTP/3's field sections and the request core's `http` types
//! ([16 §5](../../../../../docs/16-http3.md)).
//!
//! quiche decodes a field section and checks its size, and nothing else: which fields a
//! request may carry, and in what form, is this module's. A request is refused as malformed
//! where RFC 9114 §4.1.2 says so — a stream error of `H3_MESSAGE_ERROR` — and never passed
//! on in a form an HTTP/1 upstream could read differently: fields in upper case, fields
//! only a connection has (`Connection`, `Transfer-Encoding` and the like), a `Host` that
//! names another authority than `:authority`, a `Content-Length` that is not one number.
//! One whose fields are merely too many is answered 431, as over HTTP/1 and HTTP/2.

use http::header::{CONTENT_LENGTH, HOST, TE};
use http::uri::{Authority, PathAndQuery, Scheme, Uri};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Version};
use quiche::h3::{HeaderRef, NameValue};

/// What RFC 9114 §4.2.2 adds to each field's name and value in measuring a section.
const FIELD_OVERHEAD: usize = 32;

/// Why a head was not taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// Against RFC 9114 §4: the stream is reset with `H3_MESSAGE_ERROR`. What it broke is
    /// said, for whoever reads the log.
    Malformed(&'static str),
    /// Well formed, but its fields past the head limit: answered 431.
    TooLarge,
}

/// A request's head, and the length its `Content-Length` declares, which its body is held
/// to (RFC 9114 §4.1.2).
#[derive(Debug)]
pub struct RequestHead {
    /// The head, as the core takes it.
    pub parts: http::request::Parts,
    /// What `Content-Length` declares, if anything.
    pub length: Option<u64>,
}

/// How many fields the request core adds to a request's head (03 §11 in the docs).
const ADDED_BY_THE_CORE: usize = 5;

/// The request `fields` make, measured against `limit` by RFC 9114 §4.2.2's measure.
pub fn request<F: NameValue>(fields: &[F], limit: usize) -> Result<RequestHead, Refused> {
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    // Room for what the request core adds as well, so that the map does not grow to take
    // it: `Host` from `:authority`, `X-Forwarded-For`, `-Proto` and `-Host`, and `Via`.
    let mut headers = HeaderMap::with_capacity(fields.len() + ADDED_BY_THE_CORE);
    let mut size = 0_usize;
    let mut regular = false;
    for field in fields {
        let (name, value) = (field.name(), field.value());
        size = size.saturating_add(name.len() + value.len() + FIELD_OVERHEAD);
        if let Some(pseudo) = name.strip_prefix(b":") {
            // RFC 9114 §4.3: every pseudo-header before the first regular field, each once.
            if regular {
                return Err(Refused::Malformed("a pseudo-header after a regular field"));
            }
            let slot = match pseudo {
                b"method" => &mut method,
                b"scheme" => &mut scheme,
                b"authority" => &mut authority,
                b"path" => &mut path,
                // `:protocol` is extended CONNECT's, which is not announced (RFC 9220 §3).
                _ => {
                    return Err(Refused::Malformed(
                        "a pseudo-header a request does not have",
                    ));
                }
            };
            if slot.replace(value).is_some() {
                return Err(Refused::Malformed("a pseudo-header twice"));
            }
            continue;
        }
        regular = true;
        let (name, value) = regular_field(name, value)?;
        if name == TE && !value.as_bytes().eq_ignore_ascii_case(b"trailers") {
            return Err(Refused::Malformed("`TE` other than `trailers`"));
        }
        headers.append(name, value);
    }
    if size > limit {
        return Err(Refused::TooLarge);
    }

    let method = Method::from_bytes(method.ok_or(Refused::Malformed("no `:method`"))?)
        .map_err(|_| Refused::Malformed("a `:method` that is not a token"))?;
    let authority = authority
        .map(|authority| {
            (!authority.is_empty())
                .then(|| Authority::try_from(authority).ok())
                .flatten()
                .ok_or(Refused::Malformed(
                    "an `:authority` that is not an authority",
                ))
        })
        .transpose()?;
    let uri = if method == Method::CONNECT {
        // RFC 9114 §4.4: the authority to connect to, and neither a scheme nor a path.
        if scheme.is_some() || path.is_some() {
            return Err(Refused::Malformed("CONNECT with `:scheme` or `:path`"));
        }
        let authority = authority.ok_or(Refused::Malformed("CONNECT without `:authority`"))?;
        Uri::from(authority)
    } else {
        let scheme = Scheme::try_from(scheme.ok_or(Refused::Malformed("no `:scheme`"))?)
            .map_err(|_| Refused::Malformed("a `:scheme` that is not a scheme"))?;
        let path = path.ok_or(Refused::Malformed("no `:path`"))?;
        // RFC 9114 §4.3.1: never empty; `*` only for OPTIONS, anything else a path from `/`.
        let asterisk = path == b"*";
        if !(path.starts_with(b"/") || asterisk && method == Method::OPTIONS) {
            return Err(Refused::Malformed("a `:path` that is not a path"));
        }
        let path = PathAndQuery::try_from(path)
            .map_err(|_| Refused::Malformed("a `:path` that is not a path"))?;
        // A scheme with an authority needs one, said by `:authority` or `Host`, and both
        // the same where both are said.
        let hosted = scheme == Scheme::HTTP || scheme == Scheme::HTTPS;
        let mut hosts = headers.get_all(HOST).iter();
        let host = hosts.next();
        if hosts.next().is_some() {
            return Err(Refused::Malformed("`Host` twice"));
        }
        match (&authority, host) {
            (Some(authority), Some(host)) if authority.as_str().as_bytes() != host.as_bytes() => {
                return Err(Refused::Malformed("a `Host` other than `:authority`"));
            }
            (None, None) if hosted => {
                return Err(Refused::Malformed("neither `:authority` nor `Host`"));
            }
            (None, Some(host)) if host.is_empty() => {
                return Err(Refused::Malformed("an empty `Host`"));
            }
            _ => {}
        }
        let mut uri = http::uri::Parts::default();
        if let Some(authority) = authority {
            uri.scheme = Some(scheme);
            uri.authority = Some(authority);
        }
        uri.path_and_query = Some(path);
        Uri::from_parts(uri).map_err(|_| Refused::Malformed("a target that is not a URI"))?
    };
    let length = content_length(&headers)?;

    let (mut parts, ()) = http::Request::new(()).into_parts();
    parts.method = method;
    parts.uri = uri;
    parts.version = Version::HTTP_3;
    parts.headers = headers;
    Ok(RequestHead { parts, length })
}

/// The trailers `fields` make: regular fields only, held to what a request's are.
pub fn trailers<F: NameValue>(fields: &[F], limit: usize) -> Result<HeaderMap, Refused> {
    let mut trailers = HeaderMap::with_capacity(fields.len());
    let mut size = 0_usize;
    for field in fields {
        let (name, value) = (field.name(), field.value());
        size = size.saturating_add(name.len() + value.len() + FIELD_OVERHEAD);
        if name.starts_with(b":") {
            return Err(Refused::Malformed("a pseudo-header in trailers"));
        }
        let (name, value) = regular_field(name, value)?;
        trailers.append(name, value);
    }
    if size > limit {
        return Err(Refused::TooLarge);
    }
    Ok(trailers)
}

/// A regular field, as RFC 9114 §4.2 allows it: a lower-case token for its name, none of
/// the fields only a connection has, and a value with no control character but a tab.
fn regular_field(name: &[u8], value: &[u8]) -> Result<(HeaderName, HeaderValue), Refused> {
    if name.is_empty() || !name.iter().all(|&byte| is_lower_token_byte(byte)) {
        return Err(Refused::Malformed(
            "a field name that is not a lower-case token",
        ));
    }
    if is_connection_specific(name) {
        return Err(Refused::Malformed("a field only a connection has"));
    }
    let name = HeaderName::from_bytes(name)
        .map_err(|_| Refused::Malformed("a field name that is not a lower-case token"))?;
    // RFC 9110 §5.5 and RFC 9114 §10.3: no NUL, CR or LF, and no other control character
    // either, which `HeaderValue` refuses.
    let value = HeaderValue::from_bytes(value)
        .map_err(|_| Refused::Malformed("a field value with a control character"))?;
    Ok((name, value))
}

/// RFC 9110's `tchar`, without the upper-case letters RFC 9114 §4.2 forbids.
fn is_lower_token_byte(byte: u8) -> bool {
    crate::hop_by_hop::is_token_byte(byte) && !byte.is_ascii_uppercase()
}

/// The fields RFC 9114 §4.2 names as a connection's, which HTTP/3 carries in none of its
/// messages. `TE` is not among them: a request may say `trailers` with it.
fn is_connection_specific(name: &[u8]) -> bool {
    matches!(
        name,
        b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding" | b"upgrade"
    )
}

/// The one length `Content-Length` declares, however many times it is said; a malformed
/// request if it declares none or two.
fn content_length(headers: &HeaderMap) -> Result<Option<u64>, Refused> {
    let mut declared = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        let bytes = value.as_bytes();
        let length = (!bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit))
            .then(|| std::str::from_utf8(bytes).ok()?.parse::<u64>().ok())
            .flatten()
            .ok_or(Refused::Malformed(
                "a `Content-Length` that is not a length",
            ))?;
        if declared
            .replace(length)
            .is_some_and(|before| before != length)
        {
            return Err(Refused::Malformed("two lengths in `Content-Length`"));
        }
    }
    Ok(declared)
}

/// An answer's head as the fields HTTP/3 sends: `:status`, then every field but those only
/// a connection has and `TE`, borrowed from `headers` as they are.
pub(crate) fn answer<'a>(status: &'a StatusCode, headers: &'a HeaderMap) -> Vec<HeaderRef<'a>> {
    let mut fields = Vec::with_capacity(headers.len() + 1);
    fields.push(HeaderRef::new(b":status", status.as_str().as_bytes()));
    fields.extend(
        headers
            .iter()
            .filter(|(name, _)| {
                let name = name.as_str().as_bytes();
                !is_connection_specific(name) && name != b"te"
            })
            .map(|(name, value)| HeaderRef::new(name.as_str().as_bytes(), value.as_bytes())),
    );
    fields
}

/// Trailers as the fields HTTP/3 sends: every field but those only a connection has.
pub(crate) fn trailer_fields(trailers: &HeaderMap) -> Vec<HeaderRef<'_>> {
    trailers
        .iter()
        .filter(|(name, _)| !is_connection_specific(name.as_str().as_bytes()))
        .map(|(name, value)| HeaderRef::new(name.as_str().as_bytes(), value.as_bytes()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use quiche::h3::Header;

    const LIMIT: usize = 64 << 10;

    fn fields(pairs: &[(&str, &str)]) -> Vec<Header> {
        pairs
            .iter()
            .map(|(name, value)| Header::new(name.as_bytes(), value.as_bytes()))
            .collect()
    }

    fn get(more: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
        let mut pairs = vec![
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.test"),
            (":path", "/a?b=c"),
        ];
        pairs.extend_from_slice(more);
        pairs
    }

    fn refused(pairs: &[(&str, &str)]) -> &'static str {
        match request(&fields(pairs), LIMIT) {
            Err(Refused::Malformed(why)) => why,
            other => panic!("{pairs:?} was not refused as malformed: {other:?}"),
        }
    }

    #[test]
    fn a_request_is_its_pseudo_headers_and_fields() {
        let head = request(
            &fields(&get(&[
                ("accept", "*/*"),
                ("cookie", "a=1"),
                ("cookie", "b=2"),
            ])),
            LIMIT,
        )
        .unwrap();
        assert_eq!(head.parts.method, Method::GET);
        assert_eq!(head.parts.uri, "https://example.test/a?b=c");
        assert_eq!(head.parts.version, Version::HTTP_3);
        assert_eq!(head.parts.headers["accept"], "*/*");
        // Cookies may come as crumbs (RFC 9114 §4.2.1); the core joins them.
        assert_eq!(head.parts.headers.get_all("cookie").iter().count(), 2);
        assert_eq!(head.length, None);
    }

    #[test]
    fn a_host_alone_names_the_authority_and_must_agree_with_one_said() {
        let head = request(
            &fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/"),
                ("host", "h.test"),
            ]),
            LIMIT,
        )
        .unwrap();
        assert_eq!(head.parts.uri, "/");
        assert_eq!(head.parts.headers["host"], "h.test");
        request(&fields(&get(&[("host", "example.test")])), LIMIT).unwrap();
        assert_eq!(
            refused(&get(&[("host", "other.test")])),
            "a `Host` other than `:authority`"
        );
        assert_eq!(
            refused(&[(":method", "GET"), (":scheme", "https"), (":path", "/")]),
            "neither `:authority` nor `Host`"
        );
    }

    #[test]
    fn every_malformed_request_of_rfc_9114_section_4_is_refused() {
        let cases: &[(&[(&str, &str)], &str)] = &[
            (
                &[(":scheme", "https"), (":authority", "a"), (":path", "/")],
                "no `:method`",
            ),
            (
                &[(":method", "GET"), (":authority", "a"), (":path", "/")],
                "no `:scheme`",
            ),
            (
                &[
                    (":method", "GET"),
                    (":scheme", "https"),
                    (":authority", "a"),
                ],
                "no `:path`",
            ),
            (
                &[
                    (":method", "GET"),
                    (":scheme", "https"),
                    (":authority", "a"),
                    (":path", ""),
                ],
                "a `:path` that is not a path",
            ),
            (
                &[
                    (":method", "GET"),
                    (":scheme", "https"),
                    (":authority", "a"),
                    (":path", "*"),
                ],
                "a `:path` that is not a path",
            ),
            (
                &[
                    (":method", "GET"),
                    (":scheme", "https"),
                    (":authority", "a"),
                    (":path", "x"),
                ],
                "a `:path` that is not a path",
            ),
            (
                &[
                    (":method", "GET"),
                    (":method", "GET"),
                    (":scheme", "https"),
                    (":path", "/"),
                ],
                "a pseudo-header twice",
            ),
            (
                &[
                    (":method", "GET"),
                    ("accept", "*/*"),
                    (":scheme", "https"),
                    (":path", "/"),
                ],
                "a pseudo-header after a regular field",
            ),
            (
                &[
                    (":method", "GET"),
                    (":protocol", "websocket"),
                    (":path", "/"),
                ],
                "a pseudo-header a request does not have",
            ),
            (
                &[
                    (":method", "GET"),
                    (":scheme", "https"),
                    (":path", "/"),
                    (":status", "200"),
                ],
                "a pseudo-header a request does not have",
            ),
            (
                &[
                    (":method", "CONNECT"),
                    (":authority", "a:443"),
                    (":path", "/"),
                ],
                "CONNECT with `:scheme` or `:path`",
            ),
            (&[(":method", "CONNECT")], "CONNECT without `:authority`"),
            (
                &[
                    (":method", "G T"),
                    (":scheme", "https"),
                    (":authority", "a"),
                    (":path", "/"),
                ],
                "a `:method` that is not a token",
            ),
        ];
        for (pairs, why) in cases {
            assert_eq!(refused(pairs), *why, "{pairs:?}");
        }
        assert_eq!(
            refused(&get(&[("Accept", "*/*")])),
            "a field name that is not a lower-case token"
        );
        assert_eq!(
            refused(&get(&[("", "x")])),
            "a field name that is not a lower-case token"
        );
        assert_eq!(
            refused(&get(&[("a b", "x")])),
            "a field name that is not a lower-case token"
        );
        for name in [
            "connection",
            "keep-alive",
            "proxy-connection",
            "transfer-encoding",
            "upgrade",
        ] {
            assert_eq!(
                refused(&get(&[(name, "x")])),
                "a field only a connection has",
                "{name}"
            );
        }
        assert_eq!(
            refused(&get(&[("te", "gzip")])),
            "`TE` other than `trailers`"
        );
        request(&fields(&get(&[("te", "trailers")])), LIMIT).unwrap();
        for value in ["a\rb", "a\nb", "a\0b", "a\x07b"] {
            assert_eq!(
                refused(&get(&[("x-v", value)])),
                "a field value with a control character",
                "{value:?}"
            );
        }
        assert_eq!(
            refused(&get(&[("host", "a"), ("host", "a")])),
            "`Host` twice"
        );
    }

    #[test]
    fn a_content_length_is_one_number_however_often_it_is_said() {
        let length = |values: &[&'static str]| {
            let pairs: Vec<_> = values
                .iter()
                .map(|value| ("content-length", *value))
                .collect();
            request(&fields(&get(&pairs)), LIMIT).map(|head| head.length)
        };
        assert_eq!(length(&[]), Ok(None));
        assert_eq!(length(&["12"]), Ok(Some(12)));
        assert_eq!(length(&["12", "12"]), Ok(Some(12)));
        // Leading zeros are digits too (RFC 9110 §8.6), and the same length spelt twice is
        // one length: found by the fuzz target.
        assert_eq!(length(&["06"]), Ok(Some(6)));
        assert_eq!(length(&["6", "06"]), Ok(Some(6)));
        assert_eq!(
            length(&["12", "13"]),
            Err(Refused::Malformed("two lengths in `Content-Length`"))
        );
        for bad in ["", "-1", "+1", "1 2", "0x10", "99999999999999999999"] {
            assert_eq!(
                length(&[bad]),
                Err(Refused::Malformed(
                    "a `Content-Length` that is not a length"
                )),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn options_may_ask_about_the_server_as_a_whole() {
        let head = request(
            &fields(&[
                (":method", "OPTIONS"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "*"),
            ]),
            LIMIT,
        )
        .unwrap();
        assert_eq!(head.parts.uri.path(), "*");
    }

    #[test]
    fn connect_names_only_its_authority() {
        let head = request(
            &fields(&[(":method", "CONNECT"), (":authority", "a.test:443")]),
            LIMIT,
        )
        .unwrap();
        assert_eq!(head.parts.method, Method::CONNECT);
        assert_eq!(head.parts.uri.authority().unwrap(), "a.test:443");
    }

    /// Measured as RFC 9114 §4.2.2 measures it: every field's name and value, and 32 more.
    #[test]
    fn fields_past_the_limit_are_too_large_and_up_to_it_are_not() {
        let base: usize = get(&[]).iter().map(|(n, v)| n.len() + v.len() + 32).sum();
        let fill = |value_len: usize| {
            let value = "v".repeat(value_len);
            let mut pairs: Vec<(&str, &str)> = get(&[]);
            pairs.push(("x-fill", &value));
            request(&fields(&pairs), LIMIT).map(|_| ())
        };
        let room = LIMIT - base - "x-fill".len() - 32;
        assert_eq!(fill(room), Ok(()));
        assert_eq!(fill(room + 1), Err(Refused::TooLarge));
    }

    #[test]
    fn trailers_are_regular_fields_only() {
        let trailers = trailers(&fields(&[("grpc-status", "0"), ("x-sum", "1")]), LIMIT).unwrap();
        assert_eq!(trailers["grpc-status"], "0");
        assert_eq!(
            super::trailers(&fields(&[(":status", "200")]), LIMIT),
            Err(Refused::Malformed("a pseudo-header in trailers"))
        );
        assert_eq!(
            super::trailers(&fields(&[("Grpc-Status", "0")]), LIMIT),
            Err(Refused::Malformed(
                "a field name that is not a lower-case token"
            ))
        );
        assert_eq!(
            super::trailers(&fields(&[("transfer-encoding", "chunked")]), LIMIT),
            Err(Refused::Malformed("a field only a connection has"))
        );
    }

    #[test]
    fn an_answer_goes_out_as_its_status_and_every_field_a_connection_does_not_own() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("transfer-encoding", "chunked".parse().unwrap());
        headers.insert("keep-alive", "5".parse().unwrap());
        headers.insert("te", "trailers".parse().unwrap());
        headers.append("set-cookie", "a=1".parse().unwrap());
        headers.append("set-cookie", "b=2".parse().unwrap());
        let status = StatusCode::NOT_FOUND;
        let sent: Vec<(Vec<u8>, Vec<u8>)> = answer(&status, &headers)
            .iter()
            .map(|field| (field.name().to_vec(), field.value().to_vec()))
            .collect();
        let expected: Vec<(Vec<u8>, Vec<u8>)> = [
            (":status", "404"),
            ("content-type", "text/plain"),
            ("set-cookie", "a=1"),
            ("set-cookie", "b=2"),
        ]
        .iter()
        .map(|(name, value)| (name.as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect();
        assert_eq!(sent, expected);
    }

    fn name() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9-]{0,12}".prop_filter("not a connection's", |name| {
            !is_connection_specific(name.as_bytes())
                && name != "te"
                && name != "host"
                && name != "content-length"
        })
    }

    proptest! {
        /// Any request well formed by these rules comes out as the fields it went in as.
        #[test]
        fn a_well_formed_request_keeps_its_fields(
            method in "(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|[A-Z]{1,8})",
            authority in "[a-z]{1,10}(\\.[a-z]{1,5}){0,2}(:[0-9]{1,4})?",
            path in "/[a-zA-Z0-9/._~-]{0,20}(\\?[a-z0-9=&]{0,10})?",
            regular in prop::collection::vec((name(), "[ -~]{0,20}"), 0..8),
        ) {
            let mut pairs: Vec<(&str, &str)> = vec![
                (":method", &method),
                (":scheme", "https"),
                (":authority", &authority),
                (":path", &path),
            ];
            pairs.extend(regular.iter().map(|(name, value)| (name.as_str(), value.as_str())));
            let head = request(&fields(&pairs), LIMIT).unwrap();
            prop_assert_eq!(head.parts.method.as_str(), method.as_str());
            prop_assert_eq!(head.parts.uri.authority().unwrap().as_str(), authority.as_str());
            prop_assert_eq!(head.parts.uri.path_and_query().unwrap().as_str(), path.as_str());
            let kept: Vec<(String, Vec<u8>)> = head
                .parts
                .headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
                .collect();
            let mut sent: Vec<(String, Vec<u8>)> = regular
                .iter()
                .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()))
                .collect();
            // A header map keeps a name's values together, in the order they came.
            let mut sorted_kept = kept.clone();
            sorted_kept.sort();
            sent.sort();
            prop_assert_eq!(sorted_kept, sent);
        }

        /// Whatever the fields, a head is refused or taken, never a panic, and one taken
        /// has none of what RFC 9114 §4.2 forbids.
        #[test]
        fn any_fields_are_refused_or_taken_clean(
            pairs in prop::collection::vec(("[:a-zA-Z -]{0,10}", "[\\x00-\\x7f]{0,10}"), 0..10),
        ) {
            let pairs: Vec<(&str, &str)> = pairs.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
            if let Ok(head) = request(&fields(&pairs), LIMIT) {
                for (name, value) in &head.parts.headers {
                    prop_assert!(!is_connection_specific(name.as_str().as_bytes()));
                    prop_assert!(!value.as_bytes().iter().any(|b| matches!(b, b'\0' | b'\r' | b'\n')));
                }
            }
        }
    }
}
