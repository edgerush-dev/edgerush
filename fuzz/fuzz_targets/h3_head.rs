//! Fuzzes the reading of an HTTP/3 request head from the fields quiche decoded: any names
//! and values at all, in any order.
//!
//! A head taken must be one an HTTP/1 upstream reads as the client meant it: no field only
//! a connection has, lower-case token names, no control character in a value and no
//! whitespace at either end of one, a `Host` that is the `:authority` where both are said,
//! and one length at most, however it is spelt (`06` is 6). Anything else is refused, never passed on and never a panic.
//!
//! The input is a list of fields, each a byte of name length, the name, a byte of value
//! length and the value: `cargo fuzz run h3_head corpus/h3_head seeds/h3_head`.

#![no_main]

use edgerush_proxy::downstream::h3::head::request;
use libfuzzer_sys::fuzz_target;
use quiche::h3::Header;

fuzz_target!(|bytes: &[u8]| {
    let mut fields = Vec::new();
    let mut rest = bytes;
    while let Some((&name_len, after)) = rest.split_first() {
        let Some((name, after)) = after.split_at_checked(usize::from(name_len)) else {
            break;
        };
        let Some((&value_len, after)) = after.split_first() else {
            break;
        };
        let Some((value, after)) = after.split_at_checked(usize::from(value_len)) else {
            break;
        };
        fields.push(Header::new(name, value));
        rest = after;
    }
    let Ok(head) = request(&fields, 64 << 10) else {
        return;
    };
    for (name, value) in &head.parts.headers {
        let name = name.as_str();
        assert!(!matches!(
            name,
            "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
        ));
        assert!(!name.bytes().any(|byte| byte.is_ascii_uppercase()));
        assert!(
            !value
                .as_bytes()
                .iter()
                .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n'))
        );
        let blank = |byte: &u8| *byte == b' ' || *byte == b'\t';
        assert!(!value.as_bytes().first().is_some_and(blank));
        assert!(!value.as_bytes().last().is_some_and(blank));
        if name == "te" {
            assert!(value.as_bytes().eq_ignore_ascii_case(b"trailers"));
        }
    }
    if let (Some(authority), Some(host)) =
        (head.parts.uri.authority(), head.parts.headers.get("host"))
    {
        assert_eq!(authority.as_str().as_bytes(), host.as_bytes());
    }
    let lengths: Vec<_> = head
        .parts
        .headers
        .get_all("content-length")
        .iter()
        .collect();
    if let Some(length) = head.length {
        assert!(
            lengths.iter().all(
                |value| value.to_str().ok().and_then(|value| value.parse().ok()) == Some(length)
            )
        );
    } else {
        assert!(lengths.is_empty());
    }
});
