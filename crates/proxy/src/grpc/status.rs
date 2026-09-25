//! gRPC's status codes, and what a request must say to be a gRPC call
//! ([gRPC over HTTP/2], [status codes]).
//!
//! [gRPC over HTTP/2]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
//! [status codes]: https://github.com/grpc/grpc/blob/master/doc/statuscodes.md

use http::HeaderValue;

/// A gRPC status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "gRPC's whole list, as its specification gives it; the gateway itself says only some"
)]
pub(crate) enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    /// As `grpc-status` says it.
    pub(crate) fn value(self) -> HeaderValue {
        HeaderValue::from(self as u16)
    }
}

/// gRPC's names for its codes, by number: what a scrape labels them with.
pub(crate) const NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

/// The code a `grpc-status` value says, by number: one that is not one of gRPC's is
/// `UNKNOWN`, as a client reads it.
pub(crate) fn code_of(value: &[u8]) -> usize {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|code| *code < NAMES.len())
        .unwrap_or(Code::Unknown as usize)
}

/// Whether a request's `content-type` makes it a gRPC call: `application/grpc`, alone or
/// with a `+` suffix naming the message encoding, and with or without parameters. Not
/// gRPC-Web (`application/grpc-web…`), which is another protocol. Compared without regard
/// to case, as media types are.
pub(crate) fn is_grpc(content_type: &[u8]) -> bool {
    const GRPC: &[u8] = b"application/grpc";
    let Some(prefix) = content_type.get(..GRPC.len()) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case(GRPC) {
        return false;
    }
    matches!(
        content_type.get(GRPC.len()),
        None | Some(b'+' | b';' | b' ' | b'\t')
    )
}

/// A message for `grpc-message`: percent-encoded as gRPC asks, every byte outside the
/// printable ASCII range and `%` itself as `%XX`.
pub(crate) fn message(text: &str) -> HeaderValue {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        if (0x20..=0x7e).contains(&byte) && byte != b'%' {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    HeaderValue::from_str(&encoded).unwrap_or(HeaderValue::from_static(""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_grpc_call_is_told_by_its_content_type() {
        for grpc in [
            &b"application/grpc"[..],
            b"application/grpc+proto",
            b"application/grpc+json",
            b"Application/GRPC",
            b"application/grpc; charset=utf-8",
            b"application/grpc;x=y",
        ] {
            assert!(is_grpc(grpc), "{}", String::from_utf8_lossy(grpc));
        }
        for other in [
            &b"application/grpc-web"[..],
            b"application/grpc-web+proto",
            b"application/grpc-web-text",
            b"application/grpcx",
            b"application/json",
            b"application/grp",
            b"",
        ] {
            assert!(!is_grpc(other), "{}", String::from_utf8_lossy(other));
        }
    }

    #[test]
    fn a_status_value_is_read_as_its_code_and_nothing_else_as_one() {
        assert_eq!(code_of(b"0"), 0);
        assert_eq!(code_of(b"16"), 16);
        for other in [&b"17"[..], b"-1", b"", b"OK", b"1.0", b"\xff"] {
            assert_eq!(code_of(other), Code::Unknown as usize);
        }
        assert_eq!(NAMES[Code::DeadlineExceeded as usize], "DEADLINE_EXCEEDED");
        assert_eq!(NAMES[Code::Unauthenticated as usize], "UNAUTHENTICATED");
    }

    #[test]
    fn a_status_is_its_number() {
        assert_eq!(Code::Ok.value(), "0");
        assert_eq!(Code::Unimplemented.value(), "12");
        assert_eq!(Code::Unauthenticated.value(), "16");
    }

    #[test]
    fn a_message_is_percent_encoded() {
        assert_eq!(message("no route"), "no route");
        assert_eq!(message("100% gone"), "100%25 gone");
        assert_eq!(message("tab\there"), "tab%09here");
        assert_eq!(message("café"), "caf%C3%A9");
    }

    proptest! {
        /// Whatever the text, the message is a valid field value that decodes back to it.
        #[test]
        fn a_message_decodes_back_to_its_text(text in ".{0,40}") {
            let value = message(&text);
            let bytes = value.as_bytes();
            let mut decoded = Vec::new();
            let mut at = 0;
            while at < bytes.len() {
                if bytes[at] == b'%' {
                    let hex = std::str::from_utf8(&bytes[at + 1..at + 3]).unwrap();
                    decoded.push(u8::from_str_radix(hex, 16).unwrap());
                    at += 3;
                } else {
                    decoded.push(bytes[at]);
                    at += 1;
                }
            }
            prop_assert_eq!(decoded, text.as_bytes());
        }
    }
}
