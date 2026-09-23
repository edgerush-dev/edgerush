//! Connection-bound authentication and the upstream pool's isolation boundary.
//!
//! This does not implement a handshake. It recognizes the schemes whose credentials
//! must never leave a connection available to a different downstream client.

use edgerush_router::Fields;
use http::StatusCode;
use http::header::{AUTHORIZATION, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, WWW_AUTHENTICATE};

/// Read the outgoing head, after filters: proxy credentials from the client were
/// stripped, but a rule can add them, or replace or remove origin credentials.
pub(crate) fn carries_credentials<F: Fields + ?Sized>(headers: &F) -> bool {
    [AUTHORIZATION, PROXY_AUTHORIZATION]
        .iter()
        .any(|name| headers.values(name).any(uses_scheme))
}

/// A challenge is an extra reason for the custom path not to pool a connection. It
/// does not itself authenticate a client; the shared guarantee is on sent credentials.
pub fn challenges<F: Fields + ?Sized>(status: StatusCode, headers: &F) -> bool {
    let name = match status {
        StatusCode::UNAUTHORIZED => WWW_AUTHENTICATE,
        StatusCode::PROXY_AUTHENTICATION_REQUIRED => PROXY_AUTHENTICATE,
        _ => return false,
    };
    headers.values(&name).any(has_challenge)
}

fn ows(bytes: &[u8]) -> &[u8] {
    bytes.trim_ascii_start().trim_ascii_end()
}

fn uses_scheme(value: &[u8]) -> bool {
    let value = ows(value);
    let end = value
        .iter()
        .position(|byte| *byte == b' ' || *byte == b'\t')
        .unwrap_or(value.len());
    let scheme = &value[..end];
    // A comma-separated challenge list also contains auth-params. A parameter named
    // NTLM or Negotiate is not a challenge, including when whitespace precedes '='.
    (scheme.eq_ignore_ascii_case(b"ntlm") || scheme.eq_ignore_ascii_case(b"negotiate"))
        && !ows(&value[end..]).starts_with(b"=")
}

fn has_challenge(value: &[u8]) -> bool {
    let mut from = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (at, byte) in value.iter().copied().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && byte == b',' {
            if uses_scheme(&value[from..at]) {
                return true;
            }
            from = at + 1;
        }
    }
    uses_scheme(&value[from..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn connection_bound_credentials_match_schemes_not_substrings() {
        for name in [AUTHORIZATION, PROXY_AUTHORIZATION] {
            for value in [
                "NTLM token",
                "nEgOtIaTe ticket",
                "NTLM",
                " Negotiate\tvalue ",
            ] {
                let mut headers = HeaderMap::new();
                headers.append(name.clone(), HeaderValue::from_static("Basic other"));
                headers.append(name.clone(), HeaderValue::from_str(value).unwrap());
                assert!(carries_credentials(&headers), "{name}: {value}");
            }
            for value in [
                "Basic NTLM",
                "Bearer Negotiate",
                "NTLM-extra token",
                "",
                "NTLM=param",
            ] {
                let mut headers = HeaderMap::new();
                headers.insert(name.clone(), HeaderValue::from_str(value).unwrap());
                assert!(!carries_credentials(&headers), "{name}: {value}");
            }
        }
    }

    #[test]
    fn connection_bound_challenges_ignore_quoted_text_and_auth_parameters() {
        for value in [
            "NTLM",
            "nEgOtIaTe ticket",
            "Basic realm=\"other\", NTLM",
            "Digest realm=\"a,b\", qop=\"auth\", Negotiate",
            "Digest realm=\"a\\\",b\", NTLM",
        ] {
            assert!(has_challenge(value.as_bytes()), "{value}");
        }
        for value in [
            "Basic realm=\"NTLM\"",
            "Digest realm=\"a, Negotiate\"",
            "Digest realm=\"a\\\", NTLM\"",
            "Digest realm=\"a\", NTLM = \"value\"",
            "Digest Negotiate=\"value\"",
            "NTLM-extra token",
        ] {
            assert!(!has_challenge(value.as_bytes()), "{value}");
        }
        for (status, name) in [
            (StatusCode::UNAUTHORIZED, WWW_AUTHENTICATE),
            (
                StatusCode::PROXY_AUTHENTICATION_REQUIRED,
                PROXY_AUTHENTICATE,
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.append(name.clone(), HeaderValue::from_static("Basic realm=\"a\""));
            headers.append(name, HeaderValue::from_static("Negotiate"));
            assert!(challenges(status, &headers));
            assert!(!challenges(StatusCode::OK, &headers));
        }
    }
}
