//! The cookie string, whole.
//!
//! HTTP/2 lets a client split its cookie string into several `Cookie` fields, for the sake
//! of header compression; nothing before it does, and RFC 9113 §8.2.3 has whoever passes
//! such a request on put the string together again, the pieces joined by `"; "`. An
//! HTTP/1.1 upstream that is sent the pieces may read only the first, or the last, or join
//! them with a comma into a cookie string that means something else.
//!
//! It is done before routing, whatever protocol the request came in by, so that a rule
//! that looks at cookies and the upstream that gets them read the same string.

use http::HeaderMap;
use http::header::{COOKIE, HeaderValue};

/// Makes one `Cookie` field of several, in their order, and leaves one or none alone.
/// Allocates the joined string, so a request with at most one `Cookie` field is better not
/// brought here at all.
pub(crate) fn join(headers: &mut HeaderMap) {
    let mut joined = Vec::new();
    let mut sensitive = false;
    let mut pieces = 0;
    for piece in headers.get_all(COOKIE) {
        if pieces > 0 {
            joined.extend_from_slice(b"; ");
        }
        joined.extend_from_slice(piece.as_bytes());
        sensitive |= piece.is_sensitive();
        pieces += 1;
    }
    if pieces < 2 {
        return;
    }
    // Pieces that were header values, joined by what may be in one, are a header value; if
    // they ever were not, the pieces are left as they came.
    if let Ok(mut whole) = HeaderValue::from_bytes(&joined) {
        // A piece that must not be written to compression tables makes the whole so.
        whole.set_sensitive(sensitive);
        headers.insert(COOKIE, whole);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookies(pieces: &[&str]) -> Vec<String> {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("*/*"));
        for piece in pieces {
            headers.append(COOKIE, HeaderValue::from_str(piece).unwrap());
        }
        join(&mut headers);
        assert_eq!(headers.get("accept").unwrap(), "*/*");
        headers
            .get_all(COOKIE)
            .iter()
            .map(|cookie| cookie.to_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn pieces_are_joined_in_their_order() {
        assert_eq!(cookies(&["a=1", "b=2"]), ["a=1; b=2"]);
        assert_eq!(cookies(&["c=3", "a=1", "b=2"]), ["c=3; a=1; b=2"]);
        assert_eq!(cookies(&["a=1; b=2", "c=3"]), ["a=1; b=2; c=3"]);
    }

    #[test]
    fn a_whole_cookie_string_and_none_at_all_stay_as_they_are() {
        assert_eq!(cookies(&["a=1; b=2"]), ["a=1; b=2"]);
        assert_eq!(cookies(&[]), Vec::<String>::new());
    }

    #[test]
    fn pieces_are_joined_as_they_are_and_not_tidied() {
        // The RFC says to concatenate; what a piece means is for the upstream to say.
        assert_eq!(cookies(&["a=1", "", "b=2;"]), ["a=1; ; b=2;"]);
        assert_eq!(cookies(&["", "a=1"]), ["; a=1"]);
        assert_eq!(cookies(&["a=1,x=2", "b=2"]), ["a=1,x=2; b=2"]);
    }

    #[test]
    fn one_sensitive_piece_makes_the_whole_sensitive() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("a=1"));
        let mut secret = HeaderValue::from_static("session=s3cr3t");
        secret.set_sensitive(true);
        headers.append(COOKIE, secret);
        join(&mut headers);
        assert!(headers.get(COOKIE).unwrap().is_sensitive());

        let mut plain = HeaderMap::new();
        plain.append(COOKIE, HeaderValue::from_static("a=1"));
        plain.append(COOKIE, HeaderValue::from_static("b=2"));
        join(&mut plain);
        assert!(!plain.get(COOKIE).unwrap().is_sensitive());
    }
}
