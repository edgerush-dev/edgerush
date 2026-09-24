//! Writing an answer to a client: informational heads, the final head, and the framing of
//! the body after it.
//!
//! It only ever appends to a buffer; how much of that buffer has reached the socket is the
//! caller's to track, which is also where the final head's commitment is decided
//! ([14 §4](../../../../docs/14-downstream-server.md)). The head's framing is this writer's
//! own, worked out from what the body says is left of it and what the request asked: any
//! `Content-Length`, `Transfer-Encoding` or `Connection` a head still carries is not
//! written, so that nothing upstream of here — a filter above all — can put a second
//! account of the framing on the wire.

use super::date::HttpDate;
use crate::h1::{is_denied, length};
use edgerush_router::Fields;
use http::{HeaderMap, HeaderName, StatusCode, Version, header};

/// Why an answer cannot be written as asked. Each is a mistake of the caller's, or a body
/// that did not keep to what its head said; none is the client's doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    /// A final head was asked for with an informational status, or the other way round.
    #[error("the status is not one this head can carry")]
    Status,
    /// An informational head to an HTTP/1.0 client, which has none
    /// ([RFC 9110 §15.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.2)).
    #[error("an informational head to an HTTP/1.0 client")]
    InterimToHttp10,
    /// Body data for an answer that has none.
    #[error("the answer has no body, and data was written to it")]
    NoBody,
    /// More body than the length that was sent for it.
    #[error("the body is longer than the length its head gave")]
    Overran,
    /// Less body than the length that was sent for it. The connection cannot be kept: the
    /// client is still waiting for the rest.
    #[error("the body is shorter than the length its head gave")]
    Short,
    /// Anything written after the body was ended.
    #[error("the body was already ended")]
    AfterEnd,
}

/// What the request asked, as far as the answer's form turns on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Asked {
    /// Whether it was `HEAD`, whose answer describes a body that is not sent.
    pub head: bool,
    /// The version the client spoke. The answer is written in HTTP/1.1 either way; this
    /// decides only what may be sent to it.
    pub version: Version,
    /// Whether the client said `TE: trailers`, without which none are sent to it.
    pub trailers: bool,
}

/// What is known of the body before any of it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Content {
    /// Nothing: the body has already ended.
    Empty,
    /// Exactly this many bytes.
    Length(u64),
    /// Not known until it ends.
    Unknown,
}

/// How the body goes out after the head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delimited {
    /// It does not: the status or the method says there is none.
    Nothing,
    /// Exactly this many bytes.
    Length(u64),
    /// In chunks, and whether trailers may follow the last of them.
    Chunked {
        /// Whether the client may be sent trailers.
        trailers: bool,
    },
    /// Until the connection closes, for an HTTP/1.0 client that cannot read chunks.
    Close,
}

/// What writing a final head settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    /// How the body is to go out.
    pub delimited: Delimited,
    /// Whether the connection closes once this answer is done.
    pub closes: bool,
}

/// The fields of a final head as the writer takes them, whatever holds them: a header map,
/// or a raw answer's lines with its edits ([14 §6](../../../../docs/14-downstream-server.md)).
/// Read by name as any fields are, and written by the writer less the ones it writes itself.
pub trait AnswerFields: Fields {
    /// Appends every field but `Content-Length`, `Transfer-Encoding`, `Connection` and
    /// `Trailer`, one line each, and says whether a `Date` was among them.
    fn write_fields(&self, out: &mut Vec<u8>) -> bool;
}

impl AnswerFields for HeaderMap {
    fn write_fields(&self, out: &mut Vec<u8>) -> bool {
        let mut dated = false;
        for (name, value) in self {
            dated |= name == header::DATE;
            if !framing_field(name) {
                field(out, name.as_str().as_bytes(), value.as_bytes());
            }
        }
        dated
    }
}

/// The fields this writer writes for itself, or not at all.
fn framing_field(name: &HeaderName) -> bool {
    name == header::CONTENT_LENGTH
        || name == header::TRANSFER_ENCODING
        || name == header::CONNECTION
        || name == header::TRAILER
}

/// Writes the status line of an answer, always in HTTP/1.1: a server answers in the
/// highest version it conforms to that the request's major version allows
/// ([RFC 9112 §2.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-2.3)).
fn status_line(out: &mut Vec<u8>, status: StatusCode) {
    out.extend_from_slice(b"HTTP/1.1 ");
    out.extend_from_slice(status.as_str().as_bytes());
    out.push(b' ');
    // A status with no reason known here is written with an empty one, which the grammar
    // allows: the reason phrase is optional and carries nothing a client acts on.
    out.extend_from_slice(status.canonical_reason().unwrap_or("").as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Appends one field line.
pub(crate) fn field(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.extend_from_slice(name);
    out.extend_from_slice(b": ");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

/// Appends `value` in decimal, from a buffer on the stack.
fn decimal(out: &mut Vec<u8>, mut value: u64) {
    // The largest u64 is twenty digits.
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        // A remainder of ten is below ten, so it fits a byte.
        digits[at] = b'0' + u8::try_from(value % 10).unwrap_or(0);
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[at..]);
}

/// Appends `value` in hexadecimal, from a buffer on the stack.
fn hexadecimal(out: &mut Vec<u8>, mut value: u64) {
    // Upper case, as the grammar writes HEXDIG (RFC 5234 B.1) and as hyper writes it.
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    // The largest u64 is sixteen hexadecimal digits.
    let mut digits = [0u8; 16];
    let mut at = digits.len();
    loop {
        at -= 1;
        // A remainder of sixteen indexes the table.
        digits[at] = DIGITS[usize::try_from(value % 16).unwrap_or(0)];
        value /= 16;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[at..]);
}

/// Writes an informational head: a 1xx other than 101, and its fields, less the ones that
/// are this hop's.
///
/// # Errors
///
/// A status that is not informational or is 101, or a client that speaks HTTP/1.0.
pub fn write_interim(
    out: &mut Vec<u8>,
    status: StatusCode,
    headers: &HeaderMap,
    asked: Asked,
) -> Result<(), WriteError> {
    if !status.is_informational() || status == StatusCode::SWITCHING_PROTOCOLS {
        return Err(WriteError::Status);
    }
    if asked.version == Version::HTTP_10 {
        return Err(WriteError::InterimToHttp10);
    }
    status_line(out, status);
    for (name, value) in headers {
        if !framing_field(name) {
            field(out, name.as_str().as_bytes(), value.as_bytes());
        }
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

/// Writes the final head of an answer, and says how its body goes out and whether the
/// connection closes after it.
///
/// `persistent` is whether the connection may carry another request as far as everything
/// but this answer's own framing goes: what the request said, and anything the caller
/// knows that ends the connection. `date` is written where the head has no `Date` of its
/// own ([RFC 9110 §6.6.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-6.6.1)).
///
/// # Errors
///
/// An informational status, which [`write_interim`] writes.
pub fn write_head<F: AnswerFields + ?Sized>(
    out: &mut Vec<u8>,
    status: StatusCode,
    headers: &F,
    content: Content,
    asked: Asked,
    persistent: bool,
    date: &HttpDate,
) -> Result<Written, WriteError> {
    if status.is_informational() {
        return Err(WriteError::Status);
    }
    // A 204 and a 304 end at their heads whatever they say, and so does any answer to
    // HEAD; 205 is not among them, and says so with a length of nothing.
    let bodyless_status = status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED;
    let bodyless = bodyless_status || asked.head;
    let delimited = if bodyless {
        Delimited::Nothing
    } else {
        match content {
            Content::Empty => Delimited::Length(0),
            Content::Length(length) => Delimited::Length(length),
            Content::Unknown if asked.version == Version::HTTP_10 => Delimited::Close,
            Content::Unknown => Delimited::Chunked {
                trailers: asked.trailers,
            },
        }
    };
    let closes = !persistent || delimited == Delimited::Close;

    status_line(out, status);
    let dated = headers.write_fields(out);
    match delimited {
        // A HEAD answer's length describes what a GET would have been sent (RFC 9110
        // §9.3.2), so the upstream's is passed on as it came, and none is made up. A 204
        // or 304 is told nothing about a length at all.
        Delimited::Nothing => {
            if asked.head && !bodyless_status {
                let given = headers
                    .values(&header::CONTENT_LENGTH)
                    .next()
                    .filter(|value| length(value).is_ok());
                if let Some(value) = given {
                    field(out, b"content-length", value);
                }
            }
        }
        Delimited::Length(length) => {
            out.extend_from_slice(b"content-length: ");
            decimal(out, length);
            out.extend_from_slice(b"\r\n");
        }
        Delimited::Chunked { .. } => {
            out.extend_from_slice(b"transfer-encoding: chunked\r\n");
            // What the trailers will hold is declared only where they can be sent.
            for value in headers.values(&header::TRAILER) {
                field(out, b"trailer", value);
            }
        }
        Delimited::Close => {}
    }
    // Only what the version does not already say (RFC 9112 §9.3, §9.6), as hyper, HAProxy
    // and Envoy do.
    if closes && asked.version == Version::HTTP_11 {
        out.extend_from_slice(b"connection: close\r\n");
    } else if !closes && asked.version == Version::HTTP_10 {
        out.extend_from_slice(b"connection: keep-alive\r\n");
    }
    if !dated {
        field(out, b"date", date.as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    Ok(Written { delimited, closes })
}

/// Frames the body of an answer as it is written, as [`write_head`] said it would go.
#[derive(Debug)]
pub struct BodyFramer {
    delimited: Delimited,
    /// For a counted body: how many bytes are still owed.
    left: u64,
    done: bool,
}

impl BodyFramer {
    /// A framer for a body that goes out as `delimited`.
    pub fn new(delimited: Delimited) -> Self {
        Self {
            delimited,
            left: match delimited {
                Delimited::Length(length) => length,
                _ => 0,
            },
            done: false,
        }
    }

    /// Accounts for `size` bytes of payload and writes what goes before them, without
    /// copying them. Says whether a line ending is owed after them.
    ///
    /// # Errors
    ///
    /// Data for an answer that has no body, more than its length, or after its end.
    pub fn data_prefix(&mut self, out: &mut Vec<u8>, size: usize) -> Result<bool, WriteError> {
        if self.done {
            return Err(WriteError::AfterEnd);
        }
        if size == 0 {
            // Nothing to say, and saying it in chunks would say the opposite: a chunk of no
            // bytes is how a chunked body ends.
            return Ok(false);
        }
        let size = u64::try_from(size).unwrap_or(u64::MAX);
        match self.delimited {
            Delimited::Nothing => Err(WriteError::NoBody),
            Delimited::Length(_) => {
                self.left = self.left.checked_sub(size).ok_or(WriteError::Overran)?;
                Ok(false)
            }
            Delimited::Chunked { .. } => {
                hexadecimal(out, size);
                out.extend_from_slice(b"\r\n");
                Ok(true)
            }
            Delimited::Close => Ok(false),
        }
    }

    /// Ends the body: the last chunk and any trailers the client may be sent, for a chunked
    /// one; nothing but a check that every promised byte went, for a counted one.
    ///
    /// Trailers that cannot be sent — to a client that did not ask for them, or on a body
    /// that is not chunked — are dropped, not refused: the message was framed correctly and
    /// is not thrown away over a footer.
    ///
    /// # Errors
    ///
    /// A counted body that fell short of its length, or a second ending.
    pub fn finish(
        &mut self,
        out: &mut Vec<u8>,
        trailers: Option<&HeaderMap>,
    ) -> Result<(), WriteError> {
        if self.done {
            return Err(WriteError::AfterEnd);
        }
        self.done = true;
        match self.delimited {
            Delimited::Length(_) if self.left != 0 => Err(WriteError::Short),
            Delimited::Nothing | Delimited::Length(_) | Delimited::Close => Ok(()),
            Delimited::Chunked { trailers: allowed } => {
                out.extend_from_slice(b"0\r\n");
                if allowed {
                    for (name, value) in trailers.into_iter().flatten() {
                        // The same set as everywhere else: what may not travel as a trailer
                        // does not, whoever filtered it before.
                        if !is_denied(name, &[]) {
                            field(out, name.as_str().as_bytes(), value.as_bytes());
                        }
                    }
                }
                out.extend_from_slice(b"\r\n");
                Ok(())
            }
        }
    }

    /// Whether the body has been ended.
    pub fn is_done(&self) -> bool {
        self.done
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    const ELEVEN: Asked = Asked {
        head: false,
        version: Version::HTTP_11,
        trailers: false,
    };

    fn date() -> HttpDate {
        HttpDate::from_unix(784_111_777)
    }

    const DATED: &str = "date: Sun, 06 Nov 1994 08:49:37 GMT\r\n";

    fn fields(named: &[(&'static str, &'static str)]) -> HeaderMap {
        named
            .iter()
            .map(|(name, value)| (name.parse().unwrap(), HeaderValue::from_static(value)))
            .collect()
    }

    /// The head written, as text, and what it settled.
    fn head(
        status: u16,
        headers: &HeaderMap,
        content: Content,
        asked: Asked,
        persistent: bool,
    ) -> (String, Written) {
        let mut out = Vec::new();
        let written = write_head(
            &mut out,
            StatusCode::from_u16(status).unwrap(),
            headers,
            content,
            asked,
            persistent,
            &date(),
        )
        .unwrap();
        (String::from_utf8(out).unwrap(), written)
    }

    #[test]
    fn a_counted_answer_says_its_length_and_the_date() {
        let (text, written) = head(
            200,
            &fields(&[("content-type", "text/plain")]),
            Content::Length(5),
            ELEVEN,
            true,
        );
        assert_eq!(
            text,
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 5\r\n{DATED}\r\n"
            )
        );
        assert_eq!(
            written,
            Written {
                delimited: Delimited::Length(5),
                closes: false
            }
        );
    }

    #[test]
    fn a_date_the_answer_has_is_kept_and_no_second_is_added() {
        let (text, _) = head(
            200,
            &fields(&[("date", "Mon, 07 Nov 1994 00:00:00 GMT")]),
            Content::Empty,
            ELEVEN,
            true,
        );
        assert_eq!(text.matches("date:").count(), 1, "{text}");
        assert!(
            text.contains("date: Mon, 07 Nov 1994 00:00:00 GMT\r\n"),
            "{text}"
        );
    }

    /// Framing is the writer's own: what a head says about it is not what goes out.
    #[test]
    fn framing_on_the_head_is_never_written() {
        let said = fields(&[
            ("content-length", "99"),
            ("transfer-encoding", "gzip"),
            ("connection", "upgrade"),
            ("x-kept", "1"),
        ]);
        let (text, _) = head(200, &said, Content::Length(5), ELEVEN, true);
        assert_eq!(
            text,
            format!("HTTP/1.1 200 OK\r\nx-kept: 1\r\ncontent-length: 5\r\n{DATED}\r\n")
        );
    }

    #[test]
    fn a_body_of_unknown_length_is_chunked_to_1_1_and_ended_by_the_close_to_1_0() {
        let (text, written) = head(200, &HeaderMap::new(), Content::Unknown, ELEVEN, true);
        assert!(text.contains("transfer-encoding: chunked\r\n"), "{text}");
        assert_eq!(written.delimited, Delimited::Chunked { trailers: false });
        assert!(!written.closes);

        let ten = Asked {
            version: Version::HTTP_10,
            ..ELEVEN
        };
        let (text, written) = head(200, &HeaderMap::new(), Content::Unknown, ten, true);
        assert_eq!(text, format!("HTTP/1.1 200 OK\r\n{DATED}\r\n"));
        assert_eq!(written.delimited, Delimited::Close);
        assert!(written.closes, "the close is what ends the body");
    }

    /// Every answer is written in HTTP/1.1, an HTTP/1.0 client's included (RFC 9112 §2.3).
    #[test]
    fn every_answer_is_written_in_http_1_1() {
        for version in [Version::HTTP_10, Version::HTTP_11] {
            let asked = Asked { version, ..ELEVEN };
            let (text, _) = head(404, &HeaderMap::new(), Content::Empty, asked, false);
            assert!(text.starts_with("HTTP/1.1 404 Not Found\r\n"), "{text}");
        }
    }

    /// `Connection` says only what the version does not.
    #[test]
    fn connection_is_said_only_where_the_version_does_not_say_it() {
        let ten = Asked {
            version: Version::HTTP_10,
            ..ELEVEN
        };
        for (asked, persistent, said) in [
            (ELEVEN, true, None),
            (ELEVEN, false, Some("close")),
            (ten, true, Some("keep-alive")),
            (ten, false, None),
        ] {
            let (text, written) = head(
                200,
                &HeaderMap::new(),
                Content::Length(0),
                asked,
                persistent,
            );
            let connection = text
                .split("\r\n")
                .find_map(|line| line.strip_prefix("connection: "));
            assert_eq!(
                connection, said,
                "{asked:?} persistent {persistent}: {text}"
            );
            assert_eq!(written.closes, !persistent);
        }
    }

    #[test]
    fn an_empty_answer_that_may_have_a_body_says_it_has_none_205_included() {
        for status in [200, 205, 404, 502] {
            let (text, written) = head(status, &HeaderMap::new(), Content::Empty, ELEVEN, true);
            assert!(text.contains("content-length: 0\r\n"), "{status}: {text}");
            assert_eq!(written.delimited, Delimited::Length(0), "{status}");
        }
    }

    /// A 204 and a 304 are told nothing about a length; a HEAD answer passes the upstream's
    /// on as it came and makes none up.
    #[test]
    fn a_bodyless_answer_says_no_length_it_did_not_have() {
        let counted = fields(&[("content-length", "42")]);
        let asked_head = Asked {
            head: true,
            ..ELEVEN
        };
        for (status, headers, asked, said) in [
            (204, HeaderMap::new(), ELEVEN, None),
            (304, counted.clone(), ELEVEN, None),
            (200, counted.clone(), asked_head, Some("42")),
            (200, HeaderMap::new(), asked_head, None),
            (200, fields(&[("content-length", "4 2")]), asked_head, None),
            (304, counted.clone(), asked_head, None),
        ] {
            let (text, written) = head(status, &headers, Content::Length(42), asked, true);
            let length = text
                .split("\r\n")
                .find_map(|line| line.strip_prefix("content-length: "));
            assert_eq!(length, said, "{status} {asked:?}: {text}");
            assert!(!text.contains("transfer-encoding"), "{text}");
            assert_eq!(written.delimited, Delimited::Nothing);
        }
    }

    #[test]
    fn a_trailer_declaration_goes_only_with_chunks() {
        let declared = fields(&[("trailer", "x-sum")]);
        let (text, _) = head(200, &declared, Content::Unknown, ELEVEN, true);
        assert!(text.contains("trailer: x-sum\r\n"), "{text}");
        let (text, _) = head(200, &declared, Content::Length(3), ELEVEN, true);
        assert!(!text.contains("trailer"), "{text}");
    }

    #[test]
    fn a_status_with_no_known_reason_is_written_with_an_empty_one() {
        let (text, _) = head(299, &HeaderMap::new(), Content::Empty, ELEVEN, true);
        assert!(text.starts_with("HTTP/1.1 299 \r\n"), "{text}");
    }

    #[test]
    fn an_informational_head_is_written_with_its_fields_and_only_to_1_1() {
        let mut out = Vec::new();
        let links = fields(&[("link", "</a.css>; rel=preload"), ("connection", "close")]);
        write_interim(&mut out, StatusCode::from_u16(103).unwrap(), &links, ELEVEN).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\n"
        );
        let ten = Asked {
            version: Version::HTTP_10,
            ..ELEVEN
        };
        let mut out = Vec::new();
        assert_eq!(
            write_interim(&mut out, StatusCode::CONTINUE, &HeaderMap::new(), ten),
            Err(WriteError::InterimToHttp10)
        );
        for status in [101, 200] {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(
                write_interim(&mut out, status, &HeaderMap::new(), ELEVEN),
                Err(WriteError::Status)
            );
        }
        assert!(
            out.is_empty(),
            "nothing is written for a head that is refused"
        );
        assert_eq!(
            write_head(
                &mut out,
                StatusCode::CONTINUE,
                &HeaderMap::new(),
                Content::Empty,
                ELEVEN,
                true,
                &date()
            ),
            Err(WriteError::Status)
        );
    }

    /// A body framed as it goes, payload in between the framing the framer writes.
    fn framed(
        delimited: Delimited,
        pieces: &[&[u8]],
        trailers: Option<&HeaderMap>,
    ) -> Result<String, WriteError> {
        let mut framer = BodyFramer::new(delimited);
        let mut out = Vec::new();
        for piece in pieces {
            let owed = framer.data_prefix(&mut out, piece.len())?;
            out.extend_from_slice(piece);
            if owed {
                out.extend_from_slice(b"\r\n");
            }
        }
        framer.finish(&mut out, trailers)?;
        assert!(framer.is_done());
        Ok(String::from_utf8(out).unwrap())
    }

    #[test]
    fn a_chunked_body_is_chunks_then_the_last_chunk() {
        assert_eq!(
            framed(
                Delimited::Chunked { trailers: false },
                &[b"hello", b"", &[b'x'; 26]],
                None
            ),
            Ok(format!(
                "5\r\nhello\r\n1A\r\n{}\r\n0\r\n\r\n",
                "x".repeat(26)
            ))
        );
    }

    #[test]
    fn trailers_go_only_where_they_may() {
        let trailers = fields(&[("x-sum", "7"), ("content-length", "5")]);
        assert_eq!(
            framed(
                Delimited::Chunked { trailers: true },
                &[b"ab"],
                Some(&trailers)
            ),
            Ok("2\r\nab\r\n0\r\nx-sum: 7\r\n\r\n".to_owned()),
            "a denied trailer is still not sent"
        );
        assert_eq!(
            framed(
                Delimited::Chunked { trailers: false },
                &[b"ab"],
                Some(&trailers)
            ),
            Ok("2\r\nab\r\n0\r\n\r\n".to_owned())
        );
        assert_eq!(
            framed(Delimited::Length(2), &[b"ab"], Some(&trailers)),
            Ok("ab".to_owned())
        );
        assert_eq!(
            framed(Delimited::Close, &[b"ab"], Some(&trailers)),
            Ok("ab".to_owned())
        );
    }

    #[test]
    fn a_counted_body_is_held_to_its_length() {
        assert_eq!(
            framed(Delimited::Length(4), &[b"ab", b"cd"], None),
            Ok("abcd".to_owned())
        );
        assert_eq!(
            framed(Delimited::Length(3), &[b"ab", b"cd"], None),
            Err(WriteError::Overran)
        );
        assert_eq!(
            framed(Delimited::Length(5), &[b"ab", b"cd"], None),
            Err(WriteError::Short)
        );
    }

    #[test]
    fn an_answer_with_no_body_takes_none() {
        assert_eq!(
            framed(Delimited::Nothing, &[b"", b""], None),
            Ok(String::new())
        );
        assert_eq!(
            framed(Delimited::Nothing, &[b"x"], None),
            Err(WriteError::NoBody)
        );
    }

    #[test]
    fn nothing_is_written_after_the_end() {
        let mut framer = BodyFramer::new(Delimited::Chunked { trailers: false });
        let mut out = Vec::new();
        framer.finish(&mut out, None).unwrap();
        assert_eq!(framer.data_prefix(&mut out, 1), Err(WriteError::AfterEnd));
        assert_eq!(framer.finish(&mut out, None), Err(WriteError::AfterEnd));
    }

    #[test]
    fn numbers_are_written_whole() {
        for value in [0, 9, 10, 255, 4096, u64::MAX] {
            let mut out = Vec::new();
            decimal(&mut out, value);
            assert_eq!(String::from_utf8(out).unwrap(), value.to_string());
            let mut out = Vec::new();
            hexadecimal(&mut out, value);
            assert_eq!(String::from_utf8(out).unwrap(), format!("{value:X}"));
        }
    }

    mod round_trip {
        //! What the writer writes, read back by the reader the upstream side reads answers
        //! with: the same status, the same fields, the same body and the trailers the client
        //! may have. Two pieces of code written against one specification, so that a
        //! framing either gets wrong shows up as the other reading something else.

        use super::*;
        use crate::upstream::h1::H1Limits;
        use crate::upstream::h1::codec::{self, BodyReader, Head, HeadReader, Piece, delivery};
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        struct Case {
            status: u16,
            fields: Vec<(String, String)>,
            pieces: Vec<Vec<u8>>,
            known: bool,
            asked: Asked,
            persistent: bool,
            trailers: Vec<(String, String)>,
        }

        fn case() -> impl Strategy<Value = Case> {
            let status = prop::sample::select(vec![200_u16, 201, 204, 205, 299, 304, 404, 502]);
            let name = "x-[a-z]{1,6}";
            let value = "[ -~]{0,12}".prop_map(|value: String| value.trim().to_owned());
            (
                status,
                prop::collection::vec((name, value.clone()), 0..4),
                prop::collection::vec(prop::collection::vec(any::<u8>(), 0..40), 0..4),
                any::<bool>(),
                (any::<bool>(), any::<bool>(), any::<bool>()),
                any::<bool>(),
                prop::collection::vec(("x-t-[a-z]{1,4}", value), 0..3),
            )
                .prop_map(
                    |(status, fields, pieces, known, (head, old, trailers), persistent, sent)| {
                        Case {
                            status,
                            fields,
                            pieces,
                            known,
                            asked: Asked {
                                head,
                                version: if old {
                                    Version::HTTP_10
                                } else {
                                    Version::HTTP_11
                                },
                                trailers,
                            },
                            persistent,
                            trailers: sent,
                        }
                    },
                )
        }

        fn map(named: &[(String, String)]) -> HeaderMap {
            named
                .iter()
                .map(|(name, value)| {
                    (
                        HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        HeaderValue::from_str(value).unwrap(),
                    )
                })
                .collect()
        }

        proptest! {
            #[test]
            fn what_is_written_is_what_is_read(case in case()) {
                let body: Vec<u8> = case.pieces.concat();
                let content = if body.is_empty() {
                    Content::Empty
                } else if case.known {
                    Content::Length(u64::try_from(body.len()).unwrap())
                } else {
                    Content::Unknown
                };
                let status = StatusCode::from_u16(case.status).unwrap();
                let fields = map(&case.fields);
                let trailers = map(&case.trailers);

                let mut wire = Vec::new();
                let written = write_head(
                    &mut wire, status, &fields, content, case.asked, case.persistent, &date(),
                )
                .unwrap();
                let mut framer = BodyFramer::new(written.delimited);
                let bodyless = written.delimited == Delimited::Nothing;
                if !bodyless {
                    for piece in &case.pieces {
                        let owed = framer.data_prefix(&mut wire, piece.len()).unwrap();
                        wire.extend_from_slice(piece);
                        if owed {
                            wire.extend_from_slice(b"\r\n");
                        }
                    }
                }
                framer.finish(&mut wire, Some(&trailers)).unwrap();

                let limits = H1Limits::default();
                let Ok(Head::Read { head, consumed }) = HeadReader::default().read(&wire, &limits)
                else {
                    panic!("not read back: {:?}", String::from_utf8_lossy(&wire));
                };
                let head = head.of(bytes::Bytes::copy_from_slice(&wire[..consumed]));
                prop_assert_eq!(head.status, status);
                let read_back = head.to_map();
                for (name, value) in &fields {
                    prop_assert!(
                        read_back.get_all(name).iter().any(|read| read == value),
                        "{} lost", name
                    );
                }
                let asked = if case.asked.head { codec::Asked::Head } else { codec::Asked::Anything };
                let framing = delivery(&head, asked).unwrap().framing;
                let mut reader = BodyReader::new(framing);
                let mut rest = &wire[consumed..];
                let mut read = Vec::new();
                let read_trailers = loop {
                    match reader.read(rest, true, &limits).unwrap() {
                        Piece::More => panic!("the body never ended"),
                        Piece::Data { data, consumed } => {
                            read.extend_from_slice(&rest[data]);
                            rest = &rest[consumed..];
                        }
                        Piece::End { trailers, consumed } => {
                            rest = &rest[consumed..];
                            break trailers;
                        }
                    }
                };
                prop_assert!(rest.is_empty(), "left over: {:?}", rest);
                let expected = if bodyless { Vec::new() } else { body };
                prop_assert_eq!(read, expected);
                let chunked_with = matches!(written.delimited, Delimited::Chunked { trailers: true });
                let read_trailers = read_trailers.map(|read| read.fields).unwrap_or_default();
                if chunked_with {
                    prop_assert_eq!(read_trailers, trailers);
                } else {
                    prop_assert!(read_trailers.is_empty());
                }
            }
        }
    }
}
