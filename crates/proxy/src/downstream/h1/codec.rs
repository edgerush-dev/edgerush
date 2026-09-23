//! Reading what a client says: its request head, and how the body after it is framed.
//!
//! Bytes in, an answer out: nothing here waits for anything, so every rule below can be put
//! to a table of inputs, and every one of those inputs cut at each of its bytes without the
//! answer changing.
//!
//! **Anyone may send these bytes.** A client is not a backend the control plane named, and
//! the hazard of this half is a request that two readers end in two different places — the
//! gateway in one and the upstream in another — which is what request smuggling is. So a
//! head is refused, as soon as its bytes show it, wherever it could be read in more than one
//! way, and what a permissive parser would allow is not the measure of what is allowed
//! ([14 §4](../../../../docs/14-downstream-server.md)). A body is read with the upstream
//! side's own reader ([`crate::h1::BodyReader`]); only its framing is decided here.

use crate::fields::FieldLines;
use crate::h1::{Framing, MOST_FIELDS, length, reason};
use crate::hop_by_hop::{is_token_byte, options_of};
use crate::upstream::h1::H1Limits;
use edgerush_router::Fields;
use http::{Method, StatusCode, Uri, Version};

/// Why a client's request cannot be read, and so what it is answered with.
///
/// Every one of them ends the connection after the answer: what came after a request that
/// could not be read cannot be known to be the start of another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    /// The request line went past what one may be without ending.
    #[error("the request line is longer than {limit} bytes")]
    LineTooLong {
        /// What a request line may come to.
        limit: usize,
    },
    /// The head went past what a head may be without ending.
    #[error("the request head is longer than {limit} bytes")]
    HeadTooLong {
        /// What a head may come to.
        limit: usize,
    },
    /// More fields than a head may carry.
    #[error("the request head has more than {limit} fields")]
    TooManyFields {
        /// How many fields a head may carry.
        limit: usize,
    },
    /// A version written as one, but not one this speaks.
    #[error("the request is not HTTP/1.0 or HTTP/1.1")]
    Version,
    /// The head does not parse, or parses into something that is not a request.
    #[error("the request head is malformed: {0}")]
    Malformed(&'static str),
    /// Two lengths, whether or not they agree.
    #[error("the request has more than one content-length")]
    RepeatedLength,
    /// A length that is not DIGIT only, or does not fit.
    #[error("the request has a content-length that is not a plain number")]
    BadLength,
    /// Both ways of saying how long a body is, which is the shape request smuggling is
    /// built on.
    #[error("the request has both a content-length and a transfer-encoding")]
    LengthAndCoding,
    /// Chunked came with HTTP/1.1, so a 1.0 request claiming a coding is not believed.
    #[error("the request is HTTP/1.0 and claims a transfer-encoding")]
    CodingOnHttp10,
    /// A coding that does not end with `chunked`, so nothing says where the body ends.
    #[error("the request's transfer-encoding does not end with chunked")]
    NotChunkedLast,
    /// `chunked` more than once, which a sender may not do.
    #[error("the request is chunked more than once")]
    ChunkedTwice,
    /// A coding before the `chunked` that this does not decode.
    #[error("the request has a transfer coding that is not decoded here")]
    CodingNotUnderstood,
    /// A `Connection` holding something that is not a list of tokens.
    #[error("the request has a connection field that is not a list of tokens")]
    BadConnection,
}

impl RequestError {
    /// What the client is answered ([14 §4](../../../../docs/14-downstream-server.md)).
    pub fn status(self) -> StatusCode {
        match self {
            // RFC 9112 §3: a target longer than any a server wishes to parse MUST be
            // answered 414. A method that long SHOULD be 501; one status for the whole
            // line is simpler, and the connection closes either way.
            Self::LineTooLong { .. } => StatusCode::URI_TOO_LONG,
            Self::HeadTooLong { .. } | Self::TooManyFields { .. } => {
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            }
            Self::Version => StatusCode::HTTP_VERSION_NOT_SUPPORTED,
            // RFC 9112 §6.1: a coding a server does not understand SHOULD be 501. Only
            // where `chunked` is last, so that the body could at least be delimited.
            Self::CodingNotUnderstood => StatusCode::NOT_IMPLEMENTED,
            Self::Malformed(_)
            | Self::RepeatedLength
            | Self::BadLength
            | Self::LengthAndCoding
            | Self::CodingOnHttp10
            | Self::NotChunkedLast
            | Self::ChunkedTwice
            | Self::BadConnection => StatusCode::BAD_REQUEST,
        }
    }
}

/// A request head, once it has been read and found sound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    /// What is asked.
    pub method: Method,
    /// Of what, as the client wrote it: which forms of it are served is the request
    /// core's to decide.
    pub target: Uri,
    /// The version it asked in, which is HTTP/1.0 or HTTP/1.1 and nothing else.
    pub version: Version,
    /// Where its fields lie in the bytes it was read from, in the order they came, repeats
    /// and all: read out of those bytes, never copied (14 §6).
    pub fields: FieldLines,
    /// The one `Content-Length`, already checked.
    pub content_length: Option<u64>,
}

/// How far reading a head has got.
#[derive(Debug, PartialEq, Eq)]
#[expect(
    clippy::large_enum_variant,
    reason = "returned and matched at once, never stored; boxing the head would be an \
              allocation on every request to make a return value smaller"
)]
pub enum Head {
    /// Not all of it has arrived. Nothing was consumed; ask again with more.
    More,
    /// A head, and how many bytes of what was given it took, the empty line a client may
    /// send before it included. What follows those bytes is the body, or the next request.
    Read {
        /// The head that was read.
        head: RequestHead,
        /// How many bytes of what was given it took.
        consumed: usize,
    },
}

/// Which part of the request line is being checked.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Part {
    #[default]
    Method,
    Target,
    Version,
    LineFeed,
}

/// Reads a request head from bytes as they come.
///
/// The bytes grow: every call is given everything that has arrived so far, the earlier
/// bytes included. Nothing already looked at is looked at again, so a head that arrives a
/// byte at a time costs no more than one that arrives whole. Every byte is judged in the
/// order it came, bounds included, so the same bytes are refused for the same reason however
/// they were split.
#[derive(Debug, Default)]
pub struct HeadReader {
    /// Where the request line begins: past the one empty line a client may send before it,
    /// once the bytes have said whether there is one.
    start: Option<usize>,
    part: Part,
    /// Where the part being checked began.
    part_from: usize,
    /// How much of the request line has been checked.
    checked: usize,
    /// Where the request line ends, its line feed included, once it has.
    line_end: Option<usize>,
    /// How much of the rest has been searched for the empty line that ends a head.
    searched: usize,
    /// Completed field lines.
    fields: usize,
    /// Every byte looked at, for the test that holds the reader to linear work.
    #[cfg(test)]
    examined: usize,
}

impl HeadReader {
    /// Reads a head from the front of `bytes`, if all of it is there.
    ///
    /// # Errors
    ///
    /// Bytes that cannot become a request line, a line or head past `limits`, a version
    /// this does not speak, a head that does not parse, or a length that cannot be trusted.
    pub fn read(&mut self, bytes: &[u8], limits: &H1Limits) -> Result<Head, RequestError> {
        let start = match self.start {
            Some(start) => start,
            None => match bytes {
                [] | [b'\r'] => return Ok(Head::More),
                // RFC 9112 §2.2: a server SHOULD skip at least one empty line before a
                // request line. One, and it counts against the head's bound.
                [b'\r', b'\n', ..] => *self.start.insert(2),
                [b'\r', ..] => return Err(RequestError::Malformed("a bare carriage return")),
                _ => *self.start.insert(0),
            },
        };
        let line_end = match self.line_end {
            Some(end) => end,
            None => match self.request_line(bytes, start, limits)? {
                Some(end) => {
                    self.searched = end;
                    *self.line_end.insert(end)
                }
                None => return Ok(Head::More),
            },
        };
        let Some(end) = self.end_of_head(bytes, line_end, limits)? else {
            return Ok(Head::More);
        };
        // The lines are taken down against everything given up to the head's end, the
        // empty line before it included, which is what a reader of them is handed.
        let head = parse(&bytes[..end], start, self.fields, limits)?;
        Ok(Head::Read {
            head,
            consumed: end,
        })
    }

    /// Checks the request line as far as it has arrived, and says where it ends once it
    /// has: `method SP target SP HTTP/D.D CRLF`, nothing more and nothing less.
    ///
    /// Refusing here rather than at the end of the head is the point: bytes that can never
    /// be a request — a TLS handshake sent to a plaintext port, say — are not held until a
    /// bound or a deadline gives up on them.
    fn request_line(
        &mut self,
        bytes: &[u8],
        start: usize,
        limits: &H1Limits,
    ) -> Result<Option<usize>, RequestError> {
        if self.checked < start {
            self.checked = start;
            self.part_from = start;
        }
        while let Some(&byte) = bytes.get(self.checked) {
            let at = self.checked;
            #[cfg(test)]
            {
                self.examined += 1;
            }
            // The bound first, so that a line too long is refused as too long wherever the
            // bytes past it were cut.
            if at - start >= limits.request_line {
                return Err(RequestError::LineTooLong {
                    limit: limits.request_line,
                });
            }
            match self.part {
                Part::Method if byte == b' ' => {
                    if at == self.part_from {
                        return Err(RequestError::Malformed("the request line has no method"));
                    }
                    self.part = Part::Target;
                    self.part_from = at + 1;
                }
                Part::Method if !is_token_byte(byte) => {
                    return Err(RequestError::Malformed("the method is not a token"));
                }
                Part::Target if byte == b' ' => {
                    if at == self.part_from {
                        return Err(RequestError::Malformed("the request line has no target"));
                    }
                    self.part = Part::Version;
                    self.part_from = at + 1;
                }
                // Controls, spaces and DEL are what ends a target in one reader and not
                // in another. Bytes past ASCII are left for the target's own parser.
                Part::Target if byte < 0x21 || byte == 0x7f => {
                    return Err(RequestError::Malformed(
                        "the target has a byte no target has",
                    ));
                }
                Part::Version => {
                    let offset = at - self.part_from;
                    if offset == 8 {
                        if byte != b'\r' {
                            return Err(RequestError::Malformed(
                                "the request line goes on after its version",
                            ));
                        }
                        // Written as a version, so the question is only which one.
                        let version = &bytes[self.part_from..at];
                        if version != b"HTTP/1.1" && version != b"HTTP/1.0" {
                            return Err(RequestError::Version);
                        }
                        self.part = Part::LineFeed;
                    } else {
                        let fits = match offset {
                            0..=4 => byte == b"HTTP/"[offset],
                            6 => byte == b'.',
                            _ => byte.is_ascii_digit(),
                        };
                        if !fits {
                            return Err(RequestError::Malformed("its version is not one"));
                        }
                    }
                }
                Part::LineFeed => {
                    if byte != b'\n' {
                        return Err(RequestError::Malformed("a bare carriage return"));
                    }
                    self.checked = at + 1;
                    return Ok(Some(at + 1));
                }
                Part::Method | Part::Target => {}
            }
            self.checked += 1;
        }
        Ok(None)
    }

    /// Where the empty line that ends a head finishes, or `None` while there is none.
    ///
    /// # Errors
    ///
    /// A newline no carriage return comes before, which is one of the ways two readers
    /// are brought to disagree about where a line ends, or a head past its bound.
    fn end_of_head(
        &mut self,
        bytes: &[u8],
        line_end: usize,
        limits: &H1Limits,
    ) -> Result<Option<usize>, RequestError> {
        while let Some(&byte) = bytes.get(self.searched) {
            let at = self.searched;
            #[cfg(test)]
            {
                self.examined += 1;
            }
            if at >= limits.head {
                return Err(RequestError::HeadTooLong { limit: limits.head });
            }
            self.searched += 1;
            if byte != b'\n' {
                continue;
            }
            if bytes[at - 1] != b'\r' {
                return Err(RequestError::Malformed("a line ends with a bare newline"));
            }
            // A CRLF straight after the line feed that ended the line before it.
            if at == line_end + 1 || (at >= line_end + 3 && bytes[at - 2] == b'\n') {
                return Ok(Some(at + 1));
            }
            self.fields += 1;
        }
        Ok(None)
    }
}

/// Makes a head of the bytes of one, which are known to end with an empty line and to
/// begin with a sound request line.
///
/// `given` is everything up to the end of the head, which begins at `start`: its field lines
/// are taken down as places in `given`.
fn parse(
    given: &[u8],
    start: usize,
    fields: usize,
    limits: &H1Limits,
) -> Result<RequestHead, RequestError> {
    // Most requests have only a few fields; the full room is there for those that do not.
    if fields <= 16 {
        parse_with::<16>(given, start, limits)
    } else {
        parse_with::<MOST_FIELDS>(given, start, limits)
    }
}

fn parse_with<const N: usize>(
    given: &[u8],
    start: usize,
    limits: &H1Limits,
) -> Result<RequestHead, RequestError> {
    let head = given.get(start..).unwrap_or_default();
    let mut fields = [httparse::EMPTY_HEADER; N];
    let room = limits.fields.min(N);
    let mut request = httparse::Request::new(&mut fields[..room]);
    // Its defaults refuse obsolete line folding and whitespace before a colon in a
    // request, both of which RFC 9112 §5 says a server MUST reject. Named, because they
    // are a decision and not a default we happened to get.
    let config = httparse::ParserConfig::default();
    match config.parse_request(&mut request, head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => {
            return Err(RequestError::Malformed("it is cut short"));
        }
        Err(httparse::Error::TooManyHeaders) => {
            return Err(RequestError::TooManyFields { limit: room });
        }
        Err(error) => return Err(RequestError::Malformed(reason(error))),
    }

    let version = match request.version {
        Some(0) => Version::HTTP_10,
        Some(1) => Version::HTTP_11,
        _ => return Err(RequestError::Version),
    };
    let method = request
        .method
        .and_then(|method| Method::from_bytes(method.as_bytes()).ok())
        .ok_or(RequestError::Malformed("its method is not one"))?;
    let target = request
        .path
        .and_then(|target| target.parse::<Uri>().ok())
        .ok_or(RequestError::Malformed("its target is not one"))?;

    // The parser holds a field name to a token and a value to what a value may be, which
    // is all a header map would: nothing is checked again (the `h1_request` fuzz target
    // holds every head it reads to making a map without losing a field).
    let mut content_length = None;
    for field in request.headers.iter() {
        if field.name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(RequestError::RepeatedLength);
            }
            content_length = Some(length(field.value).map_err(|_| RequestError::BadLength)?);
        }
    }
    let fields = FieldLines::new(given, request.headers)
        .map_err(|_| RequestError::Malformed("a field is not a line of it"))?;

    Ok(RequestHead {
        method,
        target,
        version,
        fields,
        content_length,
    })
}

/// What a request head says about the body after it and the connection it came on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arrival {
    /// What delimits the body that follows. Never the close: a request cannot be ended by
    /// its client closing, because then there would be nobody to answer.
    pub framing: Framing,
    /// Whether this connection may carry another request once this one is answered, by
    /// what the request said. Worked out here, while `Connection` is still on the head.
    pub persistent: bool,
}

/// What follows a request head, and whether another request may follow that.
///
/// # Errors
///
/// A coding on HTTP/1.0, a coding beside a length, a coding list that does not end with a
/// single `chunked`, or a `Connection` that is not a list of tokens
/// ([14 §4](../../../../docs/14-downstream-server.md)).
pub fn arrival<F: Fields + ?Sized>(
    head: &RequestHead,
    fields: &F,
) -> Result<Arrival, RequestError> {
    let mut codings = fields.values(&http::header::TRANSFER_ENCODING).peekable();
    // Decided by whether the field is there, not by what it lists: RFC 9112 §6.3 frames by
    // its presence, so one listing nothing still overrules a length beside it.
    let framing = if codings.peek().is_some() {
        // RFC 9112 §6.1: the framing of a 1.0 message with a coding MUST be treated as
        // faulty.
        if head.version == Version::HTTP_10 {
            return Err(RequestError::CodingOnHttp10);
        }
        // RFC 9112 §6.3 rule 3: a server MAY reject this, and MUST close after it.
        if head.content_length.is_some() {
            return Err(RequestError::LengthAndCoding);
        }
        let mut last = None;
        let mut chunked = 0;
        let mut others = 0;
        let mut all_tokens = true;
        for coding in codings.flat_map(options_of) {
            if coding.eq_ignore_ascii_case(b"chunked") {
                chunked += 1;
            } else {
                others += 1;
                all_tokens &= coding.iter().copied().all(is_token_byte);
            }
            last = Some(coding);
        }
        // RFC 9112 §6.3 rule 4: without `chunked` last the length cannot be known, and the
        // server MUST answer 400. A field listing nothing has no `chunked` last either.
        if !last.is_some_and(|coding| coding.eq_ignore_ascii_case(b"chunked")) {
            return Err(RequestError::NotChunkedLast);
        }
        // RFC 9112 §7: a sender MUST NOT chunk a body twice.
        if chunked > 1 {
            return Err(RequestError::ChunkedTwice);
        }
        if !all_tokens {
            return Err(RequestError::Malformed("a transfer coding is not a token"));
        }
        if others > 0 {
            return Err(RequestError::CodingNotUnderstood);
        }
        Framing::Chunked
    } else if let Some(length) = head.content_length {
        Framing::Length(length)
    } else {
        // RFC 9112 §6.3 rule 6: neither is no body at all.
        Framing::None
    };

    let mut closing = false;
    let mut keep_alive = false;
    for value in fields.values(&http::header::CONNECTION) {
        for option in options_of(value) {
            if !option.iter().copied().all(is_token_byte) {
                return Err(RequestError::BadConnection);
            }
            closing |= option.eq_ignore_ascii_case(b"close");
            keep_alive |= option.eq_ignore_ascii_case(b"keep-alive");
        }
    }
    // Persistence is 1.1's default and only an extension of 1.0's, which asks for it.
    let persistent = !closing && (head.version == Version::HTTP_11 || keep_alive);
    Ok(Arrival {
        framing,
        persistent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn limits() -> H1Limits {
        H1Limits::default()
    }

    /// Reads a head from all of `bytes` at once.
    fn read(bytes: &[u8]) -> Result<Head, RequestError> {
        HeadReader::default().read(bytes, &limits())
    }

    /// Reads a head whole, and says what it is or why it was refused.
    fn head(bytes: &[u8]) -> RequestHead {
        match read(bytes) {
            Ok(Head::Read { head, .. }) => head,
            other => panic!("{:?}: {other:?}", String::from_utf8_lossy(bytes)),
        }
    }

    fn refused(bytes: &[u8]) -> RequestError {
        match read(bytes) {
            Err(error) => error,
            other => panic!(
                "{:?} was not refused: {other:?}",
                String::from_utf8_lossy(bytes)
            ),
        }
    }

    /// Each prefix in turn, to one reader, as a connection delivers them a byte at a time.
    /// Says the first answer that is not "more", and checks it is what the whole gives.
    fn by_the_byte(bytes: &[u8]) -> Result<Head, RequestError> {
        let whole = read(bytes);
        let mut reader = HeadReader::default();
        for end in 1..=bytes.len() {
            match reader.read(&bytes[..end], &limits()) {
                Ok(Head::More) => {}
                answer => {
                    assert_eq!(
                        answer,
                        whole,
                        "{:?} cut at {end}",
                        String::from_utf8_lossy(bytes)
                    );
                    return answer;
                }
            }
        }
        assert_eq!(
            whole,
            Ok(Head::More),
            "{:?}",
            String::from_utf8_lossy(bytes)
        );
        whole
    }

    #[test]
    fn a_request_head_is_read_with_what_it_says() {
        let bytes = b"POST /cart?x=1 HTTP/1.1\r\nHost: shop.test\r\nContent-Length: 3\r\nX-A: 1\r\nx-a: 2\r\n\r\nabc";
        let Ok(Head::Read { head, consumed }) = read(bytes) else {
            panic!("not read");
        };
        assert_eq!(consumed, bytes.len() - 3, "the body is not the head's");
        assert_eq!(head.method, Method::POST);
        assert_eq!(head.target, "/cart?x=1");
        assert_eq!(head.version, Version::HTTP_11);
        assert_eq!(head.content_length, Some(3));
        let view = head.fields.view(bytes);
        let repeated: Vec<&[u8]> = view.values(&http::HeaderName::from_static("x-a")).collect();
        assert_eq!(
            repeated,
            [b"1".as_slice(), b"2"],
            "repeats are kept, in order"
        );
    }

    /// A head after the one empty line a client may send first has its fields read where
    /// they are in what was given, the empty line included: a reader of them is handed all
    /// of it.
    #[test]
    fn fields_after_an_empty_line_are_read_where_they_are() {
        let bytes = b"\r\nGET / HTTP/1.1\r\nHost: shop.test\r\nX-A: 1\r\n\r\n";
        let Ok(Head::Read { head, consumed }) = read(bytes) else {
            panic!("not read");
        };
        assert_eq!(consumed, bytes.len());
        let view = head.fields.view(bytes);
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            [
                (b"Host".as_slice(), b"shop.test".as_slice()),
                (b"X-A", b"1")
            ]
        );
    }

    #[test]
    fn a_head_with_no_fields_is_a_head() {
        let head = head(b"GET / HTTP/1.0\r\n\r\n");
        assert_eq!(head.version, Version::HTTP_10);
        assert!(head.fields.is_empty());
    }

    #[test]
    fn every_form_of_target_is_read_and_left_to_the_core() {
        for target in ["/", "/a/b?c", "http://shop.test/a", "shop.test:443", "*"] {
            let bytes = format!("OPTIONS {target} HTTP/1.1\r\nhost: shop.test\r\n\r\n");
            assert_eq!(head(bytes.as_bytes()).target, target);
        }
    }

    /// RFC 9112 §2.2 asks a server to skip at least one empty line before a request line.
    /// This skips exactly one, and counts it as part of the head.
    #[test]
    fn one_empty_line_before_the_request_line_is_skipped_and_no_more() {
        let bytes = b"\r\nGET / HTTP/1.1\r\nhost: a\r\n\r\n";
        assert!(matches!(read(bytes), Ok(Head::Read { consumed, .. }) if consumed == bytes.len()));
        assert!(matches!(
            refused(b"\r\n\r\nGET / HTTP/1.1\r\n\r\n"),
            RequestError::Malformed(_)
        ));
        assert!(matches!(
            refused(b"\nGET / HTTP/1.1\r\n\r\n"),
            RequestError::Malformed(_)
        ));
    }

    #[test]
    fn a_line_that_does_not_end_in_crlf_is_refused() {
        for bytes in [
            &b"GET / HTTP/1.1\n\r\n"[..],
            b"GET / HTTP/1.1\r\nhost: a\n\r\n",
            b"GET / HTTP/1.1\r\nhost: a\r\n\n",
            b"GET / HTTP/1.1\rhost: a\r\n\r\n",
            b"GET / HTTP/1.1\r\nhost: a\rb\r\n\r\n",
        ] {
            assert!(
                matches!(refused(bytes), RequestError::Malformed(_)),
                "{:?}",
                String::from_utf8_lossy(bytes)
            );
        }
    }

    /// RFC 9112 §5.1 and §5.2: whitespace before a colon, and a folded line, MUST be
    /// refused with 400.
    #[test]
    fn whitespace_before_a_colon_and_folded_lines_are_refused() {
        for bytes in [
            &b"GET / HTTP/1.1\r\nhost : a\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nhost\t: a\r\n\r\n",
            b"GET / HTTP/1.1\r\nx-a: 1\r\n  folded\r\n\r\n",
            b"GET / HTTP/1.1\r\nx-a: 1\r\n\tfolded\r\n\r\n",
            b"GET / HTTP/1.1\r\n host: a\r\n\r\n",
        ] {
            let error = refused(bytes);
            assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{error:?}");
        }
    }

    #[test]
    fn a_request_line_that_cannot_become_one_is_refused_as_soon_as_it_shows() {
        for (bytes, shown_at) in [
            // A TLS handshake to a plaintext port: its first byte is no method.
            (&b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03"[..], 1),
            (b" GET / HTTP/1.1\r\n\r\n", 1),
            (b"GE(T / HTTP/1.1\r\n\r\n", 3),
            (b"GET  / HTTP/1.1\r\n\r\n", 5),
            (b"GET /a\x01 HTTP/1.1\r\n\r\n", 7),
            (b"GET /a\x7f HTTP/1.1\r\n\r\n", 7),
            (b"GET / http/1.1\r\n\r\n", 7),
            (b"GET / HTTP/1,1\r\n\r\n", 13),
            (b"GET / HTTP/1.1 \r\n\r\n", 15),
            (b"GET /\r\n\r\n", 6),
        ] {
            assert!(matches!(refused(bytes), RequestError::Malformed(_)));
            let mut reader = HeadReader::default();
            for end in 1..shown_at {
                assert_eq!(
                    reader.read(&bytes[..end], &limits()),
                    Ok(Head::More),
                    "{:?} refused before byte {shown_at}",
                    String::from_utf8_lossy(bytes)
                );
            }
            assert!(
                reader.read(&bytes[..shown_at], &limits()).is_err(),
                "{:?} not refused at byte {shown_at}",
                String::from_utf8_lossy(bytes)
            );
        }
    }

    #[test]
    fn a_version_this_does_not_speak_is_505() {
        for bytes in [
            &b"GET / HTTP/1.2\r\n\r\n"[..],
            b"GET / HTTP/2.0\r\n\r\n",
            b"GET / HTTP/0.9\r\n\r\n",
            // The HTTP/2 preface, should it ever reach this reader.
            b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
        ] {
            let error = refused(bytes);
            assert_eq!(error, RequestError::Version);
            assert_eq!(error.status(), StatusCode::HTTP_VERSION_NOT_SUPPORTED);
        }
    }

    #[test]
    fn a_request_line_past_its_bound_is_414_and_one_at_it_is_read() {
        let limits = limits();
        let fits = |line: usize| {
            // `GET /` + padding + ` HTTP/1.1\r\n` comes to `line` bytes.
            let padding = line - b"GET / HTTP/1.1\r\n".len();
            format!("GET /{} HTTP/1.1\r\n\r\n", "a".repeat(padding)).into_bytes()
        };
        assert!(matches!(
            read(&fits(limits.request_line)),
            Ok(Head::Read { .. })
        ));
        let error = refused(&fits(limits.request_line + 1));
        assert_eq!(
            error,
            RequestError::LineTooLong {
                limit: limits.request_line
            }
        );
        assert_eq!(error.status(), StatusCode::URI_TOO_LONG);
        // And without waiting for the line to end.
        let unending = vec![b'a'; limits.request_line + 1];
        assert_eq!(refused(&unending), error);
    }

    #[test]
    fn a_head_past_its_bound_is_431_and_one_at_it_is_read() {
        let limits = limits();
        let sized = |total: usize| {
            let start = b"GET / HTTP/1.1\r\nx: ";
            let end = b"\r\n\r\n";
            let mut bytes = start.to_vec();
            bytes.resize(total - end.len(), b'v');
            bytes.extend_from_slice(end);
            bytes
        };
        assert!(matches!(read(&sized(limits.head)), Ok(Head::Read { .. })));
        let error = refused(&sized(limits.head + 1));
        assert_eq!(error, RequestError::HeadTooLong { limit: limits.head });
        assert_eq!(error.status(), StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    }

    #[test]
    fn more_fields_than_a_head_may_carry_is_431() {
        let many = |count: usize| {
            let mut bytes = b"GET / HTTP/1.1\r\n".to_vec();
            for at in 0..count {
                bytes.extend_from_slice(format!("x-{at}: v\r\n").as_bytes());
            }
            bytes.extend_from_slice(b"\r\n");
            bytes
        };
        let limits = limits();
        assert!(matches!(read(&many(limits.fields)), Ok(Head::Read { .. })));
        let error = refused(&many(limits.fields + 1));
        assert_eq!(
            error,
            RequestError::TooManyFields {
                limit: limits.fields
            }
        );
        assert_eq!(error.status(), StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    }

    /// RFC 9112 §6.3 rule 5: an invalid length MUST be answered 400. Two that agree are
    /// refused all the same: what sent two may be two things.
    #[test]
    fn a_length_that_cannot_be_trusted_is_refused() {
        for (fields, expected) in [
            (
                "content-length: 5\r\ncontent-length: 5\r\n",
                RequestError::RepeatedLength,
            ),
            ("content-length: 5, 5\r\n", RequestError::BadLength),
            ("content-length: +5\r\n", RequestError::BadLength),
            ("content-length: -1\r\n", RequestError::BadLength),
            ("content-length: 0x5\r\n", RequestError::BadLength),
            ("content-length: \r\n", RequestError::BadLength),
            (
                "content-length: 18446744073709551616\r\n",
                RequestError::BadLength,
            ),
        ] {
            let bytes = format!("POST / HTTP/1.1\r\n{fields}\r\n");
            assert_eq!(refused(bytes.as_bytes()), expected, "{fields:?}");
            assert_eq!(expected.status(), StatusCode::BAD_REQUEST);
        }
        assert_eq!(
            head(b"POST / HTTP/1.1\r\ncontent-length:  7 \r\n\r\n").content_length,
            Some(7)
        );
    }

    fn arrived(fields: &str, version: &str) -> Result<Arrival, RequestError> {
        let bytes = format!("POST / {version}\r\n{fields}\r\n");
        let head = head(bytes.as_bytes());
        arrival(&head, &head.fields.view(bytes.as_bytes()))
    }

    fn framed(fields: &str) -> Result<Framing, RequestError> {
        arrived(fields, "HTTP/1.1").map(|arrival| arrival.framing)
    }

    #[test]
    fn a_body_is_framed_by_its_coding_or_its_length_or_is_none() {
        assert_eq!(framed(""), Ok(Framing::None));
        assert_eq!(framed("content-length: 0\r\n"), Ok(Framing::Length(0)));
        assert_eq!(framed("content-length: 12\r\n"), Ok(Framing::Length(12)));
        assert_eq!(
            framed("transfer-encoding: chunked\r\n"),
            Ok(Framing::Chunked)
        );
        assert_eq!(
            framed("transfer-encoding: CHUNKED\r\n"),
            Ok(Framing::Chunked)
        );
        // RFC 9110 §5.6.1.2: empty list members are not members.
        assert_eq!(
            framed("transfer-encoding: , chunked,\r\n"),
            Ok(Framing::Chunked)
        );
    }

    #[test]
    fn a_coding_that_leaves_the_end_of_the_body_in_doubt_is_refused() {
        for (fields, expected) in [
            ("transfer-encoding: gzip\r\n", RequestError::NotChunkedLast),
            (
                "transfer-encoding: chunked, gzip\r\n",
                RequestError::NotChunkedLast,
            ),
            ("transfer-encoding: \r\n", RequestError::NotChunkedLast),
            ("transfer-encoding: ,\r\n", RequestError::NotChunkedLast),
            (
                "transfer-encoding: chunked;x=y\r\n",
                RequestError::NotChunkedLast,
            ),
            (
                "transfer-encoding: chunked, chunked\r\n",
                RequestError::ChunkedTwice,
            ),
            (
                "transfer-encoding: chunked\r\ntransfer-encoding: chunked\r\n",
                RequestError::ChunkedTwice,
            ),
            (
                "transfer-encoding: chunked\r\ncontent-length: 5\r\n",
                RequestError::LengthAndCoding,
            ),
            (
                "content-length: 5\r\ntransfer-encoding: gzip, chunked\r\n",
                RequestError::LengthAndCoding,
            ),
            (
                "transfer-encoding: g(zip, chunked\r\n",
                RequestError::Malformed("a transfer coding is not a token"),
            ),
        ] {
            let error = framed(fields).unwrap_err();
            assert_eq!(error, expected, "{fields:?}");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{fields:?}");
        }
    }

    /// RFC 9112 §6.1: a coding not understood SHOULD be 501 — here only where `chunked`
    /// is last, so that the body could have been delimited.
    #[test]
    fn a_coding_before_chunked_is_501() {
        for fields in [
            "transfer-encoding: gzip, chunked\r\n",
            "transfer-encoding: gzip\r\ntransfer-encoding: chunked\r\n",
        ] {
            let error = framed(fields).unwrap_err();
            assert_eq!(error, RequestError::CodingNotUnderstood, "{fields:?}");
            assert_eq!(error.status(), StatusCode::NOT_IMPLEMENTED);
        }
    }

    #[test]
    fn a_coding_on_http_1_0_is_refused() {
        for fields in [
            "transfer-encoding: chunked\r\n",
            "transfer-encoding: gzip\r\n",
        ] {
            assert_eq!(
                arrived(fields, "HTTP/1.0"),
                Err(RequestError::CodingOnHttp10),
                "{fields:?}"
            );
        }
    }

    #[test]
    fn a_connection_persists_as_its_version_and_its_request_say() {
        let persistent = |fields: &str, version: &str| {
            arrived(fields, version).map(|arrival| arrival.persistent)
        };
        assert_eq!(persistent("", "HTTP/1.1"), Ok(true));
        assert_eq!(persistent("connection: close\r\n", "HTTP/1.1"), Ok(false));
        assert_eq!(
            persistent("connection: x-a, CLOSE\r\n", "HTTP/1.1"),
            Ok(false)
        );
        assert_eq!(persistent("", "HTTP/1.0"), Ok(false));
        assert_eq!(
            persistent("connection: keep-alive\r\n", "HTTP/1.0"),
            Ok(true)
        );
        assert_eq!(
            persistent("connection: keep-alive, close\r\n", "HTTP/1.0"),
            Ok(false)
        );
        assert_eq!(
            persistent("connection: a b\r\n", "HTTP/1.1"),
            Err(RequestError::BadConnection)
        );
    }

    /// Everything above, and more, cut at every byte: the answer never depends on where
    /// a connection happened to split what it carried.
    #[test]
    fn every_split_of_a_head_gives_the_same_answer() {
        let long_line = format!("GET /{} HTTP/1.1\r\n\r\n", "a".repeat(9000));
        let long_head = format!("GET / HTTP/1.1\r\nx: {}\r\n\r\n", "v".repeat(70000));
        let cases: Vec<&[u8]> = vec![
            b"GET / HTTP/1.1\r\nhost: a\r\n\r\n",
            b"\r\nGET / HTTP/1.1\r\n\r\n",
            b"\r\n\r\nGET / HTTP/1.1\r\n\r\n",
            b"GET / HTTP/1.1\r\nhost: a\n\r\n",
            b"GET / HTTP/2.0\r\n\r\n",
            b"GET / HTTP/1.1\r\nhost : a\r\n\r\n",
            b"POST / HTTP/1.1\r\ncontent-length: 5\r\ncontent-length: 5\r\n\r\n",
            b"\x16\x03\x01",
            long_line.as_bytes(),
            long_head.as_bytes(),
        ];
        for bytes in cases {
            let _answer = by_the_byte(bytes);
        }
    }

    /// A head that arrives a byte at a time is looked over once, not once per arrival:
    /// quadratic work for linear input is a way to hold a worker with very little.
    #[test]
    fn a_head_arriving_a_byte_at_a_time_is_looked_at_once() {
        let bytes = format!(
            "GET /{} HTTP/1.1\r\nx: {}\r\n\r\n",
            "a".repeat(4000),
            "v".repeat(50000)
        );
        let bytes = bytes.as_bytes();
        let mut reader = HeadReader::default();
        for end in 1..=bytes.len() {
            if let Ok(Head::Read { .. }) = reader.read(&bytes[..end], &limits()) {
                break;
            }
        }
        assert!(
            reader.examined <= bytes.len(),
            "{} bytes looked at for {}",
            reader.examined,
            bytes.len()
        );
    }

    /// The pieces heads are made from in the property below: some sound, some not, so that
    /// the reader is split around every kind of refusal as well as every kind of head.
    fn piece() -> impl Strategy<Value = Vec<u8>> {
        prop::sample::select(vec![
            &b"GET"[..],
            b"POST",
            b"G(T",
            b" ",
            b"  ",
            b"/",
            b"/a?b=c",
            b"http://a/b",
            b"*",
            b"\x01",
            b"HTTP/1.1",
            b"HTTP/1.0",
            b"HTTP/2.0",
            b"HTTP/1,1",
            b"\r\n",
            b"\r",
            b"\n",
            b"host: a",
            b"host : a",
            b"x:",
            b"\ty",
            b"content-length: 3",
            b"transfer-encoding: chunked",
            b"\r\n\r\n",
        ])
        .prop_map(<[u8]>::to_vec)
    }

    /// A request line most of the time, so that the rest of the head is reached, and then
    /// whatever the pieces make.
    fn request() -> impl Strategy<Value = Vec<u8>> {
        (
            prop::bool::weighted(0.8),
            prop::collection::vec(piece(), 0..16),
        )
            .prop_map(|(sound_start, pieces)| {
                let mut bytes = if sound_start {
                    b"GET / HTTP/1.1\r\n".to_vec()
                } else {
                    Vec::new()
                };
                for piece in pieces {
                    bytes.extend_from_slice(&piece);
                }
                bytes
            })
    }

    proptest! {
        #[test]
        fn any_split_gives_the_answer_the_whole_gives(bytes in request()) {
            let answer = by_the_byte(&bytes);
            // And a head that was read took no more than there was.
            if let Ok(Head::Read { consumed, .. }) = answer {
                prop_assert!(consumed <= bytes.len());
            }
        }
    }
}
