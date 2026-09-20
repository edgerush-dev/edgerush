//! Reading what an upstream says, and saying what EdgeRush means to send.
//!
//! Bytes in, an answer out: nothing here waits for anything, so every rule below can be
//! put to a table of inputs and every one of those inputs can be cut at each of its bytes
//! without the answer changing.
//!
//! **The bytes an upstream sends are not to be trusted.** They come from a backend the
//! control plane named, which is a smaller set than "anyone at all", but a backend that
//! has been taken over is exactly the case this must not fall to; the hazard of this half
//! is a connection left out of step and handed to the next request
//! ([03 §1](../../../docs/03-data-plane.md)). So what a head says is checked before it is
//! believed, and what a permissive parser would allow is not the measure of what is
//! allowed here ([13 §4](../../../docs/13-http1-upstream.md)).

use super::H1Limits;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Version};

/// The end of a head, and of a trailer section: an empty line.
const END: &[u8; 4] = b"\r\n\r\n";

/// The most fields any head is read into. A limit may ask for fewer, never for more: the
/// room is taken once, on the stack, so that reading a head allocates nothing.
pub(crate) const MOST_FIELDS: usize = 128;

/// Why what an upstream sent cannot be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CodecError {
    /// The head went past what a head may be without ending.
    #[error("the response head is longer than {limit} bytes")]
    HeadTooLong { limit: usize },
    /// More fields than a head may carry.
    #[error("the response head has more than {limit} fields")]
    TooManyFields { limit: usize },
    /// Not a version this speaks. HTTP/0.9 has no head at all, and HTTP/2 does not begin
    /// like this.
    #[error("the response is not HTTP/1.0 or HTTP/1.1")]
    Version,
    /// The head does not parse, or parses into something that is not a response.
    #[error("the response head is malformed: {0}")]
    Malformed(&'static str),
    /// Two lengths, whether or not they agree. Agreeing is not a reason to accept them:
    /// what sent two may be two things, and the one that matters may be the other.
    #[error("the response has more than one content-length")]
    RepeatedLength,
    /// A length that is not DIGIT only, or does not fit. Rust's integer parser takes a
    /// leading sign and HTTP does not.
    #[error("the response has a content-length that is not a plain number")]
    BadLength,
    /// A transfer coding that is not a lone `chunked`: a chain, a repeat, or one this
    /// does not speak.
    #[error("the response has a transfer-encoding that is not a single chunked")]
    Coding,
    /// Both ways of saying how long a body is. Which one a reader believes is what
    /// request smuggling turns on.
    #[error("the response has both a content-length and a transfer-encoding")]
    LengthAndCoding,
    /// Chunked came with HTTP/1.1, so a 1.0 response claiming it is not to be believed.
    #[error("the response is HTTP/1.0 and claims a transfer-encoding")]
    CodingOnHttp10,
    /// A body described where none may be sent.
    #[error("the response cannot have a body and says how long one would be")]
    BodyForbidden,
    /// 101, which hands the connection to a protocol this does not speak.
    #[error("the response switches to another protocol, which is not supported")]
    Upgrade,
    /// A `Connection` holding something that is not a list of tokens.
    #[error("the response has a connection field that is not a list of tokens")]
    BadConnection,
}

/// A response head, once it has been read and found sound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResponseHead {
    pub(crate) status: StatusCode,
    pub(crate) version: Version,
    pub(crate) headers: HeaderMap,
    /// The one `Content-Length`, already checked, because a [`HeaderMap`] cannot be asked
    /// afterwards whether there had been two of them.
    pub(crate) content_length: Option<u64>,
}

/// How far reading a head has got.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Head {
    /// Not all of it has arrived. Nothing was consumed; ask again with more.
    More,
    /// A head, and how many bytes of what was given it took. What follows those bytes is
    /// the body, or the next head.
    Read { head: ResponseHead, consumed: usize },
}

/// Reads response heads from bytes as they come.
///
/// The bytes grow: every call is given everything that has arrived so far, the earlier
/// bytes included. What has already been looked at is not looked at again, so a head that
/// arrives one byte at a time costs no more than one that arrives whole.
#[derive(Debug, Default)]
pub(crate) struct HeadReader {
    /// How much of the bytes has been searched for the empty line that ends a head.
    searched: usize,
}

impl HeadReader {
    /// Reads a head from the front of `bytes`, if all of it is there.
    ///
    /// # Errors
    ///
    /// A head that goes past `limits`, is not HTTP/1.0 or HTTP/1.1, does not parse, or
    /// says its length in a way that cannot be trusted.
    pub(crate) fn read(&mut self, bytes: &[u8], limits: &H1Limits) -> Result<Head, CodecError> {
        let Some(end) = self.end_of_head(bytes)? else {
            // Nothing yet, and it may never come: a head that has grown past its bound
            // without ending is not going to end well.
            if bytes.len() > limits.head {
                return Err(CodecError::HeadTooLong { limit: limits.head });
            }
            return Ok(Head::More);
        };
        if end > limits.head {
            return Err(CodecError::HeadTooLong { limit: limits.head });
        }
        let head = parse(&bytes[..end], limits)?;
        Ok(Head::Read {
            head,
            consumed: end,
        })
    }

    /// Where the empty line that ends a head finishes, or `None` while there is none.
    ///
    /// Only what has not been searched before is searched, save for the last three bytes
    /// of it: an empty line can be split across two arrivals, and those three are where
    /// the halves of one would meet. Looking at those three again changes nothing.
    ///
    /// # Errors
    ///
    /// A newline that no carriage return comes before. Every line of a head ends CRLF,
    /// and a lone LF is one of the ways two readers have been brought to disagree about
    /// where a line ends — so it is refused here and now, rather than waited on until the
    /// head outgrows its bound.
    fn end_of_head(&mut self, bytes: &[u8]) -> Result<Option<usize>, CodecError> {
        let from = self.searched.saturating_sub(END.len() - 1);
        let mut end = None;
        for at in from..bytes.len() {
            if bytes[at] != b'\n' {
                continue;
            }
            if at == 0 || bytes[at - 1] != b'\r' {
                return Err(CodecError::Malformed("a line ends with a bare newline"));
            }
            // A second CRLF with nothing between it and the first: the empty line.
            if at >= END.len() - 1 && bytes[at - 2] == b'\n' {
                end = Some(at + 1);
                break;
            }
        }
        self.searched = bytes.len();
        Ok(end)
    }
}

/// Makes a head of the bytes of one, which are known to end with an empty line.
fn parse(head: &[u8], limits: &H1Limits) -> Result<ResponseHead, CodecError> {
    let mut fields = [httparse::EMPTY_HEADER; MOST_FIELDS];
    let room = limits.fields.min(MOST_FIELDS);
    let mut response = httparse::Response::new(&mut fields[..room]);
    // The parser's own settings say no to obsolete line folding, to space before a colon
    // and to the other shapes a lenient reader would take. They are its defaults; they
    // are named here because they are a decision, not a default we happened to get.
    let config = httparse::ParserConfig::default();
    match config.parse_response(&mut response, head) {
        Ok(httparse::Status::Complete(_)) => {}
        // The bytes end with an empty line, so there is nothing more to wait for: what
        // did not parse will not parse.
        Ok(httparse::Status::Partial) => return Err(CodecError::Malformed("it is cut short")),
        Err(httparse::Error::TooManyHeaders) => {
            return Err(CodecError::TooManyFields { limit: room });
        }
        Err(error) => return Err(CodecError::Malformed(reason(error))),
    }

    let version = match response.version {
        Some(0) => Version::HTTP_10,
        Some(1) => Version::HTTP_11,
        _ => return Err(CodecError::Version),
    };
    let code = response
        .code
        .ok_or(CodecError::Malformed("it has no status"))?;
    let status =
        StatusCode::from_u16(code).map_err(|_| CodecError::Malformed("its status is not one"))?;

    // Every field is looked at while they are still apart, because a header map keeps no
    // record of a name having come twice.
    let mut content_length = None;
    let mut headers = HeaderMap::with_capacity(response.headers.len());
    for field in response.headers.iter() {
        let name = HeaderName::from_bytes(field.name.as_bytes())
            .map_err(|_| CodecError::Malformed("a field name is not one"))?;
        let value = HeaderValue::from_bytes(field.value)
            .map_err(|_| CodecError::Malformed("a field value is not one"))?;
        if name == http::header::CONTENT_LENGTH {
            if content_length.is_some() {
                return Err(CodecError::RepeatedLength);
            }
            content_length = Some(length(field.value)?);
        }
        headers.append(name, value);
    }

    Ok(ResponseHead {
        status,
        version,
        headers,
        content_length,
    })
}

/// A `Content-Length`, which is DIGIT and nothing else. No sign, no spaces inside it, no
/// list of lengths that happen to agree, and nothing that does not fit in the count of
/// bytes a body can have.
fn length(value: &[u8]) -> Result<u64, CodecError> {
    let digits = trim(value);
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(CodecError::BadLength);
    }
    let mut length: u64 = 0;
    for digit in digits {
        length = length
            .checked_mul(10)
            .and_then(|so_far| so_far.checked_add(u64::from(digit - b'0')))
            .ok_or(CodecError::BadLength)?;
    }
    Ok(length)
}

/// A field value without the spaces and tabs a sender may put around it.
fn trim(value: &[u8]) -> &[u8] {
    let is_space = |byte: &u8| *byte == b' ' || *byte == b'\t';
    let from = value.iter().position(|byte| !is_space(byte));
    let Some(from) = from else { return &[] };
    let to = value
        .iter()
        .rposition(|byte| !is_space(byte))
        .unwrap_or(from);
    &value[from..=to]
}

/// What a parser's complaint amounts to, in words of our own: its own wording is not
/// something to hand on, and none of it says anything about which upstream it was.
fn reason(error: httparse::Error) -> &'static str {
    match error {
        httparse::Error::HeaderName => "a field name is not one",
        httparse::Error::HeaderValue => "a field value is not one",
        httparse::Error::NewLine => "a line ends wrongly",
        httparse::Error::Status => "its status line is not one",
        httparse::Error::Token => "it has a token that is not one",
        httparse::Error::TooManyHeaders => "it has too many fields",
        httparse::Error::Version => "its version is not one",
    }
}

/// How the body of a response is delimited — the one question every other part of reading
/// one turns on, and the one that request smuggling is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Framing {
    /// None at all. What a head says a body would have been is not read as one.
    None,
    /// Chunk by chunk, to the zero chunk and the trailer section that follows it.
    Chunked,
    /// Exactly this many bytes.
    Length(u64),
    /// Everything until the connection closes, which is the only thing that ends it.
    UntilClose,
}

/// What a head says about the body after it and the connection it came on.
///
/// Of a *final* head: an interim one is followed by more of the same exchange, and what
/// is said here about carrying another exchange does not apply until the final one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Delivery {
    pub(crate) framing: Framing,
    /// Whether this connection may carry another exchange once this one is done. Worked
    /// out here, while `Connection` is still on the head: by the time the hop-by-hop
    /// fields have been taken off there is nothing left to work it out from.
    pub(crate) persistent: bool,
}

/// What was asked, as far as the answer's framing turns on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Asked {
    /// Anything but HEAD: a body is whatever the head says it is.
    Anything,
    /// HEAD: the head describes a body that is not sent.
    Head,
}

/// What follows a head, and whether anything may follow that.
///
/// # Errors
///
/// A transfer coding this does not speak, a length and a coding together, a coding on
/// HTTP/1.0, a body described where none may be, an upgrade, or a `Connection` that is
/// not a list of tokens.
pub(crate) fn delivery(head: &ResponseHead, asked: Asked) -> Result<Delivery, CodecError> {
    let chunked = is_chunked(&head.headers)?;
    // Two ways of saying how long a body is, and no way to know which the sender meant or
    // which the next reader will believe. This is the shape request smuggling is built on.
    if chunked && head.content_length.is_some() {
        return Err(CodecError::LengthAndCoding);
    }
    // Chunked came with HTTP/1.1. A 1.0 sender that claims it is not one to go along with.
    if chunked && head.version == Version::HTTP_10 {
        return Err(CodecError::CodingOnHttp10);
    }
    let closing = says_close(&head.headers)?;
    // A connection that is to close, one that speaks 1.0, or one whose body only the
    // close ends, carries nothing after this.
    let persists =
        |framing| !closing && head.version == Version::HTTP_11 && framing != Framing::UntilClose;

    let status = head.status.as_u16();
    // 101 hands the connection to another protocol, and this speaks none.
    if status == 101 {
        return Err(CodecError::Upgrade);
    }
    // Nothing follows these, and nothing may claim to: a length or a coding here is a
    // sender describing a body it may not send, which the next reader may go looking for.
    if head.status.is_informational() || status == 204 {
        if chunked || head.content_length.is_some() {
            return Err(CodecError::BodyForbidden);
        }
        return Ok(Delivery {
            framing: Framing::None,
            persistent: persists(Framing::None),
        });
    }
    // These describe a body that is not sent. What they say of it has been checked above
    // and is now simply not acted on — 205 is not among them, though it is next to 204.
    if status == 304 || asked == Asked::Head {
        return Ok(Delivery {
            framing: Framing::None,
            persistent: persists(Framing::None),
        });
    }

    let framing = if chunked {
        Framing::Chunked
    } else if let Some(length) = head.content_length {
        Framing::Length(length)
    } else {
        Framing::UntilClose
    };
    Ok(Delivery {
        framing,
        persistent: persists(framing),
    })
}

/// Whether the body is chunked. One `chunked` and nothing besides: a chain of codings, a
/// coding that is not the last, and a `chunked` said twice are all refused, because each
/// is a place where what this reads and what the next reader reads could differ.
fn is_chunked(headers: &HeaderMap) -> Result<bool, CodecError> {
    let mut codings = headers
        .get_all(http::header::TRANSFER_ENCODING)
        .iter()
        .flat_map(crate::hop_by_hop::options);
    let Some(only) = codings.next() else {
        return Ok(false);
    };
    if codings.next().is_some() || !only.eq_ignore_ascii_case(b"chunked") {
        return Err(CodecError::Coding);
    }
    Ok(true)
}

/// Whether `Connection` asks for the connection to close, having first checked that what
/// it holds is a list of tokens at all: a value that is not is not something to act on.
fn says_close(headers: &HeaderMap) -> Result<bool, CodecError> {
    let mut closing = false;
    for value in headers.get_all(http::header::CONNECTION) {
        for option in crate::hop_by_hop::options(value) {
            if !option.iter().copied().all(crate::hop_by_hop::is_token_byte) {
                return Err(CodecError::BadConnection);
            }
            closing |= option.eq_ignore_ascii_case(b"close");
        }
    }
    Ok(closing)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads a head from all of `bytes` at once.
    fn read(bytes: &[u8]) -> Result<Head, CodecError> {
        HeadReader::default().read(bytes, &H1Limits::default())
    }

    /// The head of a read, or a failure of the test.
    fn head_of(bytes: &[u8]) -> ResponseHead {
        match read(bytes) {
            Ok(Head::Read { head, .. }) => head,
            other => panic!("a head was expected, not {other:?}"),
        }
    }

    #[test]
    fn a_head_is_read_with_what_it_says() {
        let head = head_of(b"HTTP/1.1 204 No Content\r\nx-a: 1\r\nx-b: two\r\n\r\n");
        assert_eq!(head.status, StatusCode::NO_CONTENT);
        assert_eq!(head.version, Version::HTTP_11);
        assert_eq!(head.headers["x-a"], "1");
        assert_eq!(head.headers["x-b"], "two");
        assert_eq!(head.content_length, None);
    }

    #[test]
    fn what_follows_a_head_is_left_where_it_is() {
        let bytes = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";
        let Ok(Head::Read { consumed, .. }) = read(bytes) else {
            panic!("a head");
        };
        assert_eq!(consumed, bytes.len() - 5);
        assert_eq!(&bytes[consumed..], b"hello");
    }

    #[test]
    fn a_head_that_is_not_all_there_is_waited_for() {
        let whole = b"HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n";
        for upto in 0..whole.len() {
            assert_eq!(
                HeadReader::default().read(&whole[..upto], &H1Limits::default()),
                Ok(Head::More),
                "{upto} bytes of it"
            );
        }
    }

    /// The bytes of a head may arrive in any pieces at all, an empty line split between
    /// two of them included, and none of that may change what is read.
    #[test]
    fn a_head_read_a_byte_at_a_time_is_the_same_head() {
        for whole in [
            &b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"[..],
            &b"HTTP/1.0 404 Not Found\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nx-a: 1\r\nx-b: 2\r\nx-c: 3\r\n\r\nbody"[..],
        ] {
            let at_once = read(whole);
            let mut reader = HeadReader::default();
            let mut read_at = None;
            for upto in 0..=whole.len() {
                let step = reader.read(&whole[..upto], &H1Limits::default());
                if matches!(step, Ok(Head::Read { .. })) {
                    read_at = Some(upto);
                    assert_eq!(step, at_once, "at {upto} of {whole:?}");
                    break;
                }
                assert_eq!(step, Ok(Head::More), "at {upto} of {whole:?}");
            }
            // And it was read the moment the empty line was whole, not a byte later.
            let ends_at = whole
                .windows(END.len())
                .position(|four| four == END)
                .map(|at| at + END.len());
            assert_eq!(read_at, ends_at, "{whole:?}");
        }
    }

    #[test]
    fn no_byte_of_a_head_is_searched_twice() {
        let whole = b"HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n";
        let mut reader = HeadReader::default();
        for upto in 0..whole.len() {
            let _waiting = reader.read(&whole[..upto], &H1Limits::default());
            // Everything given has been looked at, so the next call starts at its end
            // (less the three bytes an empty line could be split across).
            assert_eq!(reader.searched, upto);
        }
    }

    #[test]
    fn a_head_that_never_ends_is_refused_once_it_is_too_long() {
        let limits = H1Limits {
            head: 64,
            ..H1Limits::default()
        };
        let mut reader = HeadReader::default();
        let short = b"HTTP/1.1 200 OK\r\nx-a: 1\r\n";
        assert_eq!(reader.read(short, &limits), Ok(Head::More));
        let long = [&short[..], &b"x".repeat(64)[..]].concat();
        assert_eq!(
            reader.read(&long, &limits),
            Err(CodecError::HeadTooLong { limit: 64 })
        );
    }

    #[test]
    fn a_head_that_ends_past_the_bound_is_refused() {
        let limits = H1Limits {
            head: 32,
            ..H1Limits::default()
        };
        let bytes = b"HTTP/1.1 200 OK\r\nx-padding: 0123456789\r\n\r\n";
        assert!(bytes.len() > 32);
        assert_eq!(
            HeadReader::default().read(bytes, &limits),
            Err(CodecError::HeadTooLong { limit: 32 })
        );
    }

    #[test]
    fn more_fields_than_are_allowed_is_refused() {
        let limits = H1Limits {
            fields: 2,
            ..H1Limits::default()
        };
        let bytes = b"HTTP/1.1 200 OK\r\nx-a: 1\r\nx-b: 2\r\nx-c: 3\r\n\r\n";
        assert_eq!(
            HeadReader::default().read(bytes, &limits),
            Err(CodecError::TooManyFields { limit: 2 })
        );
        // Right up to the bound is not too many.
        let two = b"HTTP/1.1 200 OK\r\nx-a: 1\r\nx-b: 2\r\n\r\n";
        assert!(matches!(
            HeadReader::default().read(two, &limits),
            Ok(Head::Read { .. })
        ));
    }

    #[test]
    fn only_http_1_0_and_1_1_are_read() {
        assert_eq!(
            head_of(b"HTTP/1.0 200 OK\r\n\r\n").version,
            Version::HTTP_10
        );
        assert_eq!(
            head_of(b"HTTP/1.1 200 OK\r\n\r\n").version,
            Version::HTTP_11
        );
        for other in [
            &b"HTTP/2.0 200 OK\r\n\r\n"[..],
            &b"HTTP/1.2 200 OK\r\n\r\n"[..],
            &b"HTTP/0.9 200 OK\r\n\r\n"[..],
            &b"ICY 200 OK\r\n\r\n"[..],
        ] {
            assert!(read(other).is_err(), "{other:?} was read");
        }
    }

    /// The table of lengths. What is not DIGIT is not a length, however much it looks
    /// like a number to a parser that takes a sign or stops at the first thing it does
    /// not know.
    #[test]
    fn a_length_is_digits_and_nothing_else() {
        let good: &[(&[u8], u64)] = &[
            (b"0", 0),
            (b"5", 5),
            (b"18446744073709551615", u64::MAX),
            (b"  7  ", 7),
            (b"\t7\t", 7),
            (b"007", 7),
        ];
        for (value, expected) in good {
            let bytes = [b"HTTP/1.1 200 OK\r\ncontent-length:", *value, b"\r\n\r\n"].concat();
            assert_eq!(head_of(&bytes).content_length, Some(*expected), "{value:?}");
        }

        let bad: &[&[u8]] = &[
            b"+5", // Rust's own parser takes this; HTTP does not.
            b"-5",
            b"5, 5", // A list, even one that agrees with itself.
            b"5 5",
            b"",
            b"   ",
            b"5x",
            b"x5",
            b"0x5",
            b"5\xa0",
            b"18446744073709551616", // One past what a length can be.
            b"99999999999999999999999999",
        ];
        for value in bad {
            let bytes = [b"HTTP/1.1 200 OK\r\ncontent-length:", *value, b"\r\n\r\n"].concat();
            assert_eq!(
                HeadReader::default().read(&bytes, &H1Limits::default()),
                Err(CodecError::BadLength),
                "{value:?} was taken for a length"
            );
        }
    }

    /// Two lengths are refused whether or not they agree: a head that says a thing twice
    /// may have been written by two hands, and the hand that matters may be the other.
    #[test]
    fn two_lengths_are_refused_even_when_they_agree() {
        for pair in [
            &b"content-length: 5\r\ncontent-length: 5\r\n"[..],
            &b"content-length: 5\r\ncontent-length: 6\r\n"[..],
            &b"content-length: 5\r\nx-a: 1\r\nContent-Length: 5\r\n"[..],
        ] {
            let bytes = [b"HTTP/1.1 200 OK\r\n", pair, b"\r\n"].concat();
            assert_eq!(
                HeadReader::default().read(&bytes, &H1Limits::default()),
                Err(CodecError::RepeatedLength),
                "{pair:?}"
            );
        }
    }

    /// The shapes a lenient reader would take and this one does not. Each is a way one
    /// reader has been made to see a different message from the next.
    #[test]
    fn what_a_lenient_reader_would_take_is_refused() {
        let bad: &[&[u8]] = &[
            b"HTTP/1.1 200 OK\r\nx-a : 1\r\n\r\n", // Space before the colon.
            b"HTTP/1.1 200 OK\r\nx-a: 1\r\n\tfolded\r\n\r\n", // Obsolete folding.
            b"HTTP/1.1 200 OK\r\n x-a: 1\r\n\r\n", // Space before the name.
            b"HTTP/1.1 200 OK\r\nx a: 1\r\n\r\n",  // Space inside the name.
            b"HTTP/1.1 999999 OK\r\n\r\n",         // Not a status.
            b"HTTP/1.1 OK\r\n\r\n",                // No status at all.
            b"HTTP/1.1 200 OK\nx-a: 1\n\n",        // Bare newlines.
        ];
        for bytes in bad {
            assert!(read(bytes).is_err(), "{bytes:?} was read");
        }
    }

    #[test]
    fn a_field_may_come_twice_when_it_is_not_a_length() {
        let bytes = b"HTTP/1.1 200 OK\r\nset-cookie: a=1\r\nset-cookie: b=2\r\n\r\n";
        let head = head_of(bytes);
        let cookies: Vec<&str> = head
            .headers
            .get_all("set-cookie")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert_eq!(cookies, ["a=1", "b=2"]);
    }

    #[test]
    fn an_empty_line_split_across_arrivals_is_still_an_empty_line() {
        // The four bytes of it, cut at each of the three places they can be cut.
        for cut in 1..END.len() {
            let whole = b"HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n";
            let at = whole.len() - END.len() + cut;
            let mut reader = HeadReader::default();
            assert_eq!(
                reader.read(&whole[..at], &H1Limits::default()),
                Ok(Head::More),
                "cut {cut}"
            );
            assert!(
                matches!(
                    reader.read(whole, &H1Limits::default()),
                    Ok(Head::Read { .. })
                ),
                "cut {cut}"
            );
        }
    }

    /// What a head of these fields says about its body, or why it cannot be believed.
    fn delivery_of(fields: &str, asked: Asked) -> Result<Delivery, CodecError> {
        let bytes = format!("HTTP/1.1 200 OK\r\n{fields}\r\n").into_bytes();
        delivery(&head_of(&bytes), asked)
    }

    /// The same, for a head whose status and version are the test's own.
    fn delivery_with(start: &str, fields: &str) -> Result<Delivery, CodecError> {
        let bytes = format!("{start}\r\n{fields}\r\n").into_bytes();
        delivery(&head_of(&bytes), Asked::Anything)
    }

    #[test]
    fn a_coding_wins_over_a_length_being_absent_and_a_length_over_the_close() {
        assert_eq!(
            delivery_of("transfer-encoding: chunked\r\n", Asked::Anything),
            Ok(Delivery {
                framing: Framing::Chunked,
                persistent: true
            })
        );
        assert_eq!(
            delivery_of("content-length: 12\r\n", Asked::Anything),
            Ok(Delivery {
                framing: Framing::Length(12),
                persistent: true
            })
        );
        // Nothing said: the close is what ends it, so there is no next exchange.
        assert_eq!(
            delivery_of("", Asked::Anything),
            Ok(Delivery {
                framing: Framing::UntilClose,
                persistent: false
            })
        );
    }

    #[test]
    fn a_length_and_a_coding_together_are_refused() {
        assert_eq!(
            delivery_of(
                "content-length: 12\r\ntransfer-encoding: chunked\r\n",
                Asked::Anything
            ),
            Err(CodecError::LengthAndCoding)
        );
    }

    /// One `chunked`, and nothing else at all. Each of these is a place where this reader
    /// and the next could part company over where the body ends.
    #[test]
    fn only_a_lone_chunked_coding_is_read() {
        for coding in [
            "gzip",
            "chunked, chunked",
            "chunked, gzip",
            "gzip, chunked",
            "identity",
            "chunked, identity",
            "Chunked, Chunked",
        ] {
            let fields = format!("transfer-encoding: {coding}\r\n");
            assert_eq!(
                delivery_of(&fields, Asked::Anything),
                Err(CodecError::Coding),
                "{coding}"
            );
        }
        // Said over two fields, which is the same list and is refused the same way.
        assert_eq!(
            delivery_of(
                "transfer-encoding: gzip\r\ntransfer-encoding: chunked\r\n",
                Asked::Anything
            ),
            Err(CodecError::Coding)
        );
        // The name's case is nothing to do with it.
        assert!(matches!(
            delivery_of("Transfer-Encoding: CHUNKED\r\n", Asked::Anything),
            Ok(Delivery {
                framing: Framing::Chunked,
                ..
            })
        ));
    }

    #[test]
    fn a_coding_on_http_1_0_is_refused() {
        assert_eq!(
            delivery_with("HTTP/1.0 200 OK", "transfer-encoding: chunked\r\n"),
            Err(CodecError::CodingOnHttp10)
        );
    }

    /// HTTP/1.0 is read, and never kept: this slice pools no connection that speaks it,
    /// whatever it says about keeping alive.
    #[test]
    fn an_http_1_0_response_is_never_kept() {
        assert_eq!(
            delivery_with("HTTP/1.0 200 OK", "content-length: 3\r\n"),
            Ok(Delivery {
                framing: Framing::Length(3),
                persistent: false
            })
        );
        assert_eq!(
            delivery_with(
                "HTTP/1.0 200 OK",
                "content-length: 3\r\nconnection: keep-alive\r\n"
            ),
            Ok(Delivery {
                framing: Framing::Length(3),
                persistent: false
            })
        );
    }

    /// The statuses that carry no body, and the ones that only look as though they do.
    #[test]
    fn the_statuses_that_carry_no_body() {
        for status in ["100 Continue", "103 Early Hints", "204 No Content"] {
            let start = format!("HTTP/1.1 {status}");
            assert_eq!(
                delivery_with(&start, ""),
                Ok(Delivery {
                    framing: Framing::None,
                    persistent: true
                }),
                "{status}"
            );
            // And they may not even describe one.
            for said in ["content-length: 0\r\n", "transfer-encoding: chunked\r\n"] {
                assert_eq!(
                    delivery_with(&start, said),
                    Err(CodecError::BodyForbidden),
                    "{status} with {said}"
                );
            }
        }
    }

    /// 304 says what a body would have been; the answer to a HEAD does the same. What
    /// they say is checked, and then not read as a body.
    #[test]
    fn a_body_that_is_described_and_not_sent() {
        assert_eq!(
            delivery_with("HTTP/1.1 304 Not Modified", "content-length: 99\r\n"),
            Ok(Delivery {
                framing: Framing::None,
                persistent: true
            })
        );
        assert_eq!(
            delivery_of("content-length: 99\r\n", Asked::Head),
            Ok(Delivery {
                framing: Framing::None,
                persistent: true
            })
        );
        // Checked all the same: a coding it cannot read is refused before it is ignored.
        assert_eq!(
            delivery_with("HTTP/1.1 304 Not Modified", "transfer-encoding: gzip\r\n"),
            Err(CodecError::Coding)
        );
        assert_eq!(
            delivery_of("transfer-encoding: gzip\r\n", Asked::Head),
            Err(CodecError::Coding)
        );
    }

    /// 205 sits next to 204 and is not one of the bodiless statuses: read as anything
    /// else is, so a body it sends is not left on the connection for the next request.
    #[test]
    fn a_205_is_not_a_204() {
        assert_eq!(
            delivery_with("HTTP/1.1 205 Reset Content", "content-length: 0\r\n"),
            Ok(Delivery {
                framing: Framing::Length(0),
                persistent: true
            })
        );
        assert_eq!(
            delivery_with("HTTP/1.1 205 Reset Content", ""),
            Ok(Delivery {
                framing: Framing::UntilClose,
                persistent: false
            })
        );
    }

    #[test]
    fn an_upgrade_is_refused_because_none_is_spoken() {
        assert_eq!(
            delivery_with("HTTP/1.1 101 Switching Protocols", "upgrade: websocket\r\n"),
            Err(CodecError::Upgrade)
        );
    }

    #[test]
    fn a_connection_that_asks_to_close_is_not_kept() {
        assert_eq!(
            delivery_of(
                "content-length: 0\r\nconnection: close\r\n",
                Asked::Anything
            ),
            Ok(Delivery {
                framing: Framing::Length(0),
                persistent: false
            })
        );
        // Among other options, and whatever its case.
        assert_eq!(
            delivery_of(
                "content-length: 0\r\nconnection: x-a, CLOSE\r\n",
                Asked::Anything
            ),
            Ok(Delivery {
                framing: Framing::Length(0),
                persistent: false
            })
        );
        // Something else entirely leaves the connection as it was.
        assert_eq!(
            delivery_of(
                "content-length: 0\r\nconnection: keep-alive\r\n",
                Asked::Anything
            ),
            Ok(Delivery {
                framing: Framing::Length(0),
                persistent: true
            })
        );
    }

    #[test]
    fn a_connection_that_is_not_a_list_of_tokens_is_not_acted_on() {
        for value in ["clo se", "\"close\"", "close/1"] {
            let fields = format!("content-length: 0\r\nconnection: {value}\r\n");
            assert_eq!(
                delivery_of(&fields, Asked::Anything),
                Err(CodecError::BadConnection),
                "{value}"
            );
        }
    }
}
