//! What reading and writing HTTP/1 has in common, whichever side of the gateway a message
//! is on: the length a `Content-Length` gives, bodies delimited by a length or in chunks,
//! and the trailer policy that holds in both directions.
//!
//! Bytes in, an answer out, as in each side's codec: nothing here waits for anything. How a
//! message's body is *framed* is decided by each side for itself, because what a request
//! and a response may say about their bodies differ ([14 §4](../../../docs/14-downstream-server.md)).

// What is here is `pub` because the upstream codec re-exports it, and the upstream module
// is public when the fuzz targets are built. In an ordinary build none of this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::upstream::h1::H1Limits;
use http::{HeaderMap, HeaderName, HeaderValue};
use std::ops::Range;

/// The end of a head, and of a trailer section: an empty line.
pub(crate) const END: &[u8; 4] = b"\r\n\r\n";

/// The most fields any head is read into. A limit may ask for fewer, never for more: the
/// room is taken once, on the stack, so that reading a head allocates nothing.
pub const MOST_FIELDS: usize = 128;

/// Why an HTTP/1 message cannot be read, or cannot be written as asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// The head went past what a head may be without ending.
    #[error("the head is longer than {limit} bytes")]
    HeadTooLong {
        /// What a head may come to.
        limit: usize,
    },
    /// More fields than a head may carry.
    #[error("the head has more than {limit} fields")]
    TooManyFields {
        /// How many fields a head or a trailer section may carry.
        limit: usize,
    },
    /// Not a version this speaks. HTTP/0.9 has no head at all, and HTTP/2 does not begin
    /// like this.
    #[error("the message is not HTTP/1.0 or HTTP/1.1")]
    Version,
    /// The head does not parse, or parses into something that is not a response.
    #[error("the head is malformed: {0}")]
    Malformed(&'static str),
    /// Two lengths, whether or not they agree. Agreeing is not a reason to accept them:
    /// what sent two may be two things, and the one that matters may be the other.
    #[error("the message has more than one content-length")]
    RepeatedLength,
    /// A length that is not DIGIT only, or does not fit. Rust's integer parser takes a
    /// leading sign and HTTP does not.
    #[error("the message has a content-length that is not a plain number")]
    BadLength,
    /// A transfer coding that is not a lone `chunked`: a chain, a repeat, or one this
    /// does not speak.
    #[error("the message has a transfer-encoding that is not a single chunked")]
    Coding,
    /// Both ways of saying how long a body is. Which one a reader believes is what
    /// request smuggling turns on.
    #[error("the message has both a content-length and a transfer-encoding")]
    LengthAndCoding,
    /// Chunked came with HTTP/1.1, so a 1.0 response claiming it is not to be believed.
    #[error("the message is HTTP/1.0 and claims a transfer-encoding")]
    CodingOnHttp10,
    /// An interim answer in HTTP/1.0, which defines none: a peer that writes one is
    /// contradicting itself about which HTTP it speaks.
    #[error("the response is an interim answer in HTTP/1.0, which has none")]
    InterimOnHttp10,
    /// A body described where none may be sent.
    #[error("the response cannot have a body and says how long one would be")]
    BodyForbidden,
    /// 101, which hands the connection to a protocol this does not speak.
    #[error("the response switches to another protocol, which is not supported")]
    Upgrade,
    /// A `Connection` holding something that is not a list of tokens.
    #[error("the message has a connection field that is not a list of tokens")]
    BadConnection,
    /// The connection ended part way through a body that said how long it would be. What
    /// arrived is not the answer, and saying it is would be inventing one.
    #[error("the body ended before all of it had arrived")]
    Truncated,
    /// A chunk that is not one: no size, a size too large to hold, an extension that does
    /// not parse, or something other than CRLF where a chunk ends.
    #[error("the body has a chunk that cannot be read")]
    Chunk,
    /// A chunk's size line went past what such a line may be.
    #[error("a chunk size line is longer than {limit} bytes")]
    ChunkLineTooLong {
        /// What such a line may come to.
        limit: usize,
    },
    /// The trailer section went past what one may be.
    #[error("the trailer section is longer than {limit} bytes")]
    TrailersTooLong {
        /// What a trailer section may come to.
        limit: usize,
    },
    /// More body arrived than a counted body said it would carry.
    #[error("the request body is longer than its content-length")]
    BodyOverran,
    /// A counted body ended before it had sent what it said it would. Sending it as it
    /// stands would be telling the upstream a length that is not the one it will get.
    #[error("the request body is shorter than its content-length")]
    BodyShort,
    /// Something after a body was finished, or a body on a request that has none.
    #[error("there is more of a request body after its end")]
    BodyAfterEnd,
    /// Trailers for a body that has no place to put them.
    #[error("the request body cannot carry trailers")]
    UnexpectedTrailers,
}

/// A `Content-Length`, which is DIGIT and nothing else. No sign, no spaces inside it, no
/// list of lengths that happen to agree, and nothing that does not fit in the count of
/// bytes a body can have.
pub(crate) fn length(value: &[u8]) -> Result<u64, CodecError> {
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
pub(crate) fn reason(error: httparse::Error) -> &'static str {
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
pub enum Framing {
    /// None at all. What a head says a body would have been is not read as one.
    None,
    /// Chunk by chunk, to the zero chunk and the trailer section that follows it.
    Chunked,
    /// Exactly this many bytes.
    Length(u64),
    /// Everything until the connection closes, which is the only thing that ends it.
    UntilClose,
}

/// The names that do not travel on as trailers.
///
/// This is the gateway's forwarding policy, not a register of every field whose own
/// specification forbids it in a trailer section. What is not named here and parses is
/// passed on untouched and uninterpreted — `grpc-status` and the digest fields among
/// them — because a gateway that drops what it does not recognise is a gateway nobody can
/// build on. Nor is the whole of a family denied: `Authentication-Info` is end-to-end and
/// may be a trailer where its scheme allows one.
///
/// Grouped by why each is denied rather than by alphabet, because the reason is the part
/// worth reading. Walked through rather than searched: it is short, and it is only ever
/// consulted for a chunked answer that really carries trailers. The names are lower case,
/// as a parsed field name always is.
pub(crate) const DENIED_TRAILERS: &[&str] = &[
    // Framing and routing. A trailer cannot reach back and change how the message it
    // belongs to was delimited, so these are dropped without their values being read.
    "content-length",
    "host",
    "trailer",
    "transfer-encoding",
    // About the connection this arrived on, which is not the connection it goes out on.
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "upgrade",
    // Credentials and challenges, which belong to a head where they can be acted on.
    "authorization",
    "cookie",
    "proxy-authenticate",
    "proxy-authentication-info",
    "proxy-authorization",
    "set-cookie",
    "www-authenticate",
    // About the content, which a recipient has already begun to act on by now.
    "content-encoding",
    "content-range",
    "content-type",
    // Control of the response, which is likewise decided by the time these could arrive.
    "age",
    "cache-control",
    "date",
    "expires",
    "location",
    "pragma",
    "retry-after",
    "vary",
    "warning",
    // Conditions and expectations, which are a request's business and already settled.
    "expect",
    "if-match",
    "if-modified-since",
    "if-none-match",
    "if-range",
    "if-unmodified-since",
    "max-forwards",
    "range",
];

/// What a trailer section came to.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Trailers {
    /// The ones that travel on, their values as they came.
    pub fields: HeaderMap,
    /// How many were dropped, for a counter to add up. A number and never a name: a
    /// series labelled with what a backend sent is a series a backend can invent.
    pub discarded: usize,
}

/// Whether a field may not travel on as a trailer: one of the named set, or one this
/// message's own `Connection` nominated, which makes it hop-by-hop for this hop alone.
pub(crate) fn is_denied(name: &HeaderName, nominated: &[HeaderName]) -> bool {
    DENIED_TRAILERS.contains(&name.as_str()) || nominated.contains(name)
}

/// Takes out of a trailer section what may not travel on, and says how many went.
///
/// For a body somebody else parsed. What may not be a trailer follows from being an
/// intermediary, not from how the answer was read, so a body the engine's client read is
/// filtered by this same set ([13 §4](../../../docs/13-http1-upstream.md)).
pub fn filter_trailers(fields: &mut HeaderMap, nominated: &[HeaderName]) -> usize {
    // Named first and removed after: the map cannot be read while it is changed, and a
    // name may carry more than one value.
    let denied: Vec<HeaderName> = fields
        .keys()
        .filter(|name| is_denied(name, nominated))
        .cloned()
        .collect();
    let mut discarded = 0;
    for name in denied {
        discarded += fields.get_all(&name).iter().count();
        fields.remove(&name);
    }
    discarded
}

/// Takes the denied names out of a `Trailer` declaration, leaving one that says only what
/// will really arrive, and takes the declaration away altogether when nothing will.
///
/// Doing this to the declaration is not doing it to the trailers: both are filtered, and
/// a permitted trailer that was never declared is still passed on.
pub fn filter_declaration(headers: &mut HeaderMap, nominated: &[HeaderName]) {
    let declared: Vec<HeaderValue> = headers
        .get_all(http::header::TRAILER)
        .iter()
        .cloned()
        .collect();
    if declared.is_empty() {
        return;
    }
    let mut kept: Vec<String> = Vec::new();
    for value in &declared {
        for name in crate::hop_by_hop::options(value) {
            // A name that is no name declares nothing, and goes the way of the rest.
            let Ok(name) = HeaderName::from_bytes(name) else {
                continue;
            };
            if !is_denied(&name, nominated) {
                kept.push(name.as_str().to_owned());
            }
        }
    }
    headers.remove(http::header::TRAILER);
    if kept.is_empty() {
        return;
    }
    if let Ok(value) = HeaderValue::from_str(&kept.join(", ")) {
        headers.insert(http::header::TRAILER, value);
    }
}

/// A piece of a body, as it is read.
#[derive(Debug, PartialEq, Eq)]
pub enum Piece {
    /// Nothing can be said until more bytes arrive.
    More,
    /// Body data: `bytes[data]` of what was given, with `consumed` taken off the front.
    /// The two differ where framing bytes sit around the data, as chunking's do.
    Data {
        /// Where the body data sits in what was given.
        data: Range<usize>,
        /// How many bytes of the front of it are finished with.
        consumed: usize,
    },
    /// The body is whole. `trailers` is `Some` only for a chunked body, which may carry
    /// them; they are frames of their own and are never folded into the head's fields.
    End {
        /// What came after the last chunk, for a body that was chunked.
        trailers: Option<Trailers>,
        /// How many bytes of the front of what was given are finished with.
        consumed: usize,
    },
}

/// Where the reading of a body has got to.
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// Waiting for a chunk's size line.
    Size,
    /// Inside a chunk or a counted body, with this many bytes of it still to come.
    Data { left: u64 },
    /// After a chunk's data, where its own CRLF is.
    AfterChunk,
    /// After the zero chunk, reading fields until an empty line ends them.
    Trailers,
    /// Nothing more is coming.
    Done,
}

/// Reads the body of one response, however that body is delimited.
///
/// Given the bytes that have arrived and whether the connection has ended, it says what it
/// can and how much of the front of the buffer it has finished with. It is told of the end
/// of the connection; it never decides that for itself, because no bytes having arrived is
/// not the same as no bytes ever arriving.
#[derive(Debug)]
pub struct BodyReader {
    framing: Framing,
    state: State,
    /// How much of what is in hand has been searched for the end of the line or section
    /// being read. Without it a peer sending a byte at a time makes every arrival rescan
    /// everything before it, which is quadratic work for linear input and is what §4
    /// means by bounded work per new byte.
    searched: usize,
    /// The names this message's `Connection` nominated. Taken from the head before it was
    /// stripped, because afterwards there is nothing left to take them from.
    nominated: Vec<HeaderName>,
}

impl BodyReader {
    /// A reader for a body delimited as `framing` says.
    pub fn new(framing: Framing) -> Self {
        let state = match framing {
            Framing::None => State::Done,
            Framing::Chunked => State::Size,
            Framing::Length(left) => State::Data { left },
            Framing::UntilClose => State::Data { left: u64::MAX },
        };
        Self {
            framing,
            state,
            searched: 0,
            nominated: Vec::new(),
        }
    }

    /// The same, for a message whose `Connection` nominated these names: they are
    /// hop-by-hop for this hop and do not travel on among its trailers either.
    pub fn nominating(framing: Framing, nominated: Vec<HeaderName>) -> Self {
        Self {
            nominated,
            ..Self::new(framing)
        }
    }

    /// For a counted body, how many of its bytes have still to be read out of it: what an
    /// answer passed on can say of its length before it has all arrived. `None` for a body
    /// delimited any other way, whose length nobody knows until its end.
    pub fn remaining(&self) -> Option<u64> {
        match (self.framing, &self.state) {
            (Framing::Length(_), State::Data { left }) => Some(*left),
            (Framing::Length(_), State::Done) => Some(0),
            _ => None,
        }
    }

    /// Whether the whole body has been read. Only then may the connection be kept.
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Whether every byte of the body has been delivered and only its end is left, with
    /// nothing more wanted from the peer to be sure of it.
    ///
    /// True of a counted body that has had its count. A chunked one is never sure until
    /// it has seen the chunk that says so, and a body the close ends never until it
    /// closes — for those this says no, and the end is found by reading on.
    pub fn is_spent(&self) -> bool {
        matches!(self.framing, Framing::Length(_)) && self.state == State::Data { left: 0 }
    }

    /// Reads what it can from the front of `bytes`. `ended` says the connection has
    /// closed, which only the caller can know.
    ///
    /// # Errors
    ///
    /// A chunk that is not one, a body that ends before it has been delivered in full, or
    /// anything past the bounds in `limits`.
    pub fn read(
        &mut self,
        bytes: &[u8],
        ended: bool,
        limits: &H1Limits,
    ) -> Result<Piece, CodecError> {
        loop {
            return match self.state {
                State::Done => Ok(Piece::End {
                    trailers: None,
                    consumed: 0,
                }),
                // The end of a chunk's data, or of a counted body: a step of its own,
                // which wants no bytes and gives the caller nothing to do.
                State::Data { left: 0 } => match self.framing {
                    Framing::Chunked => {
                        self.state = State::AfterChunk;
                        continue;
                    }
                    _ => {
                        self.state = State::Done;
                        Ok(Piece::End {
                            trailers: None,
                            consumed: 0,
                        })
                    }
                },
                State::Data { left } => self.data(bytes, left, ended),
                State::Size => self.size(bytes, ended, limits),
                State::AfterChunk => self.after_chunk(bytes, ended),
                State::Trailers => self.trailers(bytes, ended, limits),
            };
        }
    }

    /// Body bytes, of a counted body or of one chunk.
    fn data(&mut self, bytes: &[u8], left: u64, ended: bool) -> Result<Piece, CodecError> {
        if bytes.is_empty() {
            if !ended {
                return Ok(Piece::More);
            }
            // The close ends a body that nothing else delimits, and only that one.
            return match self.framing {
                Framing::UntilClose => {
                    self.state = State::Done;
                    Ok(Piece::End {
                        trailers: None,
                        consumed: 0,
                    })
                }
                _ => Err(CodecError::Truncated),
            };
        }
        let take = usize::try_from(left).unwrap_or(usize::MAX).min(bytes.len());
        self.state = State::Data {
            // `take` is no more than `left`, so this cannot go below zero.
            left: left - u64::try_from(take).unwrap_or(u64::MAX),
        };
        Ok(Piece::Data {
            data: 0..take,
            consumed: take,
        })
    }

    /// A chunk's size line: hexadecimal, any extensions, then CRLF.
    fn size(&mut self, bytes: &[u8], ended: bool, limits: &H1Limits) -> Result<Piece, CodecError> {
        let Some(line) = line(bytes, self.searched, limits.chunk_line)? else {
            // Nothing found, so all of it has been looked at; the next arrival starts
            // from here rather than from the beginning.
            self.searched = bytes.len();
            return if ended {
                Err(CodecError::Truncated)
            } else {
                Ok(Piece::More)
            };
        };
        self.searched = 0;
        let (size, rest) = hex(&bytes[..line.text])?;
        extensions(rest)?;
        if size == 0 {
            self.state = State::Trailers;
            return Ok(Piece::Data {
                data: 0..0,
                consumed: line.whole,
            });
        }
        self.state = State::Data { left: size };
        Ok(Piece::Data {
            data: 0..0,
            consumed: line.whole,
        })
    }

    /// The CRLF that follows a chunk's data, and nothing else.
    fn after_chunk(&mut self, bytes: &[u8], ended: bool) -> Result<Piece, CodecError> {
        match bytes {
            [b'\r', b'\n', ..] => {
                self.state = State::Size;
                Ok(Piece::Data {
                    data: 0..0,
                    consumed: 2,
                })
            }
            // Still could become one.
            [] | [b'\r'] if !ended => Ok(Piece::More),
            [] | [b'\r'] => Err(CodecError::Truncated),
            _ => Err(CodecError::Chunk),
        }
    }

    /// The fields after the zero chunk, ending at an empty line. There may be none, which
    /// is an empty line and nothing before it.
    fn trailers(
        &mut self,
        bytes: &[u8],
        ended: bool,
        limits: &H1Limits,
    ) -> Result<Piece, CodecError> {
        let Some(end) = section(bytes, self.searched, limits.trailers)? else {
            // Nothing found, so all of it has been looked at; the next arrival starts
            // from here rather than from the beginning.
            self.searched = bytes.len();
            return if ended {
                Err(CodecError::Truncated)
            } else {
                Ok(Piece::More)
            };
        };
        let trailers = fields(&bytes[..end], &self.nominated, limits)?;
        self.state = State::Done;
        Ok(Piece::End {
            trailers: Some(trailers),
            consumed: end,
        })
    }
}

/// A line of the buffer, if a whole one is there: where its text ends and where the line
/// itself does, the CRLF included.
struct Line {
    text: usize,
    whole: usize,
}

// How many bytes the searches below have looked at. A cursor changes no answer, only how
// much is looked at to reach one, so the test for it counts this rather than any result.
// Test-only: the reader keeps no such number, and nothing outside a test adds to it.
#[cfg(test)]
thread_local! {
    pub(crate) static EXAMINED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Adds what a search is about to look over.
#[cfg(test)]
fn examining(many: usize) {
    EXAMINED.with(|examined| examined.set(examined.get() + many));
}

#[cfg(not(test))]
fn examining(_many: usize) {}

/// The first line of `bytes`, or `None` while it has not ended.
fn line(bytes: &[u8], from: usize, bound: usize) -> Result<Option<Line>, CodecError> {
    examining(bytes.len().saturating_sub(from));
    for (at, byte) in bytes.iter().enumerate().skip(from) {
        if *byte != b'\n' {
            continue;
        }
        if at == 0 || bytes[at - 1] != b'\r' {
            return Err(CodecError::Chunk);
        }
        if at + 1 > bound {
            return Err(CodecError::ChunkLineTooLong { limit: bound });
        }
        return Ok(Some(Line {
            text: at - 1,
            whole: at + 1,
        }));
    }
    if bytes.len() > bound {
        return Err(CodecError::ChunkLineTooLong { limit: bound });
    }
    Ok(None)
}

/// A chunk's size, and whatever follows it on the line. Hexadecimal, at least one digit,
/// and no more than a count of bytes can hold.
fn hex(line: &[u8]) -> Result<(u64, &[u8]), CodecError> {
    let digits = line
        .iter()
        .position(|byte| !byte.is_ascii_hexdigit())
        .unwrap_or(line.len());
    if digits == 0 {
        return Err(CodecError::Chunk);
    }
    let mut size: u64 = 0;
    for byte in &line[..digits] {
        let digit = char::from(*byte).to_digit(16).ok_or(CodecError::Chunk)?;
        size = size
            .checked_mul(16)
            .and_then(|so_far| so_far.checked_add(u64::from(digit)))
            .ok_or(CodecError::Chunk)?;
    }
    Ok((size, &line[digits..]))
}

/// What may follow a chunk's size, which
/// [RFC 9112 §7.1.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-7.1.1) gives as
/// `chunk-ext = *( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )`: `;name`
/// or `;name=value`, over and over, where a value is a token or a quoted string and the
/// bad whitespace between the parts belongs to the grammar.
///
/// None of it is acted on. It is checked because a line that is not read the same way
/// twice is a line two readers can end in two places — and checked against the grammar
/// and nothing stricter, because refusing a line the grammar allows costs a connection
/// for a message that is not wrong.
fn extensions(mut rest: &[u8]) -> Result<(), CodecError> {
    while !rest.is_empty() {
        rest = bad_space(rest)
            .strip_prefix(b";")
            .ok_or(CodecError::Chunk)?;
        let (_name, after) = token(bad_space(rest))?;
        rest = match bad_space(after).strip_prefix(b"=") {
            // No value, so nothing past the name is consumed: whitespace with no mark
            // after it is whitespace the grammar ends without, and the next turn of this
            // loop is what refuses it.
            None => after,
            Some(value) => {
                let value = bad_space(value);
                if value.first() == Some(&b'"') {
                    quoted(value)?
                } else {
                    token(value)?.1
                }
            }
        };
    }
    Ok(())
}

/// Past the bad whitespace an extension's grammar allows between its parts.
fn bad_space(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t'))
        .unwrap_or(bytes.len());
    &bytes[start..]
}

/// One token, which is one or more `tchar`, and what follows it.
fn token(bytes: &[u8]) -> Result<(&[u8], &[u8]), CodecError> {
    let end = bytes
        .iter()
        .position(|byte| !crate::hop_by_hop::is_token_byte(*byte))
        .unwrap_or(bytes.len());
    if end == 0 {
        return Err(CodecError::Chunk);
    }
    Ok(bytes.split_at(end))
}

/// What follows a quoted string, the closing quote included. A backslash makes the next
/// byte part of the string, the closing quote included, which is the point of checking.
///
/// [RFC 9110 §5.6.4](https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.4) keeps
/// control bytes other than HTAB out of both halves of a quoted string, escaped or not.
/// A carriage return that one reader takes for the end of the line and the next does not
/// is exactly what an extension that nobody acts on can still be used to smuggle.
fn quoted(bytes: &[u8]) -> Result<&[u8], CodecError> {
    let mut at = 1;
    while let Some(&byte) = bytes.get(at) {
        match byte {
            b'"' => return Ok(&bytes[at + 1..]),
            // quoted-pair = "\" ( HTAB / SP / VCHAR / obs-text )
            b'\\' => match bytes.get(at + 1) {
                Some(b'\t' | b' '..=b'~' | 0x80..=0xff) => at += 2,
                _ => return Err(CodecError::Chunk),
            },
            // qdtext = HTAB / SP / %x21 / %x23-5B / %x5D-7E / obs-text, the quote and the
            // backslash having been dealt with above.
            b'\t' | b' '..=b'~' | 0x80..=0xff => at += 1,
            _ => return Err(CodecError::Chunk),
        }
    }
    Err(CodecError::Chunk)
}

/// Where a section of fields ends, an empty line included, or `None` while it has not.
fn section(bytes: &[u8], from: usize, bound: usize) -> Result<Option<usize>, CodecError> {
    // No fields at all: the empty line comes first.
    if bytes.starts_with(b"\r\n") {
        return Ok(Some(2));
    }
    // Everything else goes through the search, however little of it there is: a byte
    // passed over here would be counted as searched, and never looked at again.
    examining(bytes.len().saturating_sub(from));
    for at in from..bytes.len() {
        if bytes[at] != b'\n' {
            continue;
        }
        if at == 0 || bytes[at - 1] != b'\r' {
            return Err(CodecError::Malformed("a line ends with a bare newline"));
        }
        if at >= 3 && bytes[at - 2] == b'\n' {
            if at + 1 > bound {
                return Err(CodecError::TrailersTooLong { limit: bound });
            }
            return Ok(Some(at + 1));
        }
    }
    if bytes.len() > bound {
        return Err(CodecError::TrailersTooLong { limit: bound });
    }
    Ok(None)
}

/// The fields of a trailer section, which is a head's fields without a status line.
///
/// Every one of them is parsed and counted before any is dropped, so that a section that
/// is malformed or past its bounds is caught whether or not its fields would have been
/// kept. Only then is the deny set applied, and applying it fails nothing: a message that
/// was framed correctly is not thrown away over a footer that may not travel.
fn fields(
    section: &[u8],
    nominated: &[HeaderName],
    limits: &H1Limits,
) -> Result<Trailers, CodecError> {
    let mut room = [httparse::EMPTY_HEADER; MOST_FIELDS];
    let room = &mut room[..limits.trailer_fields.min(MOST_FIELDS)];
    let parsed = match httparse::parse_headers(section, room) {
        Ok(httparse::Status::Complete((_, parsed))) => parsed,
        Ok(httparse::Status::Partial) => {
            return Err(CodecError::Malformed("trailers are cut short"));
        }
        Err(httparse::Error::TooManyHeaders) => {
            return Err(CodecError::TooManyFields { limit: room.len() });
        }
        Err(error) => return Err(CodecError::Malformed(reason(error))),
    };
    let mut trailers = Trailers {
        fields: HeaderMap::with_capacity(parsed.len()),
        discarded: 0,
    };
    for field in parsed {
        let name = HeaderName::from_bytes(field.name.as_bytes())
            .map_err(|_| CodecError::Malformed("a trailer name is not one"))?;
        let value = HeaderValue::from_bytes(field.value)
            .map_err(|_| CodecError::Malformed("a trailer value is not one"))?;
        if is_denied(&name, nominated) {
            // Dropped without its value being looked at: a length here says nothing about
            // a body whose end has already been found.
            trailers.discarded += 1;
            continue;
        }
        trailers.fields.append(name, value);
    }
    Ok(trailers)
}
