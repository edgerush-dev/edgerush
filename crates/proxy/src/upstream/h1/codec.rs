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
pub use crate::h1::{
    BodyReader, CodecError, Framing, MOST_FIELDS, Piece, Trailers, filter_declaration,
    filter_trailers,
};
#[cfg(test)]
use crate::h1::{DENIED_TRAILERS, EXAMINED};
use crate::h1::{END, is_denied, length, reason};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version};

/// A response head, once it has been read and found sound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHead {
    /// What the upstream answered.
    pub status: StatusCode,
    /// The version it answered in, which is HTTP/1.0 or HTTP/1.1 and nothing else.
    pub version: Version,
    /// Its fields, in the order they came, repeats and all.
    pub headers: HeaderMap,
    /// The one `Content-Length`, already checked, because a [`HeaderMap`] cannot be asked
    /// afterwards whether there had been two of them.
    pub content_length: Option<u64>,
}

/// How far reading a head has got.
#[derive(Debug, PartialEq, Eq)]
pub enum Head {
    /// Not all of it has arrived. Nothing was consumed; ask again with more.
    More,
    /// A head, and how many bytes of what was given it took. What follows those bytes is
    /// the body, or the next head.
    Read {
        /// The head that was read.
        head: ResponseHead,
        /// How many bytes of what was given it took.
        consumed: usize,
    },
}

/// Reads response heads from bytes as they come.
///
/// The bytes grow: every call is given everything that has arrived so far, the earlier
/// bytes included. What has already been looked at is not looked at again, so a head that
/// arrives one byte at a time costs no more than one that arrives whole.
#[derive(Debug, Default)]
pub struct HeadReader {
    /// How much of the bytes has been searched for the empty line that ends a head.
    searched: usize,
    /// Completed nonempty lines, including the status line.
    lines: usize,
}

impl HeadReader {
    /// Reads a head from the front of `bytes`, if all of it is there.
    ///
    /// # Errors
    ///
    /// A head that goes past `limits`, is not HTTP/1.0 or HTTP/1.1, does not parse, or
    /// says its length in a way that cannot be trusted.
    pub fn read(&mut self, bytes: &[u8], limits: &H1Limits) -> Result<Head, CodecError> {
        // Past its first few bytes a head has already been found to start as one.
        if self.searched < STATUS_START {
            status_start(bytes)?;
        }
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
        let head = parse(&bytes[..end], self.lines.saturating_sub(1), limits)?;
        Ok(Head::Read {
            head,
            consumed: end,
        })
    }

    /// Where the empty line that ends a head finishes, or `None` while there is none.
    ///
    /// Each new LF is checked against its preceding bytes, including bytes from the
    /// previous arrival. Earlier bytes need not be searched again.
    ///
    /// # Errors
    ///
    /// A newline that no carriage return comes before. Every line of a head ends CRLF,
    /// and a lone LF is one of the ways two readers have been brought to disagree about
    /// where a line ends — so it is refused here and now, rather than waited on until the
    /// head outgrows its bound.
    fn end_of_head(&mut self, bytes: &[u8]) -> Result<Option<usize>, CodecError> {
        let from = self.searched;
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
            self.lines += 1;
        }
        self.searched = bytes.len();
        Ok(end)
    }
}

/// How many bytes of a head [`status_start`] looks at: `HTTP/1.x`, a space, three digits,
/// and what follows them.
const STATUS_START: usize = 13;

/// Whether what has arrived can still become the start of a status line, which is all of
/// it that is known before the line ends.
///
/// Refusing here rather than at the end of the head is the point: bytes that can never be
/// an answer are not held until a bound or a deadline gives up on them. Nothing may come
/// before the status line — [RFC 9112 §2.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-2.2)
/// lets a server skip empty lines before a request, and gives a client no such leave.
/// After the code comes the space before the reason phrase, or the end of a line that
/// stops at the code, which is read ([13 §5](../../../docs/13-http1-upstream.md)).
fn status_start(bytes: &[u8]) -> Result<(), CodecError> {
    const VERSION: &[u8] = b"HTTP/1.";
    for (at, &byte) in bytes.iter().take(STATUS_START).enumerate() {
        let fits = match at {
            0..7 => byte == VERSION[at],
            7 => byte == b'0' || byte == b'1',
            8 => byte == b' ',
            9..12 => byte.is_ascii_digit(),
            _ => byte == b' ' || byte == b'\r',
        };
        if fits {
            continue;
        }
        return Err(match at {
            0 if byte == b'\r' || byte == b'\n' => {
                CodecError::Malformed("an empty line comes before its status line")
            }
            0..8 => CodecError::Version,
            _ => CodecError::Malformed("its status line cannot become one"),
        });
    }
    Ok(())
}

/// Makes a head of the bytes of one, which are known to end with an empty line.
fn parse(head: &[u8], fields: usize, limits: &H1Limits) -> Result<ResponseHead, CodecError> {
    // Most answers have only a few fields. Do not initialise 128 header slots (4 KiB)
    // on every request; the full bound is still available to larger heads.
    if fields <= 16 {
        parse_with::<16>(head, limits)
    } else {
        parse_with::<MOST_FIELDS>(head, limits)
    }
}

fn parse_with<const N: usize>(head: &[u8], limits: &H1Limits) -> Result<ResponseHead, CodecError> {
    let mut fields = [httparse::EMPTY_HEADER; N];
    let room = limits.fields.min(N);
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
    // `StatusCode` allows 100 to 999, because libraries use the range above HTTP's for
    // errors of their own; HTTP has only 100 to 599, and
    // [RFC 9110 §15](https://www.rfc-editor.org/rfc/rfc9110.html#section-15) says
    // "Values outside the range 100..599 are invalid". A client "SHOULD process the
    // response as if it had a 5xx (Server Error) status code", which is what answering
    // 502 and letting the connection go is.
    if !(100..=599).contains(&code) {
        return Err(CodecError::Malformed(
            "its status is outside the range HTTP has",
        ));
    }
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

/// What a head says about the body after it and the connection it came on.
///
/// Of a *final* head: an interim one is followed by more of the same exchange, and what
/// is said here about carrying another exchange does not apply until the final one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// What delimits the body that follows.
    pub framing: Framing,
    /// Whether this connection may carry another exchange once this one is done. Worked
    /// out here, while `Connection` is still on the head: by the time the hop-by-hop
    /// fields have been taken off there is nothing left to work it out from.
    pub persistent: bool,
}

/// What was asked, as far as the answer's framing turns on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asked {
    /// Anything but HEAD: a body is whatever the head says it is.
    Anything,
    /// HEAD: the head describes a body that is not sent.
    Head,
}

/// What follows a head, and whether anything may follow that.
///
/// # Errors
///
/// A transfer coding this does not speak, a length and a coding together, a coding or an
/// interim answer on HTTP/1.0, a body described where none may be, an upgrade, or a
/// `Connection` that is not a list of tokens.
pub fn delivery(head: &ResponseHead, asked: Asked) -> Result<Delivery, CodecError> {
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
    // HTTP/1.0 defines no 1xx. What such a head means, and which version the rest of the
    // exchange is in, is not something to guess at, so it is refused as policy.
    if head.status.is_informational() && head.version == Version::HTTP_10 {
        return Err(CodecError::InterimOnHttp10);
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

/// Whether the body is chunked. One `chunked` and no other coding: a chain of codings, a
/// coding that is not the last, and a `chunked` said twice are all refused, because each
/// is a place where what this reads and what the next reader reads could differ.
///
/// Empty list members are not codings. `chunked,` and `, chunked` are one `chunked`,
/// because [RFC 9110 §5.6.1.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.1.2)
/// says a recipient MUST accept and ignore a reasonable number of them; every reader that
/// follows it ends the body where this does.
///
/// Decided by whether the field is there, not by what it lists. RFC 9112 §6.3 frames by
/// its presence: one that names no coding at all has no `chunked` last, so the close
/// would end the body and a length beside it would be overruled. Taking it for absent
/// would frame by that length instead, which is the two readers again.
fn is_chunked(headers: &HeaderMap) -> Result<bool, CodecError> {
    let mut fields = headers.get_all(http::header::TRANSFER_ENCODING).iter();
    let Some(first) = fields.next() else {
        return Ok(false);
    };
    let mut codings = std::iter::once(first)
        .chain(fields)
        .flat_map(crate::hop_by_hop::options);
    match (codings.next(), codings.next()) {
        (Some(only), None) if only.eq_ignore_ascii_case(b"chunked") => Ok(true),
        _ => Err(CodecError::Coding),
    }
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

/// How a request's body is to be sent. One choice, made once, and made here: a filter
/// cannot manufacture framing, and whatever the head happens to say about it is left out
/// in favour of this ([13 §1](../../../docs/13-http1-upstream.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sending {
    /// No body. Not the same as a body of nothing: nothing is said about a length at all.
    None,
    /// This many bytes, and exactly this many.
    Length(u64),
    /// Chunked, because how much there is is not known, or because trailers may follow.
    Chunked,
}

/// The target a request is written with. Origin form: the path and query alone. The
/// endpoint is who we are speaking to, not what we are asking for, and a proxy that sends
/// the whole URI is asking for the upstream to treat it as a forward proxy request.
fn origin_form(uri: &Uri) -> &str {
    uri.path_and_query()
        .map_or("/", http::uri::PathAndQuery::as_str)
}

/// The fields of a request as they are to go upstream, whatever holds them: a header map,
/// or a raw head's lines with its edits ([14 §6](../../../docs/14-downstream-server.md)).
/// Read by name as any fields are, and written by the head writer without their
/// `Content-Length` and `Transfer-Encoding`, whose place the framing it writes itself takes.
pub trait OutgoingFields: edgerush_router::Fields {
    /// What writing them comes to: every line, its line break included.
    fn written_len(&self) -> usize;

    /// Appends them to `out`, one line each.
    fn write_fields(&self, out: &mut Vec<u8>);
}

impl OutgoingFields for HeaderMap {
    fn written_len(&self) -> usize {
        self.iter()
            .filter(|(name, _)| !is_framing(name))
            .map(|(name, value)| name.as_str().len() + 2 + value.as_bytes().len() + 2)
            .sum()
    }

    fn write_fields(&self, out: &mut Vec<u8>) {
        for (name, value) in self {
            if is_framing(name) {
                continue;
            }
            out.extend_from_slice(name.as_str().as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// Whether the head writer writes this field itself.
fn is_framing(name: &HeaderName) -> bool {
    name == http::header::CONTENT_LENGTH || name == http::header::TRANSFER_ENCODING
}

/// How many bytes [`write_head`] writes for this head, so that room for them can be paid for
/// before it is made ([14 §8](../../../docs/14-downstream-server.md)).
pub fn head_len<F: OutgoingFields + ?Sized>(
    method: &Method,
    uri: &Uri,
    headers: &F,
    sending: Sending,
) -> usize {
    let line = method.as_str().len() + 1 + origin_form(uri).len() + b" HTTP/1.1\r\n".len();
    let fields = headers.written_len();
    let framing = match sending {
        Sending::None => 0,
        Sending::Length(length) => b"content-length: ".len() + decimal_len(length) + 2,
        Sending::Chunked => b"transfer-encoding: chunked\r\n".len(),
    };
    line + fields + framing + 2
}

/// How many decimal digits `number` is written with.
fn decimal_len(number: u64) -> usize {
    number.checked_ilog10().map_or(1, |log| log as usize + 1)
}

/// Writes the head of a request as an upstream is to receive it, appending to `out`.
///
/// `Content-Length` and `Transfer-Encoding` on the head are left out and `sending` is
/// written instead. The config already refuses a filter that sets either
/// (`edgerush_filters::RESERVED`); this is the backstop, so that what is sent is what was
/// decided and not what something along the way added.
///
/// What it writes is [`head_len`] bytes, which a caller makes room for first; a head over
/// the bound is refused before anything is written.
///
/// # Errors
///
/// A head that would come to more than `limits` allows, or a target that cannot be written
/// in origin form.
pub fn write_head<F: OutgoingFields + ?Sized>(
    out: &mut Vec<u8>,
    method: &Method,
    uri: &Uri,
    headers: &F,
    sending: Sending,
    limits: &H1Limits,
) -> Result<(), CodecError> {
    if head_len(method, uri, headers, sending) > limits.head {
        return Err(CodecError::HeadTooLong { limit: limits.head });
    }
    let target = origin_form(uri);
    out.extend_from_slice(method.as_str().as_bytes());
    out.push(b' ');
    out.extend_from_slice(target.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");
    headers.write_fields(out);
    match sending {
        Sending::None => {}
        Sending::Length(length) => {
            out.extend_from_slice(b"content-length: ");
            let mut digits = itoa(length);
            out.append(&mut digits);
            out.extend_from_slice(b"\r\n");
        }
        Sending::Chunked => out.extend_from_slice(b"transfer-encoding: chunked\r\n"),
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

/// What a trailer section's field lines come to before any is filtered out, without the
/// empty line that ends it.
fn trailer_section(trailers: Option<&HeaderMap>) -> usize {
    trailers
        .into_iter()
        .flatten()
        .map(|(name, value)| name.as_str().len() + 2 + value.as_bytes().len() + 2)
        .sum()
}

/// A number as its decimal digits. Small enough to build backwards on the stack, which
/// keeps formatting off the path a request takes.
fn itoa(mut number: u64) -> Vec<u8> {
    // The largest u64 is twenty digits.
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + u8::try_from(number % 10).unwrap_or(0);
        number /= 10;
        if number == 0 {
            break;
        }
    }
    digits[at..].to_vec()
}

/// Frames the body of a request as it is sent.
///
/// It only ever appends to a buffer; how much of that buffer has reached the socket is the
/// caller's to remember, so a write that went only part way is picked up where it stopped
/// and no byte is written twice.
#[derive(Debug)]
pub struct BodyWriter {
    sending: Sending,
    /// For a counted body: how many bytes are still owed.
    left: u64,
    done: bool,
}

impl BodyWriter {
    /// A writer for a body sent as `sending` says.
    pub fn new(sending: Sending) -> Self {
        Self {
            sending,
            left: match sending {
                Sending::Length(length) => length,
                _ => 0,
            },
            done: false,
        }
    }

    /// Writes one frame of body data.
    ///
    /// # Errors
    ///
    /// More bytes than a counted body said it would have, or anything at all after the
    /// body was finished.
    pub fn data(&mut self, out: &mut Vec<u8>, data: &[u8]) -> Result<(), CodecError> {
        let chunked = self.data_prefix(out, data.len())?;
        out.extend_from_slice(data);
        if chunked {
            out.extend_from_slice(b"\r\n");
        }
        Ok(())
    }

    /// Accounts for payload without copying it. Returns whether a trailing CRLF is owed.
    pub(super) fn data_prefix(
        &mut self,
        out: &mut Vec<u8>,
        size: usize,
    ) -> Result<bool, CodecError> {
        if self.done {
            return Err(CodecError::BodyAfterEnd);
        }
        if size == 0 {
            // Nothing to say, and saying it in chunked would say the opposite: a chunk of
            // no bytes is how a chunked body ends.
            return Ok(false);
        }
        match self.sending {
            Sending::None => Err(CodecError::BodyAfterEnd),
            Sending::Length(_) => {
                let length = u64::try_from(size).unwrap_or(u64::MAX);
                self.left = self
                    .left
                    .checked_sub(length)
                    .ok_or(CodecError::BodyOverran)?;
                Ok(false)
            }
            Sending::Chunked => {
                out.append(&mut hex_digits(size));
                out.extend_from_slice(b"\r\n");
                Ok(true)
            }
        }
    }

    /// Ends the body: nothing for one that was counted, beyond making sure every byte it
    /// promised was sent, and the last chunk with any trailers for one that was chunked.
    ///
    /// # Errors
    ///
    /// A counted body that fell short of what it said, trailers on a body that cannot
    /// carry them, or a second ending.
    pub fn finish(
        &mut self,
        out: &mut Vec<u8>,
        trailers: Option<&HeaderMap>,
        nominated: &[HeaderName],
        limits: &H1Limits,
    ) -> Result<(), CodecError> {
        if self.done {
            return Err(CodecError::BodyAfterEnd);
        }
        self.done = true;
        match self.sending {
            // A body that cannot carry trailers is not given any, and a trailer that
            // arrives for one is a frame with nowhere to go.
            Sending::None | Sending::Length(_) if trailers.is_some_and(|t| !t.is_empty()) => {
                Err(CodecError::UnexpectedTrailers)
            }
            Sending::None => Ok(()),
            Sending::Length(_) => {
                if self.left != 0 {
                    return Err(CodecError::BodyShort);
                }
                Ok(())
            }
            Sending::Chunked => {
                // Counted before any of it is filtered, denied names included: a bound
                // that only counted what survived filtering would be no bound on what
                // arrives ([13 §4](../../../docs/13-http1-upstream.md)). The same
                // numbers as a trailer section that comes the other way.
                let fields = trailers.map_or(0, HeaderMap::len);
                if fields > limits.trailer_fields {
                    return Err(CodecError::TooManyFields {
                        limit: limits.trailer_fields,
                    });
                }
                let section = trailer_section(trailers);
                // The empty line that ends the section is part of it.
                if section + 2 > limits.trailers {
                    return Err(CodecError::TrailersTooLong {
                        limit: limits.trailers,
                    });
                }
                out.extend_from_slice(b"0\r\n");
                for (name, value) in trailers.into_iter().flatten() {
                    // The same set as on the way back: what may not travel as a trailer
                    // may not travel in either direction.
                    if is_denied(name, nominated) {
                        continue;
                    }
                    out.extend_from_slice(name.as_str().as_bytes());
                    out.extend_from_slice(b": ");
                    out.extend_from_slice(value.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                out.extend_from_slice(b"\r\n");
                Ok(())
            }
        }
    }

    /// The most [`BodyWriter::finish`] writes with these trailers, so that room for it can
    /// be paid for before it is made: the last chunk and the whole trailer section, before
    /// any of it is filtered out. Nothing for a body that is not chunked.
    pub fn finish_room(&self, trailers: Option<&HeaderMap>) -> usize {
        match self.sending {
            Sending::Chunked => b"0\r\n".len() + trailer_section(trailers) + 2,
            Sending::None | Sending::Length(_) => 0,
        }
    }

    /// How much room a chunked body needs beside its bytes: a size line and the line
    /// ending around them, and the chunk that says the body is over. Kept back from the
    /// staging buffer, because filling that to the brim with payload and then adding
    /// these is how its bound is quietly gone past
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    ///
    /// Enough for a size line of sixteen hexadecimal digits, which is more than any
    /// staging buffer could ask for, and for the five bytes that end the body.
    pub fn framing_room(&self) -> usize {
        match self.sending {
            Sending::Chunked => 20 + 5,
            Sending::None | Sending::Length(_) => 0,
        }
    }

    /// Whether the body has been ended.
    pub fn is_done(&self) -> bool {
        self.done
    }
}

/// A chunk's size, in the hexadecimal a chunked body writes it in.
fn hex_digits(mut size: usize) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    // The largest usize is sixteen hexadecimal digits.
    let mut digits = [0u8; 16];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = DIGITS[size % 16];
        size /= 16;
        if size == 0 {
            break;
        }
    }
    digits[at..].to_vec()
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
            // Everything given has been looked at, so the next call starts at its end.
            assert_eq!(reader.searched, upto);
        }
    }

    /// A head begins `HTTP/1.0 ` or `HTTP/1.1 ` and three digits, and nothing may come
    /// before it — a leading empty line included, which RFC 9112 §2.2 lets a server skip
    /// before a request and gives no client before a response. Bytes that can no longer
    /// become that are refused as they arrive, not once a head's bound or deadline is spent.
    #[test]
    fn a_start_that_cannot_become_a_status_line_is_refused_at_once() {
        for start in [
            &b"\r"[..],
            b"\r\n",
            b"\n",
            b" ",
            b"X",
            b"http",
            b"HTTP/2",
            b"HTTP/1.2",
            b"HTTP/1.1\t",
            b"HTTP/1.10",
            b"HTTP/1.1  ",
            b"HTTP/1.1 2x",
            b"HTTP/1.1 20 ",
        ] {
            assert!(
                HeadReader::default()
                    .read(start, &H1Limits::default())
                    .is_err(),
                "{:?} was waited on",
                String::from_utf8_lossy(start)
            );
        }
        // Whole heads behind an empty line are refused too, however many there are.
        for whole in [
            &b"\r\nHTTP/1.1 200 OK\r\n\r\n"[..],
            b"\r\n\r\nHTTP/1.1 200 OK\r\n\r\n",
        ] {
            assert!(read(whole).is_err(), "{whole:?} was read");
        }
        // Every prefix of a sound head is still waited on.
        let sound = b"HTTP/1.1 200 OK\r\n\r\n";
        for upto in 0..sound.len() {
            assert_eq!(
                HeadReader::default().read(&sound[..upto], &H1Limits::default()),
                Ok(Head::More),
                "{upto}"
            );
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
    fn small_and_large_header_arrays_obey_the_same_field_limit() {
        for count in [16, 17, MOST_FIELDS, MOST_FIELDS + 1] {
            let head = format!("HTTP/1.1 200 OK\r\n{}\r\n", "x-a: 1\r\n".repeat(count));
            for limit in [15, 16, 17, MOST_FIELDS] {
                let limits = H1Limits {
                    fields: limit,
                    ..H1Limits::default()
                };
                let got = HeadReader::default().read(head.as_bytes(), &limits);
                if count > limit {
                    assert_eq!(got, Err(CodecError::TooManyFields { limit }));
                } else {
                    let Head::Read { head, .. } = got.unwrap() else {
                        panic!("incomplete head")
                    };
                    assert_eq!(head.headers.len(), count);
                }
            }
        }
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
            // A version is spelt one way, and a reader that takes near misses for it
            // (as Envoy's balsa takes `HTTP/9.1` for 1.0) is reading something else.
            &b"http/1.1 200 OK\r\n\r\n"[..],
            &b"HTTP/1.10 200 OK\r\n\r\n"[..],
            &b"HTTP/9.1 200 OK\r\n\r\n"[..],
            &b"HTTP/A.0 200 OK\r\n\r\n"[..],
            &b"HTTPS/1.1 200 OK\r\n\r\n"[..],
            &b"aHTTP/1.1 200 OK\r\n\r\n"[..],
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
            b"5,", // Lists with an empty member, which is still a list.
            b",5",
            b"5,5",
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
            // The status line has one space between its parts. HAProxy and nginx take
            // runs of whitespace; RFC 9112 §4 lets a reader, and this one does not.
            b"HTTP/1.1  200 OK\r\n\r\n",
            b"HTTP/1.1\t200 OK\r\n\r\n",
            b"HTTP/1.1 200OK\r\n\r\n",
            b"HTTP/1.1 200\rOK\r\n\r\n", // A carriage return standing for a space.
            b"HTTP/1.1 403.1 Forbidden\r\n\r\n", // IIS's substatus, which nginx takes.
            b"HTTP/1.1 200 \x00\r\n\r\n", // Control bytes in the reason.
            b"HTTP/1.1 200 O\x7fK\r\n\r\n",
            // A status line where a field should be, which nginx reads past.
            b"HTTP/1.1 200 OK\r\nHTTP/1.1 200 OK\r\n\r\n",
            // Field names that are not tokens, and a line that is no field at all.
            b"HTTP/1.1 200 OK\r\n: v\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nx-foo\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n;\r\n\r\n",
            b"HTTP/1.1 200 OK\r\ncred\x00entials: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nx\xff: 1\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nf\xc3\xb6\xc3\xb6: bar\r\n\r\n",
            // Values with control bytes in them.
            b"HTTP/1.1 200 OK\r\nx-a: 1\x002\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nx-a: 1\r2\r\n\r\n",
        ];
        for bytes in bad {
            assert!(read(bytes).is_err(), "{bytes:?} was read");
        }
        // Each separator RFC 9110 §5.6.2 keeps out of a token, in a name.
        for separator in b"\"(),/;<=>?@[\\]{}\x7f\x80" {
            let bytes = [b"HTTP/1.1 200 OK\r\nx", &[*separator][..], b"a: 1\r\n\r\n"].concat();
            assert!(read(&bytes).is_err(), "{bytes:?} was read");
        }
    }

    /// The other side of the table above: what the grammar allows is read, however
    /// unusual it looks.
    #[test]
    fn what_the_grammar_allows_is_read() {
        let good: &[&[u8]] = &[
            b"HTTP/1.1 103 \r\n\r\n", // An empty reason, which is how 103 is usually sent.
            b"HTTP/1.1 200 O\tK\r\n\r\n",
            b"HTTP/1.1 200 X\xffZ\r\n\r\n", // obs-text is part of a reason.
            b"HTTP/1.1 200 OK\r\nx_foo: 1\r\n\r\n", // An underscore is a tchar.
            b"HTTP/1.1 200 OK\r\n!#$%&'*+-.^_`|~09az: 1\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nserver: hello\tworld\r\n\r\n",
        ];
        for bytes in good {
            assert!(
                matches!(read(bytes), Ok(Head::Read { .. })),
                "{bytes:?} was not read"
            );
        }
        // A value of nothing but whitespace is an empty value.
        assert_eq!(
            head_of(b"HTTP/1.1 200 OK\r\nx-a:   \r\n\r\n").headers["x-a"],
            ""
        );
        assert_eq!(
            head_of(b"HTTP/1.1 200 OK\r\nx-a: \thello\t \r\n\r\n").headers["x-a"],
            "hello"
        );
    }

    /// A value padded far beyond any line a reader might buffer, arriving a byte at a
    /// time, is still one value (from Envoy's protocol integration tests).
    #[test]
    fn a_long_padded_value_arriving_a_byte_at_a_time_is_one_value() {
        let padding = " ".repeat(32 * 1024);
        let whole = format!("HTTP/1.1 200 OK\r\nx-a: v{padding}v\r\n\r\n").into_bytes();
        let mut reader = HeadReader::default();
        let mut head = None;
        for upto in 0..=whole.len() {
            match reader.read(&whole[..upto], &H1Limits::default()) {
                Ok(Head::More) => {}
                Ok(Head::Read { head: read, .. }) => {
                    head = Some(read);
                    break;
                }
                Err(error) => panic!("refused at {upto}: {error:?}"),
            }
        }
        let head = head.expect("a head");
        assert_eq!(head.headers["x-a"], format!("v{padding}v"));
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
            "chunked, gzip, chunked",
            "chunked;q=1", // `chunked` takes no parameters.
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

    /// Empty members of the list are ignored, as RFC 9110 §5.6.1.2 requires of a recipient,
    /// so each of these is a lone `chunked`.
    #[test]
    fn empty_members_beside_a_lone_chunked_are_ignored() {
        for coding in ["chunked,", ", chunked", " , chunked , ,", "chunked,,"] {
            let fields = format!("transfer-encoding: {coding}\r\n");
            assert!(
                matches!(
                    delivery_of(&fields, Asked::Anything),
                    Ok(Delivery {
                        framing: Framing::Chunked,
                        ..
                    })
                ),
                "{coding:?}"
            );
        }
        // The same list over two fields, one of which names nothing.
        assert!(matches!(
            delivery_of(
                "transfer-encoding: chunked\r\ntransfer-encoding: ,\r\n",
                Asked::Anything
            ),
            Ok(Delivery {
                framing: Framing::Chunked,
                ..
            })
        ));
    }

    /// A `Transfer-Encoding` that is there and names nothing is still there. RFC 9112 §6.3
    /// frames by the field's presence — a coding present outranks a length, and one whose
    /// last member is not `chunked` runs to the close — and hyper's client reads these to
    /// the close. Taken for absent, it would frame the body by the length instead, which is two
    /// readers ending one body in two places (found by Envoy, HAProxy and Pingora).
    #[test]
    fn a_coding_that_names_nothing_is_still_a_coding() {
        for value in ["", " ", "\t", ","] {
            let alone = format!("transfer-encoding: {value}\r\n");
            let with_length = format!("{alone}content-length: 5\r\n");
            for (start, fields) in [
                ("HTTP/1.1 200 OK", &alone),
                ("HTTP/1.1 200 OK", &with_length),
                ("HTTP/1.1 204 No Content", &alone),
                ("HTTP/1.0 200 OK", &with_length),
            ] {
                assert!(
                    delivery_with(start, fields).is_err(),
                    "{start} with {fields:?} was framed"
                );
            }
        }
    }

    #[test]
    fn a_coding_on_http_1_0_is_refused() {
        assert_eq!(
            delivery_with("HTTP/1.0 200 OK", "transfer-encoding: chunked\r\n"),
            Err(CodecError::CodingOnHttp10)
        );
    }

    /// HTTP/1.0 has no interim answers, so a peer that writes one in it is contradicting
    /// itself. Refused as policy; the same statuses in 1.1 are read.
    #[test]
    fn an_interim_answer_in_http_1_0_is_refused() {
        for start in [
            "HTTP/1.0 100 Continue",
            "HTTP/1.0 103 Early Hints",
            "HTTP/1.0 199 Whatever",
            "HTTP/1.0 101 Switching Protocols",
        ] {
            assert_eq!(
                delivery_with(start, ""),
                Err(CodecError::InterimOnHttp10),
                "{start}"
            );
        }
        for start in ["HTTP/1.1 100 Continue", "HTTP/1.1 103 Early Hints"] {
            assert!(delivery_with(start, "").is_ok(), "{start}");
        }
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
        for status in [
            "100 Continue",
            "102 Processing",
            "103 Early Hints",
            "104 Upload Resumption Supported",
            "199 Unknown",
            "204 No Content",
        ] {
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
        // A coding it can read is ignored like a length: nothing is waited for, and the
        // connection is kept (the case behind a fix in Pingora, and in nginx's tests).
        let described = Ok(Delivery {
            framing: Framing::None,
            persistent: true,
        });
        assert_eq!(
            delivery_with(
                "HTTP/1.1 304 Not Modified",
                "transfer-encoding: chunked\r\n"
            ),
            described
        );
        assert_eq!(
            delivery_of("transfer-encoding: chunked\r\n", Asked::Head),
            described
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
        // Nor does leaving out what it would switch to make it anything else.
        assert_eq!(
            delivery_with("HTTP/1.1 101 Switching Protocols", ""),
            Err(CodecError::Upgrade)
        );
        let head = head_of(b"HTTP/1.1 101 Switching Protocols\r\n\r\n");
        assert_eq!(delivery(&head, Asked::Head), Err(CodecError::Upgrade));
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

    /// Reads a whole body from `bytes`, a piece at a time, as the exchange would: what it
    /// delivered, and the trailers if there were any.
    fn body_of(
        framing: Framing,
        bytes: &[u8],
        ended: bool,
    ) -> Result<(Vec<u8>, Option<Trailers>), CodecError> {
        read_in_pieces(framing, bytes, ended, &H1Limits::default())
    }

    /// The same, with bounds of the caller's.
    fn read_in_pieces(
        framing: Framing,
        bytes: &[u8],
        ended: bool,
        limits: &H1Limits,
    ) -> Result<(Vec<u8>, Option<Trailers>), CodecError> {
        drive(&mut BodyReader::new(framing), bytes, ended, limits)
    }

    /// Drives a reader the caller made to the end of its body.
    fn to_the_end(
        reader: &mut BodyReader,
        bytes: &[u8],
    ) -> Result<(Vec<u8>, Option<Trailers>), CodecError> {
        drive(reader, bytes, false, &H1Limits::default())
    }

    fn drive(
        reader: &mut BodyReader,
        bytes: &[u8],
        ended: bool,
        limits: &H1Limits,
    ) -> Result<(Vec<u8>, Option<Trailers>), CodecError> {
        let mut left = bytes;
        let mut delivered = Vec::new();
        loop {
            match reader.read(left, ended, limits)? {
                Piece::More => panic!("more was wanted than {bytes:?} holds"),
                Piece::Data { data, consumed } => {
                    delivered.extend_from_slice(&left[data]);
                    left = &left[consumed..];
                }
                Piece::End { trailers, consumed } => {
                    assert!(reader.is_done());
                    left = &left[consumed..];
                    assert!(left.is_empty(), "{left:?} was left over");
                    return Ok((delivered, trailers));
                }
            }
        }
    }

    #[test]
    fn a_counted_body_is_delivered_and_no_more() {
        let (body, trailers) = body_of(Framing::Length(5), b"hello", false).unwrap();
        assert_eq!(body, b"hello");
        assert_eq!(trailers, None);
        // And a counted body of nothing is over before it starts.
        assert_eq!(body_of(Framing::Length(0), b"", false).unwrap().0, b"");
    }

    #[test]
    fn a_body_with_no_framing_at_all_is_over_at_once() {
        assert_eq!(body_of(Framing::None, b"", false).unwrap().0, b"");
    }

    #[test]
    fn a_body_the_close_ends_is_whatever_came_before_it() {
        let mut reader = BodyReader::new(Framing::UntilClose);
        let limits = H1Limits::default();
        let Ok(Piece::Data { data, consumed }) = reader.read(b"some bytes", false, &limits) else {
            panic!("the bytes");
        };
        assert_eq!(&b"some bytes"[data], b"some bytes");
        assert_eq!(consumed, 10);
        // Nothing more has come, and nothing says it will not.
        assert_eq!(reader.read(b"", false, &limits), Ok(Piece::More));
        // Now the connection has closed, and that is the end of the body.
        assert_eq!(
            reader.read(b"", true, &limits),
            Ok(Piece::End {
                trailers: None,
                consumed: 0
            })
        );
    }

    /// A close is the end of a body only where nothing else says where the end is. Where
    /// something does, a close part way through is an answer that never arrived, and
    /// handing on what did arrive would be inventing one.
    #[test]
    fn a_close_part_way_through_a_counted_body_is_a_failure() {
        assert_eq!(
            body_of(Framing::Length(5), b"hel", true),
            Err(CodecError::Truncated)
        );
        assert_eq!(
            body_of(Framing::Chunked, b"5\r\nhel", true),
            Err(CodecError::Truncated)
        );
        assert_eq!(
            body_of(Framing::Chunked, b"5\r\nhello\r\n", true),
            Err(CodecError::Truncated)
        );
        // Even where every byte of the body arrived: the zero chunk did not.
        assert_eq!(
            body_of(Framing::Chunked, b"5\r\nhello\r\n0\r\n", true),
            Err(CodecError::Truncated)
        );
    }

    #[test]
    fn a_chunked_body_is_delivered_without_its_framing() {
        let (body, trailers) = body_of(
            Framing::Chunked,
            b"5\r\nhello\r\n3\r\n th\r\n0\r\n\r\n",
            false,
        )
        .unwrap();
        assert_eq!(body, b"hello th");
        assert_eq!(trailers, Some(Trailers::default()));
    }

    #[test]
    fn a_chunked_body_of_nothing_is_the_zero_chunk_alone() {
        let (body, trailers) = body_of(Framing::Chunked, b"0\r\n\r\n", false).unwrap();
        assert!(body.is_empty());
        assert_eq!(trailers, Some(Trailers::default()));
    }

    #[test]
    fn the_trailers_after_the_last_chunk_are_read_and_kept_apart() {
        let bytes = b"0\r\ngrpc-status: 0\r\ngrpc-message: ok\r\n\r\n";
        let (body, trailers) = body_of(Framing::Chunked, bytes, false).unwrap();
        assert!(body.is_empty());
        let trailers = trailers.unwrap();
        assert_eq!(trailers.fields["grpc-status"], "0");
        assert_eq!(trailers.fields["grpc-message"], "ok");
        assert_eq!(trailers.discarded, 0);
    }

    /// However the bytes are cut up, the body that comes out is the same one.
    #[test]
    fn a_body_arriving_a_byte_at_a_time_is_the_same_body() {
        // A hundred small chunks, as nginx's keepalive tests send them.
        let tiny = [&b"a\r\n0123456789\r\n".repeat(100)[..], b"0\r\n\r\n"].concat();
        let tiny_body = b"0123456789".repeat(100);
        let cases: &[(Framing, &[u8], &[u8])] = &[
            (Framing::Length(5), b"hello", b"hello"),
            (
                Framing::Chunked,
                b"5\r\nhello\r\n3\r\n th\r\n0\r\n\r\n",
                b"hello th",
            ),
            (Framing::Chunked, b"0\r\nx-a: 1\r\n\r\n", b""),
            (Framing::Chunked, &tiny, &tiny_body),
            (Framing::Chunked, b"5\r\nhello\r\n0;a=b\r\n\r\n", b"hello"),
        ];
        for (framing, whole, expected) in cases {
            let mut reader = BodyReader::new(*framing);
            let limits = H1Limits::default();
            let mut delivered = Vec::new();
            let mut taken = 0;
            for upto in 1..=whole.len() {
                loop {
                    let left = &whole[taken..upto];
                    match reader.read(left, false, &limits).unwrap() {
                        Piece::More => break,
                        Piece::Data { data, consumed } => {
                            delivered.extend_from_slice(&left[data]);
                            taken += consumed;
                            if consumed == 0 {
                                break;
                            }
                        }
                        Piece::End { consumed, .. } => {
                            taken += consumed;
                            assert_eq!(taken, whole.len(), "{whole:?}");
                            break;
                        }
                    }
                }
            }
            assert_eq!(delivered, *expected, "{whole:?}");
            assert!(reader.is_done(), "{whole:?}");
        }
    }

    /// The table of chunk size lines. A size that is not hexadecimal, or is more than a
    /// count of bytes can hold, is not a size.
    #[test]
    fn a_chunk_size_is_hexadecimal_and_fits() {
        let good: &[(&[u8], usize)] = &[
            (b"5\r\nhello\r\n0\r\n\r\n", 5),
            (b"A\r\n0123456789\r\n0\r\n\r\n", 10),
            (b"a\r\n0123456789\r\n0\r\n\r\n", 10),
            (b"00000005\r\nhello\r\n0\r\n\r\n", 5),
        ];
        for (bytes, length) in good {
            let (body, _) = body_of(Framing::Chunked, bytes, false).unwrap();
            assert_eq!(body.len(), *length, "{bytes:?}");
        }

        let bad: &[&[u8]] = &[
            b"\r\nhello\r\n",         // No size at all.
            b"-5\r\nhello\r\n",       // Not a hexadecimal digit.
            b"0x5\r\nhello\r\n",      // Nor is this how one is written.
            b" 5\r\nhello\r\n",       // Nor a space before it.
            b"5 \r\nhello\r\n",       // Nor one after it.
            b"10000000000000000\r\n", // One past what a length can be.
            b"5\nhello\n0\n\n",       // Bare newlines.
            // From hyper's decoder tests.
            b"1;reject\nnewlines\r\n",
            b"F\rF\r\n",
            b"1 A\r\n",
            b"1\r\nZ\r\n\r\n\r\n", // The zero chunk's digit missing.
        ];
        for bytes in bad {
            assert_eq!(
                body_of(Framing::Chunked, bytes, false),
                Err(CodecError::Chunk),
                "{bytes:?}"
            );
        }

        // The largest size there is is a size, and a body that then stops short of it is
        // cut short rather than malformed (from Pingora's body tests).
        assert_eq!(
            body_of(Framing::Chunked, b"ffffffffffffffff\r\nAAAA", true),
            Err(CodecError::Truncated)
        );
        // As is a size line the close cuts in half.
        assert_eq!(
            body_of(Framing::Chunked, b"1\r", true),
            Err(CodecError::Truncated)
        );
    }

    /// What may follow a size on its line. None of it is acted on; all of it is checked,
    /// because a line read two ways is a body that ends in two places.
    /// A line that arrives a byte at a time is looked over once, not once for every
    /// byte that follows it. Without a cursor every arrival searches everything in
    /// hand, which is quadratic work for linear input and is what §4 means by
    /// bounded work per new byte.
    #[test]
    fn a_chunk_size_line_is_looked_over_once_however_it_arrives() {
        let limits = H1Limits::default();
        let mut reader = BodyReader::new(Framing::Chunked);
        let line = b"40;padding=xxxxxxxxxxxxxxxxxxxx\r\n";
        EXAMINED.with(|examined| examined.set(0));
        // Everything but the line ending, a byte at a time.
        for at in 1..line.len() {
            assert!(matches!(
                reader.read(&line[..at], false, &limits),
                Ok(Piece::More)
            ));
        }
        let examined = EXAMINED.with(std::cell::Cell::get);
        assert!(
            examined <= line.len(),
            "{examined} bytes looked over for a line of {}: the search starts over",
            line.len()
        );
    }

    /// The same for the fields after the last chunk.
    #[test]
    fn a_trailer_section_is_looked_over_once_however_it_arrives() {
        let limits = H1Limits::default();
        let mut reader = BodyReader::new(Framing::Chunked);
        assert!(matches!(
            reader.read(b"0\r\n", false, &limits),
            Ok(Piece::Data { .. })
        ));
        let section = b"x-a: 1\r\nx-b: 2\r\n\r\n";
        EXAMINED.with(|examined| examined.set(0));
        for at in 1..section.len() {
            assert!(matches!(
                reader.read(&section[..at], false, &limits),
                Ok(Piece::More)
            ));
        }
        let examined = EXAMINED.with(std::cell::Cell::get);
        assert!(
            examined <= section.len(),
            "{examined} bytes looked over for a section of {}: the search starts over",
            section.len()
        );
    }

    /// A trailer section a request sends is bounded before any of it is filtered: a
    /// bound that counted only what survived filtering would be no bound at all on
    /// what a client can make a worker hold
    /// ([13 §4](../../../docs/13-http1-upstream.md)).
    #[test]
    fn a_request_trailer_section_is_bounded_before_it_is_filtered() {
        let limits = H1Limits {
            trailers: 48,
            trailer_fields: 2,
            ..H1Limits::default()
        };
        // A name that may not travel at all, so filtering would leave nothing of it.
        let long = "x".repeat(64);
        let mut fields = HeaderMap::new();
        fields.insert(
            HeaderName::from_static("content-length"),
            HeaderValue::from_str(&long).unwrap(),
        );
        let mut writer = BodyWriter::new(Sending::Chunked);
        assert_eq!(
            writer.finish(&mut Vec::new(), Some(&fields), &[], &limits),
            Err(CodecError::TrailersTooLong { limit: 48 })
        );

        // And how many there are, counted the same way.
        let mut many = HeaderMap::new();
        for name in ["x-a", "x-b", "x-c"] {
            many.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_static("1"),
            );
        }
        let mut writer = BodyWriter::new(Sending::Chunked);
        assert_eq!(
            writer.finish(&mut Vec::new(), Some(&many), &[], &limits),
            Err(CodecError::TooManyFields { limit: 2 })
        );
    }

    #[test]
    fn a_chunk_extension_is_checked_and_then_ignored() {
        let good: &[&[u8]] = &[
            b"5;a\r\nhello\r\n0\r\n\r\n",
            b"5;a=b\r\nhello\r\n0\r\n\r\n",
            b"5;a=\"b\"\r\nhello\r\n0\r\n\r\n",
            b"5;a=\"b;c\"\r\nhello\r\n0\r\n\r\n",
            b"5;a=\"b\\\"c\"\r\nhello\r\n0\r\n\r\n",
            b"5;a;b=c\r\nhello\r\n0\r\n\r\n",
            // Bad whitespace is part of the grammar: RFC 9112 7.1.1 is
            // `chunk-ext = *( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )`.
            b"5 ;a\r\nhello\r\n0\r\n\r\n",
            b"5; a\r\nhello\r\n0\r\n\r\n",
            b"5;a =b\r\nhello\r\n0\r\n\r\n",
            b"5;a= b\r\nhello\r\n0\r\n\r\n",
            b"5 ; a = \"b\" ;c\r\nhello\r\n0\r\n\r\n",
            // A quoted string may hold HTAB and obs-text, bare or escaped.
            b"5;a=\"b\tc\"\r\nhello\r\n0\r\n\r\n",
            b"5;a=\"\x80\"\r\nhello\r\n0\r\n\r\n",
            b"5;a=\"\\\x80\"\r\nhello\r\n0\r\n\r\n",
            // The last chunk may carry extensions too, checked the same way.
            b"5\r\nhello\r\n0;a=b\r\n\r\n",
            b"5\r\nhello\r\n000;x\r\n\r\n",
        ];
        for bytes in good {
            let (body, _) = body_of(Framing::Chunked, bytes, false).unwrap();
            assert_eq!(body, b"hello", "{bytes:?}");
        }

        let bad: &[&[u8]] = &[
            b"5;\r\nhello\r\n",      // A semicolon naming nothing.
            b"5;a=\r\nhello\r\n",    // A name with nothing after the equals.
            b"5;a=\"b\r\nhello\r\n", // A quoted string that never closes.
            b"5;a \r\nhello\r\n",    // Whitespace the grammar ends without.
            b"5;a b\r\nhello\r\n",
            b"5x;a\r\nhello\r\n",
            b"5\r\nhello\r\n0;=v\r\n\r\n", // On the last chunk as on any other.
            b"5\r\nhello\r\n0;\"x\"\r\n\r\n",
        ];
        for bytes in bad {
            assert_eq!(
                body_of(Framing::Chunked, bytes, false),
                Err(CodecError::Chunk),
                "{bytes:?}"
            );
        }
    }

    /// A quoted string is `qdtext` and `quoted-pair` (RFC 9110 §5.6.4): no control byte
    /// but HTAB in either, and an escape takes only HTAB, SP, VCHAR or obs-text. A bare
    /// carriage return is what one reader takes for the end of a line and the next does
    /// not, which is the chunk-extension smuggling shape (found by Envoy and HAProxy).
    #[test]
    fn a_quoted_extension_holds_no_control_bytes() {
        for bytes in [
            &b"5;a=\"\x00\"\r\nhello\r\n0\r\n\r\n"[..],
            &b"5;a=\"\x01\"\r\nhello\r\n0\r\n\r\n"[..],
            &b"5;a=\"b\x7f\"\r\nhello\r\n0\r\n\r\n"[..],
            &b"5;a=\"\\\x00\"\r\nhello\r\n0\r\n\r\n"[..],
            &b"5;a=\"\\\r\"\r\nhello\r\n0\r\n\r\n"[..],
        ] {
            assert_eq!(
                body_of(Framing::Chunked, bytes, false),
                Err(CodecError::Chunk),
                "{bytes:?}"
            );
        }
    }

    #[test]
    fn a_chunk_that_does_not_end_in_crlf_is_refused() {
        for bytes in [
            &b"5\r\nhelloX\r\n0\r\n\r\n"[..], // Something else after the data.
            &b"5\r\nhello\n0\r\n\r\n"[..],    // A bare newline after it.
            &b"5\r\nhello0\r\n\r\n"[..],      // Nothing at all after it.
            &b"1\r\na\rn0\r\n\r\n"[..],       // A carriage return and something else.
        ] {
            assert_eq!(
                body_of(Framing::Chunked, bytes, false),
                Err(CodecError::Chunk),
                "{bytes:?}"
            );
        }
    }

    #[test]
    fn a_chunk_size_line_past_its_bound_is_refused() {
        let limits = H1Limits {
            chunk_line: 16,
            ..H1Limits::default()
        };
        let padded = [b"5;", &b"a".repeat(32)[..], b"\r\nhello\r\n0\r\n\r\n"].concat();
        assert_eq!(
            read_in_pieces(Framing::Chunked, &padded, false, &limits),
            Err(CodecError::ChunkLineTooLong { limit: 16 })
        );
        // Leading zeros count against it too: a size is not exempt for being small.
        let zeros = [&b"0".repeat(32)[..], b"5\r\nhello\r\n0\r\n\r\n"].concat();
        assert_eq!(
            read_in_pieces(Framing::Chunked, &zeros, false, &limits),
            Err(CodecError::ChunkLineTooLong { limit: 16 })
        );
    }

    #[test]
    fn a_trailer_section_past_its_bound_is_refused() {
        let limits = H1Limits {
            trailers: 16,
            ..H1Limits::default()
        };
        let padded = [b"0\r\nx-a: ", &b"1".repeat(32)[..], b"\r\n\r\n"].concat();
        assert_eq!(
            read_in_pieces(Framing::Chunked, &padded, false, &limits),
            Err(CodecError::TrailersTooLong { limit: 16 })
        );
    }

    #[test]
    fn more_trailers_than_are_allowed_is_refused() {
        let limits = H1Limits {
            trailer_fields: 1,
            ..H1Limits::default()
        };
        let bytes = b"0\r\nx-a: 1\r\nx-b: 2\r\n\r\n";
        assert_eq!(
            read_in_pieces(Framing::Chunked, bytes, false, &limits),
            Err(CodecError::TooManyFields { limit: 1 })
        );
    }

    /// Right up to the trailer bounds is not past them.
    #[test]
    fn a_trailer_section_right_at_its_bounds_is_read() {
        let limits = H1Limits::default();
        let many = |count: usize| {
            let fields: String = (0..count).map(|at| format!("x-{at}: 1\r\n")).collect();
            format!("0\r\n{fields}\r\n").into_bytes()
        };
        let (_, trailers) = body_of(Framing::Chunked, &many(limits.trailer_fields), false).unwrap();
        assert_eq!(trailers.unwrap().fields.len(), limits.trailer_fields);
        assert_eq!(
            body_of(Framing::Chunked, &many(limits.trailer_fields + 1), false),
            Err(CodecError::TooManyFields {
                limit: limits.trailer_fields
            })
        );

        // A section of exactly the bound, its empty line included, and one byte more.
        let sized = |length: usize| {
            let value = "v".repeat(length - "x-a: \r\n\r\n".len());
            format!("0\r\nx-a: {value}\r\n\r\n").into_bytes()
        };
        assert!(body_of(Framing::Chunked, &sized(limits.trailers), false).is_ok());
        assert_eq!(
            body_of(Framing::Chunked, &sized(limits.trailers + 1), false),
            Err(CodecError::TrailersTooLong {
                limit: limits.trailers
            })
        );
    }

    /// Trailer lines that are not fields. Each fails the body, and with it the connection
    /// (from HAProxy's `http_transfer_encoding.vtc`, Envoy's and hyper's decoder tests).
    #[test]
    fn a_trailer_section_that_is_not_fields_is_refused() {
        for bytes in [
            &b"0\r\nno-colon\r\n\r\n"[..],
            &b"0\r\nx tlr: value\r\n\r\n"[..],
            &b"0\r\n:status: 200\r\n\r\n"[..],
            &b"0\r\n: value\r\n\r\n"[..],
            &b"0\r\nf\xc3\xb6\xc3\xb6: bar\r\n\r\n"[..],
            &b"0\r\nx-a: val\rue\r\n\r\n"[..],
            &b"0\r\nx-a: \x00\r\n\r\n"[..],
            &b"0\r\nx-a: 1\r\n folded\r\n\r\n"[..],
            &b"0\r\nbad\r\r\n\r\n"[..],
            &b"0\r\nr\n"[..],
            &b"0\r\nabc: hi\r\nr\n"[..],
        ] {
            assert!(
                body_of(Framing::Chunked, bytes, false).is_err(),
                "{bytes:?} was read"
            );
        }
    }

    /// Drives a reader a byte at a time to its end or its first refusal: the refusal, or
    /// `None` if the body was read whole.
    fn refused_a_byte_at_a_time(framing: Framing, whole: &[u8]) -> Option<CodecError> {
        let mut reader = BodyReader::new(framing);
        let limits = H1Limits::default();
        let mut taken = 0;
        for upto in 1..=whole.len() {
            loop {
                match reader.read(&whole[taken..upto], false, &limits) {
                    Err(error) => return Some(error),
                    Ok(Piece::More) => break,
                    Ok(Piece::Data { consumed, .. }) => {
                        taken += consumed;
                        if consumed == 0 {
                            break;
                        }
                    }
                    Ok(Piece::End { .. }) => return None,
                }
            }
        }
        panic!("{whole:?} neither ended nor was refused");
    }

    /// A refusal is the same whatever pieces the bytes arrive in: a body refused whole and
    /// read when it trickles in is a body two readers end in two places (§4).
    #[test]
    fn a_refused_body_is_refused_however_it_arrives() {
        for bytes in [
            &b"5\r\nhelloX\r\n0\r\n\r\n"[..],
            &b"5;a=\r\nhello\r\n0\r\n\r\n"[..],
            &b"5\nhello\r\n0\r\n\r\n"[..],
            &b"0\r\nx-a : 1\r\n\r\n"[..],
            &b"0\r\nx-a: 1\n\r\n"[..],
            &b"0\r\nr\n"[..],
            &b"0\r\nx-a: 1\r\n\n"[..],
        ] {
            assert!(
                body_of(Framing::Chunked, bytes, false).is_err(),
                "{bytes:?} was read whole"
            );
            assert!(
                refused_a_byte_at_a_time(Framing::Chunked, bytes).is_some(),
                "{bytes:?} was read a byte at a time"
            );
        }
    }

    /// A trailer section whose first byte is a bare newline is refused, whether it arrives
    /// whole or that newline arrives on its own. A newline passed over as too little to
    /// search reaches httparse, which ends a section at a leading newline and reads the
    /// rest as nothing — the second of these would be a whole answer swallowed as
    /// "trailers" (found through hyper's decoder tests).
    #[test]
    fn a_bare_newline_opening_a_trailer_section_is_refused_however_it_arrives() {
        for bytes in [
            &b"0\r\n\nx-a: 1\r\n\r\n"[..],
            &b"0\r\n\nHTTP/1.1 200 OK\r\n\r\n"[..],
        ] {
            assert!(
                body_of(Framing::Chunked, bytes, false).is_err(),
                "{bytes:?}"
            );
            assert!(
                refused_a_byte_at_a_time(Framing::Chunked, bytes).is_some(),
                "{bytes:?} was read a byte at a time"
            );
        }
    }

    /// A field said twice among the trailers is kept twice, in order.
    #[test]
    fn a_trailer_said_twice_keeps_both_in_order() {
        let trailers = trailers_of("x-trace: first\r\nx-trace: second\r\n");
        let values: Vec<_> = trailers.fields.get_all("x-trace").iter().collect();
        assert_eq!(values, ["first", "second"]);
    }

    /// The whitespace around a trailer's value is not part of it (HAProxy c12).
    #[test]
    fn a_trailer_value_is_read_without_the_whitespace_around_it() {
        let trailers = trailers_of("x-tlr1: value1\r\nX-Tlr2: \tvalue2\t \r\n");
        assert_eq!(trailers.fields["x-tlr1"], "value1");
        assert_eq!(trailers.fields["x-tlr2"], "value2");
    }

    /// A reader that has finished takes nothing more: what follows the body belongs to
    /// whatever comes next, or to nobody (from hyper's decoder tests).
    #[test]
    fn a_finished_body_takes_no_more_bytes() {
        let limits = H1Limits::default();
        for (framing, bytes) in [
            (Framing::Length(5), &b"hello"[..]),
            (Framing::Chunked, &b"5\r\nhello\r\n0\r\n\r\n"[..]),
        ] {
            let mut reader = BodyReader::new(framing);
            drive(&mut reader, bytes, false, &limits).unwrap();
            for _ in 0..2 {
                assert_eq!(
                    reader.read(b"HTTP/1.1 200 OK\r\n", false, &limits),
                    Ok(Piece::End {
                        trailers: None,
                        consumed: 0
                    }),
                    "{framing:?}"
                );
            }
        }
    }

    /// What a trailer section came to, once what may not travel has been left behind.
    fn trailers_of(section: &str) -> Trailers {
        let bytes = format!("0\r\n{section}\r\n").into_bytes();
        body_of(Framing::Chunked, &bytes, false).unwrap().1.unwrap()
    }

    /// The named set never travels on. None of it fails the message: the body was framed
    /// from the head and read to its end, and a footer that may not be passed on is no
    /// reason to throw away an answer that was correct.
    #[test]
    fn a_denied_trailer_is_dropped_and_the_message_is_not() {
        for denied in [
            "content-length: 5",
            "transfer-encoding: chunked",
            "host: elsewhere.test",
            "trailer: x-a",
            "connection: close",
            "keep-alive: timeout=5",
            "proxy-connection: keep-alive",
            "te: trailers",
            "upgrade: websocket",
            "authorization: Basic abc",
            "proxy-authorization: Basic abc",
            "www-authenticate: Basic realm=x",
            "proxy-authenticate: Basic realm=x",
            "proxy-authentication-info: nextnonce=abc",
            "cookie: a=1",
            "set-cookie: a=1",
            "content-type: text/plain",
            "content-encoding: gzip",
            "content-range: bytes 0-1/2",
            "cache-control: no-store",
            "pragma: no-cache",
            "age: 1",
            "expires: 0",
            "date: Sun, 20 Sep 2026 00:00:00 GMT",
            "location: /elsewhere",
            "retry-after: 1",
            "vary: accept",
            "warning: 199 - x",
            "expect: 100-continue",
            "max-forwards: 1",
            "range: bytes=0-1",
            "if-match: x",
            "if-none-match: x",
            "if-modified-since: Sun, 20 Sep 2026 00:00:00 GMT",
            "if-unmodified-since: Sun, 20 Sep 2026 00:00:00 GMT",
            "if-range: x",
        ] {
            let trailers = trailers_of(&format!("{denied}\r\n"));
            assert!(trailers.fields.is_empty(), "{denied} travelled on");
            assert_eq!(trailers.discarded, 1, "{denied}");
        }
    }

    /// A section somebody else parsed is filtered by the same set, every value of a
    /// repeated name with it.
    #[test]
    fn a_section_read_elsewhere_loses_what_may_not_travel_on() {
        let mut fields = HeaderMap::new();
        fields.append(
            HeaderName::from_static("x-secret"),
            HeaderValue::from_static("a"),
        );
        fields.append(
            HeaderName::from_static("x-secret"),
            HeaderValue::from_static("b"),
        );
        fields.append(
            HeaderName::from_static("content-length"),
            HeaderValue::from_static("7"),
        );
        fields.append(
            HeaderName::from_static("x-kept"),
            HeaderValue::from_static("here"),
        );

        let nominated = vec![HeaderName::from_static("x-secret")];
        assert_eq!(filter_trailers(&mut fields, &nominated), 3);

        assert!(fields.get("x-secret").is_none());
        assert!(fields.get("content-length").is_none());
        assert_eq!(fields.get("x-kept").unwrap(), "here");
    }

    /// Whatever case it is written in.
    #[test]
    fn a_denied_trailer_is_denied_however_it_is_spelt() {
        let trailers = trailers_of("Content-Length: 5\r\nTRANSFER-ENCODING: chunked\r\n");
        assert!(trailers.fields.is_empty());
        assert_eq!(trailers.discarded, 2);
    }

    /// What is not named travels on untouched, and is not read for meaning on the way.
    #[test]
    fn an_application_trailer_travels_on_as_it_came() {
        let trailers = trailers_of("grpc-status: 0\r\nx-checksum: abc\r\n");
        assert_eq!(trailers.fields["grpc-status"], "0");
        assert_eq!(trailers.fields["x-checksum"], "abc");
        assert_eq!(trailers.discarded, 0);
    }

    /// A family is not denied wholesale: `Authentication-Info` is end-to-end and may be a
    /// trailer, though the fields around it in the set are not.
    #[test]
    fn authentication_info_is_not_denied_with_the_rest_of_its_family() {
        let trailers = trailers_of("authentication-info: nextnonce=abc\r\n");
        assert_eq!(trailers.fields["authentication-info"], "nextnonce=abc");
        assert_eq!(trailers.discarded, 0);
    }

    #[test]
    fn some_denied_and_some_not_keeps_only_the_ones_that_may_travel() {
        let trailers = trailers_of("grpc-status: 0\r\ncontent-length: 5\r\nx-a: 1\r\n");
        assert_eq!(trailers.fields.len(), 2);
        assert_eq!(trailers.fields["grpc-status"], "0");
        assert_eq!(trailers.fields["x-a"], "1");
        assert_eq!(trailers.discarded, 1);
    }

    /// What the head's `Connection` named is hop-by-hop for this hop, among the trailers
    /// as much as among the fields.
    #[test]
    fn what_the_head_nominated_does_not_travel_on_either() {
        let nominated = vec![HeaderName::from_static("x-hop")];
        let mut reader = BodyReader::nominating(Framing::Chunked, nominated);
        let bytes = b"0\r\nx-hop: 1\r\nx-a: 1\r\n\r\n";
        let trailers = to_the_end(&mut reader, bytes).unwrap().1.unwrap();
        assert_eq!(trailers.fields.len(), 1);
        assert_eq!(trailers.fields["x-a"], "1");
        assert_eq!(trailers.discarded, 1);
    }

    /// A length among the trailers is dropped without being read. It says nothing about a
    /// body whose end was found from the head, and reading it would be inviting it to.
    #[test]
    fn a_length_among_the_trailers_does_not_touch_the_framing() {
        let bytes = b"5\r\nhello\r\n0\r\ncontent-length: 99999\r\n\r\n";
        let (body, trailers) = body_of(Framing::Chunked, bytes, false).unwrap();
        assert_eq!(body, b"hello");
        let trailers = trailers.unwrap();
        assert!(trailers.fields.is_empty());
        assert_eq!(trailers.discarded, 1);
    }

    /// A section that is malformed or past its bounds still fails, whether or not what it
    /// held would have been kept: everything is counted before anything is dropped.
    #[test]
    fn a_denied_field_is_still_counted_against_the_bounds() {
        let limits = H1Limits {
            trailer_fields: 1,
            ..H1Limits::default()
        };
        // Both would have been dropped; there are still two of them.
        let bytes = b"0\r\ncontent-length: 5\r\ncontent-type: text/plain\r\n\r\n";
        assert_eq!(
            read_in_pieces(Framing::Chunked, bytes, false, &limits),
            Err(CodecError::TooManyFields { limit: 1 })
        );
    }

    #[test]
    fn a_declaration_says_only_what_will_really_arrive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::TRAILER,
            HeaderValue::from_static("grpc-status, content-length, x-a"),
        );
        filter_declaration(&mut headers, &[]);
        assert_eq!(headers[http::header::TRAILER], "grpc-status, x-a");
    }

    #[test]
    fn a_declaration_with_nothing_left_to_say_is_taken_away() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::TRAILER,
            HeaderValue::from_static("content-length, host"),
        );
        filter_declaration(&mut headers, &[]);
        assert!(!headers.contains_key(http::header::TRAILER));
    }

    #[test]
    fn a_declaration_is_filtered_by_what_the_head_nominated_too() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::TRAILER,
            HeaderValue::from_static("x-hop, x-a"),
        );
        filter_declaration(&mut headers, &[HeaderName::from_static("x-hop")]);
        assert_eq!(headers[http::header::TRAILER], "x-a");
    }

    /// Filtering the declaration is not filtering the trailers: one that was never
    /// declared and may travel is passed on all the same.
    #[test]
    fn a_trailer_that_was_never_declared_is_not_refused_for_that() {
        let trailers = trailers_of("x-undeclared: 1\r\n");
        assert_eq!(trailers.fields["x-undeclared"], "1");
        assert_eq!(trailers.discarded, 0);
    }

    /// A parsed field name is lower case, so the set it is compared against must be, and
    /// a name that appears twice is a name whose reason for being denied was unclear.
    #[test]
    fn the_denied_names_are_lower_case_and_said_once() {
        assert!(
            DENIED_TRAILERS
                .iter()
                .all(|name| name.to_lowercase() == *name),
            "{DENIED_TRAILERS:?}"
        );
        let mut once = DENIED_TRAILERS.to_vec();
        once.sort_unstable();
        once.dedup();
        assert_eq!(once.len(), DENIED_TRAILERS.len());
        // Every one of them is a field name the http crate will parse back.
        for name in DENIED_TRAILERS {
            assert!(HeaderName::from_bytes(name.as_bytes()).is_ok(), "{name}");
        }
    }

    /// A head written for an upstream, as text.
    fn written(method: &str, target: &str, fields: &[(&str, &str)], sending: Sending) -> String {
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        let mut out = Vec::new();
        write_head(
            &mut out,
            &Method::from_bytes(method.as_bytes()).unwrap(),
            &target.parse::<Uri>().unwrap(),
            &headers,
            sending,
            &H1Limits::default(),
        )
        .unwrap();
        String::from_utf8(out).unwrap()
    }

    /// The target is the path and query alone. Sending the whole URI is how a request is
    /// addressed to a forward proxy, which an upstream is not.
    #[test]
    fn a_request_is_written_in_origin_form() {
        let head = written("GET", "http://up.test:8080/a/b?c=1", &[], Sending::None);
        assert!(head.starts_with("GET /a/b?c=1 HTTP/1.1\r\n"), "{head}");
        // A target with nothing to it is still a path.
        let head = written("GET", "http://up.test", &[], Sending::None);
        assert!(head.starts_with("GET / HTTP/1.1\r\n"), "{head}");
    }

    #[test]
    fn the_fields_of_the_head_are_written_as_they_are() {
        let head = written(
            "POST",
            "/x",
            &[("host", "shop.test"), ("x-a", "1"), ("x-a", "2")],
            Sending::None,
        );
        assert!(head.contains("host: shop.test\r\n"), "{head}");
        // A field said twice is written twice, in the order it was said.
        assert!(head.contains("x-a: 1\r\nx-a: 2\r\n"), "{head}");
    }

    /// The writer owns the framing. Whatever the head carries about it is left out, so
    /// that what goes on the wire is the one choice that was made.
    #[test]
    fn the_framing_on_the_head_is_replaced_by_the_one_that_was_chosen() {
        let carried = &[
            ("host", "shop.test"),
            ("content-length", "99"),
            ("transfer-encoding", "chunked"),
        ];
        let head = written("POST", "/x", carried, Sending::Length(5));
        assert!(head.contains("content-length: 5\r\n"), "{head}");
        assert!(!head.contains("99"), "{head}");
        assert!(!head.contains("transfer-encoding"), "{head}");

        let head = written("POST", "/x", carried, Sending::Chunked);
        assert!(head.contains("transfer-encoding: chunked\r\n"), "{head}");
        assert!(!head.contains("content-length"), "{head}");

        // No body at all says nothing about a length, which is not the same as saying nil.
        let head = written("GET", "/x", carried, Sending::None);
        assert!(!head.contains("content-length"), "{head}");
        assert!(!head.contains("transfer-encoding"), "{head}");
    }

    #[test]
    fn a_length_of_nothing_is_still_a_length() {
        let head = written("POST", "/x", &[], Sending::Length(0));
        assert!(head.contains("content-length: 0\r\n"), "{head}");
    }

    #[test]
    fn a_head_past_its_bound_is_refused_and_leaves_nothing_behind() {
        let mut out = b"already here".to_vec();
        let limits = H1Limits {
            head: 16,
            ..H1Limits::default()
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-long", HeaderValue::from_static("0123456789abcdef"));
        let written = write_head(
            &mut out,
            &Method::GET,
            &"/x".parse().unwrap(),
            &headers,
            Sending::None,
            &limits,
        );
        assert_eq!(written, Err(CodecError::HeadTooLong { limit: 16 }));
        assert_eq!(out, b"already here");
    }

    /// What a written body comes to, given the frames it was made of.
    fn sent(sending: Sending, frames: &[&[u8]], trailers: Option<&HeaderMap>) -> String {
        let mut writer = BodyWriter::new(sending);
        let mut out = Vec::new();
        for frame in frames {
            writer.data(&mut out, frame).unwrap();
        }
        writer
            .finish(&mut out, trailers, &[], &H1Limits::default())
            .unwrap();
        assert!(writer.is_done());
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn a_counted_body_is_its_bytes_and_nothing_around_them() {
        assert_eq!(
            sent(Sending::Length(8), &[b"hello", b" th"], None),
            "hello th"
        );
        assert_eq!(sent(Sending::Length(0), &[], None), "");
    }

    #[test]
    fn a_chunked_body_is_written_with_framing_of_our_own() {
        assert_eq!(
            sent(Sending::Chunked, &[b"hello", b" th"], None),
            "5\r\nhello\r\n3\r\n th\r\n0\r\n\r\n"
        );
        // A chunk's size is hexadecimal, so sixteen bytes is `10`.
        assert_eq!(
            sent(Sending::Chunked, &[&[b'x'; 16]], None),
            format!("10\r\n{}\r\n0\r\n\r\n", "x".repeat(16))
        );
    }

    #[test]
    fn a_chunked_body_of_nothing_is_the_last_chunk_alone() {
        assert_eq!(sent(Sending::Chunked, &[], None), "0\r\n\r\n");
    }

    /// A frame of no bytes says nothing, and in a chunked body saying it would say the
    /// opposite: a chunk of nothing is how such a body ends.
    #[test]
    fn a_frame_of_no_bytes_ends_nothing() {
        assert_eq!(
            sent(Sending::Chunked, &[b"a", b"", b"b"], None),
            "1\r\na\r\n1\r\nb\r\n0\r\n\r\n"
        );
        assert_eq!(sent(Sending::Length(1), &[b"", b"a", b""], None), "a");
    }

    #[test]
    fn trailers_are_written_after_the_last_chunk() {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        assert_eq!(
            sent(Sending::Chunked, &[b"a"], Some(&trailers)),
            "1\r\na\r\n0\r\ngrpc-status: 0\r\n\r\n"
        );
    }

    /// A trailer said twice goes out twice, in order (hyper's encoder tests).
    #[test]
    fn a_trailer_said_twice_is_written_twice_in_order() {
        let mut trailers = HeaderMap::new();
        trailers.append("x-trace", HeaderValue::from_static("first"));
        trailers.append("x-trace", HeaderValue::from_static("second"));
        assert_eq!(
            sent(Sending::Chunked, &[b"a"], Some(&trailers)),
            "1\r\na\r\n0\r\nx-trace: first\r\nx-trace: second\r\n\r\n"
        );
    }

    /// The same set applies on the way out: what may not travel as a trailer may not
    /// travel in either direction.
    #[test]
    fn a_denied_trailer_is_not_written_either() {
        let mut trailers = HeaderMap::new();
        trailers.insert("content-length", HeaderValue::from_static("5"));
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        assert_eq!(
            sent(Sending::Chunked, &[b"a"], Some(&trailers)),
            "1\r\na\r\n0\r\ngrpc-status: 0\r\n\r\n"
        );
    }

    #[test]
    fn more_than_a_counted_body_promised_is_refused() {
        let mut writer = BodyWriter::new(Sending::Length(3));
        let mut out = Vec::new();
        assert_eq!(writer.data(&mut out, b"ab"), Ok(()));
        assert_eq!(writer.data(&mut out, b"cd"), Err(CodecError::BodyOverran));
    }

    #[test]
    fn less_than_a_counted_body_promised_is_refused() {
        let mut writer = BodyWriter::new(Sending::Length(3));
        let mut out = Vec::new();
        assert_eq!(writer.data(&mut out, b"ab"), Ok(()));
        assert_eq!(
            writer.finish(&mut out, None, &[], &H1Limits::default()),
            Err(CodecError::BodyShort)
        );
    }

    #[test]
    fn nothing_may_follow_the_end_of_a_body() {
        let mut writer = BodyWriter::new(Sending::Chunked);
        let mut out = Vec::new();
        assert_eq!(
            writer.finish(&mut out, None, &[], &H1Limits::default()),
            Ok(())
        );
        assert_eq!(writer.data(&mut out, b"a"), Err(CodecError::BodyAfterEnd));
        assert_eq!(
            writer.finish(&mut out, None, &[], &H1Limits::default()),
            Err(CodecError::BodyAfterEnd)
        );
    }

    #[test]
    fn a_body_that_was_to_be_absent_carries_nothing() {
        let mut writer = BodyWriter::new(Sending::None);
        let mut out = Vec::new();
        assert_eq!(writer.data(&mut out, b"a"), Err(CodecError::BodyAfterEnd));
        // A frame of no bytes is still nothing, and is let by.
        let mut writer = BodyWriter::new(Sending::None);
        assert_eq!(writer.data(&mut out, b""), Ok(()));
        assert_eq!(
            writer.finish(&mut out, None, &[], &H1Limits::default()),
            Ok(())
        );
        assert!(out.is_empty());
    }

    #[test]
    fn trailers_on_a_body_that_cannot_carry_them_are_refused() {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        for sending in [Sending::None, Sending::Length(0)] {
            let mut writer = BodyWriter::new(sending);
            let mut out = Vec::new();
            assert_eq!(
                writer.finish(&mut out, Some(&trailers), &[], &H1Limits::default()),
                Err(CodecError::UnexpectedTrailers),
                "{sending:?}"
            );
        }
    }

    /// What is written for a chunked body is read back as the same body, which is the
    /// only check that really matters of a framing this writes and something else reads.
    #[test]
    fn what_is_written_chunked_reads_back_as_what_went_in() {
        let frames: &[&[u8]] = &[b"hello", b" ", b"there", &[b'x'; 300]];
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        let written = sent(Sending::Chunked, frames, Some(&trailers));

        let (read, back) = body_of(Framing::Chunked, written.as_bytes(), false).unwrap();
        assert_eq!(read, frames.concat());
        assert_eq!(back.unwrap().fields["grpc-status"], "0");
    }

    /// Found by the fuzzer, and kept: which bound a bad head trips depends on how much of
    /// it has arrived. A reader given everything at once meets the bare newline; one given
    /// a byte at a time runs out of room before it reaches it. Both refuse the head, which
    /// is the part that has to be the same — the complaint is not.
    #[test]
    fn which_bound_a_bad_head_trips_depends_on_what_has_arrived() {
        let limits = H1Limits {
            head: 16,
            ..H1Limits::default()
        };
        // No empty line anywhere, longer than a head may be, and a bare newline past that.
        // It starts as a status line would, so that the start is not what refuses it.
        let bytes = b"HTTP/1.1 200 aaaaaaaa\n";

        assert_eq!(
            HeadReader::default().read(bytes, &limits),
            Err(CodecError::Malformed("a line ends with a bare newline"))
        );

        let mut reader = HeadReader::default();
        let mut refused = None;
        for upto in 0..=bytes.len() {
            if let Err(error) = reader.read(&bytes[..upto], &limits) {
                refused = Some(error);
                break;
            }
        }
        assert_eq!(refused, Some(CodecError::HeadTooLong { limit: 16 }));
    }

    /// Fields for a property test: a few names, some of them framing that the writer
    /// leaves out, with values of any length a field may have.
    fn fields() -> impl proptest::strategy::Strategy<Value = HeaderMap> {
        use proptest::prelude::*;
        let name = prop_oneof![
            Just("x-a"),
            Just("x-bb"),
            Just("host"),
            Just("content-length"),
            Just("transfer-encoding"),
            Just("connection"),
            Just("te"),
        ];
        proptest::collection::vec((name, "[ -~]{0,40}"), 0..12).prop_map(|pairs| {
            let mut fields = HeaderMap::new();
            for (name, value) in pairs {
                fields.append(name, HeaderValue::from_str(&value).unwrap());
            }
            fields
        })
    }

    proptest::proptest! {
        /// What room is made for a head is exactly what writing it takes, whatever the head
        /// and however its body goes: room paid for up front is room that suffices, and no
        /// more is paid for than is used (14 §8).
        #[test]
        fn a_head_is_as_long_as_it_was_said_to_be(
            method in proptest::sample::select(vec![Method::GET, Method::POST, Method::DELETE]),
            path in "/[a-z0-9/?=&]{0,30}",
            headers in fields(),
            sending in proptest::prop_oneof![
                proptest::strategy::Just(Sending::None),
                proptest::strategy::Just(Sending::Chunked),
                proptest::strategy::Strategy::prop_map(
                    proptest::prelude::any::<u64>(),
                    Sending::Length,
                ),
            ],
        ) {
            use proptest::prelude::*;
            let uri: Uri = path.parse().unwrap();
            let mut out = b"before".to_vec();
            let said = head_len(&method, &uri, &headers, sending);
            write_head(&mut out, &method, &uri, &headers, sending, &H1Limits::default()).unwrap();
            prop_assert_eq!(out.len() - b"before".len(), said);
        }

        /// The room made for the end of a chunked body is never less than finishing it
        /// writes, and is exactly that when nothing is filtered out of its trailers.
        #[test]
        fn the_end_of_a_body_fits_the_room_made_for_it(
            trailers in proptest::option::of(fields()),
            nominated in proptest::sample::subsequence(vec!["x-a", "x-bb"], 0..=2),
        ) {
            use proptest::prelude::*;
            let nominated: Vec<HeaderName> = nominated
                .into_iter()
                .map(HeaderName::from_static)
                .collect();
            let mut writer = BodyWriter::new(Sending::Chunked);
            let room = writer.finish_room(trailers.as_ref());
            let mut out = Vec::new();
            let limits = H1Limits { trailers: 1 << 20, ..H1Limits::default() };
            if writer.finish(&mut out, trailers.as_ref(), &nominated, &limits).is_ok() {
                prop_assert!(out.len() <= room, "{} written in {room}", out.len());
                let filtered = trailers
                    .iter()
                    .flatten()
                    .any(|(name, _)| is_denied(name, &nominated));
                if !filtered {
                    prop_assert_eq!(out.len(), room);
                }
            }
        }
    }
}
