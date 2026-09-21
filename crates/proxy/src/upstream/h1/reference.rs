//! The specification as code: a slow, obvious reader of an upstream's answer, for the
//! differential harness to compare both clients against.
//!
//! Nothing here is shared with [`super::codec`], and nothing here minds its cost. It is
//! written from [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html) and
//! [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) rather than from the code it
//! checks: two implementations of one specification disagree where at least one of them
//! is wrong, which is the whole use of it. It is given the bytes whole, keeps no state
//! between arrivals, rescans whatever it likes, and says what the message means and
//! where it ends.
//!
//! **It has no opinions of this project's own.** Where the specification names a choice
//! and leaves it open, this reads the message the permissive way and records that the
//! choice was there ([`Notable`]); where a bound is EdgeRush's rather than the
//! specification's, this applies none of it and measures the thing the bound is about
//! ([`Measured`]), so that the harness can decide for itself whether a refusal was this
//! project's to make. That is what keeps "ours refused it" and "it is not a message"
//! separate questions ([13 §8](../../../../docs/13-http1-upstream.md)).
//!
//! It says nothing about whether a connection may be used again: that needs the
//! exchange's event trace and not the answer's bytes, and it belongs to the lifecycle
//! model. What is here is the one part the bytes do settle — [`Answer::persistent`],
//! HTTP's own view of the connection, and only the first of the conditions in
//! [13 §6](../../../../docs/13-http1-upstream.md).

/// What was asked of the upstream, so far as the answer's framing turns on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asked {
    /// A method whose answer may carry a body.
    Anything,
    /// `HEAD`. The answer has the head a body would have had, and no body
    /// ([RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3) rule 1).
    Head,
}

/// Which HTTP the answer was written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// `HTTP/1.0`.
    Ten,
    /// `HTTP/1.1`.
    Eleven,
}

/// How the answer's body was delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// There is no body at all, whatever the head says about one.
    None,
    /// This many bytes of it.
    Length(u64),
    /// In chunks, ending with a zero chunk and a trailer section.
    Chunked,
    /// Until the connection closes, which is the only thing that says it is over.
    ToClose,
}

/// A place where the specification names a choice and leaves it open, or where a message
/// is outside what [13 §4](../../../../docs/13-http1-upstream.md) undertakes to support.
///
/// None of these makes a message unreadable, and none of them is a fault in a peer. They
/// are the harness's account of why two clients may both be right about one message: an
/// answer that raises any of them is outside the subset both clients support, so equality
/// is not required of it — a classified outcome on each path is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Notable {
    /// `HTTP/1.0`. Framed by its own rules, and never kept for reuse in this slice.
    Http10,
    /// The same `Content-Length` more than once.
    /// [RFC 9110 §8.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.6) lets a
    /// recipient "either reject the message as invalid or replace that invalid field
    /// value with a single instance": both choices in one sentence.
    RepeatedLength,
    /// A `Transfer-Encoding` and a `Content-Length` together.
    /// [RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3) rule 3
    /// says such a message "ought to be handled as an error", and that a forwarder which
    /// chooses to forward it MUST first remove the length. Read here as the transfer
    /// coding with the length disregarded, which is what that rule leaves standing.
    LengthWithCoding,
    /// A transfer coding this slice does not support: a chain of them, `chunked`
    /// anywhere but last, or any at all on an HTTP/1.0 message.
    UnsupportedCoding,
    /// An interim answer that claimed a body. A 1xx ends at its empty line whatever its
    /// fields say ([RFC 9110 §15.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.2)),
    /// so the framing is not in doubt; the claim is only evidence that something upstream
    /// is confused about this message.
    InterimClaimsBody,
    /// A 101. The connection would become something that is not HTTP/1.1, which this
    /// slice does not support.
    Upgrade,
}

/// What the bytes came to, measured rather than judged.
///
/// Every one of these is the subject of a bound in
/// [13 §7](../../../../docs/13-http1-upstream.md) that no specification asks for. They
/// are reported so that the harness can tell a refusal this project was entitled to make
/// from a message that cannot be read at all, without this reader having to know what any
/// of the limits are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Measured {
    /// The final head, its status line and terminator included.
    pub head: usize,
    /// How many fields it carried.
    pub fields: usize,
    /// How many interim answers came before it.
    pub interim_heads: usize,
    /// What those came to together, each one's terminator included.
    pub interim_bytes: usize,
    /// The longest chunk-size line: the bytes from the end of the previous chunk to the
    /// carriage return that ends the line, extensions included and terminator excluded.
    pub chunk_line: usize,
    /// The trailer section, its terminating empty line included.
    pub trailers: usize,
    /// How many fields were in it, before any of them were filtered.
    pub trailer_fields: usize,
}

/// An interim answer, consumed on the way to the final one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interim {
    /// Its status.
    pub status: u16,
    /// Its fields, in the order they arrived.
    pub fields: Vec<(String, String)>,
}

/// An answer, as the specification reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// Which HTTP it was written in.
    pub version: Version,
    /// Its status.
    pub status: u16,
    /// Its reason phrase, which may be empty and which nothing compares.
    pub reason: String,
    /// Its fields, lowered in name, in the order they arrived and with repeats kept: a
    /// message with one field twice is not the same message as one with it once, and
    /// folding them here would throw away the evidence that the difference existed.
    pub fields: Vec<(String, String)>,
    /// Its body, decoded — chunk framing removed, the bytes themselves untouched.
    pub body: Vec<u8>,
    /// The fields after the last chunk, in order and unfiltered.
    pub trailers: Vec<(String, String)>,
    /// The interim answers that came before it.
    pub interim: Vec<Interim>,
    /// How its body was delimited.
    pub framing: Framing,
    /// Whether HTTP leaves the connection usable, from the version and the head's own
    /// `Connection` options. The first of the conditions in
    /// [13 §6](../../../../docs/13-http1-upstream.md) and by itself no kind of proof:
    /// a body delimited by the close cannot be followed by another exchange whatever
    /// this says, and the rest of the conditions are the lifecycle model's.
    pub persistent: bool,
    /// Where the message ends, counted from the first byte the upstream said and the
    /// interim answers included. Bytes past it are the next message's, or surplus.
    ///
    /// The message's own boundary, and not a count of what any client read: a client may
    /// read past it and hold what it has not used yet.
    pub boundary: usize,
    /// Where the specification left a choice, or the message went outside what this
    /// slice supports.
    pub notable: Vec<Notable>,
    /// What the message came to, for the harness to hold against this project's bounds.
    pub measured: Measured,
}

impl Answer {
    /// Whether this answer is inside the subset both clients undertake to support, and
    /// so one they have to agree about exactly.
    #[must_use]
    pub fn shared(&self) -> bool {
        self.notable.is_empty()
    }
}

/// Why the bytes are not an answer that can be read at all.
///
/// Every one of these is the specification's own refusal and not a policy of this
/// project's: a message that raises one has no reading, so a client that produces one
/// has read something that is not there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalid {
    /// A line feed that was not preceded by a carriage return. A recipient may recognise
    /// one, but [RFC 9112 §2.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-2.2)
    /// forbids a proxy from doing it.
    BareLineFeed,
    /// A carriage return that was not followed by a line feed.
    BareCarriageReturn,
    /// A field line continued onto the next line.
    /// [RFC 9112 §5.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-5.2) requires
    /// obs-fold to be rejected in anything but a message body.
    ObsFold,
    /// Not `HTTP-version SP status-code SP [ reason-phrase ]`. The space before the
    /// reason phrase is required even when the phrase itself is absent
    /// ([RFC 9112 §4](https://www.rfc-editor.org/rfc/rfc9112.html#section-4)), so a line
    /// that stops after the code is malformed.
    StatusLine,
    /// Not `HTTP/1.0` or `HTTP/1.1`.
    Version,
    /// Not three digits.
    StatusCode,
    /// A field name that is not a token, or is empty.
    FieldName,
    /// Whitespace between a field name and its colon
    /// ([RFC 9112 §5.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-5.1)).
    SpaceBeforeColon,
    /// A field line with no colon in it at all.
    NoColon,
    /// A field value with a byte that may not appear in one.
    FieldValue,
    /// A `Content-Length` that is not a plain count, or two of them that disagree.
    Length,
    /// A chunk size that is not hexadecimal digits, or does not fit in a count.
    ChunkSize,
    /// A chunk extension that does not match the grammar in
    /// [RFC 9112 §7.1.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-7.1.1),
    /// which requires a name before any `=`.
    ChunkExtension,
    /// A chunk's data was not followed by a carriage return and line feed.
    ChunkEnd,
}

/// What the reference made of the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reading {
    /// A whole answer, and what it means. Boxed because it is much the largest of
    /// these, and an outcome is passed about by value wherever a comparison goes.
    Read(Box<Answer>),
    /// Not an answer. More bytes would not help.
    Invalid(Invalid),
    /// Not a whole answer yet, and more bytes would settle it. Where the upstream has
    /// said everything it is going to, this is a message that was cut off.
    Unfinished,
}

/// Reads `bytes` as an answer to `asked`, where `ended` says whether the upstream has
/// finished talking — which is the only thing that ends a body delimited by the
/// connection closing.
#[must_use]
pub fn read(bytes: &[u8], asked: Asked, ended: bool) -> Reading {
    match reading(bytes, asked, ended) {
        Ok(Some(answer)) => Reading::Read(Box::new(answer)),
        Ok(None) => Reading::Unfinished,
        Err(invalid) => Reading::Invalid(invalid),
    }
}

/// The same, with the three outcomes as a `Result` so that the reading can use `?`.
fn reading(bytes: &[u8], asked: Asked, ended: bool) -> Result<Option<Answer>, Invalid> {
    let mut measured = Measured::default();
    let mut interim = Vec::new();
    let mut notable = Vec::new();
    let mut at = 0;

    // The interim answers, and then the final one. A 1xx ends at its empty line whatever
    // its fields claim, so each one is read and set aside.
    let head = loop {
        let Some(head) = read_head(&bytes[at..])? else {
            return Ok(None);
        };
        at += head.bytes;
        if !(100..200).contains(&head.status) {
            break head;
        }
        if head.status == 101 {
            note(&mut notable, Notable::Upgrade);
            break head;
        }
        if named(&head.fields, "content-length").next().is_some()
            || named(&head.fields, "transfer-encoding").next().is_some()
        {
            note(&mut notable, Notable::InterimClaimsBody);
        }
        measured.interim_heads += 1;
        measured.interim_bytes += head.bytes;
        interim.push(Interim {
            status: head.status,
            fields: head.fields,
        });
    };

    measured.head = head.bytes;
    measured.fields = head.fields.len();
    if head.version == Version::Ten {
        note(&mut notable, Notable::Http10);
    }

    let framing = framing(&head, asked, &mut notable)?;
    let persistent = persistent(&head);

    let (body, trailers, boundary) = match framing {
        Framing::None => (Vec::new(), Vec::new(), at),
        Framing::Length(length) => {
            // A length no machine could hold is a message that has not all arrived,
            // which is the truth about it.
            let Some(end) = usize::try_from(length)
                .ok()
                .and_then(|length| at.checked_add(length))
            else {
                return Ok(None);
            };
            let Some(body) = bytes.get(at..end) else {
                return Ok(None);
            };
            (body.to_vec(), Vec::new(), end)
        }
        Framing::Chunked => {
            let Some(chunked) = read_chunked(&bytes[at..], &mut measured)? else {
                return Ok(None);
            };
            (chunked.body, chunked.trailers, at + chunked.bytes)
        }
        Framing::ToClose => {
            if !ended {
                return Ok(None);
            }
            (bytes[at..].to_vec(), Vec::new(), bytes.len())
        }
    };

    Ok(Some(Answer {
        version: head.version,
        status: head.status,
        reason: head.reason,
        fields: head.fields,
        body,
        trailers,
        interim,
        framing,
        persistent,
        boundary,
        notable,
        measured,
    }))
}

/// Adds `what` unless it is already there: a message with three repeated lengths is the
/// same kind of message as one with two.
fn note(notable: &mut Vec<Notable>, what: Notable) {
    if !notable.contains(&what) {
        notable.push(what);
    }
}

/// One head, read whole.
#[derive(Debug)]
struct Head {
    version: Version,
    status: u16,
    reason: String,
    fields: Vec<(String, String)>,
    /// What it came to, its terminator included.
    bytes: usize,
}

/// Reads a head from the front of `bytes`, or `None` if the whole of one is not there.
fn read_head(bytes: &[u8]) -> Result<Option<Head>, Invalid> {
    let end = index_of(bytes, b"\r\n\r\n");
    // Only as far as the head reaches: a bare line feed inside a message body is the
    // body's business, and one before the head has ended is a line ending this reader
    // may not recognise.
    let region = end.map_or(bytes, |end| &bytes[..end + 4]);
    // A head that has not ended may stop in the middle of a line ending, which is a
    // message that has not all arrived rather than one with a stray return in it.
    let region = match end {
        None => region.strip_suffix(b"\r").unwrap_or(region),
        Some(_) => region,
    };
    line_endings(region)?;
    let Some(end) = end else {
        return Ok(None);
    };

    let mut lines = bytes[..end].split(|byte| *byte == b'\n').map(strip_return);
    let Some(status) = lines.next() else {
        return Err(Invalid::StatusLine);
    };
    let (version, status, reason) = read_status(status)?;

    let mut fields = Vec::new();
    for line in lines {
        fields.push(read_field(line)?);
    }

    Ok(Some(Head {
        version,
        status,
        reason,
        fields,
        bytes: end + 4,
    }))
}

/// Every line feed preceded by a carriage return, and every carriage return followed by
/// a line feed. Nothing else ends a line here, and a message that thinks otherwise is
/// one a proxy must not guess at.
fn line_endings(bytes: &[u8]) -> Result<(), Invalid> {
    for (at, byte) in bytes.iter().enumerate() {
        match byte {
            b'\n' if at == 0 || bytes[at - 1] != b'\r' => return Err(Invalid::BareLineFeed),
            b'\r' if bytes.get(at + 1) != Some(&b'\n') => {
                return Err(Invalid::BareCarriageReturn);
            }
            _ => {}
        }
    }
    Ok(())
}

/// The line without the carriage return the split left on the end of it.
fn strip_return(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// `HTTP-version SP status-code SP [ reason-phrase ]`, and nothing looser.
fn read_status(line: &[u8]) -> Result<(Version, u16, String), Invalid> {
    let rest = line.strip_prefix(b"HTTP/1.").ok_or(Invalid::Version)?;
    let (version, rest) = rest.split_first().ok_or(Invalid::Version)?;
    let version = match version {
        b'0' => Version::Ten,
        b'1' => Version::Eleven,
        _ => return Err(Invalid::Version),
    };
    let rest = rest.strip_prefix(b" ").ok_or(Invalid::StatusLine)?;
    let (code, reason) = rest.split_at_checked(3).ok_or(Invalid::StatusCode)?;
    if !code.iter().all(u8::is_ascii_digit) {
        return Err(Invalid::StatusCode);
    }
    let reason = reason.strip_prefix(b" ").ok_or(Invalid::StatusLine)?;
    if !reason.iter().copied().all(is_text) {
        return Err(Invalid::StatusLine);
    }
    let status = code
        .iter()
        .fold(0u16, |status, digit| status * 10 + u16::from(digit - b'0'));
    Ok((version, status, text(reason)))
}

/// `field-name ":" OWS field-value OWS`, with the name lowered so that a comparison is
/// about the field and not about how a peer capitalised it.
fn read_field(line: &[u8]) -> Result<(String, String), Invalid> {
    if line.first().is_some_and(|byte| is_space(*byte)) {
        return Err(Invalid::ObsFold);
    }
    let colon = line
        .iter()
        .position(|byte| *byte == b':')
        .ok_or(Invalid::NoColon)?;
    let (name, value) = (&line[..colon], &line[colon + 1..]);
    if name.last().is_some_and(|byte| is_space(*byte)) {
        return Err(Invalid::SpaceBeforeColon);
    }
    if name.is_empty() || !name.iter().copied().all(is_token) {
        return Err(Invalid::FieldName);
    }
    let value = trim(value);
    if !value.iter().copied().all(is_text) {
        return Err(Invalid::FieldValue);
    }
    Ok((text(name).to_ascii_lowercase(), text(value)))
}

/// The framing of the body, by the rules of
/// [RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3) in their
/// order — which is the whole of what stops one message being read as two.
fn framing(head: &Head, asked: Asked, notable: &mut Vec<Notable>) -> Result<Framing, Invalid> {
    // Checked whatever the framing turns out to be: a bodyless answer's length and
    // coding are validated and then disregarded, never consumed as a body.
    let coding = coding(head, notable);
    let length = length(head, notable)?;

    // Rule 1.
    if matches!(asked, Asked::Head)
        || (100..200).contains(&head.status)
        || head.status == 204
        || head.status == 304
    {
        return Ok(Framing::None);
    }
    // Rule 3, ahead of the length: a transfer coding overrides one.
    if let Some(chunked) = coding {
        if length.is_some() {
            note(notable, Notable::LengthWithCoding);
        }
        return Ok(if chunked {
            Framing::Chunked
        } else {
            // A coding whose last element is not chunked leaves a response delimited by
            // the close, which is what rule 3 says of one.
            Framing::ToClose
        });
    }
    // Rules 5 and 6.
    Ok(match length {
        Some(length) => Framing::Length(length),
        None => Framing::ToClose,
    })
}

/// Whether the answer has a transfer coding, and whether `chunked` is the last of them.
/// Anything but a single `chunked` is noted as outside this slice.
fn coding(head: &Head, notable: &mut Vec<Notable>) -> Option<bool> {
    let codings: Vec<String> = named(&head.fields, "transfer-encoding")
        .flat_map(list)
        .collect();
    if codings.is_empty() {
        return None;
    }
    // A coding on an HTTP/1.0 message: the peer cannot have been told that this hop
    // speaks 1.1, so there is no saying what it meant by one.
    if head.version == Version::Ten {
        note(notable, Notable::UnsupportedCoding);
    }
    let chunked = codings.last().is_some_and(|coding| coding == "chunked");
    if codings.len() > 1 || !chunked {
        note(notable, Notable::UnsupportedCoding);
    }
    Some(chunked)
}

/// The answer's `Content-Length`, as one plain count.
///
/// Two that agree is a choice the specification leaves open, so it is read and noted.
/// Two that disagree, a sign, a comma list, an empty value or anything that does not fit
/// is no reading at all.
fn length(head: &Head, notable: &mut Vec<Notable>) -> Result<Option<u64>, Invalid> {
    let mut lengths = Vec::new();
    for value in named(&head.fields, "content-length") {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Invalid::Length);
        }
        lengths.push(value.parse::<u64>().map_err(|_| Invalid::Length)?);
    }
    let Some((first, rest)) = lengths.split_first() else {
        return Ok(None);
    };
    if !rest.is_empty() {
        if rest.iter().any(|length| length != first) {
            return Err(Invalid::Length);
        }
        note(notable, Notable::RepeatedLength);
    }
    Ok(Some(*first))
}

/// Whether HTTP leaves the connection usable, worked out before anything is stripped
/// from the head: `close` ends it either way, and an HTTP/1.0 answer has to ask.
fn persistent(head: &Head) -> bool {
    let options: Vec<String> = named(&head.fields, "connection").flat_map(list).collect();
    if options.iter().any(|option| option == "close") {
        return false;
    }
    match head.version {
        Version::Eleven => true,
        Version::Ten => options.iter().any(|option| option == "keep-alive"),
    }
}

/// A chunked body, read whole.
struct Chunked {
    body: Vec<u8>,
    trailers: Vec<(String, String)>,
    /// What the whole of it came to, the trailer section's terminator included.
    bytes: usize,
}

/// Reads a chunked body from the front of `bytes`, or `None` if the whole of it is not
/// there yet.
fn read_chunked(bytes: &[u8], measured: &mut Measured) -> Result<Option<Chunked>, Invalid> {
    let mut body = Vec::new();
    let mut at = 0;
    loop {
        let Some(end) = index_of(&bytes[at..], b"\r\n") else {
            return Ok(None);
        };
        let line = &bytes[at..at + end];
        line_endings(line)?;
        measured.chunk_line = measured.chunk_line.max(line.len());
        let digits = line
            .iter()
            .position(|byte| !byte.is_ascii_hexdigit())
            .unwrap_or(line.len());
        let (size, marks) = line.split_at(digits);
        if size.is_empty() {
            return Err(Invalid::ChunkSize);
        }
        read_extensions(marks)?;
        // Leading zeros are as valid as any other digits; what is refused is a size no
        // count could hold, which is the overflow the specification warns about.
        let size = u64::from_str_radix(&text(size), 16).map_err(|_| Invalid::ChunkSize)?;
        at += end + 2;

        if size == 0 {
            let Some((trailers, bytes_of)) = read_trailers(&bytes[at..])? else {
                return Ok(None);
            };
            measured.trailers = bytes_of;
            measured.trailer_fields = trailers.len();
            return Ok(Some(Chunked {
                body,
                trailers,
                bytes: at + bytes_of,
            }));
        }

        let Some(end) = usize::try_from(size)
            .ok()
            .and_then(|size| at.checked_add(size))
        else {
            return Ok(None);
        };
        let Some(data) = bytes.get(at..end) else {
            return Ok(None);
        };
        body.extend_from_slice(data);
        at = end;
        match bytes.get(at..at + 2) {
            None => return Ok(None),
            Some(b"\r\n") => at += 2,
            Some(_) => return Err(Invalid::ChunkEnd),
        }
    }
}

/// `*( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )`, and nothing else.
///
/// A recipient must ignore an extension it does not recognise, which is about names
/// nobody knows and not about bytes that are not an extension at all: there is no name
/// in `;=v`, so it does not match the grammar and there is nothing there to ignore.
fn read_extensions(mut rest: &[u8]) -> Result<(), Invalid> {
    while !rest.is_empty() {
        rest = skip_space(rest);
        rest = rest.strip_prefix(b";").ok_or(Invalid::ChunkExtension)?;
        rest = skip_space(rest);
        let name = rest
            .iter()
            .position(|byte| !is_token(*byte))
            .unwrap_or(rest.len());
        if name == 0 {
            return Err(Invalid::ChunkExtension);
        }
        rest = skip_space(&rest[name..]);
        let Some(after) = rest.strip_prefix(b"=") else {
            continue;
        };
        rest = skip_space(after);
        rest = match rest.first() {
            Some(b'"') => quoted(rest)?,
            _ => {
                let value = rest
                    .iter()
                    .position(|byte| !is_token(*byte))
                    .unwrap_or(rest.len());
                if value == 0 {
                    return Err(Invalid::ChunkExtension);
                }
                &rest[value..]
            }
        };
    }
    Ok(())
}

/// What is left after the quoted string at the front of `rest`.
fn quoted(rest: &[u8]) -> Result<&[u8], Invalid> {
    let mut at = 1;
    while let Some(byte) = rest.get(at) {
        match byte {
            b'"' => return Ok(&rest[at + 1..]),
            // A quoted pair: what follows the backslash is that byte and not a mark.
            b'\\' if rest.get(at + 1).is_some_and(|byte| is_text(*byte)) => at += 2,
            byte if is_text(*byte) => at += 1,
            _ => return Err(Invalid::ChunkExtension),
        }
    }
    Err(Invalid::ChunkExtension)
}

/// The trailer fields, and what the section came to.
type Trailers = (Vec<(String, String)>, usize);

/// Reads the fields after the last chunk, up to and including the empty line that ends
/// them, or `None` if that line has not arrived.
fn read_trailers(bytes: &[u8]) -> Result<Option<Trailers>, Invalid> {
    let mut trailers = Vec::new();
    let mut at = 0;
    loop {
        let Some(end) = index_of(&bytes[at..], b"\r\n") else {
            return Ok(None);
        };
        let line = &bytes[at..at + end];
        line_endings(line)?;
        at += end + 2;
        if line.is_empty() {
            return Ok(Some((trailers, at)));
        }
        trailers.push(read_field(line)?);
    }
}

/// Every value of the fields with this name, in order. The name is already lowered.
fn named<'a>(fields: &'a [(String, String)], name: &'a str) -> impl Iterator<Item = &'a str> {
    fields
        .iter()
        .filter(move |(field, _)| field == name)
        .map(|(_, value)| value.as_str())
}

/// A comma-separated list value, each element trimmed and lowered. Empty elements are
/// dropped, which is what the specification's list rule lets a recipient do.
fn list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|element| element.trim_matches([' ', '\t']).to_ascii_lowercase())
        .filter(|element| !element.is_empty())
        .collect()
}

/// Where `needle` first appears in `haystack`.
fn index_of(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// The bytes as text, with anything that is not UTF-8 replaced. Nothing here decodes a
/// payload: this is for a name or a value, and one with a byte in it that is not text
/// was refused before it got here.
fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Without the optional whitespace at either end of a field value.
fn trim(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !is_space(*byte))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !is_space(*byte))
        .map_or(start, |end| end + 1);
    &value[start..end]
}

/// Past any bad whitespace, which the chunk-extension grammar allows between its parts.
fn skip_space(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !is_space(*byte))
        .unwrap_or(bytes.len());
    &bytes[start..]
}

fn is_space(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

/// A `tchar`, from
/// [RFC 9110 §5.6.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.2).
fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

/// What may appear in a field value or a reason phrase: `VCHAR`, `obs-text`, or the
/// whitespace between words of it.
fn is_text(byte: u8) -> bool {
    is_space(byte) || (0x21..=0x7e).contains(&byte) || byte >= 0x80
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read as the answer to an ordinary method, with the upstream having said everything
    /// it is going to.
    fn whole(bytes: &[u8]) -> Answer {
        match read(bytes, Asked::Anything, true) {
            Reading::Read(answer) => *answer,
            other => panic!("{other:?} is not a whole answer"),
        }
    }

    /// Why the bytes are not an answer.
    fn refused(bytes: &[u8]) -> Invalid {
        match read(bytes, Asked::Anything, true) {
            Reading::Invalid(invalid) => invalid,
            other => panic!("{other:?} was read as an answer"),
        }
    }

    fn fields(answer: &Answer) -> Vec<(&str, &str)> {
        answer
            .fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }

    /// A few heads to hang generated bytes off. Arbitrary bytes are almost never a
    /// message, and a property that only ever sees refusals is not saying much.
    const HEADS: [&[u8]; 5] = [
        b"HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\n",
        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
        b"HTTP/1.1 204 No Content\r\n\r\n",
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\n\r\n",
    ];

    proptest::proptest! {
        /// Whatever the bytes are they get an answer, and a message never ends past the
        /// bytes it was read from.
        #[test]
        fn any_bytes_at_all_are_read_without_panicking(
            head in 0..HEADS.len(),
            rest: Vec<u8>,
            ended: bool,
        ) {
            let mut bytes = HEADS[head].to_vec();
            bytes.extend_from_slice(&rest);
            for asked in [Asked::Anything, Asked::Head] {
                if let Reading::Read(answer) = read(&bytes, asked, ended) {
                    proptest::prop_assert!(answer.boundary <= bytes.len());
                }
                // And bytes with no head in front of them, which are almost never a
                // message but must still be read without a panic.
                let _ = read(&rest, asked, ended);
            }
        }

        /// Bytes after a whole message do not change what the message was.
        ///
        /// This is the property with teeth. A reader whose boundary is wrong by a byte
        /// reads the next message's first byte as this one's, and the two ends of the
        /// connection are out of step from then on.
        #[test]
        fn surplus_after_a_message_changes_nothing(
            head in 0..HEADS.len(),
            rest: Vec<u8>,
            surplus: Vec<u8>,
        ) {
            let mut bytes = HEADS[head].to_vec();
            bytes.extend_from_slice(&rest);
            let first = read(&bytes, Asked::Anything, true);
            let mut more = bytes.clone();
            more.extend_from_slice(&surplus);
            let second = read(&more, Asked::Anything, true);
            match &first {
                // A body that ends with the connection is the one framing whose meaning
                // more bytes can change, because the close is what ended it; its
                // boundary is everything there was.
                Reading::Read(answer) if answer.framing == Framing::ToClose => {
                    proptest::prop_assert_eq!(answer.boundary, bytes.len());
                }
                Reading::Read(_) | Reading::Invalid(_) => {
                    proptest::prop_assert_eq!(&second, &first);
                }
                Reading::Unfinished => {}
            }
        }
    }

    #[test]
    fn a_bodyless_answer_ends_at_its_head() {
        let answer = whole(b"HTTP/1.1 204 No Content\r\n\r\nthe next message's");
        assert_eq!(answer.status, 204);
        assert_eq!(answer.framing, Framing::None);
        assert!(answer.body.is_empty());
        // Where the message ends, not where the bytes do: what follows is not this
        // answer's, and a reader that took it would be reading two messages as one.
        assert_eq!(answer.boundary, 27);
        assert!(answer.persistent);
        assert!(answer.shared());
    }

    #[test]
    fn a_counted_body_is_its_length_and_no_more() {
        let answer = whole(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nokand more");
        assert_eq!(answer.framing, Framing::Length(2));
        assert_eq!(answer.body, b"ok");
        assert_eq!(answer.boundary, 40);
        assert_eq!(answer.measured.head, 38);
        assert_eq!(answer.measured.fields, 1);
    }

    #[test]
    fn a_counted_body_that_is_short_has_not_all_arrived() {
        let bytes = b"HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\nok";
        assert_eq!(read(bytes, Asked::Anything, true), Reading::Unfinished);
        // And a length no machine could hold is the same answer, not a panic.
        let huge = b"HTTP/1.1 200 OK\r\ncontent-length: 18446744073709551615\r\n\r\nok";
        assert_eq!(read(huge, Asked::Anything, true), Reading::Unfinished);
    }

    #[test]
    fn an_answer_to_head_has_no_body_however_it_is_framed() {
        let bytes = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";
        let Reading::Read(answer) = read(bytes, Asked::Head, true) else {
            panic!("not read");
        };
        assert_eq!(answer.framing, Framing::None);
        assert!(answer.body.is_empty());
        assert_eq!(answer.boundary, 38);
        // The metadata was still validated: a bodyless answer does not get to carry a
        // length that is not a length.
        let bad = b"HTTP/1.1 200 OK\r\ncontent-length: -5\r\n\r\n";
        assert_eq!(
            read(bad, Asked::Head, true),
            Reading::Invalid(Invalid::Length)
        );
    }

    #[test]
    fn a_304_and_a_1xx_have_no_body_either_and_a_205_is_not_a_204() {
        let not_modified = whole(b"HTTP/1.1 304 Not Modified\r\ncontent-length: 5\r\n\r\nhello");
        assert_eq!(not_modified.framing, Framing::None);
        assert!(not_modified.body.is_empty());
        // 205 is neither 204 nor 304, and its body is its body.
        let reset = whole(b"HTTP/1.1 205 Reset Content\r\ncontent-length: 2\r\n\r\nok");
        assert_eq!(reset.framing, Framing::Length(2));
        assert_eq!(reset.body, b"ok");
    }

    #[test]
    fn a_chunked_body_is_its_chunks_and_then_its_trailers() {
        let answer = whole(
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
              5\r\nhello\r\n3\r\n th\r\n0\r\nx-a: 1\r\nx-b: 2\r\n\r\n",
        );
        assert_eq!(answer.framing, Framing::Chunked);
        assert_eq!(answer.body, b"hello th");
        assert_eq!(
            answer.trailers,
            [
                ("x-a".to_owned(), "1".to_owned()),
                ("x-b".to_owned(), "2".to_owned())
            ]
        );
        assert_eq!(answer.measured.trailer_fields, 2);
        assert_eq!(answer.measured.chunk_line, 1);
        assert!(answer.shared());
    }

    #[test]
    fn a_chunked_body_with_no_trailers_still_ends_with_the_empty_line() {
        let head = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
        let whole_answer =
            whole(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n");
        assert_eq!(whole_answer.body, b"hi");
        assert_eq!(whole_answer.boundary, head.len() + 7 + 5);
        assert_eq!(whole_answer.measured.trailers, 2);
        // Without that line the body has not ended.
        let short = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n";
        assert_eq!(read(short, Asked::Anything, true), Reading::Unfinished);
    }

    #[test]
    fn a_chunk_extension_needs_a_name_before_its_value() {
        let with = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2;a=b\r\nhi\r\n0\r\n\r\n";
        assert_eq!(whole(with).body, b"hi");
        // Bad whitespace is allowed between the parts, and a quoted value is a value.
        let quoted = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2 ; a = \"b;c\" ;d\r\nhi\r\n0\r\n\r\n";
        assert_eq!(whole(quoted).body, b"hi");
        // A value with no name is not an extension at all, so there is nothing to ignore.
        let nameless =
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2;=v\r\nhi\r\n0\r\n\r\n";
        assert_eq!(refused(nameless), Invalid::ChunkExtension);
        // And a quoted value that never ends is not one.
        let unended =
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2;a=\"b\r\nhi\r\n0\r\n\r\n";
        assert_eq!(refused(unended), Invalid::ChunkExtension);
    }

    #[test]
    fn a_chunk_must_be_followed_by_a_line_ending() {
        let bytes = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhixx0\r\n\r\n";
        assert_eq!(refused(bytes), Invalid::ChunkEnd);
        let size = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n;a\r\nhi\r\n0\r\n\r\n";
        assert_eq!(refused(size), Invalid::ChunkSize);
    }

    #[test]
    fn a_length_with_a_coding_is_read_as_the_coding() {
        let answer = whole(
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ncontent-length: 99\r\n\r\n2\r\nhi\r\n0\r\n\r\n",
        );
        // The coding wins and the length is disregarded, which is what RFC 9112 §6.3
        // rule 3 leaves standing. It is not a message either client has to accept.
        assert_eq!(answer.framing, Framing::Chunked);
        assert_eq!(answer.body, b"hi");
        assert_eq!(answer.notable, [Notable::LengthWithCoding]);
        assert!(!answer.shared());
    }

    #[test]
    fn the_same_length_twice_is_a_choice_the_specification_leaves_open() {
        let answer = whole(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\ncontent-length: 2\r\n\r\nok");
        assert_eq!(answer.framing, Framing::Length(2));
        assert_eq!(answer.notable, [Notable::RepeatedLength]);
        // Both of them are still there to be seen: folding them here would throw away
        // the evidence that a processor upstream had already rewritten this message.
        assert_eq!(
            fields(&answer),
            [("content-length", "2"), ("content-length", "2")]
        );
    }

    #[test]
    fn a_length_that_is_not_a_count_is_no_reading_at_all() {
        for value in [
            "-5",
            "+5",
            "5a",
            "",
            " ",
            "5, 5",
            "0x5",
            "99999999999999999999",
        ] {
            let bytes = format!("HTTP/1.1 200 OK\r\ncontent-length: {value}\r\n\r\n");
            assert_eq!(refused(bytes.as_bytes()), Invalid::Length, "{value:?}");
        }
        // Two that disagree are not a choice anyone is offered.
        let bytes = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\ncontent-length: 3\r\n\r\nok";
        assert_eq!(refused(bytes), Invalid::Length);
    }

    #[test]
    fn interim_answers_are_consumed_on_the_way_to_the_final_one() {
        let answer = whole(
            b"HTTP/1.1 100 Continue\r\n\r\n\
              HTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\n\
              HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
        );
        assert_eq!(answer.status, 200);
        assert_eq!(answer.body, b"ok");
        assert_eq!(answer.interim.len(), 2);
        assert_eq!(answer.interim[1].status, 103);
        assert_eq!(answer.measured.interim_heads, 2);
        assert_eq!(answer.measured.interim_bytes, 25 + 40);
        // The boundary counts them: they were bytes on this connection.
        assert_eq!(answer.boundary, 25 + 40 + 38 + 2);
        // Nothing about them is outside the shared subset; the caps on them are this
        // project's own, which is why they are measured and not judged here.
        assert!(answer.shared());
    }

    #[test]
    fn an_interim_answer_that_claims_a_body_is_still_only_a_head() {
        let answer = whole(
            b"HTTP/1.1 103 Early Hints\r\ncontent-length: 5\r\n\r\n\
              HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
        );
        // The claim is not read as a body: a 1xx ends at its empty line, so the final
        // answer is where it always was.
        assert_eq!(answer.status, 200);
        assert_eq!(answer.body, b"ok");
        assert_eq!(answer.notable, [Notable::InterimClaimsBody]);
        assert!(!answer.shared());
    }

    #[test]
    fn a_101_is_not_a_message_this_slice_carries_on_with() {
        let answer = whole(b"HTTP/1.1 101 Switching Protocols\r\nupgrade: h2c\r\n\r\n");
        assert_eq!(answer.status, 101);
        assert_eq!(answer.framing, Framing::None);
        assert_eq!(answer.notable, [Notable::Upgrade]);
        // It ends the run of interim answers rather than joining it: whatever follows is
        // not HTTP/1.1 and is nobody's to read.
        assert!(answer.interim.is_empty());
    }

    #[test]
    fn a_body_delimited_by_the_close_needs_the_close() {
        let bytes = b"HTTP/1.1 200 OK\r\n\r\nas much as there is";
        assert_eq!(read(bytes, Asked::Anything, false), Reading::Unfinished);
        let answer = whole(bytes);
        assert_eq!(answer.framing, Framing::ToClose);
        assert_eq!(answer.body, b"as much as there is");
        assert_eq!(answer.boundary, bytes.len());
    }

    #[test]
    fn only_a_carriage_return_and_a_line_feed_together_end_a_line() {
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\ncontent-length: 0\r\n\r\n"),
            Invalid::BareLineFeed
        );
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\rcontent-length: 0\r\n\r\n"),
            Invalid::BareCarriageReturn
        );
        // A bare line feed in the body is the body's business.
        let answer = whole(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n\n\n");
        assert_eq!(answer.body, b"\n\n");
    }

    #[test]
    fn a_head_that_never_ends_has_not_all_arrived() {
        assert_eq!(
            read(
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n",
                Asked::Anything,
                true
            ),
            Reading::Unfinished
        );
        assert_eq!(read(b"", Asked::Anything, true), Reading::Unfinished);
        // A head that stops in the middle of its line ending has not all arrived
        // either: a return at the very end of what has been said is not a stray one,
        // because the byte that would settle it has not come yet.
        assert_eq!(
            read(
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r",
                Asked::Anything,
                true
            ),
            Reading::Unfinished
        );
        assert_eq!(
            read(b"HTTP/1.1 200 OK\r", Asked::Anything, true),
            Reading::Unfinished
        );
        // Once something that is not a line feed follows it, it is a stray return.
        assert_eq!(refused(b"HTTP/1.1 200 OK\rx"), Invalid::BareCarriageReturn);
    }

    #[test]
    fn a_field_line_is_a_name_a_colon_and_a_value() {
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\r\n x-a: 1\r\n\r\n"),
            Invalid::ObsFold
        );
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\r\nx-a : 1\r\n\r\n"),
            Invalid::SpaceBeforeColon
        );
        assert_eq!(refused(b"HTTP/1.1 200 OK\r\nx-a\r\n\r\n"), Invalid::NoColon);
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\r\n: 1\r\n\r\n"),
            Invalid::FieldName
        );
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\r\nx(a): 1\r\n\r\n"),
            Invalid::FieldName
        );
        assert_eq!(
            refused(b"HTTP/1.1 200 OK\r\nx-a: \x01\r\n\r\n"),
            Invalid::FieldValue
        );
        // The value is trimmed of its optional whitespace and the name is lowered; a
        // value's own spaces stay where they are.
        let answer = whole(b"HTTP/1.1 200 OK\r\nX-A: \t one two \t\r\ncontent-length: 0\r\n\r\n");
        assert_eq!(fields(&answer)[0], ("x-a", "one two"));
    }

    #[test]
    fn the_status_line_is_the_grammar_and_nothing_looser() {
        assert_eq!(refused(b"HTTP/2.0 200 OK\r\n\r\n"), Invalid::Version);
        assert_eq!(refused(b"HTTP/1.9 200 OK\r\n\r\n"), Invalid::Version);
        assert_eq!(refused(b"200 OK\r\n\r\n"), Invalid::Version);
        assert_eq!(refused(b"HTTP/1.1 20 OK\r\n\r\n"), Invalid::StatusCode);
        assert_eq!(refused(b"HTTP/1.1 2000 OK\r\n\r\n"), Invalid::StatusLine);
        assert_eq!(refused(b"HTTP/1.1 2x0 OK\r\n\r\n"), Invalid::StatusCode);
        // The space before the reason phrase is required of a sender even when the
        // phrase is absent, so a line that stops after the code is malformed.
        assert_eq!(refused(b"HTTP/1.1 200\r\n\r\n"), Invalid::StatusLine);
        // An empty reason phrase, with its space, is a status line.
        let answer = whole(b"HTTP/1.1 200 \r\ncontent-length: 0\r\n\r\n");
        assert_eq!((answer.status, answer.reason.as_str()), (200, ""));
    }

    #[test]
    fn http_1_0_is_framed_by_its_own_rules() {
        let answer = whole(b"HTTP/1.0 200 OK\r\ncontent-length: 2\r\n\r\nok");
        assert_eq!(answer.version, Version::Ten);
        assert_eq!(answer.body, b"ok");
        // No keep-alive, so HTTP says the connection is over.
        assert!(!answer.persistent);
        assert_eq!(answer.notable, [Notable::Http10]);
        // With one, HTTP says it is not — and this slice still will not pool it, which
        // is what the note is for rather than a lie about the specification.
        let asked =
            whole(b"HTTP/1.0 200 OK\r\nconnection: keep-alive\r\ncontent-length: 0\r\n\r\n");
        assert!(asked.persistent);
        assert_eq!(asked.notable, [Notable::Http10]);
    }

    #[test]
    fn a_connection_that_says_close_is_not_persistent() {
        let answer =
            whole(b"HTTP/1.1 200 OK\r\nconnection: keep-alive, Close\r\ncontent-length: 0\r\n\r\n");
        // Case and the rest of the list make no difference to what `close` means.
        assert!(!answer.persistent);
        assert!(whole(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n").persistent);
    }

    #[test]
    fn a_coding_that_is_not_one_chunked_is_outside_this_slice() {
        // Chunked last is still chunked, and a chain of them is still noted.
        let chained = whole(
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: gzip, chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n",
        );
        assert_eq!(chained.framing, Framing::Chunked);
        assert_eq!(chained.notable, [Notable::UnsupportedCoding]);
        // Chunked anywhere else leaves a response delimited by the close.
        let misplaced =
            whole(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked, gzip\r\n\r\nwhatever");
        assert_eq!(misplaced.framing, Framing::ToClose);
        assert_eq!(misplaced.notable, [Notable::UnsupportedCoding]);
        // And a coding on an HTTP/1.0 answer is not something to guess about.
        let old =
            whole(b"HTTP/1.0 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n");
        assert_eq!(old.notable, [Notable::Http10, Notable::UnsupportedCoding]);
    }

    #[test]
    fn what_is_measured_is_what_this_projects_bounds_are_about() {
        let answer = whole(
            b"HTTP/1.1 100 Continue\r\n\r\n\
              HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
              2;padding=xxxxxxxx\r\nhi\r\n0\r\nx-a: 1\r\n\r\n",
        );
        assert_eq!(answer.measured.interim_heads, 1);
        assert_eq!(answer.measured.interim_bytes, 25);
        assert_eq!(answer.measured.head, 47);
        assert_eq!(answer.measured.fields, 1);
        // The longest size line, extensions included and the terminator not.
        assert_eq!(answer.measured.chunk_line, 18);
        assert_eq!(answer.measured.trailers, 10);
        assert_eq!(answer.measured.trailer_fields, 1);
    }

    #[test]
    fn a_trailer_section_is_read_by_the_same_rules_as_a_head() {
        let bytes = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n0\r\nx-a 1\r\n\r\n";
        assert_eq!(refused(bytes), Invalid::NoColon);
        let folded = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n0\r\n x-a: 1\r\n\r\n";
        assert_eq!(refused(folded), Invalid::ObsFold);
        // Nothing is filtered here: what a path may forward is that path's policy, and
        // an oracle that had already dropped a field could not tell whether it had.
        let denied =
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n0\r\ncontent-length: 5\r\n\r\n";
        assert_eq!(
            whole(denied).trailers,
            [("content-length".to_owned(), "5".to_owned())]
        );
    }
}
