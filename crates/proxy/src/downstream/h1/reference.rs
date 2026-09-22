//! The specification as code: a slow, obvious reader of a client's request, for the
//! differential harness to judge the request reader and the connection driver against.
//!
//! Nothing here is shared with [`super::codec`] or with the body reader both sides use,
//! and nothing here minds its cost. It is written from
//! [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html) and
//! [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) rather than from the code it
//! checks: two implementations of one specification disagree where at least one of them
//! is wrong. It is given the bytes whole, rescans whatever it likes, and says what the
//! request means and where it ends ([14 §9](../../../../docs/14-downstream-server.md)).
//!
//! **It has no opinions of this project's own.** Where the specification leaves a choice
//! open, this reads the request the permissive way and records that the choice was there
//! ([`Notable`]); where a bound is EdgeRush's rather than the specification's, this
//! applies none of it and measures what the bound is about ([`Measured`]). So "ours
//! refused it" and "it is not a request" stay separate questions, as on the upstream side.

/// Which HTTP the request was written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// `HTTP/1.0`.
    Ten,
    /// `HTTP/1.1`.
    Eleven,
}

/// How the request's body is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// There is none ([RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3)
    /// rule 6: a request with neither field has no body).
    None,
    /// This many bytes of it.
    Length(u64),
    /// In chunks, ending with a zero chunk and a trailer section.
    Chunked,
}

/// A place where the specification leaves a choice open. None of these makes the request
/// unreadable; each is one this project's policy refuses (14 §4), so a request that raises
/// one is outside what the candidate undertakes to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Notable {
    /// More than one empty line before the request line.
    /// [RFC 9112 §2.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-2.2) has a
    /// server ignore "at least one"; read here as all of them.
    EmptyLinesFirst,
    /// The same `Content-Length` more than once, which
    /// [RFC 9110 §8.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.6) lets a
    /// recipient reject or make one.
    RepeatedLength,
    /// `Transfer-Encoding` beside `Content-Length`: RFC 9112 §6.3 rule 3 lets a server
    /// reject it or read it by the coding alone, which is how it is read here.
    LengthWithCoding,
    /// A coding before the final `chunked`. The body can be delimited, but its content is
    /// still coded, and RFC 9112 §6.1 has a server that does not understand a coding
    /// answer 501.
    UnknownCoding,
    /// A `Connection` value that is not a list of tokens, which
    /// [RFC 9110 §7.6.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-7.6.1)'s
    /// grammar does not allow and §5.5 gives a recipient no rule for.
    ConnectionNotTokens,
}

/// What the bytes came to, measured rather than judged, for the harness to hold against
/// this project's bounds (14 §8).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Measured {
    /// The request line, its line ending included.
    pub request_line: usize,
    /// Where the head ends, counted from the first byte, empty lines before it included.
    pub head: usize,
    /// How many fields it carried.
    pub fields: usize,
}

/// The body, where all of it is there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    /// Its bytes, chunk framing removed.
    pub data: Vec<u8>,
    /// The fields after the last chunk, lowered in name, in order and unfiltered.
    pub trailers: Vec<(String, String)>,
    /// Where the request ends, counted from the first byte. What follows is the next
    /// request's.
    pub end: usize,
}

/// What the reference made of a body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyReading {
    /// All of it is there.
    Whole(Body),
    /// Not all of it is there yet.
    Unfinished,
    /// Not a body: its framing cannot be read.
    Invalid(Invalid),
}

/// A request, as the specification reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Which HTTP it was written in.
    pub version: Version,
    /// Its method, as it came.
    pub method: String,
    /// Its target, as it came.
    pub target: String,
    /// Its fields, lowered in name, in the order they came, repeats kept.
    pub fields: Vec<(String, String)>,
    /// How its body is delimited.
    pub framing: Framing,
    /// Whether HTTP leaves the connection open for another request, from the version and
    /// the `Connection` options alone.
    pub persistent: bool,
    /// Its body, judged on its own: a head can be sound and the body after it not.
    pub body: BodyReading,
    /// Where the specification left a choice open.
    pub notable: Vec<Notable>,
    /// What it came to.
    pub measured: Measured,
}

/// Why the bytes are not a request that can be read at all: the specification's own
/// refusals, not this project's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalid {
    /// A line feed with no carriage return before it: refused here as the smuggling
    /// shape it is, though RFC 9112 §2.2 lets a recipient recognise one.
    BareLineFeed,
    /// A carriage return with no line feed after it.
    BareCarriageReturn,
    /// Not `method SP request-target SP HTTP-version`.
    RequestLine,
    /// A target that is not one.
    Target,
    /// A version written as one that is not 1.0 or 1.1.
    UnsupportedVersion,
    /// A field line continued onto the next ([RFC 9112 §5.2]).
    ObsFold,
    /// A field name that is not a token.
    FieldName,
    /// Whitespace between a field name and its colon ([RFC 9112 §5.1]).
    SpaceBeforeColon,
    /// A field line with no colon.
    NoColon,
    /// A field value with a byte no field value has.
    FieldValue,
    /// A `Content-Length` that is not a plain count, or two that disagree
    /// (RFC 9112 §6.3 rule 5).
    Length,
    /// A transfer coding on HTTP/1.0 (RFC 9112 §6.1).
    CodingOnHttp10,
    /// A coding list that does not end with `chunked` (RFC 9112 §6.3 rule 4).
    NotChunkedLast,
    /// `chunked` more than once (RFC 9112 §7).
    ChunkedTwice,
    /// A coding that is not a token.
    Coding,
    /// A chunk that is not one.
    Chunk,
}

/// What the reference made of the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reading {
    /// A whole head, and what it means.
    Read(Box<Request>),
    /// Not a request. More bytes would not help.
    Invalid(Invalid),
    /// The head has not ended yet.
    Unfinished,
}

/// Reads the first request in `bytes`.
#[must_use]
pub fn read(bytes: &[u8]) -> Reading {
    match reading(bytes) {
        Ok(Some(request)) => Reading::Read(Box::new(request)),
        Ok(None) => Reading::Unfinished,
        Err(invalid) => Reading::Invalid(invalid),
    }
}

/// The lines of a section of fields, and where the section ends.
type Section<'a> = (Vec<&'a [u8]>, usize);

/// `tchar`, from RFC 9110 §5.6.2.
fn tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

/// Splits `bytes` from `from` into lines until the empty line that ends a section, and
/// says where it ends; `None` if it has not ended. Every line ending must be CRLF.
fn section(bytes: &[u8], from: usize) -> Result<Option<Section<'_>>, Invalid> {
    let mut lines = Vec::new();
    let mut start = from;
    let mut at = from;
    while at < bytes.len() {
        match bytes[at] {
            b'\n' => return Err(Invalid::BareLineFeed),
            b'\r' => match bytes.get(at + 1) {
                None => return Ok(None),
                Some(b'\n') => {
                    let line = &bytes[start..at];
                    if line.is_empty() {
                        return Ok(Some((lines, at + 2)));
                    }
                    lines.push(line);
                    at += 2;
                    start = at;
                }
                Some(_) => return Err(Invalid::BareCarriageReturn),
            },
            _ => at += 1,
        }
    }
    Ok(None)
}

/// A field line, `name ":" OWS value OWS`, with its name lowered.
fn field(line: &[u8]) -> Result<(String, String), Invalid> {
    if line
        .first()
        .is_some_and(|byte| *byte == b' ' || *byte == b'\t')
    {
        return Err(Invalid::ObsFold);
    }
    let colon = line
        .iter()
        .position(|byte| *byte == b':')
        .ok_or(Invalid::NoColon)?;
    let name = &line[..colon];
    if name
        .last()
        .is_some_and(|byte| *byte == b' ' || *byte == b'\t')
    {
        return Err(Invalid::SpaceBeforeColon);
    }
    if name.is_empty() || !name.iter().copied().all(tchar) {
        return Err(Invalid::FieldName);
    }
    let value = line[colon + 1..].trim_ascii();
    if !value
        .iter()
        .all(|byte| *byte == b'\t' || (b' '..=b'~').contains(byte) || *byte >= 0x80)
    {
        return Err(Invalid::FieldValue);
    }
    Ok((
        String::from_utf8_lossy(name).to_ascii_lowercase(),
        String::from_utf8_lossy(value).into_owned(),
    ))
}

/// The members of every `name` field, split at commas, trimmed, empty ones left out.
fn members(fields: &[(String, String)], name: &str) -> Vec<String> {
    fields
        .iter()
        .filter(|(field, _)| field == name)
        .flat_map(|(_, value)| value.split(','))
        .map(|member| member.trim().to_owned())
        .filter(|member| !member.is_empty())
        .collect()
}

fn reading(bytes: &[u8]) -> Result<Option<Request>, Invalid> {
    let mut notable = Vec::new();
    let mut at = 0;
    let mut empty = 0;
    while bytes[at..].starts_with(b"\r\n") {
        at += 2;
        empty += 1;
    }
    if empty > 1 {
        notable.push(Notable::EmptyLinesFirst);
    }
    // The request line is read, and judged, before anything after it: what is wrong with
    // a request is what is wrong first in the order it was sent.
    let Some(line_end) = bytes[at..]
        .iter()
        .position(|byte| *byte == b'\n' || *byte == b'\r')
        .map(|found| at + found)
    else {
        return Ok(None);
    };
    match (bytes[line_end], bytes.get(line_end + 1)) {
        (b'\n', _) => return Err(Invalid::BareLineFeed),
        (_, None) => return Ok(None),
        (_, Some(b'\n')) => {}
        (_, Some(_)) => return Err(Invalid::BareCarriageReturn),
    }
    let request_line = &bytes[at..line_end];

    let parts: Vec<&[u8]> = request_line.split(|byte| *byte == b' ').collect();
    let [method, target, version] = parts[..] else {
        return Err(Invalid::RequestLine);
    };
    if method.is_empty() || !method.iter().copied().all(tchar) {
        return Err(Invalid::RequestLine);
    }
    if target.is_empty()
        || !target
            .iter()
            .all(|byte| (0x21..=0x7e).contains(byte) || *byte >= 0x80)
    {
        return Err(Invalid::RequestLine);
    }
    let version = match version {
        b"HTTP/1.1" => Version::Eleven,
        b"HTTP/1.0" => Version::Ten,
        [b'H', b'T', b'T', b'P', b'/', major, b'.', minor]
            if major.is_ascii_digit() && minor.is_ascii_digit() =>
        {
            return Err(Invalid::UnsupportedVersion);
        }
        _ => return Err(Invalid::RequestLine),
    };
    let target = std::str::from_utf8(target).map_err(|_| Invalid::Target)?;
    target.parse::<http::Uri>().map_err(|_| Invalid::Target)?;

    let Some((field_lines, head_end)) = section(bytes, line_end + 2)? else {
        return Ok(None);
    };
    let fields = field_lines
        .iter()
        .map(|line| field(line))
        .collect::<Result<Vec<_>, _>>()?;

    let lengths: Vec<&str> = fields
        .iter()
        .filter(|(name, _)| name == "content-length")
        .map(|(_, value)| value.as_str())
        .collect();
    let mut length = None;
    for value in &lengths {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Invalid::Length);
        }
        let parsed: u64 = value.parse().map_err(|_| Invalid::Length)?;
        match length {
            Some(earlier) if earlier != parsed => return Err(Invalid::Length),
            Some(_) => notable.push(Notable::RepeatedLength),
            None => length = Some(parsed),
        }
    }

    let coded = fields.iter().any(|(name, _)| name == "transfer-encoding");
    let framing = if coded {
        if version == Version::Ten {
            return Err(Invalid::CodingOnHttp10);
        }
        if length.is_some() {
            notable.push(Notable::LengthWithCoding);
        }
        let codings = members(&fields, "transfer-encoding");
        if !codings
            .last()
            .is_some_and(|last| last.eq_ignore_ascii_case("chunked"))
        {
            return Err(Invalid::NotChunkedLast);
        }
        let chunked = codings
            .iter()
            .filter(|coding| coding.eq_ignore_ascii_case("chunked"))
            .count();
        if chunked > 1 {
            return Err(Invalid::ChunkedTwice);
        }
        if !codings.iter().all(|coding| coding.bytes().all(tchar)) {
            return Err(Invalid::Coding);
        }
        if codings.len() > 1 {
            notable.push(Notable::UnknownCoding);
        }
        Framing::Chunked
    } else {
        length.map_or(Framing::None, Framing::Length)
    };

    let options = members(&fields, "connection");
    if !options.iter().all(|option| option.bytes().all(tchar)) {
        notable.push(Notable::ConnectionNotTokens);
    }
    let said = |word: &str| {
        options
            .iter()
            .any(|option| option.eq_ignore_ascii_case(word))
    };
    let persistent = !said("close") && (version == Version::Eleven || said("keep-alive"));

    let body = match body(bytes, head_end, framing) {
        Ok(Some(whole)) => BodyReading::Whole(whole),
        Ok(None) => BodyReading::Unfinished,
        Err(invalid) => BodyReading::Invalid(invalid),
    };
    Ok(Some(Request {
        version,
        method: String::from_utf8_lossy(method).into_owned(),
        target: target.to_owned(),
        fields: fields.clone(),
        framing,
        persistent,
        body,
        notable,
        measured: Measured {
            request_line: request_line.len() + 2,
            head: head_end,
            fields: fields.len(),
        },
    }))
}

/// The body after a head ending at `from`, if all of it is there.
fn body(bytes: &[u8], from: usize, framing: Framing) -> Result<Option<Body>, Invalid> {
    match framing {
        Framing::None => Ok(Some(Body {
            data: Vec::new(),
            trailers: Vec::new(),
            end: from,
        })),
        Framing::Length(length) => {
            let end = usize::try_from(length)
                .ok()
                .and_then(|length| from.checked_add(length));
            Ok(end.filter(|end| *end <= bytes.len()).map(|end| Body {
                data: bytes[from..end].to_vec(),
                trailers: Vec::new(),
                end,
            }))
        }
        Framing::Chunked => chunked(bytes, from),
    }
}

/// A chunked body: `size [ext] CRLF data CRLF`, until a size of zero and the trailer
/// section after it.
fn chunked(bytes: &[u8], mut at: usize) -> Result<Option<Body>, Invalid> {
    let mut data = Vec::new();
    loop {
        let Some(line_end) = bytes[at..].windows(2).position(|pair| pair == b"\r\n") else {
            return Ok(None);
        };
        let line = &bytes[at..at + line_end];
        if line.contains(&b'\n') || line.contains(&b'\r') {
            return Err(Invalid::Chunk);
        }
        let digits = line
            .iter()
            .take_while(|byte| byte.is_ascii_hexdigit())
            .count();
        let size = std::str::from_utf8(&line[..digits])
            .ok()
            .and_then(|hex| u64::from_str_radix(hex, 16).ok())
            .ok_or(Invalid::Chunk)?;
        // What may follow the size is extensions, which begin with a semicolon after
        // optional whitespace; they are read no further than that here.
        let rest = line[digits..].trim_ascii_start();
        if !rest.is_empty() && rest[0] != b';' {
            return Err(Invalid::Chunk);
        }
        at += line_end + 2;
        if size == 0 {
            let Some((lines, end)) = section(bytes, at).map_err(|_| Invalid::Chunk)? else {
                return Ok(None);
            };
            let trailers = lines
                .iter()
                .map(|line| field(line))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Invalid::Chunk)?;
            return Ok(Some(Body {
                data,
                trailers,
                end,
            }));
        }
        let size = usize::try_from(size).map_err(|_| Invalid::Chunk)?;
        let Some(after) = at.checked_add(size) else {
            return Err(Invalid::Chunk);
        };
        if bytes.len() < after + 2 {
            return Ok(None);
        }
        if &bytes[after..after + 2] != b"\r\n" {
            return Err(Invalid::Chunk);
        }
        data.extend_from_slice(&bytes[at..after]);
        at = after + 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_whole(bytes: &[u8]) -> Request {
        match read(bytes) {
            Reading::Read(request) => *request,
            other => panic!("{:?}: {other:?}", String::from_utf8_lossy(bytes)),
        }
    }

    #[test]
    fn a_request_is_read_with_its_body_and_where_it_ends() {
        let bytes = b"POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET";
        let request = read_whole(bytes);
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/a");
        assert_eq!(request.fields[0], ("host".to_owned(), "x".to_owned()));
        assert_eq!(request.framing, Framing::Length(3));
        assert!(request.persistent);
        let BodyReading::Whole(body) = request.body else {
            panic!("no whole body");
        };
        assert_eq!(body.data, b"abc");
        assert_eq!(&bytes[body.end..], b"GET");
        assert!(request.notable.is_empty());
    }

    #[test]
    fn a_chunked_body_is_decoded_to_its_trailers() {
        let bytes = b"POST / HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n3;x=y\r\nabc\r\n0\r\nx-t: 1\r\n\r\nrest";
        let BodyReading::Whole(body) = read_whole(bytes).body else {
            panic!("no whole body");
        };
        assert_eq!(body.data, b"abc");
        assert_eq!(body.trailers, [("x-t".to_owned(), "1".to_owned())]);
        assert_eq!(&bytes[body.end..], b"rest");
    }

    #[test]
    fn choices_the_specification_leaves_open_are_read_and_recorded() {
        for (bytes, notable) in [
            (&b"\r\n\r\nGET / HTTP/1.1\r\n\r\n"[..], Notable::EmptyLinesFirst),
            (b"POST / HTTP/1.1\r\ncontent-length: 1\r\ncontent-length: 1\r\n\r\nx", Notable::RepeatedLength),
            (b"POST / HTTP/1.1\r\ncontent-length: 1\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n", Notable::LengthWithCoding),
            (b"POST / HTTP/1.1\r\ntransfer-encoding: gzip, chunked\r\n\r\n0\r\n\r\n", Notable::UnknownCoding),
            (b"GET / HTTP/1.1\r\nconnection: a b\r\n\r\n", Notable::ConnectionNotTokens),
        ] {
            assert_eq!(read_whole(bytes).notable, [notable], "{:?}", String::from_utf8_lossy(bytes));
        }
    }

    #[test]
    fn what_is_no_request_is_invalid() {
        for (bytes, invalid) in [
            (
                &b"GET / HTTP/1.1\nhost: x\r\n\r\n"[..],
                Invalid::BareLineFeed,
            ),
            (b"GET / HTTP/1.1\rx\r\n\r\n", Invalid::BareCarriageReturn),
            (b"GET  / HTTP/1.1\r\n\r\n", Invalid::RequestLine),
            (b"GET / HTTP/2.0\r\n\r\n", Invalid::UnsupportedVersion),
            (b"GET / http/1.1\r\n\r\n", Invalid::RequestLine),
            (b"GET / HTTP/1.1\r\nx: 1\r\n y\r\n\r\n", Invalid::ObsFold),
            (
                b"GET / HTTP/1.1\r\nx : 1\r\n\r\n",
                Invalid::SpaceBeforeColon,
            ),
            (b"GET / HTTP/1.1\r\nx(: 1\r\n\r\n", Invalid::FieldName),
            (b"GET / HTTP/1.1\r\nx\r\n\r\n", Invalid::NoColon),
            (b"GET / HTTP/1.1\r\nx: \x01\r\n\r\n", Invalid::FieldValue),
            (
                b"POST / HTTP/1.1\r\ncontent-length: 1\r\ncontent-length: 2\r\n\r\n",
                Invalid::Length,
            ),
            (
                b"POST / HTTP/1.1\r\ncontent-length: +1\r\n\r\n",
                Invalid::Length,
            ),
            (
                b"POST / HTTP/1.0\r\ntransfer-encoding: chunked\r\n\r\n",
                Invalid::CodingOnHttp10,
            ),
            (
                b"POST / HTTP/1.1\r\ntransfer-encoding: chunked, gzip\r\n\r\n",
                Invalid::NotChunkedLast,
            ),
            (
                b"POST / HTTP/1.1\r\ntransfer-encoding: chunked, chunked\r\n\r\n",
                Invalid::ChunkedTwice,
            ),
        ] {
            assert_eq!(
                read(bytes),
                Reading::Invalid(invalid),
                "{:?}",
                String::from_utf8_lossy(bytes)
            );
        }
    }

    #[test]
    fn a_head_or_body_not_all_there_is_not_yet_a_request() {
        assert_eq!(read(b"GET / HTTP/1.1\r\nhost: x\r\n"), Reading::Unfinished);
        let request = read_whole(b"POST / HTTP/1.1\r\ncontent-length: 5\r\n\r\nab");
        assert_eq!(request.body, BodyReading::Unfinished);
        let request = read_whole(b"POST / HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\nzz\r\n");
        assert_eq!(request.body, BodyReading::Invalid(Invalid::Chunk));
    }

    mod against_the_candidate {
        //! The candidate reader held to the reference on generated requests: where the
        //! reference finds nothing notable and nothing past a bound, the two must agree
        //! exactly; elsewhere the candidate must refuse, and for a reason the reference's
        //! finding accounts for.

        use super::super::*;
        use crate::downstream::h1::codec::{self, Arrival, Head, HeadReader, arrival};
        use crate::h1::{BodyReader, Piece};
        use crate::upstream::h1::H1Limits;
        use proptest::prelude::*;

        fn limits() -> H1Limits {
            H1Limits {
                request_line: 64,
                head: 256,
                fields: 6,
                ..H1Limits::default()
            }
        }

        fn piece() -> impl Strategy<Value = Vec<u8>> {
            prop::sample::select(vec![
                &b"\r\n"[..],
                b"host: a\r\n",
                b"x-a: 1\r\n",
                b"x-a: 2\r\n",
                b"x-b:\t v \r\n",
                b"x-c: \x01\r\n",
                b"x-d: caf\xc3\xa9\r\n",
                b"x : 1\r\n",
                b" folded\r\n",
                b"x\r\n",
                b"content-length: 3\r\n",
                b"content-length: 4\r\n",
                b"content-length: 03\r\n",
                b"content-length: 3, 3\r\n",
                b"transfer-encoding: chunked\r\n",
                b"transfer-encoding: gzip\r\n",
                b"transfer-encoding: gzip, chunked\r\n",
                b"transfer-encoding: chunked, chunked\r\n",
                b"connection: close\r\n",
                b"connection: keep-alive\r\n",
                b"connection: a b\r\n",
                b"trailer: x-t\r\n",
                b"x-long: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n",
                b"x-e: 1\n",
                // Past the head bound in one field, so that the bound is met often and not
                // only when several long fields happen to come together.
                b"x-huge: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n",
            ])
            .prop_map(<[u8]>::to_vec)
        }

        fn request_line() -> impl Strategy<Value = Vec<u8>> {
            prop::sample::select(vec![
                &b"GET / HTTP/1.1\r\n"[..],
                b"POST /a?b=c HTTP/1.1\r\n",
                b"POST http://h/p HTTP/1.1\r\n",
                b"OPTIONS * HTTP/1.1\r\n",
                b"POST / HTTP/1.0\r\n",
                b"\r\nGET / HTTP/1.1\r\n",
                b"\r\n\r\nGET / HTTP/1.1\r\n",
                b"GET / HTTP/1.2\r\n",
                b"GET /aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n",
                b"G(T / HTTP/1.1\r\n",
                b"GET /\x7f HTTP/1.1\r\n",
                b"GET /\x01 HTTP/1.1\r\n",
            ])
            .prop_map(<[u8]>::to_vec)
        }

        fn body() -> impl Strategy<Value = Vec<u8>> {
            prop::sample::select(vec![
                &b""[..],
                b"abc",
                b"abcd",
                b"3\r\nabc\r\n0\r\n\r\n",
                b"3;x=1\r\nabc\r\n0\r\nx-t: 1\r\n\r\n",
                b"0\r\n\r\n",
                b"zz\r\n",
            ])
            .prop_map(<[u8]>::to_vec)
        }

        /// A request line, fields, the empty line, and something after it: always a
        /// whole head, so that what is compared is a verdict on all of it.
        fn request() -> impl Strategy<Value = Vec<u8>> {
            (request_line(), prop::collection::vec(piece(), 0..8), body()).prop_map(
                |(line, pieces, body)| {
                    let mut bytes = line;
                    for piece in pieces {
                        // A lone empty line would end the head early; the head's own
                        // end is added below.
                        if piece != b"\r\n" {
                            bytes.extend_from_slice(&piece);
                        }
                    }
                    bytes.extend_from_slice(b"\r\n");
                    bytes.extend_from_slice(&body);
                    bytes
                },
            )
        }

        /// The statuses a refusal of this reading may carry.
        fn allowed(reading: &Reading, limits: &H1Limits) -> Vec<u16> {
            let mut allowed = Vec::new();
            match reading {
                // Nothing is measured of what is not a request, and a bound can be met
                // before whatever is wrong with it is reached.
                Reading::Invalid(invalid) => {
                    allowed.push(if *invalid == Invalid::UnsupportedVersion {
                        505
                    } else {
                        400
                    });
                    allowed.extend([414, 431]);
                }
                Reading::Unfinished => {}
                Reading::Read(request) => {
                    for notable in &request.notable {
                        allowed.push(match notable {
                            Notable::UnknownCoding => 501,
                            _ => 400,
                        });
                    }
                    if request.measured.request_line > limits.request_line {
                        allowed.push(414);
                    }
                    if request.measured.head > limits.head
                        || request.measured.fields > limits.fields
                    {
                        allowed.push(431);
                    }
                }
            }
            allowed
        }

        /// The candidate's body, read with the reader both sides share, if all of it is
        /// there.
        fn candidate_body(
            bytes: &[u8],
            framing: crate::h1::Framing,
        ) -> Result<Option<(Vec<u8>, usize)>, ()> {
            let limits = limits();
            let mut reader = BodyReader::new(framing);
            let mut at = 0;
            let mut data = Vec::new();
            loop {
                match reader.read(&bytes[at..], false, &limits).map_err(|_| ())? {
                    Piece::More => return Ok(None),
                    Piece::Data {
                        data: range,
                        consumed,
                    } => {
                        data.extend_from_slice(&bytes[at..][range]);
                        at += consumed;
                    }
                    Piece::End { consumed, .. } => return Ok(Some((data, at + consumed))),
                }
            }
        }

        proptest! {
            #[test]
            fn the_candidate_reads_what_the_reference_reads(bytes in request()) {
                let limits = limits();
                let reference = read(&bytes);
                let candidate = HeadReader::default()
                    .read(&bytes, &limits)
                    .and_then(|head| match head {
                        Head::Read { head, consumed } => {
                            arrival(&head).map(|arrival| Some((head, consumed, arrival)))
                        }
                        Head::More => Ok(None),
                    });
                let within = |request: &Request| {
                    request.notable.is_empty()
                        && request.measured.request_line <= limits.request_line
                        && request.measured.head <= limits.head
                        && request.measured.fields <= limits.fields
                };
                match (&reference, candidate) {
                    (Reading::Read(request), Ok(Some((head, consumed, Arrival { framing, persistent }))))
                        if within(request) =>
                    {
                        prop_assert_eq!(consumed, request.measured.head);
                        prop_assert_eq!(head.method.as_str(), request.method.as_str());
                        prop_assert_eq!(head.target.to_string(), request.target.clone());
                        let version = if head.version == http::Version::HTTP_10 { Version::Ten } else { Version::Eleven };
                        prop_assert_eq!(version, request.version);
                        let fields: Vec<(String, String)> = head
                            .headers
                            .iter()
                            .map(|(name, value)| (name.as_str().to_owned(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
                            .collect();
                        let mut expected = request.fields.clone();
                        let mut fields = fields;
                        // A header map groups repeats by name; order within a name is kept.
                        expected.sort_by(|a, b| a.0.cmp(&b.0));
                        fields.sort_by(|a, b| a.0.cmp(&b.0));
                        prop_assert_eq!(fields, expected);
                        let expected_framing = match request.framing {
                            Framing::None => crate::h1::Framing::None,
                            Framing::Length(length) => crate::h1::Framing::Length(length),
                            Framing::Chunked => crate::h1::Framing::Chunked,
                        };
                        prop_assert_eq!(framing, expected_framing);
                        prop_assert_eq!(persistent, request.persistent);
                        // And the body the two readers find, where the reference finds one.
                        let found = candidate_body(&bytes[consumed..], framing);
                        match (&request.body, found) {
                            (BodyReading::Whole(body), Ok(Some((data, used)))) => {
                                prop_assert_eq!(data, body.data.clone());
                                prop_assert_eq!(consumed + used, body.end);
                            }
                            (BodyReading::Unfinished, Ok(None))
                            | (BodyReading::Invalid(_), Err(())) => {}
                            (reference, candidate) => prop_assert!(
                                false,
                                "the body: reference {:?}, candidate {:?}",
                                reference,
                                candidate
                            ),
                        }
                    }
                    (Reading::Read(request), Ok(Some(_))) => {
                        prop_assert!(false, "accepted what is notable or past a bound: {:?}", request);
                    }
                    (_, Ok(Some(_))) => {
                        prop_assert!(false, "accepted what the reference reads as {:?}", reference);
                    }
                    (Reading::Unfinished, Ok(None)) => {}
                    (_, Ok(None)) => {
                        prop_assert!(false, "waited for more of a whole head: {:?}", reference);
                    }
                    (_, Err(error)) => {
                        let status = codec::RequestError::status(error).as_u16();
                        let allowed = allowed(&reference, &limits);
                        prop_assert!(
                            allowed.contains(&status),
                            "refused {:?} ({}) where the reference read {:?}", error, status, reference
                        );
                        prop_assert!(
                            !matches!(&reference, Reading::Read(request) if within(request)),
                            "refused {:?} what the reference reads cleanly: {:?}", error, reference
                        );
                    }
                }
            }
        }
    }
}
