//! A TLS ClientHello, read and not answered: as much of it as routes a connection by the
//! name its client asks for ([17 §3](../../../../docs/17-tcp-and-tls-passthrough.md)).
//!
//! [`read`] is handed every byte the client has sent so far and says whether that is not
//! enough yet, a whole ClientHello and the name it asks for (if any), or something not to be
//! routed. Asked again with more bytes, it reads them all again: a ClientHello is read once a
//! connection and is a few kilobytes, and a reader that keeps nothing between calls is one
//! that splitting the same bytes differently cannot lead to another answer. The bytes stay
//! the connection's, to go to the backend unchanged.
//!
//! What it reads, it holds to what BoringSSL, the likeliest backend, holds it to, and it
//! refuses where BoringSSL would: a connection is never routed by a name that the backend
//! reads otherwise, or not at all. So the handshake message may span records (RFC 8446
//! §5.1), but no other record may come between its fragments, and none may be empty or
//! longer than 2^14 bytes. Every length inside the ClientHello must add up exactly. No
//! extension may come twice, and `server_name` must hold exactly one `host_name` (RFC 6066
//! §3, and BoringSSL's reading of it). Beyond BoringSSL, the name must be one that routing
//! can match: a DNS host name and not an address, which is also what RFC 6066 allows.

use std::borrow::Cow;
use std::net::IpAddr;

/// The most of a connection read for its ClientHello, which must be whole within it:
/// NGINX's `preread_buffer_size`, and several times what a ClientHello with post-quantum key
/// shares takes.
pub const LIMIT: usize = 16 << 10;

/// A record's header: content type, legacy version, length.
const RECORD_HEADER: usize = 5;
/// The longest record payload a peer may send (RFC 8446 §5.1).
const MAX_RECORD: usize = 1 << 14;
/// The content type of a handshake record.
const HANDSHAKE: u8 = 22;
/// A handshake message's header: type and a 24-bit length.
const MESSAGE_HEADER: usize = 4;
const CLIENT_HELLO: u8 = 1;
const SERVER_NAME: u16 = 0;
const HOST_NAME: u8 = 0;
/// The longest name DNS has room for.
const MAX_NAME: usize = 253;
const MAX_LABEL: usize = 63;

/// What the bytes a client has sent so far come to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hello {
    /// Not enough yet: the ClientHello is not whole, and may still be within [`LIMIT`].
    More,
    /// A whole ClientHello, and the host name it asks for, in ASCII lower case, if it asks
    /// for one.
    Whole(Option<String>),
    /// Nothing to route by.
    Refused(Refusal),
}

/// Why a connection's first bytes are not routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The first record is not a TLS handshake record.
    #[error("not a TLS handshake")]
    NotTls,
    /// The first handshake message is not a ClientHello.
    #[error("the first handshake message is not a ClientHello")]
    NotClientHello,
    /// No whole ClientHello within [`LIMIT`] bytes.
    #[error("no whole ClientHello within the first 16 KiB")]
    TooLarge,
    /// A record, or the ClientHello in them, does not parse.
    #[error("the ClientHello does not parse")]
    Malformed,
    /// An extension comes more than once.
    #[error("an extension of the ClientHello comes more than once")]
    RepeatedExtension,
    /// The name asked for is not one host name.
    #[error("the server name asked for is not a host name")]
    BadName,
}

/// Reads the ClientHello at the start of `bytes`, everything a client has sent so far.
/// Only the first [`LIMIT`] bytes are looked at.
#[must_use]
pub fn read(bytes: &[u8]) -> Hello {
    let seen = bytes.get(..LIMIT).unwrap_or(bytes);
    match message(seen) {
        Gathered::More if bytes.len() >= LIMIT => Hello::Refused(Refusal::TooLarge),
        Gathered::More => Hello::More,
        Gathered::Refused(refusal) => Hello::Refused(refusal),
        Gathered::Whole(message) => {
            match client_hello(message.get(MESSAGE_HEADER..).unwrap_or(&[])) {
                Ok(name) => Hello::Whole(name),
                Err(refusal) => Hello::Refused(refusal),
            }
        }
    }
}

/// The first handshake message, as far as the records in `bytes` hold it.
enum Gathered<'a> {
    More,
    Refused(Refusal),
    /// The whole message, header included: borrowed when one record holds it.
    Whole(Cow<'a, [u8]>),
}

/// Finds the first handshake message across the records it spans, and copies it together
/// only when it spans more than one.
fn message(bytes: &[u8]) -> Gathered<'_> {
    let mut head = [0_u8; MESSAGE_HEADER];
    let mut gathered = 0;
    let mut whole = None;
    for fragment in Fragments::new(bytes) {
        let fragment = match fragment {
            Ok(fragment) => fragment,
            Err(refusal) => return Gathered::Refused(refusal),
        };
        if gathered < MESSAGE_HEADER {
            let take = (MESSAGE_HEADER - gathered).min(fragment.len());
            head[gathered..gathered + take].copy_from_slice(&fragment[..take]);
        }
        gathered += fragment.len();
        if gathered >= 1 && head[0] != CLIENT_HELLO {
            return Gathered::Refused(Refusal::NotClientHello);
        }
        if gathered >= MESSAGE_HEADER {
            let length = MESSAGE_HEADER
                + (usize::from(head[1]) << 16 | usize::from(head[2]) << 8 | usize::from(head[3]));
            // Even in a single record, a message this long is not whole within the limit.
            if RECORD_HEADER + length > LIMIT {
                return Gathered::Refused(Refusal::TooLarge);
            }
            if gathered >= length {
                whole = Some(length);
                break;
            }
        }
    }
    let Some(length) = whole else {
        return Gathered::More;
    };
    let mut fragments = Fragments::new(bytes).map_while(Result::ok);
    let Some(first) = fragments.next() else {
        return Gathered::More;
    };
    if let Some(message) = first.get(..length) {
        return Gathered::Whole(Cow::Borrowed(message));
    }
    let mut message = Vec::with_capacity(length);
    for fragment in std::iter::once(first).chain(fragments) {
        let take = (length - message.len()).min(fragment.len());
        message.extend_from_slice(&fragment[..take]);
        if message.len() == length {
            break;
        }
    }
    Gathered::Whole(Cow::Owned(message))
}

/// The handshake payloads of the records at the start of `bytes`, the last as much of it as
/// has come; a record that may not be there ends them with its refusal.
struct Fragments<'a> {
    bytes: &'a [u8],
    at: usize,
    ended: bool,
}

impl<'a> Fragments<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            at: 0,
            ended: false,
        }
    }
}

impl<'a> Iterator for Fragments<'a> {
    type Item = Result<&'a [u8], Refusal>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        let header = self.bytes.get(self.at..self.at + RECORD_HEADER)?;
        let first = self.at == 0;
        let refused = |refusal| {
            if first { Refusal::NotTls } else { refusal }
        };
        // A record of another type between a handshake message's fragments is not allowed
        // (RFC 8446 §5.1); as the first record, it is not TLS at all.
        if header[0] != HANDSHAKE || header[1] != 3 {
            self.ended = true;
            return Some(Err(refused(Refusal::Malformed)));
        }
        let length = usize::from(u16::from_be_bytes([header[3], header[4]]));
        // Empty handshake fragments and oversized records are both forbidden (§5.1).
        if length == 0 || length > MAX_RECORD {
            self.ended = true;
            return Some(Err(Refusal::Malformed));
        }
        let start = self.at + RECORD_HEADER;
        let end = start + length;
        let payload = self.bytes.get(start..end.min(self.bytes.len()))?;
        if end > self.bytes.len() {
            // As much of the record as has come.
            self.ended = true;
        }
        self.at = end;
        Some(Ok(payload))
    }
}

/// Reads a ClientHello's body: the name its `server_name` extension asks for, if it has one.
fn client_hello(body: &[u8]) -> Result<Option<String>, Refusal> {
    let malformed = Refusal::Malformed;
    let mut hello = Cursor(body);
    // legacy_version and random.
    hello.take(2 + 32).ok_or(malformed)?;
    let session = hello.prefixed8().ok_or(malformed)?;
    let suites = hello.prefixed16().ok_or(malformed)?;
    let compression = hello.prefixed8().ok_or(malformed)?;
    if session.len() > 32 || suites.is_empty() || suites.len() % 2 != 0 || compression.is_empty() {
        return Err(malformed);
    }
    // A ClientHello with no extensions at all is TLS 1.2's to send, and asks for no name.
    if hello.is_empty() {
        return Ok(None);
    }
    let mut extensions = Cursor(hello.prefixed16().ok_or(malformed)?);
    if !hello.is_empty() {
        return Err(malformed);
    }
    let mut kinds = Vec::new();
    let mut server_name = None;
    while !extensions.is_empty() {
        let kind = extensions.u16().ok_or(malformed)?;
        let data = extensions.prefixed16().ok_or(malformed)?;
        kinds.push(kind);
        if kind == SERVER_NAME {
            server_name = Some(data);
        }
    }
    kinds.sort_unstable();
    if kinds.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Refusal::RepeatedExtension);
    }
    server_name.map(name_asked_for).transpose()
}

/// The host name a `server_name` extension holds. BoringSSL reads the list as exactly one
/// entry, whatever RFC 6066 once meant to allow, and so does this.
fn name_asked_for(data: &[u8]) -> Result<String, Refusal> {
    let malformed = Refusal::Malformed;
    let mut extension = Cursor(data);
    let mut list = Cursor(extension.prefixed16().ok_or(malformed)?);
    let kind = list.u8().ok_or(malformed)?;
    let name = list.prefixed16().ok_or(malformed)?;
    if !extension.is_empty() || !list.is_empty() {
        return Err(malformed);
    }
    if kind != HOST_NAME {
        return Err(Refusal::BadName);
    }
    host_name(name).ok_or(Refusal::BadName)
}

/// `name` in ASCII lower case, if it is a DNS host name: labels of letters, digits, hyphens
/// and underscores (which the HTTP listeners' hosts may hold too), none empty, no trailing
/// dot (RFC 6066 §3), and not an address.
fn host_name(name: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(name).ok()?;
    let label = |label: &str| {
        !label.is_empty()
            && label.len() <= MAX_LABEL
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    };
    let is_host_name = !name.is_empty()
        && name.len() <= MAX_NAME
        && name.split('.').all(label)
        && name.parse::<IpAddr>().is_err();
    is_host_name.then(|| name.to_ascii_lowercase())
}

/// Reads a ClientHello's fields front to back.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let taken = self.0.get(..length)?;
        self.0 = &self.0[length..];
        Some(taken)
    }

    fn u8(&mut self) -> Option<u8> {
        let (&[byte], rest) = self.0.split_first_chunk::<1>()?;
        self.0 = rest;
        Some(byte)
    }

    fn u16(&mut self) -> Option<u16> {
        let (&bytes, rest) = self.0.split_first_chunk::<2>()?;
        self.0 = rest;
        Some(u16::from_be_bytes(bytes))
    }

    /// A field with a one-byte length before it.
    fn prefixed8(&mut self) -> Option<&'a [u8]> {
        let length = self.u8()?;
        self.take(usize::from(length))
    }

    /// A field with a two-byte length before it.
    fn prefixed16(&mut self) -> Option<&'a [u8]> {
        let length = self.u16()?;
        self.take(usize::from(length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::h3_peer::certificate;
    use boring::ssl::{
        HandshakeError, NameType, SslAcceptor, SslConnector, SslMethod, SslVerifyMode,
    };
    use proptest::prelude::*;
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Mutex};

    /// A transport that keeps what is written to it and has nothing to read but `input`.
    struct Wire {
        input: Vec<u8>,
        output: Vec<u8>,
    }

    impl Read for Wire {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.input.is_empty() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let n = buf.len().min(self.input.len());
            buf[..n].copy_from_slice(&self.input[..n]);
            self.input.drain(..n);
            Ok(n)
        }
    }

    impl Write for Wire {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// What BoringSSL's client sends first, asking for `name` or for none: its ClientHello,
    /// post-quantum key share and all.
    fn boring_hello(name: Option<&str>) -> Vec<u8> {
        let connector = SslConnector::builder(SslMethod::tls()).unwrap().build();
        let mut config = connector.configure().unwrap();
        config.set_verify_hostname(false);
        config.set_use_server_name_indication(name.is_some());
        let ssl = config.into_ssl(name.unwrap_or("unused.test")).unwrap();
        let wire = Wire {
            input: Vec::new(),
            output: Vec::new(),
        };
        match ssl.connect(wire) {
            Err(HandshakeError::WouldBlock(mid)) => mid.get_ref().output.clone(),
            Err(error) => panic!("the client failed: {error}"),
            Ok(_) => panic!("a handshake with nobody finished"),
        }
    }

    /// What BoringSSL's server makes of `bytes` as a client's first: `None` if it refused
    /// them before it looked for a name, else the name it read, if any, as bytes (it takes
    /// names that are not UTF-8).
    fn boring_reads(bytes: &[u8]) -> Option<Option<Vec<u8>>> {
        let (leaf, key) = certificate();
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
        acceptor.set_certificate(&leaf).unwrap();
        acceptor.set_private_key(&key).unwrap();
        acceptor.set_verify(SslVerifyMode::NONE);
        let read: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::default();
        let keep = Arc::clone(&read);
        acceptor.set_servername_callback(move |ssl, _alert| {
            let name = ssl.servername_raw(NameType::HOST_NAME).map(<[u8]>::to_vec);
            *keep.lock().unwrap() = Some(name);
            Ok(())
        });
        let wire = Wire {
            input: bytes.to_vec(),
            output: Vec::new(),
        };
        let _ = acceptor.build().accept(wire);
        read.lock().unwrap().clone()
    }

    /// `message` in handshake records, cut where `cuts` says.
    fn records(message: &[u8], cuts: &[usize]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut from = 0;
        for &to in cuts.iter().chain(std::iter::once(&message.len())) {
            let fragment = &message[from..to];
            out.extend_from_slice(&[HANDSHAKE, 3, 1]);
            out.extend_from_slice(&u16::try_from(fragment.len()).unwrap().to_be_bytes());
            out.extend_from_slice(fragment);
            from = to;
        }
        out
    }

    /// The handshake message in a one-record ClientHello.
    fn message_of(record: &[u8]) -> &[u8] {
        let length = usize::from(u16::from_be_bytes([record[3], record[4]]));
        assert_eq!(record.len(), RECORD_HEADER + length, "one record");
        &record[RECORD_HEADER..]
    }

    /// A ClientHello's fields, to be changed and put back together.
    #[derive(Clone)]
    struct Parts {
        /// legacy_version, random, session ID, cipher suites and compression methods, as
        /// they were sent.
        front: Vec<u8>,
        extensions: Vec<(u16, Vec<u8>)>,
    }

    impl Parts {
        fn of(message: &[u8]) -> Self {
            let body = &message[MESSAGE_HEADER..];
            let mut hello = Cursor(body);
            hello.take(34).unwrap();
            hello.prefixed8().unwrap();
            hello.prefixed16().unwrap();
            hello.prefixed8().unwrap();
            let front = body[..body.len() - hello.0.len()].to_vec();
            let mut all = Cursor(hello.prefixed16().unwrap());
            let mut extensions = Vec::new();
            while !all.is_empty() {
                let kind = all.u16().unwrap();
                extensions.push((kind, all.prefixed16().unwrap().to_vec()));
            }
            Self { front, extensions }
        }

        /// The message, header and all.
        fn message(&self) -> Vec<u8> {
            let mut extensions = Vec::new();
            for (kind, data) in &self.extensions {
                extensions.extend_from_slice(&kind.to_be_bytes());
                extensions.extend_from_slice(&u16::try_from(data.len()).unwrap().to_be_bytes());
                extensions.extend_from_slice(data);
            }
            let mut body = self.front.clone();
            body.extend_from_slice(&u16::try_from(extensions.len()).unwrap().to_be_bytes());
            body.extend_from_slice(&extensions);
            let length = u32::try_from(body.len()).unwrap().to_be_bytes();
            let mut message = vec![CLIENT_HELLO, length[1], length[2], length[3]];
            message.extend_from_slice(&body);
            message
        }

        fn record(&self) -> Vec<u8> {
            records(&self.message(), &[])
        }

        /// The same with its `server_name` extension's data replaced, or taken out.
        fn named(&self, data: Option<Vec<u8>>) -> Self {
            let mut parts = self.clone();
            parts.extensions.retain(|(kind, _)| *kind != SERVER_NAME);
            if let Some(data) = data {
                parts.extensions.insert(0, (SERVER_NAME, data));
            }
            parts
        }
    }

    /// A `server_name` extension's data holding `entries`, each a name type and a name.
    fn server_name(entries: &[(u8, &[u8])]) -> Vec<u8> {
        let mut list = Vec::new();
        for (kind, name) in entries {
            list.push(*kind);
            list.extend_from_slice(&u16::try_from(name.len()).unwrap().to_be_bytes());
            list.extend_from_slice(name);
        }
        let mut data = u16::try_from(list.len()).unwrap().to_be_bytes().to_vec();
        data.extend_from_slice(&list);
        data
    }

    fn named(name: &str) -> Hello {
        Hello::Whole(Some(name.to_owned()))
    }

    fn refused(refusal: Refusal) -> Hello {
        Hello::Refused(refusal)
    }

    /// A real ClientHello is read whole, and every part of it short of the whole is not
    /// enough yet; with no name asked for, it is whole and names none.
    #[test]
    fn a_real_client_hello_is_read_whole_and_not_before() {
        for (asked, read_as) in [
            (Some("api.example.com"), named("api.example.com")),
            (None, Hello::Whole(None)),
        ] {
            let hello = boring_hello(asked);
            // Post-quantum key shares make it large, but not past one record.
            assert!(hello.len() > 1_000, "{} bytes", hello.len());
            assert_eq!(read(&hello), read_as);
            for cut in 0..hello.len() {
                assert_eq!(read(&hello[..cut]), Hello::More, "cut at {cut}");
            }
            // What comes after it is the backend's business.
            let mut more = hello.clone();
            more.extend_from_slice(&[23, 3, 3, 0, 1, 0]);
            assert_eq!(read(&more), read_as);
        }
    }

    /// The handshake message is followed across records, wherever they cut it, its header
    /// included, and even with a record for every byte.
    #[test]
    fn a_client_hello_across_records_reads_the_same() {
        let hello = boring_hello(Some("api.example.com"));
        let message = message_of(&hello);
        for cut in 1..message.len() {
            let bytes = records(message, &[cut]);
            assert_eq!(read(&bytes), named("api.example.com"), "cut at {cut}");
            // Whole only once the last byte has come.
            assert_eq!(read(&bytes[..bytes.len() - 1]), Hello::More, "cut at {cut}");
        }
        let every_byte: Vec<usize> = (1..message.len()).collect();
        let bytes = records(message, &every_byte);
        assert!(bytes.len() < LIMIT, "{} bytes", bytes.len());
        assert_eq!(read(&bytes), named("api.example.com"));
    }

    /// What is not a TLS handshake that starts with a ClientHello is refused as soon as it
    /// can be told.
    #[test]
    fn what_is_not_a_client_hello_is_refused() {
        assert_eq!(read(b"GET / HTTP/1.1\r\n"), refused(Refusal::NotTls));
        // An SSL 2 ClientHello, which carries no name.
        assert_eq!(read(&[0x80, 0x2e, 1, 3, 1]), refused(Refusal::NotTls));
        assert_eq!(read(&[23, 3, 3, 0, 1, 0]), refused(Refusal::NotTls));
        assert_eq!(read(&[HANDSHAKE, 2, 0, 0, 1, 1]), refused(Refusal::NotTls));
        // A ServerHello, and its first byte is enough to tell.
        assert_eq!(
            read(&[HANDSHAKE, 3, 3, 0, 4, 2]),
            refused(Refusal::NotClientHello)
        );
        // An empty fragment, and a record past 2^14 bytes.
        assert_eq!(read(&[HANDSHAKE, 3, 1, 0, 0]), refused(Refusal::Malformed));
        assert_eq!(
            read(&[HANDSHAKE, 3, 1, 0x40, 1]),
            refused(Refusal::Malformed)
        );
        // A record of another type between two fragments of the message, even one that
        // carries the very byte that belongs there.
        let hello = boring_hello(Some("a.test"));
        let message = message_of(&hello);
        let mut interleaved = records(&message[..100], &[]);
        interleaved.extend_from_slice(&[20, 3, 3, 0, 1, message[100]]);
        interleaved.extend_from_slice(&records(&message[101..], &[]));
        assert_eq!(read(&interleaved), refused(Refusal::Malformed));
        assert_eq!(read(&[]), Hello::More);
        assert_eq!(read(&[HANDSHAKE, 3]), Hello::More);
    }

    /// A ClientHello not whole within the limit is refused: at once when its header says it
    /// cannot be, otherwise once the limit has been read. One record of exactly the limit is
    /// within it.
    #[test]
    fn a_client_hello_past_the_limit_is_refused() {
        let header = |length: usize| {
            let length = u32::try_from(length).unwrap().to_be_bytes();
            [CLIENT_HELLO, length[1], length[2], length[3]]
        };
        let mut too_long = vec![HANDSHAKE, 3, 1, 0x40, 0];
        too_long.extend_from_slice(&header(LIMIT));
        assert_eq!(read(&too_long), refused(Refusal::TooLarge));

        // The longest message one record of the limit's size holds: whole at the limit,
        // and read (these zeros are no ClientHello).
        let mut message = header(LIMIT - RECORD_HEADER - MESSAGE_HEADER).to_vec();
        message.resize(LIMIT - RECORD_HEADER, 0);
        let one = records(&message, &[]);
        assert_eq!(one.len(), LIMIT);
        assert_eq!(read(&one[..LIMIT - 1]), Hello::More);
        assert_eq!(read(&one), refused(Refusal::Malformed));
        // The same message in two records is five bytes past it.
        let two = records(&message, &[100]);
        assert_eq!(read(&two[..LIMIT - 1]), Hello::More);
        assert_eq!(read(&two[..LIMIT]), refused(Refusal::TooLarge));
        assert_eq!(read(&two), refused(Refusal::TooLarge));
    }

    /// Every length inside a ClientHello must add up, and an extension may come once; where
    /// these are refused, BoringSSL refuses them too.
    #[test]
    fn a_client_hello_that_does_not_add_up_is_refused_as_boring_refuses_it() {
        let parts = Parts::of(message_of(&boring_hello(Some("api.example.com"))));
        assert_eq!(read(&parts.record()), named("api.example.com"));
        assert_eq!(
            boring_reads(&parts.record()),
            Some(Some(b"api.example.com".to_vec()))
        );

        let mut cases: Vec<(&str, Vec<u8>, Refusal)> = Vec::new();
        let mut twice = parts.clone();
        let supported_groups = twice
            .extensions
            .iter()
            .find(|(kind, _)| *kind == 10)
            .cloned()
            .unwrap();
        twice.extensions.push(supported_groups);
        cases.push((
            "an extension twice",
            twice.record(),
            Refusal::RepeatedExtension,
        ));
        let mut names_twice = parts.clone();
        names_twice
            .extensions
            .push((SERVER_NAME, server_name(&[(HOST_NAME, b"api.example.com")])));
        cases.push((
            "server_name twice",
            names_twice.record(),
            Refusal::RepeatedExtension,
        ));
        let mut message = parts.message();
        message.push(0);
        let length = u32::try_from(message.len() - MESSAGE_HEADER)
            .unwrap()
            .to_be_bytes();
        message[1..4].copy_from_slice(&length[1..]);
        cases.push((
            "a byte after the extensions",
            records(&message, &[]),
            Refusal::Malformed,
        ));
        let mut message = parts.message();
        // The extensions' own length a byte short of what follows.
        let at = MESSAGE_HEADER + parts.front.len();
        let length = u16::from_be_bytes([message[at], message[at + 1]]) - 1;
        message[at..at + 2].copy_from_slice(&length.to_be_bytes());
        cases.push((
            "extensions a byte short",
            records(&message, &[]),
            Refusal::Malformed,
        ));
        let two = server_name(&[(HOST_NAME, b"api.example.com"), (HOST_NAME, b"b.test")]);
        cases.push((
            "two names",
            parts.named(Some(two)).record(),
            Refusal::Malformed,
        ));
        let mut trailing = server_name(&[(HOST_NAME, b"api.example.com")]);
        trailing.push(0);
        cases.push((
            "a byte after the name list",
            parts.named(Some(trailing)).record(),
            Refusal::Malformed,
        ));
        cases.push((
            "a name of another type",
            parts
                .named(Some(server_name(&[(1, b"api.example.com")])))
                .record(),
            Refusal::BadName,
        ));
        cases.push((
            "an empty name",
            parts.named(Some(server_name(&[(HOST_NAME, b"")]))).record(),
            Refusal::BadName,
        ));
        cases.push((
            "a name with a NUL",
            parts
                .named(Some(server_name(&[(HOST_NAME, b"a\0.test")])))
                .record(),
            Refusal::BadName,
        ));
        for (case, bytes, refusal) in cases {
            assert_eq!(read(&bytes), refused(refusal), "{case}");
            assert_eq!(boring_reads(&bytes), None, "BoringSSL takes {case}");
        }
    }

    /// The name must be one routing can match: a DNS host name, lower-cased, never an
    /// address or a name with a trailing dot. BoringSSL takes all of these, and reads the
    /// name as it was sent.
    #[test]
    fn the_name_asked_for_is_a_host_name_in_lower_case() {
        let parts = Parts::of(message_of(&boring_hello(Some("api.example.com"))));
        let with = |name: &[u8]| {
            parts
                .named(Some(server_name(&[(HOST_NAME, name)])))
                .record()
        };
        for (name, read_as) in [
            ("API.Example.COM", named("api.example.com")),
            ("my_service.internal", named("my_service.internal")),
            ("xn--caf-dma.example", named("xn--caf-dma.example")),
            ("localhost", named("localhost")),
            ("10.0.0.1", refused(Refusal::BadName)),
            ("::1", refused(Refusal::BadName)),
            ("example.com.", refused(Refusal::BadName)),
            ("a..example.com", refused(Refusal::BadName)),
            (".example.com", refused(Refusal::BadName)),
            ("*.example.com", refused(Refusal::BadName)),
            ("a b.example.com", refused(Refusal::BadName)),
            ("café.example", refused(Refusal::BadName)),
        ] {
            let bytes = with(name.as_bytes());
            assert_eq!(read(&bytes), read_as, "{name}");
            assert_eq!(
                boring_reads(&bytes),
                Some(Some(name.as_bytes().to_vec())),
                "{name}"
            );
        }
        let label = "a".repeat(MAX_LABEL);
        assert_eq!(read(&with(label.as_bytes())), named(&label));
        let longer = "a".repeat(MAX_LABEL + 1);
        assert_eq!(read(&with(longer.as_bytes())), refused(Refusal::BadName));
        let longest = [label.as_str(); 4].join(".")[..MAX_NAME].to_owned();
        assert_eq!(read(&with(longest.as_bytes())), named(&longest));
        let past = format!("{longest}a");
        assert_eq!(read(&with(past.as_bytes())), refused(Refusal::BadName));
        // Bytes that are not UTF-8, which BoringSSL takes as they are (the fuzz target
        // found that its `servername` then says there is no name).
        let not_utf8 = with(b"a\xff.test");
        assert_eq!(read(&not_utf8), refused(Refusal::BadName));
        assert_eq!(boring_reads(&not_utf8), Some(Some(b"a\xff.test".to_vec())));
        // No server_name at all asks for no name.
        assert_eq!(read(&parts.named(None).record()), Hello::Whole(None));
        assert_eq!(boring_reads(&parts.named(None).record()), Some(None));
    }

    /// A ClientHello of TLS 1.2's with no extensions at all is whole and names nothing; its
    /// fields are held to their lengths all the same.
    #[test]
    fn a_client_hello_without_extensions_names_nothing() {
        let mut body = vec![3, 3];
        body.extend_from_slice(&[7; 32]);
        // No session ID, one cipher suite, one compression method.
        body.extend_from_slice(&[0, 0, 2, 0xc0, 0x2f, 1, 0]);
        let length = u32::try_from(body.len()).unwrap().to_be_bytes();
        let mut message = vec![CLIENT_HELLO, length[1], length[2], length[3]];
        message.extend_from_slice(&body);
        assert_eq!(read(&records(&message, &[])), Hello::Whole(None));
        // A session ID of 33 bytes, cipher suites of one byte or none, no compression.
        for (at, value) in [(38, 33), (40, 1), (40, 0), (43, 0)] {
            let mut broken = message.clone();
            broken[at] = value;
            assert_eq!(
                read(&records(&broken, &[])),
                refused(Refusal::Malformed),
                "byte {at} as {value}"
            );
        }
    }

    /// The reader as written in the plainest way: every handshake fragment copied together
    /// first, then the message read field by field. What `read` finds, this must find too.
    fn reference(bytes: &[u8]) -> Hello {
        let bytes = &bytes[..bytes.len().min(LIMIT)];
        let mut message: Vec<u8> = Vec::new();
        let mut at = 0;
        let mut needed = None;
        loop {
            if let Some(needed) = needed
                && message.len() >= needed
            {
                break;
            }
            if bytes.len() < at + 5 {
                return if bytes.len() == LIMIT {
                    refused(Refusal::TooLarge)
                } else {
                    Hello::More
                };
            }
            if bytes[at] != 22 || bytes[at + 1] != 3 {
                return refused(if at == 0 {
                    Refusal::NotTls
                } else {
                    Refusal::Malformed
                });
            }
            let length = usize::from(bytes[at + 3]) * 256 + usize::from(bytes[at + 4]);
            if length == 0 || length > 16_384 {
                return refused(Refusal::Malformed);
            }
            let end = (at + 5 + length).min(bytes.len());
            message.extend_from_slice(&bytes[at + 5..end]);
            if !message.is_empty() && message[0] != 1 {
                return refused(Refusal::NotClientHello);
            }
            if needed.is_none() && message.len() >= 4 {
                let length = 4
                    + usize::from(message[1]) * 65_536
                    + usize::from(message[2]) * 256
                    + usize::from(message[3]);
                if length + 5 > LIMIT {
                    return refused(Refusal::TooLarge);
                }
                needed = Some(length);
            }
            at += 5 + length;
        }
        let Some(needed) = needed else {
            unreachable!("the loop ends only once the message is whole");
        };
        let body = &message[4..needed];
        // A length-prefixed field at `i`, its length `width` bytes wide.
        let field = |i: &mut usize, width: usize| -> Option<Vec<u8>> {
            let length = if width == 1 {
                usize::from(*body.get(*i)?)
            } else {
                usize::from(*body.get(*i)?) * 256 + usize::from(*body.get(*i + 1)?)
            };
            let data = body.get(*i + width..*i + width + length)?.to_vec();
            *i += width + length;
            Some(data)
        };
        if body.len() < 34 {
            return refused(Refusal::Malformed);
        }
        let mut i = 34;
        let (Some(session), Some(suites), Some(compression)) =
            (field(&mut i, 1), field(&mut i, 2), field(&mut i, 1))
        else {
            return refused(Refusal::Malformed);
        };
        if session.len() > 32
            || suites.is_empty()
            || suites.len() % 2 == 1
            || compression.is_empty()
        {
            return refused(Refusal::Malformed);
        }
        if i == body.len() {
            return Hello::Whole(None);
        }
        let Some(extensions) = field(&mut i, 2) else {
            return refused(Refusal::Malformed);
        };
        if i != body.len() {
            return refused(Refusal::Malformed);
        }
        let mut j = 0;
        let mut kinds = Vec::new();
        let mut sni = None;
        while j < extensions.len() {
            if j + 4 > extensions.len() {
                return refused(Refusal::Malformed);
            }
            let kind = u16::from(extensions[j]) * 256 + u16::from(extensions[j + 1]);
            let length = usize::from(extensions[j + 2]) * 256 + usize::from(extensions[j + 3]);
            let Some(data) = extensions.get(j + 4..j + 4 + length) else {
                return refused(Refusal::Malformed);
            };
            kinds.push(kind);
            if kind == 0 {
                sni = Some(data.to_vec());
            }
            j += 4 + length;
        }
        for a in 0..kinds.len() {
            for b in a + 1..kinds.len() {
                if kinds[a] == kinds[b] {
                    return refused(Refusal::RepeatedExtension);
                }
            }
        }
        let Some(sni) = sni else {
            return Hello::Whole(None);
        };
        if sni.len() < 2 || usize::from(sni[0]) * 256 + usize::from(sni[1]) != sni.len() - 2 {
            return refused(Refusal::Malformed);
        }
        let list = &sni[2..];
        if list.len() < 3 || usize::from(list[1]) * 256 + usize::from(list[2]) != list.len() - 3 {
            return refused(Refusal::Malformed);
        }
        if list[0] != 0 {
            return refused(Refusal::BadName);
        }
        let name = &list[3..];
        let text: String = name.iter().map(|&b| char::from(b)).collect();
        let good = !name.is_empty()
            && name.len() <= 253
            && name.is_ascii()
            && text.split('.').all(|label| {
                (1..=63).contains(&label.len())
                    && label
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            })
            && text.parse::<IpAddr>().is_err();
        if good {
            Hello::Whole(Some(text.to_ascii_lowercase()))
        } else {
            refused(Refusal::BadName)
        }
    }

    /// A real ClientHello with the name changed to any of the characters a name might hold,
    /// or not, and its message cut into records anywhere.
    fn hellos() -> impl Strategy<Value = Vec<u8>> {
        let template = message_of(&boring_hello(Some("api.example.com"))).to_vec();
        let names = prop::string::string_regex("[a-zA-Z0-9_. -]{0,70}").unwrap();
        (names, prop::collection::vec(1_usize..2_000, 0..6)).prop_map(move |(name, cuts)| {
            let parts =
                Parts::of(&template).named(Some(server_name(&[(HOST_NAME, name.as_bytes())])));
            let message = parts.message();
            let mut cuts: Vec<usize> = cuts.into_iter().filter(|&c| c < message.len()).collect();
            cuts.sort_unstable();
            cuts.dedup();
            records(&message, &cuts)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// On ClientHellos however cut, on them damaged anywhere, and on any bytes at all,
        /// `read` finds what the plainest reader finds.
        #[test]
        fn read_agrees_with_the_plain_reader(
            hello in hellos(),
            noise in prop::collection::vec(any::<u8>(), 0..64),
            at in any::<prop::sample::Index>(),
        ) {
            prop_assert_eq!(read(&hello), reference(&hello));
            prop_assert_eq!(read(&noise), reference(&noise));
            let mut damaged = hello.clone();
            let from = at.index(hello.len());
            damaged[from..].iter_mut().zip(&noise).for_each(|(byte, &n)| *byte ^= n);
            prop_assert_eq!(read(&damaged), reference(&damaged));
        }

        /// Once `read` has decided, more bytes change nothing; before, every shorter part
        /// is not enough.
        #[test]
        fn a_decision_stands_as_more_comes(hello in hellos(), cut in any::<prop::sample::Index>()) {
            let decided = read(&hello);
            prop_assert_ne!(&decided, &Hello::More);
            prop_assert_eq!(read(&hello[..cut.index(hello.len())]), Hello::More);
            let mut more = hello.clone();
            more.extend_from_slice(b"anything at all");
            prop_assert_eq!(read(&more), decided);
        }
    }
}
