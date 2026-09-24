//! A scripted HTTP/2 peer: frames written and read one at a time, as a test says.
//!
//! A server or client library answers the way a well-behaved peer does. The cases that
//! matter most for limits are the ones it will not produce: SETTINGS that shrink a limit
//! mid-stream, a GOAWAY that leaves some streams unprocessed, a reset of a stream the other
//! side has not accepted yet, CONTINUATION without end, SETTINGS never acknowledged. This
//! peer writes whatever frame it is told to and reports every frame it reads, so a test can
//! assert on the wire rather than on what a library chose to surface (15 §3, §8).
//!
//! It is deliberately not a codec. What it sends is encoded in the plainest form HPACK has —
//! literal fields, never indexed, never Huffman — and what it receives is left as bytes:
//! the frame header is parsed, the payload is kept whole, and only the frames whose
//! payload a test needs (SETTINGS, GOAWAY, RST_STREAM, WINDOW_UPDATE, PING) are read
//! further. Every read is bounded in time and size, so a peer that waits for a frame that
//! never comes fails the test instead of hanging it.

#![allow(
    dead_code,
    reason = "a toolkit: each test binary that includes it uses a different part"
)]

use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// What a client says first (RFC 9113 §3.4).
pub(crate) const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The largest payload the peer will read. A test that provokes a larger frame is testing
/// something this peer is not for.
const MAX_PAYLOAD: usize = 1 << 20;

/// How long a read waits before the test is failed rather than left hanging.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// Frame types (RFC 9113 §6).
pub(crate) mod kind {
    pub(crate) const DATA: u8 = 0x0;
    pub(crate) const HEADERS: u8 = 0x1;
    pub(crate) const PRIORITY: u8 = 0x2;
    pub(crate) const RST_STREAM: u8 = 0x3;
    pub(crate) const SETTINGS: u8 = 0x4;
    pub(crate) const PUSH_PROMISE: u8 = 0x5;
    pub(crate) const PING: u8 = 0x6;
    pub(crate) const GOAWAY: u8 = 0x7;
    pub(crate) const WINDOW_UPDATE: u8 = 0x8;
    pub(crate) const CONTINUATION: u8 = 0x9;
}

/// Frame flags (RFC 9113 §6).
pub(crate) mod flag {
    pub(crate) const END_STREAM: u8 = 0x1;
    pub(crate) const ACK: u8 = 0x1;
    pub(crate) const END_HEADERS: u8 = 0x4;
}

/// SETTINGS identifiers (RFC 9113 §6.5.2).
pub(crate) mod setting {
    pub(crate) const HEADER_TABLE_SIZE: u16 = 0x1;
    pub(crate) const ENABLE_PUSH: u16 = 0x2;
    pub(crate) const MAX_CONCURRENT_STREAMS: u16 = 0x3;
    pub(crate) const INITIAL_WINDOW_SIZE: u16 = 0x4;
    pub(crate) const MAX_FRAME_SIZE: u16 = 0x5;
    pub(crate) const MAX_HEADER_LIST_SIZE: u16 = 0x6;
}

/// Error codes (RFC 9113 §7).
pub(crate) mod code {
    pub(crate) const NO_ERROR: u32 = 0x0;
    pub(crate) const PROTOCOL_ERROR: u32 = 0x1;
    pub(crate) const INTERNAL_ERROR: u32 = 0x2;
    pub(crate) const FLOW_CONTROL_ERROR: u32 = 0x3;
    pub(crate) const STREAM_CLOSED: u32 = 0x5;
    pub(crate) const REFUSED_STREAM: u32 = 0x7;
    pub(crate) const CANCEL: u32 = 0x8;
    pub(crate) const COMPRESSION_ERROR: u32 = 0x9;
    pub(crate) const ENHANCE_YOUR_CALM: u32 = 0xb;
}

/// One frame, as read or as it is to be written.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Frame {
    pub(crate) kind: u8,
    pub(crate) flags: u8,
    pub(crate) stream: u32,
    pub(crate) payload: Vec<u8>,
}

impl std::fmt::Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self.kind {
            kind::DATA => "DATA",
            kind::HEADERS => "HEADERS",
            kind::PRIORITY => "PRIORITY",
            kind::RST_STREAM => "RST_STREAM",
            kind::SETTINGS => "SETTINGS",
            kind::PUSH_PROMISE => "PUSH_PROMISE",
            kind::PING => "PING",
            kind::GOAWAY => "GOAWAY",
            kind::WINDOW_UPDATE => "WINDOW_UPDATE",
            kind::CONTINUATION => "CONTINUATION",
            _ => "UNKNOWN",
        };
        write!(
            f,
            "{name}(stream {}, flags {:#x}, {} bytes)",
            self.stream,
            self.flags,
            self.payload.len()
        )
    }
}

impl Frame {
    pub(crate) fn new(kind: u8, flags: u8, stream: u32, payload: Vec<u8>) -> Frame {
        Frame {
            kind,
            flags,
            stream,
            payload,
        }
    }

    /// The frame as it goes on the wire: the nine-byte header, then the payload.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let length = u32::try_from(self.payload.len()).expect("payload fits a frame");
        assert!(length < 1 << 24, "payload fits a frame");
        let mut out = Vec::with_capacity(9 + self.payload.len());
        out.extend_from_slice(&length.to_be_bytes()[1..]);
        out.push(self.kind);
        out.push(self.flags);
        out.extend_from_slice(&(self.stream & 0x7fff_ffff).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub(crate) fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    /// A SETTINGS frame's parameters, in the order sent.
    pub(crate) fn settings(&self) -> Vec<(u16, u32)> {
        assert_eq!(self.kind, kind::SETTINGS, "{self:?} is not SETTINGS");
        self.payload
            .as_chunks::<6>()
            .0
            .iter()
            .map(|p| {
                (
                    u16::from_be_bytes([p[0], p[1]]),
                    u32::from_be_bytes([p[2], p[3], p[4], p[5]]),
                )
            })
            .collect()
    }

    /// A GOAWAY frame's last stream identifier and error code.
    pub(crate) fn goaway(&self) -> (u32, u32) {
        assert_eq!(self.kind, kind::GOAWAY, "{self:?} is not GOAWAY");
        let p = &self.payload;
        (
            u32::from_be_bytes([p[0], p[1], p[2], p[3]]) & 0x7fff_ffff,
            u32::from_be_bytes([p[4], p[5], p[6], p[7]]),
        )
    }

    /// A RST_STREAM frame's error code.
    pub(crate) fn reset(&self) -> u32 {
        assert_eq!(self.kind, kind::RST_STREAM, "{self:?} is not RST_STREAM");
        let p = &self.payload;
        u32::from_be_bytes([p[0], p[1], p[2], p[3]])
    }

    /// A WINDOW_UPDATE frame's increment.
    pub(crate) fn increment(&self) -> u32 {
        assert_eq!(
            self.kind,
            kind::WINDOW_UPDATE,
            "{self:?} is not WINDOW_UPDATE"
        );
        let p = &self.payload;
        u32::from_be_bytes([p[0], p[1], p[2], p[3]]) & 0x7fff_ffff
    }
}

pub(crate) fn settings(parameters: &[(u16, u32)]) -> Frame {
    let mut payload = Vec::with_capacity(parameters.len() * 6);
    for (id, value) in parameters {
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&value.to_be_bytes());
    }
    Frame::new(kind::SETTINGS, 0, 0, payload)
}

pub(crate) fn settings_ack() -> Frame {
    Frame::new(kind::SETTINGS, flag::ACK, 0, Vec::new())
}

/// A HEADERS frame carrying a whole header block.
pub(crate) fn headers(stream: u32, block: Vec<u8>, end_stream: bool) -> Frame {
    let flags = flag::END_HEADERS | if end_stream { flag::END_STREAM } else { 0 };
    Frame::new(kind::HEADERS, flags, stream, block)
}

pub(crate) fn data(stream: u32, bytes: &[u8], end_stream: bool) -> Frame {
    let flags = if end_stream { flag::END_STREAM } else { 0 };
    Frame::new(kind::DATA, flags, stream, bytes.to_vec())
}

pub(crate) fn rst_stream(stream: u32, code: u32) -> Frame {
    Frame::new(kind::RST_STREAM, 0, stream, code.to_be_bytes().to_vec())
}

pub(crate) fn goaway(last_stream: u32, code: u32) -> Frame {
    let mut payload = (last_stream & 0x7fff_ffff).to_be_bytes().to_vec();
    payload.extend_from_slice(&code.to_be_bytes());
    Frame::new(kind::GOAWAY, 0, 0, payload)
}

pub(crate) fn window_update(stream: u32, increment: u32) -> Frame {
    Frame::new(
        kind::WINDOW_UPDATE,
        0,
        stream,
        increment.to_be_bytes().to_vec(),
    )
}

pub(crate) fn ping(payload: [u8; 8]) -> Frame {
    Frame::new(kind::PING, 0, 0, payload.to_vec())
}

/// An HPACK integer with an `n`-bit prefix, the prefix's other bits `first` (RFC 7541 §5.1).
fn integer(out: &mut Vec<u8>, first: u8, n: u32, mut value: usize) {
    let max = (1usize << n) - 1;
    if value < max {
        out.push(first | u8::try_from(value).expect("under the prefix"));
        return;
    }
    out.push(first | u8::try_from(max).expect("the prefix"));
    value -= max;
    while value >= 128 {
        out.push(u8::try_from(value % 128).expect("seven bits") | 0x80);
        value /= 128;
    }
    out.push(u8::try_from(value).expect("seven bits"));
}

/// A header block of literal fields, each "without indexing" with a literal name and
/// without Huffman coding (RFC 7541 §6.2.2): nothing enters either side's dynamic table,
/// so every block stands alone and the order of blocks never matters.
pub(crate) fn block(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in fields {
        out.push(0x00);
        integer(&mut out, 0x00, 7, name.len());
        out.extend_from_slice(name.as_bytes());
        integer(&mut out, 0x00, 7, value.len());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

/// A request's header block, for a peer that is a client.
pub(crate) fn request(method: &str, path: &str) -> Vec<u8> {
    block(&[
        (":method", method),
        (":scheme", "http"),
        (":authority", "example.com"),
        (":path", path),
    ])
}

/// A response's header block, for a peer that is a server.
pub(crate) fn response(status: u16) -> Vec<u8> {
    block(&[(":status", &status.to_string())])
}

/// One end of a connection, driven by the test.
pub(crate) struct Peer<S> {
    io: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Peer<S> {
    pub(crate) fn new(io: S) -> Peer<S> {
        Peer { io }
    }

    /// Opens the connection as a client does: the preface and a SETTINGS frame.
    pub(crate) async fn open_as_client(io: S, parameters: &[(u16, u32)]) -> Peer<S> {
        let mut peer = Peer::new(io);
        peer.write_raw(PREFACE).await;
        peer.send(&settings(parameters)).await;
        peer
    }

    /// Accepts a connection as a server does: reads the client's preface, sends SETTINGS,
    /// and returns the client's first SETTINGS frame, which is what it asked for.
    pub(crate) async fn accept_as_server(io: S, parameters: &[(u16, u32)]) -> (Peer<S>, Frame) {
        let mut peer = Peer::new(io);
        let mut preface = [0u8; PREFACE.len()];
        within(peer.io.read_exact(&mut preface))
            .await
            .expect("preface");
        assert_eq!(preface, PREFACE, "the client's preface");
        peer.send(&settings(parameters)).await;
        let first = peer.next().await;
        assert_eq!(first.kind, kind::SETTINGS, "a client's first frame");
        (peer, first)
    }

    pub(crate) async fn send(&mut self, frame: &Frame) {
        self.write_raw(&frame.encode()).await;
    }

    pub(crate) async fn write_raw(&mut self, bytes: &[u8]) {
        within(self.io.write_all(bytes)).await.expect("write");
        within(self.io.flush()).await.expect("flush");
    }

    /// The next frame, which must come within [`PATIENCE`].
    pub(crate) async fn next(&mut self) -> Frame {
        match self.try_next().await {
            Some(frame) => frame,
            None => panic!("the connection closed while a frame was expected"),
        }
    }

    /// The next frame, or `None` once the other side has closed the connection. Fails the
    /// test if neither comes within [`PATIENCE`].
    pub(crate) async fn try_next(&mut self) -> Option<Frame> {
        read_frame(&mut self.io).await
    }

    /// The next frame that `wanted` accepts; the ones before it are returned too, so a test
    /// can say what may and may not come first.
    pub(crate) async fn until(&mut self, wanted: impl Fn(&Frame) -> bool) -> (Frame, Vec<Frame>) {
        let mut before = Vec::new();
        loop {
            let frame = self.next().await;
            if wanted(&frame) {
                return (frame, before);
            }
            assert!(
                before.len() < 10_000,
                "no wanted frame in 10,000: {before:?}"
            );
            before.push(frame);
        }
    }

    /// Sends a PING and returns every frame that came before its acknowledgement. A
    /// connection reads frames in order, so once the acknowledgement is back, everything
    /// sent before the PING has been read and acted on: a test can then look at the other
    /// side's state without sleeping and hoping. A connection the other side has closed is
    /// settled too: the frames up to the close are returned.
    pub(crate) async fn barrier(&mut self) -> Vec<Frame> {
        const MARK: [u8; 8] = *b"barrier!";
        // A PING the other side cannot take any more is not a failure: the read below
        // finds the close.
        let _ = within(self.io.write_all(&ping(MARK).encode())).await;
        let _ = within(self.io.flush()).await;
        let mut before = Vec::new();
        while let Some(frame) = self.try_next().await {
            if frame.kind == kind::PING && frame.has(flag::ACK) && frame.payload == MARK {
                break;
            }
            assert!(
                before.len() < 10_000,
                "no acknowledgement in 10,000: {before:?}"
            );
            before.push(frame);
        }
        before
    }

    /// Every frame the other side has queued by now, for asserting that something was *not*
    /// sent. One barrier is not enough: h2 answers a PING before it writes work its
    /// streams queued in the same turn, such as a WINDOW_UPDATE after a local release. The
    /// turn that answers the first PING takes that work, so it is out before the second
    /// acknowledgement.
    pub(crate) async fn settled(&mut self) -> Vec<Frame> {
        let mut frames = self.barrier().await;
        frames.extend(self.barrier().await);
        frames
    }

    /// Sends a frame to a side that may have closed the connection already, as a probe
    /// that pushes until the other side gives up does.
    pub(crate) async fn send_if_open(&mut self, frame: &Frame) {
        let _ = within(self.io.write_all(&frame.encode())).await;
        let _ = within(self.io.flush()).await;
    }

    /// Every frame until the other side closes the connection.
    pub(crate) async fn rest(&mut self) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some(frame) = self.try_next().await {
            frames.push(frame);
            assert!(frames.len() < 100_000, "the other side never closes");
        }
        frames
    }

    /// Whatever frames arrive within `quiet`, returning once nothing has come for that long.
    /// For asserting that something is *not* sent: absence has to be given a time.
    pub(crate) async fn drain_for(&mut self, quiet: Duration) -> Vec<Frame> {
        let mut frames = Vec::new();
        loop {
            match tokio::time::timeout(quiet, self.try_next()).await {
                Ok(Some(frame)) => frames.push(frame),
                Ok(None) | Err(_) => return frames,
            }
            assert!(frames.len() < 100_000, "the other side never goes quiet");
        }
    }

    /// The socket, for a test that has to read or write before speaking HTTP/2.
    pub(crate) fn io_mut(&mut self) -> &mut S {
        &mut self.io
    }

    /// The socket, for a test that goes on reading and writing it separately.
    pub(crate) fn into_inner(self) -> S {
        self.io
    }
}

/// The next frame read from `io`, or `None` once it is closed. Fails the test if neither
/// comes within [`PATIENCE`]. For a test that reads a half it has split off.
pub(crate) async fn read_frame(io: &mut (impl AsyncRead + Unpin)) -> Option<Frame> {
    let mut head = [0u8; 9];
    match within(io.read_exact(&mut head)).await {
        Ok(_) => {}
        Err(e) if is_closed(&e) => return None,
        Err(e) => panic!("reading a frame: {e}"),
    }
    let length = usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
    assert!(
        length <= MAX_PAYLOAD,
        "a {length}-byte frame is beyond this peer"
    );
    let mut payload = vec![0u8; length];
    within(io.read_exact(&mut payload))
        .await
        .expect("a frame's payload");
    Some(Frame {
        kind: head[3],
        flags: head[4],
        stream: u32::from_be_bytes([head[5], head[6], head[7], head[8]]) & 0x7fff_ffff,
        payload,
    })
}

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(PATIENCE, future)
        .await
        .expect("the scripted peer waited too long")
}

fn is_closed(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe
    )
}
