//! What a connection's driver sends: the datagrams quiche makes, handed to the socket in
//! batches where the kernel can cut them apart ([16 §4](../../../../../docs/16-http3.md)).
//!
//! A flush that finds quiche with much to send, a large answer's packets above all, would
//! otherwise make a system call for every packet. On Linux, UDP segmentation offload (GSO,
//! the `UDP_SEGMENT` control message) takes up to 64 datagrams of one size to one address
//! in a single call, the last of them shorter if it must be. HAProxy, NGINX (`quic_gso`),
//! quinn and tokio-quiche all send so. Elsewhere, and on a Linux whose kernel or network
//! card refuses to cut datagrams, each goes in a call of its own.
//!
//! One gathering buffer serves every connection of a worker: a flush never waits, so no
//! two overlap, and a buffer a connection would be 64 KiB for each of thousands.

use crate::downstream::h3::conn::Conn;
use crate::downstream::h3::listener::Shared;
use std::cell::{Cell, RefCell};
use std::io;
use std::net::SocketAddr;
use std::task::Context;

/// The most datagrams one call may carry: Linux's `UDP_MAX_SEGMENTS`.
const MOST_SEGMENTS: usize = 64;

/// The most bytes one call may carry: a UDP datagram's, whose payload the kernel then cuts.
const MOST_BYTES: usize = 65_000;

/// What a worker's connections send through.
pub(crate) struct Sending {
    /// Where a flush gathers what quiche makes.
    gathered: RefCell<Vec<u8>>,
    /// How many datagrams one call may carry: 1 where the kernel does not cut them.
    segments: Cell<usize>,
}

impl Sending {
    /// Sending that cuts datagrams where the platform can.
    pub(crate) fn new() -> Self {
        Self {
            gathered: RefCell::new(Vec::with_capacity(MOST_BYTES)),
            segments: Cell::new(if cfg!(target_os = "linux") {
                MOST_SEGMENTS
            } else {
                1
            }),
        }
    }
}

/// Datagrams quiche made that the socket had no room for, kept in order for the next
/// flush: each a run of datagrams of `segment` bytes to one address, the last maybe shorter.
pub(crate) type Unsent = Vec<Run>;

/// Datagrams to one address, every one `segment` bytes but the last.
pub(crate) struct Run {
    bytes: Vec<u8>,
    segment: usize,
    to: SocketAddr,
}

/// Where the datagram quiche has just made goes, given the run gathered before it, and
/// what a run that is over is cut into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Joins {
    /// Into the run, which may take more.
    Run,
    /// Into the run, which it ends, being shorter or the run full: the run goes, cut into
    /// datagrams of `segment` bytes.
    RunAndEnds { segment: usize },
    /// Not into it, being for another address or longer than the run's datagrams: the run
    /// goes without it, cut into datagrams of `segment` bytes, to `to`, and it begins the
    /// next.
    Next { segment: usize, to: SocketAddr },
}

/// A run being gathered: how many datagrams, of what size, to where.
#[derive(Debug, Default)]
struct Gathering {
    count: usize,
    segment: usize,
    to: Option<SocketAddr>,
}

impl Gathering {
    /// Where a datagram of `len` bytes for `to` goes, `most` datagrams being as many as a
    /// run may hold and `datagram` the largest quiche makes.
    fn admit(&mut self, len: usize, to: SocketAddr, most: usize, datagram: usize) -> Joins {
        let before = std::mem::take(self);
        if let Some(run_to) = before.to
            && (run_to != to || len > before.segment)
        {
            *self = Self {
                count: 1,
                segment: len,
                to: Some(to),
            };
            return Joins::Next {
                segment: before.segment,
                to: run_to,
            };
        }
        let run = Self {
            count: before.count + 1,
            segment: if before.count == 0 {
                len
            } else {
                before.segment
            },
            to: Some(to),
        };
        let full = run.count >= most || (run.count + 1) * datagram > MOST_BYTES;
        if len < run.segment || full {
            return Joins::RunAndEnds {
                segment: run.segment,
            };
        }
        *self = run;
        Joins::Run
    }
}

/// Sends what quiche wants sent, until it has nothing more or the socket has no room;
/// what was left unsent before goes first. `datagram` is the largest quiche makes.
pub(crate) fn flush(
    conn: &Conn,
    shared: &Shared,
    datagram: usize,
    unsent: &mut Unsent,
    cx: &mut Context<'_>,
) {
    while !unsent.is_empty() {
        let run = &unsent[0];
        if !send(shared, &run.bytes, run.segment, run.to, cx) {
            return;
        }
        unsent.remove(0);
    }
    let sending = &shared.sending;
    let mut gathered = sending.gathered.borrow_mut();
    gathered.clear();
    let mut gathering = Gathering::default();
    let most = sending.segments.get();
    loop {
        let start = gathered.len();
        gathered.resize(start + datagram, 0);
        let made = conn.with(|state| state.quic.send(&mut gathered[start..]));
        let Ok((len, info)) = made else {
            gathered.truncate(start);
            break;
        };
        gathered.truncate(start + len);
        match gathering.admit(len, info.to, most, datagram) {
            Joins::Run => {}
            Joins::RunAndEnds { segment } => {
                if !send_or_keep(shared, &gathered, segment, info.to, unsent, cx) {
                    return;
                }
                gathered.clear();
            }
            Joins::Next { segment, to } => {
                if !send_or_keep(shared, &gathered[..start], segment, to, unsent, cx) {
                    // The datagram that did not join is kept too, after the run.
                    unsent.push(Run {
                        bytes: gathered[start..].to_vec(),
                        segment: len,
                        to: info.to,
                    });
                    return;
                }
                gathered.copy_within(start.., 0);
                gathered.truncate(len);
            }
        }
    }
    if let Some(to) = gathering.to
        && !gathered.is_empty()
    {
        send_or_keep(shared, &gathered, gathering.segment, to, unsent, cx);
    }
}

/// Sends a run, or keeps it for the next flush if the socket has no room: false then.
fn send_or_keep(
    shared: &Shared,
    bytes: &[u8],
    segment: usize,
    to: SocketAddr,
    unsent: &mut Unsent,
    cx: &mut Context<'_>,
) -> bool {
    if send(shared, bytes, segment, to, cx) {
        return true;
    }
    unsent.push(Run {
        bytes: bytes.to_vec(),
        segment,
        to,
    });
    false
}

/// Sends a run: false if the socket has no room, which it then says when it has.
fn send(
    shared: &Shared,
    bytes: &[u8],
    segment: usize,
    to: SocketAddr,
    cx: &mut Context<'_>,
) -> bool {
    let sent = if bytes.len() > segment {
        send_segmented(&shared.socket, bytes, segment, to)
    } else {
        shared.socket.try_send_to(bytes, to).map(drop)
    };
    match sent {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            // Ready at once only if room came meanwhile, which the next turn finds.
            let _ready = shared.socket.poll_send_ready(cx);
            false
        }
        Err(error) if bytes.len() > segment && refused_segmenting(&error) => {
            // The kernel or the network card will not cut datagrams (quinn finds this the
            // same way): never ask again, and send these one at a time. Any the socket
            // has no room for are lost, as a datagram may be on any path; quiche sends
            // them again.
            shared.sending.segments.set(1);
            for one in bytes.chunks(segment) {
                let _sent = shared.socket.try_send_to(one, to);
            }
            true
        }
        // Lost, as a datagram may be on any path: quiche sends it again.
        Err(_) => true,
    }
}

/// Whether a segmented send failed because segmenting is not to be had here.
fn refused_segmenting(error: &io::Error) -> bool {
    // EIO: a network card that cannot checksum what it cuts; EINVAL, ENOPROTOOPT: a
    // kernel without UDP_SEGMENT.
    matches!(error.raw_os_error(), Some(5 | 22 | 92))
}

/// The `level` and `type` of the control message that asks for datagrams to be cut
/// (`SOL_UDP`, `UDP_SEGMENT` in Linux's `linux/udp.h`).
#[cfg(target_os = "linux")]
const SOL_UDP: i32 = 17;
#[cfg(target_os = "linux")]
const UDP_SEGMENT: i32 = 103;

/// A control message's parts are aligned to the width of a `size_t`.
#[cfg(target_os = "linux")]
const WORD: usize = std::mem::size_of::<usize>();

#[cfg(target_os = "linux")]
const fn aligned(length: usize) -> usize {
    length.div_ceil(WORD) * WORD
}

/// `struct cmsghdr`: `cmsg_len` (a `size_t`), `cmsg_level` and `cmsg_type` (`int`s).
#[cfg(target_os = "linux")]
const HEADER: usize = aligned(WORD + 2 * 4);

/// `CMSG_SPACE(2)`: the header, then the segment size as a `u16`, padded.
#[cfg(target_os = "linux")]
const CONTROL: usize = HEADER + aligned(2);

/// The control message that asks the kernel to cut a datagram into ones of `segment`
/// bytes.
#[cfg(target_os = "linux")]
fn segment_control(segment: u16) -> [u8; CONTROL] {
    let mut control = [0; CONTROL];
    // `CMSG_LEN(2)`: the header and the data, without the padding after.
    control[..WORD].copy_from_slice(&(HEADER + 2).to_ne_bytes());
    control[WORD..WORD + 4].copy_from_slice(&SOL_UDP.to_ne_bytes());
    control[WORD + 4..WORD + 8].copy_from_slice(&UDP_SEGMENT.to_ne_bytes());
    control[HEADER..HEADER + 2].copy_from_slice(&segment.to_ne_bytes());
    control
}

/// Sends `bytes` to `to` in one call, for the kernel to cut into datagrams of `segment`
/// bytes.
#[cfg(target_os = "linux")]
fn send_segmented(
    socket: &tokio::net::UdpSocket,
    bytes: &[u8],
    segment: usize,
    to: SocketAddr,
) -> io::Result<()> {
    let segment = u16::try_from(segment).map_err(|_| io::ErrorKind::InvalidInput)?;
    let control = segment_control(segment);
    let address = socket2::SockAddr::from(to);
    let buffers = [io::IoSlice::new(bytes)];
    let message = socket2::MsgHdr::new()
        .with_addr(&address)
        .with_buffers(&buffers)
        .with_control(&control);
    socket
        .try_io(tokio::io::Interest::WRITABLE, || {
            socket2::SockRef::from(socket).sendmsg(&message, 0)
        })
        .map(drop)
}

/// Nowhere else are datagrams cut: a run there is always a single datagram.
#[cfg(not(target_os = "linux"))]
fn send_segmented(
    _socket: &tokio::net::UdpSocket,
    _bytes: &[u8],
    _segment: usize,
    _to: SocketAddr,
) -> io::Result<()> {
    Err(io::ErrorKind::Unsupported.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    /// Where each of `datagrams` (length, port) goes, a run holding at most `most`.
    fn runs(datagrams: &[(usize, u16)], most: usize) -> Vec<Joins> {
        let mut gathering = Gathering::default();
        datagrams
            .iter()
            .map(|&(len, port)| gathering.admit(len, at(port), most, 1_350))
            .collect()
    }

    /// Full datagrams to one address make one run, which a shorter one ends.
    #[test]
    fn a_run_is_datagrams_of_one_size_to_one_address() {
        // Cut at the size of the full ones, the shorter last included.
        assert_eq!(
            runs(&[(1_350, 1), (1_350, 1), (1_350, 1), (400, 1)], 64),
            [
                Joins::Run,
                Joins::Run,
                Joins::Run,
                Joins::RunAndEnds { segment: 1_350 }
            ]
        );
        // A run is the size of its first: a small one first goes alone, since the full
        // ones after it cannot join its run, and they make the next.
        assert_eq!(
            runs(&[(200, 1), (1_350, 1), (1_350, 1)], 64),
            [
                Joins::Run,
                Joins::Next {
                    segment: 200,
                    to: at(1)
                },
                Joins::Run
            ]
        );
        // A datagram for another address sends the run before it as it was.
        assert_eq!(
            runs(&[(1_350, 1), (1_350, 1), (700, 2)], 64),
            [
                Joins::Run,
                Joins::Run,
                Joins::Next {
                    segment: 1_350,
                    to: at(1)
                }
            ]
        );
    }

    /// A datagram to another address, or larger than the run's, does not join it.
    #[test]
    fn a_datagram_that_cannot_join_begins_the_next_run() {
        assert_eq!(
            runs(&[(1_350, 1), (1_350, 2), (1_350, 2)], 64),
            [
                Joins::Run,
                Joins::Next {
                    segment: 1_350,
                    to: at(1)
                },
                Joins::Run
            ]
        );
        assert_eq!(
            runs(&[(1_000, 1), (1_000, 1), (1_350, 1), (1_350, 1)], 64),
            [
                Joins::Run,
                Joins::Run,
                Joins::Next {
                    segment: 1_000,
                    to: at(1)
                },
                Joins::Run
            ]
        );
    }

    /// A run ends at as many datagrams as one call may carry, and before its bytes would
    /// pass what one UDP datagram holds; where nothing is cut, every datagram is a run.
    #[test]
    fn a_run_ends_where_one_call_can_carry_no_more() {
        let full = vec![(1_350, 1); 50];
        let ends: Vec<usize> = runs(&full, 64)
            .iter()
            .enumerate()
            .filter(|(_, joins)| **joins == Joins::RunAndEnds { segment: 1_350 })
            .map(|(at, _)| at)
            .collect();
        // 48 of 1,350 bytes are 64,800; a 49th would pass 65,000.
        assert_eq!(ends, [47]);
        let ends = Joins::RunAndEnds { segment: 1_350 };
        assert_eq!(runs(&[(1_350, 1); 3], 2), [Joins::Run, ends, Joins::Run]);
        assert_eq!(runs(&[(1_350, 1); 3], 1), [ends; 3]);
        // A shorter one alone, where nothing is cut, is a run of its own size.
        assert_eq!(runs(&[(400, 1)], 1), [Joins::RunAndEnds { segment: 400 }]);
    }

    /// One call with the segmenting control message arrives as separate datagrams of the
    /// size asked for, the last shorter, each with its own bytes.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_segmented_send_arrives_as_datagrams() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut bytes = Vec::new();
            for (at, len) in [1_000, 1_000, 1_000, 300].into_iter().enumerate() {
                bytes.extend(std::iter::repeat_n(u8::try_from(at).unwrap(), len));
            }
            // tokio says a socket it has not yet seen writable would block.
            sender.writable().await.unwrap();
            send_segmented(&sender, &bytes, 1_000, receiver.local_addr().unwrap()).unwrap();
            let mut buf = [0; 2_000];
            for (at, len) in [1_000, 1_000, 1_000, 300].into_iter().enumerate() {
                let (read, _) = receiver.recv_from(&mut buf).await.unwrap();
                assert_eq!(read, len, "datagram {at}");
                assert!(buf[..read].iter().all(|&b| usize::from(b) == at));
            }
        });
    }
}
