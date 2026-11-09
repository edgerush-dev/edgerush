//! A `udp` listener's datagrams, carried through sessions to its route's backends and
//! their answers carried back ([24](../../docs/24-udp-proxying.md)).
//!
//! A worker serves a `udp` listener with one task on its socket, which reads datagrams in
//! bounded batches, finds or makes each one's session ([`Sessions`]) and sends the
//! datagram on the session's socket, `connect()`ed to its endpoint; and one task for each
//! session, which reads its endpoint's answers and sends each out of the listener's
//! socket, from the address the client's datagram arrived on. Nothing is held between
//! the two: a datagram either way that its socket has no room for is dropped (24 §4).

use super::{Proxy, Worker, balance_of};
use crate::balance::Tried;
use crate::connections::{Loads, QUIC_MOST};
use crate::descriptors::Descriptor;
use crate::drain::Drain;
use crate::metrics::{Dropped, SessionEnd};
use crate::random::random;
use crate::udp::sessions::{Client, Dropped as Unmade, Sessions};
use crate::upstream::balancing::InFlight;
use crate::upstream::dial::{self, Unconnected};
use edgerush_config::Destination;
use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

/// What a datagram is read into: every UDP datagram fits whole, but an IPv6 jumbogram, so
/// one that fills it was cut (24 §4).
pub(crate) const BUFFER: usize = 64 << 10;

/// Datagrams read off one socket before everything else the worker has gets a turn:
/// HTTP/3's batch (16 §6).
const BATCH: usize = 64;

impl Worker {
    /// Serves the `udp` listener whose slot is `listener` on `socket`, until the worker
    /// drains or the listener is taken away, which ends its sessions.
    ///
    /// # Errors
    ///
    /// A socket with no address of its own, or a wildcard one that cannot report each
    /// datagram's destination (Linux).
    pub fn serve_udp(
        self: Rc<Self>,
        listener: usize,
        socket: UdpSocket,
    ) -> io::Result<impl Future<Output = ()> + use<>> {
        self.serve_udp_reading(listener, socket, BUFFER)
    }

    /// The same, reading into a buffer of `buffer` bytes: the worker's, made by the first
    /// `udp` listener it serves, at the size that one asks.
    pub(super) fn serve_udp_reading(
        self: Rc<Self>,
        listener: usize,
        socket: UdpSocket,
        buffer: usize,
    ) -> io::Result<impl Future<Output = ()> + use<>> {
        let local = socket.local_addr()?;
        let wildcard = local.ip().is_unspecified();
        // As HTTP/3's (16 §3): on a wildcard bind, each datagram's destination is asked for,
        // so that its answers go from it. Linux only, as HTTP/3 has it.
        #[cfg(target_os = "linux")]
        if wildcard {
            crate::downstream::h3::pktinfo::enable(&socket, local.is_ipv6())?;
        }
        let idle = self
            .proxy
            .current
            .load()
            .listener(listener)
            // A slot no listener has is never served: what the config says unless told.
            .map_or(Duration::from_secs(60), |compiled| compiled.session_idle);
        // A worker made apart from the others counts its sessions in loads of its own.
        let (loads, worker) = match &self.connections {
            Some(loads) => (Arc::clone(loads), usize::from(self.position)),
            None => (Loads::new(1, usize::MAX, QUIC_MOST, listener + 1), 0),
        };
        let buffer = Rc::clone(
            self.udp_buffer
                .get_or_init(|| Rc::new(RefCell::new(vec![0; buffer].into_boxed_slice()))),
        );
        #[cfg(target_os = "linux")]
        let control = RefCell::new(if wildcard {
            crate::downstream::h3::pktinfo::control_space()
        } else {
            Vec::new()
        });
        let listening = Rc::new(Listening {
            slot: listener,
            socket,
            local,
            wildcard,
            sessions: RefCell::new(Sessions::new(idle, loads, worker, listener)),
            buffer,
            #[cfg(target_os = "linux")]
            control,
        });
        let drain = self.listening(listener);
        Ok(listening.serve(self, drain))
    }
}

/// A `udp` listener on one worker.
struct Listening {
    slot: usize,
    socket: UdpSocket,
    /// The address it is bound to.
    local: SocketAddr,
    /// Bound to every address of the host: each datagram says which it arrived on (Linux).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    wildcard: bool,
    sessions: RefCell<Sessions<Held>>,
    /// The worker's, lent to whichever of its tasks reads, never across an await.
    buffer: Rc<RefCell<Box<[u8]>>>,
    /// Room for the control message a wildcard bind's datagram's destination comes in.
    #[cfg(target_os = "linux")]
    control: RefCell<Vec<u8>>,
}

/// What a session holds of its own: a socket to its endpoint, but for one picked for an
/// invalid backend (24 §4).
struct Held {
    /// Counted as a connection accepted and open until the session ends.
    _open: Open,
    connected: Option<Connected>,
}

/// A session's socket to its endpoint, and what reads it.
struct Connected {
    socket: Rc<UdpSocket>,
    /// Load on its endpoint for as long as it is open, as a tunnel is (03 §6).
    _load: InFlight,
    /// One of the worker's open files (03 §9).
    _file: Descriptor,
    /// Its answers' task, which ends with it.
    _answers: Aborted,
}

impl Connected {
    /// Sends `datagram` to the endpoint, if the socket has room for it now. Asked of the
    /// kernel itself: the runtime knows a new socket writable only once its next turn has
    /// heard so, and a session's first datagram goes out before then.
    fn send(&self, datagram: &[u8]) -> io::Result<usize> {
        socket2::SockRef::from(&*self.socket).send(datagram)
    }
}

/// A task ended when this is dropped.
struct Aborted(JoinHandle<()>);

impl Drop for Aborted {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A session counted as a connection of its listener's, accepted and open, until dropped
/// (24 §6).
struct Open {
    proxy: Arc<Proxy>,
    listener: usize,
}

impl Open {
    fn new(proxy: &Arc<Proxy>, listener: usize) -> Self {
        if let Some(counters) = proxy.metrics.listener(listener) {
            counters.accepted.inc();
            counters.active.inc();
        }
        Self {
            proxy: Arc::clone(proxy),
            listener,
        }
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if let Some(counters) = self.proxy.metrics.listener(self.listener) {
            counters.active.dec();
        }
    }
}

/// What a read off a socket gave.
enum Read {
    /// A datagram of this many bytes, from `peer`, which arrived on `local`.
    Datagram(usize, Client),
    /// One longer than the buffer, which was cut.
    Truncated,
}

impl Listening {
    /// Serves until `drain` starts, then ends every session.
    async fn serve(self: Rc<Self>, worker: Rc<Worker>, drain: Rc<Drain>) {
        let mut idle = pin!(tokio::time::sleep_until(tokio::time::Instant::now()));
        loop {
            if drain.is_on() {
                self.end_every(&worker);
                return;
            }
            // One clock read a batch: what a datagram's session is kept from its bound by.
            let now = Instant::now();
            self.expire(&worker, now);
            let read = self.read_batch(&worker, now);
            if read == BATCH {
                // A full batch: everything else the worker has goes first.
                tokio::task::yield_now().await;
                continue;
            }
            let soonest = self.sessions.borrow_mut().soonest();
            if let Some(soonest) = soonest {
                idle.as_mut().reset(soonest.into());
            }
            let mut draining = pin!(drain.notified());
            poll_fn(|cx| {
                if drain.poll_on(draining.as_mut(), cx).is_ready() {
                    return Poll::Ready(());
                }
                if soonest.is_some() && idle.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
                self.socket.poll_recv_ready(cx).map(|_| ())
            })
            .await;
        }
    }

    /// Reads up to a batch of datagrams and carries each: how many it read.
    fn read_batch(self: &Rc<Self>, worker: &Rc<Worker>, now: Instant) -> usize {
        // The config in force once a batch: a reload applies from the next.
        let snapshot = worker.proxy.current.load();
        let route = snapshot
            .listener(self.slot)
            .and_then(|listener| listener.udp.as_ref());
        let mut read = 0;
        while read < BATCH {
            let mut buffer = self.buffer.borrow_mut();
            let Some(got) = self.recv(&mut buffer) else {
                break;
            };
            read += 1;
            let (len, client) = match got {
                Read::Datagram(len, client) => (len, client),
                Read::Truncated => {
                    self.dropped(worker, Dropped::Truncated);
                    continue;
                }
            };
            let Some(route) = route else {
                self.dropped(worker, Dropped::NoRoute);
                continue;
            };
            let datagram = buffer.get(..len).unwrap_or_default();
            let mut sessions = self.sessions.borrow_mut();
            let opened = sessions.arrived(client, now, &route.backends, random, |backend| {
                self.open(worker, &snapshot, backend, client)
            });
            let why = match opened {
                Ok(session) => match &session.value_mut().connected {
                    Some(connected) => match sent(|| connected.send(datagram)) {
                        Ok(_) => continue,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            Dropped::SendFull
                        }
                        // The endpoint's refusal, which a later step ends the session for
                        // (24 §8, step 3b); the datagram is lost to it, as UDP loses one.
                        Err(_) => continue,
                    },
                    None => Dropped::InvalidBackend,
                },
                Err(Unmade::NoBackend) => Dropped::InvalidBackend,
                Err(Unmade::NoRoom) => Dropped::NoRoom,
                Err(Unmade::Unopened(why)) => why,
            };
            drop(sessions);
            self.dropped(worker, why);
        }
        read
    }

    /// What a new session to `backend` holds: a socket connected to an endpoint picked as a
    /// tunnel's is, and the task that reads its answers; nothing for an invalid backend.
    /// Without an endpoint, a socket or a connect, why the datagram is dropped.
    fn open(
        self: &Rc<Self>,
        worker: &Rc<Worker>,
        snapshot: &super::Snapshot,
        backend: Destination,
        client: Client,
    ) -> Result<Held, Dropped> {
        let upstream = match backend {
            Destination::Upstream(upstream) => upstream,
            Destination::Invalid(_) => {
                return Ok(Held {
                    _open: Open::new(&worker.proxy, self.slot),
                    connected: None,
                });
            }
        };
        // An upstream with no endpoint now makes no session: the client's next datagram
        // picks again, and is served once one comes (24 §4).
        let destinations = snapshot.destinations.of(upstream.0);
        let picked = balance_of(&worker.balancing, snapshot, upstream.0).and_then(|balance| {
            balance.pick(destinations, snapshot.zoned(upstream.0), &Tried::default())
        });
        let Some((endpoint, load)) =
            picked.and_then(|(at, load)| Some((snapshot.destinations.at(upstream.0, at)?, load)))
        else {
            return Err(Dropped::InvalidBackend);
        };
        let Some(file) = worker.files.take() else {
            return Err(Dropped::Exhausted);
        };
        let socket = match connected(endpoint.address()) {
            Ok(socket) => Rc::new(socket),
            Err(unconnected) => {
                // The worker's own shortage, or the endpoint's, which sets it aside as a
                // tunnel's failed connect does (17 §1).
                let Some(why) = unconnected.aside() else {
                    return Err(Dropped::Exhausted);
                };
                endpoint.set_aside(why);
                return Err(Dropped::Unreachable);
            }
        };
        let answers = tokio::task::spawn_local(Rc::clone(self).answers(
            Rc::clone(worker),
            client,
            Rc::clone(&socket),
        ));
        Ok(Held {
            _open: Open::new(&worker.proxy, self.slot),
            connected: Some(Connected {
                socket,
                _load: load,
                _file: file,
                _answers: Aborted(answers),
            }),
        })
    }

    /// Carries `client`'s session's answers, read off `socket`, back to the client, until
    /// the session ends, which aborts it.
    async fn answers(self: Rc<Self>, worker: Rc<Worker>, client: Client, socket: Rc<UdpSocket>) {
        loop {
            if socket.readable().await.is_err() {
                return;
            }
            let now = Instant::now();
            let mut read = 0;
            while read < BATCH {
                let mut buffer = self.buffer.borrow_mut();
                let len = match socket.try_recv(&mut buffer) {
                    Ok(len) if len < buffer.len() => len,
                    Ok(_) => {
                        read += 1;
                        self.dropped(&worker, Dropped::Truncated);
                        continue;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if truncated(&error) => {
                        read += 1;
                        self.dropped(&worker, Dropped::Truncated);
                        continue;
                    }
                    // The endpoint's refusal, which a later step ends the session for
                    // (24 §8, step 3b): nothing came to carry.
                    Err(_) => {
                        read += 1;
                        continue;
                    }
                };
                read += 1;
                let answer = buffer.get(..len).unwrap_or_default();
                match self.send_to_client(answer, &client) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        self.dropped(&worker, Dropped::SendFull);
                    }
                    // Nothing to be done for one the client's side would not take.
                    Err(_) => {}
                }
                drop(buffer);
                // An answer keeps its session from its idle bound as a datagram does.
                let _heard = self.sessions.borrow_mut().answered(&client, now);
            }
            if read == BATCH {
                tokio::task::yield_now().await;
            }
        }
    }

    /// One datagram off the listener's socket, with whose it is and where it arrived;
    /// `None` once the socket has nothing more ready.
    fn recv(&self, buffer: &mut [u8]) -> Option<Read> {
        let full = buffer.len();
        #[cfg(target_os = "linux")]
        if self.wildcard {
            let mut control = self.control.borrow_mut();
            return match crate::downstream::h3::pktinfo::recv(&self.socket, buffer, &mut control) {
                Ok(Some((len, _, _))) if len >= full => Some(Read::Truncated),
                Ok(Some((len, peer, dst))) => {
                    // As HTTP/3's (16 §3): an IPv4 client of a dual-stack `[::]` bind, and a
                    // datagram whose destination went unread, are answered from the bound
                    // address, the source left to the kernel's routing.
                    let mapped = self.local.is_ipv6() && peer.ip().to_canonical().is_ipv4();
                    let local = match dst {
                        Some(dst) if !mapped => SocketAddr::new(dst, self.local.port()),
                        _ => self.local,
                    };
                    Some(Read::Datagram(len, Client { peer, local }))
                }
                Ok(None) | Err(_) => None,
            };
        }
        loop {
            return match self.socket.try_recv_from(buffer) {
                Ok((len, _)) if len >= full => Some(Read::Truncated),
                Ok((len, peer)) => Some(Read::Datagram(
                    len,
                    Client {
                        peer,
                        local: self.local,
                    },
                )),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => None,
                Err(error) if truncated(&error) => Some(Read::Truncated),
                // An ICMP error for something sent earlier, which Windows reports on the
                // next read: about one datagram, and nothing to stop for.
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => continue,
                Err(_) => None,
            };
        }
    }

    /// Sends `answer` to `client` from the address its datagrams arrive on.
    fn send_to_client(&self, answer: &[u8], client: &Client) -> io::Result<()> {
        sent(|| {
            #[cfg(target_os = "linux")]
            if client.local != self.local {
                return crate::downstream::h3::pktinfo::send(
                    &self.socket,
                    answer,
                    client.peer,
                    client.local.ip(),
                    None,
                );
            }
            self.socket.try_send_to(answer, client.peer).map(drop)
        })
    }

    /// Ends the sessions past their idle bound at `now`.
    fn expire(&self, worker: &Worker, now: Instant) {
        let mut ended = 0_u64;
        self.sessions
            .borrow_mut()
            .expire(now, |_, _session| ended += 1);
        self.ended(worker, SessionEnd::Idle, ended);
    }

    /// Ends every session: the listener was taken away, or the worker drains.
    fn end_every(&self, worker: &Worker) {
        let mut ended = 0_u64;
        self.sessions
            .borrow_mut()
            .end_every(|_, _session| ended += 1);
        self.ended(worker, SessionEnd::Drained, ended);
    }

    fn ended(&self, worker: &Worker, how: SessionEnd, count: u64) {
        if let Some(counters) = worker.proxy.metrics.listener(self.slot) {
            for _ in 0..count {
                counters.session(how);
            }
        }
    }

    fn dropped(&self, worker: &Worker, why: Dropped) {
        if let Some(counters) = worker.proxy.metrics.listener(self.slot) {
            counters.dropped(why);
        }
    }
}

/// A socket of its own for a session, `connect()`ed to `endpoint`, so that the kernel
/// hands it that endpoint's datagrams alone (24 §3). Whose a failure is, as a TCP
/// connect's is told ([`dial`]).
fn connected(endpoint: SocketAddr) -> Result<UdpSocket, Unconnected> {
    let any: SocketAddr = if endpoint.is_ipv4() {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    } else {
        (Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = dial::udp_socket(any)?;
    dial::udp_connect(&socket, endpoint)?;
    socket.set_nonblocking(true).map_err(Unconnected::Short)?;
    UdpSocket::from_std(socket).map_err(Unconnected::Short)
}

/// What `sending` gave: a full socket's would-block, where a test has this thread's
/// sockets full.
fn sent<T>(sending: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    #[cfg(test)]
    if FULL.get() {
        return Err(io::ErrorKind::WouldBlock.into());
    }
    sending()
}

#[cfg(test)]
thread_local! {
    /// Every socket a `udp` listener sends on from this thread has no room.
    static FULL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Has every socket a `udp` listener sends on from this thread have no room, or none from
/// now on: what a loopback, which takes a datagram at once, cannot be made into.
#[cfg(test)]
pub(super) fn sockets_full(full: bool) {
    FULL.set(full);
}

/// Whether a read failed for a datagram longer than its buffer: Windows says so, where
/// Unix cuts it and says nothing.
fn truncated(error: &io::Error) -> bool {
    // WSAEMSGSIZE.
    cfg!(windows) && error.raw_os_error() == Some(10040)
}
