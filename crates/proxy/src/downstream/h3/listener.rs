//! A worker's HTTP/3 listener: its UDP socket, the connections it holds and the IDs they
//! are known by ([16 §3, §4](../../../../../docs/16-http3.md)).
//!
//! Datagrams are read in bounded batches, and each goes straight to its connection's quiche,
//! found by the destination ID its first packet names. A version 1 Initial that names no
//! connection may start one: only in a datagram of the size RFC 9000 §14.1 requires, while
//! the worker is not draining and holds fewer connections than its bound, a few to a batch
//! as TCP's accept is paced, and — past a number of handshakes under way — only from a
//! client that has proved its address with a Retry token. A version the server does not
//! speak is answered with a version negotiation.
//!
//! A datagram whose ID names another worker's connection — one whose client's address
//! changed, so that the kernel's hash picked another socket — is handed to that worker's
//! inbox, once (16 §3). An inbox is bounded by count and by bytes; a datagram that finds it
//! full is dropped and counted, and QUIC sends it again.

use crate::downstream::h1::connection::Answered;
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h3::Settings;
use crate::downstream::h3::conn::Conn;
use crate::downstream::h3::connection::drive;
use crate::downstream::h3::send::Sending;
use crate::drain::Drain;
use crate::forwarding::Client;
use crate::interim::Interim;
use crate::metrics::Quic;
use crate::quic::header::{self, Header, VERSION_1};
use crate::quic::id::{self, Codec, IdError, Keys, Nonces};
use crate::quic::token;
use crate::request_body::RequestBody;
use crate::storage::Storage;
use crate::timers::Timers;
use crate::tls::Tls;
use bytes::Bytes;
use http::Request;
use http_body::Body;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// The smallest datagram a client's first Initial may come in (RFC 9000 §14.1).
const INITIAL_DATAGRAM: usize = 1_200;

/// The most a datagram can be.
const DATAGRAM: usize = 65_535;

/// The most datagrams waiting in one worker's inbox on one listener.
pub(crate) const INBOX_DATAGRAMS: usize = 1_024;

/// The most bytes waiting there.
pub(crate) const INBOX_BYTES: usize = 2 << 20;

/// A datagram received by one worker for another's connection.
pub(crate) struct Forwarded {
    datagram: Box<[u8]>,
    from: SocketAddr,
}

/// One worker's inbox on a listener, as the other workers see it.
struct Inbox {
    sender: mpsc::Sender<Forwarded>,
    /// What waits in it, in bytes.
    bytes: AtomicUsize,
}

/// A worker's share of a listener's forwarding: its own inbox, and every worker's to hand
/// datagrams to.
pub struct Forwarding {
    worker: usize,
    inbox: mpsc::Receiver<Forwarded>,
    inboxes: Arc<[Inbox]>,
}

impl std::fmt::Debug for Forwarding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forwarding")
            .field("worker", &self.worker)
            .field("workers", &self.inboxes.len())
            .finish_non_exhaustive()
    }
}

impl Forwarding {
    /// The shares of `workers` workers serving one listener, the `n`th worker's `n`th.
    #[must_use]
    pub fn group(workers: usize) -> Vec<Self> {
        let (inboxes, receivers): (Vec<Inbox>, Vec<_>) = (0..workers)
            .map(|_| {
                let (sender, receiver) = mpsc::channel(INBOX_DATAGRAMS);
                (
                    Inbox {
                        sender,
                        bytes: AtomicUsize::new(0),
                    },
                    receiver,
                )
            })
            .unzip();
        let inboxes: Arc<[Inbox]> = inboxes.into();
        receivers
            .into_iter()
            .enumerate()
            .map(|(worker, inbox)| Self {
                worker,
                inbox,
                inboxes: Arc::clone(&inboxes),
            })
            .collect()
    }

    /// How many workers share the listener.
    fn workers(&self) -> usize {
        self.inboxes.len()
    }

    /// Which of them this share is. Only tests ask: a worker's share is handed to it.
    #[cfg(test)]
    pub(crate) fn worker(&self) -> usize {
        self.worker
    }

    /// Hands `datagram` to worker `to`'s inbox: false if it is full.
    fn forward(&self, to: usize, datagram: &[u8], from: SocketAddr) -> bool {
        let Some(inbox) = self.inboxes.get(to) else {
            return false;
        };
        let len = datagram.len();
        if inbox.bytes.fetch_add(len, Ordering::Relaxed) + len > INBOX_BYTES {
            inbox.bytes.fetch_sub(len, Ordering::Relaxed);
            return false;
        }
        let forwarded = Forwarded {
            datagram: datagram.into(),
            from,
        };
        if inbox.sender.try_send(forwarded).is_err() {
            inbox.bytes.fetch_sub(len, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Accounts for a datagram taken out of this worker's own inbox.
    fn taken(&self, forwarded: &Forwarded) {
        if let Some(inbox) = self.inboxes.get(self.worker) {
            inbox
                .bytes
                .fetch_sub(forwarded.datagram.len(), Ordering::Relaxed);
        }
    }
}

/// The keys a process's workers share for QUIC: made once at start and never shared with
/// another process (16 §3).
#[derive(Clone)]
pub(crate) struct Secrets {
    ids: Keys,
    retry: [u8; 32],
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secrets(..)")
    }
}

impl Secrets {
    /// Keys drawn from BoringSSL's generator.
    pub(crate) fn new() -> Result<Self, boring::error::ErrorStack> {
        let mut ids = [0; 16];
        let mut reset = [0; 32];
        let mut retry = [0; 32];
        boring::rand::rand_bytes(&mut ids)?;
        boring::rand::rand_bytes(&mut reset)?;
        boring::rand::rand_bytes(&mut retry)?;
        Ok(Self {
            ids: Keys::new(ids, reset),
            retry,
        })
    }
}

/// The IDs a worker issues, and their reset tokens.
pub(crate) struct Issuer {
    codec: Codec,
    nonces: Nonces,
    worker: u16,
}

impl Issuer {
    fn new(secrets: &Secrets, worker: u16) -> Result<Self, IdError> {
        let mut start = [0; 16];
        boring::rand::rand_bytes(&mut start).map_err(IdError::Crypto)?;
        Ok(Self {
            codec: Codec::new(&secrets.ids)?,
            nonces: Nonces::starting_at(u128::from_be_bytes(start)),
            worker,
        })
    }

    /// A new ID of this worker's, and its reset token.
    pub(crate) fn issue(&mut self) -> Result<([u8; id::LEN], u128), IdError> {
        let id = self.codec.encode(self.worker, &self.nonces.draw())?;
        let token = self.codec.reset_token(&id)?;
        Ok((id, token))
    }

    /// The worker `dcid` names, if it is an ID of this process's form.
    pub(crate) fn owner(&mut self, dcid: &[u8]) -> Option<usize> {
        self.codec.decode(dcid).ok().flatten().map(usize::from)
    }
}

/// What a worker's listener and its connections share.
pub(crate) struct Shared {
    pub(crate) socket: UdpSocket,
    local: SocketAddr,
    pub(crate) settings: Settings,
    pub(crate) h3: quiche::h3::Config,
    /// Every connection, by every ID it is known by.
    pub(crate) table: RefCell<HashMap<Vec<u8>, Rc<Conn>>>,
    pub(crate) issuer: RefCell<Issuer>,
    retry_key: [u8; 32],
    pub(crate) timers: Rc<Timers>,
    pub(crate) drain: Rc<Drain>,
    /// The worker's account of what it holds for requests, charged what quiche holds for
    /// the listener's connections (16 §6).
    pub(crate) storage: Rc<Storage>,
    /// Connections not yet through their handshake.
    pub(crate) handshakes: Cell<usize>,
    /// Connections held.
    pub(crate) connections: Cell<usize>,
    /// A connection ended while the worker drains: the listener looks whether it was the
    /// last.
    pub(crate) ended: tokio::sync::Notify,
    /// Datagrams handed to another worker's inbox.
    pub(crate) forwarded: Cell<u64>,
    /// Datagrams dropped, the inbox they were for being full.
    pub(crate) dropped: Cell<u64>,
    /// What its connections send through.
    pub(crate) sending: Sending,
    /// Where what the listener does is counted.
    count: Box<dyn Fn(Quic)>,
}

/// Why a listener cannot be served.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ListenerError {
    #[error("the socket has no address: {0}")]
    Address(#[from] io::Error),
    #[error(transparent)]
    Ids(#[from] IdError),
    #[error("HTTP/3 settings: {0}")]
    H3(#[from] quiche::h3::Error),
}

impl Shared {
    /// A listener on `socket`, as the `worker`th worker of those `secrets` are shared by,
    /// charging what its connections hold to `storage`.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is the worker's, handed in once when the listener is made"
    )]
    pub(crate) fn new(
        socket: UdpSocket,
        settings: Settings,
        secrets: &Secrets,
        worker: u16,
        timers: Rc<Timers>,
        drain: Rc<Drain>,
        storage: Rc<Storage>,
        count: Box<dyn Fn(Quic)>,
    ) -> Result<Self, ListenerError> {
        Ok(Self {
            local: socket.local_addr()?,
            socket,
            settings,
            h3: settings.h3()?,
            table: RefCell::new(HashMap::new()),
            issuer: RefCell::new(Issuer::new(secrets, worker)?),
            retry_key: secrets.retry,
            timers,
            drain,
            storage,
            handshakes: Cell::new(0),
            connections: Cell::new(0),
            ended: tokio::sync::Notify::new(),
            forwarded: Cell::new(0),
            dropped: Cell::new(0),
            sending: Sending::new(),
            count,
        })
    }
}

/// The quiche configuration a listener accepts with, for the TLS it was made from.
struct Accepting {
    tls: Arc<Tls>,
    config: quiche::Config,
}

/// What the config in force says a new connection to the listener is accepted with.
pub(crate) struct InForce {
    pub(crate) tls: Arc<Tls>,
    /// Every client proves its address with a Retry first, however few handshakes are under
    /// way.
    pub(crate) force_retry: bool,
    /// What a connection accepted with `tls` drains with: the worker's drain, and a reload
    /// that replaces the client validation it was accepted under (03 §3).
    pub(crate) drain: Rc<Drain>,
}

/// Serves the listener until the worker drains and its last connection is gone. `in_force`
/// says what the config in force accepts a connection with, read at each client's first
/// packet, so that a new config applies to the next; `respond` answers each request;
/// `date` dates an answer; what `opened` makes of a connection's drain is held by the
/// connection for as long as it lives; `forwarding` is this worker's share of the
/// listener's inboxes.
pub(crate) async fn serve<T, R, F, B, D, O, G>(
    shared: Rc<Shared>,
    in_force: T,
    respond: Rc<R>,
    date: Rc<D>,
    opened: O,
    mut forwarding: Forwarding,
) where
    T: Fn() -> Option<InForce>,
    R: Fn(Request<RequestBody>, Interim, Rc<Client>) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
    O: Fn(&Rc<Drain>) -> G,
    G: 'static,
{
    let mut buffer = vec![0; DATAGRAM];
    let mut serving = Serving {
        shared: &shared,
        in_force: &in_force,
        respond: &respond,
        date: &date,
        opened: &opened,
        accepting: None,
        admitted: 0,
    };
    // A forwarded datagram the wait below took out of the inbox.
    let mut waiting: Option<Forwarded> = None;
    loop {
        if shared.drain.is_on() && shared.table.borrow().is_empty() {
            return;
        }
        serving.admitted = 0;
        let mut read = 0;
        while read < shared.settings.batch {
            let (len, from) = match shared.socket.try_recv_from(&mut buffer) {
                Ok(received) => received,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                // An ICMP error for something sent earlier, which Windows reports on the next
                // read: about one datagram, and nothing to stop for.
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => continue,
                Err(_) => break,
            };
            read += 1;
            serving.datagram(&mut buffer[..len], from, Some(&forwarding));
        }
        let mut taken = 0;
        while taken < shared.settings.batch {
            let Some(mut forwarded) = waiting.take().or_else(|| forwarding.inbox.try_recv().ok())
            else {
                break;
            };
            taken += 1;
            forwarding.taken(&forwarded);
            // Forwarded once and no more: here it is found, admitted or dropped.
            serving.datagram(&mut forwarded.datagram, forwarded.from, None);
        }
        if read == shared.settings.batch || taken == shared.settings.batch {
            // A full batch: everything else the worker has goes first.
            tokio::task::yield_now().await;
            continue;
        }
        let was_draining = shared.drain.is_on();
        let mut draining = std::pin::pin!(shared.drain.notified());
        let mut ended = std::pin::pin!(shared.ended.notified());
        std::future::poll_fn(|cx| {
            // When draining starts, and each time a connection ends after, the loop comes
            // round to see whether the last connection went.
            if !was_draining && shared.drain.poll_on(draining.as_mut(), cx).is_ready() {
                return Poll::Ready(());
            }
            if ended.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
            if let Poll::Ready(Some(forwarded)) = forwarding.inbox.poll_recv(cx) {
                waiting = Some(forwarded);
                return Poll::Ready(());
            }
            shared.socket.poll_recv_ready(cx).map(|_| ())
        })
        .await;
    }
}

/// What routing a datagram needs, for as long as the listener serves.
struct Serving<'a, T, R, D, O> {
    shared: &'a Rc<Shared>,
    in_force: &'a T,
    respond: &'a Rc<R>,
    date: &'a Rc<D>,
    opened: &'a O,
    accepting: Option<Accepting>,
    /// Connections admitted in this batch.
    admitted: usize,
}

impl<T, R, F, B, D, O, G> Serving<'_, T, R, D, O>
where
    T: Fn() -> Option<InForce>,
    R: Fn(Request<RequestBody>, Interim, Rc<Client>) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
    O: Fn(&Rc<Drain>) -> G,
    G: 'static,
{
    /// Routes one datagram: to its connection here; to another worker's inbox, if its ID
    /// names another worker and `forwarding` allows (not for one already forwarded); or to
    /// admission.
    fn datagram(&mut self, datagram: &mut [u8], from: SocketAddr, forwarding: Option<&Forwarding>) {
        let shared = self.shared;
        let Some(dcid) = destination(datagram) else {
            return;
        };
        let found = dcid
            .as_ref()
            .and_then(|dcid| shared.table.borrow().get(dcid.as_slice()).cloned());
        if let Some(conn) = found {
            deliver(&conn, datagram, from, shared.local);
            return;
        }
        if let (Some(forwarding), Some(dcid)) = (forwarding, &dcid) {
            let owner = shared.issuer.borrow_mut().owner(dcid.as_slice());
            if let Some(owner) =
                owner.filter(|&owner| owner != forwarding.worker && owner < forwarding.workers())
            {
                if forwarding.forward(owner, datagram, from) {
                    shared.forwarded.set(shared.forwarded.get() + 1);
                    (shared.count)(Quic::Forwarded);
                } else {
                    shared.dropped.set(shared.dropped.get() + 1);
                    (shared.count)(Quic::InboxFull);
                }
                return;
            }
        }
        if self.admitted >= shared.settings.admit_per_batch {
            return;
        }
        let Some(conn) = admit(shared, self.in_force, &mut self.accepting, datagram, from) else {
            return;
        };
        self.admitted += 1;
        deliver(&conn, datagram, from, shared.local);
        let guard = (self.opened)(&conn.drain);
        let chosen = dcid
            .as_ref()
            .map_or_else(Vec::new, |dcid| dcid.as_slice().to_vec());
        let _detached = tokio::task::spawn_local(drive(
            conn,
            Rc::clone(shared),
            chosen,
            Rc::clone(self.respond),
            Rc::clone(self.date),
            guard,
        ));
    }
}

/// The destination ID of a datagram's first packet, copied out of it: `None` if there is no
/// header to read, `Some(None)` for an ID longer than any connection is known by.
fn destination(datagram: &[u8]) -> Option<Option<Id>> {
    let dcid = match header::read(datagram, id::LEN)? {
        Header::Short { dcid } => dcid,
        Header::Long(long) => long.dcid,
    };
    Some(Id::of(dcid))
}

/// Hands `datagram` to `conn`'s quiche and tells the connection's driver.
fn deliver(conn: &Rc<Conn>, datagram: &mut [u8], from: SocketAddr, to: SocketAddr) {
    conn.with(|state| {
        // What quiche cannot use it drops, and what ends the connection it acts on itself.
        let _received = state.quic.recv(datagram, quiche::RecvInfo { from, to });
    });
    conn.stir();
}

/// A connection for `datagram`, a client's first, if it may have one; a Retry or a version
/// negotiation sent back instead, where one is due.
fn admit<T: Fn() -> Option<InForce>>(
    shared: &Shared,
    in_force: &T,
    accepting: &mut Option<Accepting>,
    datagram: &mut [u8],
    from: SocketAddr,
) -> Option<Rc<Conn>> {
    let Header::Long(long) = header::read(datagram, id::LEN)? else {
        // A short header for no connection of ours: one that is gone, or never was.
        return None;
    };
    if long.version != VERSION_1 {
        // A version negotiation is never answered, and a small datagram not either, so
        // that nothing sent back is larger than what came (RFC 9000 §6.1).
        if long.version != 0 && datagram.len() >= INITIAL_DATAGRAM {
            negotiate(shared, long.scid, long.dcid, from);
        }
        return None;
    }
    // A worker whose storage is full takes no one, as at its bound (16 §6).
    if !long.is_initial()
        || datagram.len() < INITIAL_DATAGRAM
        || shared.drain.is_on()
        || shared.connections.get() >= shared.settings.connections
        || !shared.storage.has_room()
    {
        return None;
    }
    // A listener the config no longer gives TLS takes no one.
    let in_force = in_force()?;
    // quiche's parser reads the token, which only an Initial carries.
    let header = quiche::Header::from_slice(datagram, id::LEN).ok()?;
    // The client's choice, or after a Retry the ID of this worker's it was sent to.
    let dcid = header.dcid.to_vec();
    let scid_of_client = header.scid.to_vec();
    let token = header.token.unwrap_or_default();
    let now = unix_now();
    let must_prove = in_force.force_retry || shared.handshakes.get() >= shared.settings.retry_above;
    let mut retried = None;
    if !token.is_empty() {
        match token::validate(&shared.retry_key, now, from, &token) {
            Some(original) => retried = Some(original.to_vec()),
            // Not ours, or stale: as if there were none, unless a token is required.
            None if must_prove => return None,
            None => {}
        }
    }
    if retried.is_none() && must_prove {
        retry(shared, &scid_of_client, &dcid, now, from);
        return None;
    }

    let (scid, reset) = shared.issuer.borrow_mut().issue().ok()?;
    let accepting = accepting_for(accepting, in_force.tls, &shared.settings)?;
    accepting.config.set_stateless_reset_token(Some(reset));
    let scid = quiche::ConnectionId::from_ref(&scid);
    let quic = match &retried {
        Some(original) => quiche::accept_with_retry(
            &scid,
            quiche::RetryConnectionIds {
                original_destination_cid: &quiche::ConnectionId::from_ref(original),
                retry_source_cid: &quiche::ConnectionId::from_ref(&dcid),
            },
            shared.local,
            from,
            &mut accepting.config,
        ),
        None => quiche::accept(&scid, None, shared.local, from, &mut accepting.config),
    }
    .ok()?;
    let conn = Conn::new(quic, in_force.drain);
    {
        let mut table = shared.table.borrow_mut();
        table.insert(scid.to_vec(), Rc::clone(&conn));
        // Until the handshake is done, a client may send its Initial again to the ID it
        // chose, which reaches this worker by the same addresses.
        table.insert(dcid, Rc::clone(&conn));
    }
    shared.handshakes.set(shared.handshakes.get() + 1);
    shared.connections.set(shared.connections.get() + 1);
    Some(conn)
}

/// The quiche configuration for the listener's TLS in force, `current`, made again if the
/// TLS changed.
fn accepting_for<'a>(
    accepting: &'a mut Option<Accepting>,
    current: Arc<Tls>,
    settings: &Settings,
) -> Option<&'a mut Accepting> {
    let stale = accepting
        .as_ref()
        .is_none_or(|kept| !Arc::ptr_eq(&kept.tls, &current));
    if stale {
        let context = current.quic_context().ok()?;
        let config = settings
            .transport(context, current.quic_ticket_key())
            .ok()?;
        *accepting = Some(Accepting {
            tls: current,
            config,
        });
    }
    accepting.as_mut()
}

/// Sends a version negotiation to a client whose Initial spoke a version not ours.
fn negotiate(shared: &Shared, scid: &[u8], dcid: &[u8], to: SocketAddr) {
    let mut out = [0; INITIAL_DATAGRAM];
    let written = quiche::negotiate_version(
        &quiche::ConnectionId::from_ref(scid),
        &quiche::ConnectionId::from_ref(dcid),
        &mut out,
    );
    if let Ok(written) = written {
        // Lost, the client sends its Initial again.
        let _sent = shared.socket.try_send_to(&out[..written], to);
        (shared.count)(Quic::Negotiation);
    }
}

/// Sends a Retry: the client is to send its Initial again, to a new ID of this worker's,
/// with a token that proves it received this at its address.
fn retry(shared: &Shared, scid: &[u8], odcid: &[u8], now: u64, to: SocketAddr) {
    let Some(minted) = token::mint(&shared.retry_key, now, to, odcid) else {
        return;
    };
    let Ok((retry_id, _)) = shared.issuer.borrow_mut().issue() else {
        return;
    };
    let mut out = [0; INITIAL_DATAGRAM];
    let written = quiche::retry(
        &quiche::ConnectionId::from_ref(scid),
        &quiche::ConnectionId::from_ref(odcid),
        &quiche::ConnectionId::from_ref(&retry_id),
        &minted,
        VERSION_1,
        &mut out,
    );
    if let Ok(written) = written {
        let _sent = shared.socket.try_send_to(&out[..written], to);
        (shared.count)(Quic::Retry);
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// A destination ID copied off a datagram, so that the datagram can be handed on.
struct Id {
    bytes: [u8; MOST_ID],
    len: usize,
}

/// The longest ID a connection is known by (RFC 9000 §17.2).
const MOST_ID: usize = 20;

impl Id {
    fn of(id: &[u8]) -> Option<Self> {
        let mut bytes = [0; MOST_ID];
        bytes.get_mut(..id.len())?.copy_from_slice(id);
        Some(Self {
            bytes,
            len: id.len(),
        })
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from() -> SocketAddr {
        "192.0.2.1:4433".parse().unwrap()
    }

    /// An inbox takes datagrams up to its count and its bytes, and drops the rest; what is
    /// taken out of it makes room again.
    #[test]
    fn an_inbox_is_bounded_by_count_and_by_bytes() {
        let mut group = Forwarding::group(2);
        let mut owner = group.remove(1);
        let sender = group.remove(0);
        for _ in 0..INBOX_DATAGRAMS {
            assert!(sender.forward(1, &[0; 100], from()));
        }
        assert!(!sender.forward(1, &[0; 100], from()), "past the count");
        let taken = owner.inbox.try_recv().unwrap();
        owner.taken(&taken);
        assert!(sender.forward(1, &[0; 100], from()), "room made");

        let mut group = Forwarding::group(2);
        let mut owner = group.remove(1);
        let sender = group.remove(0);
        let big = vec![0; 60_000];
        let fits = INBOX_BYTES / big.len();
        for _ in 0..fits {
            assert!(sender.forward(1, &big, from()));
        }
        assert!(!sender.forward(1, &big, from()), "past the bytes");
        let taken = owner.inbox.try_recv().unwrap();
        owner.taken(&taken);
        assert!(sender.forward(1, &big, from()), "room made");
        // No worker past the group.
        assert!(!sender.forward(2, &[0; 1], from()));
    }
}
