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

use crate::downstream::h1::connection::Answered;
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h3::Settings;
use crate::downstream::h3::conn::Conn;
use crate::downstream::h3::connection::drive;
use crate::drain::Drain;
use crate::interim::Interim;
use crate::quic::header::{self, Header, VERSION_1};
use crate::quic::id::{self, Codec, IdError, Keys, Nonces};
use crate::quic::token;
use crate::request_body::RequestBody;
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
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;

/// The smallest datagram a client's first Initial may come in (RFC 9000 §14.1).
const INITIAL_DATAGRAM: usize = 1_200;

/// The most a datagram can be.
const DATAGRAM: usize = 65_535;

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
    /// Connections not yet through their handshake.
    pub(crate) handshakes: Cell<usize>,
    /// Connections held.
    pub(crate) connections: Cell<usize>,
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
    /// A listener on `socket`, as the `worker`th worker of those `secrets` are shared by.
    pub(crate) fn new(
        socket: UdpSocket,
        settings: Settings,
        secrets: &Secrets,
        worker: u16,
        timers: Rc<Timers>,
        drain: Rc<Drain>,
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
            handshakes: Cell::new(0),
            connections: Cell::new(0),
        })
    }
}

/// The quiche configuration a listener accepts with, for the TLS it was made from.
struct Accepting {
    tls: Arc<Tls>,
    config: quiche::Config,
}

/// Serves the listener until the worker drains and its last connection is gone. `tls`
/// says the listener's TLS of the config in force; `respond` answers each request; `date`
/// dates an answer; `opened` is held by each connection for as long as it lives.
pub(crate) async fn serve<T, R, F, B, D, O, G>(
    shared: Rc<Shared>,
    tls: T,
    respond: Rc<R>,
    date: Rc<D>,
    opened: O,
) where
    T: Fn() -> Option<Arc<Tls>>,
    R: Fn(Request<RequestBody>, Interim) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
    O: Fn() -> G,
    G: 'static,
{
    let mut datagram = vec![0; DATAGRAM];
    let mut accepting: Option<Accepting> = None;
    loop {
        if shared.drain.is_on() && shared.table.borrow().is_empty() {
            return;
        }
        let mut admitted = 0;
        let mut read = 0;
        while read < shared.settings.batch {
            let (len, from) = match shared.socket.try_recv_from(&mut datagram) {
                Ok(received) => received,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                // An ICMP error for something sent earlier, which Windows reports on the next
                // read: about one datagram, and nothing to stop for.
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => continue,
                Err(_) => break,
            };
            read += 1;
            let datagram = &mut datagram[..len];
            let Some(dcid) = destination(datagram) else {
                continue;
            };
            let found = dcid
                .as_ref()
                .and_then(|dcid| shared.table.borrow().get(dcid.as_slice()).cloned());
            if let Some(conn) = found {
                deliver(&conn, datagram, from, shared.local);
                continue;
            }
            if admitted < shared.settings.admit_per_batch
                && let Some(conn) = admit(&shared, &tls, &mut accepting, datagram, from)
            {
                admitted += 1;
                deliver(&conn, datagram, from, shared.local);
                let guard = opened();
                let chosen = dcid
                    .as_ref()
                    .map_or_else(Vec::new, |dcid| dcid.as_slice().to_vec());
                let _detached = tokio::task::spawn_local(drive(
                    conn,
                    Rc::clone(&shared),
                    chosen,
                    Rc::clone(&respond),
                    Rc::clone(&date),
                    guard,
                ));
            }
        }
        if read == shared.settings.batch {
            // A full batch: everything else the worker has goes first.
            tokio::task::yield_now().await;
        } else {
            let mut draining = std::pin::pin!(shared.drain.notified());
            std::future::poll_fn(|cx| {
                // Draining, the loop comes round to see whether the last connection went.
                if shared.drain.poll_on(draining.as_mut(), cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
                shared.socket.poll_recv_ready(cx).map(|_| ())
            })
            .await;
            if shared.drain.is_on() {
                // Woken now and then until the table empties: each connection's end is
                // a turn of the worker's, and one more read costs nothing.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
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
fn admit<T: Fn() -> Option<Arc<Tls>>>(
    shared: &Shared,
    tls: &T,
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
    if !long.is_initial()
        || datagram.len() < INITIAL_DATAGRAM
        || shared.drain.is_on()
        || shared.connections.get() >= shared.settings.connections
    {
        return None;
    }
    // quiche's parser reads the token, which only an Initial carries.
    let header = quiche::Header::from_slice(datagram, id::LEN).ok()?;
    // The client's choice, or after a Retry the ID of this worker's it was sent to.
    let dcid = header.dcid.to_vec();
    let scid_of_client = header.scid.to_vec();
    let token = header.token.unwrap_or_default();
    let now = unix_now();
    let mut retried = None;
    if !token.is_empty() {
        match token::validate(&shared.retry_key, now, from, &token) {
            Some(original) => retried = Some(original.to_vec()),
            // Not ours, or stale: as if there were none, unless a token is required.
            None if shared.handshakes.get() >= shared.settings.retry_above => return None,
            None => {}
        }
    }
    if retried.is_none() && shared.handshakes.get() >= shared.settings.retry_above {
        retry(shared, &scid_of_client, &dcid, now, from);
        return None;
    }

    let (scid, reset) = shared.issuer.borrow_mut().issue().ok()?;
    let accepting = accepting_for(accepting, tls, &shared.settings)?;
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
    let conn = Conn::new(quic);
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

/// The quiche configuration for the listener's TLS in force, made again if the TLS changed.
fn accepting_for<'a, T: Fn() -> Option<Arc<Tls>>>(
    accepting: &'a mut Option<Accepting>,
    tls: &T,
    settings: &Settings,
) -> Option<&'a mut Accepting> {
    let current = tls()?;
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
