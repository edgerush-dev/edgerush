//! One worker's HTTP/2 connections to its upstreams: opening them, driving them, and
//! handing out places on them as the pool decides ([15 §4, §5](../../../../docs/15-http2-and-grpc.md)).
//!
//! The pool ([`super::pool`]) decides; this carries out what it decides and tells it what
//! happened. Each connection has a task of its own on the worker, which drives h2's
//! connection for as long as it lives — idle ones included, or h2 would not answer the
//! peer's PINGs and SETTINGS — and is also where the pool learns of what only h2 sees: the
//! peer changing how many streams it allows, GOAWAY, the connection ending. Nothing here
//! crosses threads, and no `RefCell` borrow is held across an `.await` or a poll of h2.

use super::pool::{Action, ConnectionId, Failure, Limits, Pool, Taken, WaiterId};
use crate::downstream::h2::writer::Outgoing;
use crate::received::Received;
use crate::upstream::destination::{Aside, ReuseIdentity};
use crate::upstream::dial;
use ::h2::client::{Connection, SendRequest};
use edgerush_config::Keepalive;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::poll_fn;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

/// What HTTP/2 connections to upstreams are opened and kept with (15 §3, §4).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    /// What the pool will not go beyond.
    pub(crate) pool: Limits,
    /// How long opening a connection, handshake included, may take; and how long a
    /// request may wait for a place.
    pub(crate) connect: Duration,
    /// What an upstream may send on one stream before it is given more.
    pub(crate) stream_window: u32,
    /// The same for the whole connection.
    pub(crate) connection_window: u32,
    /// The largest header list accepted from an upstream.
    pub(crate) header_list: u32,
    /// What h2 may hold of one stream's request before it is written.
    pub(crate) send_buffer: usize,
    /// How long a connection the pool has let go of has to close before it is dropped.
    pub(crate) closing: Duration,
}

impl Settings {
    /// h2's client built with these, every bound set explicitly rather than left to h2's
    /// defaults, so that a new version of h2 cannot move them unseen (15 §3).
    fn builder(&self) -> ::h2::client::Builder {
        let mut builder = ::h2::client::Builder::new();
        builder
            // Nothing is pushed to a proxy, and an upstream opens no streams of its own.
            .enable_push(false)
            .max_concurrent_streams(0)
            // Until the peer's SETTINGS say how many it allows, one: RFC 9113's default is
            // no limit at all, so one is always safe to send at once, and more could find
            // themselves in h2's queue behind a limit that arrives after them, which the
            // pool never uses (15 §3). The SETTINGS replace it, with the peer's number or,
            // if it names none, with no limit.
            .initial_max_send_streams(1)
            .initial_window_size(self.stream_window)
            .initial_connection_window_size(self.connection_window)
            .max_header_list_size(self.header_list)
            .header_table_size(4096)
            .max_frame_size(16_384)
            .max_send_buffer_size(self.send_buffer)
            .max_concurrent_reset_streams(50)
            .reset_stream_duration(Duration::from_secs(1))
            .max_local_error_reset_streams(Some(1024));
        builder
    }
}

/// Why a request got no place on a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PlaceError {
    /// As many requests wait for this destination as may.
    #[error("too many requests are waiting for this upstream")]
    Full,
    /// It waited as long as it may.
    #[error("no connection to the upstream had room in time")]
    TimedOut,
    /// No connection to the destination could be opened, and none is left.
    #[error("the upstream could not be reached")]
    Unreachable,
    /// The same, for want of the worker's own: a socket to connect with.
    #[error("the worker had no socket to connect to the upstream with")]
    Exhausted,
}

/// The most a destination's keepalive interval is doubled: 64 times what was configured.
const MOST_DOUBLINGS: u32 = 6;

/// What a connection to an upstream is carried over, before HTTP/2 is spoken on it.
enum Transport {
    Plain(TcpStream),
    Secured(tokio_boring::SslStream<TcpStream>),
}

/// One connection, as this worker holds it.
struct Link {
    key: u64,
    /// What streams are opened with. Taken away when the pool lets the connection go, so
    /// that h2 closes it once its last stream has ended.
    send: RefCell<Option<SendRequest<Outgoing>>>,
    /// Wakes the connection's task when the pool lets it go.
    released: Notify,
    /// The peer's SETTINGS have been heard: what it announced can be read off the handle.
    settled: Cell<bool>,
    /// Wakes whoever waits for them.
    heard: Notify,
    /// Closed because the worker could not pay for what h2 held of its answers.
    shed: Cell<bool>,
}

impl Link {
    fn settle(&self) {
        self.settled.set(true);
        self.heard.notify_waiters();
    }
}

/// One worker's HTTP/2 client.
pub(crate) struct Client {
    settings: Settings,
    pool: RefCell<Pool>,
    links: RefCell<HashMap<ConnectionId, Rc<Link>>>,
    waiters: RefCell<HashMap<WaiterId, oneshot::Sender<Result<ConnectionId, Failure>>>>,
    /// Every destination asked for, by key: where to open the connections the pool asks
    /// for, and whether a reload has retired it. A retired one is kept for as long as the
    /// pool holds anything of it, so that whoever already waits for it is still served
    /// (15 §4).
    destinations: RefCell<HashMap<u64, Arc<ReuseIdentity>>>,
    /// How many times a destination's keepalive interval has been doubled, by key, for
    /// telling this client to calm down: gRPC's backoff for too many PINGs.
    calmer: RefCell<HashMap<u64, u32>>,
    /// The worker's HTTP/2 connections, where each of these is charged what h2 holds of
    /// what its upstream sent (15 §3).
    received: Rc<Received>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("connections", &self.pool.borrow().connections())
            .finish_non_exhaustive()
    }
}

/// A place on a connection: the right to open one stream on it, until this is dropped.
pub(crate) struct Place {
    client: Rc<Client>,
    key: u64,
    id: ConnectionId,
    send: SendRequest<Outgoing>,
    /// The connection's, read once it has gone.
    link: Rc<Link>,
}

/// Whether a connection was closed for the worker's storage: what h2 tells its streams then
/// is no fault of the upstream's.
pub(crate) struct Shed(Rc<Link>);

impl Shed {
    /// Whether it was.
    pub(crate) fn is_shed(&self) -> bool {
        self.0.shed.get()
    }
}

impl std::fmt::Debug for Place {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Place")
            .field("key", &self.key)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Place {
    /// What the stream is opened with.
    pub(crate) fn sender(&mut self) -> &mut SendRequest<Outgoing> {
        &mut self.send
    }

    /// Whether the place's connection was closed because the worker could not pay for
    /// what h2 held of its answers, asked once it has gone.
    pub(crate) fn shed(&self) -> Shed {
        Shed(Rc::clone(&self.link))
    }

    /// Once the peer's SETTINGS have been heard on the place's connection. The pool lets a
    /// connection's first stream go before then (15 §3); what the peer announced, such as
    /// extended CONNECT, is known only after ([19 §4](../../../../docs/19-websocket.md)).
    pub(crate) async fn settled(&self) {
        let Some(link) = self.client.links.borrow().get(&self.id).cloned() else {
            return;
        };
        loop {
            // Made before the look, so that a settling in between is not missed.
            let heard = link.heard.notified();
            if link.settled.get() {
                return;
            }
            heard.await;
        }
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        let (key, id) = (self.key, self.id);
        self.client
            .event(|pool, now, actions| pool.ended(key, id, now, actions));
    }
}

/// A request waiting for a place, which gives the place up if the request does.
struct Waiting<'a> {
    client: &'a Rc<Client>,
    key: u64,
    waiter: WaiterId,
    granted: oneshot::Receiver<Result<ConnectionId, Failure>>,
    settled: bool,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // Granted a moment ago, with nobody left to use it: the place goes back.
        if let Ok(Ok(id)) = self.granted.try_recv() {
            let key = self.key;
            self.client
                .event(|pool, now, actions| pool.ended(key, id, now, actions));
            return;
        }
        self.client.waiters.borrow_mut().remove(&self.waiter);
        let (key, waiter) = (self.key, self.waiter);
        self.client
            .event(|pool, now, actions| pool.cancel(key, waiter, now, actions));
    }
}

impl Client {
    /// A client with no connections yet, whose connections are charged in `received`.
    pub(crate) fn new(settings: Settings, received: Rc<Received>) -> Rc<Self> {
        Rc::new(Self {
            settings,
            pool: RefCell::new(Pool::new(settings.pool)),
            links: RefCell::new(HashMap::new()),
            waiters: RefCell::new(HashMap::new()),
            destinations: RefCell::new(HashMap::new()),
            calmer: RefCell::new(HashMap::new()),
            received,
        })
    }

    /// A place on a connection to `destination`: at once if one has room, or once one
    /// has, within [`Settings::connect`].
    ///
    /// # Errors
    ///
    /// A [`PlaceError`] for a request that finds no room to wait, waits too long, or
    /// whose destination cannot be reached.
    pub(crate) async fn place(
        self: &Rc<Self>,
        destination: &Arc<ReuseIdentity>,
    ) -> Result<Place, PlaceError> {
        let key = destination.key();
        let mut actions = Vec::new();
        let taken = self
            .pool
            .borrow_mut()
            .take(key, Instant::now(), &mut actions);
        let waiter = match taken {
            Taken::Place(id) => {
                self.act(actions);
                return self.place_on(key, id);
            }
            Taken::Refused => {
                self.act(actions);
                return Err(PlaceError::Full);
            }
            Taken::Waiting(waiter) => waiter,
        };
        // Known before anything the pool asked for is done: a connection it wants opened
        // is opened to it.
        self.destinations
            .borrow_mut()
            .entry(key)
            .or_insert_with(|| Arc::clone(destination));
        let (grant, granted) = oneshot::channel();
        self.waiters.borrow_mut().insert(waiter, grant);
        self.act(actions);

        let mut waiting = Waiting {
            client: self,
            key,
            waiter,
            granted,
            settled: false,
        };
        let outcome = tokio::time::timeout(self.settings.connect, &mut waiting.granted).await;
        match outcome {
            Ok(Ok(Ok(id))) => {
                waiting.settled = true;
                self.place_on(key, id)
            }
            Ok(Ok(Err(Failure::Unreachable)) | Err(_)) => {
                waiting.settled = true;
                Err(PlaceError::Unreachable)
            }
            Ok(Ok(Err(Failure::Exhausted))) => {
                waiting.settled = true;
                Err(PlaceError::Exhausted)
            }
            // Dropping `waiting` gives the place in the queue up, or a grant that came in
            // the same moment.
            Err(_) => Err(PlaceError::TimedOut),
        }
    }

    /// Connections open or being opened, to every destination.
    pub(crate) fn connections(&self) -> usize {
        self.pool.borrow().connections()
    }

    /// Lets go of what has been idle too long, retires what has had its time, and winds
    /// down destinations a reload has retired. For the worker's sweep.
    pub(crate) fn sweep(self: &Rc<Self>) {
        let retired: Vec<u64> = self
            .destinations
            .borrow()
            .iter()
            .filter(|(_, destination)| destination.is_retired())
            .map(|(key, _)| *key)
            .collect();
        for key in retired {
            self.event(|pool, now, actions| pool.retire(key, now, actions));
        }
        self.event(|pool, now, actions| pool.sweep(now, actions));
        let pool = self.pool.borrow();
        self.destinations
            .borrow_mut()
            .retain(|key, destination| !destination.is_retired() || pool.holds(*key));
    }

    /// The place already reserved on connection `id`.
    fn place_on(self: &Rc<Self>, key: u64, id: ConnectionId) -> Result<Place, PlaceError> {
        let link = self.links.borrow().get(&id).cloned();
        let send = link
            .as_ref()
            .and_then(|link| link.send.borrow().clone())
            .zip(link);
        match send {
            Some((send, link)) => Ok(Place {
                client: Rc::clone(self),
                key,
                id,
                send,
                link,
            }),
            // A place is reserved only on a connection that is up, and a connection with
            // places taken is not let go of; so this is not known to happen. The
            // reservation goes back.
            None => {
                self.event(|pool, now, actions| pool.ended(key, id, now, actions));
                Err(PlaceError::Unreachable)
            }
        }
    }

    /// Tells the pool something, and does what it answers.
    fn event(self: &Rc<Self>, tell: impl FnOnce(&mut Pool, Instant, &mut Vec<Action>)) {
        let mut actions = Vec::new();
        tell(&mut self.pool.borrow_mut(), Instant::now(), &mut actions);
        self.act(actions);
    }

    /// Does what the pool asked, in order.
    fn act(self: &Rc<Self>, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Dial(key, id) => {
                    let destination = self.destinations.borrow().get(&key).cloned();
                    match destination {
                        // Taken along, not looked up again when the task runs: a sweep
                        // between the two may have let go of it, and a connection is
                        // opened with what the pool asked for or not at all.
                        Some(destination) => {
                            let _dialling = tokio::task::spawn_local(Rc::clone(self).dial(
                                key,
                                id,
                                destination,
                            ));
                        }
                        // Every dial is for a request that said where to.
                        None => self.event(|pool, now, actions| {
                            pool.failed(key, id, Failure::Unreachable, now, actions);
                        }),
                    }
                }
                Action::Grant(waiter, id) => {
                    let grant = self.waiters.borrow_mut().remove(&waiter);
                    let delivered = grant.is_some_and(|grant| grant.send(Ok(id)).is_ok());
                    if !delivered {
                        // Its request is gone: the place goes back.
                        let key = self.links.borrow().get(&id).map(|link| link.key);
                        if let Some(key) = key {
                            self.event(|pool, now, actions| pool.ended(key, id, now, actions));
                        }
                    }
                }
                Action::Fail(waiter, failure) => {
                    if let Some(grant) = self.waiters.borrow_mut().remove(&waiter) {
                        let _gone = grant.send(Err(failure));
                    }
                }
                Action::Close(id) => {
                    if let Some(link) = self.links.borrow_mut().remove(&id) {
                        link.send.borrow_mut().take();
                        link.released.notify_one();
                    }
                }
            }
        }
    }

    /// Opens connection `id` to `destination`, under `key`, and drives it until it ends.
    async fn dial(self: Rc<Self>, key: u64, id: ConnectionId, destination: Arc<ReuseIdentity>) {
        let settings = self.settings;
        let address = destination.address();
        let secure = destination.secure().cloned();
        let keepalive = destination.keepalive();
        // Whether TCP got through: only a connect that did not is the endpoint set aside
        // for, not a handshake that failed after it, and not the worker's own shortage of
        // sockets (03 §6).
        let connected = Cell::new(false);
        // What it is set aside for if it did not: out of connect time unless told otherwise.
        let why = Cell::new(Some(Aside::Connect));
        let opening = async {
            let socket = match dial::connect(address).await {
                Ok(socket) => socket,
                Err(unconnected) => {
                    why.set(unconnected.aside());
                    return None;
                }
            };
            connected.set(true);
            // Worth having, not worth refusing an upstream over.
            let _unset = socket.set_nodelay(true);
            Some(match secure {
                None => Transport::Plain(socket),
                Some(secure) => Transport::Secured(secure.connect(socket).await.ok()?),
            })
        };
        let Ok(Some(transport)) = tokio::time::timeout(settings.connect, opening).await else {
            let why = match why.get() {
                None => Failure::Exhausted,
                Some(aside) => {
                    if !connected.get() {
                        destination.set_aside(aside);
                    }
                    Failure::Unreachable
                }
            };
            self.event(|pool, now, actions| pool.failed(key, id, why, now, actions));
            return;
        };
        match transport {
            Transport::Plain(socket) => self.handshaken(key, id, keepalive, socket).await,
            Transport::Secured(socket) => self.handshaken(key, id, keepalive, socket).await,
        }
    }

    /// Runs HTTP/2's own handshake on `socket` for connection `id` to `key`, and drives
    /// the connection, kept alive as `keepalive` says, until it ends.
    async fn handshaken<S>(
        self: Rc<Self>,
        key: u64,
        id: ConnectionId,
        keepalive: Option<Keepalive>,
        socket: S,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let settings = self.settings;
        let handshake = settings.builder().handshake(socket);
        let Ok(Ok((send, connection))) = tokio::time::timeout(settings.connect, handshake).await
        else {
            self.event(|pool, now, actions| {
                pool.failed(key, id, Failure::Unreachable, now, actions);
            });
            return;
        };
        let peer = u32::try_from(send.current_max_send_streams()).unwrap_or(u32::MAX);
        let link = Rc::new(Link {
            key,
            send: RefCell::new(Some(send.clone())),
            released: Notify::new(),
            settled: Cell::new(false),
            heard: Notify::new(),
            shed: Cell::new(false),
        });
        self.links.borrow_mut().insert(id, Rc::clone(&link));
        self.event(|pool, now, actions| pool.opened(key, id, peer, now, actions));
        self.drive(key, id, keepalive, &link, send, connection)
            .await;
        self.links.borrow_mut().remove(&id);
        self.event(|pool, now, actions| pool.closed(key, id, now, actions));
    }

    /// Drives the connection until it ends, telling the pool what only h2 sees on the way.
    async fn drive<S>(
        self: &Rc<Self>,
        key: u64,
        id: ConnectionId,
        keepalive: Option<Keepalive>,
        link: &Link,
        watch: SendRequest<Outgoing>,
        mut connection: Connection<S, Outgoing>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // A handle kept to see the peer's limit and GOAWAY through, which opens nothing.
        let mut watch = Some(watch);
        let mut limit = watch
            .as_ref()
            .map_or(0, SendRequest::current_max_send_streams);
        let mut away = false;
        // One PING, whose answer says the peer's SETTINGS have been heard: a peer sends
        // them before anything else, and frames arrive in order. Until then the pool
        // counts on the connection without dialling beside it. The same handle sends the
        // keepalive's PINGs after it, one at a time.
        let mut pings = connection.ping_pong();
        let mut settling = pings
            .as_mut()
            .is_some_and(|pinging| pinging.send_ping(::h2::Ping::opaque()).is_ok());
        if !settling {
            let peer = u32::try_from(limit).unwrap_or(u32::MAX);
            self.event(|pool, at, actions| pool.settled(key, id, peer, at, actions));
            link.settle();
        }
        let calmer = self.calmer.borrow().get(&key).copied().unwrap_or(0);
        let interval = keepalive.map(|keepalive| {
            Duration::from_secs(keepalive.interval_seconds).saturating_mul(1 << calmer)
        });
        let mut next_ping = interval.map(|interval| Box::pin(tokio::time::sleep(interval)));
        // The settling PING is held to the keepalive's timeout as its PINGs are: a
        // connection that never answers one is as dead as one that stops.
        let mut pong_due: Option<std::pin::Pin<Box<tokio::time::Sleep>>> =
            keepalive.filter(|_| settling).map(|keepalive| {
                Box::pin(tokio::time::sleep(Duration::from_secs(
                    keepalive.timeout_seconds,
                )))
            });
        let mut letting_go = pin!(link.released.notified());
        // Its place among the worker's HTTP/2 connections, charged what h2 holds for it.
        let account = self.received.open();
        // Made when the pool lets the connection go: with the last handle gone h2 closes
        // it once its streams are done, and it has so long to do so.
        let mut closing: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
        poll_fn(|cx| {
            if closing.is_none() && letting_go.as_mut().poll(cx).is_ready() {
                watch = None;
                closing = Some(Box::pin(tokio::time::sleep(self.settings.closing)));
            }
            if closing
                .as_mut()
                .is_some_and(|closing| closing.as_mut().poll(cx).is_ready())
            {
                return Poll::Ready(());
            }
            if let Poll::Ready(ended) = std::pin::Pin::new(&mut connection).poll(cx) {
                // Told to calm down: its PINGs, gRPC says, were too many. The next
                // connection to it waits twice as long between them.
                if let Err(error) = ended
                    && error.is_go_away()
                    && error.is_remote()
                    && error.reason() == Some(::h2::Reason::ENHANCE_YOUR_CALM)
                {
                    let mut calmer = self.calmer.borrow_mut();
                    let doubled = calmer.entry(key).or_insert(0);
                    *doubled = (*doubled + 1).min(MOST_DOUBLINGS);
                }
                return Poll::Ready(());
            }
            // What h2 holds of what the upstream sent — answers not yet read — is the
            // worker's storage as well (15 §3); a connection closed to make room for others
            // goes now, and whatever is on it with it.
            account.drive_with(cx.waker());
            account.settle(connection.received_unreleased());
            if account.is_shed() {
                link.shed.set(true);
                return Poll::Ready(());
            }
            if let Some(pinging) = pings.as_mut()
                && (settling || pong_due.is_some())
                && let Poll::Ready(answered) = pinging.poll_pong(cx)
            {
                pong_due = None;
                if settling {
                    settling = false;
                    // A connection that failed instead is about to end, and says so then.
                    if answered.is_ok()
                        && let Some(watching) = watch.as_ref()
                    {
                        limit = watching.current_max_send_streams();
                        let peer = u32::try_from(limit).unwrap_or(u32::MAX);
                        self.event(|pool, at, actions| pool.settled(key, id, peer, at, actions));
                        link.settle();
                    }
                }
            }
            if let Some(due) = pong_due.as_mut()
                && due.as_mut().poll(cx).is_ready()
            {
                // No answer in time: taken for dead, and whatever is on it with it.
                return Poll::Ready(());
            }
            if let (Some(keepalive), Some(interval), Some(next)) =
                (keepalive, interval, next_ping.as_mut())
                && next.as_mut().poll(cx).is_ready()
            {
                next.as_mut().reset(Instant::now() + interval);
                let calls = self.pool.borrow().streams(key, id) > 0;
                if !settling
                    && pong_due.is_none()
                    && (calls || keepalive.without_calls)
                    && let Some(pinging) = pings.as_mut()
                    && pinging.send_ping(::h2::Ping::opaque()).is_ok()
                {
                    let timeout = Duration::from_secs(keepalive.timeout_seconds);
                    pong_due = Some(Box::pin(tokio::time::sleep(timeout)));
                }
                // Polled again, so that the timer just set is watched.
                let _armed = next.as_mut().poll(cx);
                if let Some(due) = pong_due.as_mut() {
                    let _armed = due.as_mut().poll(cx);
                }
            }
            if let Some(watching) = watch.as_mut() {
                let now = watching.current_max_send_streams();
                if now != limit {
                    limit = now;
                    let peer = u32::try_from(now).unwrap_or(u32::MAX);
                    self.event(|pool, at, actions| pool.peer_limit(key, id, peer, at, actions));
                }
                // A handle that has opened nothing is ready unless the connection can take
                // no new streams at all: GOAWAY, or an error.
                if !away && matches!(watching.poll_ready(cx), Poll::Ready(Err(_))) {
                    away = true;
                    self.event(|pool, at, actions| pool.going_away(key, id, at, actions));
                }
            }
            Poll::Pending
        })
        .await;
    }
}
