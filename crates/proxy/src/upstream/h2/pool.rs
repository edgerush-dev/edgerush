//! Which HTTP/2 connection a request's stream goes on, and when another is opened
//! ([15 §4](../../../../docs/15-http2-and-grpc.md)).
//!
//! One pool per worker, as for HTTP/1, and it never leaves that worker. Unlike HTTP/1's,
//! a connection is not taken out: many streams share it, so the pool keeps every
//! connection and hands out *places* on them. It does no I/O and reads no clock — it is
//! told what happened (a connection opened, a stream ended, the peer changed its limit,
//! GOAWAY) and at what time, and answers with what to do ([`Action`]). The driver does it.
//!
//! **Fill-first.** A request takes a place on the first connection, in the order they were
//! opened, that has one; another connection is opened only when none has. The configured
//! stream cap per connection is what spreads load over several connections. There is no
//! scoring: age and request count retire a connection, they do not rank it.
//!
//! **First come, first served.** A request that finds no place waits, in order, and a
//! request that comes later never overtakes one that is waiting, even when a place opens
//! up at the moment it arrives. The wait is bounded; a request past the bound is refused
//! at once, and one whose own deadline passes cancels its place in the queue.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

/// What a pool will not go beyond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limits {
    /// Streams at once on one connection, however many the peer would allow.
    pub(crate) streams: u32,
    /// Connections to one destination, opening and closing ones included.
    pub(crate) connections: usize,
    /// Connections to all destinations together, the same way.
    pub(crate) connections_total: usize,
    /// Requests waiting for a place, for one destination.
    pub(crate) waiting: usize,
    /// Connections being opened at once to one destination.
    pub(crate) dialing: usize,
    /// Streams a connection carries in its life before it is retired.
    pub(crate) requests: u64,
    /// How long a connection is used for new streams before it is retired.
    pub(crate) age: Duration,
    /// How long a connection with no streams is kept before it is closed.
    pub(crate) idle: Duration,
}

/// A connection, as the pool names it. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ConnectionId(u64);

/// A request waiting for a place, as the pool names it. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct WaiterId(u64);

/// What became of a request that asked for a place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Taken {
    /// A place on this connection, reserved: open the stream on it now.
    Place(ConnectionId),
    /// No place yet. It is granted, or refused, by an [`Action`] later; or the request
    /// gives up and cancels it.
    Waiting(WaiterId),
    /// No place and no room to wait.
    Refused,
}

/// Why a waiting request was refused after all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// Every connection that could have served it failed or went, and there is no other
    /// being opened.
    Unreachable,
    /// The same, the last to fail having failed for want of the worker's own: a socket to
    /// connect with ([03 §6](../../../../docs/03-data-plane.md)).
    Exhausted,
}

/// What the driver is to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Open a connection to the destination under this key; say how it went with
    /// [`Pool::opened`] or [`Pool::failed`].
    Dial(u64, ConnectionId),
    /// The waiting request has a place on this connection, reserved.
    Grant(WaiterId, ConnectionId),
    /// The waiting request is refused.
    Fail(WaiterId, Failure),
    /// Close this connection: nothing is on it, and nothing will be.
    Close(ConnectionId),
}

/// Where a connection is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Being opened; its streams will be the waiting requests'.
    Connecting,
    /// Takes new streams while it has room.
    Usable,
    /// Takes no new streams; closed when the last one ends.
    Draining,
}

#[derive(Debug)]
struct Connection {
    id: ConnectionId,
    state: State,
    /// Streams reserved on it and not yet ended.
    active: u32,
    /// What the peer allows at once, as its latest SETTINGS said.
    peer: u32,
    /// Whether the peer's first SETTINGS have been heard. Until then `peer` is what may
    /// be sent before them, and the room it will have is counted as on its way.
    settled: bool,
    /// Streams it has carried, those under way included.
    requests: u64,
    opened: Instant,
    /// When its last stream ended, or it opened: the start of its idleness.
    quiet_since: Instant,
}

impl Connection {
    /// Whether it can take one more stream now.
    fn has_room(&self, limits: &Limits) -> bool {
        self.state == State::Usable && self.active < limits.streams.min(self.peer)
    }

    /// Whether it has had its time or its share and must take no more.
    fn is_spent(&self, now: Instant, limits: &Limits) -> bool {
        self.requests >= limits.requests || now.saturating_duration_since(self.opened) >= limits.age
    }
}

/// One destination's connections and waiting requests.
#[derive(Debug, Default)]
struct Destination {
    /// In the order they were opened: the order they are filled in.
    connections: Vec<Connection>,
    waiting: VecDeque<WaiterId>,
    /// Gone from the running config: once nobody waits, its connections take no more.
    retired: bool,
}

impl Destination {
    fn dialing(&self) -> usize {
        self.connections
            .iter()
            .filter(|connection| connection.state == State::Connecting)
            .count()
    }

    fn position(&self, id: ConnectionId) -> Option<usize> {
        self.connections
            .iter()
            .position(|connection| connection.id == id)
    }
}

/// One worker's HTTP/2 connections, by destination key
/// ([`crate::upstream::destination::ReuseIdentity::key`]).
#[derive(Debug)]
pub(crate) struct Pool {
    limits: Limits,
    destinations: HashMap<u64, Destination>,
    /// Connections to every destination, kept as a number.
    total: usize,
    next: u64,
}

impl Pool {
    /// An empty pool held to `limits`.
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            limits,
            destinations: HashMap::new(),
            total: 0,
            next: 0,
        }
    }

    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// A place for a request to the destination under `key`, at `now`: on a connection
    /// that has one, or in the queue, or refused. What else is to be done as a result —
    /// open a connection, close a spent one — is added to `actions`.
    pub(crate) fn take(&mut self, key: u64, now: Instant, actions: &mut Vec<Action>) -> Taken {
        let limits = self.limits;
        let before = self.total;
        let destination = self.destinations.entry(key).or_default();
        // Nobody overtakes a request that is already waiting. Places are granted the moment
        // they free up, so while anyone waits there is none to take; this keeps the order
        // from resting on that alone.
        if destination.waiting.is_empty() {
            let (placed, retired) = place(destination, now, &limits);
            if retired {
                self.reap(key, actions);
                self.wake_starved(before, now, None, actions);
            }
            if let Some(connection) = placed {
                return Taken::Place(connection);
            }
        }
        let destination = self.destinations.entry(key).or_default();
        if destination.waiting.len() >= limits.waiting {
            self.forget_if_empty(key);
            return Taken::Refused;
        }
        let waiter = WaiterId(self.id());
        let destination = self.destinations.entry(key).or_default();
        destination.waiting.push_back(waiter);
        self.dial_if_needed(key, now, actions);
        Taken::Waiting(waiter)
    }

    /// The connection `id` to `key` is open, and `peer` streams may be sent on it before
    /// the peer's SETTINGS are heard ([`Pool::settled`]).
    pub(crate) fn opened(
        &mut self,
        key: u64,
        id: ConnectionId,
        peer: u32,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        if let Some(connection) = self.connection(key, id) {
            if connection.state == State::Connecting {
                connection.state = State::Usable;
                connection.opened = now;
                connection.quiet_since = now;
            }
            connection.peer = peer;
        }
        self.serve_waiting(key, now, actions);
        // Serving the last waiter of a retired destination closes what it no longer needs.
        self.wake_starved(before, now, None, actions);
    }

    /// The peer of connection `id` to `key` has been heard from: its SETTINGS allow `peer`
    /// streams at once.
    pub(crate) fn settled(
        &mut self,
        key: u64,
        id: ConnectionId,
        peer: u32,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        if let Some(connection) = self.connection(key, id) {
            connection.peer = peer;
            connection.settled = true;
        }
        self.serve_waiting(key, now, actions);
        self.wake_starved(before, now, None, actions);
    }

    /// The connection `id` to `key` could not be opened, `why` being what those it leaves
    /// with nothing are refused with.
    ///
    /// Nothing is dialled again in its place: whoever waited is served by another
    /// connection that is up or being opened, or, when there is none, refused. Trying again
    /// at once would turn an upstream that refuses connections into a storm of attempts.
    pub(crate) fn failed(
        &mut self,
        key: u64,
        id: ConnectionId,
        why: Failure,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        self.remove(key, id);
        if !self.refuse_if_hopeless(key, why, actions) {
            self.grant_waiting(key, now, actions);
            self.wind_down(key, actions);
            self.forget_if_empty(key);
        }
        // Others kept waiting by the worker's bound may open one now; not this one.
        self.wake_starved(before, now, Some(key), actions);
    }

    /// A stream on connection `id` to `key` has ended, whichever way.
    pub(crate) fn ended(
        &mut self,
        key: u64,
        id: ConnectionId,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        if let Some(connection) = self.connection(key, id) {
            connection.active = connection.active.saturating_sub(1);
            if connection.active == 0 {
                connection.quiet_since = now;
                if connection.state == State::Draining {
                    actions.push(Action::Close(id));
                    self.remove(key, id);
                }
            }
        }
        self.serve_waiting(key, now, actions);
        self.wake_starved(before, now, None, actions);
    }

    /// The peer of connection `id` to `key` now allows `peer` streams at once. Streams
    /// already over a lowered limit finish; no more are opened until there is room.
    pub(crate) fn peer_limit(
        &mut self,
        key: u64,
        id: ConnectionId,
        peer: u32,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        if let Some(connection) = self.connection(key, id) {
            connection.peer = peer;
            // A change is the peer's SETTINGS heard.
            connection.settled = true;
        }
        self.serve_waiting(key, now, actions);
        self.wake_starved(before, now, None, actions);
    }

    /// Connection `id` to `key` takes no new streams: its peer said GOAWAY, or it is to be
    /// retired. It is closed once its streams end.
    pub(crate) fn going_away(
        &mut self,
        key: u64,
        id: ConnectionId,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        if let Some(connection) = self.connection(key, id) {
            connection.state = State::Draining;
            if connection.active == 0 {
                actions.push(Action::Close(id));
                self.remove(key, id);
            }
        }
        self.after_loss(key, now, actions);
        self.wake_starved(before, now, None, actions);
    }

    /// Connection `id` to `key` is gone: its driver has finished, and whatever streams it
    /// had have failed with it.
    pub(crate) fn closed(
        &mut self,
        key: u64,
        id: ConnectionId,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        self.remove(key, id);
        self.after_loss(key, now, actions);
        self.wake_starved(before, now, None, actions);
    }

    /// The waiting request `waiter` for `key` gives up its place in the queue. The last to
    /// wait for a retired destination winds it down, as the last served does.
    pub(crate) fn cancel(
        &mut self,
        key: u64,
        waiter: WaiterId,
        now: Instant,
        actions: &mut Vec<Action>,
    ) {
        let before = self.total;
        if let Some(destination) = self.destinations.get_mut(&key)
            && let Some(at) = destination.waiting.iter().position(|w| *w == waiter)
        {
            destination.waiting.remove(at);
        }
        self.wind_down(key, actions);
        self.forget_if_empty(key);
        self.wake_starved(before, now, None, actions);
    }

    /// The destination under `key` is gone from the running config: nothing more is
    /// opened to it once its waiting requests are served, and its connections close as
    /// their streams end.
    pub(crate) fn retire(&mut self, key: u64, now: Instant, actions: &mut Vec<Action>) {
        let before = self.total;
        let Some(destination) = self.destinations.get_mut(&key) else {
            return;
        };
        destination.retired = true;
        self.wind_down(key, actions);
        self.wake_starved(before, now, None, actions);
    }

    /// For a retired destination that nobody waits for: its connections take no more
    /// streams, and those with none left are closed.
    fn wind_down(&mut self, key: u64, actions: &mut Vec<Action>) {
        let Some(destination) = self.destinations.get_mut(&key) else {
            return;
        };
        if !destination.retired || !destination.waiting.is_empty() {
            return;
        }
        for connection in &mut destination.connections {
            if connection.state == State::Usable {
                connection.state = State::Draining;
            }
        }
        self.reap(key, actions);
        self.forget_if_empty(key);
    }

    /// Closes what has been idle too long, and retires what has had its time, at `now`.
    pub(crate) fn sweep(&mut self, now: Instant, actions: &mut Vec<Action>) {
        let limits = self.limits;
        let before = self.total;
        let mut closed = Vec::new();
        // Destinations that lost a connection, or a connection's use: whoever waits for
        // them may need another opened.
        let mut touched = Vec::new();
        for (key, destination) in &mut self.destinations {
            for connection in &mut destination.connections {
                let mut changed = false;
                if connection.state == State::Usable && connection.is_spent(now, &limits) {
                    connection.state = State::Draining;
                    changed = true;
                }
                let idle_too_long = connection.active == 0
                    && now.saturating_duration_since(connection.quiet_since) >= limits.idle;
                let finished = connection.state == State::Draining && connection.active == 0;
                if connection.state != State::Connecting && (idle_too_long || finished) {
                    closed.push((*key, connection.id));
                    changed = true;
                }
                if changed && !touched.contains(key) {
                    touched.push(*key);
                }
            }
        }
        for (key, id) in closed {
            actions.push(Action::Close(id));
            self.remove(key, id);
        }
        for key in touched {
            self.after_loss(key, now, actions);
        }
        self.wake_starved(before, now, None, actions);
    }

    /// After connections went, from `before` to fewer: whoever waits with nothing being
    /// opened for them — held back by a bound, or left with none — may have one opened
    /// now. Except for `except`, whose attempt has just failed.
    fn wake_starved(
        &mut self,
        before: usize,
        now: Instant,
        except: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        if self.total >= before {
            return;
        }
        // Only when a connection went, which is rare beside requests.
        let starved: Vec<u64> = self
            .destinations
            .iter()
            .filter(|(key, destination)| {
                Some(**key) != except
                    && !destination.waiting.is_empty()
                    && destination.dialing() == 0
            })
            .map(|(key, _)| *key)
            .collect();
        for key in starved {
            self.dial_if_needed(key, now, actions);
        }
    }

    /// Connections to every destination, opening and closing ones included.
    pub(crate) fn connections(&self) -> usize {
        self.total
    }

    /// Whether anything of `key` is still here: a request waiting for it, or a connection
    /// to it, opening and closing ones included. Nothing is ever asked of a destination
    /// the pool no longer holds.
    pub(crate) fn holds(&self, key: u64) -> bool {
        self.destinations.contains_key(&key)
    }

    /// The streams connection `id` to `key` has on it now.
    pub(crate) fn streams(&self, key: u64, id: ConnectionId) -> u32 {
        self.destinations
            .get(&key)
            .and_then(|destination| {
                destination
                    .connections
                    .iter()
                    .find(|connection| connection.id == id)
            })
            .map_or(0, |connection| connection.active)
    }

    fn connection(&mut self, key: u64, id: ConnectionId) -> Option<&mut Connection> {
        let destination = self.destinations.get_mut(&key)?;
        let at = destination.position(id)?;
        destination.connections.get_mut(at)
    }

    fn remove(&mut self, key: u64, id: ConnectionId) {
        if let Some(destination) = self.destinations.get_mut(&key)
            && let Some(at) = destination.position(id)
        {
            destination.connections.remove(at);
            self.total -= 1;
        }
        self.forget_if_empty(key);
    }

    /// Closes the connections to `key` that take no more streams and have none left.
    fn reap(&mut self, key: u64, actions: &mut Vec<Action>) {
        let Some(destination) = self.destinations.get_mut(&key) else {
            return;
        };
        let before = destination.connections.len();
        destination.connections.retain(|connection| {
            let finished = connection.state == State::Draining && connection.active == 0;
            if finished {
                actions.push(Action::Close(connection.id));
            }
            !finished
        });
        self.total -= before - destination.connections.len();
    }

    fn forget_if_empty(&mut self, key: u64) {
        if self.destinations.get(&key).is_some_and(|destination| {
            destination.connections.is_empty() && destination.waiting.is_empty()
        }) {
            self.destinations.remove(&key);
        }
    }

    /// Grants places to waiting requests in order while there are places, then opens
    /// another connection if some are still waiting and there is room for one.
    fn serve_waiting(&mut self, key: u64, now: Instant, actions: &mut Vec<Action>) {
        self.grant_waiting(key, now, actions);
        self.dial_if_needed(key, now, actions);
        self.wind_down(key, actions);
        self.forget_if_empty(key);
    }

    /// Grants places to waiting requests in order while there are places.
    fn grant_waiting(&mut self, key: u64, now: Instant, actions: &mut Vec<Action>) {
        let limits = self.limits;
        let mut retired = false;
        if let Some(destination) = self.destinations.get_mut(&key) {
            while let Some(&waiter) = destination.waiting.front() {
                let (placed, retiring) = place(destination, now, &limits);
                retired |= retiring;
                let Some(connection) = placed else {
                    break;
                };
                destination.waiting.pop_front();
                actions.push(Action::Grant(waiter, connection));
            }
        }
        if retired {
            self.reap(key, actions);
        }
    }

    /// After a connection is lost or stops taking streams: whoever is waiting is served by
    /// another, or by one opened for them, or — if none is left or being opened and none
    /// can be — refused.
    fn after_loss(&mut self, key: u64, now: Instant, actions: &mut Vec<Action>) {
        self.serve_waiting(key, now, actions);
        self.refuse_if_hopeless(key, Failure::Unreachable, actions);
    }

    /// Refuses whoever waits for `key`, with `why`, when no connection that could take them
    /// is up or being opened, and says whether it did.
    fn refuse_if_hopeless(&mut self, key: u64, why: Failure, actions: &mut Vec<Action>) -> bool {
        let Some(destination) = self.destinations.get_mut(&key) else {
            return false;
        };
        let hope = destination
            .connections
            .iter()
            .any(|connection| connection.state != State::Draining);
        let refused = !hope && !destination.waiting.is_empty();
        if refused {
            for waiter in destination.waiting.drain(..) {
                actions.push(Action::Fail(waiter, why));
            }
        }
        self.forget_if_empty(key);
        refused
    }

    /// Opens a connection for the requests waiting on `key` if none has a place for them,
    /// none being opened will be enough, and the bounds allow another.
    fn dial_if_needed(&mut self, key: u64, now: Instant, actions: &mut Vec<Action>) {
        let limits = self.limits;
        let total = self.total;
        let Some(destination) = self.destinations.get_mut(&key) else {
            return;
        };
        if destination.waiting.is_empty() {
            return;
        }
        // What the connections being opened will take between them, and those open but not
        // yet heard from will take once they are.
        let unsettled: usize = destination
            .connections
            .iter()
            .filter(|connection| connection.state == State::Usable && !connection.settled)
            .map(|connection| limits.streams.saturating_sub(connection.active) as usize)
            .sum();
        let coming = destination.dialing() * limits.streams as usize + unsettled;
        if destination.waiting.len() <= coming
            || destination.dialing() >= limits.dialing
            || destination.connections.len() >= limits.connections
            || total >= limits.connections_total
        {
            return;
        }
        let id = ConnectionId(self.id());
        let destination = self.destinations.entry(key).or_default();
        destination.connections.push(Connection {
            id,
            state: State::Connecting,
            active: 0,
            // What `opened` will say may be sent before the peer's SETTINGS.
            peer: 1,
            settled: false,
            requests: 0,
            opened: now,
            quiet_since: now,
        });
        self.total += 1;
        actions.push(Action::Dial(key, id));
    }
}

/// Reserves a place on the first connection that has one, retiring on the way any that
/// has had its time or its share. Says whether it retired any, so that the caller can
/// close those with nothing on them.
fn place(
    destination: &mut Destination,
    now: Instant,
    limits: &Limits,
) -> (Option<ConnectionId>, bool) {
    let mut retired = false;
    for connection in &mut destination.connections {
        if connection.state == State::Usable && connection.is_spent(now, limits) {
            connection.state = State::Draining;
            retired = true;
            continue;
        }
        if connection.has_room(limits) {
            connection.active += 1;
            connection.requests += 1;
            return (Some(connection.id), retired);
        }
    }
    (None, retired)
}

#[cfg(test)]
mod tests;
