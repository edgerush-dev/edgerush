//! A worker's sessions on one `udp` listener
//! ([24 §3](../../../../docs/24-udp-proxying.md)): which backend each client's datagrams go
//! to, until neither the client nor its backend has sent anything for the session's idle
//! bound, or until the bound of its drain (24 §5).
//!
//! A client is its address and port with the local address its datagram arrived on. Its
//! first datagram makes its session, and picks the session's backend by weight, once: every
//! later datagram of the client's goes where the first went. A session counts as one of its
//! worker's connections, on its listener, from when it is made until it ends
//! ([`Loads::take`]): a new client that finds no room has no session, and its datagram is
//! dropped. A session picked for an invalid backend counts as any other, though it holds
//! no socket, so that sessions nothing can serve are bounded as the others are (24 §4).
//!
//! The idle bound is a deadline in a [`Heap`] that moves later with every datagram either
//! way. Moving later touches the session's slot alone, so a datagram costs a lookup and a
//! time stored, and the heap is touched at most once an idle bound a session. A session
//! takes the listener's idle bound as it is when the session is made, and keeps it, as a
//! tunnel keeps its own. Draining gives it a second deadline, which a datagram never moves:
//! it ends at whichever comes first.
//!
//! Where its listener says how many answers a session expects for each datagram (24 §3), a
//! session counts the datagrams that went to its endpoint and the answers that came back,
//! and says when the answers are that many for each datagram, for the caller to end it; it
//! keeps the count it was made with, as it keeps its idle bound. The idle bound is held in
//! whole seconds, as a listener's config gives it, so that it and the counts take the room
//! a `Duration` alone would: a session of a listener that counts nothing is no larger for
//! the counting.
//!
//! Nothing here reads a clock, draws a random number or touches a socket: the time and the
//! randomness are handed in, and what a session holds of its own is the caller's.

use crate::connections::{Held, Loads};
use crate::timers::heap::{Heap, Key};
use edgerush_config::{Destination, WeightedBackends};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Whose datagrams a session carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Client {
    /// The client's address and port.
    pub peer: SocketAddr,
    /// The address its datagrams arrive on: one of a wildcard listener's.
    pub local: SocketAddr,
}

/// Hashed as three numbers, where the derived hash hands the hasher a dozen pieces, each
/// at a cost of its own, and was most of what a datagram's lookup took. Addresses are
/// hashed as IPv6 ones, an IPv4 one as it maps there; an IPv6 address's flow label and
/// scope are not hashed, and are still told apart by equality.
impl Hash for Client {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u128(bits(self.peer.ip()));
        state.write_u128(bits(self.local.ip()));
        state.write_u32(u32::from(self.peer.port()) << 16 | u32::from(self.local.port()));
    }
}

/// An address as the 128 bits of the IPv6 address it is or maps to.
fn bits(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => u128::from(v4.to_ipv6_mapped()),
        IpAddr::V6(v6) => u128::from(v6),
    }
}

/// One client's session.
#[derive(Debug)]
pub struct Session<T> {
    backend: Destination,
    deadline: Key,
    /// When it ends however busy it is, once it drains.
    drain_by: Option<Instant>,
    /// Its idle bound, in seconds: its listener's when it was made.
    idle: u32,
    /// What it counts toward ending on its answers.
    answers: Answers,
    /// The connection it counts as, given back when it ends: held for its drop alone.
    _held: Held,
    value: T,
}

/// A session's datagrams and answers, counted toward its end (24 §3).
#[derive(Debug, Clone, Copy)]
struct Answers {
    /// The answers it expects for each datagram; `None` where its listener expects no
    /// number, and nothing is counted.
    each: Option<NonZeroU32>,
    /// Its client's datagrams that went to its endpoint.
    sent: u32,
    /// Its endpoint's answers read.
    read: u32,
}

impl<T> Session<T> {
    /// Where its datagrams go: picked by its first.
    #[must_use]
    pub fn backend(&self) -> Destination {
        self.backend
    }

    /// What the caller opened for it.
    #[must_use]
    pub fn value(&self) -> &T {
        &self.value
    }

    /// What the caller opened for it.
    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// One of its client's datagrams went to its endpoint: an answer more to wait for.
    pub fn sent(&mut self) {
        let answers = &mut self.answers;
        if answers.each.is_some() {
            answers.sent = answers.sent.saturating_add(1);
        }
    }

    /// An answer was read from its endpoint: whether the answers are now as many as it
    /// expects for each datagram that went, for the caller to end it. Never for a session
    /// whose listener expects no number, nor before a datagram has gone: what comes before
    /// is nothing its client asked for.
    pub fn answered(&mut self) -> bool {
        let answers = &mut self.answers;
        let Some(each) = answers.each else {
            return false;
        };
        answers.read = answers.read.saturating_add(1);
        answers.sent > 0
            && u64::from(answers.read) >= u64::from(each.get()) * u64::from(answers.sent)
    }
}

/// How a session's time ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// It carried nothing either way for its idle bound.
    Idle,
    /// Its drain's bound came.
    Drained,
}

/// Why a datagram from a new client made no session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dropped<E> {
    /// No backend of the route has a share: every weight is 0.
    NoBackend,
    /// The worker has no room for another connection, or the listener holds its share.
    NoRoom,
    /// The listener drains: it makes no session (24 §5).
    Draining,
    /// What the session was to hold could not be opened, for the reason given.
    Unopened(E),
}

/// A worker's sessions on one `udp` listener.
#[derive(Debug)]
pub struct Sessions<T> {
    /// Hashed with std's keyed hash: the keys are what clients choose, and must not let
    /// them choose which ones collide.
    by_client: HashMap<Client, Session<T>>,
    deadlines: Heap<Client>,
    /// The idle bound of the sessions made from now on, in seconds.
    idle: u32,
    /// The answers they expect for each datagram, if their listener says.
    answers: Option<NonZeroU32>,
    /// Every session drains, and none is made.
    draining: bool,
    loads: Arc<Loads>,
    worker: usize,
    listener: usize,
    /// The slots of the sessions an expiry ends, given up once the heap has handed them
    /// all over: kept between expiries, so that ending sessions does not allocate.
    ended: Vec<Key>,
}

impl<T> Sessions<T> {
    /// No sessions yet, of a listener whose idle bound is `idle` seconds, counted as
    /// connections of `worker`'s on `listener` in `loads`.
    #[must_use]
    pub fn new(idle: u32, loads: Arc<Loads>, worker: usize, listener: usize) -> Self {
        Self {
            by_client: HashMap::new(),
            deadlines: Heap::default(),
            idle,
            answers: None,
            draining: false,
            loads,
            worker,
            listener,
            ended: Vec::new(),
        }
    }

    /// A datagram from `client` came at `now`: the client's session, made if it had none,
    /// with its backend picked from `backends` by `random`, room taken for it, and what it
    /// holds of its own made by `open`, which is told the backend. Each of the three is
    /// asked only for a new session, and in that order: what is not needed is not drawn,
    /// taken or opened. Without a session, the datagram is dropped, for the reason given.
    ///
    /// [`Sessions::expire`] is asked for `now` first: until then, a session past its bound
    /// is carried on. Once every session drains ([`Sessions::drain_every`]), a new client
    /// is given none.
    pub fn arrived<E>(
        &mut self,
        client: Client,
        now: Instant,
        backends: &WeightedBackends,
        random: impl FnOnce() -> u64,
        open: impl FnOnce(Destination) -> Result<T, E>,
    ) -> Result<&mut Session<T>, Dropped<E>> {
        match self.by_client.entry(client) {
            Entry::Occupied(found) => {
                let session = found.into_mut();
                touch(&mut self.deadlines, session, now);
                Ok(session)
            }
            Entry::Vacant(_) if self.draining => Err(Dropped::Draining),
            Entry::Vacant(vacant) => {
                let backend = backends.pick(random()).ok_or(Dropped::NoBackend)?;
                let held = self
                    .loads
                    .take(self.worker, self.listener)
                    .ok_or(Dropped::NoRoom)?;
                // Dropping `held` with the error gives the room back.
                let value = open(backend).map_err(Dropped::Unopened)?;
                let session = vacant.insert(Session {
                    backend,
                    deadline: self.deadlines.add(client),
                    drain_by: None,
                    idle: self.idle,
                    answers: Answers {
                        each: self.answers,
                        sent: 0,
                        read: 0,
                    },
                    _held: held,
                    value,
                });
                touch(&mut self.deadlines, session, now);
                Ok(session)
            }
        }
    }

    /// Something passed on `client`'s session at `now`: its backend's answer, or a datagram
    /// from the client where no session is to be made for it: the session, if it has one,
    /// which this keeps from its idle bound.
    pub fn heard(&mut self, client: &Client, now: Instant) -> Option<&mut Session<T>> {
        let session = self.by_client.get_mut(client)?;
        touch(&mut self.deadlines, session, now);
        Some(session)
    }

    /// The idle bound of the sessions made from now on, in seconds; those there are keep
    /// theirs.
    pub fn set_idle(&mut self, idle: u32) {
        self.idle = idle;
    }

    /// The answers the sessions made from now on expect for each datagram, `None` for none;
    /// those there are keep theirs.
    pub fn set_answers(&mut self, answers: Option<NonZeroU32>) {
        self.answers = answers;
    }

    /// Every session `which` picks that does not drain already drains: it ends at `by`, or
    /// at its idle bound if that comes first.
    pub fn drain(&mut self, by: Instant, mut which: impl FnMut(&Session<T>) -> bool) {
        for session in self.by_client.values_mut() {
            if session.drain_by.is_some() || !which(session) {
                continue;
            }
            session.drain_by = Some(by);
            let due = self
                .deadlines
                .due(session.deadline)
                .map_or(by, |due| due.min(by));
            self.deadlines.wait(session.deadline, due, || due, |_| {});
        }
    }

    /// Every session drains, by `by` at the latest, and none is made from now on: the
    /// listener was taken away, or its worker drains.
    pub fn drain_every(&mut self, by: Instant) {
        self.draining = true;
        self.drain(by, |_| true);
    }

    /// Whether every session drains, and none is made.
    #[must_use]
    pub fn draining(&self) -> bool {
        self.draining
    }

    /// Ends every session `which` picks, now: each handed to `each`, with its client.
    pub fn end_where(
        &mut self,
        mut which: impl FnMut(&Session<T>) -> bool,
        mut each: impl FnMut(Client, Session<T>),
    ) {
        for (client, session) in self.by_client.extract_if(|_, session| which(session)) {
            self.deadlines.remove(session.deadline);
            each(client, session);
        }
    }

    /// Ends `client`'s session before its idle bound, as a refused one ends: handed back,
    /// for the caller to drop, which gives its room back. The client's next datagram makes
    /// a new one.
    pub fn end(&mut self, client: &Client) -> Option<Session<T>> {
        let session = self.by_client.remove(client)?;
        self.deadlines.remove(session.deadline);
        Some(session)
    }

    /// When the soonest idle bound may come, for the caller to ask [`Sessions::expire`]
    /// then: never later than it comes, sooner if the session has been heard from since.
    pub fn soonest(&mut self) -> Option<Instant> {
        self.deadlines.soonest()
    }

    /// The time is `now`: every session that has carried nothing either way for its idle
    /// bound up to it, or whose drain's bound has come, is ended and handed to `each`, with
    /// its client and which it was.
    pub fn expire(&mut self, now: Instant, mut each: impl FnMut(Client, Session<T>, Ended)) {
        let Self {
            by_client,
            deadlines,
            ended,
            ..
        } = self;
        deadlines.expire(now, |client| {
            if let Some(session) = by_client.remove(client) {
                ended.push(session.deadline);
                let how = if session.drain_by.is_some_and(|by| by <= now) {
                    Ended::Drained
                } else {
                    Ended::Idle
                };
                each(*client, session, how);
            }
        });
        for deadline in ended.drain(..) {
            deadlines.remove(deadline);
        }
    }

    /// How many sessions there are.
    #[must_use]
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn len(&self) -> usize {
        self.by_client.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_client.is_empty()
    }
}

/// `session` was heard from at `now`: its idle bound is reckoned from then, and it ends
/// then unless its drain's bound comes first.
fn touch<T>(deadlines: &mut Heap<Client>, session: &Session<T>, now: Instant) {
    // An idle bound past what the clock can count to is one never reached.
    let idle = Duration::from_secs(u64::from(session.idle));
    let due = match (now.checked_add(idle), session.drain_by) {
        (Some(idle), Some(by)) => idle.min(by),
        (Some(due), None) | (None, Some(due)) => due,
        (None, None) => return,
    };
    deadlines.wait(session.deadline, due, || due, |_| {});
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::QUIC_MOST;
    use edgerush_config::UpstreamId;
    use proptest::prelude::*;
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

    const IDLE: Duration = Duration::from_secs(60);

    /// `idle` in the whole seconds a listener's idle bound is given in.
    fn seconds(idle: Duration) -> u32 {
        assert_eq!(idle.subsec_nanos(), 0, "whole seconds");
        u32::try_from(idle.as_secs()).unwrap()
    }

    fn client(port: u16) -> Client {
        Client {
            peer: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port),
            local: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 53),
        }
    }

    fn backends(weights: &[u32]) -> WeightedBackends {
        WeightedBackends::new(
            weights
                .iter()
                .enumerate()
                .map(|(at, weight)| (UpstreamId(at), *weight)),
        )
    }

    /// Sessions of the one worker's one listener, with room for `cap`.
    fn sessions(cap: usize) -> (Sessions<u32>, Arc<Loads>) {
        let loads = Loads::new(1, cap, QUIC_MOST, 1);
        (
            Sessions::new(seconds(IDLE), Arc::clone(&loads), 0, 0),
            loads,
        )
    }

    fn opened(_: Destination) -> Result<u32, ()> {
        Ok(7)
    }

    /// A client's datagram at `now`, with `random` drawn if it makes a session; its backend.
    fn datagram(
        sessions: &mut Sessions<u32>,
        client: Client,
        now: Instant,
        backends: &WeightedBackends,
        random: u64,
    ) -> Result<Destination, Dropped<()>> {
        sessions
            .arrived(client, now, backends, || random, opened)
            .map(|session| session.backend())
    }

    fn ended(sessions: &mut Sessions<u32>, now: Instant) -> Vec<Client> {
        ended_how(sessions, now)
            .into_iter()
            .map(|(client, _)| client)
            .collect()
    }

    /// The sessions ended at `now`, and how, in the order of their clients' ports.
    fn ended_how(sessions: &mut Sessions<u32>, now: Instant) -> Vec<(Client, Ended)> {
        let mut ended = Vec::new();
        sessions.expire(now, |client, _, how| ended.push((client, how)));
        ended.sort_by_key(|(client, _)| client.peer.port());
        ended
    }

    #[test]
    fn every_datagram_of_a_client_goes_where_its_first_went() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[1, 1]);
        let start = Instant::now();
        let drawn = Cell::new(0);
        let draw = |random| {
            drawn.set(drawn.get() + 1);
            random
        };
        let first = sessions
            .arrived(client(1), start, &backends, || draw(1), opened)
            .map(|session| session.backend());
        assert_eq!(first, Ok(Destination::Upstream(UpstreamId(1))));
        let later = sessions
            .arrived(client(1), start, &backends, || draw(0), opened)
            .map(|session| session.backend());
        assert_eq!(later, first);
        assert_eq!(drawn.get(), 1, "only the first datagram picks");
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn what_a_session_holds_is_opened_once_with_its_backend() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[0, 1]);
        let start = Instant::now();
        let opens = Cell::new(Vec::new());
        let open = |backend| {
            let mut seen = opens.take();
            seen.push(backend);
            opens.set(seen);
            Ok::<_, ()>(9)
        };
        let session = sessions
            .arrived(client(1), start, &backends, || 0, open)
            .map(|session| *session.value_mut());
        assert_eq!(session, Ok(9));
        let again = sessions
            .arrived(client(1), start, &backends, || 0, open)
            .map(|session| *session.value_mut());
        assert_eq!(again, Ok(9));
        assert_eq!(opens.take(), [Destination::Upstream(UpstreamId(1))]);
    }

    #[test]
    fn clients_are_told_apart_by_the_address_they_sent_to() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[1, 1]);
        let start = Instant::now();
        let other = Client {
            local: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 5353),
            ..client(1)
        };
        assert_eq!(
            datagram(&mut sessions, client(1), start, &backends, 0),
            Ok(Destination::Upstream(UpstreamId(0)))
        );
        assert_eq!(
            datagram(&mut sessions, other, start, &backends, 1),
            Ok(Destination::Upstream(UpstreamId(1)))
        );
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn clients_that_hash_alike_are_still_told_apart() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[1]);
        let start = Instant::now();
        let v4 = Ipv4Addr::new(192, 0, 2, 1);
        let alike = [
            SocketAddr::new(IpAddr::V4(v4), 4000),
            SocketAddr::new(IpAddr::V6(v4.to_ipv6_mapped()), 4000),
            SocketAddr::V6(SocketAddrV6::new(v4.to_ipv6_mapped(), 4000, 1, 0)),
            SocketAddr::V6(SocketAddrV6::new(v4.to_ipv6_mapped(), 4000, 0, 2)),
        ];
        for peer in alike {
            let client = Client { peer, ..client(0) };
            assert!(datagram(&mut sessions, client, start, &backends, 0).is_ok());
        }
        assert_eq!(sessions.len(), alike.len());
        for peer in alike {
            assert!(sessions.end(&Client { peer, ..client(0) }).is_some());
        }
    }

    #[test]
    fn a_session_ends_at_its_idle_bound_from_the_last_it_carried_either_way() {
        let (mut sessions, loads) = sessions(10);
        let backends = backends(&[1]);
        let start = Instant::now();
        let heard = start + IDLE / 2;
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        assert!(datagram(&mut sessions, client(2), start, &backends, 0).is_ok());
        assert!(sessions.heard(&client(2), heard).is_some());
        assert_eq!(sessions.soonest(), Some(start + IDLE));
        let nanosecond = Duration::from_nanos(1);
        assert_eq!(ended(&mut sessions, start + IDLE - nanosecond), []);
        assert_eq!(ended(&mut sessions, start + IDLE), [client(1)]);
        assert_eq!(loads.now(), [1], "its room is given back");
        assert_eq!(sessions.soonest(), Some(heard + IDLE));
        assert_eq!(ended(&mut sessions, heard + IDLE - nanosecond), []);
        assert_eq!(ended(&mut sessions, heard + IDLE), [client(2)]);
        assert!(sessions.is_empty());
        assert_eq!(sessions.soonest(), None);
    }

    #[test]
    fn a_new_client_that_finds_no_room_has_no_session_until_one_ends() {
        let (mut sessions, loads) = sessions(1);
        let backends = backends(&[1]);
        let start = Instant::now();
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        assert_eq!(
            datagram(&mut sessions, client(2), start, &backends, 0),
            Err(Dropped::NoRoom)
        );
        assert!(
            datagram(&mut sessions, client(1), start, &backends, 0).is_ok(),
            "a session that has room keeps it"
        );
        assert_eq!(loads.now(), [1]);
        assert!(sessions.end(&client(1)).is_some());
        assert_eq!(loads.now(), [0]);
        assert!(datagram(&mut sessions, client(2), start, &backends, 0).is_ok());
    }

    /// The sessions picked are ended at once and handed over, their room given back and
    /// their bounds gone; the others go on, and the clients ended pick again.
    #[test]
    fn ending_the_sessions_picked_hands_each_over_and_gives_their_room_back() {
        let (mut sessions, loads) = sessions(3);
        let backends = backends(&[1, 1]);
        let start = Instant::now();
        for port in 1..=3 {
            let random = u64::from(port % 2);
            assert!(datagram(&mut sessions, client(port), start, &backends, random).is_ok());
        }
        let mut handed = Vec::new();
        sessions.end_where(
            |session| session.backend() == Destination::Upstream(UpstreamId(1)),
            |client, _| handed.push(client.peer.port()),
        );
        handed.sort_unstable();
        assert_eq!(handed, [1, 3]);
        assert_eq!(sessions.len(), 1);
        assert_eq!(loads.now(), [1]);
        assert_eq!(sessions.deadlines.taken(), 1, "their slots are given up");
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        assert_eq!(ended(&mut sessions, start + IDLE), [client(1), client(2)]);
    }

    /// A session keeps the idle bound it was made with: one set later is for the sessions
    /// made after it.
    #[test]
    fn a_session_keeps_the_idle_bound_it_was_made_with() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[1]);
        let start = Instant::now();
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        sessions.set_idle(seconds(IDLE * 2));
        assert!(datagram(&mut sessions, client(2), start, &backends, 0).is_ok());
        sessions.set_idle(seconds(IDLE / 2));
        // Heard from again, each is kept for its own bound from then.
        let heard = start + IDLE / 4;
        assert!(sessions.heard(&client(1), heard).is_some());
        assert!(datagram(&mut sessions, client(2), heard, &backends, 0).is_ok());
        assert_eq!(ended(&mut sessions, heard + IDLE / 2), []);
        assert_eq!(ended(&mut sessions, heard + IDLE), [client(1)]);
        assert_eq!(ended(&mut sessions, heard + IDLE * 2), [client(2)]);
    }

    /// A session whose listener expects `each` answers a datagram, after `sent` datagrams
    /// went to its endpoint and `read` answers came: whether the last answer said it is
    /// answered.
    fn answered_after(each: Option<u32>, sent: u32, read: u32) -> bool {
        let (mut sessions, _loads) = sessions(10);
        sessions.set_answers(each.and_then(NonZeroU32::new));
        let session = sessions
            .arrived(client(1), Instant::now(), &backends(&[1]), || 0, opened)
            .unwrap();
        for _ in 0..sent {
            session.sent();
        }
        let mut last = false;
        for _ in 0..read {
            last = session.answered();
        }
        last
    }

    /// A session is answered once the answers read are as many as its listener expects
    /// for each datagram that went to its endpoint, and not before.
    #[test]
    fn a_session_is_answered_once_every_datagram_has_its_answers() {
        assert!(answered_after(Some(1), 1, 1));
        assert!(!answered_after(Some(2), 1, 1));
        assert!(answered_after(Some(2), 1, 2));
        assert!(!answered_after(Some(1), 2, 1));
        assert!(answered_after(Some(1), 2, 2));
        assert!(answered_after(Some(3), 2, 6));
        assert!(!answered_after(Some(3), 2, 5));
    }

    /// One whose listener expects no number is never answered, nor one whose endpoint
    /// sends before any datagram went to it.
    #[test]
    fn a_session_is_never_answered_unasked_nor_without_a_count() {
        assert!(!answered_after(None, 1, 1));
        assert!(!answered_after(None, 0, 5));
        assert!(!answered_after(Some(1), 0, 1));
        assert!(!answered_after(Some(1), 0, 3));
    }

    /// The answers a listener expects are those of the sessions made from then on: one made
    /// before keeps the count it was made with.
    #[test]
    fn a_session_keeps_the_answers_it_was_made_with() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[1]);
        let start = Instant::now();
        sessions.set_answers(NonZeroU32::new(1));
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        sessions.set_answers(NonZeroU32::new(2));
        assert!(datagram(&mut sessions, client(2), start, &backends, 0).is_ok());
        sessions.set_answers(None);
        assert!(datagram(&mut sessions, client(3), start, &backends, 0).is_ok());
        let answered = |sessions: &mut Sessions<u32>, client: Client| {
            let session = sessions.heard(&client, start).unwrap();
            session.sent();
            session.answered()
        };
        assert!(answered(&mut sessions, client(1)));
        assert!(!answered(&mut sessions, client(2)));
        assert!(!answered(&mut sessions, client(3)));
    }

    /// A session's answer counts cost nothing in room, counted or not: with its idle bound
    /// they take what the idle bound took as a `Duration`, so that a session is the size of
    /// what it held before it counted answers.
    #[test]
    fn a_session_is_no_larger_for_its_answer_counts() {
        let before = size_of::<Destination>()
            + size_of::<Key>()
            + size_of::<Duration>()
            + size_of::<Option<Instant>>()
            + size_of::<Held>();
        assert_eq!(size_of::<Session<()>>(), before);
    }

    /// A drained session ends at its drain's bound however busy it is, or at its idle bound
    /// if that comes first; draining it again moves neither; one not picked goes on.
    #[test]
    fn a_drained_session_ends_at_its_drains_bound_or_idle_first() {
        let (mut sessions, _loads) = sessions(10);
        let backends = backends(&[1, 1]);
        let start = Instant::now();
        for port in 1..=3 {
            let random = u64::from(port == 3);
            assert!(datagram(&mut sessions, client(port), start, &backends, random).is_ok());
        }
        let by = start + IDLE / 2;
        sessions.drain(by, |session| {
            session.backend() == Destination::Upstream(UpstreamId(0))
        });
        sessions.drain(start + IDLE, |_| true);
        // Client 1 busy to the end, client 2 quiet: both end at the drain's bound.
        let nanosecond = Duration::from_nanos(1);
        assert!(sessions.heard(&client(1), by - nanosecond).is_some());
        assert_eq!(ended_how(&mut sessions, by - nanosecond), []);
        assert_eq!(
            ended_how(&mut sessions, by),
            [(client(1), Ended::Drained), (client(2), Ended::Drained)]
        );
        // Client 3 drains from the second alone, by when its idle bound comes: the drain's
        // bound, come, is what ends it.
        assert_eq!(
            ended_how(&mut sessions, start + IDLE),
            [(client(3), Ended::Drained)]
        );

        let (mut sessions, _loads) = self::sessions(10);
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        sessions.drain(start + IDLE * 2, |_| true);
        assert_eq!(
            ended_how(&mut sessions, start + IDLE),
            [(client(1), Ended::Idle)],
            "its idle bound came first"
        );
    }

    /// Once every session drains, a new client is given none, while the sessions there are
    /// carry on to their drain's bound.
    #[test]
    fn once_every_session_drains_no_new_one_is_made() {
        let (mut sessions, loads) = sessions(10);
        let backends = backends(&[1]);
        let start = Instant::now();
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        assert!(!sessions.draining());
        sessions.drain_every(start + IDLE / 2);
        assert!(sessions.draining());
        assert_eq!(
            datagram(&mut sessions, client(2), start, &backends, 0),
            Err(Dropped::Draining)
        );
        assert_eq!(loads.now(), [1], "no room is taken for it");
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        assert_eq!(
            ended_how(&mut sessions, start + IDLE / 2),
            [(client(1), Ended::Drained)]
        );
        assert!(sessions.is_empty());
    }

    #[test]
    fn nothing_is_opened_for_a_new_client_that_finds_no_room() {
        let (mut sessions, _loads) = sessions(0);
        let opened = Cell::new(false);
        let refused = sessions
            .arrived(
                client(1),
                Instant::now(),
                &backends(&[1]),
                || 0,
                |_| {
                    opened.set(true);
                    Ok::<_, ()>(0)
                },
            )
            .map(|_| ());
        assert_eq!(refused, Err(Dropped::NoRoom));
        assert!(!opened.get());
    }

    #[test]
    fn a_session_whose_value_cannot_be_opened_is_not_made_and_takes_no_room() {
        let (mut sessions, loads) = sessions(10);
        let backends = backends(&[1]);
        let refused = sessions
            .arrived(
                client(1),
                Instant::now(),
                &backends,
                || 0,
                |_| Err("no file"),
            )
            .map(|_| ());
        assert_eq!(refused, Err(Dropped::Unopened("no file")));
        assert!(sessions.is_empty());
        assert_eq!(loads.now(), [0]);
        assert_eq!(sessions.soonest(), None);
    }

    #[test]
    fn a_route_with_no_share_drops_every_datagram_and_makes_no_session() {
        let (mut sessions, loads) = sessions(10);
        let backends = backends(&[0, 0]);
        let start = Instant::now();
        let opened = Cell::new(false);
        let refused = sessions
            .arrived(
                client(1),
                start,
                &backends,
                || 0,
                |_| {
                    opened.set(true);
                    Ok::<_, ()>(0)
                },
            )
            .map(|_| ());
        assert_eq!(refused, Err(Dropped::NoBackend));
        assert!(!opened.get());
        assert!(sessions.is_empty());
        assert_eq!(loads.now(), [0]);
    }

    #[test]
    fn a_session_picked_for_an_invalid_backend_counts_as_any_other() {
        let (mut sessions, loads) = sessions(1);
        let backends = WeightedBackends::with_invalid([(Err("no Service".to_owned()), 1)]);
        let start = Instant::now();
        assert_eq!(
            datagram(&mut sessions, client(1), start, &backends, 0),
            Ok(Destination::Invalid(0))
        );
        assert_eq!(loads.now(), [1]);
        assert_eq!(
            datagram(&mut sessions, client(2), start, &backends, 0),
            Err(Dropped::NoRoom)
        );
    }

    #[test]
    fn a_session_ended_early_is_never_expired_and_its_client_picks_again() {
        let (mut sessions, loads) = sessions(10);
        let backends = backends(&[1, 1]);
        let start = Instant::now();
        assert_eq!(
            datagram(&mut sessions, client(1), start, &backends, 0),
            Ok(Destination::Upstream(UpstreamId(0)))
        );
        assert!(sessions.end(&client(1)).is_some());
        assert!(sessions.end(&client(1)).is_none());
        assert_eq!(loads.now(), [0]);
        assert!(sessions.heard(&client(1), start).is_none());
        let later = start + IDLE / 2;
        assert_eq!(
            datagram(&mut sessions, client(1), later, &backends, 1),
            Ok(Destination::Upstream(UpstreamId(1)))
        );
        assert_eq!(
            ended(&mut sessions, start + IDLE),
            [],
            "the old bound is gone"
        );
        assert_eq!(ended(&mut sessions, later + IDLE), [client(1)]);
    }

    /// What may happen to a listener's sessions, a number of seconds after the last.
    #[derive(Debug, Clone)]
    enum Op {
        Datagram {
            client: u8,
            after: u64,
            random: u64,
        },
        Answered {
            client: u8,
            after: u64,
        },
        End {
            client: u8,
            after: u64,
        },
        /// The idle bound of the sessions made from then.
        Idle {
            idle: u64,
            after: u64,
        },
        /// The clients in `which`, as bits, drain, by `within` from then.
        Drain {
            which: u8,
            within: u64,
            after: u64,
        },
        /// Every session drains, by `within` from then, and none is made.
        DrainEvery {
            within: u64,
            after: u64,
        },
        /// The clients in `which`, as bits, are ended.
        EndWhere {
            which: u8,
            after: u64,
        },
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0_u8..8, 0_u64..1500, any::<u64>()).prop_map(|(client, after, random)| Op::Datagram {
                client,
                after,
                random
            }),
            (0_u8..8, 0_u64..1500).prop_map(|(client, after)| Op::Answered { client, after }),
            (0_u8..8, 0_u64..1500).prop_map(|(client, after)| Op::End { client, after }),
            (1_u64..3000, 0_u64..1500).prop_map(|(idle, after)| Op::Idle { idle, after }),
            (any::<u8>(), 0_u64..3000, 0_u64..1500).prop_map(|(which, within, after)| Op::Drain {
                which,
                within,
                after
            }),
            (0_u64..3000, 0_u64..1500).prop_map(|(within, after)| Op::DrainEvery { within, after }),
            (any::<u8>(), 0_u64..1500).prop_map(|(which, after)| Op::EndWhere { which, after }),
        ]
    }

    /// A client in the model: its backend, the last time anything was heard of it, its idle
    /// bound and when it ends draining, all in seconds.
    #[derive(Debug, Clone, Copy)]
    struct Modelled {
        backend: usize,
        last: u64,
        idle: u64,
        drain_by: Option<u64>,
    }

    impl Modelled {
        fn due(&self) -> u64 {
            let idle = self.last + self.idle;
            self.drain_by.map_or(idle, |by| idle.min(by))
        }

        fn ended(&self, now: u64) -> Option<Ended> {
            if self.due() > now {
                return None;
            }
            Some(if self.drain_by.is_some_and(|by| by <= now) {
                Ended::Drained
            } else {
                Ended::Idle
            })
        }
    }

    /// Clients on two local addresses, so that one port on each is two clients.
    fn numbered(n: u8) -> Client {
        Client {
            peer: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), u16::from(n % 4)),
            local: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 53 + u16::from(n / 4)),
        }
    }

    /// The backend a weighted pick gives for `random`, the long way round: walking the
    /// weights to the one whose span holds the point.
    fn naive_pick(weights: &[u32], random: u64) -> Option<usize> {
        let total: u64 = weights.iter().map(|weight| u64::from(*weight)).sum();
        let point = random.checked_rem(total)?;
        let mut reached = 0;
        weights.iter().position(|weight| {
            reached += u64::from(*weight);
            point < reached
        })
    }

    proptest! {
        #[test]
        fn sessions_are_what_a_list_of_clients_and_their_last_times_says(
            weights in prop::collection::vec(0_u32..4, 1..5),
            idle_s in 1_u32..3000,
            cap in 0_usize..6,
            ops in prop::collection::vec(op(), 1..60),
        ) {
            let loads = Loads::new(1, cap, QUIC_MOST, 1);
            let mut sessions = Sessions::<u8>::new(idle_s, Arc::clone(&loads), 0, 0);
            let backends = backends(&weights);
            let start = Instant::now();
            // The model: each client with a session, and the idle bound and drain of those
            // made from now.
            let mut model: BTreeMap<u8, Modelled> = BTreeMap::new();
            let mut idle_s = u64::from(idle_s);
            let mut draining = false;
            let mut now_s = 0;
            for op in ops {
                let (Op::Datagram { after, .. }
                | Op::Answered { after, .. }
                | Op::End { after, .. }
                | Op::Idle { after, .. }
                | Op::Drain { after, .. }
                | Op::DrainEvery { after, .. }
                | Op::EndWhere { after, .. }) = op;
                now_s += after;
                let now = start + Duration::from_secs(now_s);

                let mut expired: Vec<(Client, Ended)> = Vec::new();
                sessions.expire(now, |client, _, how| expired.push((client, how)));
                expired.sort_by_key(|(client, _)| (client.local, client.peer));
                let mut due: Vec<(Client, Ended)> = model
                    .iter()
                    .filter_map(|(client, modelled)| Some((numbered(*client), modelled.ended(now_s)?)))
                    .collect();
                model.retain(|_, modelled| modelled.ended(now_s).is_none());
                due.sort_by_key(|(client, _)| (client.local, client.peer));
                prop_assert_eq!(expired, due);

                // The value a session holds is its client's number, which `which` picks by.
                let picked = |which: u8, client: u8| which & (1 << client) != 0;
                match op {
                    Op::Datagram { client, random, .. } => {
                        let found = sessions
                            .arrived(numbered(client), now, &backends, || random, |_| Ok::<_, ()>(client))
                            .map(|session| (session.backend(), *session.value_mut()));
                        let expected = if let Some(modelled) = model.get_mut(&client) {
                            modelled.last = now_s;
                            Ok(modelled.backend)
                        } else if draining {
                            Err(Dropped::Draining)
                        } else {
                            match naive_pick(&weights, random) {
                                None => Err(Dropped::NoBackend),
                                Some(_) if model.len() == cap => Err(Dropped::NoRoom),
                                Some(backend) => {
                                    model.insert(client, Modelled { backend, last: now_s, idle: idle_s, drain_by: None });
                                    Ok(backend)
                                }
                            }
                        };
                        let expected = expected
                            .map(|backend| (Destination::Upstream(UpstreamId(backend)), client));
                        prop_assert_eq!(found, expected);
                    }
                    Op::Answered { client, .. } => {
                        let found = sessions.heard(&numbered(client), now).map(|session| session.backend());
                        let expected = model.get_mut(&client).map(|modelled| {
                            modelled.last = now_s;
                            Destination::Upstream(UpstreamId(modelled.backend))
                        });
                        prop_assert_eq!(found, expected);
                    }
                    Op::End { client, .. } => {
                        let found = sessions.end(&numbered(client)).map(|session| session.backend());
                        let expected = model
                            .remove(&client)
                            .map(|modelled| Destination::Upstream(UpstreamId(modelled.backend)));
                        prop_assert_eq!(found, expected);
                    }
                    Op::Idle { idle, .. } => {
                        sessions.set_idle(u32::try_from(idle).unwrap());
                        idle_s = idle;
                    }
                    Op::Drain { which, within, .. } => {
                        let by = now_s + within;
                        sessions.drain(start + Duration::from_secs(by), |session| picked(which, *session.value()));
                        for (client, modelled) in &mut model {
                            if picked(which, *client) && modelled.drain_by.is_none() {
                                modelled.drain_by = Some(by);
                            }
                        }
                    }
                    Op::DrainEvery { within, .. } => {
                        let by = now_s + within;
                        sessions.drain_every(start + Duration::from_secs(by));
                        draining = true;
                        for modelled in model.values_mut() {
                            modelled.drain_by.get_or_insert(by);
                        }
                    }
                    Op::EndWhere { which, .. } => {
                        let mut found: Vec<u8> = Vec::new();
                        sessions.end_where(|session| picked(which, *session.value()), |_, session| found.push(*session.value()));
                        found.sort_unstable();
                        let expected: Vec<u8> = model.keys().copied().filter(|client| picked(which, *client)).collect();
                        model.retain(|client, _| !picked(which, *client));
                        prop_assert_eq!(found, expected);
                    }
                }

                prop_assert_eq!(sessions.len(), model.len());
                prop_assert_eq!(sessions.draining(), draining);
                prop_assert_eq!(loads.now(), vec![model.len()]);
                prop_assert_eq!(sessions.deadlines.taken(), model.len(), "an ended session's slot is given up");
                let first_due = model.values().map(Modelled::due).min();
                match (sessions.soonest(), first_due) {
                    (None, None) => {}
                    (Some(soonest), Some(due)) => prop_assert!(
                        soonest <= start + Duration::from_secs(due),
                        "the soonest bound is never later than it comes"
                    ),
                    (soonest, due) => prop_assert!(false, "soonest {soonest:?}, due {due:?}"),
                }
            }
        }

        #[test]
        fn each_backend_has_its_weights_share_of_clients_each_from_a_new_port(
            weights in prop::collection::vec(0_u32..10, 1..6)
                .prop_filter("some backend has a share", |weights| weights.iter().any(|w| *w > 0)),
            seed in any::<u64>(),
        ) {
            const CLIENTS: u16 = 4000;
            let loads = Loads::new(1, usize::MAX, QUIC_MOST, 1);
            let mut sessions = Sessions::<()>::new(seconds(IDLE), loads, 0, 0);
            let backends = backends(&weights);
            let now = Instant::now();
            let mut random = seed;
            let mut picked = vec![0_u32; weights.len()];
            for port in 0..CLIENTS {
                let backend = sessions
                    .arrived(client(port), now, &backends, || splitmix(&mut random), |_| Ok::<_, ()>(()))
                    .map(|session| session.backend());
                let Ok(Destination::Upstream(UpstreamId(at))) = backend else {
                    panic!("no session for port {port}: {backend:?}");
                };
                picked[at] += 1;
            }
            let total: u32 = weights.iter().sum();
            for (at, weight) in weights.iter().enumerate() {
                let share = f64::from(picked[at]) / f64::from(CLIENTS);
                let asked = f64::from(*weight) / f64::from(total);
                // Gateway API's weighted test allows each share 0.05 either way.
                prop_assert!((share - asked).abs() <= 0.05, "backend {at}: {share} of {asked}");
                if *weight == 0 {
                    prop_assert_eq!(picked[at], 0, "a weight of 0 is never picked");
                }
            }
        }
    }

    /// The next of a stream of numbers uniform over `u64`, as the data plane's own are.
    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}
