//! A worker's sessions on one `udp` listener
//! ([24 §3](../../../../docs/24-udp-proxying.md)): which backend each client's datagrams go
//! to, until neither the client nor its backend has sent anything for the listener's idle
//! bound.
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
//! time stored, and the heap is touched at most once an idle bound a session.
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
    /// The connection it counts as, given back when it ends: held for its drop alone.
    _held: Held,
    value: T,
}

impl<T> Session<T> {
    /// Where its datagrams go: picked by its first.
    #[must_use]
    #[cfg_attr(
        not(any(test, feature = "fuzzing")),
        expect(
            dead_code,
            reason = "a session's access log record names it (24 §8, step 3b)"
        )
    )]
    pub fn backend(&self) -> Destination {
        self.backend
    }

    /// What the caller opened for it.
    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// Why a datagram from a new client made no session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dropped<E> {
    /// No backend of the route has a share: every weight is 0.
    NoBackend,
    /// The worker has no room for another connection, or the listener holds its share.
    NoRoom,
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
    idle: Duration,
    loads: Arc<Loads>,
    worker: usize,
    listener: usize,
    /// The slots of the sessions an expiry ends, given up once the heap has handed them
    /// all over: kept between expiries, so that ending sessions does not allocate.
    ended: Vec<Key>,
}

impl<T> Sessions<T> {
    /// No sessions yet, of a listener whose idle bound is `idle`, counted as connections of
    /// `worker`'s on `listener` in `loads`.
    #[must_use]
    pub fn new(idle: Duration, loads: Arc<Loads>, worker: usize, listener: usize) -> Self {
        Self {
            by_client: HashMap::new(),
            deadlines: Heap::default(),
            idle,
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
    /// is carried on.
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
                touch(&mut self.deadlines, session.deadline, now, self.idle);
                Ok(session)
            }
            Entry::Vacant(vacant) => {
                let backend = backends.pick(random()).ok_or(Dropped::NoBackend)?;
                let held = self
                    .loads
                    .take(self.worker, self.listener)
                    .ok_or(Dropped::NoRoom)?;
                // Dropping `held` with the error gives the room back.
                let value = open(backend).map_err(Dropped::Unopened)?;
                let deadline = self.deadlines.add(client);
                touch(&mut self.deadlines, deadline, now, self.idle);
                Ok(vacant.insert(Session {
                    backend,
                    deadline,
                    _held: held,
                    value,
                }))
            }
        }
    }

    /// The backend answered `client` at `now`: its session, if it has one, which the
    /// answer keeps from its idle bound as a datagram from the client does.
    pub fn answered(&mut self, client: &Client, now: Instant) -> Option<&mut Session<T>> {
        let session = self.by_client.get_mut(client)?;
        touch(&mut self.deadlines, session.deadline, now, self.idle);
        Some(session)
    }

    /// Ends `client`'s session before its idle bound, as a refused one ends: handed back,
    /// for the caller to drop, which gives its room back. The client's next datagram makes
    /// a new one.
    #[cfg_attr(
        not(any(test, feature = "fuzzing")),
        expect(
            dead_code,
            reason = "the refused end ends a session so (24 §8, step 3b)"
        )
    )]
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

    /// The time is `now`: every session that has carried nothing either way for the idle
    /// bound up to it is ended and handed to `each`, with its client.
    pub fn expire(&mut self, now: Instant, mut each: impl FnMut(Client, Session<T>)) {
        let Self {
            by_client,
            deadlines,
            ended,
            ..
        } = self;
        deadlines.expire(now, |client| {
            if let Some(session) = by_client.remove(client) {
                ended.push(session.deadline);
                each(*client, session);
            }
        });
        for deadline in ended.drain(..) {
            deadlines.remove(deadline);
        }
    }

    /// Ends every session, as a listener taken away ends them: each handed to `each`, with
    /// its client.
    pub fn end_every(&mut self, mut each: impl FnMut(Client, Session<T>)) {
        for (client, session) in self.by_client.drain() {
            each(client, session);
        }
        self.deadlines = Heap::default();
    }

    /// How many sessions there are.
    #[must_use]
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn len(&self) -> usize {
        self.by_client.len()
    }

    /// Whether there are none.
    #[must_use]
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn is_empty(&self) -> bool {
        self.by_client.is_empty()
    }
}

/// The session whose slot is `deadline` was heard from at `now`: its idle bound is
/// reckoned from then.
fn touch(deadlines: &mut Heap<Client>, deadline: Key, now: Instant, idle: Duration) {
    // An idle bound past what the clock can count to is one never reached.
    if let Some(due) = now.checked_add(idle) {
        deadlines.wait(deadline, due, || due, |_| {});
    }
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
        (Sessions::new(IDLE, Arc::clone(&loads), 0, 0), loads)
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
        let mut ended = Vec::new();
        sessions.expire(now, |client, _| ended.push(client));
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
        assert!(sessions.answered(&client(2), heard).is_some());
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

    #[test]
    fn ending_every_session_hands_each_over_and_gives_their_room_back() {
        let (mut sessions, loads) = sessions(3);
        let backends = backends(&[1]);
        let start = Instant::now();
        for port in 1..=3 {
            assert!(datagram(&mut sessions, client(port), start, &backends, 0).is_ok());
        }
        let mut handed = Vec::new();
        sessions.end_every(|client, _| handed.push(client.peer.port()));
        handed.sort_unstable();
        assert_eq!(handed, [1, 2, 3]);
        assert!(sessions.is_empty());
        assert_eq!(loads.now(), [0]);
        assert_eq!(sessions.soonest(), None, "no idle bound is left to come");
        // The table goes on as new: a client is given a session again, and its bound comes.
        assert!(datagram(&mut sessions, client(1), start, &backends, 0).is_ok());
        assert_eq!(ended(&mut sessions, start + IDLE), [client(1)]);
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
        assert!(sessions.answered(&client(1), start).is_none());
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

    /// What may happen to a listener's sessions, a number of milliseconds after the last.
    #[derive(Debug, Clone)]
    enum Op {
        Datagram { client: u8, after: u64, random: u64 },
        Answered { client: u8, after: u64 },
        End { client: u8, after: u64 },
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
        ]
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
            idle_ms in 1_u64..3000,
            cap in 0_usize..6,
            ops in prop::collection::vec(op(), 1..60),
        ) {
            let idle = Duration::from_millis(idle_ms);
            let loads = Loads::new(1, cap, QUIC_MOST, 1);
            let mut sessions = Sessions::<u8>::new(idle, Arc::clone(&loads), 0, 0);
            let backends = backends(&weights);
            let start = Instant::now();
            // The model: each client with a session, its backend and the last time, in
            // milliseconds, anything was heard of it.
            let mut model: BTreeMap<u8, (usize, u64)> = BTreeMap::new();
            let mut now_ms = 0;
            for op in ops {
                let (Op::Datagram { client, after, .. }
                | Op::Answered { client, after }
                | Op::End { client, after }) = op;
                now_ms += after;
                let now = start + Duration::from_millis(now_ms);

                let mut expired: Vec<Client> = Vec::new();
                sessions.expire(now, |client, _| expired.push(client));
                expired.sort_by_key(|client| (client.local, client.peer));
                let due: Vec<u8> = model
                    .iter()
                    .filter(|(_, (_, last))| last + idle_ms <= now_ms)
                    .map(|(client, _)| *client)
                    .collect();
                model.retain(|_, (_, last)| *last + idle_ms > now_ms);
                let mut due: Vec<Client> = due.into_iter().map(numbered).collect();
                due.sort_by_key(|client| (client.local, client.peer));
                prop_assert_eq!(expired, due);

                match op {
                    Op::Datagram { random, .. } => {
                        let found = sessions
                            .arrived(numbered(client), now, &backends, || random, |_| Ok::<_, ()>(client))
                            .map(|session| (session.backend(), *session.value_mut()));
                        let expected = if let Some((backend, last)) = model.get_mut(&client) {
                            *last = now_ms;
                            Ok(*backend)
                        } else {
                            match naive_pick(&weights, random) {
                                None => Err(Dropped::NoBackend),
                                Some(_) if model.len() == cap => Err(Dropped::NoRoom),
                                Some(backend) => {
                                    model.insert(client, (backend, now_ms));
                                    Ok(backend)
                                }
                            }
                        };
                        let expected = expected
                            .map(|backend| (Destination::Upstream(UpstreamId(backend)), client));
                        prop_assert_eq!(found, expected);
                    }
                    Op::Answered { .. } => {
                        let found = sessions.answered(&numbered(client), now).map(|session| session.backend());
                        let expected = model.get_mut(&client).map(|(backend, last)| {
                            *last = now_ms;
                            Destination::Upstream(UpstreamId(*backend))
                        });
                        prop_assert_eq!(found, expected);
                    }
                    Op::End { .. } => {
                        let found = sessions.end(&numbered(client)).map(|session| session.backend());
                        let expected = model
                            .remove(&client)
                            .map(|(backend, _)| Destination::Upstream(UpstreamId(backend)));
                        prop_assert_eq!(found, expected);
                    }
                }

                prop_assert_eq!(sessions.len(), model.len());
                prop_assert_eq!(loads.now(), vec![model.len()]);
                prop_assert_eq!(sessions.deadlines.taken(), model.len(), "an ended session's slot is given up");
                let first_due = model.values().map(|(_, last)| last + idle_ms).min();
                match (sessions.soonest(), first_due) {
                    (None, None) => {}
                    (Some(soonest), Some(due)) => prop_assert!(
                        soonest <= start + Duration::from_millis(due),
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
            let mut sessions = Sessions::<()>::new(IDLE, loads, 0, 0);
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
