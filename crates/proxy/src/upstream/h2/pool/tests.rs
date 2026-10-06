use super::*;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

const KEY: u64 = 7;

fn limits() -> Limits {
    Limits {
        streams: 2,
        connections: 3,
        connections_total: 8,
        waiting: 4,
        dialing: 1,
        requests: 1_000,
        age: Duration::from_secs(3_600),
        idle: Duration::from_secs(60),
    }
}

/// A pool and the time, with what each call asked to be done.
struct Harness {
    pool: Pool,
    now: Instant,
}

impl Harness {
    fn new(limits: Limits) -> Self {
        Self {
            pool: Pool::new(limits),
            now: Instant::now(),
        }
    }

    fn take(&mut self, key: u64) -> (Taken, Vec<Action>) {
        let mut actions = Vec::new();
        let taken = self.pool.take(key, self.now, &mut actions);
        (taken, actions)
    }

    fn opened(&mut self, key: u64, id: ConnectionId, peer: u32) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.opened(key, id, peer, self.now, &mut actions);
        actions
    }

    fn failed(&mut self, key: u64, id: ConnectionId) -> Vec<Action> {
        self.failed_for(key, id, Failure::Unreachable)
    }

    fn failed_for(&mut self, key: u64, id: ConnectionId, why: Failure) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.failed(key, id, why, self.now, &mut actions);
        actions
    }

    fn ended(&mut self, key: u64, id: ConnectionId) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.ended(key, id, self.now, &mut actions);
        actions
    }

    fn peer_limit(&mut self, key: u64, id: ConnectionId, peer: u32) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.peer_limit(key, id, peer, self.now, &mut actions);
        actions
    }

    fn going_away(&mut self, key: u64, id: ConnectionId) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.going_away(key, id, self.now, &mut actions);
        actions
    }

    fn cancel(&mut self, key: u64, waiter: WaiterId) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.cancel(key, waiter, self.now, &mut actions);
        actions
    }

    fn sweep(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.sweep(self.now, &mut actions);
        actions
    }

    /// Opens the first connection to `key`: one request waits, the connection is dialled
    /// and opens, and the request is granted a place on it.
    fn first_connection(&mut self, key: u64, peer: u32) -> ConnectionId {
        let (taken, actions) = self.take(key);
        let Taken::Waiting(waiter) = taken else {
            panic!("an empty pool had a place: {taken:?}");
        };
        let [Action::Dial(dialled, id)] = actions[..] else {
            panic!("nothing was dialled: {actions:?}");
        };
        assert_eq!(dialled, key);
        assert_eq!(self.opened(key, id, peer), [Action::Grant(waiter, id)]);
        assert!(self.settled(key, id, peer).is_empty());
        id
    }

    fn settled(&mut self, key: u64, id: ConnectionId, peer: u32) -> Vec<Action> {
        let mut actions = Vec::new();
        self.pool.settled(key, id, peer, self.now, &mut actions);
        actions
    }
}

#[test]
fn the_first_request_waits_while_a_connection_is_opened_and_the_next_takes_a_place_on_it() {
    let mut pool = Harness::new(limits());
    let first = pool.first_connection(KEY, 100);
    assert_eq!(pool.take(KEY), (Taken::Place(first), vec![]));
    assert_eq!(pool.pool.connections(), 1);
}

/// The first connection with a place gets the stream; another is opened only when none
/// has one, and the stream cap is what spreads them.
#[test]
fn connections_are_filled_first_in_the_order_they_were_opened() {
    let mut pool = Harness::new(limits());
    let first = pool.first_connection(KEY, 100);
    assert_eq!(pool.take(KEY).0, Taken::Place(first));
    // Both of the first's places taken: the next waits, and a second is opened for it.
    let (taken, actions) = pool.take(KEY);
    let Taken::Waiting(waiter) = taken else {
        panic!("{taken:?}")
    };
    let [Action::Dial(KEY, second)] = actions[..] else {
        panic!("{actions:?}")
    };
    assert_eq!(
        pool.opened(KEY, second, 100),
        [Action::Grant(waiter, second)]
    );
    // A place on the first comes free: it is the one filled again.
    assert!(pool.ended(KEY, first).is_empty());
    assert_eq!(pool.take(KEY).0, Taken::Place(first));
}

/// A request that comes while another waits goes behind it, even when a place opens up
/// the moment it arrives; and places are granted in the order requests came.
#[test]
fn nobody_overtakes_a_request_that_is_waiting() {
    let mut pool = Harness::new(Limits {
        connections: 1,
        ..limits()
    });
    let only = pool.first_connection(KEY, 100);
    assert_eq!(pool.take(KEY).0, Taken::Place(only));
    let (Taken::Waiting(second), none) = pool.take(KEY) else {
        panic!()
    };
    // No second connection: one is all there may be.
    assert!(none.is_empty());
    let (Taken::Waiting(third), _) = pool.take(KEY) else {
        panic!()
    };
    assert_eq!(pool.ended(KEY, only), [Action::Grant(second, only)]);
    // A newcomer finds `third` waiting and queues behind it, though nothing is free now.
    let (Taken::Waiting(fourth), _) = pool.take(KEY) else {
        panic!()
    };
    assert_eq!(pool.ended(KEY, only), [Action::Grant(third, only)]);
    assert_eq!(pool.ended(KEY, only), [Action::Grant(fourth, only)]);
}

#[test]
fn past_its_bound_a_request_is_refused_rather_than_queued() {
    let mut pool = Harness::new(Limits {
        connections: 1,
        waiting: 2,
        ..limits()
    });
    let only = pool.first_connection(KEY, 1);
    let _ = only;
    assert!(matches!(pool.take(KEY).0, Taken::Waiting(_)));
    assert!(matches!(pool.take(KEY).0, Taken::Waiting(_)));
    assert_eq!(pool.take(KEY).0, Taken::Refused);
}

/// A limit the peer lowers below what is open lets those streams finish and opens no more
/// until there is room; one it raises again is used at once.
#[test]
fn the_peers_limit_is_honoured_as_it_changes() {
    let mut pool = Harness::new(Limits {
        streams: 4,
        connections: 1,
        ..limits()
    });
    let only = pool.first_connection(KEY, 4);
    for _ in 0..3 {
        assert_eq!(pool.take(KEY).0, Taken::Place(only));
    }
    // Four open; the peer now allows one.
    assert!(pool.peer_limit(KEY, only, 1).is_empty());
    let (Taken::Waiting(waiter), _) = pool.take(KEY) else {
        panic!()
    };
    for _ in 0..3 {
        assert!(pool.ended(KEY, only).is_empty(), "granted over the limit");
    }
    // Down to one open, which is all the peer allows; then it relents.
    assert_eq!(pool.peer_limit(KEY, only, 2), [Action::Grant(waiter, only)]);
}

/// A peer that allows no streams at all has no place to give, and the requests waiting
/// for one get another connection if there may be one.
#[test]
fn a_peer_that_allows_nothing_has_another_connection_opened_beside_it() {
    let mut pool = Harness::new(limits());
    let (Taken::Waiting(waiter), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, first)] = actions[..] else {
        panic!()
    };
    // Not heard from yet: what it will take is counted as coming.
    assert!(pool.opened(KEY, first, 0).is_empty());
    let actions = pool.settled(KEY, first, 0);
    let [Action::Dial(KEY, second)] = actions[..] else {
        panic!("{actions:?}")
    };
    assert_eq!(
        pool.opened(KEY, second, 10),
        [Action::Grant(waiter, second)]
    );
}

/// On a connection not yet heard from, only what may be sent before the peer's SETTINGS
/// goes; the rest wait for them rather than have another connection opened beside it.
#[test]
fn a_new_connection_is_counted_on_until_its_peer_is_heard_from() {
    let mut pool = Harness::new(Limits {
        streams: 3,
        ..limits()
    });
    let (Taken::Waiting(first), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, id)] = actions[..] else {
        panic!()
    };
    let (Taken::Waiting(second), none) = pool.take(KEY) else {
        panic!()
    };
    assert!(none.is_empty());
    // One may go before the SETTINGS.
    assert_eq!(pool.opened(KEY, id, 1), [Action::Grant(first, id)]);
    let (Taken::Waiting(third), none) = pool.take(KEY) else {
        panic!()
    };
    assert!(none.is_empty(), "another was opened beside it: {none:?}");
    assert_eq!(
        pool.settled(KEY, id, 100),
        [Action::Grant(second, id), Action::Grant(third, id)]
    );
}

/// A connection that cannot be opened fails what waits for it only when there is nothing
/// else that could serve them.
#[test]
fn a_failed_connection_fails_its_waiters_only_when_nothing_else_is_left() {
    let mut pool = Harness::new(limits());
    let (Taken::Waiting(waiter), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, first)] = actions[..] else {
        panic!()
    };
    assert_eq!(
        pool.failed(KEY, first),
        [Action::Fail(waiter, Failure::Unreachable)]
    );
    assert_eq!(pool.pool.connections(), 0);

    // With a connection already up but full, a second that fails leaves them waiting for
    // the first.
    let up = pool.first_connection(KEY, 1);
    let (Taken::Waiting(waiter), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, second)] = actions[..] else {
        panic!()
    };
    assert!(pool.failed(KEY, second).is_empty());
    assert_eq!(pool.ended(KEY, up), [Action::Grant(waiter, up)]);
}

/// A connection that could not be opened for want of the worker's own, a socket, refuses
/// whoever it leaves with nothing for that, not for an upstream out of reach.
#[test]
fn a_connection_the_worker_had_no_socket_for_refuses_its_waiters_as_that() {
    let mut pool = Harness::new(limits());
    let (Taken::Waiting(waiter), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, first)] = actions[..] else {
        panic!()
    };
    assert_eq!(
        pool.failed_for(KEY, first, Failure::Exhausted),
        [Action::Fail(waiter, Failure::Exhausted)]
    );
}

/// GOAWAY: the connection takes no more, is closed when its streams end, and whoever waits
/// is served by another.
#[test]
fn a_connection_told_to_go_away_finishes_its_streams_and_closes() {
    let mut pool = Harness::new(limits());
    let first = pool.first_connection(KEY, 1);
    let (Taken::Waiting(waiter), _) = pool.take(KEY) else {
        panic!()
    };
    // The dial for `waiter` is under way; the first is told to go.
    let second = ConnectionId(pool.pool.next);
    assert!(pool.going_away(KEY, first).is_empty());
    assert_eq!(pool.opened(KEY, second, 1), [Action::Grant(waiter, second)]);
    assert_eq!(pool.ended(KEY, first), [Action::Close(first)]);
    assert_eq!(pool.pool.connections(), 1);
}

/// A connection that has carried its share, or had its time, is retired: new streams go
/// elsewhere, and it closes once its own end.
#[test]
fn a_connection_is_retired_by_its_request_count_and_its_age() {
    let mut pool = Harness::new(Limits {
        streams: 10,
        requests: 2,
        ..limits()
    });
    let first = pool.first_connection(KEY, 10);
    assert_eq!(pool.take(KEY).0, Taken::Place(first));
    // Two carried: the third is not given it.
    let (taken, actions) = pool.take(KEY);
    assert!(matches!(taken, Taken::Waiting(_)), "{taken:?}");
    assert!(matches!(actions[..], [Action::Dial(KEY, _)]), "{actions:?}");
    assert!(pool.ended(KEY, first).is_empty());
    assert_eq!(pool.ended(KEY, first), [Action::Close(first)]);

    let mut pool = Harness::new(limits());
    let old = pool.first_connection(KEY, 10);
    assert!(pool.ended(KEY, old).is_empty());
    pool.now += limits().age;
    // Spent and empty: closed at once, and the request waits for a fresh one.
    let (taken, actions) = pool.take(KEY);
    assert!(matches!(taken, Taken::Waiting(_)), "{taken:?}");
    assert_eq!(actions[0], Action::Close(old));
    assert!(
        matches!(actions[1..], [Action::Dial(KEY, _)]),
        "{actions:?}"
    );
}

#[test]
fn an_idle_connection_is_closed_by_the_sweep_and_a_busy_one_is_not() {
    let mut pool = Harness::new(limits());
    let idle = pool.first_connection(KEY, 10);
    let busy = pool.first_connection(KEY + 1, 10);
    assert!(pool.ended(KEY, idle).is_empty());
    pool.now += limits().idle;
    assert_eq!(pool.sweep(), [Action::Close(idle)]);
    assert_eq!(pool.pool.connections(), 1);
    assert_eq!(pool.take(KEY + 1).0, Taken::Place(busy));
}

#[test]
fn connections_are_bounded_per_destination_and_per_worker() {
    let mut pool = Harness::new(Limits {
        streams: 1,
        connections: 2,
        connections_total: 3,
        waiting: 10,
        dialing: 10,
        ..limits()
    });
    let dials = |actions: &[Action]| {
        actions
            .iter()
            .filter(|action| matches!(action, Action::Dial(..)))
            .count()
    };
    let mut dialled = 0;
    for _ in 0..5 {
        dialled += dials(&pool.take(KEY).1);
    }
    assert_eq!(dialled, 2, "past the bound for one destination");
    for _ in 0..5 {
        dialled += dials(&pool.take(KEY + 1).1);
    }
    assert_eq!(dialled, 3, "past the bound for the worker");
    assert_eq!(pool.pool.connections(), 3);
}

#[test]
fn a_request_that_gives_up_is_never_granted() {
    let mut pool = Harness::new(Limits {
        connections: 1,
        ..limits()
    });
    let only = pool.first_connection(KEY, 1);
    let (Taken::Waiting(gone), _) = pool.take(KEY) else {
        panic!()
    };
    let (Taken::Waiting(stays), _) = pool.take(KEY) else {
        panic!()
    };
    assert!(pool.cancel(KEY, gone).is_empty());
    assert_eq!(pool.ended(KEY, only), [Action::Grant(stays, only)]);
}

/// A destination gone from the config closes what it has as its streams end, and opens
/// nothing that outlives whoever still waits.
#[test]
fn a_retired_destination_winds_down() {
    let mut pool = Harness::new(limits());
    let idle = pool.first_connection(KEY, 1);
    // Full, so the next request has a second connection opened for it.
    let (Taken::Waiting(waiter), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, busy)] = actions[..] else {
        panic!("{actions:?}")
    };
    assert_eq!(pool.opened(KEY, busy, 1), [Action::Grant(waiter, busy)]);
    assert!(pool.ended(KEY, idle).is_empty());
    let mut actions = Vec::new();
    pool.pool.retire(KEY, pool.now, &mut actions);
    assert_eq!(actions, [Action::Close(idle)]);
    // Held while a connection to it is.
    assert!(pool.pool.holds(KEY));
    assert_eq!(pool.ended(KEY, busy), [Action::Close(busy)]);
    assert_eq!(pool.pool.connections(), 0);
    assert!(!pool.pool.holds(KEY));
}

/// A connection closed because its retired destination no longer needs it frees a place
/// under the worker's bound, and a request another destination has waiting for one gets
/// a connection dialled, whichever news let the last waiter be served: a connection
/// opening, its peer's SETTINGS heard, or its peer's limit rising.
#[test]
fn a_place_freed_under_the_workers_bound_is_dialled_for_whoever_waits() {
    const OTHER: u64 = KEY + 1;
    for news in ["opened", "settled", "peer limit"] {
        let mut pool = Harness::new(Limits {
            streams: 1,
            connections_total: 3,
            dialing: 2,
            ..limits()
        });
        let (Taken::Waiting(served), first) = pool.take(KEY) else {
            panic!()
        };
        let (Taken::Waiting(gone), second) = pool.take(KEY) else {
            panic!()
        };
        let ([Action::Dial(KEY, idle)], [Action::Dial(KEY, serving)]) = (&first[..], &second[..])
        else {
            panic!("{first:?} {second:?}")
        };
        let (idle, serving) = (*idle, *serving);
        // The other destination's one connection, busy.
        pool.first_connection(OTHER, 1);
        // At the worker's bound: this one waits with nothing dialled for it.
        let (Taken::Waiting(_), held) = pool.take(OTHER) else {
            panic!()
        };
        assert!(held.is_empty(), "{held:?}");
        assert!(pool.cancel(KEY, gone).is_empty());
        let mut retiring = Vec::new();
        pool.pool.retire(KEY, pool.now, &mut retiring);
        assert!(retiring.is_empty(), "{retiring:?}");
        // Open, but nothing may be sent on it before its peer is heard from.
        assert!(pool.opened(KEY, idle, 0).is_empty());
        let actions = match news {
            "opened" => pool.opened(KEY, serving, 1),
            "settled" => {
                assert!(pool.opened(KEY, serving, 0).is_empty());
                pool.settled(KEY, serving, 1)
            }
            _ => {
                assert!(pool.opened(KEY, serving, 0).is_empty());
                pool.peer_limit(KEY, serving, 1)
            }
        };
        let [
            Action::Grant(granted, on),
            Action::Close(closed),
            Action::Dial(OTHER, _),
        ] = actions[..]
        else {
            panic!("{news}: {actions:?}")
        };
        assert_eq!((granted, on, closed), (served, serving, idle), "{news}");
    }
}

/// A retired destination whose last waiting request gives up winds down then, as when its
/// last is served: what has nothing on it is closed, and the place it held under the
/// worker's bound goes to a request another destination has waiting, rather than being
/// held until the idle sweep.
#[test]
fn a_retired_destination_whose_last_waiter_gives_up_winds_down() {
    const OTHER: u64 = KEY + 1;
    let mut pool = Harness::new(Limits {
        streams: 1,
        connections_total: 3,
        ..limits()
    });
    let busy = pool.first_connection(KEY, 1);
    let (Taken::Waiting(gives_up), actions) = pool.take(KEY) else {
        panic!()
    };
    let [Action::Dial(KEY, idle)] = actions[..] else {
        panic!("{actions:?}")
    };
    // Open, but nothing may be sent on it before its peer is heard from.
    assert!(pool.opened(KEY, idle, 0).is_empty());
    pool.first_connection(OTHER, 1);
    // At the worker's bound: this one waits with nothing dialled for it.
    let (Taken::Waiting(_), held) = pool.take(OTHER) else {
        panic!()
    };
    assert!(held.is_empty(), "{held:?}");
    let mut retiring = Vec::new();
    pool.pool.retire(KEY, pool.now, &mut retiring);
    assert!(retiring.is_empty(), "{retiring:?}");
    let actions = pool.cancel(KEY, gives_up);
    let [Action::Close(closed), Action::Dial(OTHER, _)] = actions[..] else {
        panic!("{actions:?}")
    };
    assert_eq!(closed, idle);
    // The busy one takes no more, and closes when its stream ends.
    assert_eq!(pool.ended(KEY, busy), [Action::Close(busy)]);
}

// ---------------------------------------------------------------------------------------
// Random runs: whatever happens, in whatever order, the pool keeps its promises.

#[derive(Debug, Clone)]
enum Event {
    Take(u64),
    Open(usize, u32),
    Fail(usize),
    End(usize),
    PeerLimit(usize, u32),
    GoAway(usize),
    Close(usize),
    Cancel(usize),
    Sweep(u64),
    Retire(u64),
    Settle(usize, u32),
}

fn event() -> impl Strategy<Value = Event> {
    prop_oneof![
        4 => (0..2_u64).prop_map(Event::Take),
        2 => (any::<usize>(), 0..4_u32).prop_map(|(at, peer)| Event::Open(at, peer)),
        1 => any::<usize>().prop_map(Event::Fail),
        4 => any::<usize>().prop_map(Event::End),
        1 => (any::<usize>(), 0..4_u32).prop_map(|(at, peer)| Event::PeerLimit(at, peer)),
        1 => any::<usize>().prop_map(Event::GoAway),
        1 => any::<usize>().prop_map(Event::Close),
        1 => any::<usize>().prop_map(Event::Cancel),
        1 => (0..120_u64).prop_map(Event::Sweep),
        1 => (0..2_u64).prop_map(Event::Retire),
        2 => (any::<usize>(), 0..4_u32).prop_map(|(at, peer)| Event::Settle(at, peer)),
    ]
}

/// What the driver would know: which connections exist and what is on them, who waits.
#[derive(Debug, Default)]
struct World {
    /// Connections being opened, by key.
    dialling: Vec<(u64, ConnectionId)>,
    /// Open connections: their key, the peer's limit as last said, streams on them.
    open: BTreeMap<ConnectionId, (u64, u32, u32)>,
    /// Waiting requests, in the order they queued, by key.
    waiting: BTreeMap<u64, Vec<WaiterId>>,
    /// Every waiter ever granted, failed or cancelled.
    settled: BTreeSet<WaiterId>,
}

impl World {
    fn apply(&mut self, actions: &[Action], limits: &Limits) -> Result<(), TestCaseError> {
        for action in actions {
            match *action {
                Action::Dial(key, id) => self.dialling.push((key, id)),
                Action::Grant(waiter, id) => {
                    prop_assert!(self.settled.insert(waiter), "{waiter:?} settled twice");
                    let (key, peer, streams) = self.open.get_mut(&id).ok_or_else(|| {
                        TestCaseError::fail(format!("granted on {id:?}, which is not open"))
                    })?;
                    let queue = self.waiting.entry(*key).or_default();
                    // In order: the one granted is the first still waiting.
                    prop_assert_eq!(queue.first(), Some(&waiter), "granted out of order");
                    queue.remove(0);
                    prop_assert!(*streams < limits.streams.min(*peer), "granted past a limit");
                    *streams += 1;
                }
                Action::Fail(waiter, _) => {
                    prop_assert!(self.settled.insert(waiter), "{waiter:?} settled twice");
                    for queue in self.waiting.values_mut() {
                        queue.retain(|w| *w != waiter);
                    }
                }
                Action::Close(id) => {
                    if let Some((_, _, streams)) = self.open.remove(&id) {
                        prop_assert_eq!(streams, 0, "closed with streams on it");
                    }
                }
            }
        }
        Ok(())
    }
}

/// What must hold of the pool between any two events.
fn invariants(pool: &Pool, world: &World) -> Result<(), TestCaseError> {
    let limits = pool.limits;
    let counted: usize = pool
        .destinations
        .values()
        .map(|destination| destination.connections.len())
        .sum();
    prop_assert_eq!(counted, pool.total);
    prop_assert!(pool.total <= limits.connections_total);
    for destination in pool.destinations.values() {
        prop_assert!(destination.connections.len() <= limits.connections);
        prop_assert!(destination.dialing() <= limits.dialing);
        prop_assert!(destination.waiting.len() <= limits.waiting);
        // Nobody waits with nothing that could ever serve them, while the bounds would
        // allow a connection to be opened for them.
        let could_dial = destination.connections.len() < limits.connections
            && pool.total < limits.connections_total;
        if !destination.waiting.is_empty() && could_dial {
            prop_assert!(
                destination
                    .connections
                    .iter()
                    .any(|connection| connection.state != State::Draining),
                "a request waits for nothing"
            );
        }
        // Nobody is left waiting while a connection has a place for them.
        if !destination.waiting.is_empty() {
            prop_assert!(
                !destination
                    .connections
                    .iter()
                    .any(|connection| connection.has_room(&limits)),
                "a request waits beside a free place"
            );
        }
        for connection in &destination.connections {
            if let Some((_, _, streams)) = world.open.get(&connection.id) {
                prop_assert_eq!(connection.active, *streams);
            }
        }
        prop_assert!(
            destination
                .waiting
                .iter()
                .all(|w| !world.settled.contains(w)),
            "a settled request still waits"
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn whatever_happens_the_pool_keeps_its_promises(
        events in prop::collection::vec(event(), 1..120),
        streams in 1..4_u32,
        connections in 1..4_usize,
        waiting in 1..6_usize,
        dialing in 1..3_usize,
        requests in 1..6_u64,
    ) {
        let limits = Limits {
            streams,
            connections,
            connections_total: 5,
            waiting,
            dialing,
            requests,
            age: Duration::from_secs(90),
            idle: Duration::from_secs(60),
        };
        let mut pool = Pool::new(limits);
        let mut world = World::default();
        let mut now = Instant::now();
        let pick = |at: usize, of: usize| at.checked_rem(of);
        for event in events {
            let mut actions = Vec::new();
            match event {
                Event::Take(key) => match pool.take(key, now, &mut actions) {
                    Taken::Place(id) => {
                        let queue = world.waiting.entry(key).or_default();
                        prop_assert!(queue.is_empty(), "placed past a waiting request");
                        let (_, peer, streams) = world
                            .open
                            .get_mut(&id)
                            .ok_or_else(|| TestCaseError::fail("placed on a closed connection"))?;
                        prop_assert!(*streams < limits.streams.min(*peer));
                        *streams += 1;
                    }
                    Taken::Waiting(waiter) => world.waiting.entry(key).or_default().push(waiter),
                    Taken::Refused => {}
                },
                Event::Open(at, peer) => {
                    if let Some(at) = pick(at, world.dialling.len()) {
                        let (key, id) = world.dialling.remove(at);
                        // What may be sent before the peer's SETTINGS: one, or none.
                        let peer = peer.min(1);
                        world.open.insert(id, (key, peer, 0));
                        pool.opened(key, id, peer, now, &mut actions);
                    }
                }
                Event::Fail(at) => {
                    if let Some(at) = pick(at, world.dialling.len()) {
                        let (key, id) = world.dialling.remove(at);
                        pool.failed(key, id, Failure::Unreachable, now, &mut actions);
                    }
                }
                Event::End(at) => {
                    let busy: Vec<ConnectionId> = world
                        .open
                        .iter()
                        .filter(|(_, (_, _, streams))| *streams > 0)
                        .map(|(id, _)| *id)
                        .collect();
                    if let Some(at) = pick(at, busy.len()) {
                        let id = busy[at];
                        let (key, _, streams) = world.open.get_mut(&id).unwrap();
                        *streams -= 1;
                        let key = *key;
                        pool.ended(key, id, now, &mut actions);
                    }
                }
                Event::PeerLimit(at, peer) => {
                    let ids: Vec<ConnectionId> = world.open.keys().copied().collect();
                    if let Some(at) = pick(at, ids.len()) {
                        let entry = world.open.get_mut(&ids[at]).unwrap();
                        entry.1 = peer;
                        let key = entry.0;
                        pool.peer_limit(key, ids[at], peer, now, &mut actions);
                    }
                }
                Event::GoAway(at) => {
                    let ids: Vec<ConnectionId> = world.open.keys().copied().collect();
                    if let Some(at) = pick(at, ids.len()) {
                        let key = world.open[&ids[at]].0;
                        pool.going_away(key, ids[at], now, &mut actions);
                    }
                }
                Event::Close(at) => {
                    let ids: Vec<ConnectionId> = world.open.keys().copied().collect();
                    if let Some(at) = pick(at, ids.len()) {
                        // Its driver is gone, and its streams with it.
                        let (key, _, _) = world.open.remove(&ids[at]).unwrap();
                        pool.closed(key, ids[at], now, &mut actions);
                    }
                }
                Event::Cancel(at) => {
                    let all: Vec<(u64, WaiterId)> = world
                        .waiting
                        .iter()
                        .flat_map(|(key, queue)| queue.iter().map(|w| (*key, *w)))
                        .collect();
                    if let Some(at) = pick(at, all.len()) {
                        let (key, waiter) = all[at];
                        pool.cancel(key, waiter, now, &mut actions);
                        world.settled.insert(waiter);
                        for queue in world.waiting.values_mut() {
                            queue.retain(|w| *w != waiter);
                        }
                    }
                }
                Event::Sweep(seconds) => {
                    now += Duration::from_secs(seconds);
                    pool.sweep(now, &mut actions);
                }
                Event::Retire(key) => pool.retire(key, now, &mut actions),
                Event::Settle(at, peer) => {
                    let ids: Vec<ConnectionId> = world.open.keys().copied().collect();
                    if let Some(at) = pick(at, ids.len()) {
                        let entry = world.open.get_mut(&ids[at]).unwrap();
                        entry.1 = peer;
                        let key = entry.0;
                        pool.settled(key, ids[at], peer, now, &mut actions);
                    }
                }
            }
            world.apply(&actions, &limits)?;
            invariants(&pool, &world)?;
        }
    }
}
