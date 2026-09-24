//! The connections a worker keeps to the upstreams it has spoken to.
//!
//! One pool per worker, and it never leaves that worker: a connection is a thing with a
//! state, and a state shared between threads is a state two requests can be half way
//! through at once. Nothing here parses HTTP or chooses a backend; it holds sockets and
//! says whether one may be handed out ([13 §1](../../../docs/13-http1-upstream.md)).
//!
//! **A connection is taken out, not borrowed.** What [`Pool::take`] hands over is gone
//! from the pool, so there is no arrangement under which two exchanges could be given the
//! same socket. Putting one back is something a caller does on purpose and only when it
//! has earned it; a socket that is simply dropped is a connection closed, which is what
//! should become of one whose state nobody knows.

use super::H1Limits;
use crate::upstream::destination::ReuseIdentity;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use tokio::time::Instant;

/// A connection nobody is using, and what is known about it.
#[derive(Debug)]
struct Idle<S> {
    socket: S,
    /// The destination it was opened to. Held here so that a sweep can see for itself
    /// that the destination is gone, rather than being told.
    identity: Arc<ReuseIdentity>,
    /// When it was opened, which bounds how long it may go on being reused however busy
    /// it is: an old connection has had more chances to have quietly stopped working.
    opened: Instant,
    /// When it was last put back.
    since: Instant,
}

/// The idle connections of one worker, by destination.
///
/// Filed under the destination's key and nothing else — not an address, not a position in
/// a config ([`ReuseIdentity`]).
#[derive(Debug)]
pub struct Pool<S> {
    idle: HashMap<u64, Vec<Idle<S>>, BuildHasherDefault<Keyed>>,
    /// Kept as a number so the whole-worker bound costs no walking.
    total: usize,
}

impl<S> Default for Pool<S> {
    fn default() -> Self {
        Self {
            idle: HashMap::default(),
            total: 0,
        }
    }
}

impl<S> Pool<S> {
    /// A connection for `identity` that is fit to be used again, and when it was opened.
    ///
    /// The time it was opened travels with it, because a connection that started its age
    /// again on every reuse would have no age at all: one in steady use would go on being
    /// trusted for ever, which is the one case the bound is there for.
    ///
    /// Anything found to be too old or too long idle is dropped rather than handed out,
    /// and so is everything held for a destination that is gone: a config can change
    /// while a connection sits here.
    pub fn take(&mut self, identity: &ReuseIdentity, limits: &H1Limits) -> Option<(S, Instant)> {
        if identity.is_retired() {
            self.forget(identity.key());
            return None;
        }
        let now = Instant::now();
        let held = self.idle.get_mut(&identity.key())?;
        // From the back: the one put back last is the one most recently known to work.
        while let Some(connection) = held.pop() {
            self.total -= 1;
            if is_fit(&connection, now, limits) {
                if held.is_empty() {
                    self.idle.remove(&identity.key());
                }
                return Some((connection.socket, connection.opened));
            }
        }
        self.idle.remove(&identity.key());
        None
    }

    /// Puts a connection back, if there is room for it and it is still worth keeping.
    ///
    /// Gives it back when there is not: a connection the pool will not hold is one the
    /// caller must close, and saying so is how it comes to be closed rather than leaked.
    pub fn put(
        &mut self,
        identity: &Arc<ReuseIdentity>,
        socket: S,
        opened: Instant,
        limits: &H1Limits,
    ) -> Option<S> {
        let now = Instant::now();
        if identity.is_retired() || now.saturating_duration_since(opened) >= limits.max_age {
            return Some(socket);
        }
        if self.total >= limits.idle_total {
            return Some(socket);
        }
        let held = self.idle.entry(identity.key()).or_default();
        if held.len() >= limits.idle_per_destination {
            return Some(socket);
        }
        held.push(Idle {
            socket,
            identity: Arc::clone(identity),
            opened,
            since: now,
        });
        self.total += 1;
        None
    }

    /// Drops what is no longer worth keeping: connections idle too long, connections too
    /// old, and everything held for a destination that a config no longer has.
    ///
    /// One sweep for the whole worker rather than a timer per connection, and it is what
    /// clears out a destination that stopped receiving traffic the moment it went.
    pub fn sweep(&mut self, limits: &H1Limits) -> usize {
        let now = Instant::now();
        let before = self.total;
        self.idle.retain(|_, held| {
            // Every connection in a bucket is for the one destination, so one of them
            // answers for all of them.
            if held
                .first()
                .is_some_and(|first| first.identity.is_retired())
            {
                return false;
            }
            held.retain(|connection| is_fit(connection, now, limits));
            !held.is_empty()
        });
        self.total = self.idle.values().map(Vec::len).sum();
        before - self.total
    }

    /// How many connections are idle here.
    pub fn idle(&self) -> usize {
        self.total
    }

    /// Throws away everything held for a destination, and the bucket with it, so that the
    /// churn of many reloads cannot leave a map full of nothing.
    fn forget(&mut self, key: u64) {
        if let Some(held) = self.idle.remove(&key) {
            self.total -= held.len();
        }
    }
}

/// Hashes a destination's key, which is a number this process handed out and not anything a
/// client or a config chose, so no key can be picked to collide with others and a keyed
/// hash would buy nothing for what it costs on every take and put. One multiplication by
/// the golden ratio spreads keys that are neighbours over the high bits as well as the low
/// ones, which the map sorts by too.
#[derive(Debug, Default)]
struct Keyed(u64);

impl Hasher for Keyed {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write_u64(&mut self, key: u64) {
        self.0 = (self.0 ^ key).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    // Only keys are hashed here, which come as a `u64`; anything else still hashes.
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.write_u64(u64::from(byte));
        }
    }
}

/// A connection taken out of a pool, or newly opened, for the length of one exchange.
///
/// While this holds it the pool does not, so no arrangement gives two exchanges the same
/// socket. Dropping it closes the connection: a lease let go of rather than finished is
/// one whose connection is in a state nobody knows, and the only way back is
/// [`Lease::keep`], which wants the proof that an exchange finished.
///
/// The handle on the pool is a weak one, so a lease outliving the worker it came from
/// does not keep that worker's pool alive; it simply has nowhere to put its connection
/// back, and closes it.
#[derive(Debug)]
pub struct Lease<S> {
    #[cfg_attr(
        not(any(test, feature = "fuzzing")),
        expect(
            dead_code,
            reason = "a lease holds a socket only in the pool's own tests; one in use holds none"
        )
    )]
    socket: Option<S>,
    identity: Arc<ReuseIdentity>,
    opened: Instant,
    pool: Weak<RefCell<Pool<S>>>,
}

impl<S> Lease<S> {
    /// A lease on a connection that is in use elsewhere — inside an exchange, or inside
    /// the body of the answer it is reading. It holds no connection itself; what it holds
    /// is where the one being used came from and where it may go back to.
    pub fn in_use(
        identity: Arc<ReuseIdentity>,
        opened: Instant,
        pool: &Rc<RefCell<Pool<S>>>,
    ) -> Self {
        Self {
            socket: None,
            identity,
            opened,
            pool: Rc::downgrade(pool),
        }
    }

    /// A lease on `socket`, which was opened to `identity` at `opened`.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn new(
        socket: S,
        identity: Arc<ReuseIdentity>,
        opened: Instant,
        pool: &Rc<RefCell<Pool<S>>>,
    ) -> Self {
        Self {
            socket: Some(socket),
            identity,
            opened,
            pool: Rc::downgrade(pool),
        }
    }

    /// The connection, taken out for the length of an exchange. The lease stays behind
    /// to say where it came from and where it may go back to; whatever becomes of it in
    /// the meantime, only [`Lease::keep`] puts one back.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn take_socket(&mut self) -> Option<S> {
        self.socket.take()
    }

    /// Puts a connection back, given the proof that an exchange finished with nothing
    /// owing. The pool may still refuse it — for being one too many, or too old — and
    /// then it is closed here, which is what refusing it means.
    pub fn keep(self, socket: S, limits: &H1Limits) {
        let Some(pool) = self.pool.upgrade() else {
            // The worker has gone. There is nowhere to put this, and holding it would
            // only keep a socket open that nobody will ever come for.
            return;
        };
        let refused = pool
            .borrow_mut()
            .put(&self.identity, socket, self.opened, limits);
        drop(refused);
    }
}

/// Whether a connection is still worth handing out: not too long idle, and not so old
/// that it has had more chances to have quietly stopped working than is worth the risk.
fn is_fit<S>(connection: &Idle<S>, now: Instant, limits: &H1Limits) -> bool {
    now.saturating_duration_since(connection.since) < limits.idle_timeout
        && now.saturating_duration_since(connection.opened) < limits.max_age
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::destination::{Destinations, Keys};
    use edgerush_config::{Config, compile};
    use std::time::Duration;

    /// Destinations for the named upstreams, each with the addresses given.
    fn destinations(upstreams: &[(&str, &[&str])], keys: &Keys) -> Destinations {
        let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
        for (name, addresses) in upstreams {
            let listed: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
            yaml += &format!("  {name}: {{ endpoints: [{}] }}\n", listed.join(", "));
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        Destinations::reconcile(&compile(&config).unwrap(), &Destinations::default(), keys)
    }

    /// One destination, for the tests that only need one.
    fn one(keys: &Keys) -> (Destinations, Arc<ReuseIdentity>) {
        let destinations = destinations(&[("web", &["127.0.0.1:1"])], keys);
        let identity = Arc::clone(destinations.at(0, 0).unwrap());
        (destinations, identity)
    }

    /// Keys handed out one after another land apart in both the bits the map files by and
    /// the ones it tells entries apart with, so neighbours neither share buckets nor look
    /// alike to its probe.
    #[test]
    fn neighbouring_keys_hash_apart() {
        let hashes: Vec<u64> = (0..128u64)
            .map(|key| {
                let mut hasher = Keyed::default();
                hasher.write_u64(key);
                hasher.finish()
            })
            .collect();
        let low: std::collections::HashSet<u64> = hashes.iter().map(|hash| hash & 127).collect();
        let high: std::collections::HashSet<u64> = hashes.iter().map(|hash| hash >> 57).collect();
        assert_eq!(low.len(), 128);
        assert!(high.len() >= 100, "{}", high.len());
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_put_back_is_the_one_taken_out() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        assert!(pool.put(&identity, 7, Instant::now(), &limits).is_none());
        assert_eq!(pool.idle(), 1);
        assert_eq!(pool.take(&identity, &limits).map(|(s, _)| s), Some(7));
        assert_eq!(pool.idle(), 0, "it was left in as well as handed out");
    }

    /// Taken out and not borrowed: what is handed out is gone from the pool, so there is
    /// no arrangement under which a second exchange could be given the same socket.
    #[tokio::test(start_paused = true)]
    async fn a_connection_handed_out_is_not_handed_out_again() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        pool.put(&identity, 1, Instant::now(), &limits);
        assert_eq!(pool.take(&identity, &limits).map(|(s, _)| s), Some(1));
        assert!(pool.take(&identity, &limits).is_none());
    }

    /// The destination is the key, and nothing else is. Two upstreams at one address do
    /// not share what either of them opened.
    #[tokio::test(start_paused = true)]
    async fn connections_are_not_shared_between_destinations() {
        let keys = Keys::default();
        let held = destinations(
            &[("one", &["127.0.0.1:1"]), ("two", &["127.0.0.1:1"])],
            &keys,
        );
        let (first, second) = (
            Arc::clone(held.at(0, 0).unwrap()),
            Arc::clone(held.at(1, 0).unwrap()),
        );
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        pool.put(&first, 1, Instant::now(), &limits);
        assert_eq!(first.address(), second.address());
        assert!(pool.take(&second, &limits).is_none(), "one's went to two");
        assert_eq!(pool.take(&first, &limits).map(|(s, _)| s), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn no_more_are_kept_for_one_destination_than_are_allowed() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits {
            idle_per_destination: 2,
            ..H1Limits::default()
        };
        let mut pool = Pool::default();

        assert!(pool.put(&identity, 1, Instant::now(), &limits).is_none());
        assert!(pool.put(&identity, 2, Instant::now(), &limits).is_none());
        // The third is handed straight back, for the caller to close.
        assert_eq!(pool.put(&identity, 3, Instant::now(), &limits), Some(3));
        assert_eq!(pool.idle(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn no_more_are_kept_by_one_worker_than_are_allowed() {
        let keys = Keys::default();
        let held = destinations(&[("a", &["127.0.0.1:1"]), ("b", &["127.0.0.1:2"])], &keys);
        let limits = H1Limits {
            idle_total: 2,
            ..H1Limits::default()
        };
        let mut pool = Pool::default();
        let (first, second) = (
            Arc::clone(held.at(0, 0).unwrap()),
            Arc::clone(held.at(1, 0).unwrap()),
        );

        pool.put(&first, 1, Instant::now(), &limits);
        pool.put(&second, 2, Instant::now(), &limits);
        assert_eq!(pool.put(&second, 3, Instant::now(), &limits), Some(3));
        assert_eq!(pool.idle(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn one_that_has_been_idle_too_long_is_not_handed_out() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        pool.put(&identity, 1, Instant::now(), &limits);
        tokio::time::sleep(limits.idle_timeout * 2).await;
        assert!(pool.take(&identity, &limits).is_none());
        assert_eq!(
            pool.idle(),
            0,
            "it was dropped rather than left to be found again"
        );
    }

    /// Age bounds a connection however busy it is: one in constant use is still a
    /// connection that has had a long time to have quietly stopped working.
    #[tokio::test(start_paused = true)]
    async fn one_that_is_too_old_is_not_handed_out_however_lately_it_was_used() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();
        let opened = Instant::now();

        tokio::time::sleep(limits.max_age * 2).await;
        // Put back this very moment, and still too old to be kept.
        assert_eq!(pool.put(&identity, 1, opened, &limits), Some(1));
        assert_eq!(pool.idle(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_kept_for_a_destination_that_is_gone() {
        let keys = Keys::default();
        let (held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        pool.put(&identity, 1, Instant::now(), &limits);
        // A config without it.
        let _after = Destinations::reconcile(
            &compile(
                &serde_saphyr::from_str::<Config>("listeners: {}\nroutes: []\nupstreams: {}")
                    .unwrap(),
            )
            .unwrap(),
            &held,
            &keys,
        );
        assert!(identity.is_retired());

        assert!(pool.take(&identity, &limits).is_none());
        assert_eq!(pool.idle(), 0, "a retired destination kept its connections");
        assert_eq!(pool.put(&identity, 2, Instant::now(), &limits), Some(2));
    }

    /// A destination that stops receiving traffic has nobody to notice it went, so the
    /// sweep is what clears it out. The destination is really retired here, by a config
    /// that no longer has it, rather than the test saying so.
    #[tokio::test(start_paused = true)]
    async fn the_sweep_clears_out_what_nobody_will_come_back_for() {
        let keys = Keys::default();
        let held = destinations(&[("a", &["127.0.0.1:1"]), ("b", &["127.0.0.1:2"])], &keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();
        let (staying, going) = (
            Arc::clone(held.at(0, 0).unwrap()),
            Arc::clone(held.at(1, 0).unwrap()),
        );
        pool.put(&staying, 1, Instant::now(), &limits);
        pool.put(&going, 2, Instant::now(), &limits);

        // A config with only `a` in it, which retires `b`.
        let _after = Destinations::reconcile(
            &compile(
                &serde_saphyr::from_str::<Config>(
                    "listeners: {}\nroutes: []\nupstreams:\n  a: { endpoints: [\"127.0.0.1:1\"] }\n",
                )
                .unwrap(),
            )
            .unwrap(),
            &held,
            &keys,
        );
        assert!(going.is_retired());
        assert!(!staying.is_retired());

        assert_eq!(pool.sweep(&limits), 1);
        assert_eq!(pool.idle(), 1);
        assert_eq!(pool.take(&staying, &limits).map(|(s, _)| s), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn the_sweep_clears_out_what_has_gone_stale() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        pool.put(&identity, 1, Instant::now(), &limits);
        tokio::time::sleep(limits.idle_timeout * 2).await;
        assert_eq!(pool.sweep(&limits), 1);
        assert_eq!(pool.idle(), 0);
    }

    /// Reload after reload leaves no trace: a bucket with nothing in it is a bucket that
    /// goes, so churn cannot grow the map without bound.
    #[tokio::test(start_paused = true)]
    async fn churn_does_not_leave_the_map_full_of_nothing() {
        let keys = Keys::default();
        let limits = H1Limits::default();
        let mut pool = Pool::default();
        let mut previous = Destinations::default();

        for round in 0..64u16 {
            let address = format!("127.0.0.1:{}", round + 1);
            let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
            yaml += &format!("  web: {{ endpoints: [\"{address}\"] }}\n");
            let config: Config = serde_saphyr::from_str(&yaml).unwrap();
            previous = Destinations::reconcile(&compile(&config).unwrap(), &previous, &keys);

            let identity = Arc::clone(previous.at(0, 0).unwrap());
            pool.put(&identity, round, Instant::now(), &limits);
            pool.sweep(&limits);
        }
        assert_eq!(pool.idle(), 1);
        assert_eq!(
            pool.idle.len(),
            1,
            "a bucket was left behind for every reload"
        );
    }

    /// A connection keeps the time it was opened through every reuse, so one in steady
    /// use is still bounded by its age. Were the clock to start again each time it went
    /// back, a busy connection would go on being trusted for ever.
    #[tokio::test(start_paused = true)]
    async fn reuse_does_not_make_a_connection_young_again() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        // Gaps short enough that it is never idle too long, and an age it can reach.
        let limits = H1Limits {
            idle_timeout: Duration::from_secs(30),
            max_age: Duration::from_secs(100),
            ..H1Limits::default()
        };
        let mut pool = Pool::default();
        let opened = Instant::now();

        // Used over and over, never idle for long, always put straight back.
        for round in 0..4 {
            assert!(pool.put(&identity, 1, opened, &limits).is_none(), "{round}");
            tokio::time::sleep(Duration::from_secs(20)).await;
            let (socket, was_opened) = pool.take(&identity, &limits).expect("still fit");
            assert_eq!(socket, 1);
            assert_eq!(was_opened, opened, "it forgot when it was opened");
        }

        // Eighty seconds of use so far, and none of it spent idle for long. A little
        // more and it is past its age, however lately it was used.
        tokio::time::sleep(Duration::from_secs(25)).await;
        assert_eq!(pool.put(&identity, 1, opened, &limits), Some(1));
        assert_eq!(pool.idle(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_that_is_kept_puts_its_connection_back() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let pool = Rc::new(RefCell::new(Pool::default()));

        let lease = Lease::new(9, Arc::clone(&identity), Instant::now(), &pool);
        let socket = lease_socket(&lease);
        lease.keep(socket, &limits);

        assert_eq!(pool.borrow().idle(), 1);
        assert_eq!(
            pool.borrow_mut().take(&identity, &limits).map(|(s, _)| s),
            Some(9)
        );
    }

    /// A lease let go of rather than finished takes its connection with it. There is no
    /// way to put one back by accident, and none to leave one half used in the pool.
    #[tokio::test(start_paused = true)]
    async fn a_lease_that_is_dropped_keeps_nothing() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let pool = Rc::new(RefCell::new(Pool::default()));

        drop(Lease::new(9, Arc::clone(&identity), Instant::now(), &pool));

        assert_eq!(pool.borrow().idle(), 0);
        assert!(pool.borrow_mut().take(&identity, &limits).is_none());
    }

    /// A lease that outlives its worker has nowhere to put anything, and does not keep
    /// the worker's pool alive by holding on to it.
    #[tokio::test(start_paused = true)]
    async fn a_lease_whose_worker_has_gone_closes_what_it_holds() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let pool = Rc::new(RefCell::new(Pool::<i32>::default()));
        let watching = Rc::downgrade(&pool);

        let lease = Lease::new(9, Arc::clone(&identity), Instant::now(), &pool);
        drop(pool);
        assert!(
            watching.upgrade().is_none(),
            "the lease kept the pool alive"
        );

        // Nothing to put it back into, and nothing that panics for the want of one.
        lease.keep(9, &limits);
    }

    /// What the pool will not hold, the lease does not hold either: refusing a connection
    /// is how it comes to be closed rather than kept somewhere nobody looks.
    #[tokio::test(start_paused = true)]
    async fn a_connection_the_pool_refuses_is_not_kept_anywhere() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits {
            idle_per_destination: 1,
            ..H1Limits::default()
        };
        let pool = Rc::new(RefCell::new(Pool::default()));

        Lease::new(1, Arc::clone(&identity), Instant::now(), &pool).keep(1, &limits);
        Lease::new(2, Arc::clone(&identity), Instant::now(), &pool).keep(2, &limits);

        assert_eq!(pool.borrow().idle(), 1, "the second was kept as well");
    }

    /// Taking the socket out is not putting it back: a lease unwrapped for an exchange
    /// that then went wrong leaves the pool with nothing.
    #[tokio::test(start_paused = true)]
    async fn taking_a_socket_out_of_a_lease_is_not_returning_it() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let pool = Rc::new(RefCell::new(Pool::default()));

        let mut lease = Lease::new(9, Arc::clone(&identity), Instant::now(), &pool);
        assert_eq!(lease.take_socket(), Some(9));
        assert_eq!(pool.borrow().idle(), 0);
    }

    /// The socket a lease is holding, for a test that means to hand it straight back.
    fn lease_socket(lease: &Lease<i32>) -> i32 {
        lease.socket.expect("a lease that still holds one")
    }
}
