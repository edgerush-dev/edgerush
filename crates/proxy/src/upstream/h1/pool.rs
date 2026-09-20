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
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Instant;

/// A connection nobody is using, and what is known about it.
#[derive(Debug)]
struct Idle<S> {
    socket: S,
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
    idle: HashMap<u64, Vec<Idle<S>>>,
    /// Kept as a number so the whole-worker bound costs no walking.
    total: usize,
}

impl<S> Default for Pool<S> {
    fn default() -> Self {
        Self {
            idle: HashMap::new(),
            total: 0,
        }
    }
}

impl<S> Pool<S> {
    /// A connection for `identity` that is fit to be used again, if there is one.
    ///
    /// Anything found to be too old or too long idle is dropped rather than handed out,
    /// and so is everything held for a destination that is gone: a config can change
    /// while a connection sits here.
    pub fn take(&mut self, identity: &ReuseIdentity, limits: &H1Limits) -> Option<S> {
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
                let socket = connection.socket;
                if held.is_empty() {
                    self.idle.remove(&identity.key());
                }
                return Some(socket);
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
    pub fn sweep(&mut self, retired: impl Fn(u64) -> bool, limits: &H1Limits) -> usize {
        let now = Instant::now();
        let before = self.total;
        self.idle.retain(|key, held| {
            if retired(*key) {
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

    /// How many are idle for one destination.
    pub fn idle_for(&self, identity: &ReuseIdentity) -> usize {
        self.idle.get(&identity.key()).map_or(0, Vec::len)
    }

    /// Throws away everything held for a destination, and the bucket with it, so that the
    /// churn of many reloads cannot leave a map full of nothing.
    fn forget(&mut self, key: u64) {
        if let Some(held) = self.idle.remove(&key) {
            self.total -= held.len();
        }
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

    #[tokio::test(start_paused = true)]
    async fn a_connection_put_back_is_the_one_taken_out() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        assert!(pool.put(&identity, 7, Instant::now(), &limits).is_none());
        assert_eq!(pool.idle(), 1);
        assert_eq!(pool.take(&identity, &limits), Some(7));
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
        assert_eq!(pool.take(&identity, &limits), Some(1));
        assert_eq!(pool.take(&identity, &limits), None);
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
        assert_eq!(pool.take(&second, &limits), None, "one's went to two");
        assert_eq!(pool.take(&first, &limits), Some(1));
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
        assert_eq!(pool.take(&identity, &limits), None);
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

        assert_eq!(pool.take(&identity, &limits), None);
        assert_eq!(pool.idle(), 0, "a retired destination kept its connections");
        assert_eq!(pool.put(&identity, 2, Instant::now(), &limits), Some(2));
    }

    /// A destination that stops receiving traffic has nobody to notice it went, so the
    /// sweep is what clears it out.
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

        let gone = going.key();
        assert_eq!(pool.sweep(|key| key == gone, &limits), 1);
        assert_eq!(pool.idle(), 1);
        assert_eq!(pool.take(&staying, &limits), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn the_sweep_clears_out_what_has_gone_stale() {
        let keys = Keys::default();
        let (_held, identity) = one(&keys);
        let limits = H1Limits::default();
        let mut pool = Pool::default();

        pool.put(&identity, 1, Instant::now(), &limits);
        tokio::time::sleep(limits.idle_timeout * 2).await;
        assert_eq!(pool.sweep(|_| false, &limits), 1);
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
            pool.sweep(|key| key != identity.key(), &limits);
        }
        assert_eq!(pool.idle(), 1);
        assert_eq!(
            pool.idle.len(),
            1,
            "a bucket was left behind for every reload"
        );
    }
}
