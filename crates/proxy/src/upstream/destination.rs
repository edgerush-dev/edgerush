//! Which destination a connection was opened to, and when two are the same destination.
//!
//! **A kept connection is matched against the destination itself, never against where the
//! destination happened to sit in a config.** An upstream's position is a position: the
//! compiled config orders upstreams by name, so adding one called `aaa` moves every other
//! upstream along by one. A pool keyed on those positions would then hand the connections
//! opened for one upstream to whatever had taken its place — traffic delivered to a
//! backend it was never meant for, and no error anywhere to say so
//! ([13 §3](../../../docs/13-http1-upstream.md)).
//!
//! So a destination is named by a key of its own, given out once and never again, and a
//! reload keeps that key only where the destination really is the same one.

use crate::upstream::secure::Secure;
use edgerush_config::{Compiled, Keepalive, UpstreamProtocol, UpstreamTls};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Hands out destination keys. One per process; a key it has given out is never given
/// again, so a key held past the life of what it named cannot come to name something else.
#[derive(Debug, Default)]
pub struct Keys(AtomicU64);

impl Keys {
    /// The next key, which no destination has had before.
    fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// A destination a connection may be kept for.
///
/// Two connections are interchangeable exactly when their destinations have the same key.
/// The key stands for the upstream the connection was opened for, the address it was
/// opened to, the protocol it speaks, and the TLS it was secured with, if any: whom the
/// endpoint was verified to be and by whose word.
#[derive(Debug)]
pub struct ReuseIdentity {
    key: u64,
    /// The upstream this belongs to. Two upstreams pointing at one address are two
    /// destinations: what they are for differs, whatever they currently resolve to.
    upstream: Box<str>,
    address: SocketAddr,
    protocol: UpstreamProtocol,
    /// What its connections are secured with; none is plain TCP.
    secure: Option<Arc<Secure>>,
    /// PINGs on its HTTP/2 connections, if any.
    keepalive: Option<Keepalive>,
    /// Set when a config without this destination is published. Nothing retired is ever
    /// kept or taken out again; an exchange already under way finishes as it is.
    retired: AtomicBool,
}

impl ReuseIdentity {
    /// What a pool files its connections under. Small and numeric on purpose: finding a
    /// connection costs no hashing of names and no building of addresses.
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Where to connect for it.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// What its connections speak.
    pub fn protocol(&self) -> UpstreamProtocol {
        self.protocol
    }

    /// What its connections are secured with, if anything.
    pub(crate) fn secure(&self) -> Option<&Arc<Secure>> {
        self.secure.as_ref()
    }

    /// PINGs on its HTTP/2 connections, if any.
    pub(crate) fn keepalive(&self) -> Option<Keepalive> {
        self.keepalive
    }

    /// Whether this destination is gone from the running config. Checked when a
    /// connection is put back and again when one is taken out: a config can change while
    /// a connection sits idle.
    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
}

/// Every destination of one compiled config, by the position of the upstream and then of
/// the endpoint — the same order a request finds them in.
#[derive(Debug, Default)]
pub struct Destinations(Vec<Vec<Arc<ReuseIdentity>>>);

impl Destinations {
    /// The destinations of `config`, keeping those of `previous` wherever the destination
    /// is the same one, and retiring those of `previous` that this config does not have.
    /// `secure` is what each upstream's connections are secured with, by position, for a
    /// destination that is new.
    ///
    /// Sameness is the upstream's name, the address, the protocol and the TLS, and nothing
    /// about where any of them sits: a config that says the same thing in a different order says the same thing.
    /// A destination that goes and comes back is a new one, because nothing here can tell
    /// whether what answers at that address is still what answered before.
    pub(crate) fn reconcile(
        config: &Compiled,
        previous: &Self,
        keys: &Keys,
        secure: &[Option<Arc<Secure>>],
    ) -> Self {
        type Same<'a> = (
            &'a str,
            SocketAddr,
            UpstreamProtocol,
            Option<&'a UpstreamTls>,
            Option<Keepalive>,
        );
        let mut known: HashMap<Same<'_>, &Arc<ReuseIdentity>> = previous
            .0
            .iter()
            .flatten()
            .map(|identity| {
                let tls = identity.secure.as_deref().map(Secure::source);
                let same = (
                    &*identity.upstream,
                    identity.address,
                    identity.protocol,
                    tls,
                    identity.keepalive,
                );
                (same, identity)
            })
            .collect();

        let destinations = config
            .upstreams
            .iter()
            .enumerate()
            .map(|(position, upstream)| {
                upstream
                    .endpoints
                    .iter()
                    .map(|address| {
                        // Taken out as it is used, so that whatever is left over at the
                        // end is exactly what this config no longer has.
                        known
                            .remove(&(
                                upstream.name.as_str(),
                                *address,
                                upstream.protocol,
                                upstream.tls.as_ref(),
                                upstream.keepalive,
                            ))
                            .map_or_else(
                                || {
                                    Arc::new(ReuseIdentity {
                                        key: keys.next(),
                                        upstream: upstream.name.as_str().into(),
                                        address: *address,
                                        protocol: upstream.protocol,
                                        secure: secure.get(position).cloned().flatten(),
                                        keepalive: upstream.keepalive,
                                        retired: AtomicBool::new(false),
                                    })
                                },
                                Arc::clone,
                            )
                    })
                    .collect()
            })
            .collect();

        for gone in known.into_values() {
            gone.retire();
        }
        Self(destinations)
    }

    /// The destination of the endpoint at `endpoint` of the upstream at `upstream`.
    pub fn at(&self, upstream: usize, endpoint: usize) -> Option<&Arc<ReuseIdentity>> {
        self.0.get(upstream)?.get(endpoint)
    }

    /// How many endpoints the upstream at `upstream` has.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn endpoints(&self, upstream: usize) -> usize {
        self.0.get(upstream).map_or(0, Vec::len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Config, compile};

    /// A config of the named upstreams, each with the addresses given.
    fn config(upstreams: &[(&str, &[&str])]) -> Compiled {
        let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
        for (name, addresses) in upstreams {
            let listed: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
            yaml += &format!("  {name}: {{ endpoints: [{}] }}\n", listed.join(", "));
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
    }

    /// The keys of every destination, in order.
    fn keys_of(destinations: &Destinations) -> Vec<u64> {
        destinations.0.iter().flatten().map(|d| d.key()).collect()
    }

    #[test]
    fn a_destination_that_has_not_changed_keeps_its_key() {
        let keys = Keys::default();
        let before = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let after =
            Destinations::reconcile(&config(&[("web", &["127.0.0.1:1"])]), &before, &keys, &[]);
        assert_eq!(keys_of(&before), keys_of(&after));
        assert!(!before.at(0, 0).unwrap().is_retired());
    }

    /// **The invariant.** Upstreams are compiled in the order of their names, so adding
    /// one called `aaa` moves every other upstream along. Keyed on those positions, the
    /// connections opened for `web` would be handed to `aaa` — a request answered by a
    /// backend it was never routed to, and nothing anywhere to say so.
    #[test]
    fn a_reload_that_moves_an_upstream_does_not_move_its_connections() {
        let keys = Keys::default();
        let before = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"]), ("zed", &["127.0.0.1:2"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let web = before.at(0, 0).unwrap().key();
        let zed = before.at(1, 0).unwrap().key();

        // `aaa` sorts first, so every position shifts by one.
        let after = Destinations::reconcile(
            &config(&[
                ("aaa", &["127.0.0.1:3"]),
                ("web", &["127.0.0.1:1"]),
                ("zed", &["127.0.0.1:2"]),
            ]),
            &before,
            &keys,
            &[],
        );
        assert_eq!(after.at(1, 0).unwrap().key(), web, "web changed hands");
        assert_eq!(after.at(2, 0).unwrap().key(), zed, "zed changed hands");
        // And the newcomer, which is at web's old position, is nobody's old destination.
        let newcomer = after.at(0, 0).unwrap().key();
        assert_ne!(newcomer, web);
        assert_ne!(newcomer, zed);
    }

    /// Two upstreams at one address are two destinations. They are for different things,
    /// whatever they currently point at, and one's connections are not the other's.
    #[test]
    fn two_upstreams_at_one_address_do_not_share_connections() {
        let keys = Keys::default();
        let destinations = Destinations::reconcile(
            &config(&[("one", &["127.0.0.1:1"]), ("two", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let (one, two) = (
            destinations.at(0, 0).unwrap(),
            destinations.at(1, 0).unwrap(),
        );
        assert_eq!(one.address(), two.address());
        assert_ne!(one.key(), two.key());
    }

    #[test]
    fn a_destination_a_config_no_longer_has_is_retired() {
        let keys = Keys::default();
        let before = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1", "127.0.0.1:2"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let kept = Arc::clone(before.at(0, 0).unwrap());
        let dropped = Arc::clone(before.at(0, 1).unwrap());

        let after =
            Destinations::reconcile(&config(&[("web", &["127.0.0.1:1"])]), &before, &keys, &[]);
        assert!(!kept.is_retired());
        assert!(
            dropped.is_retired(),
            "an endpoint that is gone was left live"
        );
        assert_eq!(after.at(0, 0).unwrap().key(), kept.key());
        assert_eq!(after.endpoints(0), 1);
    }

    #[test]
    fn an_upstream_that_is_gone_altogether_is_retired() {
        let keys = Keys::default();
        let before = Destinations::reconcile(
            &config(&[("gone", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let gone = Arc::clone(before.at(0, 0).unwrap());
        let _after = Destinations::reconcile(&config(&[]), &before, &keys, &[]);
        assert!(gone.is_retired());
    }

    /// A destination that goes and comes back is a new one. Nothing here can tell whether
    /// what answers at that address is what answered before, so a connection opened to
    /// the old one is not lent to the new.
    #[test]
    fn a_destination_that_returns_is_not_the_one_that_left() {
        let keys = Keys::default();
        let first = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let was = Arc::clone(first.at(0, 0).unwrap());

        let without = Destinations::reconcile(&config(&[]), &first, &keys, &[]);
        assert!(was.is_retired());

        let again =
            Destinations::reconcile(&config(&[("web", &["127.0.0.1:1"])]), &without, &keys, &[]);
        assert_ne!(again.at(0, 0).unwrap().key(), was.key());
        assert!(!again.at(0, 0).unwrap().is_retired());
    }

    #[test]
    fn an_endpoint_that_moves_is_a_different_destination() {
        let keys = Keys::default();
        let before = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let was = Arc::clone(before.at(0, 0).unwrap());
        let after =
            Destinations::reconcile(&config(&[("web", &["127.0.0.1:2"])]), &before, &keys, &[]);
        assert_ne!(after.at(0, 0).unwrap().key(), was.key());
        assert!(was.is_retired());
    }

    /// An upstream that changes the protocol it is spoken to in is a different destination:
    /// a connection opened to speak HTTP/1.1 is never lent to a request that is to go by
    /// HTTP/2, nor the other way round.
    #[test]
    fn an_upstream_that_changes_protocol_is_a_different_destination() {
        let keys = Keys::default();
        let before = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let was = Arc::clone(before.at(0, 0).unwrap());
        assert_eq!(was.protocol(), UpstreamProtocol::Http1);
        let mut http2 = config(&[("web", &["127.0.0.1:1"])]);
        http2.upstreams[0].protocol = UpstreamProtocol::Http2;
        let after = Destinations::reconcile(&http2, &before, &keys, &[]);
        let now = after.at(0, 0).unwrap();
        assert_ne!(now.key(), was.key());
        assert_eq!(now.protocol(), UpstreamProtocol::Http2);
        assert!(was.is_retired());
    }

    /// Keys are never given out twice, however many configs come and go, so one held past
    /// the life of what it named can never come to name something else.
    #[test]
    fn a_key_is_never_given_out_twice() {
        let keys = Keys::default();
        let mut seen = Vec::new();
        let mut previous = Destinations::default();
        for round in 0..8 {
            let address = format!("127.0.0.1:{}", round + 1);
            let config = config(&[("web", &[&address])]);
            previous = Destinations::reconcile(&config, &previous, &keys, &[]);
            seen.extend(keys_of(&previous));
        }
        let mut once = seen.clone();
        once.sort_unstable();
        once.dedup();
        assert_eq!(once.len(), seen.len(), "a key came round again: {seen:?}");
    }
}
