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

use crate::balance::Share;
use crate::proxy_protocol::Version;
use crate::upstream::secure::Secure;
use edgerush_config::{
    Compiled, CompiledUpstreamTls, HealthCheck, Keepalive, ProxyProtocolVersion, UpstreamProtocol,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// The bit of [`ReuseIdentity`]'s standing that says its probes fail: the top one, which
/// milliseconds since the process started will not reach.
const UNHEALTHY: u64 = 1 << 63;

/// Milliseconds since the first time anything asked, plus one: what a ramp's start is kept
/// as, so that the health checker's thread and every worker's read one clock, and 0 can
/// mean no ramp at all.
fn now() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let since = EPOCH.get_or_init(Instant::now).elapsed().as_millis();
    u64::try_from(since).unwrap_or(u64::MAX - 1) + 1
}

/// Hands out keys: of destinations, and of what tunnels are routed by (`routed`). One per
/// process; a key it has given out is never given again, so a key held past the life of
/// what it named cannot come to name something else.
#[derive(Debug, Default)]
pub struct Keys(AtomicU64);

impl Keys {
    /// The next key, which nothing has had before.
    pub(crate) fn next(&self) -> u64 {
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
    /// How its endpoint is probed, if it is.
    health_check: Option<HealthCheck>,
    /// The PROXY protocol header its tunnels, and its HTTP probes, send first, if any
    /// (20 §4).
    proxy_protocol: Option<Version>,
    /// Whether it may be picked, in one word so that a pick reads it in one look: 0 when it
    /// may. [`UNHEALTHY`] when its last probes say it does not serve, set by the health
    /// checker — healthy until a probe says otherwise, as HAProxy and Pingora start a
    /// server. Below that, when it was set aside, as [`now`] tells it, because a try could
    /// not connect to it (03 §6): set by the worker whose try it was, and cleared by the
    /// checker once a connect probe gets through.
    standing: AtomicU64,
    /// Set when a config without this destination is published. Nothing retired is ever
    /// kept or taken out again; an exchange already under way finishes as it is.
    retired: AtomicBool,
    /// When its slow start began, as [`now`] tells it; 0 when it is not ramping. Set when
    /// it is added beside an endpoint its upstream keeps, when it passes its checks again
    /// after failing them, and when a connect probe brings it back from being set aside
    /// (03 §6); cleared by the first pick after its ramp is over,
    /// so that an endpoint done ramping reads no clock.
    ramping_since: AtomicU64,
}

impl ReuseIdentity {
    /// What a pool files its connections under. Small and numeric on purpose: finding a
    /// connection costs no hashing of names and no building of addresses.
    pub fn key(&self) -> u64 {
        self.key
    }

    /// The name of the upstream it belongs to.
    pub(crate) fn upstream(&self) -> &str {
        &self.upstream
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

    /// The scheme a request to it names in HTTP/2's `:scheme`: `https` over TLS, `http`
    /// otherwise. The request is the gateway's own, and says `https` only when it is
    /// secured (RFC 9110 §4.2.2); what the client used goes in `X-Forwarded-Proto`.
    pub(crate) fn scheme(&self) -> http::uri::Scheme {
        if self.secure.is_some() {
            http::uri::Scheme::HTTPS
        } else {
            http::uri::Scheme::HTTP
        }
    }

    /// PINGs on its HTTP/2 connections, if any.
    pub(crate) fn keepalive(&self) -> Option<Keepalive> {
        self.keepalive
    }

    /// How its endpoint is probed, if it is.
    pub(crate) fn proxy_protocol(&self) -> Option<Version> {
        self.proxy_protocol
    }

    pub(crate) fn health_check(&self) -> Option<&HealthCheck> {
        self.health_check.as_ref()
    }

    /// Whether it is to be picked: its probes, if any, say it serves, and it is not set
    /// aside.
    pub(crate) fn serves(&self) -> bool {
        self.standing.load(Ordering::Relaxed) == 0
    }

    /// Whether its probes, if any, say it serves.
    pub(crate) fn is_healthy(&self) -> bool {
        self.standing.load(Ordering::Relaxed) & UNHEALTHY == 0
    }

    /// What the health checker has found.
    pub(crate) fn set_healthy(&self, healthy: bool) {
        if healthy {
            self.standing.fetch_and(!UNHEALTHY, Ordering::Relaxed);
        } else {
            self.standing.fetch_or(UNHEALTHY, Ordering::Relaxed);
        }
    }

    /// Sets it aside, now: a try could not connect to it. Whether this is what set it
    /// aside; one already set aside keeps when that was.
    pub(crate) fn set_aside(&self) -> bool {
        let now = now() & !UNHEALTHY;
        self.standing
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |standing| {
                (standing & !UNHEALTHY == 0).then_some(standing | now)
            })
            .is_ok()
    }

    /// How long it has been set aside, or waited since its last connect probe failed; none
    /// when it is not set aside.
    pub(crate) fn set_aside_for(&self) -> Option<Duration> {
        let since = self.standing.load(Ordering::Relaxed) & !UNHEALTHY;
        (since != 0).then(|| Duration::from_millis(now().saturating_sub(since)))
    }

    /// Starts its wait again: a connect probe did not get through.
    pub(crate) fn wait_again(&self) {
        let now = now() & !UNHEALTHY;
        let _unless_brought_back =
            self.standing
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |standing| {
                    (standing & !UNHEALTHY != 0).then_some((standing & UNHEALTHY) | now)
                });
    }

    /// Takes it back: a connect probe got through.
    pub(crate) fn bring_back(&self) {
        self.standing.fetch_and(UNHEALTHY, Ordering::Relaxed);
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

    /// Starts its slow start now.
    pub(crate) fn start_ramp(&self) {
        self.ramping_since.store(now(), Ordering::Relaxed);
    }

    /// Its share under a slow start of `window` milliseconds, none for an upstream without
    /// one. Reads the clock only while it is ramping.
    pub(crate) fn share(&self, window: Option<u64>) -> Share {
        let Some(window) = window else {
            return Share::FULL;
        };
        let since = self.ramping_since.load(Ordering::Relaxed);
        if since == 0 {
            return Share::FULL;
        }
        let elapsed = now().saturating_sub(since);
        if elapsed >= window {
            // Over: once, whichever worker sees it first. A ramp begun again meanwhile is
            // left as it is.
            let _over =
                self.ramping_since
                    .compare_exchange(since, 0, Ordering::Relaxed, Ordering::Relaxed);
            return Share::FULL;
        }
        Share::ramped(elapsed, window)
    }

    /// Whether it is ramping, as far as anything has yet looked.
    #[cfg(test)]
    pub(crate) fn is_ramping(&self) -> bool {
        self.ramping_since.load(Ordering::Relaxed) != 0
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
            Option<&'a CompiledUpstreamTls>,
            Option<Keepalive>,
            Option<&'a HealthCheck>,
            Option<Version>,
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
                    identity.health_check.as_ref(),
                    identity.proxy_protocol,
                );
                (same, identity)
            })
            .collect();

        let destinations = config
            .upstreams()
            .iter()
            .enumerate()
            .map(|(position, upstream)| {
                let mut kept_any = false;
                let mut added = Vec::new();
                let destinations: Vec<Arc<ReuseIdentity>> = upstream
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
                                upstream.health_check.as_ref(),
                                upstream.proxy_protocol.map(version_of),
                            ))
                            .map_or_else(
                                || {
                                    let made = Arc::new(ReuseIdentity {
                                        key: keys.next(),
                                        upstream: upstream.name.as_str().into(),
                                        address: *address,
                                        protocol: upstream.protocol,
                                        secure: secure.get(position).cloned().flatten(),
                                        keepalive: upstream.keepalive,
                                        health_check: upstream.health_check.clone(),
                                        proxy_protocol: upstream.proxy_protocol.map(version_of),
                                        standing: AtomicU64::new(0),
                                        retired: AtomicBool::new(false),
                                        ramping_since: AtomicU64::new(0),
                                    });
                                    added.push(Arc::clone(&made));
                                    made
                                },
                                |kept| {
                                    kept_any = true;
                                    Arc::clone(kept)
                                },
                            )
                    })
                    .collect();
                // Slow start is for an endpoint added beside one its upstream keeps: an
                // upstream whose endpoints are all new, at start or all replaced at once,
                // ramps none, since every one would ramp alike (03 §6).
                if kept_any {
                    for endpoint in &added {
                        endpoint.start_ramp();
                    }
                }
                destinations
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

    /// The destinations of the upstream at `upstream`, by endpoint.
    pub(crate) fn of(&self, upstream: usize) -> &[Arc<ReuseIdentity>] {
        self.0.get(upstream).map_or(&[], Vec::as_slice)
    }

    /// Every destination, of every upstream.
    pub(crate) fn all(&self) -> impl Iterator<Item = &Arc<ReuseIdentity>> {
        self.0.iter().flatten()
    }

    /// How many endpoints the upstream at `upstream` has.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn endpoints(&self, upstream: usize) -> usize {
        self.0.get(upstream).map_or(0, Vec::len)
    }
}

/// The PROXY protocol version a config names, as the writers take it.
fn version_of(version: ProxyProtocolVersion) -> Version {
    match version {
        ProxyProtocolVersion::V1 => Version::V1,
        ProxyProtocolVersion::V2 => Version::V2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Config, compile};

    /// A config of the named upstreams, each with the addresses given.
    fn config(upstreams: &[(&str, &[&str])]) -> Compiled {
        compile(&model(upstreams)).unwrap()
    }

    /// The same as the model states it, for a test to change before it is compiled.
    fn model(upstreams: &[(&str, &[&str])]) -> Config {
        let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
        for (name, addresses) in upstreams {
            let listed: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
            yaml += &format!(
                "  {name}: {{ load_balancer: p2c, endpoints: [{}] }}\n",
                listed.join(", ")
            );
        }
        serde_saphyr::from_str(&yaml).unwrap()
    }

    /// Health and being set aside share a word and never overwrite each other: a pick takes
    /// only a destination that is neither unhealthy nor set aside; the checker's findings
    /// neither set it aside nor bring it back, and bringing it back leaves its health as it
    /// was (03 §6).
    #[test]
    fn health_and_being_set_aside_are_each_their_own() {
        let destinations = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &Keys::default(),
            &[],
        );
        let destination = destinations.at(0, 0).unwrap();
        assert!(destination.serves() && destination.set_aside_for().is_none());

        assert!(destination.set_aside());
        assert!(!destination.set_aside(), "one already set aside keeps when");
        assert!(!destination.serves() && destination.is_healthy());
        assert!(destination.set_aside_for().is_some());

        destination.set_healthy(false);
        destination.set_healthy(true);
        assert!(
            !destination.serves(),
            "passing its checks does not bring it back"
        );

        destination.set_healthy(false);
        destination.wait_again();
        assert!(destination.set_aside_for().is_some() && !destination.is_healthy());
        destination.bring_back();
        assert!(destination.set_aside_for().is_none());
        assert!(
            !destination.is_healthy(),
            "brought back as unhealthy as it was"
        );
        assert!(!destination.serves());

        destination.set_healthy(true);
        assert!(destination.serves());
        destination.wait_again();
        assert!(
            destination.set_aside_for().is_none(),
            "waiting again sets nothing aside"
        );
    }

    /// The keys of every destination, in order.
    fn keys_of(destinations: &Destinations) -> Vec<u64> {
        destinations.0.iter().flatten().map(|d| d.key()).collect()
    }

    /// Which endpoints of the upstream at `upstream` are ramping, by position.
    fn ramping(destinations: &Destinations, upstream: usize) -> Vec<bool> {
        destinations
            .of(upstream)
            .iter()
            .map(|destination| destination.is_ramping())
            .collect()
    }

    /// An endpoint added beside one its upstream keeps ramps; none ramps at start, nor in an
    /// upstream whose endpoints are all replaced at once, where each would ramp alike
    /// (03 §6).
    #[test]
    fn an_endpoint_added_beside_one_kept_ramps_and_no_other() {
        let keys = Keys::default();
        let first = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"]), ("new", &[])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        assert_eq!(ramping(&first, 1), [false]);
        let grown = Destinations::reconcile(
            &config(&[
                ("web", &["127.0.0.1:1", "127.0.0.1:2"]),
                ("new", &["127.0.0.1:9"]),
            ]),
            &first,
            &keys,
            &[],
        );
        assert_eq!(ramping(&grown, 1), [false, true]);
        // An upstream that had none has kept none.
        assert_eq!(ramping(&grown, 0), [false]);
        let replaced = Destinations::reconcile(
            &config(&[
                ("web", &["127.0.0.1:3", "127.0.0.1:4"]),
                ("new", &["127.0.0.1:9"]),
            ]),
            &grown,
            &keys,
            &[],
        );
        assert_eq!(ramping(&replaced, 1), [false, false]);
    }

    /// A ramping endpoint's share rises with its window, reads the clock only while it
    /// ramps, and is full again, for good, once the window is over.
    #[test]
    fn a_ramp_is_over_once_its_window_is() {
        let keys = Keys::default();
        let destinations = Destinations::reconcile(
            &config(&[("web", &["127.0.0.1:1"])]),
            &Destinations::default(),
            &keys,
            &[],
        );
        let endpoint = destinations.at(0, 0).unwrap();
        assert_eq!(endpoint.share(Some(60_000)), Share::FULL);
        endpoint.start_ramp();
        // No slow start, no ramp, whatever was stamped.
        assert_eq!(endpoint.share(None), Share::FULL);
        let early = endpoint.share(Some(3_600_000));
        assert_ne!(early, Share::FULL);
        assert!(early == Share::ramped(0, 3_600_000) || early == Share::ramped(1, 3_600_000));
        assert!(endpoint.is_ramping());
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(endpoint.share(Some(2)), Share::FULL);
        assert!(!endpoint.is_ramping());
        assert_eq!(endpoint.share(Some(3_600_000)), Share::FULL);
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
        let mut http2 = model(&[("web", &["127.0.0.1:1"])]);
        http2.upstreams.get_mut("web").unwrap().protocol = UpstreamProtocol::Http2;
        let after = Destinations::reconcile(&compile(&http2).unwrap(), &before, &keys, &[]);
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
