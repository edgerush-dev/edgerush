//! The data plane as a whole: the snapshots it is made and reloaded with, its drain, and
//! what it tells a scrape and the health checker.

use super::{ACCEPT_PAUSE, Proxy, ProxyError, Snapshot, authority, is_about_one_connection};
use crate::downstream::h3::listener as h3_listener;
use crate::metrics::{AcceptPause, Metrics};
use crate::routed::Routed;
use crate::tls::Tls;
use crate::upstream::destination::{Destinations, Keys, ReuseIdentity};
use crate::upstream::secure::Secure;
use arc_swap::ArcSwap;
use edgerush_config::Compiled;
use edgerush_filters::HeaderModifier;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl Snapshot {
    pub(super) fn new(
        config: Compiled,
        listeners: &[String],
        metrics: &Metrics,
        previous: Option<&Snapshot>,
        keys: &Keys,
    ) -> Result<Self, ProxyError> {
        let endpoints = config
            .upstreams()
            .iter()
            .map(|upstream| upstream.endpoints.iter().map(authority).collect())
            .collect::<Result<_, _>>()?;
        let nothing_routed = Routed::default();
        let previous_routed = previous.map_or(&nothing_routed, |previous| &previous.routed);
        let routed = Routed::reconcile(&config, listeners, previous_routed, keys);
        let listeners: Vec<Option<usize>> = listeners
            .iter()
            .map(|name| config.listeners().iter().position(|l| l.name == *name))
            .collect();
        let tls = listeners
            .iter()
            .enumerate()
            .map(|(position, at)| {
                let Some(listener) = at.and_then(|at| config.listeners().get(at)) else {
                    return Ok(None);
                };
                let Some(source) = &listener.tls else {
                    return Ok(None);
                };
                let before = previous
                    .and_then(|previous| previous.tls.get(position))
                    .and_then(Option::as_ref);
                if let Some(kept) = before.filter(|kept| kept.is_for(source)) {
                    return Ok(Some(Arc::clone(kept)));
                }
                // New certificates behind the front the listener had, if it can keep it.
                before
                    .map_or_else(|| Tls::new(source), |before| Tls::after(before, source))
                    .map(|tls| Some(Arc::new(tls)))
                    .map_err(|error| ProxyError::Tls {
                        listener: listener.name.clone(),
                        error,
                    })
            })
            .collect::<Result<_, _>>()?;
        // Where HTTP/3 is served: the first config says, as its sockets are the ones bound.
        // A port the operating system chose is one the config does not know.
        let quic: Vec<Option<u16>> = match previous {
            Some(previous) => previous.quic.clone(),
            None => listeners
                .iter()
                .map(|at| {
                    let listener = at.and_then(|at| config.listeners().get(at))?;
                    listener.http3?;
                    Some(listener.address.port()).filter(|port| *port != 0)
                })
                .collect(),
        };
        // Advertised only where a socket serves it, on the port it is on.
        let alt_svc = listeners
            .iter()
            .zip(&quic)
            .map(|(at, port)| alt_svc(at.and_then(|at| config.listeners().get(at))?, (*port)?))
            .collect();
        let upstream_slots = config
            .upstreams()
            .iter()
            .map(|upstream| metrics.upstream_slot(&upstream.name))
            .collect();
        let secure: Vec<Option<Arc<Secure>>> = config
            .upstreams()
            .iter()
            .map(|upstream| {
                let Some(source) = &upstream.tls else {
                    return Ok(None);
                };
                let kept = previous
                    .into_iter()
                    .flat_map(|previous| previous.secure.iter().flatten())
                    .find(|kept| kept.is_for(source, upstream.protocol));
                match kept {
                    Some(kept) => Ok(Some(Arc::clone(kept))),
                    None => Secure::new(source, upstream.protocol)
                        .map(|secure| Some(Arc::new(secure)))
                        .map_err(|error| ProxyError::UpstreamTls {
                            upstream: upstream.name.clone(),
                            error,
                        }),
                }
            })
            .collect::<Result<_, _>>()?;
        let nothing_yet = Destinations::default();
        let previous_destinations =
            previous.map_or(&nothing_yet, |previous| &previous.destinations);
        let destinations = Destinations::reconcile(&config, previous_destinations, keys, &secure);
        Ok(Self {
            config,
            generation: previous.map_or(0, |previous| previous.generation + 1),
            listeners,
            endpoints,
            upstream_slots,
            destinations,
            tls,
            quic,
            alt_svc,
            secure,
            routed,
        })
    }
}

/// The `Alt-Svc` a listener's answers carry: HTTP/3 on `port`, where its UDP socket is, for
/// as long as its config says (RFC 7838 §3). None for a listener whose config has no HTTP/3.
fn alt_svc(listener: &edgerush_config::CompiledListener, port: u16) -> Option<HeaderModifier> {
    let http3 = listener.http3?;
    let value = format!("h3=\":{port}\"; ma={}", http3.alt_svc_max_age);
    HeaderModifier::new([("alt-svc", value.as_str())], [], []).ok()
}

impl Proxy {
    /// A data plane that runs `config` on `workers` of them. Nothing is listened on or
    /// connected to yet, and no worker exists until [`Worker::new`] makes one.
    ///
    /// The worker count is told, not guessed: it is what the counters are sharded by, and
    /// a process left to work it out for itself reads the machine rather than what it was
    /// given ([03 §2] in the docs).
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] for an endpoint address that cannot be part of a request
    /// target (one with an IPv6 zone).
    ///
    /// [`Worker::new`]: super::Worker::new
    pub fn new(config: Compiled, workers: NonZeroUsize) -> Result<Self, ProxyError> {
        let listeners: Vec<String> = config
            .listeners()
            .iter()
            .map(|listener| listener.name.clone())
            .collect();
        // A shard for every worker, so that no two write to one line of cache.
        let metrics = Metrics::new(workers, listeners.len());
        let keys = Keys::default();
        let snapshot = Snapshot::new(config, &listeners, &metrics, None, &keys)?;
        let quic =
            h3_listener::Secrets::new().map_err(|error| ProxyError::Random(error.to_string()))?;
        Ok(Self {
            listeners,
            current: ArcSwap::from_pointee(snapshot),
            metrics,
            keys,
            draining: AtomicBool::new(false),
            quic,
        })
    }

    /// The names of the listeners that can be served: those of the config the data plane
    /// was made with, in its order.
    #[must_use]
    pub fn listeners(&self) -> &[String] {
        &self.listeners
    }

    /// Drains the data plane: every worker, at its next sweep, stops accepting and lets its
    /// connections go as they finish (03 §10). It cannot be undone.
    pub fn drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    /// Runs `config` from now on. It is published whole and at once: every request is
    /// served by one config or by the other, none waits and none is dropped. Requests that
    /// are under way finish by the rule they began with; sockets stay open and upstream
    /// connections stay warm.
    ///
    /// A listener keeps its socket by its name. While a config does not have it, nothing
    /// that comes in on its socket has a route; listeners that only a later config has are
    /// not served, as no socket is theirs.
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] as [`Proxy::new`] does; the data plane then runs on as it
    /// was.
    pub fn reload(&self, config: Compiled) -> Result<(), ProxyError> {
        // Against the config on its way out, so that a destination which has not changed
        // keeps what its connections are filed under and one that has gone is retired.
        let previous = self.current.load();
        let snapshot = Snapshot::new(
            config,
            &self.listeners,
            &self.metrics,
            Some(&previous),
            &self.keys,
        )?;
        drop(previous);
        // Only now that all of it is accepted: a config refused for one listener must not
        // have changed the certificates of another.
        for tls in snapshot.tls.iter().flatten() {
            tls.install();
        }
        self.current.store(Arc::new(snapshot));
        self.metrics.reloads.inc();
        let now = SystemTime::now().duration_since(UNIX_EPOCH);
        let now = now.map_or(0, |since_epoch| since_epoch.as_secs());
        self.metrics.last_reload.store(now, Ordering::Relaxed);
        Ok(())
    }

    /// What has been counted, in the Prometheus text format: per listener and per upstream
    /// of the current config. Counters are kept outside the config, so a reload resets
    /// none of them. For scrapes: it adds up the shards of every series and allocates.
    #[must_use]
    pub fn metrics(&self) -> String {
        let snapshot = self.current.load();
        let upstreams: Vec<(&str, usize)> = snapshot
            .config
            .upstreams()
            .iter()
            .zip(&snapshot.upstream_slots)
            .map(|(upstream, slot)| (upstream.name.as_str(), *slot))
            .collect();
        // Read where the health checker and the workers write it, at the moment of the
        // scrape.
        let endpoints: Vec<(&str, usize, usize)> = snapshot
            .config
            .upstreams()
            .iter()
            .enumerate()
            .map(|(position, upstream)| {
                let destinations = snapshot.destinations.of(position);
                let count = |which: fn(&ReuseIdentity) -> bool| {
                    destinations
                        .iter()
                        .filter(|destination| which(destination))
                        .count()
                };
                let serving = count(ReuseIdentity::is_healthy);
                let aside = count(|destination| destination.set_aside_for().is_some());
                (upstream.name.as_str(), serving, aside)
            })
            .collect();
        self.metrics.render(&self.listeners, &upstreams, &endpoints)
    }

    /// How many connections are open, on every listener and every worker, over TCP and
    /// QUIC alike: what a draining process waits on before it exits (03 §10).
    #[must_use]
    pub fn open_connections(&self) -> usize {
        self.metrics.open_connections()
    }

    /// Probes the endpoints of every upstream that asks for health checks, for as long as
    /// the data plane runs, and keeps those that fail them out of load balancing
    /// ([03 §6](../../docs/03-data-plane.md)). For a thread and runtime of its own, in a
    /// `LocalSet`: the probes must still run when the workers are saturated.
    pub async fn check_health(self: Arc<Self>) {
        crate::health::check(self).await;
    }

    /// The destinations of the running config that are set aside, and how long each waits
    /// for its connect probe: the data plane's `set_aside_ms`.
    pub(crate) fn set_aside(&self) -> (Vec<Arc<ReuseIdentity>>, Duration) {
        let snapshot = self.current.load();
        let aside = snapshot
            .destinations
            .all()
            .filter(|destination| destination.set_aside_for().is_some())
            .map(Arc::clone)
            .collect();
        (aside, snapshot.config.data_plane().set_aside)
    }

    /// Counts `destination` as set aside, for its upstream. For the health checker, as it
    /// finds one: it takes a lock, which is why no worker counts it.
    pub(crate) fn count_set_aside(&self, destination: &ReuseIdentity) {
        let slot = self.metrics.upstream_slot(destination.upstream());
        if let Some(counters) = self.metrics.upstream(slot) {
            counters.set_asides.inc();
        }
    }

    /// The destinations of the running config whose endpoints are probed.
    pub(crate) fn checked(&self) -> impl Iterator<Item = Arc<ReuseIdentity>> + use<> {
        let snapshot = self.current.load();
        let checked: Vec<Arc<ReuseIdentity>> = snapshot
            .destinations
            .all()
            .filter(|destination| destination.health_check().is_some())
            .map(Arc::clone)
            .collect();
        checked.into_iter()
    }

    /// Counts a failure to accept on the socket of the listener at position `listener`,
    /// and says how long to wait before accepting again: not at all after the failure of
    /// the one connection that was next in line, a moment after one that is not about a
    /// connection — out of file descriptors, say — and would only happen again at once.
    /// For whoever accepts by themselves and serves with [`Worker::serve_connection`].
    ///
    /// [`Worker::serve_connection`]: super::Worker::serve_connection
    pub fn accept_failed(&self, listener: usize, error: &io::Error) -> Option<Duration> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.accept_errors.inc();
        }
        (!is_about_one_connection(error)).then_some(ACCEPT_PAUSE)
    }

    /// Counts the listener at position `listener` stopping accepting, for `why`
    /// ([03 §9](../../docs/03-data-plane.md)). For whoever accepts by themselves.
    pub fn accept_paused(&self, listener: usize, why: AcceptPause) {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.paused(why);
        }
    }
}
