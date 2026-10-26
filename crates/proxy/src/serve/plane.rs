//! The data plane as a whole: the snapshots it is made and reloaded with, its drain, and
//! what it tells a scrape and the health checker.

use super::{ACCEPT_PAUSE, Proxy, ProxyError, Snapshot, authority, is_about_one_connection};
use crate::access_log::{Logs, Sink};
use crate::downstream::h3::listener as h3_listener;
use crate::metrics::{AcceptPause, Metrics};
use crate::routed::Routed;
use crate::tls::Tls;
use crate::upstream::balancing::Carry;
use crate::upstream::destination::{Aside, Destinations, Keys, Retiring, ReuseIdentity};
use crate::upstream::secure::Secure;
use arc_swap::ArcSwap;
use edgerush_config::{Compiled, CompiledRateLimiting};
use edgerush_discovery::Endpoints;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Refuses a config with a rate-limiting service on a machine without the native 128-bit
/// compare-and-swap its keys' entries need (`supported` false): a lock in its place would
/// put one on every request. And one with a limit whose largest allowance a cycle could
/// pass what a key's entry holds, rather than hold it to less and refuse what it allows.
pub(super) fn runs_rate_limits(config: &Compiled, supported: bool) -> Result<(), ProxyError> {
    let Some(service) = config.rate_limit_service() else {
        return Ok(());
    };
    if !supported {
        return Err(ProxyError::RateLimitsUnsupported);
    }
    let cycle = u64::try_from(service.cycle.as_nanos()).unwrap_or(u64::MAX);
    for limit in config.limits() {
        let contract = edgerush_ratelimit::service::Contract {
            limit: limit.count,
            window: limit.window,
            burst: limit.burst,
        };
        if !edgerush_ratelimit::service::pooled::fits(contract, cycle) {
            return Err(ProxyError::LimitTooLarge(limit.name.clone()));
        }
    }
    Ok(())
}

/// The EndpointSlices of each namespace followed, as last told, by namespace.
pub(crate) type Discovered = HashMap<String, Arc<[EndpointSlice]>>;

/// Each upstream's endpoints, by position: those the config lists, or those its Service's
/// slices say take requests (03 §6), none while its namespace is not followed. Worked out
/// whenever a snapshot is made, so that a config that names another Service of a namespace
/// followed has its endpoints at once.
fn resolved(config: &Compiled, discovered: &Discovered) -> Vec<Vec<SocketAddr>> {
    config
        .upstreams()
        .iter()
        .map(|upstream| match &upstream.discovery {
            Some(service) => discovered
                .get(&service.namespace)
                .map(|slices| {
                    Endpoints::of(service, slices)
                        .in_use()
                        .iter()
                        .copied()
                        .collect()
                })
                .unwrap_or_default(),
            None => upstream.endpoints.clone(),
        })
        .collect()
}

impl Snapshot {
    /// The snapshot of `config` with the endpoints `discovered` has for its discovered
    /// upstreams, after `previous` if there is one, and the destinations of `previous` it
    /// does not have, retired only once it is taken in.
    pub(super) fn new(
        config: Arc<Compiled>,
        discovered: &Discovered,
        listeners: &[String],
        metrics: &Metrics,
        previous: Option<&Snapshot>,
        keys: &Keys,
        logs: Vec<Option<Sink>>,
    ) -> Result<(Self, Retiring), ProxyError> {
        runs_rate_limits(&config, edgerush_ratelimit::service::pooled::supported())?;
        let resolved = resolved(&config, discovered);
        let endpoints = resolved
            .iter()
            .map(|endpoints| endpoints.iter().map(authority).collect())
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
            .map(|(at, port)| {
                crate::way_back::alt_svc(at.and_then(|at| config.listeners().get(at))?, (*port)?)
            })
            .collect();
        let upstream_slots = config
            .upstreams()
            .iter()
            .map(|upstream| metrics.upstream_slot(&upstream.name))
            .collect();
        let limit_slots = config
            .limits()
            .iter()
            .map(|limit| metrics.limit_slot(&limit.name))
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
        let (destinations, retiring) =
            Destinations::prepare(&config, &resolved, previous_destinations, keys, &secure);
        let carried = previous.map_or_else(
            || vec![Carry::New; config.upstreams().len()],
            |previous| {
                Carry::between(
                    &previous.config,
                    &previous.destinations,
                    &config,
                    &destinations,
                )
            },
        );
        let snapshot = Self {
            config,
            generation: previous.map_or(0, |previous| previous.generation + 1),
            listeners,
            endpoints,
            upstream_slots,
            limit_slots,
            carried,
            destinations,
            tls,
            quic,
            alt_svc,
            secure,
            routed,
            logs,
        };
        Ok((snapshot, retiring))
    }
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
        let logs = Logs::new(workers.get(), Arc::clone(&metrics.logs_dropped));
        let prepared = logs.prepare(&config, &listeners)?;
        // After none, nothing to retire; and nothing discovered yet.
        let (snapshot, _none) = Snapshot::new(
            Arc::new(config),
            &Discovered::new(),
            &listeners,
            &metrics,
            None,
            &keys,
            prepared.by_listener(),
        )?;
        let quic =
            h3_listener::Secrets::new().map_err(|error| ProxyError::Random(error.to_string()))?;
        logs.commit(prepared);
        Ok(Self {
            listeners,
            current: ArcSwap::from_pointee(snapshot),
            metrics,
            workers,
            rate_limits: std::sync::Mutex::default(),
            keys,
            draining: AtomicBool::new(false),
            quic,
            logs,
            discovered: Mutex::default(),
            making: Mutex::default(),
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
        let _making = self.making.lock().unwrap_or_else(PoisonError::into_inner);
        // Against the config on its way out, so that a destination which has not changed
        // keeps what its connections are filed under and one that has gone is retired.
        let previous = self.current.load();
        let prepared = self.logs.prepare(&config, &self.listeners)?;
        let discovered = self
            .discovered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (snapshot, retiring) = Snapshot::new(
            Arc::new(config),
            &discovered,
            &self.listeners,
            &self.metrics,
            Some(&previous),
            &self.keys,
            prepared.by_listener(),
        )?;
        drop(discovered);
        drop(previous);
        // Held until the config is in, so that no worker pairs either config with the
        // other's thread.
        let mut rate_limits = self
            .rate_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Not started yet, starting them starts it for whichever config runs then.
        let hub = match &rate_limits.failed {
            Some(failed) => self.hub_for(&snapshot.config, rate_limits.hub.as_ref(), failed)?,
            None => None,
        };
        let local = match &rate_limits.failed {
            Some(failed) => self.local_for(&snapshot.config, rate_limits.local.as_ref(), failed)?,
            None => None,
        };
        let synchronous = match &rate_limits.failed {
            Some(failed) => {
                self.sync_for(&snapshot.config, rate_limits.synchronous.as_ref(), failed)?
            }
            None => None,
        };
        // Only now that all of it is accepted: a config refused for one listener must not
        // have changed the certificates of another, nor opened a file it logs to, nor let
        // go of the running config's connections.
        for tls in snapshot.tls.iter().flatten() {
            tls.install();
        }
        retiring.retire();
        // A thread kept keeps the keys it holds, now within this config's bound (06 §8).
        if let (Some(hub), Some(service)) = (&hub, snapshot.config.rate_limit_service()) {
            hub.set_capacity(service.max_key_bytes);
        }
        // Before any request of it can make a record.
        self.logs.commit(prepared);
        self.current.store(Arc::new(snapshot));
        rate_limits.hub = hub;
        rate_limits.local = local;
        rate_limits.synchronous = synchronous;
        drop(rate_limits);
        self.metrics.reloads.inc();
        let now = SystemTime::now().duration_since(UNIX_EPOCH);
        let now = now.map_or(0, |since_epoch| since_epoch.as_secs());
        self.metrics.last_reload.store(now, Ordering::Relaxed);
        Ok(())
    }

    /// Takes in the EndpointSlices of namespaces followed: for each, every slice it now
    /// has, or none when it is no longer followed. An upstream whose Service is in one is
    /// served with the endpoints they give it from now on, published as a reload publishes
    /// a config, the config itself as it was: an endpoint kept keeps its connections and
    /// what is known of it, one added slow-starts beside them, and one gone is retired, an
    /// exchange under way finishing (03 §6). The slices are kept, so that a config that
    /// comes to name another Service of the namespace has its endpoints at once. Whether
    /// the running config's endpoints changed.
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] as [`Proxy::new`] does; nothing is taken in then.
    pub fn rediscover(
        &self,
        changes: impl IntoIterator<Item = (String, Option<Arc<[EndpointSlice]>>)>,
    ) -> Result<bool, ProxyError> {
        let _making = self.making.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = self.current.load();
        let mut discovered = self
            .discovered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut next = discovered.clone();
        for (namespace, slices) in changes {
            match slices {
                Some(slices) => next.insert(namespace, slices),
                None => next.remove(&namespace),
            };
        }
        if resolved(&previous.config, &next) == resolved(&previous.config, &discovered) {
            *discovered = next;
            return Ok(false);
        }
        // The running config's listeners, logs and TLS are as they were, and are kept.
        let (snapshot, retiring) = Snapshot::new(
            Arc::clone(&previous.config),
            &next,
            &self.listeners,
            &self.metrics,
            Some(&previous),
            &self.keys,
            previous.logs.clone(),
        )?;
        *discovered = next;
        drop(previous);
        // As a reload holds it, so that no worker makes its limiter between the two.
        let rate_limits = self
            .rate_limits
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        retiring.retire();
        self.current.store(Arc::new(snapshot));
        drop(rate_limits);
        Ok(true)
    }

    /// Has the access logs' files opened again by their paths, for whoever moved them
    /// aside to rotate them: what `SIGUSR1` asks for, as it asks NGINX and Envoy.
    pub fn reopen_logs(&self) {
        self.logs.reopen();
    }

    /// Has every worker hand over the access-log records it holds, and waits up to `within`
    /// for all of them to be written: for the end of a drain, before the process exits.
    /// Whether everything was written in time.
    #[must_use]
    pub fn finish_logs(&self, within: Duration) -> bool {
        self.logs.finish(within)
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
        let limits: Vec<(&str, usize, bool)> = snapshot
            .config
            .limits()
            .iter()
            .zip(snapshot.limit_slots.iter())
            .map(|(limit, slot)| (limit.name.as_str(), *slot, limit.dry_run))
            .collect();
        let mut scrape = self.metrics.render(&self.listeners, &upstreams, &endpoints);
        // Only a config that enforces limits shows them.
        let max_key_bytes = match snapshot.config.rate_limiting() {
            CompiledRateLimiting::Local { max_key_bytes } => Some(*max_key_bytes),
            CompiledRateLimiting::Asynchronous(service) => Some(service.max_key_bytes),
            CompiledRateLimiting::Synchronous(synchronous) => Some(synchronous.max_key_bytes),
            CompiledRateLimiting::Off => None,
        };
        if let Some(max_key_bytes) = max_key_bytes
            && !limits.is_empty()
        {
            scrape.push_str(&self.metrics.render_limits(&limits, max_key_bytes));
            // The process thread's own measures.
            let (_, hub) = self.hub();
            if let Some(hub) = hub {
                scrape.push_str(&hub.render_stats());
            }
            let (_, synchronous) = self.sync_limits();
            if let Some(synchronous) = synchronous {
                scrape.push_str(&synchronous.render_stats());
            }
        }
        scrape
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

    /// The rate-limiting thread `config` decides its limits with: none without a service,
    /// `running` if it reaches the service as it is set, else one started, calling `failed`
    /// if it fails.
    fn hub_for(
        &self,
        config: &Compiled,
        running: Option<&Arc<crate::ratelimit::Hub>>,
        failed: &crate::ratelimit::Failed,
    ) -> Result<Option<Arc<crate::ratelimit::Hub>>, ProxyError> {
        let Some(service) = config.rate_limit_service() else {
            return Ok(None);
        };
        if let Some(running) = running.filter(|running| running.serves(service)) {
            return Ok(Some(Arc::clone(running)));
        }
        crate::ratelimit::Hub::start(
            service,
            self.workers.get(),
            service.max_key_bytes,
            Some(failed.clone()),
        )
        .map(Some)
    }

    /// The keys `config` decides its limits with in local mode: none in another mode;
    /// `running`, within the new cap, if there are some, so that a reload keeps them; else
    /// new ones, their sweep calling `failed` if it fails.
    fn local_for(
        &self,
        config: &Compiled,
        running: Option<&Arc<crate::ratelimit::local::Process>>,
        failed: &crate::ratelimit::Failed,
    ) -> Result<Option<Arc<crate::ratelimit::local::Process>>, ProxyError> {
        let CompiledRateLimiting::Local { max_key_bytes } = *config.rate_limiting() else {
            return Ok(None);
        };
        if let Some(running) = running {
            running.set_capacity(max_key_bytes);
            return Ok(Some(Arc::clone(running)));
        }
        crate::ratelimit::local::Process::start(
            self.workers.get(),
            max_key_bytes,
            Some(failed.clone()),
        )
        .map(Some)
        .map_err(|error| ProxyError::RateLimitThread(format!("no thread: {error}")))
    }

    /// The synchronous mode's side of the process for `config`: none in another mode;
    /// `running`, if it asks as `config` says, so that its workers keep their streams; else
    /// a new one, carrying across what `running` knows of an outage, its probe thread
    /// calling `failed` if it fails.
    fn sync_for(
        &self,
        config: &Compiled,
        running: Option<&Arc<crate::ratelimit::synchronous::Process>>,
        failed: &crate::ratelimit::Failed,
    ) -> Result<Option<Arc<crate::ratelimit::synchronous::Process>>, ProxyError> {
        let CompiledRateLimiting::Synchronous(synchronous) = config.rate_limiting() else {
            return Ok(None);
        };
        if let Some(running) = running
            && running.is_for(synchronous)
        {
            return Ok(Some(Arc::clone(running)));
        }
        crate::ratelimit::synchronous::Process::start(
            synchronous.clone(),
            self.workers,
            Some(failed.clone()),
            running.map(|running| &**running),
        )
        .map(Some)
        .map_err(|error| ProxyError::RateLimitThread(format!("no thread: {error}")))
    }

    /// The running config and the keys its limits are decided with in local mode, taken in
    /// together, as [`hub`](Self::hub) takes the thread.
    pub(super) fn local_limits(
        &self,
    ) -> (Arc<Snapshot>, Option<Arc<crate::ratelimit::local::Process>>) {
        let rate_limits = self
            .rate_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (self.current.load_full(), rate_limits.local.clone())
    }

    /// The running config and the synchronous mode's side of the process, taken in
    /// together, as [`hub`](Self::hub) takes the thread.
    pub(super) fn sync_limits(
        &self,
    ) -> (
        Arc<Snapshot>,
        Option<Arc<crate::ratelimit::synchronous::Process>>,
    ) {
        let rate_limits = self
            .rate_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (self.current.load_full(), rate_limits.synchronous.clone())
    }

    /// The running config and the rate-limiting thread its limits are decided with, which
    /// are always taken in together; no thread for a config with limits if they were not
    /// started ([`Proxy::start_rate_limits`]). For a worker making its limiter: it takes a
    /// lock.
    pub(super) fn hub(&self) -> (Arc<Snapshot>, Option<Arc<crate::ratelimit::Hub>>) {
        let rate_limits = self
            .rate_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (self.current.load_full(), rate_limits.hub.clone())
    }

    /// Starts the data plane's rate limits: the process's rate-limiting thread for the
    /// running config, if it has a rate-limiting service, and from now on for every config
    /// taken in whose service is set otherwise, a reload refused if its thread cannot be
    /// started ([06 §4](../../docs/06-rate-limiting.md)). Called before a request is
    /// served: until then a request under a limit is refused, as one its limits cannot
    /// decide (503).
    ///
    /// `failed` is called if a thread fails: it panics, or a task of its does
    /// ([03 §2](../../docs/03-data-plane.md)). Whoever runs the data plane fails the process
    /// then, as when a worker's thread fails: without the thread every request that asks it
    /// is refused and nothing more is reported to the service. A thread that ends because a
    /// reload replaced it, or the data plane went, has not failed.
    ///
    /// Once started, a later call does nothing.
    ///
    /// # Errors
    ///
    /// [`ProxyError::RateLimitThread`] if the thread cannot be started: the data plane
    /// cannot run its config.
    pub fn start_rate_limits(
        &self,
        failed: impl Fn() + Send + Sync + 'static,
    ) -> Result<(), ProxyError> {
        let mut rate_limits = self
            .rate_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if rate_limits.failed.is_some() {
            return Ok(());
        }
        let failed = crate::ratelimit::Failed(Arc::new(failed));
        rate_limits.hub = self.hub_for(&self.current.load().config, None, &failed)?;
        rate_limits.local = self.local_for(&self.current.load().config, None, &failed)?;
        rate_limits.synchronous = self.sync_for(&self.current.load().config, None, &failed)?;
        rate_limits.failed = Some(failed);
        Ok(())
    }

    /// Counts `destination` as set aside, for its upstream. For the health checker, as it
    /// finds one: it takes a lock, which is why no worker counts it.
    pub(crate) fn count_set_aside(&self, destination: &ReuseIdentity) {
        let slot = self.metrics.upstream_slot(destination.upstream());
        if let Some(counters) = self.metrics.upstream(slot) {
            match destination.aside() {
                Some(Aside::NoPort) => counters.set_asides_no_port.inc(),
                _ => counters.set_asides.inc(),
            }
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
