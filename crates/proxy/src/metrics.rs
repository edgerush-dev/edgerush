//! What the data plane counts, and how a scrape reads it.
//!
//! Series belong to the data plane, not to a config snapshot, so a reload never resets a
//! counter. A listener's series are found by the position of its socket. An upstream's are
//! found by a **slot** that its name is given when a config that has it first arrives: a
//! request carries the slot, a plain number, across the wait for its upstream, and
//! counting is a load and an add — no lock, no reference count, no name. Slots are never
//! handed back, and there are [`UPSTREAM_SLOTS`] of them; upstreams beyond that are
//! counted together in one series, as docs/08 wants of anything that a config can make
//! arbitrarily many of.

use crate::request::Rejection;
use edgerush_telemetry::{Counter, Exposition, Gauge, Histogram, Kind, Sharded};
use http::StatusCode;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

/// How many upstream names get series of their own over the life of a data plane.
pub(crate) const UPSTREAM_SLOTS: usize = 4096;

/// The slot of the series that upstreams beyond [`UPSTREAM_SLOTS`] share, and the name it
/// is shown under.
const OVERFLOW_SLOT: usize = 0;
const OVERFLOW_NAME: &str = "_overflow";

/// Upper bounds of the time to the response head, in nanoseconds: half a millisecond to
/// ten seconds.
const HEAD_TIME_BOUNDS: [u64; 14] = [
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
];

const CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];

/// Why an exchange by EdgeRush's own path ended without an answer.
///
/// A fixed list, and what a counter is labelled with is a name from it: an upstream
/// cannot invent a new series by failing in a new way, and no error text or address ever
/// reaches a label ([13 §7](../../docs/13-http1-upstream.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stopped {
    /// What the upstream said could not be read.
    Codec,
    /// The connection itself failed.
    Io,
    /// The request's own body could not be read.
    RequestBody,
    /// The upstream went away without answering.
    Closed,
    /// The upstream said something before it was asked anything.
    Unsolicited,
    /// More interim answers, or longer ones, than an exchange will wait through.
    Interim,
    /// No final head within the time an exchange has for one.
    TooSlow,
    /// Whatever was being waited for stopped happening for long enough.
    Idle,
    /// The worker could not pay for storage the exchange needed.
    Exhausted,
}

impl Stopped {
    /// The name this reason is counted under.
    fn name(self) -> &'static str {
        match self {
            Self::Codec => "codec",
            Self::Io => "io",
            Self::RequestBody => "request_body",
            Self::Closed => "closed",
            Self::Unsolicited => "unsolicited",
            Self::Interim => "interim",
            Self::TooSlow => "too_slow",
            Self::Idle => "idle",
            Self::Exhausted => "exhausted",
        }
    }

    /// Every one of them, for a scrape that shows a series whether it has happened or not.
    const ALL: [Self; 9] = [
        Self::Codec,
        Self::Io,
        Self::RequestBody,
        Self::Closed,
        Self::Unsolicited,
        Self::Interim,
        Self::TooSlow,
        Self::Idle,
        Self::Exhausted,
    ];
}

/// What became of a connection to an upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Socket {
    /// Opened for this exchange, there being none to take.
    Opened,
    /// Taken from what the worker was keeping.
    Reused,
    /// Dropped rather than used or kept: it had something to say at checkout, or its age
    /// or idleness had run out.
    Discarded,
}

impl Socket {
    fn name(self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::Reused => "reused",
            Self::Discarded => "discarded",
        }
    }

    const ALL: [Self; 3] = [Self::Opened, Self::Reused, Self::Discarded];
}

/// Why the data plane answered a request itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Answer {
    BadHost,
    BadPath,
    BadConnection,
    BadTarget,
    NoRoute,
    NoBackend,
    NoEndpoints,
    UpstreamFailed,
    /// This worker already has as many exchanges in hand as it will take.
    TooBusy,
    /// This worker could not pay for the storage an exchange needed
    /// ([14 §8](../../docs/14-downstream-server.md)): its own failing, not the upstream's.
    Exhausted,
}

impl Answer {
    const ALL: [Self; 10] = [
        Self::BadHost,
        Self::BadPath,
        Self::BadConnection,
        Self::BadTarget,
        Self::NoRoute,
        Self::NoBackend,
        Self::NoEndpoints,
        Self::UpstreamFailed,
        Self::TooBusy,
        Self::Exhausted,
    ];

    /// The status that is answered with.
    pub(crate) fn status(self) -> StatusCode {
        match self {
            Self::BadHost | Self::BadPath | Self::BadConnection | Self::BadTarget => {
                StatusCode::BAD_REQUEST
            }
            Self::NoRoute => StatusCode::NOT_FOUND,
            Self::NoBackend => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NoEndpoints | Self::TooBusy | Self::Exhausted => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamFailed => StatusCode::BAD_GATEWAY,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::BadHost => "bad_host",
            Self::BadPath => "bad_path",
            Self::BadConnection => "bad_connection",
            Self::BadTarget => "bad_target",
            Self::NoRoute => "no_route",
            Self::NoBackend => "no_backend",
            Self::NoEndpoints => "no_endpoints",
            Self::UpstreamFailed => "upstream_failed",
            Self::TooBusy => "too_busy",
            Self::Exhausted => "exhausted",
        }
    }
}

impl From<Rejection> for Answer {
    fn from(rejection: Rejection) -> Self {
        match rejection {
            Rejection::Host(_) => Self::BadHost,
            Rejection::Path(_) => Self::BadPath,
            Rejection::Connection(_) => Self::BadConnection,
            Rejection::Target => Self::BadTarget,
            Rejection::NoRoute => Self::NoRoute,
            Rejection::NoBackend => Self::NoBackend,
        }
    }
}

/// What is counted per listener: everything a request counts on its way, in one group.
#[derive(Debug, Default)]
pub(crate) struct ListenerCounters {
    pub(crate) accepted: Counter,
    pub(crate) active: Gauge,
    pub(crate) accept_errors: Counter,
    responses: [Counter; 5],
    answers: [Counter; Answer::ALL.len()],
    head_time: Histogram<14>,
}

impl ListenerCounters {
    /// A response is on its way to the client, `nanoseconds` after its request came in.
    pub(crate) fn responded(&self, status: StatusCode, nanoseconds: u64) {
        if let Some(class) = self.responses.get(class_of(status)) {
            class.inc();
        }
        self.head_time.observe(&HEAD_TIME_BOUNDS, nanoseconds);
    }

    /// The response is one of the data plane's own.
    pub(crate) fn answered(&self, answer: Answer) {
        let position = Answer::ALL.iter().position(|other| *other == answer);
        if let Some(counter) = position.and_then(|position| self.answers.get(position)) {
            counter.inc();
        }
    }
}

/// What is counted per upstream.
#[derive(Debug, Default)]
pub(crate) struct UpstreamCounters {
    pub(crate) requests: Counter,
    responses: [Counter; 5],
    pub(crate) failures: Counter,
    /// Answers whose head was handed on and whose body then failed. Counted apart from
    /// `failures`, which is answers that never arrived: by the time one of these happens
    /// the status has been counted and the client has been told
    /// ([13 §7](../../docs/13-http1-upstream.md)).
    pub(crate) body_failures: Counter,
}

impl UpstreamCounters {
    pub(crate) fn responded(&self, status: StatusCode) {
        if let Some(class) = self.responses.get(class_of(status)) {
            class.inc();
        }
    }
}

/// The position of a status in [`CLASSES`]. `http` has no status below 100 or above 999;
/// what is above 599 counts as 5xx.
fn class_of(status: StatusCode) -> usize {
    usize::from(status.as_u16() / 100).clamp(1, 5) - 1
}

/// All the series of a data plane.
#[derive(Debug)]
pub(crate) struct Metrics {
    shards: NonZeroUsize,
    /// By the position of the listener's socket.
    listeners: Vec<Sharded<ListenerCounters>>,
    /// By slot; a slot is filled when it is given out and stays filled.
    upstreams: Box<[OnceLock<Sharded<UpstreamCounters>>]>,
    /// Which slot an upstream's name has. Only reloads and scrapes come here.
    slots: Mutex<HashMap<String, usize>>,
    pub(crate) reloads: Counter,
    /// Seconds since the Unix epoch; zero before the first reload.
    pub(crate) last_reload: AtomicU64,
    /// Exchanges of EdgeRush's own that ended without an answer, by reason.
    stopped: Sharded<[Counter; Stopped::ALL.len()]>,
    /// What became of the connections a worker used, by which of the three it was.
    connections: Sharded<[Counter; 3]>,
    /// What each worker has in hand, sampled by the worker itself as it sweeps.
    workers: Sharded<WorkerGauges>,
}

/// What one worker holds at the moment it last looked. Sampled rather than kept up to
/// date on the request path: a gauge is for how much there is now, and a sweep already
/// walks everything this asks about.
#[derive(Debug, Default)]
pub(crate) struct WorkerGauges {
    /// Exchanges in hand, from before a connection is looked for until the answer's body
    /// has been let go of.
    exchanges: Gauge,
    /// Connections the worker is keeping for an upstream to be asked again.
    idle: Gauge,
}

impl WorkerGauges {
    /// What this worker holds now. A gauge counts up and down rather than being told a
    /// number, so what it is given is the difference from what it last said.
    pub(crate) fn holding(&self, exchanges: usize, idle: usize) {
        move_to(&self.exchanges, exchanges);
        move_to(&self.idle, idle);
    }
}

/// Moves a gauge to `now`, a step at a time in whichever direction.
fn move_to(gauge: &Gauge, now: usize) {
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    let mut was = gauge.get();
    while was < now {
        gauge.inc();
        was += 1;
    }
    while was > now {
        gauge.dec();
        was -= 1;
    }
}

impl Metrics {
    pub(crate) fn new(shards: NonZeroUsize, listeners: usize) -> Self {
        Self {
            shards,
            listeners: (0..listeners).map(|_| Sharded::new(shards)).collect(),
            upstreams: (0..UPSTREAM_SLOTS).map(|_| OnceLock::new()).collect(),
            slots: Mutex::default(),
            reloads: Counter::default(),
            last_reload: AtomicU64::new(0),
            stopped: Sharded::new(shards),
            connections: Sharded::new(shards),
            workers: Sharded::new(shards),
        }
    }

    /// Counts an exchange of our own that ended without an answer.
    pub(crate) fn stopped(&self, why: Stopped) {
        if let Some(counter) = self.stopped.local().get(why as usize) {
            counter.inc();
        }
    }

    /// Counts what became of a connection.
    pub(crate) fn socket(&self, what: Socket) {
        if let Some(counter) = self.connections.local().get(what as usize) {
            counter.inc();
        }
    }

    /// This worker's gauges, for it to say what it is holding.
    pub(crate) fn worker(&self) -> &WorkerGauges {
        self.workers.local()
    }

    /// This thread's shard of a listener's counters.
    pub(crate) fn listener(&self, socket: usize) -> Option<&ListenerCounters> {
        self.listeners.get(socket).map(Sharded::local)
    }

    /// This thread's shard of the counters in an upstream's slot.
    pub(crate) fn upstream(&self, slot: usize) -> Option<&UpstreamCounters> {
        self.upstreams.get(slot)?.get().map(Sharded::local)
    }

    /// The slot of the upstream of that name, which it keeps for good. For reloads: it
    /// takes a lock and may allocate.
    pub(crate) fn upstream_slot(&self, name: &str) -> usize {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = match slots.get(name) {
            Some(slot) => *slot,
            None => {
                // Slot zero is the one that is shared, so names start at one.
                let next = slots.len() + 1;
                if next >= self.upstreams.len() {
                    OVERFLOW_SLOT
                } else {
                    slots.insert(name.to_owned(), next);
                    next
                }
            }
        };
        if let Some(series) = self.upstreams.get(slot) {
            series.get_or_init(|| Sharded::new(self.shards));
        }
        slot
    }

    /// The scrape: the listeners by their names, and the upstreams of the current config
    /// by theirs, each with the slot it has.
    pub(crate) fn render(&self, listeners: &[String], upstreams: &[(&str, usize)]) -> String {
        let mut scrape = Exposition::new();
        let listeners = || listeners.iter().zip(&self.listeners);

        let name = "edgerush_listener_connections_accepted_total";
        scrape.family(name, Kind::Counter, "Connections accepted.");
        for (listener, series) in listeners() {
            let labels = [("listener", listener.as_str())];
            scrape.sample(name, &labels, series.sum(|shard| shard.accepted.get()));
        }
        let name = "edgerush_listener_connections_active";
        scrape.family(name, Kind::Gauge, "Connections that are open.");
        for (listener, series) in listeners() {
            let labels = [("listener", listener.as_str())];
            scrape.sample(name, &labels, series.sum(|shard| shard.active.get()));
        }
        let name = "edgerush_listener_accept_errors_total";
        scrape.family(
            name,
            Kind::Counter,
            "Connections that could not be accepted.",
        );
        for (listener, series) in listeners() {
            let labels = [("listener", listener.as_str())];
            scrape.sample(name, &labels, series.sum(|shard| shard.accept_errors.get()));
        }
        let name = "edgerush_listener_responses_total";
        let help = "Responses sent, the upstreams' and the gateway's own, by status class.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for (position, class) in CLASSES.iter().enumerate() {
                let labels = [("listener", listener.as_str()), ("class", class)];
                let count = |shard: &ListenerCounters| {
                    shard.responses.get(position).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_local_answers_total";
        let help = "Responses that are the gateway's own, by the reason for them.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for (position, answer) in Answer::ALL.iter().enumerate() {
                let labels = [("listener", listener.as_str()), ("reason", answer.label())];
                let count =
                    |shard: &ListenerCounters| shard.answers.get(position).map_or(0, Counter::get);
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_time_to_response_head_seconds";
        let help = "From a request's head coming in to its response's head going out; \
                    bodies stream beyond it.";
        scrape.family(name, Kind::Histogram, help);
        for (listener, series) in listeners() {
            // Added up bucket by bucket over the shards.
            let mut counts = [0_u64; HEAD_TIME_BOUNDS.len() + 1];
            for shard in series.shards() {
                for (total, count) in counts.iter_mut().zip(shard.head_time.counts()) {
                    *total = total.wrapping_add(count);
                }
            }
            scrape.histogram(
                name,
                &[("listener", listener.as_str())],
                HEAD_TIME_BOUNDS.map(seconds),
                counts,
                seconds(series.sum(|shard| shard.head_time.sum())),
            );
        }

        // Upstreams that share a series are shown once, under a name of its own.
        let mut shown: Vec<(&str, usize)> = Vec::new();
        for (upstream, slot) in upstreams {
            let upstream = if *slot == OVERFLOW_SLOT {
                OVERFLOW_NAME
            } else {
                upstream
            };
            if !shown.contains(&(upstream, *slot)) {
                shown.push((upstream, *slot));
            }
        }
        let upstreams = || {
            shown
                .iter()
                .filter_map(|(upstream, slot)| Some((*upstream, self.upstreams.get(*slot)?.get()?)))
        };
        let name = "edgerush_upstream_requests_total";
        scrape.family(name, Kind::Counter, "Requests sent to the upstream.");
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.requests.get()));
        }
        let name = "edgerush_upstream_responses_total";
        scrape.family(
            name,
            Kind::Counter,
            "Responses of the upstream, by status class.",
        );
        for (upstream, series) in upstreams() {
            for (position, class) in CLASSES.iter().enumerate() {
                let labels = [("upstream", upstream), ("class", class)];
                let count = |shard: &UpstreamCounters| {
                    shard.responses.get(position).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_upstream_failures_total";
        let help = "Requests the upstream did not answer: not reached, or not in HTTP.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.failures.get()));
        }

        let name = "edgerush_upstream_body_failures_total";
        let help = "Answers whose head was handed on and whose body then failed.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.body_failures.get()));
        }

        let name = "edgerush_upstream_exchanges_stopped_total";
        let help = "Exchanges by EdgeRush's own path that ended without an answer.";
        scrape.family(name, Kind::Counter, help);
        for why in Stopped::ALL {
            let labels = [("reason", why.name())];
            let count = |shard: &[Counter; Stopped::ALL.len()]| shard[why as usize].get();
            scrape.sample(name, &labels, self.stopped.sum(count));
        }

        let name = "edgerush_upstream_connections_total";
        let help = "Connections to upstreams, by what became of each.";
        scrape.family(name, Kind::Counter, help);
        for what in Socket::ALL {
            let labels = [("state", what.name())];
            let count = |shard: &[Counter; 3]| shard[what as usize].get();
            scrape.sample(name, &labels, self.connections.sum(count));
        }

        let name = "edgerush_upstream_exchanges_active";
        let help = "Exchanges a worker has in hand, as its last sweep found them.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(
            name,
            &[],
            self.workers
                .sum(|shard| shard.exchanges.get().max(0).cast_unsigned()),
        );
        let name = "edgerush_upstream_connections_idle";
        let help = "Connections a worker is keeping, as its last sweep found them.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(
            name,
            &[],
            self.workers
                .sum(|shard| shard.idle.get().max(0).cast_unsigned()),
        );

        let name = "edgerush_config_reloads_total";
        scrape.family(name, Kind::Counter, "Configs taken over while running.");
        scrape.sample(name, &[], self.reloads.get());
        let name = "edgerush_config_last_reload_timestamp_seconds";
        let help = "When the last config was taken over; zero if none has been.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(name, &[], self.last_reload.load(Ordering::Relaxed));
        scrape.finish()
    }
}

/// Nanoseconds as seconds, for writing out. What is lost beyond 2⁵³ does not show.
fn seconds(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1e9
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Metrics {
        Metrics::new(NonZeroUsize::new(4).unwrap(), 2)
    }

    #[test]
    fn a_status_is_counted_in_its_class() {
        for (status, class) in [
            (100, 0),
            (200, 1),
            (204, 1),
            (301, 2),
            (404, 3),
            (599, 4),
            (999, 4),
        ] {
            assert_eq!(
                class_of(StatusCode::from_u16(status).unwrap()),
                class,
                "{status}"
            );
        }
    }

    #[test]
    fn every_rejection_of_the_core_is_an_answer_with_the_same_status() {
        use crate::{ConnectionError, HostError};
        use edgerush_router::NormaliseError;
        for rejection in [
            Rejection::Host(HostError::Missing),
            Rejection::Path(NormaliseError::Backslash),
            Rejection::Connection(ConnectionError::Malformed),
            Rejection::Target,
            Rejection::NoRoute,
            Rejection::NoBackend,
        ] {
            assert_eq!(
                Answer::from(rejection).status(),
                rejection.status(),
                "{rejection:?}"
            );
        }
        assert_eq!(Answer::NoEndpoints.status(), 503);
        assert_eq!(Answer::UpstreamFailed.status(), 502);
    }

    #[test]
    fn answers_have_labels_of_their_own() {
        let mut labels: Vec<&str> = Answer::ALL.iter().map(|answer| answer.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), Answer::ALL.len());
    }

    #[test]
    fn an_upstream_keeps_the_slot_its_name_was_given() {
        let metrics = metrics();
        let cart = metrics.upstream_slot("cart");
        let pages = metrics.upstream_slot("pages");
        assert_ne!(cart, pages);
        assert_ne!(cart, OVERFLOW_SLOT);
        assert_eq!(metrics.upstream_slot("cart"), cart);
        assert!(metrics.upstream(cart).is_some());
        assert!(metrics.upstream(UPSTREAM_SLOTS - 1).is_none());
        assert!(metrics.upstream(UPSTREAM_SLOTS).is_none());
    }

    #[test]
    fn upstreams_beyond_the_slots_share_one_series() {
        let metrics = metrics();
        let slots: Vec<usize> = (0..UPSTREAM_SLOTS + 10)
            .map(|upstream| metrics.upstream_slot(&format!("upstream-{upstream}")))
            .collect();
        let own = slots.iter().filter(|slot| **slot != OVERFLOW_SLOT).count();
        assert_eq!(own, UPSTREAM_SLOTS - 1);
        assert_eq!(slots.last(), Some(&OVERFLOW_SLOT));
        // The names that came first still have their own.
        assert_eq!(metrics.upstream_slot("upstream-0"), slots[0]);

        metrics.upstream(OVERFLOW_SLOT).unwrap().requests.inc();
        let scrape = metrics.render(&[], &[("late-a", OVERFLOW_SLOT), ("late-b", OVERFLOW_SLOT)]);
        let line = "edgerush_upstream_requests_total{upstream=\"_overflow\"} 1\n";
        assert_eq!(scrape.matches(line).count(), 1, "{scrape}");
        assert!(!scrape.contains("late-a"), "{scrape}");
    }

    #[test]
    fn a_scrape_shows_what_was_counted() {
        let metrics = metrics();
        let listeners = ["admin".to_owned(), "web".to_owned()];
        let cart = metrics.upstream_slot("cart");

        let web = metrics.listener(1).unwrap();
        web.accepted.inc();
        web.active.inc();
        web.responded(StatusCode::OK, 700_000);
        web.responded(StatusCode::NOT_FOUND, 90_000);
        web.answered(Answer::NoRoute);
        let upstream = metrics.upstream(cart).unwrap();
        upstream.requests.inc();
        upstream.responded(StatusCode::OK);
        metrics.reloads.inc();

        let scrape = metrics.render(&listeners, &[("cart", cart)]);
        for line in [
            "# TYPE edgerush_listener_responses_total counter\n",
            "edgerush_listener_connections_accepted_total{listener=\"web\"} 1\n",
            "edgerush_listener_connections_accepted_total{listener=\"admin\"} 0\n",
            "edgerush_listener_connections_active{listener=\"web\"} 1\n",
            "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"} 1\n",
            "edgerush_listener_responses_total{listener=\"web\",class=\"4xx\"} 1\n",
            "edgerush_listener_responses_total{listener=\"web\",class=\"5xx\"} 0\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"no_route\"} 1\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"bad_host\"} 0\n",
            "edgerush_listener_time_to_response_head_seconds_bucket{listener=\"web\",le=\"0.0005\"} 1\n",
            "edgerush_listener_time_to_response_head_seconds_bucket{listener=\"web\",le=\"0.001\"} 2\n",
            "edgerush_listener_time_to_response_head_seconds_bucket{listener=\"web\",le=\"+Inf\"} 2\n",
            "edgerush_listener_time_to_response_head_seconds_sum{listener=\"web\"} 0.00079\n",
            "edgerush_listener_time_to_response_head_seconds_count{listener=\"web\"} 2\n",
            "edgerush_upstream_requests_total{upstream=\"cart\"} 1\n",
            "edgerush_upstream_responses_total{upstream=\"cart\",class=\"2xx\"} 1\n",
            "edgerush_upstream_failures_total{upstream=\"cart\"} 0\n",
            "edgerush_config_reloads_total 1\n",
            "edgerush_config_last_reload_timestamp_seconds 0\n",
        ] {
            assert!(scrape.contains(line), "{line} is not in\n{scrape}");
        }
    }

    #[test]
    fn what_is_counted_on_other_threads_shows_too() {
        let metrics = std::sync::Arc::new(metrics());
        let threads: Vec<_> = (0..6)
            .map(|_| {
                let metrics = std::sync::Arc::clone(&metrics);
                std::thread::spawn(move || {
                    metrics
                        .listener(0)
                        .unwrap()
                        .responded(StatusCode::OK, 1_000);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let scrape = metrics.render(&["web".to_owned()], &[]);
        let line = "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"} 6\n";
        assert!(scrape.contains(line), "{scrape}");
    }

    #[test]
    fn an_upstream_that_the_config_no_longer_has_is_not_shown_and_not_forgotten() {
        let metrics = metrics();
        let cart = metrics.upstream_slot("cart");
        metrics.upstream(cart).unwrap().requests.add(5);
        assert!(!metrics.render(&[], &[]).contains("cart"));
        let again = metrics.upstream_slot("cart");
        let scrape = metrics.render(&[], &[("cart", again)]);
        assert!(scrape.contains("edgerush_upstream_requests_total{upstream=\"cart\"} 5\n"));
    }
}
