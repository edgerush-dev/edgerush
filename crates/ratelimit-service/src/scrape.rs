//! A pod of the store's (`cluster`) metrics, either mode's, served at `/metrics` where its file
//! says, in the text format a data plane's scrape uses: whether it serves, its keys and what
//! they hold against what they may (06 §8), what its mode counts, its peers' clocks, what it
//! dropped and the table it follows; and at `/ready` whether it is ready, for a readiness probe.

use crate::cluster::mode::{DecidedFromCopy, Mode};
use crate::cluster::pod::{Observed, StorePod};
use bytes::Bytes;
use edgerush_cluster_asynchronous::TurnedAway;
use edgerush_control_service::accept;
use edgerush_telemetry::{Exposition, Kind};
use http::{HeaderValue, Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Scrapes served at once: more are closed as they come, as the data plane's are.
const MOST: usize = 10;

/// How long a scrape has, from its connection to its answer sent.
const WITHIN: Duration = Duration::from_secs(5);

/// What a store pod's mode counts of itself, as its scrape shows it.
pub trait Counted {
    /// Its families and samples, added to `scrape`.
    fn render(&self, scrape: &mut Exposition);
}

impl Counted for DecidedFromCopy {
    fn render(&self, scrape: &mut Exposition) {
        let name = "edgerush_rate_limit_decided_from_copy_total";
        let help = "Requests from data planes the pod decided from its copy of the key, the \
                    key's owner unheard for the store's stop_serving_ms, rather than forward \
                    them.";
        scrape.family(name, Kind::Counter, help);
        scrape.sample(name, &[], self.decided_from_copy);
    }
}

impl Counted for TurnedAway {
    fn render(&self, scrape: &mut Exposition) {
        let whys = [
            ("elsewhere", self.elsewhere),
            ("not_serving", self.not_serving),
        ];
        let name = "edgerush_rate_limit_turned_away_entries_total";
        let help = "Entries of data planes' batches the pod applied none of, by why: its table \
                    names another pod their key's owner, or none; or names it, while it is not \
                    serving. Each data plane sends them again to the owner its table names.";
        scrape.family(name, Kind::Counter, help);
        for (why, turned) in whys {
            scrape.sample(name, &[("why", why)], turned.entries);
        }
        let name = "edgerush_rate_limit_turned_away_hits_total";
        let help = "The hits those entries reported, by why.";
        scrape.family(name, Kind::Counter, help);
        for (why, turned) in whys {
            scrape.sample(name, &[("why", why)], turned.hits);
        }
    }
}

/// The text of a store pod's scrape, of what `observed` says of it: whether it serves, its keys
/// and those it owns against their cap, what its mode counts, its peers' clocks, what its
/// sending dropped, and the table it follows.
#[must_use]
pub fn render_store<C: Counted>(observed: Observed<C>) -> String {
    let mut scrape = Exposition::new();
    let name = "edgerush_rate_limit_pod_serving";
    let help = "1 while the pod serves data planes: its table names it and it holds its lease.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], u8::from(observed.ready));
    let name = "edgerush_rate_limit_key_bytes";
    let help = "Bytes the keys hold, owned and copies, as an estimate of their own data and \
                of their places in the schedule of when each may be forgotten.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], observed.key_bytes);
    if let Some(most) = observed.max_key_bytes {
        let name = "edgerush_rate_limit_max_key_bytes";
        scrape.family(name, Kind::Gauge, "Bytes the keys may hold.");
        scrape.sample(name, &[], most);
    }
    let name = "edgerush_rate_limit_keys";
    scrape.family(name, Kind::Gauge, "Keys held, owned and copies.");
    scrape.sample(name, &[], observed.keys);
    let name = "edgerush_rate_limit_keys_owned";
    let help = "Keys held of the shards the table the pod follows names it the owner of.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], observed.owned_keys);
    let name = "edgerush_rate_limit_keys_evicted_total";
    let help = "Keys evicted, those that may be forgotten soonest first, to keep within the \
                bytes they may hold; each starts again full.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], observed.evicted);
    let name = "edgerush_rate_limit_keys_forgotten_total";
    let help = "Keys forgotten, their time come: full again, or their window over.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], observed.forgotten);
    observed.mode.render(&mut scrape);
    let name = "edgerush_rate_limit_peers_skewed";
    let help = "Peers whose wall clocks are off from the pod's past the store's skew warning.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], observed.skewed);
    let name = "edgerush_rate_limit_messages_dropped_total";
    let help = "Messages to other pods dropped, by why: past what their queue holds, their \
                connection lost or the peer's address unknown, or longer than a receiver takes.";
    scrape.family(name, Kind::Counter, help);
    let dropped = observed.dropped;
    for (why, count) in [
        ("full", dropped.full),
        ("lost", dropped.lost),
        ("too_long", dropped.too_long),
    ] {
        scrape.sample(name, &[("why", why)], count);
    }
    let name = "edgerush_rate_limit_leader";
    scrape.family(
        name,
        Kind::Gauge,
        "1 while the pod leads the store's cluster.",
    );
    scrape.sample(name, &[], u8::from(observed.leader));
    let name = "edgerush_rate_limit_table_term";
    let help = "The term of the table the pod has committed, 0 for none: its generation in \
                the upper half.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], observed.table.0);
    let name = "edgerush_rate_limit_table_version";
    let help = "The version of the table the pod has committed, rising with every table.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], observed.table.1);
    scrape.finish()
}

/// Serves a store `pod`'s scrape on `listener` for as long as it runs, `GET /metrics`, each
/// connection one scrape, at most ten at once, each given five seconds; and its readiness: `GET
/// /ready`, 200 while its store has it ready and 503 while not, for a readiness probe.
pub async fn serve_store<M: Mode<Counted: Counted>>(listener: TcpListener, pod: StorePod<M>) {
    serve_with(listener, move |asked| {
        let pod = pod.clone();
        async move {
            match (asked, pod.observed().await) {
                (_, None) => Said::Stopped,
                (Asked::Metrics, Some(observed)) => Said::Scrape(render_store(observed)),
                (Asked::Ready, Some(observed)) => Said::Ready(observed.ready),
            }
        }
    })
    .await;
}

/// What a request to the listener asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Asked {
    /// `GET /metrics`.
    Metrics,
    /// `GET /ready`.
    Ready,
}

/// What a pod says to a request.
enum Said {
    /// Its scrape.
    Scrape(String),
    /// Whether it is ready.
    Ready(bool),
    /// Nothing: it has stopped.
    Stopped,
}

/// Serves on `listener` what `said` says to each request, nothing once its pod has stopped:
/// `GET /metrics` and `GET /ready`, each connection one request, at most
/// ten at once, each given five seconds.
async fn serve_with<F, S>(listener: TcpListener, said: F)
where
    F: Fn(Asked) -> S + Clone + Send + Sync + 'static,
    S: Future<Output = Said> + Send,
{
    let room = Arc::new(Semaphore::new(MOST));
    loop {
        // A connection that cannot be taken, out of files most likely, is tried again only
        // after a pause, as on the pod's gRPC listener.
        let (stream, _) = accept::next(|| listener.accept()).await;
        // Past the most at once, closed unanswered.
        let Ok(permit) = Arc::clone(&room).try_acquire_owned() else {
            continue;
        };
        let said = said.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let answering = service_fn(move |request: Request<Incoming>| {
                let said = said.clone();
                async move { Ok::<_, Infallible>(answer(&request, said).await) }
            });
            let mut http1 = hyper::server::conn::http1::Builder::new();
            http1
                .timer(TokioTimer::new())
                .header_read_timeout(WITHIN)
                .keep_alive(false);
            let serving = http1.serve_connection(TokioIo::new(stream), answering);
            let _ended = tokio::time::timeout(WITHIN, serving).await;
        });
    }
}

/// The answer to `request`, as `said` says: the scrape for `GET /metrics`, 200 or 503 for `GET
/// /ready` as the pod is ready or not, 404 for anything else, 503 if the pod has stopped.
async fn answer<F, S>(request: &Request<Incoming>, said: F) -> Response<Full<Bytes>>
where
    F: Fn(Asked) -> S,
    S: Future<Output = Said>,
{
    let mut response = Response::new(Full::new(Bytes::new()));
    let asked = match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => Asked::Metrics,
        (&Method::GET, "/ready") => Asked::Ready,
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
            return response;
        }
    };
    match said(asked).await {
        Said::Scrape(scrape) => {
            *response.body_mut() = Full::new(Bytes::from(scrape));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            );
        }
        Said::Ready(true) => *response.body_mut() = Full::new(Bytes::from_static(b"ready\n")),
        Said::Ready(false) | Said::Stopped => {
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::link::Dropped;
    use edgerush_cluster_asynchronous::Turned;

    #[test]
    fn a_store_pods_scrape_says_whether_it_serves_and_what_it_holds_drops_and_follows() {
        let observed = Observed {
            ready: true,
            leader: false,
            table: (4_294_967_297, 12),
            keys: 10,
            owned_keys: 7,
            key_bytes: 9_710,
            max_key_bytes: Some(1 << 20),
            forgotten: 5,
            evicted: 4,
            skewed: 1,
            dropped: Dropped {
                full: 3,
                lost: 2,
                too_long: 1,
            },
            mode: DecidedFromCopy {
                decided_from_copy: 6,
            },
        };
        let scrape = render_store(observed);
        for line in [
            "edgerush_rate_limit_pod_serving 1\n",
            "edgerush_rate_limit_key_bytes 9710\n",
            "edgerush_rate_limit_max_key_bytes 1048576\n",
            "edgerush_rate_limit_keys 10\n",
            "edgerush_rate_limit_keys_owned 7\n",
            "edgerush_rate_limit_keys_evicted_total 4\n",
            "edgerush_rate_limit_keys_forgotten_total 5\n",
            "edgerush_rate_limit_decided_from_copy_total 6\n",
            "edgerush_rate_limit_peers_skewed 1\n",
            "edgerush_rate_limit_messages_dropped_total{why=\"full\"} 3\n",
            "edgerush_rate_limit_messages_dropped_total{why=\"lost\"} 2\n",
            "edgerush_rate_limit_messages_dropped_total{why=\"too_long\"} 1\n",
            "edgerush_rate_limit_leader 0\n",
            "edgerush_rate_limit_table_term 4294967297\n",
            "edgerush_rate_limit_table_version 12\n",
        ] {
            assert!(scrape.contains(line), "{line}{scrape}");
        }
    }

    /// An asynchronous pod's scrape says the entries it turned away and their hits, by why,
    /// and nothing of the synchronous mode's copies.
    #[test]
    fn an_asynchronous_pods_scrape_says_what_it_turned_away_by_why() {
        let scrape = render_store(Observed {
            mode: TurnedAway {
                elsewhere: Turned {
                    entries: 3,
                    hits: 7,
                },
                not_serving: Turned {
                    entries: 1,
                    hits: 2,
                },
            },
            ..Observed::default()
        });
        for line in [
            "edgerush_rate_limit_turned_away_entries_total{why=\"elsewhere\"} 3\n",
            "edgerush_rate_limit_turned_away_entries_total{why=\"not_serving\"} 1\n",
            "edgerush_rate_limit_turned_away_hits_total{why=\"elsewhere\"} 7\n",
            "edgerush_rate_limit_turned_away_hits_total{why=\"not_serving\"} 2\n",
            "edgerush_rate_limit_pod_serving 0\n",
        ] {
            assert!(scrape.contains(line), "{line}{scrape}");
        }
        assert!(!scrape.contains("decided_from_copy"), "{scrape}");
    }

    #[test]
    fn a_store_pod_leading_not_ready_with_no_cap_or_table_says_so() {
        let scrape = render_store(Observed::<DecidedFromCopy> {
            leader: true,
            ..Observed::default()
        });
        for line in [
            "edgerush_rate_limit_pod_serving 0\n",
            "edgerush_rate_limit_leader 1\n",
            "edgerush_rate_limit_table_term 0\n",
            "edgerush_rate_limit_table_version 0\n",
        ] {
            assert!(scrape.contains(line), "{line}{scrape}");
        }
        assert!(!scrape.contains("max_key_bytes"), "{scrape}");
    }
}
