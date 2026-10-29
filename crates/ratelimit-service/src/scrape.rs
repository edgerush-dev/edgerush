//! A service pod's metrics, served at `/metrics` where `--metrics` says, in the text format
//! a data plane's scrape uses: whether it serves, and how often and how long it has gone
//! without hearing a majority (06 §4), in either mode; and for the asynchronous mode's
//! pod, what its keys hold against what they may (06 §8).

use crate::pod::{Pod, Usage};
use crate::synchronous::SyncPod;
use bytes::Bytes;
use edgerush_ratelimit::service::membership::Sight;
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

/// How long to wait after an accept that failed for want of something the whole process
/// lacks, such as file descriptors, before trying again.
const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

/// The text of an asynchronous pod's scrape: what its keys hold, as `usage` says, against
/// `max_key_bytes`, and whether it is `serving` and how it hears the others, as `sight`
/// says.
#[must_use]
pub fn render(usage: Usage, max_key_bytes: u64, serving: bool, sight: Sight) -> String {
    let mut scrape = Exposition::new();
    hearing(&mut scrape, serving, sight);
    let name = "edgerush_rate_limit_key_bytes";
    let help = "Bytes the keys hold, owned and copies, as an estimate of their own data.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], usage.key_bytes);
    let name = "edgerush_rate_limit_max_key_bytes";
    scrape.family(name, Kind::Gauge, "Bytes the keys may hold.");
    scrape.sample(name, &[], max_key_bytes);
    let name = "edgerush_rate_limit_keys";
    scrape.family(name, Kind::Gauge, "Keys held, owned and copies.");
    scrape.sample(name, &[], usage.keys);
    let name = "edgerush_rate_limit_keys_evicted_total";
    let help = "Keys evicted, least recently changed first, to keep within the bytes they \
                may hold; each starts again full.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], usage.evicted);
    let name = "edgerush_rate_limit_copies_too_long_total";
    let help = "Copies of keys not sent to another pod, one alone longer than a message \
                takes: its records of data planes too many.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], usage.copies_too_long);
    scrape.finish()
}

/// The text of a synchronous pod's scrape: whether it is `serving`, and how it hears the
/// others, as `sight` says.
#[must_use]
pub fn render_synchronous(serving: bool, sight: Sight) -> String {
    let mut scrape = Exposition::new();
    hearing(&mut scrape, serving, sight);
    scrape.finish()
}

/// Whether a pod serves, and how often and how long it has gone without a majority.
fn hearing(scrape: &mut Exposition, serving: bool, sight: Sight) {
    let name = "edgerush_rate_limit_pod_serving";
    let help = "1 while the pod serves data planes: the view takes it in and it hears a \
                majority of the pods.";
    scrape.family(name, Kind::Gauge, help);
    scrape.sample(name, &[], u8::from(serving));
    let name = "edgerush_rate_limit_majority_lost_total";
    let help = "Times the pod lost sight of a majority of the pods, after hearing one, and \
                stopped serving.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], sight.lost);
    let name = "edgerush_rate_limit_majority_regained_total";
    let help = "Times the pod heard a majority of the pods again, after losing sight of one.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], sight.regained);
    let name = "edgerush_rate_limit_without_majority_seconds_total";
    let help = "Time the pod has gone without hearing a majority of the pods, from each \
                time it stopped serving.";
    scrape.family(name, Kind::Counter, help);
    scrape.sample(name, &[], Duration::from_nanos(sight.without).as_secs_f64());
}

/// Serves an asynchronous `pod`'s scrape on `listener` for as long as it runs: `GET
/// /metrics`, each connection one scrape, at most ten at once, each given five seconds.
pub async fn serve(listener: TcpListener, pod: Pod, max_key_bytes: u64) {
    serve_with(listener, move || {
        let pod = pod.clone();
        async move {
            let usage = pod.usage().await?;
            let (serving, sight) = pod.sight().await?;
            Some(render(usage, max_key_bytes, serving, sight))
        }
    })
    .await;
}

/// Serves a synchronous `pod`'s scrape on `listener` for as long as it runs, as [`serve`]
/// serves an asynchronous pod's.
pub async fn serve_synchronous(listener: TcpListener, pod: SyncPod) {
    serve_with(listener, move || {
        let pod = pod.clone();
        async move { Some(render_synchronous(pod.view().1, pod.sight())) }
    })
    .await;
}

/// Serves on `listener` the scrape `scraped` gives, none once its pod has stopped: `GET
/// /metrics`, each connection one scrape, at most ten at once, each given five seconds.
async fn serve_with<F, S>(listener: TcpListener, scraped: F)
where
    F: Fn() -> S + Clone + Send + Sync + 'static,
    S: Future<Output = Option<String>> + Send,
{
    let room = Arc::new(Semaphore::new(MOST));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(_) => {
                tokio::time::sleep(ACCEPT_PAUSE).await;
                continue;
            }
        };
        // Past the most at once, closed unanswered.
        let Ok(permit) = Arc::clone(&room).try_acquire_owned() else {
            continue;
        };
        let scraped = scraped.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let answering = service_fn(move |request: Request<Incoming>| {
                let scraped = scraped.clone();
                async move { Ok::<_, Infallible>(answer(&request, scraped).await) }
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

/// The answer to `request`: the scrape `scraped` gives for `GET /metrics`, 404 for anything
/// else, 503 if the pod has stopped.
async fn answer<F, S>(request: &Request<Incoming>, scraped: F) -> Response<Full<Bytes>>
where
    F: Fn() -> S,
    S: Future<Output = Option<String>>,
{
    let mut response = Response::new(Full::new(Bytes::new()));
    if request.method() != Method::GET || request.uri().path() != "/metrics" {
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    }
    let Some(scrape) = scraped().await else {
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        return response;
    };
    *response.body_mut() = Full::new(Bytes::from(scrape));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_synchronous_scrape_says_whether_the_pod_serves_and_how_it_hears() {
        let scrape = render_synchronous(false, Sight::default());
        for line in [
            "edgerush_rate_limit_pod_serving 0\n",
            "edgerush_rate_limit_majority_lost_total 0\n",
            "edgerush_rate_limit_majority_regained_total 0\n",
            "edgerush_rate_limit_without_majority_seconds_total 0\n",
        ] {
            assert!(scrape.contains(line), "{line}{scrape}");
        }
        assert!(!scrape.contains("key_bytes"), "{scrape}");
    }

    #[test]
    fn a_scrape_says_what_the_keys_hold_against_their_bound() {
        let usage = Usage {
            key_bytes: 9_710,
            keys: 10,
            evicted: 4,
            copies_too_long: 2,
        };
        let sight = Sight {
            lost: 3,
            regained: 2,
            without: 1_250_000_000,
        };
        let scrape = render(usage, 1 << 20, true, sight);
        for line in [
            "edgerush_rate_limit_pod_serving 1\n",
            "edgerush_rate_limit_majority_lost_total 3\n",
            "edgerush_rate_limit_majority_regained_total 2\n",
            "edgerush_rate_limit_without_majority_seconds_total 1.25\n",
            "edgerush_rate_limit_key_bytes 9710\n",
            "edgerush_rate_limit_max_key_bytes 1048576\n",
            "edgerush_rate_limit_keys 10\n",
            "edgerush_rate_limit_keys_evicted_total 4\n",
            "edgerush_rate_limit_copies_too_long_total 2\n",
        ] {
            assert!(scrape.contains(line), "{line}{scrape}");
        }
    }
}
