//! A service pod's metrics, served at `/metrics` where `--metrics` says: what its keys hold
//! against what they may (06 §8), in the text format a data plane's scrape uses.

use crate::pod::{Pod, Usage};
use bytes::Bytes;
use edgerush_telemetry::{Exposition, Kind};
use http::{HeaderValue, Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::convert::Infallible;
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

/// The text of a pod's scrape: what its keys hold, as `usage` says, against
/// `max_key_bytes`.
#[must_use]
pub fn render(usage: Usage, max_key_bytes: u64) -> String {
    let mut scrape = Exposition::new();
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
    scrape.finish()
}

/// Serves `pod`'s scrape on `listener` for as long as it runs: `GET /metrics`, each
/// connection one scrape, at most ten at once, each given five seconds.
pub async fn serve(listener: TcpListener, pod: Pod, max_key_bytes: u64) {
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
        let pod = pod.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let answering = service_fn(move |request: Request<Incoming>| {
                let pod = pod.clone();
                async move { Ok::<_, Infallible>(answer(&request, &pod, max_key_bytes).await) }
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

/// The answer to `request`: the scrape for `GET /metrics`, 404 for anything else, 503 if
/// the pod has stopped.
async fn answer(
    request: &Request<Incoming>,
    pod: &Pod,
    max_key_bytes: u64,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    if request.method() != Method::GET || request.uri().path() != "/metrics" {
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    }
    let Some(usage) = pod.usage().await else {
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        return response;
    };
    *response.body_mut() = Full::new(Bytes::from(render(usage, max_key_bytes)));
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
    fn a_scrape_says_what_the_keys_hold_against_their_bound() {
        let usage = Usage {
            key_bytes: 9_710,
            keys: 10,
            evicted: 4,
        };
        let scrape = render(usage, 1 << 20);
        for line in [
            "edgerush_rate_limit_key_bytes 9710\n",
            "edgerush_rate_limit_max_key_bytes 1048576\n",
            "edgerush_rate_limit_keys 10\n",
            "edgerush_rate_limit_keys_evicted_total 4\n",
        ] {
            assert!(scrape.contains(line), "{line}{scrape}");
        }
    }
}
