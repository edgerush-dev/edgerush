//! The rate limits route rules are under, over HTTP/2 and HTTP/3 as over HTTP/1.1
//! (`tests/ratelimit.rs`): the request core decides them whatever a request came by, with
//! three service pods on real sockets, and refuses what is over with 429.

use super::*;
use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client as PodClient, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use tonic::transport::server::TcpIncoming;

/// Three service pods, once they agree all three serve: their addresses.
async fn service() -> Vec<String> {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let timing = Timing {
        cycle: Duration::from_millis(10),
        heartbeat: Duration::from_millis(20),
        silence: Duration::from_millis(150),
        settle: Duration::from_millis(60),
    };
    let mut pods = Vec::new();
    for (id, listener) in (0..3).zip(listeners) {
        let peers: BTreeMap<PodId, PodClient> = (0..3)
            .filter(|pod| *pod != id)
            .map(|pod| (pod, PodClient::new(&addresses[pod as usize]).unwrap()))
            .collect();
        let pod = Pod::spawn(id, vec![0, 1, 2], 1, timing, peers);
        let server = Server::new(pod.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(server, TcpIncoming::from(listener))
                .await
                .unwrap();
        });
        pods.push(pod);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    for pod in &pods {
        while !matches!(pod.view().await, Some((view, true)) if view.pods == [0, 1, 2]) {
            assert!(Instant::now() < deadline, "the pods never agreed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    // A pod taken in makes no key afresh for three heartbeats, in case one is handed to it.
    tokio::time::sleep(Duration::from_millis(80)).await;
    addresses
}

/// `config` with its first route's first rule under ten requests an hour for each client,
/// a burst of ten, shared through `service`.
fn limited(mut config: Config, service: &[String]) -> Config {
    config.routes[0].rules[0].rate_limits = vec!["per-client".to_owned()];
    config.limits = Some(
        serde_saphyr::from_str(
            "limits:\n  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }\n",
        )
        .unwrap(),
    );
    config.rate_limit_service = Some(edgerush_config::RateLimitService {
        pods: service.to_vec(),
        cycle_ms: Some(10),
    });
    config
}

fn tally(statuses: &[u16]) -> (usize, usize) {
    (
        statuses.iter().filter(|s| **s == 200).count(),
        statuses.iter().filter(|s| **s == 429).count(),
    )
}

/// Fifteen requests over HTTP/2 from one client, on one connection: ten admitted, the
/// rest refused 429, saying when to come back; then a gRPC call, refused as gRPC is.
#[tokio::test]
async fn http2_requests_are_held_to_their_limit() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let config = limited(everything_config(upstream), &service);
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for _ in 0..15 {
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    assert_eq!(response.headers()["retry-after"], "360");
                    assert_eq!(response.headers()["ratelimit"], r#""per-client";r=0;t=360"#);
                }
                statuses.push(response.status().as_u16());
            }
            assert_eq!(tally(&statuses), (10, 5), "{statuses:?}");
            // A gRPC call is told its status, and nothing of when to come back.
            send = within(send.ready()).await.unwrap();
            let (answer, _) = send.send_request(grpc_call("/", None), true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.headers()["grpc-status"], "8");
            assert!(answer.headers().get("retry-after").is_none());
            assert!(answer.headers().get("ratelimit").is_none());
        })
        .await;
}

/// Fifteen requests over HTTP/3 from one client, on one connection: ten admitted, the
/// rest refused 429.
#[tokio::test]
async fn http3_requests_are_held_to_their_limit() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client as H3Client;
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let config = limited(h3_config(upstream, http3), &service);
            let (front, _) = serving_h3(&config).await;
            let mut client = H3Client::connect(front, "a.test").await;
            let mut statuses = Vec::new();
            for _ in 0..15 {
                let answer = client.get("a.test", "/").await;
                let status = answer.final_status().and_then(|s| s.parse().ok());
                statuses.push(status.unwrap_or(0));
            }
            assert_eq!(tally(&statuses), (10, 5), "{statuses:?}");
        })
        .await;
}
