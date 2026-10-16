//! The rate limits route rules are under, end to end: the engine's limits named by routes,
//! the data plane's worker deciding each request under all of them at once as a data plane
//! of the rate-limiting service, three service pods on real sockets, and clients told apart
//! by their loopback addresses.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_config::{Config, compile};
use edgerush_proxy::{H1Limits, Proxy, Worker};
use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tonic::transport::server::TcpIncoming;

/// An upstream that answers every request `200 ok`, keeping its connections.
async fn upstream() -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0; 4096];
                loop {
                    while let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        buffer.drain(..end + 4);
                        if stream
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                    }
                }
            });
        }
    });
    address
}

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
        let peers: BTreeMap<PodId, Client> = (0..3)
            .filter(|pod| *pod != id)
            .map(|pod| (pod, Client::new(&addresses[pod as usize]).unwrap()))
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

/// The engine's limits: ten requests an hour for each client on `/by-client`, three POSTs
/// under `/api/orders` on `/api`, and twenty over the whole data plane.
const LIMITS: &str = r"
global: { rate_limits: [everything] }
limits:
  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }
  - { name: writes, key: client, match: { methods: [POST], path_prefix: /api/orders }, model: { bucket: { rate: 3/h, burst: 3 } } }
  - { name: everything, key: client, model: { bucket: { rate: 20/h, burst: 20 } } }
";

/// A data plane of one worker under [`LIMITS`], and where it listens.
async fn proxy(upstream: SocketAddr, service: &[String]) -> SocketAddr {
    let pods = service
        .iter()
        .map(|address| format!("\"{address}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
routes:
  - name: limited
    listeners: [web]
    hostnames: [{{ name: "*", falls_through: true }}]
    rules:
      - matches: [{{ path: {{ prefix: /by-client }} }}]
        forward: {{ backends: [{{ upstream: web, weight: 1 }}] }}
        rate_limits: [per-client]
      - matches: [{{ path: {{ prefix: /api }} }}]
        forward: {{ backends: [{{ upstream: web, weight: 1 }}] }}
        rate_limits: [writes]
      - matches: [{{ path: {{ prefix: / }} }}]
        forward: {{ backends: [{{ upstream: web, weight: 1 }}] }}
upstreams:
  web: {{ load_balancer: p2c, endpoints: ["{upstream}"] }}
rate_limit_service: {{ pods: [{pods}], cycle_ms: 10 }}
"#
    );
    let mut config: Config = serde_saphyr::from_str(&yaml).unwrap();
    config.limits = Some(serde_saphyr::from_str(LIMITS).unwrap());
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let worker = Worker::with_limits(proxy, H1Limits::default());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
    let _sweeping = tokio::task::spawn_local(worker.maintain());
    front
}

/// The status of `method path` through `stream`.
async fn ask(stream: &mut TcpStream, method: &str, path: &str) -> u16 {
    let request =
        format!("{method} {path} HTTP/1.1\r\nhost: example.com\r\ncontent-length: 0\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        assert_eq!(stream.read(&mut byte).await.unwrap(), 1, "the answer ended");
        head.push(byte[0]);
    }
    let text = String::from_utf8_lossy(&head).to_lowercase();
    let length: usize = text
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map_or(0, |value| value.trim().parse().unwrap());
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    text[9..12].parse().unwrap()
}

/// The statuses of `count` requests `method path` from the client at `client`, a loopback
/// address of its own, on one connection.
async fn statuses(
    front: SocketAddr,
    client: [u8; 4],
    method: &str,
    path: &str,
    count: usize,
) -> Vec<u16> {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(SocketAddr::from((client, 0))).unwrap();
    let mut stream = socket.connect(front).await.unwrap();
    let mut out = Vec::new();
    for _ in 0..count {
        out.push(ask(&mut stream, method, path).await);
    }
    out
}

fn tally(statuses: &[u16]) -> (usize, usize) {
    (
        statuses.iter().filter(|s| **s == 200).count(),
        statuses.iter().filter(|s| **s == 429).count(),
    )
}

/// `test` run against a data plane on three service pods.
async fn with_proxy<F: Future<Output = ()>>(test: impl FnOnce(SocketAddr) -> F) {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let front = proxy(upstream().await, &service).await;
            tokio::time::timeout(Duration::from_secs(20), test(front))
                .await
                .expect("answered in time");
        })
        .await;
}

#[tokio::test]
async fn a_rule_limits_each_client_to_its_bucket_and_refuses_the_rest_with_429() {
    with_proxy(|front| async move {
        let one = statuses(front, [127, 0, 0, 1], "GET", "/by-client", 15).await;
        assert_eq!(tally(&one), (10, 5), "{one:?}");
        let other = statuses(front, [127, 0, 0, 2], "GET", "/by-client", 3).await;
        assert_eq!(
            tally(&other),
            (3, 0),
            "another client has a bucket of its own"
        );
    })
    .await;
}

#[tokio::test]
async fn a_limit_narrower_than_its_rule_counts_only_what_it_matches() {
    with_proxy(|front| async move {
        let client = [127, 0, 0, 3];
        let reads = statuses(front, client, "GET", "/api/orders", 5).await;
        assert_eq!(tally(&reads), (5, 0), "not a POST");
        let writes = statuses(front, client, "POST", "/api/orders/7", 5).await;
        assert_eq!(tally(&writes), (3, 2), "{writes:?}");
        let elsewhere = statuses(front, client, "POST", "/api/other", 5).await;
        assert_eq!(tally(&elsewhere), (5, 0), "not under its path");
    })
    .await;
}

/// A request under two limits is charged to both or to neither: the five `per-client`
/// refuses leave the data plane's limit ten requests to give, not five.
#[tokio::test]
async fn a_request_is_charged_to_all_its_limits_or_none() {
    with_proxy(|front| async move {
        let client = [127, 0, 0, 4];
        let limited = statuses(front, client, "GET", "/by-client", 15).await;
        assert_eq!(tally(&limited), (10, 5), "{limited:?}");
        let rest = statuses(front, client, "GET", "/elsewhere", 15).await;
        assert_eq!(tally(&rest), (10, 5), "{rest:?}");
    })
    .await;
}

/// Two data planes share a client's bucket through the service: what one admitted counts
/// against the other.
#[tokio::test]
async fn two_data_planes_share_a_key_through_the_service() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let upstream = upstream().await;
            let first = proxy(upstream, &service).await;
            let second = proxy(upstream, &service).await;
            let client = [127, 0, 0, 5];
            let one = statuses(first, client, "GET", "/by-client", 6).await;
            // The first's admissions reach the owner within a cycle or two.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let other = statuses(second, client, "GET", "/by-client", 8).await;
            let admitted = tally(&one).0 + tally(&other).0;
            assert_eq!(admitted, 10, "{one:?} {other:?}");
        })
        .await;
}
