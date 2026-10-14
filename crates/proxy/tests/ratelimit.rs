//! A rule's rate limit (experimental), end to end: the data plane's worker deciding as a
//! data plane of the rate-limiting service, three service pods on real sockets.

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
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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
    addresses
}

/// A data plane of one worker whose `/api` rule allows ten requests an hour for each value
/// of `x-key`, a burst of ten; and where it listens.
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
      - matches: [{{ path: {{ prefix: /api }} }}]
        forward: {{ backends: [{{ upstream: web, weight: 1 }}] }}
        rate_limit: {{ key: {{ header: x-key }}, limit: 10, window_ms: 3600000, burst: 10 }}
      - matches: [{{ path: {{ prefix: / }} }}]
        forward: {{ backends: [{{ upstream: web, weight: 1 }}] }}
upstreams:
  web: {{ load_balancer: p2c, endpoints: ["{upstream}"] }}
rate_limit_service: {{ pods: [{pods}], cycle_ms: 10 }}
"#
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let worker = Worker::with_limits(proxy, H1Limits::default());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
    let _sweeping = tokio::task::spawn_local(worker.maintain());
    front
}

/// The status of a GET of `path` through `stream`, with `x-key: key` if there is one.
async fn get(stream: &mut TcpStream, path: &str, key: Option<&str>) -> u16 {
    let key = key.map_or(String::new(), |key| format!("x-key: {key}\r\n"));
    let request = format!("GET {path} HTTP/1.1\r\nhost: example.com\r\n{key}\r\n");
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

async fn statuses(front: SocketAddr, path: &str, key: Option<&str>, count: usize) -> Vec<u16> {
    let mut stream = TcpStream::connect(front).await.unwrap();
    let mut out = Vec::new();
    for _ in 0..count {
        out.push(get(&mut stream, path, key).await);
    }
    out
}

fn tally(statuses: &[u16]) -> (usize, usize) {
    (
        statuses.iter().filter(|s| **s == 200).count(),
        statuses.iter().filter(|s| **s == 429).count(),
    )
}

#[tokio::test]
async fn a_rule_limits_each_key_to_its_bucket_and_refuses_the_rest_with_429() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let front = proxy(upstream().await, &service).await;
            let a = tokio::time::timeout(
                Duration::from_secs(10),
                statuses(front, "/api/x", Some("a"), 15),
            )
            .await
            .expect("answered in time");
            assert_eq!(tally(&a), (10, 5), "{a:?}");
            let b = statuses(front, "/api/x", Some("b"), 3).await;
            assert_eq!(tally(&b), (3, 0), "another key has a bucket of its own");
            let none = statuses(front, "/api/x", None, 15).await;
            assert_eq!(
                tally(&none),
                (15, 0),
                "a request without the key is not limited"
            );
            let free = statuses(front, "/elsewhere", Some("a"), 5).await;
            assert_eq!(
                tally(&free),
                (5, 0),
                "a rule without a limit is not limited"
            );
        })
        .await;
}

/// Two data planes share a key's bucket through the service: what one admitted counts
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
            let one = statuses(first, "/api/x", Some("shared"), 6).await;
            // The first's admissions reach the owner within a cycle or two.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let other = statuses(second, "/api/x", Some("shared"), 8).await;
            let admitted = tally(&one).0 + tally(&other).0;
            assert_eq!(admitted, 10, "{one:?} {other:?}");
        })
        .await;
}
