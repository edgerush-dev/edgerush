//! The rate limits route rules are under, end to end: the engine's limits named by routes,
//! the data plane's worker deciding each request under all of them at once as a data plane
//! of the rate-limiting service, three pods of its store on real sockets, and clients told
//! apart by their loopback addresses.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::Config as StoreConfig;
use edgerush_cluster_asynchronous::Pod as AsyncPod;
use edgerush_config::{Config, compile};
use edgerush_proxy::{H1Limits, Proxy, Worker};
use edgerush_ratelimit_service::cluster::forming::Forming;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer};
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
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

/// A naming file of the scratch directory's, apart from every other test's.
fn naming_file() -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory =
        std::env::temp_dir().join(format!("edgerush-ratelimit-test-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(NEXT.fetch_add(1, Ordering::Relaxed).to_string())
}

/// Three pods of the store, asynchronous, formed as the bench's example program forms them,
/// once ready on one table: their addresses, the data planes' seeds.
async fn service() -> Vec<String> {
    slow_service(None).await
}

/// The same, what each pod tells a data plane's stream given out `delay` after it is told, if
/// given: a network slow enough that a data plane answers its client before it hears from the
/// service, the pods' own links as quick as ever.
async fn slow_service(delay: Option<Duration>) -> Vec<String> {
    let config = StoreConfig {
        shards: 16,
        ..StoreConfig::defaults()
    };
    let mut pods = Vec::new();
    let mut addresses = Vec::new();
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut mode = AsyncPod::new(id, address.clone(), config, [], 10_000_000);
        mode.set_life(id);
        let pod = StorePod::start(mode, address.clone(), Dial::Plain, Vec::new());
        let server = Slowed {
            server: StoreServer::new(pod.clone()),
            delay: delay.unwrap_or_default(),
        };
        tokio::spawn(async move {
            let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
            let _ended = tonic::transport::Server::builder()
                .serve_with_incoming(server, incoming)
                .await;
        });
        pods.push(pod);
        addresses.push(address);
    }
    let forming = Forming::new(&config, naming_file()).unwrap();
    let forming_at = addresses.clone();
    tokio::spawn(async move {
        let _stopped = forming.run(forming_at).await;
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut stamps = Vec::new();
        for pod in &pods {
            stamps.push(
                pod.with(|node, now| node.ready(now).then(|| node.committed_stamp()))
                    .await
                    .flatten(),
            );
        }
        if stamps.iter().all(Option::is_some) && stamps.windows(2).all(|pair| pair[0] == pair[1]) {
            break;
        }
        assert!(Instant::now() < deadline, "the pods never formed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

/// A data plane of one worker under [`LIMITS`], finding the store's pods at `service` with
/// `levers` besides the defaults (`, name: value` each), and where it listens.
async fn proxy(upstream: SocketAddr, service: &[String], levers: &str) -> SocketAddr {
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
  web: {{ load_balancer: p2c, endpoints: {{ listed: ["{upstream}"] }} }}
rate_limiting: {{ mode: asynchronous, service: {{ addresses: [{pods}], key_secret: 00112233445566778899aabbccddeeff, cycle_ms: 10{levers} }} }}
"#
    );
    let mut config: Config = serde_saphyr::from_str(&yaml).unwrap();
    config.limits = Some(serde_saphyr::from_str(LIMITS).unwrap());
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    proxy.start_rate_limits(|| {}).unwrap();
    let worker = Worker::with_limits(proxy, H1Limits::default());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
    let _sweeping = tokio::task::spawn_local(worker.maintain());
    front
}

/// The status of `method path` through `stream`.
async fn ask(stream: &mut TcpStream, method: &str, path: &str) -> u16 {
    answer(stream, method, path).await.0
}

/// The status of `method path` through `stream`, its `Retry-After` and its `RateLimit`, if
/// it has them.
/// A refusal by `limit` saying to come back in `seconds` — or a second more, a key's wait
/// being padded so that it is never short and rounded up to whole seconds — with the
/// `RateLimit` field saying the same.
fn refused_by(answer: &(u16, Option<u64>, Option<String>), limit: &str, seconds: u64) -> bool {
    let (status, after, field) = answer;
    let Some(after) = *after else {
        return false;
    };
    *status == 429
        && (after == seconds || after == seconds + 1)
        && field.as_deref() == Some(format!(r#""{limit}";r=0;t={after}"#).as_str())
}

async fn answer(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
) -> (u16, Option<u64>, Option<String>) {
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
    let retry = text
        .lines()
        .find_map(|line| line.strip_prefix("retry-after:"))
        .map(|value| value.trim().parse().unwrap());
    let field = text
        .lines()
        .find_map(|line| line.strip_prefix("ratelimit:"))
        .map(|value| value.trim().to_owned());
    (text[9..12].parse().unwrap(), retry, field)
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
            let front = proxy(upstream().await, &service, "").await;
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

/// Two data planes share a client's bucket through the service: once the other has heard
/// from the owner, what one admitted counts against it, exactly, and the first no longer
/// counting as admitting the key, neither is held to a share of what is left. The other's
/// first request, before it has heard, is decided from a full bucket (as the next test
/// shows), and its answer brings the owner's count.
#[tokio::test]
async fn two_data_planes_share_a_key_through_the_service() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let upstream = upstream().await;
            let first = proxy(upstream, &service, "").await;
            let second = proxy(upstream, &service, "").await;
            let client = [127, 0, 0, 5];
            let one = statuses(first, client, "GET", "/by-client", 6).await;
            // Longer than a data plane counts as admitting a key after its last admission,
            // 300 ms (`ACTIVE_CYCLES`): the first's reports have reached the owner, and it
            // no longer shares the bucket with the second.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let heard = statuses(second, client, "GET", "/by-client", 1).await;
            // Its report and the owner's answer, a cycle or two.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let other = statuses(second, client, "GET", "/by-client", 5).await;
            assert_eq!(one, [200; 6]);
            assert_eq!(heard, [200]);
            assert_eq!(other, [200, 200, 200, 429, 429], "three left of ten");
        })
        .await;
}

/// By default a data plane decides a key it has not heard of from a full bucket until the
/// owner's word reaches it, and then owes what it admitted meanwhile: on a network slow
/// enough that the second data plane hears nothing while its client is answered — every
/// pod's word to it slow — it admits all eight on top of the first's six, and once it has
/// heard, refuses.
#[tokio::test]
async fn a_data_plane_decides_a_key_it_has_not_heard_of_from_a_full_bucket() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = slow_service(Some(Duration::from_millis(80))).await;
            let upstream = upstream().await;
            let first = proxy(upstream, &service, "").await;
            let second = proxy(upstream, &service, "").await;
            let client = [127, 0, 0, 7];
            let one = statuses(first, client, "GET", "/by-client", 6).await;
            // The first's admissions reach the owner once it has the table, which the slow
            // network brings 80 ms late, and a cycle after: measured, 150 ms was always
            // enough and 100 ms seldom, with or without polls; the rest is for a loaded
            // machine.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let other = statuses(second, client, "GET", "/by-client", 8).await;
            assert_eq!(one, [200; 6]);
            assert_eq!(other, [200; 8], "decided before any word could arrive");
            // An exchange both ways through the slow network, with time to spare.
            tokio::time::sleep(Duration::from_secs(1)).await;
            let after = statuses(second, client, "GET", "/by-client", 1).await;
            assert_eq!(after, [429], "fourteen admitted against ten: owed");
        })
        .await;
}

/// A store pod's server that gives out what it tells a data plane's stream `delay` after it
/// is told, each piece of the stream's body in its turn.
#[derive(Clone)]
struct Slowed {
    server: StoreServer<AsyncPod>,
    delay: Duration,
}

impl tonic::server::NamedService for Slowed {
    const NAME: &'static str = <StoreServer<AsyncPod> as tonic::server::NamedService>::NAME;
}

impl tower_service::Service<http::Request<tonic::body::Body>> for Slowed {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.server.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let slowed = request.uri().path().ends_with("/Exchange") && !self.delay.is_zero();
        let delay = self.delay;
        let answer = self.server.call(request);
        Box::pin(async move {
            let answer = answer.await?;
            if !slowed {
                return Ok(answer);
            }
            Ok(answer.map(|body| {
                tonic::body::Body::new(SlowBody {
                    inner: Some(body),
                    delay,
                    queue: VecDeque::new(),
                    ended: None,
                    sleep: None,
                })
            }))
        })
    }
}

/// What a frame of a stream's body is polled as.
type Polled = Option<Result<http_body::Frame<bytes::Bytes>, tonic::Status>>;

/// A stream's body, each piece given out `delay` after it was read, the end after them.
struct SlowBody {
    inner: Option<tonic::body::Body>,
    delay: Duration,
    queue: VecDeque<(tokio::time::Instant, http_body::Frame<bytes::Bytes>)>,
    ended: Option<Polled>,
    sleep: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl http_body::Body for SlowBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;

    fn poll_frame(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Polled> {
        let delay = self.delay;
        // Everything the body has now, each piece stamped with when it goes out.
        while let Some(inner) = &mut self.inner {
            match std::pin::Pin::new(inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    let at = tokio::time::Instant::now() + delay;
                    self.queue.push_back((at, frame));
                }
                Poll::Ready(other) => {
                    self.ended = Some(other);
                    self.inner = None;
                }
                Poll::Pending => break,
            }
        }
        loop {
            let Some((at, _)) = self.queue.front() else {
                return match self.ended.take() {
                    Some(ended) => Poll::Ready(ended),
                    None => Poll::Pending,
                };
            };
            let at = *at;
            if tokio::time::Instant::now() >= at {
                self.sleep = None;
                let (_, frame) = self.queue.pop_front().unwrap();
                return Poll::Ready(Some(Ok(frame)));
            }
            let sleep = self
                .sleep
                .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(at)));
            sleep.as_mut().reset(at);
            if sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
        }
    }
}

/// A refusal says when to come back: `Retry-After`, in seconds, when the refusing limit
/// would first admit the request — refused by several at once, the latest of them — and
/// `RateLimit` names that limit, none of it left for as long.
#[tokio::test]
async fn a_refusal_says_when_the_request_would_be_admitted() {
    with_proxy(|front| async move {
        let client = [127, 0, 0, 6];
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind(SocketAddr::from((client, 0))).unwrap();
        let mut stream = socket.connect(front).await.unwrap();
        for _ in 0..10 {
            assert_eq!(
                answer(&mut stream, "GET", "/by-client").await,
                (200, None, None)
            );
        }
        // Ten an hour: one back every 360 seconds.
        let refused = answer(&mut stream, "GET", "/by-client").await;
        assert!(refused_by(&refused, "per-client", 360), "{refused:?}");
        // Three POSTs an hour spent, with three of the twenty an hour over everything; then
        // the rest of the twenty: both refuse, `writes` for 1,200 s, `everything` for 180.
        for _ in 0..3 {
            assert_eq!(answer(&mut stream, "POST", "/api/orders").await.0, 200);
        }
        let refused = loop {
            let answer = answer(&mut stream, "GET", "/elsewhere").await;
            if answer.0 != 200 {
                break answer;
            }
        };
        assert!(refused_by(&refused, "everything", 180), "{refused:?}");
        let latest = answer(&mut stream, "POST", "/api/orders").await;
        assert!(
            refused_by(&latest, "writes", 1_200),
            "the latest of the two: {latest:?}"
        );
        // Another client spends everything elsewhere: `/by-client`, its own bucket full, is
        // refused by the second of its limits.
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind(SocketAddr::from(([127, 0, 0, 7], 0))).unwrap();
        let mut stream = socket.connect(front).await.unwrap();
        for _ in 0..20 {
            assert_eq!(answer(&mut stream, "GET", "/elsewhere").await.0, 200);
        }
        let refused = answer(&mut stream, "GET", "/by-client").await;
        assert!(refused_by(&refused, "everything", 180), "{refused:?}");
    })
    .await;
}
