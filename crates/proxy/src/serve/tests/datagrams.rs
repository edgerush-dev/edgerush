//! The datagrams of `udp` listeners, carried through sessions to a backend and back
//! (24).

use super::*;
use crate::serve::datagrams::{sends_refused, sockets_full};
use crate::upstream::dial;

/// A `udp` listener `dns` whose one route goes to `backends`, each `(upstream, weight)`,
/// with `upstreams` the upstreams' YAML and `extra` in the listener's settings.
fn udp_config(backends: &str, upstreams: &str, extra: &str) -> Compiled {
    let yaml = format!(
        "listeners: {{ dns: {{ address: \"127.0.0.1:0\", protocol: udp{extra} }} }}\n\
         routes: []\n\
         udp_routes: [{{ name: dns, listeners: [dns], backends: [{backends}] }}]\n\
         upstreams: {{ {upstreams} }}\n"
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    compile(&config).unwrap()
}

/// A `udp` upstream `name` whose endpoints are `endpoints`.
fn udp_upstream(name: &str, endpoints: &[SocketAddr]) -> String {
    let listed: Vec<String> = endpoints.iter().map(|at| format!("\"{at}\"")).collect();
    format!(
        "{name}: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [{}] }} }}",
        listed.join(", ")
    )
}

/// A `udp` listener `dns` whose one route goes to `up`, at `endpoints`.
fn udp_to(endpoints: &[SocketAddr], extra: &str) -> Compiled {
    udp_config(
        "{ upstream: up, weight: 1 }",
        &udp_upstream("up", endpoints),
        extra,
    )
}

/// A backend that answers every datagram with the same bytes.
async fn echo_backend() -> SocketAddr {
    answering_backend(|datagram| datagram.to_vec()).await
}

/// A backend that answers every datagram with `name`.
async fn naming_backend(name: &'static str) -> SocketAddr {
    answering_backend(move |_| name.as_bytes().to_vec()).await
}

/// A backend that answers every datagram with what `answer` makes of it.
async fn answering_backend(answer: impl Fn(&[u8]) -> Vec<u8> + 'static) -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        let mut buffer = vec![0; 1 << 16];
        while let Ok((len, from)) = socket.recv_from(&mut buffer).await {
            let _sent = socket.send_to(&answer(&buffer[..len]), from).await;
        }
    });
    address
}

/// A backend that reads nothing: what is sent to it goes nowhere.
async fn silent_backend() -> (SocketAddr, UdpSocket) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    (socket.local_addr().unwrap(), socket)
}

/// A worker serving `config`'s one listener, a `udp` one, reading into a buffer of
/// `buffer` bytes; where it listens, the worker, and the task serving it.
async fn serving_udp_reading(
    config: Compiled,
    limits: H1Limits,
    buffer: usize,
) -> (SocketAddr, Rc<Worker>, tokio::task::JoinHandle<()>) {
    let proxy = Proxy::new(config, NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let serving = Rc::clone(&worker)
        .serve_udp_reading(0, socket, buffer)
        .unwrap();
    (front, worker, tokio::task::spawn_local(serving))
}

/// The same, with the worker's own buffer and bounds.
async fn serving_udp(config: Compiled) -> (SocketAddr, Rc<Worker>) {
    let (front, worker, _serving) =
        serving_udp_reading(config, H1Limits::default(), crate::serve::datagrams::BUFFER).await;
    (front, worker)
}

/// A client of its own, connected to `front`: it takes datagrams from there alone.
async fn client_of(front: SocketAddr) -> UdpSocket {
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(front).await.unwrap();
    client
}

/// What `client` is answered with after it sends `datagram`.
async fn exchange(client: &UdpSocket, datagram: &[u8]) -> Vec<u8> {
    client.send(datagram).await.unwrap();
    answer(client).await
}

/// The next datagram `client` takes.
async fn answer(client: &UdpSocket) -> Vec<u8> {
    let mut buffer = vec![0; 1 << 16];
    let len = within(client.recv(&mut buffer)).await.unwrap();
    buffer.truncate(len);
    buffer
}

/// Whether `client` takes nothing for a while.
async fn unanswered(client: &UdpSocket) -> bool {
    let mut buffer = [0; 64];
    tokio::time::timeout(Duration::from_millis(200), client.recv(&mut buffer))
        .await
        .is_err()
}

/// The scrape's line `name{listener="dns",…}`, `labels` after the listener's, says `count`.
fn line(name: &str, labels: &str, count: u64) -> String {
    let labels = if labels.is_empty() {
        String::new()
    } else {
        format!(",{labels}")
    };
    format!("{name}{{listener=\"dns\"{labels}}} {count}\n")
}

/// Whether the worker's scrape says `line`, soon: never long, since waiting on it sleeps.
async fn counted(worker: &Worker, line: &str) {
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        while !worker.proxy().metrics().contains(line) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "never counted {line}in\n{}",
        worker.proxy().metrics()
    );
}

fn dropped(reason: &str, count: u64) -> String {
    line(
        "edgerush_listener_udp_datagrams_dropped_total",
        &format!("reason=\"{reason}\""),
        count,
    )
}

fn sessions_ended(outcome: &str, count: u64) -> String {
    line(
        "edgerush_listener_udp_sessions_total",
        &format!("outcome=\"{outcome}\""),
        count,
    )
}

fn accepted(count: u64) -> String {
    line("edgerush_listener_connections_accepted_total", "", count)
}

fn active(count: u64) -> String {
    line("edgerush_listener_connections_active", "", count)
}

/// Every datagram is carried to the backend as it came, and every answer back as it
/// came: one out for each in, never joined or split, from empty to near UDP's largest.
#[tokio::test]
async fn datagrams_go_both_ways_unchanged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let (front, worker) = serving_udp(udp_to(&[backend], "")).await;
            let client = client_of(front).await;
            for len in [0, 1, 1_472, 9_000, 60_000] {
                let datagram: Vec<u8> = (0..len).map(|at| (at % 251) as u8).collect();
                assert_eq!(exchange(&client, &datagram).await, datagram, "{len} bytes");
            }
            // All of them one session's, counted as one connection accepted and open.
            counted(&worker, &accepted(1)).await;
            counted(&worker, &active(1)).await;
        })
        .await;
}

/// Every datagram of a client goes to the endpoint its first went to, though another
/// endpoint of the upstream has fewer sessions.
#[tokio::test]
async fn a_clients_datagrams_all_go_to_one_endpoint() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let a = naming_backend("a").await;
            let b = naming_backend("b").await;
            let (front, _worker) = serving_udp(udp_to(&[a, b], "")).await;
            let client = client_of(front).await;
            let first = exchange(&client, b"?").await;
            for _ in 0..20 {
                assert_eq!(exchange(&client, b"?").await, first);
            }
            // A second client is a session of its own: p2c gives it the other endpoint,
            // the first's being held by its session.
            let other = client_of(front).await;
            assert_ne!(exchange(&other, b"?").await, first);
        })
        .await;
}

/// New clients are shared out among the route's backends by weight, one session each, as
/// Gateway API's weighted test sends them (24 §3).
#[tokio::test]
async fn new_clients_go_to_backends_by_weight() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let a = naming_backend("a").await;
            let b = naming_backend("b").await;
            let upstreams = format!("{}, {}", udp_upstream("a", &[a]), udp_upstream("b", &[b]));
            let config = udp_config(
                "{ upstream: a, weight: 1 }, { upstream: b, weight: 3 }",
                &upstreams,
                "",
            );
            let (front, _worker) = serving_udp(config).await;
            let mut to_a = 0;
            const CLIENTS: usize = 400;
            for _ in 0..CLIENTS {
                let client = client_of(front).await;
                if exchange(&client, b"?").await == b"a" {
                    to_a += 1;
                }
            }
            // A quarter, 100, with a standard deviation near 9: far from either bound
            // unless the weights are not kept.
            assert!((60..=140).contains(&to_a), "{to_a} of {CLIENTS} went to a");
        })
        .await;
}

/// A session that carries nothing either way for its idle bound ends, counted, and gives
/// its room back; the client's next datagram makes another.
#[tokio::test]
async fn an_idle_session_ends_at_its_bound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let (front, worker) =
                serving_udp(udp_to(&[backend], ", session_idle_seconds: 1")).await;
            let client = client_of(front).await;
            let started = Instant::now();
            assert_eq!(exchange(&client, b"one").await, b"one");
            counted(&worker, &sessions_ended("idle", 1)).await;
            let took = started.elapsed();
            assert!(
                took >= Duration::from_secs(1) - EARLY
                    && took <= Duration::from_secs(1) + SLACK + SLACK,
                "ended after {took:?}"
            );
            counted(&worker, &active(0)).await;
            assert_eq!(exchange(&client, b"two").await, b"two");
            counted(&worker, &accepted(2)).await;
        })
        .await;
}

/// An answer keeps a session from its idle bound as a datagram from its client does.
#[tokio::test]
async fn answers_keep_a_session_from_its_idle_bound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, socket) = silent_backend().await;
            let (front, worker) =
                serving_udp(udp_to(&[backend], ", session_idle_seconds: 1")).await;
            let client = client_of(front).await;
            client.send(b"hello").await.unwrap();
            let mut buffer = [0; 16];
            let (_, session) = within(socket.recv_from(&mut buffer)).await.unwrap();
            // Answered every 300 ms for twice the bound: the session lives on.
            for _ in 0..7 {
                socket.send_to(b"tick", session).await.unwrap();
                assert_eq!(answer(&client).await, b"tick");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            assert!(
                worker
                    .proxy()
                    .metrics()
                    .contains(&sessions_ended("idle", 0)),
                "{}",
                worker.proxy().metrics()
            );
            counted(&worker, &sessions_ended("idle", 1)).await;
        })
        .await;
}

/// A `udp` listener no UDP route serves keeps its port, and drops every datagram.
#[tokio::test]
async fn a_listener_with_no_route_drops_every_datagram() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let yaml = "listeners: { dns: { address: \"127.0.0.1:0\", protocol: udp } }\nroutes: []\nupstreams: {}\n";
            let config: Config = serde_saphyr::from_str(yaml).unwrap();
            let (front, worker) = serving_udp(compile(&config).unwrap()).await;
            let client = client_of(front).await;
            client.send(b"one").await.unwrap();
            client.send(b"two").await.unwrap();
            counted(&worker, &dropped("no_route", 2)).await;
            assert!(unanswered(&client).await);
            counted(&worker, &accepted(0)).await;
        })
        .await;
}

/// An invalid backend's sessions drop their datagrams, its share of them, and count as
/// connections as any session does (24 §4).
#[tokio::test]
async fn an_invalid_backends_sessions_drop_their_datagrams() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let config = udp_config(
                "{ invalid: \"no such Service\", weight: 1 }",
                &udp_upstream("up", &["127.0.0.1:1".parse().unwrap()]),
                "",
            );
            let (front, worker) = serving_udp(config).await;
            let client = client_of(front).await;
            client.send(b"one").await.unwrap();
            client.send(b"two").await.unwrap();
            counted(&worker, &dropped("invalid_backend", 2)).await;
            counted(&worker, &accepted(1)).await;
            counted(&worker, &active(1)).await;
            assert!(unanswered(&client).await);
        })
        .await;
}

/// An upstream with no endpoint now makes no session: the datagram is dropped, and the
/// client's next one picks again, so that it is served as soon as an endpoint comes
/// (24 §4).
#[tokio::test]
async fn an_upstream_with_no_endpoint_makes_no_session_until_one_comes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (front, worker) = serving_udp(udp_to(&[], "")).await;
            let client = client_of(front).await;
            client.send(b"one").await.unwrap();
            client.send(b"two").await.unwrap();
            counted(&worker, &dropped("invalid_backend", 2)).await;
            counted(&worker, &accepted(0)).await;
            let backend = echo_backend().await;
            worker.proxy().reload(udp_to(&[backend], "")).unwrap();
            assert_eq!(exchange(&client, b"three").await, b"three");
        })
        .await;
}

/// A route none of whose backends has a share makes no session: every datagram dropped.
#[tokio::test]
async fn a_route_with_no_share_makes_no_session() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let config = udp_config(
                "{ upstream: up, weight: 0 }",
                &udp_upstream("up", &[backend]),
                "",
            );
            let (front, worker) = serving_udp(config).await;
            let client = client_of(front).await;
            client.send(b"one").await.unwrap();
            client.send(b"two").await.unwrap();
            counted(&worker, &dropped("invalid_backend", 2)).await;
            counted(&worker, &accepted(0)).await;
            assert!(unanswered(&client).await);
        })
        .await;
}

/// A new client that finds no room among its worker's connections is dropped, and served
/// once a session has ended and given its room back.
#[tokio::test]
async fn a_new_client_finds_no_room_until_a_session_ends() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let proxy = Proxy::new(
                udp_to(&[backend], ", session_idle_seconds: 1"),
                NonZeroUsize::MIN,
            )
            .unwrap();
            let loads = Loads::new(1, 1, QUIC_MOST, 1);
            let worker = Worker::at(Arc::new(proxy), H1Limits::default(), 0, loads);
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving =
                tokio::task::spawn_local(Rc::clone(&worker).serve_udp(0, socket).unwrap());
            let first = client_of(front).await;
            assert_eq!(exchange(&first, b"one").await, b"one");
            let second = client_of(front).await;
            second.send(b"two").await.unwrap();
            counted(&worker, &dropped("no_room", 1)).await;
            assert!(unanswered(&second).await);
            counted(&worker, &sessions_ended("idle", 1)).await;
            assert_eq!(exchange(&second, b"three").await, b"three");
        })
        .await;
}

/// A new client whose session the worker has no file for is dropped: the worker's own
/// shortage, which sets no endpoint aside.
#[tokio::test]
async fn a_new_client_the_worker_has_no_file_for_is_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let limits = H1Limits {
                descriptors: Some(0),
                ..H1Limits::default()
            };
            let (front, worker, _serving) =
                serving_udp_reading(udp_to(&[backend], ""), limits, 1 << 16).await;
            let client = client_of(front).await;
            client.send(b"one").await.unwrap();
            counted(&worker, &dropped("exhausted", 1)).await;
            counted(&worker, &accepted(0)).await;
            assert!(unanswered(&client).await);
            let endpoint = Arc::clone(worker.proxy().current.load().destinations.at(0, 0).unwrap());
            assert_eq!(endpoint.set_aside_for(), None);
        })
        .await;
}

/// The same for a socket the system will not make, as a process out of descriptors is
/// refused one.
#[tokio::test]
async fn a_new_client_the_system_has_no_socket_for_is_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let (front, worker) = serving_udp(udp_to(&[backend], "")).await;
            let client = client_of(front).await;
            dial::short_of_sockets(true);
            client.send(b"one").await.unwrap();
            counted(&worker, &dropped("exhausted", 1)).await;
            dial::short_of_sockets(false);
            assert_eq!(exchange(&client, b"two").await, b"two");
            let endpoint = Arc::clone(worker.proxy().current.load().destinations.at(0, 0).unwrap());
            assert_eq!(endpoint.set_aside_for(), None);
        })
        .await;
}

/// A new client whose endpoint cannot be reached is dropped, and the endpoint set aside,
/// as a tunnel's failed connect sets it aside: the next client goes to another.
#[tokio::test]
async fn an_endpoint_that_cannot_be_reached_is_set_aside() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let gone = naming_backend("gone").await;
            let there = naming_backend("there").await;
            dial::unreachable(gone, true);
            let (front, worker) = serving_udp(udp_to(&[gone, there], "")).await;
            let snapshot = worker.proxy().current.load();
            let endpoints = snapshot.destinations.of(0);
            // Clients until one is sent to the endpoint that cannot be reached.
            let mut tried = 0;
            while endpoints
                .iter()
                .all(|endpoint| endpoint.set_aside_for().is_none())
            {
                tried += 1;
                assert!(tried <= 64, "never sent to {gone}");
                let client = client_of(front).await;
                client.send(b"?").await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            counted(&worker, &dropped("unreachable", 1)).await;
            let aside: Vec<SocketAddr> = endpoints
                .iter()
                .filter(|endpoint| endpoint.set_aside_for().is_some())
                .map(|endpoint| endpoint.address())
                .collect();
            assert_eq!(aside, [gone]);
            for _ in 0..8 {
                let client = client_of(front).await;
                assert_eq!(exchange(&client, b"?").await, b"there");
            }
            dial::unreachable(gone, false);
        })
        .await;
}

/// A client's datagram longer than the buffer it is read into is dropped, never carried
/// cut: nothing of it reaches the backend.
#[tokio::test]
async fn a_clients_datagram_longer_than_the_buffer_is_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, socket) = silent_backend().await;
            let (front, worker, _serving) =
                serving_udp_reading(udp_to(&[backend], ""), H1Limits::default(), 16).await;
            let client = client_of(front).await;
            // What fills the buffer may have been cut: dropped too.
            client.send(&[1; 16]).await.unwrap();
            client.send(&[2; 100]).await.unwrap();
            counted(&worker, &dropped("truncated", 2)).await;
            let mut buffer = [0; 128];
            let came = tokio::time::timeout(Duration::from_millis(200), socket.recv(&mut buffer));
            assert!(came.await.is_err(), "the backend was sent something");
            // One that fits goes through whole.
            client.send(&[3; 15]).await.unwrap();
            let len = within(socket.recv(&mut buffer)).await.unwrap();
            assert_eq!(&buffer[..len], [3; 15]);
        })
        .await;
}

/// A backend's answer longer than the buffer it is read into is dropped, never carried
/// cut: the client is sent nothing of it.
#[tokio::test]
async fn an_answer_longer_than_the_buffer_is_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Answers twice as long as what it is sent.
            let backend = answering_backend(|datagram| [datagram, datagram].concat()).await;
            let (front, worker, _serving) =
                serving_udp_reading(udp_to(&[backend], ""), H1Limits::default(), 16).await;
            let client = client_of(front).await;
            // 7 bytes go and 14 come back; 8 go and 16, which fills the buffer, are
            // dropped on the way back, as 20 are.
            assert_eq!(exchange(&client, &[3; 7]).await, [3; 14]);
            client.send(&[4; 8]).await.unwrap();
            client.send(&[5; 10]).await.unwrap();
            counted(&worker, &dropped("truncated", 2)).await;
            assert!(unanswered(&client).await);
        })
        .await;
}

/// A datagram either way that its socket has no room for is dropped, never held (24 §4).
#[tokio::test]
async fn a_datagram_its_socket_has_no_room_for_is_dropped_either_way() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, socket) = silent_backend().await;
            let (front, worker) = serving_udp(udp_to(&[backend], "")).await;
            let client = client_of(front).await;
            client.send(b"hello").await.unwrap();
            let mut buffer = [0; 16];
            let (_, session) = within(socket.recv_from(&mut buffer)).await.unwrap();
            sockets_full(true);
            // The client's, to the backend's socket; the backend's, to the listener's.
            client.send(b"lost").await.unwrap();
            counted(&worker, &dropped("send_full", 1)).await;
            socket.send_to(b"lost too", session).await.unwrap();
            counted(&worker, &dropped("send_full", 2)).await;
            sockets_full(false);
            assert!(unanswered(&client).await);
            client.send(b"again").await.unwrap();
            let (len, _) = within(socket.recv_from(&mut buffer)).await.unwrap();
            assert_eq!(&buffer[..len], b"again", "nothing held came first");
            socket.send_to(b"back", session).await.unwrap();
            assert_eq!(answer(&client).await, b"back");
        })
        .await;
}

/// An endpoint whose port is closed answers with ICMP port unreachable: the session ends,
/// counted, the endpoint is set aside, and the next client goes to another (24 §3).
#[tokio::test]
async fn a_refused_session_ends_and_its_endpoint_is_set_aside() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A port nothing listens on: bound, and let go.
            let closed = std::net::UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            let there = naming_backend("there").await;
            let (front, worker) = serving_udp(udp_to(&[closed, there], "")).await;
            // Kept, so that each holds its session, and p2c sends the next elsewhere.
            let mut clients = Vec::new();
            for _ in 0..2 {
                let client = client_of(front).await;
                client.send(b"?").await.unwrap();
                clients.push(client);
            }
            counted(&worker, &sessions_ended("refused", 1)).await;
            counted(&worker, &active(1)).await;
            let snapshot = worker.proxy().current.load();
            let aside: Vec<SocketAddr> = snapshot
                .destinations
                .of(0)
                .iter()
                .filter(|endpoint| endpoint.set_aside_for().is_some())
                .map(|endpoint| endpoint.address())
                .collect();
            assert_eq!(aside, [closed]);
            // The refused client's next datagram is a new session, on the endpoint there.
            for client in &clients {
                assert_eq!(exchange(client, b"?").await, b"there");
            }
        })
        .await;
}

/// A refusal the kernel reports on a send, before the session's own read sees it, ends the
/// session as one on a read does, and the datagram is not sent again.
#[tokio::test]
async fn a_session_refused_on_a_send_ends_and_its_endpoint_is_set_aside() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, socket) = silent_backend().await;
            let (front, worker) = serving_udp(udp_to(&[backend], "")).await;
            let client = client_of(front).await;
            sends_refused(true);
            client.send(b"refused").await.unwrap();
            counted(&worker, &sessions_ended("refused", 1)).await;
            sends_refused(false);
            counted(&worker, &active(0)).await;
            let endpoint = Arc::clone(worker.proxy().current.load().destinations.at(0, 0).unwrap());
            assert!(endpoint.set_aside_for().is_some());
            // Nothing of the refused one reached the backend; the next is a session anew.
            client.send(b"again").await.unwrap();
            let mut buffer = [0; 16];
            let (len, _) = within(socket.recv_from(&mut buffer)).await.unwrap();
            assert_eq!(&buffer[..len], b"again");
            counted(&worker, &accepted(2)).await;
        })
        .await;
}

/// A listener taken away, or a worker that drains, ends every session, counted, and the
/// listener's task with them.
#[tokio::test]
async fn a_listener_stopped_ends_its_sessions() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let (front, worker, serving) =
                serving_udp_reading(udp_to(&[backend], ""), H1Limits::default(), 1 << 16).await;
            for _ in 0..3 {
                let client = client_of(front).await;
                assert_eq!(exchange(&client, b"?").await, b"?");
            }
            worker.stop(0);
            within(serving).await.unwrap();
            counted(&worker, &sessions_ended("drained", 3)).await;
            counted(&worker, &active(0)).await;
        })
        .await;
}

/// On a wildcard bind, a client answers from the address its datagram arrived on: a
/// client connected to 127.0.0.2, which takes datagrams from there alone, is answered.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_wildcard_listener_answers_from_the_address_a_datagram_arrived_on() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = echo_backend().await;
            let proxy = Proxy::new(udp_to(&[backend], ""), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
            let port = socket.local_addr().unwrap().port();
            let _serving =
                tokio::task::spawn_local(Rc::clone(&worker).serve_udp(0, socket).unwrap());
            // The whole of 127.0.0.0/8 is loopback on Linux.
            for to in ["127.0.0.2", "127.0.0.3"] {
                let client = client_of(SocketAddr::new(to.parse().unwrap(), port)).await;
                assert_eq!(exchange(&client, to.as_bytes()).await, to.as_bytes());
            }
        })
        .await;
}
