//! The HTTP/3 server against quiche's own client over real UDP sockets
//! ([16 §7, step 2](../../../../../docs/16-http3.md)), with a request core that answers
//! from what it read: the transport's behaviour, apart from any upstream.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests: a helper that fails fails the test"
)]

use crate::downstream::h1::connection::Answered;
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h3::Settings;
use crate::downstream::h3::code;
use crate::downstream::h3::conn::State;
use crate::downstream::h3::listener::{self, Forwarding, InForce, Secrets, Shared};
use crate::downstream::h3::testing::{Client, PATIENCE, get};
use crate::drain::Drain;
use crate::interim::Interim;
use crate::request_body::{RequestBody, RequestBodyError};
use crate::storage::{LIMIT, Storage};
use crate::timers::Timers;
use crate::tls::Tls;
use crate::tls::testing::certificate;
use bytes::Bytes;
use http::{Request, Response};
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Full, StreamBody};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio_stream::StreamExt;

/// How late a deadline may be seen to fire on a loaded machine.
const SLACK: Duration = Duration::from_millis(600);

type Answering = Pin<Box<dyn Future<Output = Answered<Full<Bytes>>>>>;

/// A listener served on the loopback, and what its tests reach into.
struct Server {
    address: SocketAddr,
    drain: Rc<Drain>,
    shared: Rc<Shared>,
}

impl Server {
    /// What `look` finds in the one connection the server holds.
    fn connection<T>(&self, look: impl FnOnce(&mut State) -> T) -> T {
        let table = self.shared.table.borrow();
        let conn = table.values().next().expect("a connection");
        conn.with(look)
    }

    /// quiche's counts for the one connection the server holds.
    fn stats(&self) -> quiche::Stats {
        self.connection(|state| state.quic.stats())
    }
}

/// A request core whose every exchange waits for ever, as one on a stalled upstream does,
/// counting them: started, alive, and the most alive at once.
#[derive(Clone, Default)]
struct Exchanges {
    started: Rc<Cell<usize>>,
    alive: Rc<Cell<usize>>,
    most: Rc<Cell<usize>>,
}

/// One exchange, counted alive until it is dropped.
struct Alive(Exchanges);

impl Drop for Alive {
    fn drop(&mut self) {
        self.0.alive.set(self.0.alive.get() - 1);
    }
}

impl Exchanges {
    fn core(&self) -> impl Fn(Request<RequestBody>, Interim) -> Answering + 'static {
        let exchanges = self.clone();
        move |_request, _interim| {
            exchanges.started.set(exchanges.started.get() + 1);
            exchanges.alive.set(exchanges.alive.get() + 1);
            exchanges
                .most
                .set(exchanges.most.get().max(exchanges.alive.get()));
            let alive = Alive(exchanges.clone());
            Box::pin(async move {
                let _alive = alive;
                std::future::pending().await
            })
        }
    }
}

/// Runs `test` on a `LocalSet`, as a worker's tasks run.
fn locally<F: Future>(test: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, test)
}

/// Settings short enough for deadlines to be tested on real time.
fn short() -> Settings {
    Settings {
        first_request: Duration::from_millis(400),
        keep_alive: Duration::from_millis(600),
        stream_idle: Duration::from_millis(800),
        drain_within: Duration::from_millis(900),
        ..Settings::default()
    }
}

/// Serves a listener for `a.test` with `settings`, answering each request with `respond`.
async fn serving<F, B>(
    settings: Settings,
    respond: impl Fn(Request<RequestBody>, Interim) -> F + 'static,
) -> Server
where
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serving_as(
        settings,
        respond,
        &Secrets::new().unwrap(),
        Forwarding::group(1).remove(0),
    )
    .await
}

/// The same for a worker whose storage is `storage`.
async fn serving_in<F, B>(
    settings: Settings,
    storage: Rc<Storage>,
    respond: impl Fn(Request<RequestBody>, Interim) -> F + 'static,
) -> Server
where
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serving_on(
        settings,
        respond,
        &Secrets::new().unwrap(),
        Forwarding::group(1).remove(0),
        storage,
    )
    .await
}

/// The same as the worker `forwarding` is the share of, among those `secrets` are shared by.
async fn serving_as<F, B>(
    settings: Settings,
    respond: impl Fn(Request<RequestBody>, Interim) -> F + 'static,
    secrets: &Secrets,
    forwarding: Forwarding,
) -> Server
where
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serving_on(settings, respond, secrets, forwarding, Storage::new(LIMIT)).await
}

/// The same, for a worker whose storage is `storage`.
async fn serving_on<F, B>(
    settings: Settings,
    respond: impl Fn(Request<RequestBody>, Interim) -> F + 'static,
    secrets: &Secrets,
    forwarding: Forwarding,
    storage: Rc<Storage>,
) -> Server
where
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let tls = Arc::new(
        Tls::new(&edgerush_config::Tls {
            certificates: vec![certificate(&["a.test"])],
            client_validation: None,
        })
        .unwrap(),
    );
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let timers = Timers::new();
    let running = Rc::clone(&timers);
    tokio::task::spawn_local(async move {
        let never = running.run().await;
        match never {}
    });
    let drain = Rc::new(Drain::default());
    let shared = Rc::new(
        Shared::new(
            socket,
            settings,
            secrets,
            u16::try_from(forwarding.worker()).unwrap(),
            timers,
            Rc::clone(&drain),
            storage,
            Box::new(|_| {}),
        )
        .unwrap(),
    );
    tokio::task::spawn_local(listener::serve(
        Rc::clone(&shared),
        move || {
            Some(InForce {
                tls: Arc::clone(&tls),
                force_retry: false,
            })
        },
        // Who the client is, the request core's to act on, is nothing these cores look at.
        Rc::new(move |request, interim, _client| respond(request, interim)),
        Rc::new(|| HttpDate::from_unix(0)),
        || (),
        forwarding,
    ));
    Server {
        address,
        drain,
        shared,
    }
}

/// Answers with what it read: the method, the path, the body's length and its trailers,
/// or what failed.
fn echo(request: Request<RequestBody>, _interim: Interim) -> Answering {
    Box::pin(async move {
        let (head, body) = request.into_parts();
        let said = match body.collect().await {
            Ok(collected) => {
                let trailers = collected.trailers().cloned();
                let sum = trailers
                    .as_ref()
                    .and_then(|trailers| trailers.get("x-sum"))
                    .map(|value| value.to_str().unwrap().to_owned());
                let length = collected.to_bytes().len();
                format!("{} {} {length} {sum:?}", head.method, head.uri.path())
            }
            Err(RequestBodyError::Invalid(error)) => format!("invalid: {error}"),
            Err(error) => format!("failed: {error}"),
        };
        Answered::Map(Response::new(Full::new(Bytes::from(said))))
    })
}

fn body_of(answer: &crate::downstream::h3::testing::Answer) -> String {
    String::from_utf8(answer.body.clone()).unwrap()
}

#[test]
fn a_get_is_answered() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let answer = client.get("a.test", "/x").await;
        assert_eq!(answer.final_status(), Some("200"));
        assert_eq!(body_of(&answer), "GET /x 0 None");
        // A date on the answer, as over every other version.
        assert!(answer.heads[0].iter().any(|(name, _)| name == "date"));
    });
}

#[test]
fn many_requests_on_one_connection_are_each_answered() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let ids: Vec<u64> = (0..20)
            .map(|at| client.request(&get("a.test", &format!("/{at}")), true))
            .collect();
        for (at, id) in ids.into_iter().enumerate() {
            let answer = client.answer(id).await;
            assert_eq!(body_of(&answer), format!("GET /{at} 0 None"));
        }
    });
}

/// A connection's record of the answers on their way, which a drain waits for, forgets them
/// as they arrive: however many a long-lived connection has answered, it holds no more than
/// twice the streams the client may have open.
#[test]
fn the_answers_on_their_way_are_forgotten_as_they_arrive() {
    locally(async {
        let settings = Settings {
            streams: 4,
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        for at in 0..20 {
            client.get("a.test", &format!("/{at}")).await;
        }
        let recorded = server.connection(|state| state.delivering.len());
        assert!(recorded <= 2 * 4, "{recorded} recorded");
    });
}

/// A request answered at once gets one datagram back, the answer with the request's ACK in
/// it: an ACK alone waits for something to go with (16 §2).
#[test]
fn an_answer_carries_the_ack_of_its_request() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        client.for_a_while(Duration::from_millis(100)).await;
        let before = client.received.len();
        let answer = client.get("a.test", "/x").await;
        assert_eq!(body_of(&answer), "GET /x 0 None");
        client.for_a_while(Duration::from_millis(100)).await;
        assert_eq!(client.received.len() - before, 1);
    });
}

/// An answer slow in coming does not hold back its request's ACK, which goes alone before it
/// once its wait is over (RFC 9000 §13.2.1). Only that it goes before the answer is held
/// here. When the wait ends is for quiche's own tests: on real time, the client's probe may
/// end it first (its PTO allows for the 25 ms the server may wait, and a coarse timer can
/// be later than that), and then its ACK is a second datagram.
#[test]
fn a_slow_answer_does_not_hold_back_the_ack() {
    locally(async {
        let server = serving(short(), |request, interim| -> Answering {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                echo(request, interim).await
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        client.for_a_while(Duration::from_millis(100)).await;
        let before = client.received.len();
        let id = client.request(&get("a.test", "/x"), true);
        client.for_a_while(Duration::from_millis(150)).await;
        let acknowledged = client.received.len() - before;
        assert!(acknowledged >= 1, "nothing came back in 150 ms");
        assert!(!client.answers.contains_key(&id), "the answer came early");
        let answer = client.answer(id).await;
        assert_eq!(body_of(&answer), "GET /x 0 None");
        assert!(client.received.len() - before > acknowledged);
    });
}

/// Four megabytes back, thousands of datagrams, sent in runs the kernel cuts apart where it
/// can (Linux), arrive whole and in order.
#[test]
fn a_large_answer_arrives_whole() {
    locally(async {
        const SIZE: usize = 4 << 20;
        let server = serving(short(), |_request, _interim| -> Answering {
            Box::pin(async move {
                // A pattern that no two nearby datagrams share, so that a run cut wrong or
                // out of order shows.
                let body: Vec<u8> = (0..SIZE).map(|at| (at % 251) as u8).collect();
                Answered::Map(Response::new(Full::new(Bytes::from(body))))
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let answer = client.get("a.test", "/big").await;
        assert_eq!(answer.final_status(), Some("200"));
        assert_eq!(answer.body.len(), SIZE);
        assert!(
            answer
                .body
                .iter()
                .enumerate()
                .all(|(at, &byte)| usize::from(byte) == at % 251)
        );
    });
}

/// A connection that finds no room in the socket goes on at once if room is back by the time
/// it waits for it, as it may be: a wait that is over before it began is not lost. The client
/// gives the server's retransmission a long wait, so that it does not rescue the answer.
#[test]
fn room_back_before_the_wait_is_not_missed() {
    locally(async {
        let settings = Settings {
            keep_alive: Duration::from_secs(10),
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::connect_with(server.address, "a.test", |config| {
            config.set_max_ack_delay(2_000);
        })
        .await;
        client.for_a_while(Duration::from_millis(50)).await;
        server.shared.sending.refuse_briefly.set(1);
        let id = client.request(&get("a.test", "/x"), true);
        client.flush().await;
        let asked = tokio::time::Instant::now();
        while !client
            .answers
            .get(&id)
            .is_some_and(|answer| answer.finished)
            && asked.elapsed() < Duration::from_millis(500)
        {
            client.hear_for(Duration::from_millis(10)).await;
        }
        assert_eq!(server.shared.sending.refuse_briefly.get(), 0);
        let answer = client.answers.get(&id).cloned().unwrap_or_default();
        assert_eq!(
            body_of(&answer),
            "GET /x 0 None",
            "left waiting: {answer:?}"
        );
    });
}

/// Connections that find no room in the socket they share all go on once it has some
/// (16 §4): each waits for it on its own, where tokio keeps a single waker for
/// `poll_send_ready` and a connection that waited there before another was forgotten. On
/// Linux, where a send on the socket is what tells epoll it has room. The clients give the
/// server's retransmission a long wait, so that it does not rescue a connection left
/// waiting.
#[cfg(target_os = "linux")]
#[test]
fn connections_that_found_no_room_all_go_on_when_there_is() {
    locally(async {
        let started = Rc::new(Cell::new(0));
        let gate = Rc::new(Notify::new());
        let (counting, opening) = (Rc::clone(&started), Rc::clone(&gate));
        let settings = Settings {
            keep_alive: Duration::from_secs(10),
            ..short()
        };
        let server = serving(settings, move |request, interim| -> Answering {
            counting.set(counting.get() + 1);
            let gate = Rc::clone(&opening);
            Box::pin(async move {
                gate.notified().await;
                echo(request, interim).await
            })
        })
        .await;
        let patient = |config: &mut quiche::Config| config.set_max_ack_delay(2_000);
        let mut clients = [
            Client::connect_with(server.address, "a.test", patient).await,
            Client::connect_with(server.address, "a.test", patient).await,
        ];
        let mut ids = Vec::new();
        for client in &mut clients {
            ids.push(client.request(&get("a.test", "/x"), true));
            client.flush().await;
        }
        clients[0].until(|_| started.get() == 2).await;
        for client in &mut clients {
            client.for_a_while(Duration::from_millis(50)).await;
        }

        // Both answers are made while the socket is full, and wait.
        server.shared.sending.full.set(true);
        gate.notify_waiters();
        for client in &mut clients {
            client.hear_for(Duration::from_millis(50)).await;
        }
        let stalls = |clients: &[Client]| {
            clients
                .iter()
                .map(|client| client.stalls.note())
                .collect::<String>()
        };
        assert!(
            server.shared.sending.refused.get() >= 2,
            "the answers were not made while the socket was full{}",
            stalls(&clients)
        );
        assert!(clients.iter().all(|client| client.answers.is_empty()));

        // Room again: the kernel says so to whoever asked.
        server.shared.sending.full.set(false);
        let sink = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket2::SockRef::from(&server.shared.socket)
            .send_to(b"room", &sink.local_addr().unwrap().into())
            .unwrap();
        let finished = |clients: &[Client]| {
            clients
                .iter()
                .zip(&ids)
                .all(|(client, id)| client.answers.get(id).is_some_and(|answer| answer.finished))
        };
        let room = tokio::time::Instant::now();
        while !finished(&clients) && room.elapsed() < Duration::from_millis(500) {
            for client in &mut clients {
                client.hear_for(Duration::from_millis(10)).await;
            }
        }
        for (client, id) in clients.iter().zip(&ids) {
            let answer = client.answers.get(id).cloned().unwrap_or_default();
            assert_eq!(
                body_of(&answer),
                "GET /x 0 None",
                "left waiting: {answer:?}{}",
                client.stalls.note()
            );
        }
    });
}

/// An answer crosses a path that carries no datagram larger than 1,232 bytes, IPv6's
/// minimum MTU of 1,280 less its headers, and so does the next after the client's NAT
/// rebinds it: every datagram is of a size every path carries (16 §6).
#[test]
fn an_answer_crosses_a_path_of_the_smallest_mtu() {
    locally(async {
        const SIZE: usize = 64 << 10;
        let server = serving(Settings::default(), |_request, _interim| -> Answering {
            Box::pin(async { Answered::Map(Response::new(Full::new(Bytes::from(vec![42; SIZE])))) })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        client.path_carries = Some(1_232);
        for path in ["/first", "/rebound"] {
            if path == "/rebound" {
                client.rebind().await;
            }
            let id = client.request(&get("a.test", path), true);
            client.flush().await;
            let asked = tokio::time::Instant::now();
            while !client
                .answers
                .get(&id)
                .is_some_and(|answer| answer.finished)
                && asked.elapsed() < Duration::from_secs(3)
            {
                client.for_a_while(Duration::from_millis(20)).await;
            }
            let answer = client.answers.get(&id).cloned().unwrap_or_default();
            let lost = client
                .received
                .iter()
                .filter(|datagram| datagram.len() > 1_232)
                .count();
            assert_eq!(
                answer.body.len(),
                SIZE,
                "{path}: {lost} datagrams too large for the path"
            );
        }
    });
}

/// A megabyte's upload, far past a single piece, reaches the core whole, and so does a
/// client's trailers after it.
#[test]
fn an_upload_and_its_trailers_reach_the_core() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/up");
        head[0].1 = "POST";
        let id = client.request(&head, false);
        client.body(id, &vec![7; 1 << 20], false).await;
        client.trailers(id, &[("x-sum", "7")]).await;
        let answer = client.answer(id).await;
        assert_eq!(body_of(&answer), r#"POST /up 1048576 Some("7")"#);
    });
}

/// A body whose trailers the core does not want still reads them, to the message's end,
/// and ends where they were (03 §11).
#[test]
fn an_upload_whose_trailers_are_dropped_ends_where_they_were() {
    locally(async {
        let server = serving(short(), |request: Request<RequestBody>, interim| {
            let (head, mut body) = request.into_parts();
            body.drop_trailers();
            echo(Request::from_parts(head, body), interim)
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/up");
        head[0].1 = "POST";
        let id = client.request(&head, false);
        client.body(id, &vec![7; 1 << 16], false).await;
        client.trailers(id, &[("x-sum", "7")]).await;
        let answer = client.answer(id).await;
        assert_eq!(body_of(&answer), "POST /up 65536 None");
    });
}

/// The POST of a body that is to come.
fn post(path: &str) -> Vec<(&str, &str)> {
    let mut head = get("a.test", path);
    head[0].1 = "POST";
    head
}

/// A worker is charged what quiche holds for its connections — an upload not yet read, in
/// bytes and 128 bytes more for each piece it is held in — until it is read, and what is
/// left until the connection goes (16 §6).
#[test]
fn the_worker_is_charged_what_quiche_holds_for_it() {
    locally(async {
        const SIZE: usize = 100 << 10;
        let storage = Storage::new(LIMIT);
        let gate = Rc::new(Notify::new());
        let opening = Rc::clone(&gate);
        let server = serving_in(short(), Rc::clone(&storage), move |request, interim| {
            let gate = Rc::clone(&opening);
            Box::pin(async move {
                gate.notified().await;
                echo(request, interim).await
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let id = client.request(&post("/up"), false);
        client.body(id, &vec![1; SIZE], true).await;
        client.until(|_| storage.used() >= SIZE + 128).await;
        let charged = storage.used();
        assert!(charged < SIZE + (64 << 10), "{charged} charged");

        // Read, it is quiche's no more.
        gate.notify_one();
        assert_eq!(
            body_of(&client.answer(id).await),
            format!("POST /up {SIZE} None")
        );
        client.until(|_| storage.used() < 4 << 10).await;

        client.quic.close(true, code::NO_ERROR, b"").unwrap();
        client.flush().await;
        client.until(|_| storage.used() == 0).await;
    });
}

/// A head that has come in part is charged as it comes: quiche holds its frame whole until
/// it has all come.
#[test]
fn a_head_that_has_come_in_part_is_charged() {
    locally(async {
        let storage = Storage::new(LIMIT);
        let server = serving_in(short(), Rc::clone(&storage), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        client.for_a_while(Duration::from_millis(50)).await;
        let before = storage.used();
        // A HEADERS frame of 100 KiB, of which 50 KiB have come.
        let mut part = vec![0x01, 0x80, 0x01, 0x90, 0x00];
        part.extend(vec![0; 50 << 10]);
        assert_eq!(client.quic.stream_send(0, &part, false), Ok(part.len()));
        client.flush().await;
        client
            .until(|_| storage.used() >= before + (50 << 10))
            .await;
    });
}

/// A worker whose storage is full admits no new QUIC connection: its Initial is dropped, as
/// at the connection bound, and the client comes in once there is room (16 §6).
#[test]
fn a_full_worker_admits_no_new_connection() {
    locally(async {
        let storage = Storage::new(1 << 20);
        let server = serving_in(short(), Rc::clone(&storage), echo).await;
        let full = storage.reserve(1 << 20).unwrap();
        let mut client = Client::new(server.address, "a.test").await;
        client.for_a_while(Duration::from_millis(300)).await;
        assert!(!client.quic.is_established());
        assert_eq!(server.shared.connections.get(), 0);

        drop(full);
        client.until(|client| client.quic.is_established()).await;
    });
}

/// A connection that asks for nothing more is not closed while the worker's own answers take
/// its storage past the limit, as they may (14 §8): only more is refused there.
#[test]
fn a_connection_asking_nothing_more_is_kept_past_the_limit() {
    locally(async {
        let storage = Storage::with_provision(1 << 20, 1 << 20);
        let server = serving_in(short(), Rc::clone(&storage), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        assert_eq!(
            body_of(&client.get("a.test", "/one").await),
            "GET /one 0 None"
        );
        client.for_a_while(Duration::from_millis(50)).await;
        let _answers = storage
            .reserve_answer((1 << 20) - storage.used() + 1)
            .unwrap();
        client.quic.send_ack_eliciting().unwrap();
        client.for_a_while(Duration::from_millis(100)).await;
        assert_eq!(client.closed_by_server(), None);
    });
}

/// A worker that runs out closes its QUIC connection holding the most, with
/// H3_EXCESSIVE_LOAD, and lets go of its charge at once: the connection that asked for
/// more goes on (16 §6).
#[test]
fn a_worker_that_runs_out_closes_its_heaviest_connection() {
    locally(async {
        let storage = Storage::new(1 << 20);
        let exchanges = Exchanges::default();
        let server = serving_in(short(), Rc::clone(&storage), exchanges.core()).await;
        let mut heavy = Client::connect(server.address, "a.test").await;
        let mut light = Client::connect(server.address, "a.test").await;
        let id = heavy.request(&post("/heavy"), false);
        heavy.body(id, &vec![1; 700 << 10], false).await;
        heavy.until(|_| storage.used() >= 700 << 10).await;

        // Both driven, so that what the server's socket had no room for is sent again.
        let id = light.request(&post("/light"), false);
        light.body(id, &vec![1; 400 << 10], false).await;
        let started = tokio::time::Instant::now();
        while heavy.closed_by_server().is_none() {
            assert!(started.elapsed() < PATIENCE, "the heaviest was not closed");
            light.for_a_while(Duration::from_millis(20)).await;
            heavy.for_a_while(Duration::from_millis(20)).await;
        }
        assert_eq!(heavy.closed_by_server(), Some((true, code::EXCESSIVE_LOAD)));
        light.for_a_while(Duration::from_millis(100)).await;
        assert_eq!(light.closed_by_server(), None);
        assert!(storage.used() <= 1 << 20);
    });
}

/// An upload that waits in quiche is read out a piece of 16 KiB at a time, however much of
/// it has come: what one read takes, and the credit it gives back, stay within a piece.
#[test]
fn an_upload_is_read_a_piece_at_a_time() {
    locally(async {
        let server = serving(short(), |mut request, _interim| -> Answering {
            Box::pin(async move {
                // Everything is in quiche by the time the first piece is read.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let frame = request.body_mut().frame().await.unwrap().unwrap();
                let length = frame.into_data().unwrap().len();
                Answered::Map(Response::new(Full::new(Bytes::from(length.to_string()))))
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/up");
        head[0].1 = "POST";
        let id = client.request(&head, false);
        client.body(id, &vec![42; 64 << 10], true).await;
        let answer = client.answer(id).await;
        assert_eq!(body_of(&answer), (16 << 10).to_string());
    });
}

/// The echo core, which also keeps what it would have said, for a stream reset before its
/// answer could go.
fn heard_echo(
    heard: &Rc<RefCell<Vec<String>>>,
) -> impl Fn(Request<RequestBody>, Interim) -> Answering + 'static {
    let heard = Rc::clone(heard);
    move |request, interim| {
        let heard = Rc::clone(&heard);
        Box::pin(async move {
            let said = echo(request, interim)
                .await
                .into_response()
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes();
            heard
                .borrow_mut()
                .push(String::from_utf8(said.to_vec()).unwrap());
            Answered::Map(Response::new(Full::new(said)))
        })
    }
}

/// A body is held to the length its head declared, both ways, an empty DATA frame that ends
/// the stream short of it included, and its trailers are held to the rules of a head: a
/// body that breaks either is malformed (RFC 9114 §4.1.2). The core's read fails, and the
/// stream is reset with H3_MESSAGE_ERROR, as HTTP/2's is with PROTOCOL_ERROR.
#[test]
fn a_body_that_is_not_the_length_declared_fails() {
    locally(async {
        let heard = Rc::new(RefCell::new(Vec::new()));
        let server = serving(short(), heard_echo(&heard)).await;
        let mut client = Client::connect(server.address, "a.test").await;
        // The length declared, the bytes sent, and the trailers after them.
        type Case = (&'static str, usize, &'static [(&'static str, &'static str)]);
        let cases: [Case; 4] = [
            ("10", 11, &[]),
            ("10", 9, &[]),
            ("10", 0, &[]),
            ("1", 1, &[("connection", "close")]),
        ];
        for (at, (declared, sent, trailers)) in cases.into_iter().enumerate() {
            let mut head = get("a.test", "/up");
            head[0].1 = "POST";
            head.push(("content-length", declared));
            let id = client.request(&head, false);
            client.body(id, &vec![1; sent], trailers.is_empty()).await;
            if !trailers.is_empty() {
                client.trailers(id, trailers).await;
            }
            let answer = client.answer(id).await;
            let case = format!("{declared} declared, {sent} sent, trailers {trailers:?}");
            assert_eq!(answer.reset, Some(code::MESSAGE_ERROR), "{case}");
            client.until(|_| heard.borrow().len() > at).await;
            let said = heard.borrow()[at].clone();
            assert!(said.starts_with("invalid:"), "{case}: {said}");
        }
    });
}

/// A request whose head ends the stream while its `Content-Length` declares a body is
/// malformed (RFC 9114 §4.1.2): reset with H3_MESSAGE_ERROR before the core sees it, as
/// HTTP/2 resets one, and the connection goes on. A length of 0 declares none.
#[test]
fn a_request_whose_head_ends_it_but_declares_a_body_is_reset() {
    locally(async {
        let asked = Rc::new(Cell::new(0));
        let counting = Rc::clone(&asked);
        let server = serving(short(), move |request, interim| {
            counting.set(counting.get() + 1);
            echo(request, interim)
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/up");
        head[0].1 = "POST";
        head.push(("content-length", "10"));
        let id = client.request(&head, true);
        let answer = client.answer(id).await;
        assert_eq!(answer.reset, Some(code::MESSAGE_ERROR));
        assert!(answer.heads.is_empty());
        assert_eq!(asked.get(), 0, "the core was asked");

        head.last_mut().unwrap().1 = "0";
        let id = client.request(&head, true);
        assert_eq!(body_of(&client.answer(id).await), "POST /up 0 None");
    });
}

/// A head merely past the limit is answered 431, and the connection goes on; a malformed
/// one is reset, and the connection goes on.
#[test]
fn a_head_too_large_is_answered_431_and_a_malformed_one_reset() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let large = "v".repeat(100 << 10);
        let mut head = get("a.test", "/big");
        head.push(("x-large", &large));
        let id = client.request(&head, true);
        let answer = client.answer(id).await;
        assert_eq!(answer.final_status(), Some("431"));

        // A field only a connection has, which HTTP/3 carries in no message (RFC 9114 §4.2).
        let mut head = get("a.test", "/connection");
        head.push(("connection", "close"));
        let id = client.request(&head, true);
        let answer = client.answer(id).await;
        assert_eq!(answer.reset, Some(code::MESSAGE_ERROR));
        assert!(answer.heads.is_empty());

        let answer = client.get("a.test", "/after").await;
        assert_eq!(body_of(&answer), "GET /after 0 None");
    });
}

/// A client that resets its request before it is answered takes the request's exchange
/// with it: the core's future is dropped where it stands.
#[test]
fn a_request_the_client_resets_is_let_go_of() {
    locally(async {
        let dropped = Rc::new(Cell::new(false));
        let seen = Rc::clone(&dropped);
        let server = serving(short(), move |_request, _interim| -> Answering {
            struct Flag(Rc<Cell<bool>>);
            impl Drop for Flag {
                fn drop(&mut self) {
                    self.0.set(true);
                }
            }
            let flag = Flag(Rc::clone(&seen));
            Box::pin(async move {
                let _flag = flag;
                std::future::pending::<()>().await;
                unreachable!()
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let id = client.request(&get("a.test", "/never"), false);
        client.for_a_while(Duration::from_millis(100)).await;
        assert!(!dropped.get());
        client
            .quic
            .stream_shutdown(id, quiche::Shutdown::Write, code::REQUEST_CANCELLED)
            .unwrap();
        client
            .quic
            .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
            .unwrap();
        client.for_a_while(Duration::from_millis(200)).await;
        assert!(dropped.get(), "the exchange outlived its request");
    });
}

/// A rapid reset (CVE-2023-44487): requests asked and given up at once, each let settle so
/// that the server gives the stream back. Once the connection has had its judged number of
/// requests, half or more of them given up before their answer's head, it is closed with
/// H3_EXCESSIVE_LOAD, as HTTP/2 closes one with ENHANCE_YOUR_CALM (15 §3).
#[test]
fn a_rapid_reset_is_cut_off_by_its_share_of_early_resets() {
    locally(async {
        let settings = Settings {
            reset_judged_after: 50,
            ..short()
        };
        let exchanges = Exchanges::default();
        let server = serving(settings, exchanges.core()).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut cut_off = None;
        for asked in 1..=100 {
            client
                .until(|client| {
                    client.quic.peer_streams_left_bidi() > 0 || client.closed_by_server().is_some()
                })
                .await;
            if client.closed_by_server().is_some() {
                cut_off = Some(asked - 1);
                break;
            }
            let id = client.request(&get("a.test", "/held"), false);
            client.until(|_| exchanges.started.get() == asked).await;
            for side in [quiche::Shutdown::Write, quiche::Shutdown::Read] {
                client
                    .quic
                    .stream_shutdown(id, side, code::REQUEST_CANCELLED)
                    .unwrap();
            }
            client.for_a_while(Duration::from_millis(5)).await;
        }
        let asked = cut_off.expect("never cut off");
        assert!((50..=55).contains(&asked), "cut off after {asked} requests");
        assert_eq!(
            client.closed_by_server(),
            Some((true, code::EXCESSIVE_LOAD))
        );
    });
}

/// Half given up early is enough: a client that has every other request answered, and gives
/// up the rest, is closed once it has had the judged number.
#[test]
fn half_the_requests_given_up_early_is_a_rapid_reset() {
    locally(async {
        let settings = Settings {
            reset_judged_after: 50,
            ..short()
        };
        let server = serving(settings, |request, interim| -> Answering {
            if request.uri().path() == "/held" {
                return Box::pin(std::future::pending());
            }
            echo(request, interim)
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        for asked in 1..=50 {
            if asked % 2 == 1 {
                client.get("a.test", "/").await;
                continue;
            }
            let id = client.request(&get("a.test", "/held"), true);
            client.for_a_while(Duration::from_millis(5)).await;
            client
                .quic
                .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
                .unwrap();
        }
        client
            .until(|client| client.closed_by_server().is_some())
            .await;
        assert_eq!(
            client.closed_by_server(),
            Some((true, code::EXCESSIVE_LOAD))
        );
    });
}

/// A request the client stopped before the server had seen its head is given up all the
/// same: its exchange goes, and it counts towards a rapid reset. quiche tells of a stop
/// once, and here before the stream has a task to tell: the stop came with the head, or
/// the head's packet was lost and sent again after it. By the time the head is read,
/// quiche has let go of a stream the head ended, both its sides done, and still holds one
/// whose body is to follow.
#[test]
fn a_request_stopped_before_its_head_was_seen_is_given_up() {
    for (ended, head_lost) in [(true, false), (true, true), (false, true)] {
        locally(stopped_before_its_head(ended, head_lost));
    }
}

/// The client stops a request whose head `ended` the stream or had a body to follow: in
/// the same flush as the head, or after the head's packet was lost, which then goes again.
async fn stopped_before_its_head(ended: bool, head_lost: bool) {
    // The server's own deadlines, not the tests' short ones: a lost head goes again at the
    // client's loss timer, which a loaded machine stretches past a short first-request one.
    let settings = Settings {
        reset_judged_after: 1,
        ..Settings::default()
    };
    let exchanges = Exchanges::default();
    let server = serving(settings, exchanges.core()).await;
    let mut client = Client::connect(server.address, "a.test").await;
    let lost = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let method = if ended { "GET" } else { "POST" };
    let head = [
        (":method", method),
        (":scheme", "https"),
        (":authority", "a.test"),
        (":path", "/"),
    ];
    let id = client.request(&head, ended);
    if head_lost {
        client.send_to = Some(lost.local_addr().unwrap());
        client.flush().await;
        client.send_to = None;
    }
    client
        .quic
        .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
        .unwrap();
    client
        .until(|client| client.closed_by_server().is_some())
        .await;
    let case = format!("ended: {ended}, head lost: {head_lost}");
    assert_eq!(
        client.closed_by_server(),
        Some((true, code::EXCESSIVE_LOAD)),
        "{case}"
    );
    assert_eq!(exchanges.alive.get(), 0, "the exchange went on; {case}");
}

/// A request given up while its answer's head waits for room to be sent is given up early
/// too: a client that grants an answer no room and then cancels is judged as one that
/// cancels at once.
#[test]
fn a_request_given_up_before_its_head_had_room_counts_as_early() {
    locally(async {
        let settings = Settings {
            reset_judged_after: 10,
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::connect_with(server.address, "a.test", |config| {
            // No room on the streams it opens for any answer.
            config.set_initial_max_stream_data_bidi_local(0);
        })
        .await;
        for _ in 0..10 {
            let id = client.request(&get("a.test", "/"), true);
            client.for_a_while(Duration::from_millis(20)).await;
            assert!(!client.answers.contains_key(&id), "the head went");
            client
                .quic
                .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
                .unwrap();
        }
        client
            .until(|client| client.closed_by_server().is_some())
            .await;
        assert_eq!(
            client.closed_by_server(),
            Some((true, code::EXCESSIVE_LOAD))
        );
    });
}

/// An answer the server gives up, its head given no room for the stream's idle bound, is not
/// given up by the client, and does not count towards the rule.
#[test]
fn an_answer_the_server_gives_up_is_not_counted_against_the_client() {
    locally(async {
        let settings = Settings {
            reset_judged_after: 2,
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::connect_with(server.address, "a.test", |config| {
            config.set_initial_max_stream_data_bidi_local(0);
        })
        .await;
        let ids = [
            client.request(&get("a.test", "/"), true),
            client.request(&get("a.test", "/"), true),
        ];
        client
            .until(|client| {
                ids.iter().all(|id| {
                    client
                        .answers
                        .get(id)
                        .is_some_and(|answer| answer.reset.is_some())
                })
            })
            .await;
        client.for_a_while(Duration::from_millis(100)).await;
        assert_ne!(
            client.closed_by_server(),
            Some((true, code::EXCESSIVE_LOAD))
        );
    });
}

/// A client that gives up a request now and then — one in ten — is nowhere near the rule,
/// and keeps its connection.
#[test]
fn a_client_that_cancels_now_and_then_keeps_its_connection() {
    locally(async {
        let settings = Settings {
            reset_judged_after: 50,
            ..short()
        };
        let server = serving(settings, |request, interim| -> Answering {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                echo(request, interim).await
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        for asked in 0..100 {
            let id = client.request(&get("a.test", "/"), true);
            if asked % 10 == 0 {
                client.flush().await;
                client
                    .quic
                    .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
                    .unwrap();
                client.for_a_while(Duration::from_millis(5)).await;
            } else {
                client.answer(id).await;
            }
        }
        assert_eq!(client.closed_by_server(), None);
    });
}

/// A client that stops reading an answer takes its exchange with it, whether or not its
/// request was whole (RFC 9114 §4.1.1): nothing waits on an upstream for an answer nobody
/// wants. quiche answers the STOP_SENDING with a RESET_STREAM of its own, with the client's
/// code, and the server sends no second one.
#[test]
fn an_answer_the_client_stops_reading_is_given_up() {
    locally(async {
        let exchanges = Exchanges::default();
        let server = serving(short(), exchanges.core()).await;
        let mut client = Client::connect(server.address, "a.test").await;
        for (asked, ended) in [(1, true), (2, false)] {
            let id = client.request(&get("a.test", "/never"), ended);
            client.until(|_| exchanges.started.get() == asked).await;
            let resets = server.stats().reset_stream_count_local;
            client
                .quic
                .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
                .unwrap();
            client.until(|_| exchanges.alive.get() == 0).await;
            assert_eq!(
                server.stats().reset_stream_count_local,
                resets + 1,
                "request ended: {ended}"
            );
        }
    });
}

/// However often a client asks and then stops reading, it holds no more exchanges at once
/// than it has streams. quiche gives a stopped stream's credit back once its RESET_STREAM is
/// acknowledged, so the exchange has to go with it, or the stream bound bounds nothing.
#[test]
fn a_client_that_asks_and_stops_again_and_again_holds_no_more_than_its_streams() {
    locally(async {
        let settings = Settings {
            streams: 4,
            ..short()
        };
        let exchanges = Exchanges::default();
        let server = serving(settings, exchanges.core()).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let asks = 3 * usize::try_from(settings.streams).unwrap();
        for asked in 1..=asks {
            client
                .until(|client| client.quic.peer_streams_left_bidi() > 0)
                .await;
            let id = client.request(&get("a.test", "/never"), true);
            client.until(|_| exchanges.started.get() == asked).await;
            client
                .quic
                .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
                .unwrap();
        }
        assert!(
            exchanges.most.get() <= usize::try_from(settings.streams).unwrap(),
            "{} exchanges at once",
            exchanges.most.get()
        );
        client.until(|_| exchanges.alive.get() == 0).await;
    });
}

/// A client that resets its request has the answer's side of the stream reset too, so that
/// the stream ends both ways (RFC 9114 §4.1.1) and its credit comes back: however often it
/// does so, it is never left without a stream.
#[test]
fn a_request_the_client_resets_has_its_answer_reset_too() {
    locally(async {
        let settings = Settings {
            streams: 4,
            ..short()
        };
        let exchanges = Exchanges::default();
        let server = serving(settings, exchanges.core()).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let asks = 3 * usize::try_from(settings.streams).unwrap();
        for asked in 1..=asks {
            client
                .until(|client| client.quic.peer_streams_left_bidi() > 0)
                .await;
            let id = client.request(&get("a.test", "/never"), false);
            client.until(|_| exchanges.started.get() == asked).await;
            client
                .quic
                .stream_shutdown(id, quiche::Shutdown::Write, code::REQUEST_CANCELLED)
                .unwrap();
            let answer = client.answer(id).await;
            assert_eq!(answer.reset, Some(code::REQUEST_CANCELLED));
        }
        assert_eq!(exchanges.alive.get(), 0);
    });
}

/// An answer the core gives without reading the upload, whole before its request is, ends
/// the stream's sending side as any answer does and is not reset with the rest: only
/// reading stops (RFC 9114 §4.1).
#[test]
fn an_answer_whole_before_its_upload_arrives_whole() {
    locally(async {
        const SIZE: usize = 100 << 10;
        let server = serving(short(), |_request, _interim| -> Answering {
            Box::pin(
                async move { Answered::Map(Response::new(Full::new(Bytes::from(vec![7; SIZE])))) },
            )
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/up");
        head[0].1 = "POST";
        let id = client.request(&head, false);
        client.body(id, &[1; 1_000], false).await;
        let answer = client.answer(id).await;
        assert_eq!(answer.reset, None);
        assert_eq!(answer.body.len(), SIZE);
    });
}

/// Answers whole before their uploads give their streams back once the client has ended its
/// side, however many there are: a client of four streams is not left without one.
#[test]
fn answers_whole_before_their_uploads_give_their_streams_back() {
    locally(async {
        let settings = Settings {
            streams: 4,
            ..short()
        };
        let server = serving(settings, |_request, _interim| -> Answering {
            Box::pin(async { Answered::Map(Response::new(Full::new(Bytes::from("early")))) })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/up");
        head[0].1 = "POST";
        for _ in 0..3 * settings.streams {
            client
                .until(|client| client.quic.peer_streams_left_bidi() > 0)
                .await;
            let id = client.request(&head, false);
            client.body(id, &[1; 1_000], false).await;
            assert_eq!(body_of(&client.answer(id).await), "early");
        }
    });
}

/// A connection that sends no request is closed at its first-request deadline.
#[test]
fn a_connection_without_a_request_is_closed_at_its_deadline() {
    locally(async {
        let settings = short();
        let server = serving(settings, echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let started = tokio::time::Instant::now();
        client
            .until(|client| client.quic.is_closed() || client.quic.is_draining())
            .await;
        let took = started.elapsed();
        assert!(took <= settings.first_request + SLACK, "{took:?}");
        assert_eq!(client.closed_by_server(), Some((true, code::NO_ERROR)));
    });
}

/// The time to the first request counts from the handshake's end, not from the first
/// packet: a handshake slowed by loss leaves the request all of its time (16 §6).
#[test]
fn the_first_request_clock_starts_when_the_handshake_ends() {
    locally(async {
        let settings = Settings {
            handshake: Duration::from_millis(2_000),
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::new(server.address, "a.test").await;
        client.flush().await;
        // The handshake takes longer than the first request is given.
        tokio::time::sleep(settings.first_request + Duration::from_millis(200)).await;
        client.until(|client| client.quic.is_established()).await;
        let config = quiche::h3::Config::new().unwrap();
        client.h3 =
            Some(quiche::h3::Connection::with_transport(&mut client.quic, &config).unwrap());
        tokio::time::sleep(settings.first_request / 2).await;
        let answer = client.get("a.test", "/late").await;
        assert_eq!(body_of(&answer), "GET /late 0 None");
    });
}

/// A client that does not finish its handshake is closed at the handshake's bound, however
/// long the first request is given after it.
#[test]
fn a_handshake_not_finished_in_time_is_closed() {
    locally(async {
        let settings = Settings {
            handshake: Duration::from_millis(800),
            first_request: Duration::from_millis(3_000),
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::new(server.address, "a.test").await;
        client.flush().await;
        let started = tokio::time::Instant::now();
        while client.closed_by_server().is_none() {
            assert!(
                started.elapsed() < settings.first_request,
                "not closed at the handshake's bound"
            );
            client.hear_for(Duration::from_millis(20)).await;
        }
        let took = started.elapsed();
        assert!(
            took >= settings.handshake - Duration::from_millis(50),
            "{took:?}"
        );
        assert!(took <= settings.handshake + SLACK, "{took:?}");
    });
}

/// A connection with no request open for its keep-alive time is told to go and closed.
#[test]
fn an_idle_connection_is_told_to_go_at_its_keep_alive_deadline() {
    locally(async {
        let settings = short();
        let server = serving(settings, echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        client.get("a.test", "/").await;
        let started = tokio::time::Instant::now();
        client
            .until(|client| client.quic.is_closed() || client.quic.is_draining())
            .await;
        let took = started.elapsed();
        assert!(
            took >= settings.keep_alive - Duration::from_millis(50),
            "{took:?}"
        );
        assert!(took <= settings.keep_alive + SLACK, "{took:?}");
        assert_eq!(client.goaway, Some(4));
        assert_eq!(client.closed_by_server(), Some((true, code::NO_ERROR)));
    });
}

/// Draining, a connection is told which requests were not seen, finishes those that were,
/// and closes; the listener takes no new connection meanwhile.
#[test]
fn a_draining_connection_finishes_its_requests_then_closes() {
    locally(async {
        let gate = Rc::new(tokio::sync::Notify::new());
        let opening = Rc::clone(&gate);
        let server = serving(short(), move |request, interim| {
            let gate = Rc::clone(&opening);
            Box::pin(async move {
                gate.notified().await;
                echo(request, interim).await
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let id = client.request(&get("a.test", "/slow"), true);
        client.for_a_while(Duration::from_millis(100)).await;
        server.drain.start();
        client.until(|client| client.goaway.is_some()).await;
        assert_eq!(client.goaway, Some(4), "the request in hand was seen");
        gate.notify_one();
        let answer = client.answer(id).await;
        assert_eq!(body_of(&answer), "GET /slow 0 None");
        client
            .until(|client| client.quic.is_closed() || client.quic.is_draining())
            .await;

        let mut late = Client::new(server.address, "a.test").await;
        late.for_a_while(Duration::from_millis(300)).await;
        assert!(
            !late.quic.is_established(),
            "a draining listener took a connection"
        );
    });
}

/// An answer's body in pieces, each sent once its gate opens. A piece of no bytes sends
/// nothing, so that the body's end goes alone.
fn gated(
    pieces: Vec<(Rc<Notify>, Option<Bytes>)>,
) -> impl Body<Data = Bytes, Error = Infallible> + 'static {
    StreamBody::new(
        tokio_stream::iter(pieces)
            .then(|(gate, piece)| async move {
                gate.notified().await;
                piece
            })
            .filter_map(|piece| piece.map(|bytes| Ok(Frame::data(bytes)))),
    )
}

/// Which packet of a drained answer is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loss {
    First,
    Last,
    EndAlone,
}

/// Draining, a connection closes once its answers have arrived, or at the drain's bound:
/// closing ends what quiche would send again, so an answer's lost packet — its first, its
/// last, or one carrying the stream's end alone — is sent again first (16 §4).
#[test]
fn a_draining_connection_closes_once_its_answers_have_arrived() {
    const PART: usize = 4_000;
    const TAIL: usize = 1_000;
    for loss in [Loss::First, Loss::Last, Loss::EndAlone] {
        locally(async move {
            let gates = [Rc::new(Notify::new()), Rc::new(Notify::new())];
            let mut pieces = vec![(Rc::clone(&gates[0]), Some(Bytes::from(vec![1; PART])))];
            let whole = match loss {
                Loss::First => PART,
                Loss::Last => {
                    pieces.push((Rc::clone(&gates[1]), Some(Bytes::from(vec![2; TAIL]))));
                    PART + TAIL
                }
                Loss::EndAlone => {
                    pieces.push((Rc::clone(&gates[1]), None));
                    PART
                }
            };
            let settings = Settings {
                drain_within: Duration::from_secs(5),
                ..short()
            };
            let server = serving(settings, move |_request, _interim| {
                let pieces = pieces.clone();
                async move { Answered::Map(Response::new(gated(pieces))) }
            })
            .await;
            let mut client = Client::connect(server.address, "a.test").await;
            let id = client.request(&get("a.test", "/drained"), true);
            client.for_a_while(Duration::from_millis(100)).await;
            server.drain.start();
            client.until(|client| client.goaway.is_some()).await;
            client.for_a_while(Duration::from_millis(50)).await;
            if loss != Loss::First {
                gates[0].notify_one();
                client
                    .until(|client| {
                        client
                            .answers
                            .get(&id)
                            .is_some_and(|answer| answer.body.len() == PART)
                    })
                    .await;
                client.for_a_while(Duration::from_millis(50)).await;
            }
            client.lose_next = true;
            gates[usize::from(loss != Loss::First)].notify_one();
            client
                .until(|client| {
                    client.quic.is_closed()
                        || client.quic.is_draining()
                        || client
                            .answers
                            .get(&id)
                            .is_some_and(|answer| answer.finished)
                })
                .await;
            let answer = client.answers.get(&id).cloned().unwrap_or_default();
            assert!(
                answer.finished,
                "{loss:?}: cut at {} bytes, closed with {:?}",
                answer.body.len(),
                client.closed_by_server()
            );
            assert_eq!(answer.body.len(), whole, "{loss:?}");
            client
                .until(|client| client.quic.is_closed() || client.quic.is_draining())
                .await;
            assert_eq!(client.closed_by_server(), Some((true, code::NO_ERROR)));
            // Its last connection gone, the listener ends and lets its socket go.
            client
                .until(|_| Rc::strong_count(&server.shared) == 1)
                .await;
        });
    }
}

/// Draining, a 431 the driver answered itself, with no task behind it, is waited for as any
/// answer is.
#[test]
fn a_draining_connection_waits_for_its_431_too() {
    locally(async {
        let settings = Settings {
            head_limit: 256,
            field_section: 1 << 10,
            drain_within: Duration::from_secs(5),
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        client.for_a_while(Duration::from_millis(50)).await;
        let large = "v".repeat(400);
        let mut head = get("a.test", "/large");
        head.push(("x-large", &large));
        client.lose_next = true;
        let id = client.request(&head, true);
        client.until(|client| !client.lose_next).await;
        server.drain.start();
        client
            .until(|client| {
                client.quic.is_closed()
                    || client.quic.is_draining()
                    || client
                        .answers
                        .get(&id)
                        .is_some_and(|answer| answer.finished)
            })
            .await;
        let answer = client.answers.get(&id).cloned().unwrap_or_default();
        assert_eq!(answer.final_status(), Some("431"));
        assert!(answer.finished);
    });
}

/// Draining, the listener reads its socket as it did before (16 §4): an upload under way
/// is read, and echoed back, a piece at a time without waiting.
#[test]
fn a_draining_listener_reads_what_comes_at_once() {
    locally(async {
        let settings = Settings {
            drain_within: Duration::from_secs(5),
            ..short()
        };
        let server = serving(
            settings,
            |request: Request<RequestBody>, _interim| async move {
                Answered::Map(Response::new(request.into_body()))
            },
        )
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let mut head = get("a.test", "/echo");
        head[0].1 = "POST";
        let id = client.request(&head, false);
        let echoed = |client: &Client, length: usize| {
            client
                .answers
                .get(&id)
                .is_some_and(|answer| answer.body.len() == length)
        };
        client.body(id, b"0", false).await;
        client.until(|client| echoed(client, 1)).await;
        server.drain.start();
        client.until(|client| client.goaway.is_some()).await;
        let started = tokio::time::Instant::now();
        for length in 2..=11 {
            client.body(id, b"x", false).await;
            client.until(|client| echoed(client, length)).await;
        }
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(250),
            "ten round trips took {took:?}"
        );
        client.body(id, b"", true).await;
        assert_eq!(client.answer(id).await.body.len(), 11);
    });
}

/// A client that speaks a version the server does not is told which it does, and one whose
/// first datagram is smaller than QUIC allows is not answered at all.
#[test]
fn an_unknown_version_is_negotiated_and_a_short_initial_ignored() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::new(server.address, "a.test").await;
        let mut datagram = vec![0xc0, 0x1a, 0x2a, 0x3a, 0x4a, 8];
        datagram.extend([0xd1; 8]);
        datagram.push(8);
        datagram.extend([0x51; 8]);
        datagram.resize(1_200, 0);
        client.send_raw(&datagram, server.address).await;
        let answered = client.read_raw(Duration::from_millis(300)).await;
        assert_eq!(answered.len(), 1, "one version negotiation");
        let negotiation = &answered[0];
        assert_eq!(&negotiation[1..5], &[0, 0, 0, 0], "version 0");
        assert!(
            negotiation.windows(4).any(|word| word == [0, 0, 0, 1]),
            "offers version 1"
        );

        // A real client's first datagram, cut short of the 1,200 bytes RFC 9000 §14.1 asks.
        let mut out = vec![0; 1_500];
        let (len, _) = client.quic.send(&mut out).unwrap();
        assert!(len >= 1_200);
        client.send_raw(&out[..1_000], server.address).await;
        assert!(client.read_raw(Duration::from_millis(300)).await.is_empty());
        assert_eq!(server.shared.connections.get(), 0);
    });
}

/// Past its handshakes' threshold, a listener sends a Retry first, and a client that comes
/// back with its token is served.
#[test]
fn past_the_threshold_a_client_proves_its_address_first() {
    locally(async {
        let settings = Settings {
            retry_above: 0,
            ..short()
        };
        let server = serving(settings, echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let first = &client.received[0];
        // A long header of version 1 whose type is Retry (RFC 9000 §17.2.5).
        assert_eq!(first[0] & 0xf0, 0xf0, "{:#x}", first[0]);
        let answer = client.get("a.test", "/retried").await;
        assert_eq!(body_of(&answer), "GET /retried 0 None");
    });
}

/// A client whose datagrams land on another worker's socket, from a new port as after a
/// NAT rebinding, is served by its own connection all the same: the other worker reads the
/// owner from the ID and hands each datagram over, once (16 §3). A NAT rebinds a client
/// most often once it has been quiet: this one, for a second. The workers keep their own
/// deadlines, not the tests' short ones: one that closed the connection meanwhile would
/// tell only the address the client had left.
#[test]
fn a_client_that_lands_on_another_worker_is_served_by_its_own() {
    locally(async {
        let secrets = Secrets::new().unwrap();
        let mut group = Forwarding::group(2);
        let other = group.remove(1);
        let owner = group.remove(0);
        let owner = serving_as(Settings::default(), echo, &secrets, owner).await;
        let other = serving_as(Settings::default(), echo, &secrets, other).await;
        let mut client = Client::connect(owner.address, "a.test").await;
        assert_eq!(
            body_of(&client.get("a.test", "/here").await),
            "GET /here 0 None"
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
        client.rebind().await;
        client.send_to = Some(other.address);
        let answer = client.get("a.test", "/astray").await;
        assert_eq!(body_of(&answer), "GET /astray 0 None");
        assert!(other.shared.forwarded.get() > 0, "nothing was forwarded");
        assert_eq!(other.shared.dropped.get(), 0);
        assert_eq!(
            other.shared.connections.get(),
            0,
            "the other worker took it on"
        );
        assert_eq!(owner.shared.connections.get(), 1);
    });
}

/// A client whose NAT rebinds it again and again, the client none the wiser, is followed to
/// each new address (16 §3): quiche keeps two paths, and each new one takes the place of
/// the one before. This client has no spare ID to give, so each new path shares the ID of
/// the path it follows. A NAT rebinds a client most often once it has been quiet: once
/// here, for a second. The server keeps its own deadlines, not the tests' short ones: one
/// that closed the connection meanwhile would tell only the address the client had left.
#[test]
fn a_client_rebound_again_and_again_is_followed() {
    locally(async {
        let server = serving(Settings::default(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        for rebinding in 0..5 {
            if rebinding == 2 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            client.rebind().await;
            let path = format!("/{rebinding}");
            let answer = client.get("a.test", &path).await;
            assert_eq!(body_of(&answer), format!("GET {path} 0 None"));
        }
    });
}

/// A client rebound by its NAT while a large answer is on its way, the client none the
/// wiser, still gets all of it: what was in flight to the old address is sent again to
/// the new one.
#[test]
fn a_client_rebound_during_a_large_answer_gets_it_whole() {
    locally(async {
        const SIZE: usize = 4 << 20;
        let server = serving(short(), |_request, _interim| -> Answering {
            Box::pin(async move {
                let body: Vec<u8> = (0..SIZE).map(|at| (at % 251) as u8).collect();
                Answered::Map(Response::new(Full::new(Bytes::from(body))))
            })
        })
        .await;
        let mut client = Client::connect(server.address, "a.test").await;
        let id = client.request(&get("a.test", "/big"), true);
        client
            .until(|client| {
                client
                    .answers
                    .get(&id)
                    .is_some_and(|answer| answer.body.len() > 256 << 10)
            })
            .await;
        client.rebind().await;
        // The client says something from its new address, as a client does whose
        // acknowledgements are owed: nothing else would tell the server.
        client.quic.send_ack_eliciting().unwrap();
        client.flush().await;
        let answer = client.answer(id).await;
        assert_eq!(answer.body.len(), SIZE);
    });
}

/// The client notes a stretch in which the test did not run while it waited, for a failure
/// to say that the machine stood it still rather than that the server did nothing.
#[test]
fn the_client_notes_a_stall_while_it_waits() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        let id = client.request(&get("a.test", "/x"), true);
        let stood = Cell::new(false);
        client
            .until(|client| {
                // The thread put to sleep once mid-wait, as a stalled machine would hold it.
                if !stood.replace(true) {
                    std::thread::sleep(Duration::from_millis(400));
                }
                client
                    .answers
                    .get(&id)
                    .is_some_and(|answer| answer.finished)
            })
            .await;
        let pause = client.stalls.longest().expect("no stall noted");
        assert!(pause.gap >= Duration::from_millis(400), "{pause:?}");
        if cfg!(target_os = "linux") {
            assert!(client.stalls.note().contains("stood still"), "{pause:?}");
        }
    });
}

// HTTP/0.9 over QUIC, as quic-interop-runner's transport cases speak it (16 §8).

/// A GET's line goes to the core as an HTTP/3 GET of the name the client asked for, and the
/// answer's body comes back alone, then the stream's end.
#[test]
fn an_hq_get_is_answered_with_its_body_alone() {
    locally(async {
        let server = serving(short(), |request, _interim| -> Answering {
            let said = format!(
                "{} {} {:?}",
                request.method(),
                request.uri(),
                request.version()
            );
            Box::pin(async move { Answered::Map(Response::new(Full::new(Bytes::from(said)))) })
        })
        .await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        let id = client.hq_request(b"GET /x?y=1\r\n", true);
        let answer = client.answer(id).await;
        assert!(answer.finished);
        assert!(answer.heads.is_empty());
        assert_eq!(body_of(&answer), "GET https://a.test/x?y=1 HTTP/3.0");
    });
}

/// Requests on many streams at once are each answered on their own, and a line may come in
/// pieces, end with a bare line feed, or end with its stream.
#[test]
fn hq_requests_are_answered_however_their_lines_come() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        let ids: Vec<u64> = (0..20)
            .map(|at| client.hq_request(format!("GET /{at}\r\n").as_bytes(), true))
            .collect();
        for (at, id) in ids.into_iter().enumerate() {
            let answer = client.answer(id).await;
            assert_eq!(body_of(&answer), format!("GET /{at} 0 None"));
        }

        let split = client.hq_request(b"GET /sp", false);
        client.for_a_while(Duration::from_millis(100)).await;
        assert!(
            !client.answers.contains_key(&split),
            "half a line was taken"
        );
        client.quic.stream_send(split, b"lit\n", true).unwrap();
        assert_eq!(body_of(&client.answer(split).await), "GET /split 0 None");

        let bare = client.hq_request(b"GET /bare", true);
        assert_eq!(body_of(&client.answer(bare).await), "GET /bare 0 None");
    });
}

/// A line that is not a GET of a path, or that runs on past any path's length, has its
/// stream reset as a malformed HTTP/3 request's is, and the connection goes on.
#[test]
fn a_malformed_hq_request_is_reset() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        for line in [
            b"POST /x\r\n".as_slice(),
            b"GET x\r\n",
            b"GET /a b\r\n",
            b"GET /x HTTP/1.0\r\n",
            b"GET\r\n",
            b"\r\n",
            b"",
        ] {
            let id = client.hq_request(line, true);
            let answer = client.answer(id).await;
            let shown = String::from_utf8_lossy(line);
            assert_eq!(answer.reset, Some(code::MESSAGE_ERROR), "{shown:?}");
            assert!(answer.body.is_empty(), "{shown:?}");
        }
        // Not ended, and past any path's length: refused without waiting for the rest.
        let long = [b"GET /".as_slice(), &[b'a'; 9 << 10]].concat();
        let id = client.hq_request(&long, false);
        assert_eq!(client.answer(id).await.reset, Some(code::MESSAGE_ERROR));

        let id = client.hq_request(b"GET /after\r\n", true);
        assert_eq!(body_of(&client.answer(id).await), "GET /after 0 None");
    });
}

/// Four megabytes back, far past what quiche takes at once, arrive whole and in order: the
/// answer is sent as the stream makes room.
#[test]
fn a_large_hq_answer_arrives_whole() {
    locally(async {
        const SIZE: usize = 4 << 20;
        let server = serving(short(), |_request, _interim| -> Answering {
            Box::pin(async move {
                let body: Vec<u8> = (0..SIZE).map(|at| (at % 251) as u8).collect();
                Answered::Map(Response::new(Full::new(Bytes::from(body))))
            })
        })
        .await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        let id = client.hq_request(b"GET /big\r\n", true);
        let answer = client.answer(id).await;
        assert!(
            answer.finished,
            "reset {:?} after {} bytes{}",
            answer.reset,
            answer.body.len(),
            client.stalls.note()
        );
        assert_eq!(answer.body.len(), SIZE);
        assert!(
            answer
                .body
                .iter()
                .enumerate()
                .all(|(at, &byte)| usize::from(byte) == at % 251)
        );
    });
}

/// An answer whose body fails part way has its stream reset, never ended as if it were
/// whole.
#[test]
fn an_hq_answer_that_fails_is_reset() {
    locally(async {
        let server = serving(short(), |_request, _interim| async {
            let pieces = [
                Ok(Frame::data(Bytes::from_static(b"half"))),
                Err(std::io::Error::other("the upstream went")),
            ];
            Answered::Map(Response::new(StreamBody::new(tokio_stream::iter(pieces))))
        })
        .await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        let id = client.hq_request(b"GET /cut\r\n", true);
        let answer = client.answer(id).await;
        assert_eq!(answer.reset, Some(code::INTERNAL_ERROR));
        assert!(!answer.finished);
    });
}

/// A client that takes none of its answer for the stream's idle bound, once the stream's
/// window is full, has the stream reset.
#[test]
fn an_hq_answer_the_client_takes_none_of_is_reset() {
    locally(async {
        // Twice the client's stream window, which is all it grants while it reads nothing.
        const SIZE: usize = 8 << 20;
        let settings = short();
        let server = serving(settings, |_request, _interim| -> Answering {
            Box::pin(
                async move { Answered::Map(Response::new(Full::new(Bytes::from(vec![7; SIZE])))) },
            )
        })
        .await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        client.hq_unread = true;
        let id = client.hq_request(b"GET /big\r\n", true);
        client
            .until(|client| client.received.iter().map(Vec::len).sum::<usize>() >= 4 << 20)
            .await;
        client.for_a_while(settings.stream_idle + SLACK).await;
        client.hq_unread = false;
        let answer = client.answer(id).await;
        assert_eq!(answer.reset, Some(code::REQUEST_CANCELLED));
        assert!(answer.body.len() < SIZE);
    });
}

/// An HTTP/0.9 request the client stops is given up at once, as an HTTP/3 one is: its
/// exchange goes rather than running on until the answer finds no one to take it. The stop
/// may come once the line has been seen, or before: with the line in one flush, or after
/// the line's packet was lost, which then goes again.
#[test]
fn a_stopped_hq_request_lets_its_exchange_go() {
    for (line_seen, line_lost) in [(true, false), (false, false), (false, true)] {
        locally(stopped_hq_request(line_seen, line_lost));
    }
}

/// The client stops an HTTP/0.9 request once the core has it if `line_seen`, else in the
/// same flush as its line, or after the line's packet was lost if `line_lost`.
async fn stopped_hq_request(line_seen: bool, line_lost: bool) {
    // The server's own deadlines, not the tests' short ones: a lost line goes again at the
    // client's loss timer, which a loaded machine stretches past a short first-request one.
    let exchanges = Exchanges::default();
    let server = serving(Settings::default(), exchanges.core()).await;
    let mut client = Client::connect_hq(server.address, "a.test").await;
    let lost = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let id = client.hq_request(b"GET /\r\n", true);
    if line_seen {
        let started = exchanges.started.clone();
        client.until(move |_| started.get() == 1).await;
    } else if line_lost {
        client.send_to = Some(lost.local_addr().unwrap());
        client.flush().await;
        client.send_to = None;
    }
    client
        .quic
        .stream_shutdown(id, quiche::Shutdown::Read, code::REQUEST_CANCELLED)
        .unwrap();
    let (started, alive) = (exchanges.started.clone(), exchanges.alive.clone());
    client
        .until(move |_| started.get() == 1 && alive.get() == 0)
        .await;
}

/// An HTTP/0.9 connection with no request open is closed at its keep-alive deadline, with
/// no GOAWAY before it: HTTP/0.9 has none.
#[test]
fn an_idle_hq_connection_is_closed_at_its_keep_alive_deadline() {
    locally(async {
        let settings = short();
        let server = serving(settings, echo).await;
        let mut client = Client::connect_hq(server.address, "a.test").await;
        let id = client.hq_request(b"GET /\r\n", true);
        client.answer(id).await;
        let started = tokio::time::Instant::now();
        client
            .until(|client| client.quic.is_closed() || client.quic.is_draining())
            .await;
        let took = started.elapsed();
        assert!(
            took >= settings.keep_alive - Duration::from_millis(50),
            "{took:?}"
        );
        assert!(took <= settings.keep_alive + SLACK, "{took:?}");
        assert_eq!(client.closed_by_server(), Some((true, code::NO_ERROR)));
    });
}
