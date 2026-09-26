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
use crate::downstream::h3::testing::{Client, get};
use crate::drain::Drain;
use crate::interim::Interim;
use crate::request_body::{RequestBody, RequestBodyError};
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
        Rc::new(respond),
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
/// owner from the ID and hands each datagram over, once (16 §3).
#[test]
fn a_client_that_lands_on_another_worker_is_served_by_its_own() {
    locally(async {
        let secrets = Secrets::new().unwrap();
        let mut group = Forwarding::group(2);
        let other = group.remove(1);
        let owner = group.remove(0);
        let owner = serving_as(short(), echo, &secrets, owner).await;
        let other = serving_as(short(), echo, &secrets, other).await;
        let mut client = Client::connect(owner.address, "a.test").await;
        assert_eq!(
            body_of(&client.get("a.test", "/here").await),
            "GET /here 0 None"
        );

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
/// the path it follows.
#[test]
fn a_client_rebound_again_and_again_is_followed() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        for rebinding in 0..5 {
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
        assert!(answer.finished);
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
