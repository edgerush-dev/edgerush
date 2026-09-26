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
use crate::downstream::h3::listener::{self, Forwarding, Secrets, Shared};
use crate::downstream::h3::testing::{Client, get};
use crate::drain::Drain;
use crate::interim::Interim;
use crate::request_body::{RequestBody, RequestBodyError};
use crate::timers::Timers;
use crate::tls::Tls;
use crate::tls::testing::certificate;
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

/// How late a deadline may be seen to fire on a loaded machine.
const SLACK: Duration = Duration::from_millis(600);

type Answering = Pin<Box<dyn Future<Output = Answered<Full<Bytes>>>>>;

/// A listener served on the loopback, and what its tests reach into.
struct Server {
    address: SocketAddr,
    drain: Rc<Drain>,
    shared: Rc<Shared>,
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
async fn serving(
    settings: Settings,
    respond: impl Fn(Request<RequestBody>, Interim) -> Answering + 'static,
) -> Server {
    serving_as(
        settings,
        respond,
        &Secrets::new().unwrap(),
        Forwarding::group(1).remove(0),
    )
    .await
}

/// The same as the worker `forwarding` is the share of, among those `secrets` are shared by.
async fn serving_as(
    settings: Settings,
    respond: impl Fn(Request<RequestBody>, Interim) -> Answering + 'static,
    secrets: &Secrets,
    forwarding: Forwarding,
) -> Server {
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
        move || Some(Arc::clone(&tls)),
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

/// A body is held to the length its head declared, both ways (RFC 9114 §4.1.2).
#[test]
fn a_body_that_is_not_the_length_declared_fails() {
    locally(async {
        let server = serving(short(), echo).await;
        let mut client = Client::connect(server.address, "a.test").await;
        for (declared, sent) in [("10", 11), ("10", 9)] {
            let mut head = get("a.test", "/up");
            head[0].1 = "POST";
            head.push(("content-length", declared));
            let id = client.request(&head, false);
            client.body(id, &vec![1; sent], true).await;
            let answer = client.answer(id).await;
            assert!(
                body_of(&answer).starts_with("invalid:"),
                "{declared} declared, {sent} sent: {}",
                body_of(&answer)
            );
        }
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
        let server = serving(short(), move |_request, _interim| {
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
