//! Mirrors.

use super::*;

/// A mirror is sent the request as it stands at its place in the rule's filters: one
/// before the rewrite, the path and host routed on; one after it, the rewritten ones,
/// as the upstream is (18 §5).
#[tokio::test]
async fn a_mirror_is_sent_the_request_as_it_stands_at_its_place() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, sent) = recording_upstream("200 OK");
            let (before, copied_before) = recording_upstream("200 OK");
            let (after, copied_after) = recording_upstream("200 OK");
            let mut config = everything_config(primary);
            let mut copy = config.upstreams["up"].clone();
            copy.endpoints = vec![before];
            config.upstreams.insert("before".to_owned(), copy.clone());
            copy.endpoints = vec![after];
            config.upstreams.insert("after".to_owned(), copy);
            let filters = [
                "{ type: request_mirror, upstream: before, fraction: { numerator: 1, denominator: 1 } }",
                "{ type: url_rewrite, host: one.example.org, path: { replace_prefix: /api } }",
                "{ type: request_mirror, upstream: after, fraction: { numerator: 1, denominator: 1 } }",
            ];
            for filter in filters {
                config.routes[0].rules[0]
                    .filters
                    .push(serde_saphyr::from_str(filter).unwrap());
            }
            let (front, _worker) = serving_config(&config).await;

            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            until(|| copied_before.borrow().len() == 1 && copied_after.borrow().len() == 1)
                .await;
            let rewritten = |head: &str| {
                head.starts_with("get /api/a/b?c=d http/1.1\r\n")
                    && head.contains("\r\nhost: one.example.org\r\n")
            };
            assert!(rewritten(&sent.borrow()[0]), "{:?}", sent.borrow());
            assert!(rewritten(&copied_after.borrow()[0]), "{:?}", copied_after.borrow());
            let routed_on = &copied_before.borrow()[0];
            assert!(
                routed_on.starts_with("get /a/b?c=d http/1.1\r\n"),
                "{routed_on}"
            );
            assert!(
                routed_on.contains("\r\nhost: shop.example.com\r\n"),
                "{routed_on}"
            );
            // The rest of the request is the request's.
            assert!(routed_on.contains("\r\naccept: */*\r\n"), "{routed_on}");
        })
        .await;
}

/// A worker whose one rule goes to `up` at `primary`, speaking what it says, and mirrors
/// every request to each of `mirrors`: a name, where it is (nowhere, if not given) and
/// what it speaks.
async fn serving_mirroring_worker(
    (primary, speaking): (SocketAddr, UpstreamProtocol),
    mirrors: &[(&str, Option<SocketAddr>, UpstreamProtocol)],
) -> (SocketAddr, Rc<Worker>) {
    let mut config = everything_config(primary);
    config.upstreams.get_mut("up").unwrap().protocol = speaking;
    for &(name, at, protocol) in mirrors {
        let mut upstream = config.upstreams["up"].clone();
        upstream.endpoints = at.into_iter().collect();
        upstream.protocol = protocol;
        config.upstreams.insert(name.to_owned(), upstream);
        config.routes[0].rules[0]
            .filters
            .push(edgerush_config::Filter::RequestMirror(
                edgerush_config::Mirror {
                    upstream: name.to_owned(),
                    fraction: edgerush_config::Fraction {
                        numerator: 1,
                        denominator: 1,
                    },
                },
            ));
    }
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// An HTTP/2 upstream that takes every request and reads none of its body: a mirror
/// that stops keeping up once its stream window is spent. What it was asked for, by
/// path.
async fn unread_h2_upstream() -> (SocketAddr, Rc<RefCell<Vec<String>>>) {
    let asked = Rc::new(RefCell::new(Vec::new()));
    let asking = Rc::clone(&asked);
    let script: Script = Rc::new(move |request, _respond| {
        asking.borrow_mut().push(request.uri().path().to_owned());
        Box::pin(async move {
            // Held, body unread, until the connection goes.
            let _held = request;
            std::future::pending::<()>().await;
        })
    });
    (scripted_h2_upstream(script).await, asked)
}

/// Every mirror gets a copy of the request, body and all, in the protocol it speaks,
/// and its answer goes nowhere: the client gets the request's own.
#[tokio::test]
async fn a_mirror_gets_a_copy_and_the_client_the_answer_of_the_request() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, sent) = statuses_upstream(vec![200]).await;
            let (shadow, copied) = statuses_upstream(vec![500]).await;
            let seen = Rc::new(RefCell::new(Vec::new()));
            let seeing = Rc::clone(&seen);
            let script: Script = Rc::new(move |request, mut respond| {
                let seeing = Rc::clone(&seeing);
                Box::pin(async move {
                    let path = request.uri().path().to_owned();
                    let mut body = request.into_body();
                    let mut all = Vec::new();
                    while let Some(Ok(chunk)) = body.data().await {
                        let _ = body.flow_control().release_capacity(chunk.len());
                        all.extend_from_slice(&chunk);
                    }
                    seeing.borrow_mut().push((path, all));
                    let answer = Response::builder().status(503).body(()).unwrap();
                    let _ = respond.send_response(answer, true);
                })
            });
            let shadow_h2 = scripted_h2_upstream(script).await;
            let (front, worker) = serving_mirroring_worker(
                (primary, UpstreamProtocol::Http1),
                &[
                    ("shadow", Some(shadow), UpstreamProtocol::Http1),
                    ("shadow-h2", Some(shadow_h2), UpstreamProtocol::Http2),
                ],
            )
            .await;
            let request =
                b"POST /copied?q=1 HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi";
            let answer = h1_answer(front, request).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            assert_eq!(*sent.borrow(), vec![b"hi".to_vec()]);
            until(|| copied.borrow().len() == 1 && seen.borrow().len() == 1).await;
            assert_eq!(*copied.borrow(), vec![b"hi".to_vec()]);
            assert_eq!(
                *seen.borrow(),
                vec![("/copied".to_owned(), b"hi".to_vec())]
            );
            // Each mirror is an upstream like any other, counted as one.
            let scrape = worker.proxy().metrics();
            for name in ["shadow", "shadow-h2"] {
                let line = format!("edgerush_upstream_requests_total{{upstream=\"{name}\"}} 1\n");
                assert!(scrape.contains(&line), "{scrape}");
            }
        })
        .await;
}

/// A body that says it has ended with its last frame, as an HTTP/2 client's does, is
/// sent to an HTTP/2 upstream without its end ever being asked for: its copy goes whole
/// all the same, ended as the request was, not cut off as if the request went.
#[tokio::test]
async fn a_mirror_gets_the_whole_of_a_body_that_ends_with_its_last_frame() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // What each upstream was sent, and how it ended.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let reading = |name: &'static str, status: u16| -> Script {
                let seeing = Rc::clone(&seen);
                Rc::new(move |request, mut respond| {
                    let seeing = Rc::clone(&seeing);
                    Box::pin(async move {
                        let mut body = request.into_body();
                        let mut all = Vec::new();
                        let ended = loop {
                            match body.data().await {
                                Some(Ok(chunk)) => {
                                    let _ = body.flow_control().release_capacity(chunk.len());
                                    all.extend_from_slice(&chunk);
                                }
                                Some(Err(error)) => break Err(error.to_string()),
                                None => break Ok(()),
                            }
                        };
                        seeing.borrow_mut().push((name, all, ended));
                        let answer = Response::builder().status(status).body(()).unwrap();
                        let _ = respond.send_response(answer, true);
                    })
                })
            };
            let primary = scripted_h2_upstream(reading("primary", 200)).await;
            let shadow = scripted_h2_upstream(reading("shadow", 503)).await;
            let (front, _worker) = serving_mirroring_worker(
                (primary, UpstreamProtocol::Http2),
                &[("shadow", Some(shadow), UpstreamProtocol::Http2)],
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::builder()
                .method(Method::POST)
                .uri("http://a.test/")
                .body(())
                .unwrap();
            let (answer, mut upload) = send.send_request(request, false).unwrap();
            upload.send_data(Bytes::from_static(b"hi"), true).unwrap();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            until(|| seen.borrow().len() == 2).await;
            let mut seen = seen.borrow().clone();
            seen.sort();
            assert_eq!(
                seen,
                vec![
                    ("primary", b"hi".to_vec(), Ok(())),
                    ("shadow", b"hi".to_vec(), Ok(())),
                ]
            );
        })
        .await;
}

/// A mirror that stops reading is given up on once it is too far behind; the request
/// goes on at its own upstream's pace, whole. The rest of the body waits for the copy's
/// head to reach the mirror: sent whole, it can be past the bound before the copy has
/// begun, and a copy given up on then is never sent at all.
#[tokio::test]
async fn a_mirror_that_stops_reading_never_holds_the_request_up() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, sent) = statuses_upstream(vec![200]).await;
            let (shadow, asked) = unread_h2_upstream().await;
            let (front, worker) = serving_mirroring_worker(
                (primary, UpstreamProtocol::Http1),
                &[("shadow", Some(shadow), UpstreamProtocol::Http2)],
            )
            .await;
            let size = 1 << 20;
            let mut client = TcpStream::connect(front).await.unwrap();
            let head = format!(
                "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\nx"
            );
            client.write_all(head.as_bytes()).await.unwrap();
            until(|| asked.borrow().len() == 1).await;
            client.write_all(&vec![b'x'; size - 1]).await.unwrap();
            let mut answer = Vec::new();
            let _ended = within(client.read_to_end(&mut answer)).await;
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            assert_eq!(sent.borrow()[0].len(), size);
            let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"shadow\",reason=\"behind\"} 1\n";
            until(|| worker.proxy().metrics().contains(line)).await;
        })
        .await;
}

/// A mirror given up on while it waits for room its upstream will never give — its
/// window spent, the request's body going on without it — lets go of its exchange at
/// once: its stream is reset and it is counted then, not held with its place until its
/// idle bound, 30 s, runs out. Whether a copy is waiting there when it is given up on is
/// a race in a request sent whole; this one's body is sent in two, the mirror's window
/// spent in between.
#[tokio::test]
async fn a_mirror_given_up_on_while_it_waits_for_room_lets_go_at_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, sent) = statuses_upstream(vec![200]).await;
            // Takes what comes and never gives the room back: h2's first window's
            // worth, and no more.
            let window = 65_535;
            let taken = Rc::new(std::cell::Cell::new(0));
            let let_go = Rc::new(std::cell::Cell::new(false));
            let (taking, letting_go) = (Rc::clone(&taken), Rc::clone(&let_go));
            let script: Script = Rc::new(move |request, respond| {
                let (taking, letting_go) = (Rc::clone(&taking), Rc::clone(&letting_go));
                Box::pin(async move {
                    let _unanswered = respond;
                    let mut body = request.into_body();
                    while let Some(Ok(chunk)) = body.data().await {
                        taking.set(taking.get() + chunk.len());
                    }
                    letting_go.set(true);
                })
            });
            let shadow = scripted_h2_upstream(script).await;
            let (front, worker) = serving_mirroring_worker(
                (primary, UpstreamProtocol::Http1),
                &[("shadow", Some(shadow), UpstreamProtocol::Http2)],
            )
            .await;
            let size = 1 << 20;
            let mut client = TcpStream::connect(front).await.unwrap();
            let head = format!(
                "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
            );
            client.write_all(head.as_bytes()).await.unwrap();
            // A piece at a time, each well within its bound and taken before the next,
            // until its window is spent with a byte of the last still in hand: it waits
            // for room. Sent whole, the body is past its bound before its copy begins.
            let piece = 16 * 1024;
            let mut given = 0;
            while given <= window {
                client.write_all(&vec![b'x'; piece]).await.unwrap();
                given += piece;
                until(|| taken.get() == given.min(window)).await;
            }
            // The rest takes it past its bound while it waits.
            client.write_all(&vec![b'x'; size - given]).await.unwrap();
            let mut answer = Vec::new();
            let _ended = within(client.read_to_end(&mut answer)).await;
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            assert_eq!(sent.borrow()[0].len(), size);
            let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"shadow\",reason=\"behind\"} 1\n";
            until(|| let_go.get() && worker.proxy().metrics().contains(line)).await;
        })
        .await;
}

/// A copy that cannot go is counted by why, and the request goes as it would have:
/// a mirror with no endpoint, and a request with credentials bound to its client's
/// connection.
#[tokio::test]
async fn a_copy_that_cannot_go_is_counted_and_the_request_goes_anyway() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, sent) = statuses_upstream(vec![200]).await;
            let (shadow, copied) = statuses_upstream(vec![200]).await;
            let (front, worker) = serving_mirroring_worker(
                (primary, UpstreamProtocol::Http1),
                &[
                    ("nowhere", None, UpstreamProtocol::Http1),
                    ("shadow", Some(shadow), UpstreamProtocol::Http1),
                ],
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            until(|| copied.borrow().len() == 1).await;
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"nowhere\",reason=\"nowhere\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");

            let bound = b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nauthorization: Negotiate abc\r\n\r\n";
            let answer = h1_answer(front, bound).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            assert_eq!(sent.borrow().len(), 2);
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"shadow\",reason=\"credentials\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            assert_eq!(copied.borrow().len(), 1, "credentials went to a mirror");
        })
        .await;
}
