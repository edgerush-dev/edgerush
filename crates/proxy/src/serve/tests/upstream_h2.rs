//! Upstreams spoken to in HTTP/2.

use super::*;

/// An upstream configured for HTTP/2 is spoken to in HTTP/2, whatever the client
/// spoke: the host in `:authority`, nothing about a connection, and the answer back.
#[tokio::test]
async fn an_http2_upstream_is_spoken_to_in_http2() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert!(answer.contains("\r\n\r\n2\r\nok\r\n0\r\n\r\n"), "{answer}");
            {
                let requests = seen.requests.borrow();
                let (request, _) = &requests[0];
                assert_eq!(request.version(), Version::HTTP_2);
                assert_eq!(request.method(), Method::GET);
                // Reached without TLS: `http`, whatever the client used.
                assert_eq!(request.uri().scheme_str(), Some("http"));
                assert_eq!(request.uri().authority().unwrap(), "shop.example.com");
                assert_eq!(request.uri().path_and_query().unwrap(), "/a/b?c=d");
                assert!(request.headers().get("connection").is_none());
                assert!(request.headers().get("host").is_none());
                assert_eq!(request.headers()["accept"], "*/*");
            }

            // And from a client that spoke HTTP/2 itself.
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://shop.example.com/x").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert_eq!(seen.requests.borrow().len(), 2);
            assert_eq!(seen.connections.get(), 1, "the connection was not shared");
        })
        .await;
}

/// Requests share a connection up to its stream cap; past it another is opened, and no
/// more than needed.
#[tokio::test]
async fn requests_share_http2_connections_up_to_their_stream_cap() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let how = UpstreamH2 {
                gated: true,
                ..UpstreamH2::default()
            };
            let (upstream, seen, gate) = h2_upstream(how).await;
            let limits = H1Limits {
                h2_streams: 2,
                ..H1Limits::default()
            };
            let (front, worker) = serving_worker_to_h2(upstream, limits).await;
            // A burst on a cold destination: on each new connection only the first
            // stream goes before the upstream's SETTINGS are heard, and what the
            // connection will take once they are is counted on meanwhile.
            let held: Vec<_> = (0..5)
                .map(|_| tokio::task::spawn_local(h1_answer(front, CLOSING_GET)))
                .collect();
            until(|| seen.open.get() == 5).await;
            assert_eq!(seen.connections.get(), 3);
            assert_eq!(worker.h2_connections(), 3);
            // One more for the request after them.
            gate.add_permits(6);
            for answer in held {
                let answer = answer.await.unwrap();
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            }
            // All three are kept for what comes next.
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert_eq!(seen.connections.get(), 3);
        })
        .await;
}

/// An upstream's own limit on streams, below ours, is what fills a connection: past it
/// the next request goes on another, never into h2's queue behind the limit.
#[tokio::test]
async fn an_upstreams_stream_limit_below_ours_is_honoured() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let how = UpstreamH2 {
                streams: 1,
                gated: true,
                ..UpstreamH2::default()
            };
            let (upstream, seen, gate) = h2_upstream(how).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            // The first opens a connection and learns the limit from its SETTINGS.
            let first = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
            until(|| seen.open.get() == 1).await;
            let rest: Vec<_> = (0..2)
                .map(|_| tokio::task::spawn_local(h1_answer(front, CLOSING_GET)))
                .collect();
            until(|| seen.open.get() == 3).await;
            assert_eq!(seen.connections.get(), 3);
            gate.add_permits(3);
            for answer in std::iter::once(first).chain(rest) {
                assert!(answer.await.unwrap().starts_with("HTTP/1.1 200 OK\r\n"));
            }
        })
        .await;
}

/// A request's body goes up while its answer is waited for, and on after an answer that
/// came before the upstream had read it.
#[tokio::test]
async fn an_upload_goes_up_to_an_http2_upstream_before_and_after_its_answer() {
    let body = vec![b'x'; 200 * 1024];
    let mut request =
        b"POST /upload HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 204800\r\n\r\n"
            .to_vec();
    request.extend_from_slice(&body);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for early in [false, true] {
                let how = UpstreamH2 {
                    early,
                    ..UpstreamH2::default()
                };
                let (upstream, seen, _gate) = h2_upstream(how).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let answer = h1_answer(front, &request).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                if !early {
                    assert!(answer.contains("x-read: 204800\r\n"), "{answer}");
                }
                until(|| {
                    seen.requests
                        .borrow()
                        .first()
                        .is_some_and(|(_, read)| *read == 204_800)
                })
                .await;
                let requests = seen.requests.borrow();
                assert_eq!(requests[0].0.headers()["content-length"], "204800");
            }
        })
        .await;
}

/// An HTTP/2 upstream nobody listens at is answered 502, promptly.
#[tokio::test]
async fn an_unreachable_http2_upstream_is_answered_502() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_held, upstream) = refusing();
            let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            until(|| worker.h2_connections() == 0).await;
        })
        .await;
}

/// Connection-bound credentials are not sent to an HTTP/2 upstream, where they would
/// authenticate every client's streams (15 §5).
#[tokio::test]
async fn connection_bound_credentials_are_not_sent_to_an_http2_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let asked = b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nauthorization: NTLM TlRMTVNTUAABAAAA\r\n\r\n";
            let answer = h1_answer(front, asked).await;
            assert!(answer.starts_with("HTTP/1.1 501 "), "{answer}");
            assert!(seen.requests.borrow().is_empty());
            assert_eq!(seen.connections.get(), 0);
            let scrape = worker.proxy().metrics();
            assert!(scrape.contains("reason=\"connection_auth\"} 1"), "{scrape}");
        })
        .await;
}

/// Requests past the queue's bound are refused at once; one that waits too long for a
/// place is refused when its time is up; the two are told apart.
#[tokio::test]
async fn waiting_for_a_place_is_bounded_in_number_and_in_time() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let how = UpstreamH2 {
                gated: true,
                ..UpstreamH2::default()
            };
            let (upstream, seen, gate) = h2_upstream(how).await;
            let limits = H1Limits {
                h2_streams: 1,
                h2_connections: 1,
                h2_waiting: 1,
                connect: Duration::from_millis(500),
                ..H1Limits::default()
            };
            let (front, worker) = serving_worker_to_h2(upstream, limits).await;
            let first = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
            until(|| seen.open.get() == 1).await;
            let waiting = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
            // Let the second join the queue before the third arrives.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let refused = h1_answer(front, CLOSING_GET).await;
            assert!(refused.starts_with("HTTP/1.1 503 "), "{refused}");
            let timed_out = waiting.await.unwrap();
            assert!(timed_out.starts_with("HTTP/1.1 503 "), "{timed_out}");
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("reason=\"upstream_queue_full\"} 1"),
                "{scrape}"
            );
            assert!(
                scrape.contains("reason=\"upstream_queue_timeout\"} 1"),
                "{scrape}"
            );
            // The one that gave up gave its place in the queue up with it: another may
            // wait there, and is served once the first is answered.
            let next = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
            tokio::time::sleep(Duration::from_millis(100)).await;
            gate.add_permits(2);
            assert!(first.await.unwrap().starts_with("HTTP/1.1 200 OK\r\n"));
            let next = next.await.unwrap();
            assert!(next.starts_with("HTTP/1.1 200 OK\r\n"), "{next}");
        })
        .await;
}

/// An upstream that says GOAWAY finishes what it has, and what comes next goes on a new
/// connection.
#[tokio::test]
async fn after_goaway_requests_go_on_a_new_http2_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let how = UpstreamH2 {
                away_after: Some(2),
                ..UpstreamH2::default()
            };
            let (upstream, seen, _gate) = h2_upstream(how).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            for _ in 0..5 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            }
            assert_eq!(seen.connections.get(), 3);

            // A request that comes while a connection told to go still has a stream
            // on it goes on a new one, rather than on the one going.
            let how = UpstreamH2 {
                away_after: Some(1),
                gated: true,
                ..UpstreamH2::default()
            };
            let (upstream, seen, gate) = h2_upstream(how).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let first = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
            until(|| seen.open.get() == 1).await;
            // Long enough for the GOAWAY to be heard.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let second = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
            until(|| seen.open.get() == 2).await;
            assert_eq!(seen.connections.get(), 2);
            gate.add_permits(2);
            for answer in [first, second] {
                let answer = answer.await.unwrap();
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            }
        })
        .await;
}

/// A request whose body stalls on its way to an HTTP/2 upstream, before any answer, is
/// answered 408 as the client's doing, not 502 as the upstream's.
#[tokio::test]
async fn a_stalled_upload_to_an_http2_upstream_is_answered_408() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://example.test/")
                .version(Version::HTTP_2)
                .body(())
                .unwrap();
            let (response, mut upload) = send.send_request(request, false).unwrap();
            upload.send_data(Bytes::from_static(b"abc"), false).unwrap();
            let response = within(response).await.unwrap();
            assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        })
        .await;
}

/// Interim answers from an HTTP/2 upstream reach the client in order, before the final
/// one; past the exchange's bound on them the upstream is given up on.
#[tokio::test]
async fn interim_answers_from_an_http2_upstream_reach_the_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let script: Script = Rc::new(|request, mut respond| {
                Box::pin(async move {
                    let many: usize = request
                        .uri()
                        .path()
                        .trim_start_matches('/')
                        .parse()
                        .unwrap_or(1);
                    for _ in 0..many {
                        let hint = Response::builder()
                            .status(103)
                            .header("link", "</style.css>; rel=preload")
                            .body(())
                            .unwrap();
                        if respond.send_informational(hint).is_err() {
                            return;
                        }
                    }
                    let _ = respond.send_response(ok_head(), true);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/2").body(()).unwrap();
            let (mut answer, _) = send.send_request(request, true).unwrap();
            let mut interim = Vec::new();
            while let Some(head) =
                within(std::future::poll_fn(|cx| answer.poll_informational(cx))).await
            {
                let head = head.unwrap();
                interim.push((head.status().as_u16(), head.headers()["link"].clone()));
            }
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert_eq!(interim.len(), 2);
            assert!(interim.iter().all(|(status, _)| *status == 103));

            let answer = h1_answer(
                front,
                b"GET /1 HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 103 "), "{answer}");
            assert!(answer.contains("\r\n\r\nHTTP/1.1 200 OK\r\n"), "{answer}");

            // Seventeen are one past the sixteen an exchange takes.
            let answer = h1_answer(
                front,
                b"GET /17 HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
            )
            .await;
            assert!(answer.contains("HTTP/1.1 502 "), "{answer}");
        })
        .await;
}

/// A request that said `Expect: 100-continue` has its body held back until the upstream
/// says `100`; one whose upstream answers first never has it sent.
#[tokio::test]
async fn a_body_waits_for_an_http2_upstreams_100_continue() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let read = Rc::new(Cell::new(None::<usize>));
            let reading = Rc::clone(&read);
            let script: Script = Rc::new(move |request, mut respond| {
                let reading = Rc::clone(&reading);
                Box::pin(async move {
                    let refuse = request.uri().path() == "/refuse";
                    let mut body = request.into_body();
                    if refuse {
                        let no = Response::builder().status(417).body(()).unwrap();
                        let _ = respond.send_response(no, true);
                        // Whatever arrives after the answer.
                        reading.set(Some(read_all(&mut body).await));
                        return;
                    }
                    let go = Response::builder().status(100).body(()).unwrap();
                    let _ = respond.send_informational(go);
                    reading.set(Some(read_all(&mut body).await));
                    let _ = respond.send_response(ok_head(), true);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let limits = H1Limits {
                continue_wait: Duration::from_secs(60),
                ..H1Limits::default()
            };
            let (front, _worker) = serving_worker_to_h2(upstream, limits).await;

            let mut stream = TcpStream::connect(front).await.unwrap();
            stream
                .write_all(b"POST /go HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nexpect: 100-continue\r\ncontent-length: 5\r\n\r\n")
                .await
                .unwrap();
            let mut first = vec![0; 64];
            let got = within(stream.read(&mut first)).await.unwrap();
            let first = String::from_utf8_lossy(&first[..got]).into_owned();
            assert!(first.starts_with("HTTP/1.1 100 "), "{first}");
            stream.write_all(b"hello").await.unwrap();
            let mut rest = Vec::new();
            let _ = within(stream.read_to_end(&mut rest)).await;
            let rest = String::from_utf8_lossy(&rest);
            assert!(rest.contains("HTTP/1.1 200 OK\r\n"), "{rest}");
            until(|| read.get() == Some(5)).await;

            // A client that sends its body without waiting to be told: the gateway still
            // holds it for the upstream, which answers without asking for it.
            read.set(None);
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream
                .write_all(b"POST /refuse HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nexpect: 100-continue\r\ncontent-length: 5\r\n\r\nhello")
                .await
                .unwrap();
            let mut answer = vec![0; 64];
            let got = within(stream.read(&mut answer)).await.unwrap();
            let answer = String::from_utf8_lossy(&answer[..got]).into_owned();
            assert!(answer.starts_with("HTTP/1.1 417 "), "{answer}");
            drop(stream);
            until(|| read.get().is_some()).await;
            assert_eq!(read.get(), Some(0), "the body went up after the answer");
        })
        .await;
}

/// An HTTP/2 upstream's trailers reach an HTTP/2 client, as gRPC's status does; the
/// client's own go no further than the gateway (03 §11), and its body ends where they
/// would have been.
#[tokio::test]
async fn an_answers_trailers_travel_and_a_requests_do_not_through_an_http2_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // What the upstream heard after the body: `Some(None)` for no trailers.
            let heard = Rc::new(RefCell::new(None::<Option<http::HeaderMap>>));
            let hearing = Rc::clone(&heard);
            let script: Script = Rc::new(move |request, mut respond| {
                let hearing = Rc::clone(&hearing);
                Box::pin(async move {
                    let mut body = request.into_body();
                    let _ = read_all(&mut body).await;
                    let trailers = std::future::poll_fn(|cx| body.poll_trailers(cx)).await;
                    *hearing.borrow_mut() = Some(trailers.ok().flatten());
                    let Ok(mut sending) = respond.send_response(ok_head(), false) else {
                        return;
                    };
                    let _ = sending.send_data(Bytes::from_static(b"reply"), false);
                    let mut status = http::HeaderMap::new();
                    status.insert("grpc-status", "0".parse().unwrap());
                    let _ = sending.send_trailers(status);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://a.test/rpc")
                .header("te", "trailers")
                .body(())
                .unwrap();
            let (answer, mut upload) = send.send_request(request, false).unwrap();
            upload
                .send_data(Bytes::from_static(b"call"), false)
                .unwrap();
            let mut sent = http::HeaderMap::new();
            sent.insert("x-checksum", "abc".parse().unwrap());
            upload.send_trailers(sent).unwrap();

            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            let mut body = answer.into_body();
            let mut data = Vec::new();
            while let Some(chunk) = within(body.data()).await {
                let chunk = chunk.unwrap();
                let _ = body.flow_control().release_capacity(chunk.len());
                data.extend_from_slice(&chunk);
            }
            assert_eq!(data, b"reply");
            let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx)))
                .await
                .unwrap()
                .expect("no trailers");
            assert_eq!(trailers["grpc-status"], "0");
            until(|| heard.borrow().is_some()).await;
            assert_eq!(
                heard.borrow().as_ref(),
                Some(&None),
                "request trailers went up"
            );
        })
        .await;
}

/// An HTTP/2 upstream's trailers reach an HTTP/2 client less the names that may not
/// travel as trailers, as an HTTP/1 upstream's do (13 §4): `grpc-status` goes on,
/// `set-cookie` does not, and a section with nothing else in it is no section at all.
#[tokio::test]
async fn an_http2_upstreams_denied_trailers_do_not_reach_an_http2_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let script: Script = Rc::new(move |request, mut respond| {
                Box::pin(async move {
                    let only_denied = request.uri().path() == "/only-denied";
                    let mut body = request.into_body();
                    let _ = read_all(&mut body).await;
                    let Ok(mut sending) = respond.send_response(ok_head(), false) else {
                        return;
                    };
                    let _ = sending.send_data(Bytes::from_static(b"reply"), false);
                    let mut trailers = http::HeaderMap::new();
                    if !only_denied {
                        trailers.insert("grpc-status", "0".parse().unwrap());
                    }
                    trailers.insert("set-cookie", "session=late".parse().unwrap());
                    let _ = sending.send_trailers(trailers);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            for path in ["/rpc", "/only-denied"] {
                let request = Request::get(format!("http://a.test{path}"))
                    .header("te", "trailers")
                    .body(())
                    .unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                let mut body = answer.into_body();
                let mut data = Vec::new();
                while let Some(chunk) = within(body.data()).await {
                    let chunk = chunk.unwrap();
                    let _ = body.flow_control().release_capacity(chunk.len());
                    data.extend_from_slice(&chunk);
                }
                assert_eq!(data, b"reply", "{path}");
                let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx)))
                    .await
                    .unwrap();
                match path {
                    "/rpc" => {
                        let trailers = trailers.expect("no trailers");
                        assert_eq!(trailers["grpc-status"], "0");
                        assert!(
                            !trailers.contains_key("set-cookie"),
                            "a denied trailer was forwarded: {trailers:?}"
                        );
                    }
                    _ => assert_eq!(trailers, None, "a section of denied names was sent"),
                }
            }
        })
        .await;
}

/// An HTTP/2 upstream that resets a stream part way through its answer has the client's
/// stream reset: with the same reason where it means the same thing on this hop, with
/// INTERNAL_ERROR where it was about the upstream's hop.
#[tokio::test]
async fn an_http2_upstreams_reset_is_passed_on_where_it_means_the_same() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let script: Script = Rc::new(|request, mut respond| {
                Box::pin(async move {
                    let reason = match request.uri().path() {
                        "/cancel" => ::h2::Reason::CANCEL,
                        _ => ::h2::Reason::PROTOCOL_ERROR,
                    };
                    let Ok(mut sending) = respond.send_response(ok_head(), false) else {
                        return;
                    };
                    let _ = sending.send_data(Bytes::from_static(b"part"), false);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    sending.send_reset(reason);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            for (path, expected) in [
                ("/cancel", ::h2::Reason::CANCEL),
                ("/protocol", ::h2::Reason::INTERNAL_ERROR),
            ] {
                let request = Request::get(format!("http://a.test{path}"))
                    .body(())
                    .unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                let mut body = answer.into_body();
                let failed = loop {
                    match within(body.data()).await {
                        Some(Ok(chunk)) => {
                            let _ = body.flow_control().release_capacity(chunk.len());
                        }
                        Some(Err(error)) => break error,
                        None => panic!("{path}: the answer ended cleanly"),
                    }
                };
                assert_eq!(failed.reason(), Some(expected), "{path}");
            }
        })
        .await;
}

/// One client that stops reading holds only its own stream's window: another stream on
/// the same upstream connection is answered in full meanwhile.
#[tokio::test]
async fn a_slow_reader_does_not_hold_up_another_on_the_same_http2_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const BIG: usize = 4 << 20;
            let upstream = scripted_h2_upstream(answering_with(BIG)).await;
            let limits = H1Limits {
                h2_connections: 1,
                ..H1Limits::default()
            };
            let (front, _worker) = serving_worker_to_h2(upstream, limits).await;

            let mut slow = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/slow").body(()).unwrap();
            let (stalled, _) = slow.send_request(request, true).unwrap();
            // Its head arrives; its body is never read.
            let _stalled = within(stalled).await.unwrap();

            let mut fast = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/fast").body(()).unwrap();
            let (answer, _) = fast.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            let mut body = answer.into_body();
            let mut read = 0;
            while let Some(chunk) = within(body.data()).await {
                let chunk = chunk.unwrap();
                read += chunk.len();
                let _ = body.flow_control().release_capacity(chunk.len());
            }
            assert_eq!(read, BIG);
        })
        .await;
}

/// What h2 holds of an answer from an HTTP/2 upstream that its client is not reading —
/// up to the stream window it gave the upstream — is the worker's storage, as an upload
/// nobody reads is (15 §3).
#[tokio::test]
async fn an_answer_nobody_reads_from_an_http2_upstream_is_charged_to_the_worker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(answering_with(4 << 20)).await;
            // The usual deadlines: a stalled answer is not given up on while it is looked at.
            let mut config = everything_config(upstream);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::new(Arc::new(proxy));
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut slow = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/slow").body(()).unwrap();
            let (stalled, _) = slow.send_request(request, true).unwrap();
            // Its head arrives; its body is never read.
            let _stalled = within(stalled).await.unwrap();
            // All of the window but what the exchange took out of h2 before the client's
            // own window stopped it: well over three quarters.
            let window = h2_settings(&H1Limits::default()).stream_window as usize;
            let held = window * 3 / 4;
            let used = || worker.blocks.borrow().storage().used();
            for _ in 0..100 {
                if used() >= held {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                used() >= held,
                "{} charged while h2 holds most of the upstream's stream window",
                used()
            );
        })
        .await;
}

/// When what h2 holds of answers nobody reads can no longer be paid for, the upstream
/// connection holding them is closed, and the clients waiting on it are told so rather
/// than left waiting (15 §3).
#[tokio::test]
async fn an_upstream_connection_holding_more_than_the_worker_can_pay_for_is_closed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(answering_with(4 << 20)).await;
            let mut config = everything_config(upstream);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            // Less than two of the upstream's stream windows, on one connection.
            let limits = H1Limits {
                storage: 3 << 19,
                h2_connections: 1,
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(Arc::new(proxy), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            // Clients that grant no room read nothing at all, so no writer takes storage
            // of its own for what it would send them: what runs out is what h2 holds of
            // the answers. A writer that asked first would be refused and reset its own
            // stream, its answer's charge going with it, and nothing need be closed.
            let mut granting_nothing = ::h2::client::Builder::new();
            granting_nothing.initial_window_size(0);
            let mut stalled = Vec::new();
            for path in ["/one", "/two"] {
                let mut client = h2_library_client(front, &granting_nothing).await;
                let request = Request::get(format!("http://a.test{path}"))
                    .body(())
                    .unwrap();
                let (answer, _) = client.send_request(request, true).unwrap();
                // The second answer's data is what the worker cannot pay for, and the
                // connection may be closed in the very turn that answer's head was handed
                // to its client's stream: the stream is then reset before its head is
                // written, and h2 drops a head still queued, so that client hears only
                // the reset. Either way both were in flight.
                let body = match within(answer).await {
                    Ok(answer) => {
                        assert_eq!(answer.status(), StatusCode::OK, "{path}");
                        Some(answer.into_body())
                    }
                    Err(reset)
                        if path == "/two"
                            && reset.reason() == Some(::h2::Reason::INTERNAL_ERROR) =>
                    {
                        None
                    }
                    Err(error) => panic!("{path}: {error}"),
                };
                stalled.push((client, body));
            }
            // Neither is read, and nothing is released: the upstream connection under them
            // would hold more than the worker can pay for, and is closed, its charge with
            // it. (Each client is told when its stream's writer gives up on it, at the idle
            // bound: a writer waiting for the client's room is not reading the answer.)
            let used = || worker.blocks.borrow().storage().used();
            for _ in 0..200 {
                if worker.h2_connections() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                worker.h2_connections(),
                0,
                "the upstream connection was kept"
            );
            assert!(used() < 1 << 20, "{} still charged", used());
            drop(stalled);
        })
        .await;
}

/// A request the upstream refuses outright — RST_STREAM(REFUSED_STREAM), which says it
/// was never processed — is sent once more when all of it can be sent again: its body
/// too, if it had one no bigger than what is kept. One refused twice is not sent a
/// third time.
#[tokio::test]
async fn a_request_an_http2_upstream_refused_unprocessed_is_sent_once_more() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let asked = Rc::new(Cell::new(0_usize));
            let asking = Rc::clone(&asked);
            let bodies = Rc::new(RefCell::new(Vec::new()));
            let taking = Rc::clone(&bodies);
            let script: Script = Rc::new(move |request, mut respond| {
                let asking = Rc::clone(&asking);
                let taking = Rc::clone(&taking);
                Box::pin(async move {
                    asking.set(asking.get() + 1);
                    // `/twice` is refused every time; the rest only the first time.
                    let refuse =
                        request.uri().path() == "/twice" || asking.get() % 2 == 1;
                    if refuse {
                        respond.send_reset(::h2::Reason::REFUSED_STREAM);
                        return;
                    }
                    let mut body = request.into_body();
                    let mut all = Vec::new();
                    while let Some(Ok(chunk)) = body.data().await {
                        let _ = body.flow_control().release_capacity(chunk.len());
                        all.extend_from_slice(&chunk);
                    }
                    taking.borrow_mut().push(all);
                    let _ = respond.send_response(ok_head(), true);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert_eq!(asked.get(), 2);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                "{scrape}"
            );

            // A body, kept as it went the first time, goes again with it.
            asked.set(0);
            bodies.borrow_mut().clear();
            let with_body = b"POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi";
            let answer = h1_answer(front, with_body).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert_eq!(asked.get(), 2);
            assert_eq!(*bodies.borrow(), vec![b"hi".to_vec()]);

            // One bigger than what is kept is not.
            asked.set(0);
            let size = crate::retry::replay::MOST + 1;
            let mut big = format!(
                "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
            )
            .into_bytes();
            big.resize(big.len() + size, b'x');
            let answer = h1_answer(front, &big).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            assert_eq!(asked.get(), 1);

            // Refused again: not a third time.
            asked.set(0);
            let twice = b"GET /twice HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n";
            let answer = h1_answer(front, twice).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            assert_eq!(asked.get(), 2);
        })
        .await;
}

/// A body that says it has ended with its last frame, as an HTTP/2 client's does, is
/// sent to an HTTP/2 upstream without its end ever being asked for: refused unprocessed
/// once all of it has gone, it is sent once more all the same. The upstream reads the
/// whole body before it refuses, so that none of it is still to go when it does.
#[tokio::test]
async fn a_body_that_ends_with_its_last_frame_is_sent_once_more_when_refused() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let bodies = Rc::new(RefCell::new(Vec::new()));
            let taking = Rc::clone(&bodies);
            let script: Script = Rc::new(move |request, mut respond| {
                let taking = Rc::clone(&taking);
                Box::pin(async move {
                    let mut body = request.into_body();
                    let mut all = Vec::new();
                    while let Some(Ok(chunk)) = body.data().await {
                        let _ = body.flow_control().release_capacity(chunk.len());
                        all.extend_from_slice(&chunk);
                    }
                    let first = taking.borrow().is_empty();
                    taking.borrow_mut().push(all);
                    if first {
                        respond.send_reset(::h2::Reason::REFUSED_STREAM);
                    } else {
                        let _ = respond.send_response(ok_head(), true);
                    }
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::builder()
                .method(Method::POST)
                .uri("http://a.test/")
                .body(())
                .unwrap();
            let (answer, mut upload) = send.send_request(request, false).unwrap();
            upload.send_data(Bytes::from_static(b"hi"), true).unwrap();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            assert_eq!(*bodies.borrow(), vec![b"hi".to_vec(), b"hi".to_vec()]);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                "{scrape}"
            );
        })
        .await;
}

/// A request left above the last stream an upstream's GOAWAY accepted was never
/// processed either, and goes once more — on another connection, the first going.
#[tokio::test]
async fn a_request_left_above_an_http2_goaway_is_sent_once_more() {
    use crate::h2_peer::{self, Peer, code, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = socket.local_addr().unwrap();
            let connections = Rc::new(Cell::new(0_usize));
            let counting = Rc::clone(&connections);
            let _accepting = tokio::task::spawn_local(async move {
                while let Ok((stream, _)) = socket.accept().await {
                    counting.set(counting.get() + 1);
                    let first = counting.get() == 1;
                    let _serving = tokio::task::spawn_local(async move {
                        let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                        let (asked, _) = peer.until(|frame| frame.kind == kind::HEADERS).await;
                        if first {
                            // Nothing accepted: the stream is above the last one.
                            peer.send(&h2_peer::goaway(0, code::NO_ERROR)).await;
                            let _ = peer.rest().await;
                            return;
                        }
                        let answer = h2_peer::headers(asked.stream, h2_peer::response(200), true);
                        peer.send(&answer).await;
                        let _ = peer.rest().await;
                    });
                }
            });
            let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert_eq!(connections.get(), 2);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                "{scrape}"
            );
        })
        .await;
}

/// A worker whose one upstream `up`, at `upstream`, is spoken to in HTTP/2 with
/// `keepalive`.
async fn serving_worker_with_keepalive(
    upstream: SocketAddr,
    keepalive: edgerush_config::Keepalive,
) -> (SocketAddr, Rc<Worker>) {
    let mut config = everything_config(upstream);
    let up = config.upstreams.get_mut("up").unwrap();
    up.protocol = UpstreamProtocol::Http2;
    up.keepalive = Some(keepalive);
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

fn every_second(without_calls: bool) -> edgerush_config::Keepalive {
    edgerush_config::Keepalive {
        interval_seconds: 1,
        timeout_seconds: 1,
        without_calls,
        backend_allows_short_intervals: true,
    }
}

/// What a raw HTTP/2 upstream of the keepalive tests does with PINGs.
#[derive(Clone, Copy, PartialEq)]
enum Pinged {
    /// Answers every one.
    Answers,
    /// Answers the first (the settling PING), then none.
    GoesQuiet,
    /// Answers the first, and says GOAWAY(ENHANCE_YOUR_CALM) at the next.
    CalmsDown,
    /// Answers none, the settling PING included.
    Deaf,
}

/// A raw HTTP/2 upstream that answers each request `200` after `hold`, does with
/// PINGs as `pinged` says on its first connection (and answers them on the rest), and
/// tells, in order, when each PING arrived and on which connection.
async fn pinged_upstream(
    hold: Duration,
    pinged: Pinged,
) -> (SocketAddr, Rc<RefCell<Vec<(usize, tokio::time::Instant)>>>) {
    use crate::h2_peer::{self, Frame, Peer, code, flag, kind};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let pings = Rc::new(RefCell::new(Vec::new()));
    let recording = Rc::clone(&pings);
    let _accepting = tokio::task::spawn_local(async move {
        let mut connections = 0;
        while let Ok((stream, _)) = socket.accept().await {
            connections += 1;
            let (connection, recording) = (connections, Rc::clone(&recording));
            let pinged = if connection == 1 {
                pinged
            } else {
                Pinged::Answers
            };
            let _serving = tokio::task::spawn_local(async move {
                let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                let mut due: Vec<(u32, tokio::time::Instant)> = Vec::new();
                let mut seen = 0;
                loop {
                    let next_answer = due.first().map(|(_, at)| *at);
                    let frame = match next_answer {
                        Some(at) => tokio::select! {
                            frame = peer.try_next() => frame,
                            () = tokio::time::sleep_until(at) => {
                                let (stream, _) = due.remove(0);
                                let answer = h2_peer::headers(stream, h2_peer::response(200), true);
                                peer.send(&answer).await;
                                continue;
                            }
                        },
                        None => peer.try_next().await,
                    };
                    let Some(frame) = frame else { return };
                    if frame.kind == kind::HEADERS {
                        due.push((frame.stream, tokio::time::Instant::now() + hold));
                    } else if frame.kind == kind::PING && !frame.has(flag::ACK) {
                        seen += 1;
                        recording
                            .borrow_mut()
                            .push((connection, tokio::time::Instant::now()));
                        let answers = match pinged {
                            Pinged::Answers => true,
                            Pinged::GoesQuiet | Pinged::CalmsDown => seen == 1,
                            Pinged::Deaf => false,
                        };
                        if answers {
                            let ack = Frame::new(kind::PING, flag::ACK, 0, frame.payload.clone());
                            peer.send(&ack).await;
                        } else if pinged == Pinged::CalmsDown {
                            peer.send(&h2_peer::goaway(0, code::ENHANCE_YOUR_CALM))
                                .await;
                            return;
                        }
                    }
                }
            });
        }
    });
    (address, pings)
}

/// PINGs go while a call is open, one a second, and stop when the connection is idle.
#[tokio::test]
async fn keepalive_pings_while_a_call_is_open_and_not_when_idle() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, pings) =
                pinged_upstream(Duration::from_millis(2500), Pinged::Answers).await;
            let (front, _worker) =
                serving_worker_with_keepalive(upstream, every_second(false)).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            // The settling PING, then one a second while the call was open.
            let while_open = pings.borrow().len();
            assert!(
                while_open >= 3,
                "{while_open} PINGs while the call was open"
            );
            tokio::time::sleep(Duration::from_millis(2500)).await;
            assert_eq!(pings.borrow().len(), while_open, "PINGs with no call open");
        })
        .await;
}

/// A PING nobody answers in time takes its connection for dead: what is on it fails,
/// and the next request goes on a new one.
#[tokio::test]
async fn a_ping_nobody_answers_takes_its_connection_for_dead() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, pings) =
                pinged_upstream(Duration::from_secs(8), Pinged::GoesQuiet).await;
            let (front, worker) =
                serving_worker_with_keepalive(upstream, every_second(false)).await;
            let asked = tokio::time::Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            let took = asked.elapsed();
            assert!(
                took >= Duration::from_secs(2) && took < Duration::from_secs(4),
                "given up on after {took:?}"
            );
            until(|| worker.h2_connections() == 0).await;
            assert!(pings.borrow().len() >= 2);
        })
        .await;
}

/// A connection that never answers even its first PING is as dead as one that stops:
/// it is given up on at the keepalive's timeout.
#[tokio::test]
async fn a_connection_that_never_answers_a_ping_is_given_up_on() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _pings) = pinged_upstream(Duration::from_secs(8), Pinged::Deaf).await;
            let (front, _worker) =
                serving_worker_with_keepalive(upstream, every_second(false)).await;
            let asked = tokio::time::Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            let took = asked.elapsed();
            assert!(took < Duration::from_secs(3), "given up on after {took:?}");
        })
        .await;
}

/// Told to calm down, the client waits twice as long between PINGs on its next
/// connection to the same upstream: gRPC's backoff, so there is no storm of them.
#[tokio::test]
async fn told_to_calm_down_the_next_connection_pings_half_as_often() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, pings) = pinged_upstream(Duration::ZERO, Pinged::CalmsDown).await;
            let (front, worker) = serving_worker_with_keepalive(upstream, every_second(true)).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            // Its first keepalive PING draws the GOAWAY, and the connection goes.
            until(|| pings.borrow().len() >= 2).await;
            until(|| worker.h2_connections() == 0).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            until(|| pings.borrow().iter().filter(|(on, _)| *on == 2).count() >= 2).await;
            let second: Vec<tokio::time::Instant> = pings
                .borrow()
                .iter()
                .filter(|(on, _)| *on == 2)
                .map(|(_, at)| *at)
                .collect();
            let gap = second[1] - second[0];
            assert!(
                gap >= Duration::from_millis(1800) && gap < Duration::from_millis(2800),
                "{gap:?} between the second connection's PINGs"
            );
        })
        .await;
}

/// A field value that starts or ends with whitespace makes an HTTP/2 request malformed
/// (RFC 9113 §8.2.1): its stream is reset with PROTOCOL_ERROR, and it is never forwarded.
/// Forwarded, an HTTP/1.1 upstream would read the value without the whitespace, while a
/// rule's header predicate compares it with it.
#[tokio::test]
async fn an_http2_field_value_with_whitespace_at_either_end_is_not_forwarded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut outcomes = Vec::new();
            for value in [" canary", "canary\t"] {
                let request = Request::get("http://shop.example.com/x")
                    .header("x-env", value)
                    .body(())
                    .unwrap();
                send = within(send.ready()).await.unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let outcome = match within(answer).await {
                    Ok(answer) => format!("answered {}", answer.status()),
                    Err(error) => format!("reset {:?}", error.reason()),
                };
                outcomes.push((value, outcome));
            }
            let forwarded: Vec<_> = seen
                .requests
                .borrow()
                .iter()
                .filter_map(|(request, _)| request.headers().get("x-env").cloned())
                .collect();
            assert!(
                forwarded.is_empty(),
                "malformed requests reached the upstream with x-env {forwarded:?}; \
                 the client was {outcomes:?}"
            );
            for (value, outcome) in &outcomes {
                assert_eq!(outcome, "reset Some(PROTOCOL_ERROR)", "{value:?}");
            }
        })
        .await;
}

/// An upload to an HTTP/2 upstream whose pieces the worker cannot pay for is the worker's
/// own shortage, as it is going to an HTTP/1 upstream: answered 503 and not counted against
/// the upstream, which did nothing wrong (14 §8). The worker can pay for the client's one
/// frame, which h2 holds until the next is asked for, and for nothing more: the piece the
/// upload would stage beside it is what it cannot pay for. The upstream reads the whole body
/// before it answers.
#[tokio::test]
async fn an_upload_to_an_http2_upstream_the_worker_cannot_pay_for_is_answered_503() {
    const FRAME: usize = 4096;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let limits = H1Limits {
                storage: FRAME + 1024,
                ..H1Limits::default()
            };
            let (front, worker) = serving_worker_to_h2(upstream, limits).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://a.test/upload").body(()).unwrap();
            send = within(send.ready()).await.unwrap();
            let (answer, mut body) = send.send_request(request, false).unwrap();
            body.send_data(Bytes::from(vec![b'x'; FRAME]), true)
                .unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            let proxy = &worker.proxy;
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy
                .metrics
                .render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains("edgerush_upstream_failures_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
        })
        .await;
}

/// An upstream connection the worker closes because it cannot pay for what h2 holds of
/// its answers is closed by the worker's shortage, not the upstream's failing: a request
/// still waiting on it for its head is answered 503 and not counted against the upstream
/// (14 §8). Two clients that read nothing fill the worker's storage with their answers on
/// the one connection the worker may open, while a third waits there for an answer the
/// upstream holds back.
#[tokio::test]
async fn a_request_on_an_upstream_connection_shed_for_storage_is_answered_503() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let big = answering_with(4 << 20);
            let script: Script = Rc::new(move |request, respond| {
                if request.uri().path() == "/held" {
                    // Kept and never answered.
                    Box::pin(async move {
                        let _respond = respond;
                        std::future::pending::<()>().await;
                    })
                } else {
                    big(request, respond)
                }
            });
            let upstream = scripted_h2_upstream(script).await;
            let limits = H1Limits {
                storage: 3 << 19,
                h2_connections: 1,
                ..H1Limits::default()
            };
            let (front, worker) = serving_worker_to_h2(upstream, limits).await;
            let mut waiting = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/held").body(()).unwrap();
            let (held, _) = waiting.send_request(request, true).unwrap();
            let mut granting_nothing = ::h2::client::Builder::new();
            granting_nothing.initial_window_size(0);
            let mut stalled = Vec::new();
            for path in ["/one", "/two"] {
                let mut client = h2_library_client(front, &granting_nothing).await;
                let request = Request::get(format!("http://a.test{path}"))
                    .body(())
                    .unwrap();
                let (answer, _) = client.send_request(request, true).unwrap();
                stalled.push((client, answer));
            }
            let held = within(held).await.unwrap();
            assert_eq!(held.status(), StatusCode::SERVICE_UNAVAILABLE);
            let proxy = &worker.proxy;
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy
                .metrics
                .render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains("edgerush_upstream_failures_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
            drop(stalled);
        })
        .await;
}

/// An answer already under way on an upstream connection the worker closes for storage is
/// cut by the worker's shortage, not the upstream's: it is not counted among the upstream's
/// failed bodies (14 §8). A client reads its answer's head and stops, holding what h2 has of
/// its answer; one more that reads nothing takes the worker past what it can pay for on the
/// one connection it may open, and the connection goes. The first client then reads on, and
/// its answer ends cut short.
#[tokio::test]
async fn an_answer_cut_by_a_connection_shed_for_storage_is_not_the_upstreams_body_failing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(answering_with(4 << 20)).await;
            let limits = H1Limits {
                storage: 3 << 19,
                h2_connections: 1,
                ..H1Limits::default()
            };
            let (front, worker) = serving_worker_to_h2(upstream, limits).await;
            let mut reading = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/read").body(()).unwrap();
            let (answer, _) = reading.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            let mut body = answer.into_body();
            let mut granting_nothing = ::h2::client::Builder::new();
            granting_nothing.initial_window_size(0);
            let mut stalled = h2_library_client(front, &granting_nothing).await;
            let request = Request::get("http://a.test/stalled").body(()).unwrap();
            let (_stalled, _) = stalled.send_request(request, true).unwrap();
            until(|| worker.h2_connections() == 0).await;
            let mut read = 0;
            let ended = loop {
                match within(body.data()).await {
                    Some(Ok(data)) => {
                        read += data.len();
                        let _released = body.flow_control().release_capacity(data.len());
                    }
                    Some(Err(error)) => break Some(error),
                    None => break None,
                }
            };
            assert!(ended.is_some(), "all {read} bytes of a cut answer came");
            let proxy = &worker.proxy;
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy
                .metrics
                .render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains("edgerush_upstream_body_failures_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
        })
        .await;
}

/// A field an HTTP/2 client sent "never indexed" goes to an HTTP/2 upstream the same way,
/// and an upstream's back to the client (RFC 7541 §6.2.3, §7.1.3): never in the dynamic
/// table of an upstream connection other clients' requests share. Each side's decoder
/// marks such a field sensitive, so a mark seen there is the representation it came in.
#[tokio::test]
async fn a_field_sent_never_indexed_stays_never_indexed_both_ways_through_an_http2_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let heard = Rc::new(RefCell::new(None::<(bool, bool)>));
            let hearing = Rc::clone(&heard);
            let script: Script = Rc::new(move |request, mut respond| {
                let hearing = Rc::clone(&hearing);
                Box::pin(async move {
                    let fields = request.headers();
                    *hearing.borrow_mut() = Some((
                        fields["x-api-key"].is_sensitive(),
                        fields["x-plain"].is_sensitive(),
                    ));
                    let mut token = http::HeaderValue::from_static("secret");
                    token.set_sensitive(true);
                    let answer = http::Response::builder()
                        .header("x-token", token)
                        .header("x-plain", "a")
                        .body(())
                        .unwrap();
                    let _ = respond.send_response(answer, true);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut key = http::HeaderValue::from_static("secret");
            key.set_sensitive(true);
            let request = Request::get("http://a.test/")
                .header("x-api-key", key)
                .header("x-plain", "a")
                .body(())
                .unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert_eq!(*heard.borrow(), Some((true, false)));
            assert!(answer.headers()["x-token"].is_sensitive());
            assert!(!answer.headers()["x-plain"].is_sensitive());
        })
        .await;
}
