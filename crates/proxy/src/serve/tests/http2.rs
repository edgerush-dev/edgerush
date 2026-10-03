//! Clients that speak HTTP/2.

use super::*;

/// An HTTP/2 connection with no stream open is told to go at its keep-alive deadline —
/// a graceful GOAWAY, naming no last stream yet — and, as this client never answers the
/// PING that comes with it, closed at its closing bound after that. hyper's HTTP/2
/// server, before ours, held such a connection for ever.
#[tokio::test]
async fn an_idle_http2_connection_is_told_to_go_at_its_keep_alive_deadline() {
    use crate::h2_peer::{code, flag, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            let mut peer = h2_client(front).await;
            h2_get(&mut peer, 1, "/").await;
            peer.until(|f| f.stream == 1 && f.has(flag::END_STREAM))
                .await;
            let answered = tokio::time::Instant::now();

            let (goaway, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
            let took = answered.elapsed();
            assert!(
                took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                "told to go after {took:?}"
            );
            assert_eq!(goaway.goaway(), (0x7fff_ffff, code::NO_ERROR));

            let told = tokio::time::Instant::now();
            let _rest = peer.rest().await;
            let took = told.elapsed();
            let closing = Bounds::default().next_head.min(SHORT.next_request);
            assert!(took < closing + SLACK, "closed {took:?} after GOAWAY");
        })
        .await;
}

/// A stream still waiting for its answer keeps its connection: the keep-alive clock
/// runs only while no stream is open, and starts when the last one ends.
#[tokio::test]
async fn an_http2_connection_with_a_stream_open_is_not_idle() {
    use crate::h2_peer::{flag, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let front = serving_worker(upstream).await;
            let mut peer = h2_client(front).await;
            // Held unanswered by the upstream.
            h2_get(&mut peer, 1, "/held").await;
            let quiet = peer.drain_for(SHORT.next_request * 2).await;
            assert!(
                quiet.iter().all(|f| f.kind != kind::GOAWAY),
                "told to go with a stream open: {quiet:?}"
            );

            // The upstream goes away; the stream is answered for it, and ends.
            held.borrow_mut().clear();
            peer.until(|f| f.stream == 1 && f.has(flag::END_STREAM))
                .await;
            let ended = tokio::time::Instant::now();
            peer.until(|f| f.kind == kind::GOAWAY).await;
            let took = ended.elapsed();
            assert!(
                took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                "told to go {took:?} after the last stream ended"
            );
        })
        .await;
}

/// A stream the client resets ends with nothing written, and the keep-alive clock still
/// starts then: the stream's end wakes the connection, where no frame would.
#[tokio::test]
async fn the_keep_alive_clock_starts_when_the_client_resets_its_last_stream() {
    use crate::h2_peer::{self, code, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let front = serving_worker(upstream).await;
            let mut peer = h2_client(front).await;
            h2_get(&mut peer, 1, "/held").await;
            peer.settled().await;
            peer.send(&h2_peer::rst_stream(1, code::CANCEL)).await;
            let reset = tokio::time::Instant::now();
            peer.until(|f| f.kind == kind::GOAWAY).await;
            let took = reset.elapsed();
            assert!(
                took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                "told to go {took:?} after the reset"
            );
        })
        .await;
}

/// An upload that stops while it is waited on is answered 408 at its idle deadline, on
/// its own stream: the connection and its other streams carry on (14 §8).
#[tokio::test]
async fn a_stalled_http2_upload_is_answered_408_at_its_idle_deadline() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let front = serving_worker(upstream).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://example.test/held")
                .version(Version::HTTP_2)
                .body(())
                .unwrap();
            let (response, mut upload) = send.send_request(request, false).unwrap();
            upload.send_data(Bytes::from_static(b"abc"), false).unwrap();
            let stalled = tokio::time::Instant::now();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .expect("never answered")
                .unwrap();
            let took = stalled.elapsed();
            assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
            assert!(
                took + EARLY >= SHORT.idle && took < SHORT.idle + SLACK,
                "answered after {took:?}"
            );
        })
        .await;
}

/// An upload that stops after its answer has begun cannot be answered 408 any more: the
/// stream is cancelled at the idle deadline, CANCEL and not INTERNAL_ERROR, since it
/// was the client that stopped.
#[tokio::test]
async fn an_http2_upload_stalled_under_its_answer_is_cancelled_at_its_idle_deadline() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let front = serving_worker(upstream).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://example.test/endless")
                .version(Version::HTTP_2)
                .body(())
                .unwrap();
            let (response, mut upload) = send.send_request(request, false).unwrap();
            upload.send_data(Bytes::from_static(b"abc"), false).unwrap();
            let stalled = tokio::time::Instant::now();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .expect("no head")
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let mut body = response.into_body();
            let error = loop {
                match tokio::time::timeout(Duration::from_secs(10), body.data())
                    .await
                    .expect("never cancelled")
                {
                    Some(Ok(data)) => {
                        let _ = body.flow_control().release_capacity(data.len());
                    }
                    Some(Err(error)) => break error,
                    None => panic!("the endless answer ended"),
                }
            };
            let took = stalled.elapsed();
            assert_eq!(error.reason(), Some(::h2::Reason::CANCEL), "{error:?}");
            assert!(
                took + EARLY >= SHORT.idle && took < SHORT.idle + SLACK,
                "cancelled after {took:?}"
            );
        })
        .await;
}

/// A client that gives no room for its answer has the stream cancelled at the idle
/// deadline, and the upstream's answer is let go of with it.
#[tokio::test]
async fn an_http2_answer_given_no_room_is_cancelled_at_its_idle_deadline() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            let mut builder = ::h2::client::Builder::new();
            builder.initial_window_size(0);
            let mut send = h2_library_client(front, &builder).await;
            let request = Request::get("http://example.test/")
                .version(Version::HTTP_2)
                .body(())
                .unwrap();
            let (response, _) = send.send_request(request, true).unwrap();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .expect("no head")
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let headed = tokio::time::Instant::now();
            let mut body = response.into_body();
            let ended = tokio::time::timeout(Duration::from_secs(10), body.data())
                .await
                .expect("never cancelled");
            let took = headed.elapsed();
            let error = match ended {
                Some(Err(error)) => error,
                other => panic!("not cancelled: {other:?}"),
            };
            assert_eq!(error.reason(), Some(::h2::Reason::CANCEL), "{error:?}");
            assert!(
                took + EARLY >= SHORT.idle && took < SHORT.idle + SLACK,
                "cancelled after {took:?}"
            );
        })
        .await;
}

/// An upstream that reads a request's head — and its chunked body to the end, if
/// `whole` — then says `said` and holds the connection.
async fn saying_upstream(said: &'static [u8], whole: bool) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = backend.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        loop {
            let (mut stream, _) = backend.accept().await.unwrap();
            let _answering = tokio::task::spawn_local(async move {
                let mut seen = Vec::new();
                let mut byte = [0; 1];
                let end: &[u8] = if whole { b"0\r\n\r\n" } else { b"\r\n\r\n" };
                while !seen.ends_with(end) {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => seen.push(byte[0]),
                    }
                }
                let _ = stream.write_all(said).await;
                let mut rest = Vec::new();
                let _ = stream.read_to_end(&mut rest).await;
            });
        }
    });
    address
}

/// The interim heads a response future hands over, until the final head is next.
async fn interim_heads(
    response: &mut ::h2::client::ResponseFuture,
) -> Vec<(StatusCode, http::HeaderMap)> {
    let mut heads = Vec::new();
    while let Some(head) = tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| response.poll_informational(cx)),
    )
    .await
    .expect("no interim head nor final one")
    {
        let head = head.unwrap();
        heads.push((head.status(), head.headers().clone()));
    }
    heads
}

/// An upstream's 103 reaches an HTTP/2 client before its final answer, with its fields:
/// what hyper's HTTP/2 server could not send (14 §5, 15 §1).
#[tokio::test]
async fn an_upstream_103_reaches_an_http2_client_before_its_answer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (open, gate) = tokio::sync::oneshot::channel();
            let upstream = gated_upstream(
                b"HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\n",
                gate,
                b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
            )
            .await;
            let front = serving_worker(upstream).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://example.test/")
                .version(Version::HTTP_2)
                .body(())
                .unwrap();
            let (mut response, _) = send.send_request(request, true).unwrap();
            // Heard while the upstream is still working on its answer: the final head is
            // not written until the hint has arrived.
            let hint = tokio::time::timeout(
                Duration::from_secs(10),
                std::future::poll_fn(|cx| response.poll_informational(cx)),
            )
            .await
            .expect("the hint waited for the answer")
            .expect("the final head came first")
            .unwrap();
            assert_eq!(hint.status(), StatusCode::EARLY_HINTS);
            assert_eq!(hint.headers()["link"], "</a.css>; rel=preload");
            open.send(()).unwrap();
            assert!(interim_heads(&mut response).await.is_empty());
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        })
        .await;
}

/// An HTTP/2 client that asks to be told before it sends its body is told, with a
/// `100`, and answered once it has sent it.
#[tokio::test]
async fn an_http2_client_expecting_continue_is_told_to_send_its_body() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                saying_upstream(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok", true).await;
            let front = serving_worker(upstream).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://example.test/")
                .version(Version::HTTP_2)
                .header("expect", "100-continue")
                .body(())
                .unwrap();
            let (mut response, mut upload) = send.send_request(request, false).unwrap();
            // Nothing sent until told to: the first thing heard is the 100.
            let told = tokio::time::timeout(
                Duration::from_secs(10),
                std::future::poll_fn(|cx| response.poll_informational(cx)),
            )
            .await
            .expect("never told to send")
            .expect("the final head came first")
            .unwrap();
            assert_eq!(told.status(), StatusCode::CONTINUE);
            upload.send_data(Bytes::from_static(b"abc"), true).unwrap();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        })
        .await;
}

/// Where the upstream is not asked to say yes first — a filter takes `Expect` off — the
/// client is told to send its body as soon as the body is wanted, not after the
/// continue wait (14 §5).
#[tokio::test]
async fn an_http2_client_expecting_continue_is_told_at_once_when_the_upstream_is_not_asked() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                saying_upstream(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok", true).await;
            let yaml = format!(
                r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        filters:
          - type: request_header_modifier
            remove: [expect]
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
upstreams:
  up: {{ load_balancer: p2c, endpoints: ["{upstream}"] }}
"#
            );
            let config: Config = serde_saphyr::from_str(&yaml).unwrap();
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::post("http://example.test/")
                .version(Version::HTTP_2)
                .header("expect", "100-continue")
                .body(())
                .unwrap();
            let asked = tokio::time::Instant::now();
            let (mut response, mut upload) = send.send_request(request, false).unwrap();
            let told = tokio::time::timeout(
                Duration::from_secs(10),
                std::future::poll_fn(|cx| response.poll_informational(cx)),
            )
            .await
            .expect("never told to send")
            .expect("the final head came first")
            .unwrap();
            assert_eq!(told.status(), StatusCode::CONTINUE);
            let took = asked.elapsed();
            assert!(
                took < H1Limits::default().continue_wait / 2,
                "told only after {took:?}: by the wait, not the wanted body"
            );
            upload.send_data(Bytes::from_static(b"abc"), true).unwrap();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        })
        .await;
}

/// A rapid reset (CVE-2023-44487): streams opened and reset at once, each let settle so
/// that h2's own bound on resets waiting to be accepted is not what stops it. Past 500
/// streams, half or more reset before their answer, the connection is told to calm
/// down and closed (15 §3).
#[tokio::test]
async fn a_rapid_reset_is_cut_off_by_its_share_of_early_resets() {
    use crate::h2_peer::{self, code, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let front = serving_worker(upstream).await;
            let mut peer = h2_client(front).await;
            let mut cut_off = None;
            for n in 0..600u32 {
                let id = n * 2 + 1;
                h2_get(&mut peer, id, "/held").await;
                peer.send_if_open(&h2_peer::rst_stream(id, code::CANCEL))
                    .await;
                let frames = peer.settled().await;
                if let Some(goaway) = frames.iter().find(|f| f.kind == kind::GOAWAY) {
                    cut_off = Some((n + 1, goaway.goaway().1));
                    break;
                }
            }
            let (after, code) = cut_off.expect("never cut off");
            assert_eq!(code, code::ENHANCE_YOUR_CALM);
            assert!(
                (500..=510).contains(&after),
                "cut off after {after} streams"
            );
        })
        .await;
}

/// A client that cancels now and then — one stream in ten — is nowhere near the rule,
/// and keeps its connection.
#[tokio::test]
async fn a_client_that_cancels_now_and_then_keeps_its_connection() {
    use crate::h2_peer::{self, code, flag, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            let mut peer = h2_client(front).await;
            for n in 0..600u32 {
                let id = n * 2 + 1;
                h2_get(&mut peer, id, "/").await;
                if n % 10 == 0 {
                    peer.send_if_open(&h2_peer::rst_stream(id, code::CANCEL))
                        .await;
                    let frames = peer.settled().await;
                    assert!(frames.iter().all(|f| f.kind != kind::GOAWAY), "at {n}");
                } else {
                    let (_, before) = peer
                        .until(|f| f.stream == id && f.has(flag::END_STREAM))
                        .await;
                    assert!(before.iter().all(|f| f.kind != kind::GOAWAY), "at {n}");
                }
            }
        })
        .await;
}

/// Streams the client resets while their upstream is still working let go of what they
/// held: the worker's count of exchanges in hand goes back to nothing.
#[tokio::test]
async fn resetting_http2_streams_leaks_no_admission() {
    use crate::h2_peer::{self, code};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let (front, worker) = serving_worker_and(upstream).await;
            let mut peer = h2_client(front).await;
            for n in 0..20u32 {
                h2_get(&mut peer, n * 2 + 1, "/held").await;
            }
            until(|| held.borrow().len() == 20).await;
            assert_eq!(worker.places.held(), 20);
            for n in 0..20u32 {
                peer.send(&h2_peer::rst_stream(n * 2 + 1, code::CANCEL))
                    .await;
            }
            until(|| worker.places.held() == 0).await;
        })
        .await;
}

/// The HTTP/2 server holds header lists to 64 KiB, as HTTP/1 holds heads: one under it is
/// served, one over it answered 431 by h2 before the core sees it, and the connection
/// carries on (15 §3).
#[tokio::test]
async fn an_http2_header_list_past_64_kib_is_answered_431() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for size in [60_000, 70_000] {
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .header("x-large", "a".repeat(size))
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .unwrap()
                    .unwrap();
                statuses.push(response.status());
            }
            assert_eq!(
                statuses,
                vec![StatusCode::OK, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE]
            );
        })
        .await;
}

/// A client that gives its request up gives its stream's place back: with room for one
/// stream on one connection, the next request is served rather than kept waiting.
#[tokio::test]
async fn a_request_given_up_gives_its_http2_place_back() {
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
                ..H1Limits::default()
            };
            let (front, _worker) = serving_worker_to_h2(upstream, limits).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/").body(()).unwrap();
            let (answer, _sending) = send.send_request(request, true).unwrap();
            until(|| seen.open.get() == 1).await;
            // Dropping what waits for the answer resets the stream.
            drop(answer);
            drop(_sending);
            tokio::time::sleep(Duration::from_millis(100)).await;
            // One permit for the given-up stream, which finds it reset; one for this.
            gate.add_permits(2);
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
        })
        .await;
}
