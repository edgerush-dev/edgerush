//! Connects, requests and tries that take too long.

use super::*;

/// A connect that never completes is given up on at the limit and not before, and one
/// that completes inside it is kept (linkerd2-proxy tests its connect timeout the same
/// way, with a connector that never finishes).
#[tokio::test(start_paused = true)]
async fn a_connect_that_never_completes_is_given_up_on_at_its_limit() {
    let limit = Duration::from_secs(5);
    let started = tokio::time::Instant::now();
    let failed = connect_within(limit, std::future::pending::<Result<(), Unconnected>>())
        .await
        .unwrap_err();
    assert!(
        matches!(&failed, ExchangeError::Unconnected(Unconnected::Endpoint(error)) if error.kind() == io::ErrorKind::TimedOut),
        "{failed}"
    );
    assert_eq!(started.elapsed(), limit);

    let slow = async {
        tokio::time::sleep(limit - Duration::from_millis(1)).await;
        Ok::<_, Unconnected>("connected")
    };
    assert_eq!(connect_within(limit, slow).await.unwrap(), "connected");

    let refused = async {
        Err::<(), _>(crate::upstream::dial::connect_failed(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    };
    let failed = connect_within(limit, refused).await.unwrap_err();
    assert!(
        matches!(&failed, ExchangeError::Unconnected(Unconnected::Endpoint(error)) if error.kind() == io::ErrorKind::ConnectionRefused),
        "{failed}"
    );
}

/// An HTTP/1 upstream that leaves the first `silent` requests it is sent unanswered,
/// each holding its connection until the proxy lets go of it, and answers every one
/// after them `200`; and how many requests it was sent. `reading` is whether the silent
/// ones are read at all, or their bytes left in the socket. Bounded: nothing is held for
/// longer than ten seconds, so that a test whose deadline is missing fails rather than
/// hangs.
async fn upstream_silent_at_first(silent: usize, reading: bool) -> (SocketAddr, Rc<Cell<usize>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let asked = Rc::new(Cell::new(0_usize));
    let counting = Rc::clone(&asked);
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let counting = Rc::clone(&counting);
            let _serving = tokio::task::spawn_local(async move {
                let held = Duration::from_secs(10);
                let mut read = Vec::new();
                let mut chunk = [0; 16 * 1024];
                // A head is waited for even by an upstream that then reads no more.
                while !read.windows(4).any(|four| four == b"\r\n\r\n") {
                    match tokio::time::timeout(held, stream.read(&mut chunk)).await {
                        Ok(Ok(n)) if n > 0 => read.extend_from_slice(&chunk[..n]),
                        _ => return,
                    }
                }
                let turn = counting.get();
                counting.set(turn + 1);
                if turn < silent {
                    if reading {
                        // Until the proxy lets go of the connection.
                        let _held = tokio::time::timeout(held, async {
                            while matches!(stream.read(&mut chunk).await, Ok(n) if n > 0) {}
                        })
                        .await;
                    } else {
                        tokio::time::sleep(held).await;
                    }
                    return;
                }
                let answer = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
                let _ = stream.write_all(answer.as_bytes()).await;
            });
        }
    });
    (address, asked)
}

/// Bounds short enough for a test to wait out, whichever hop's clock it means to run
/// out: nothing else is short.
fn quick(limits: impl FnOnce(&mut H1Limits)) -> H1Limits {
    let mut quick = H1Limits::default();
    limits(&mut quick);
    quick
}

/// An upstream that never answers, with its head deadline run out, is answered `504`
/// rather than `502`, and counted as the upstream failing and as what it was.
#[tokio::test]
async fn an_http1_upstream_that_never_answers_is_answered_504() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, asked) = upstream_silent_at_first(usize::MAX, true).await;
            let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
            let (front, worker) =
                serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert_eq!(asked.get(), 1);
            let scrape = worker.proxy().metrics();
            let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// An answer that stops coming before its head, for the idle bound, is the same: `504`.
#[tokio::test]
async fn an_http1_upstream_that_goes_quiet_before_its_head_is_answered_504() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
            let limits = quick(|limits| limits.idle = Duration::from_millis(300));
            let (front, worker) =
                serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// An upstream that stops taking a request's body, while the request is still going out
/// and nothing has been answered, is `504` too: the write-idle clock, and not the
/// client's, ran out.
#[tokio::test]
async fn an_http1_upstream_that_stops_taking_the_upload_is_answered_504() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _asked) = upstream_silent_at_first(usize::MAX, false).await;
            let limits = quick(|limits| limits.idle = Duration::from_millis(300));
            let (front, _worker) =
                serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
            // More than the two sockets' buffers hold, so that the write blocks.
            let size = 64 * 1024 * 1024;
            let head = format!(
                "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
            );
            let (mut read, mut write) = TcpStream::connect(front).await.unwrap().into_split();
            let _uploading = tokio::task::spawn_local(async move {
                let _sent = write.write_all(head.as_bytes()).await;
                let chunk = vec![b'x'; 64 * 1024];
                for _ in 0..size / chunk.len() {
                    if write.write_all(&chunk).await.is_err() {
                        return;
                    }
                }
            });
            let mut answer = Vec::new();
            let _ended = within(read.read_to_end(&mut answer)).await;
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
        })
        .await;
}

/// A client whose upload stops while its request is going upstream is the client
/// being slow, whichever clock sees it first: `408`, and not counted as the upstream
/// failing. Here the exchange's own clock for the request's body runs out before the
/// client connection's does.
#[tokio::test]
async fn an_upload_the_client_stops_is_the_clients_whichever_clock_sees_it() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
            // Shorter than the client connection's idle bound (`SHORT`, 500 ms).
            let limits = quick(|limits| limits.idle = Duration::from_millis(200));
            let (front, worker) =
                serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
            let (mut read, mut write) = TcpStream::connect(front).await.unwrap().into_split();
            write
                .write_all(
                    b"POST / HTTP/1.1\r\nhost: a.test\r\ncontent-length: 1000\r\n\r\nten bytes!",
                )
                .await
                .unwrap();
            let mut answer = Vec::new();
            let _ended = within(read.read_to_end(&mut answer)).await;
            drop(write);
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 408 "), "{answer}");
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 0\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// The same for HTTP/2 upstream: no head within its deadline is `504`.
#[tokio::test]
async fn an_http2_upstream_that_never_answers_is_answered_504() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(never_answering()).await;
            let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
            let (front, worker) = serving_worker_to_h2(upstream, limits).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            let scrape = worker.proxy().metrics();
            let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A gRPC call whose upstream never answers ends with `DEADLINE_EXCEEDED`, in its head:
/// nothing had been said. Not `UNAVAILABLE`, which is what would be sent again.
#[tokio::test]
async fn a_grpc_call_to_an_upstream_that_never_answers_is_deadline_exceeded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(never_answering()).await;
            let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
            let (front, _worker) = serving_worker_to_h2(upstream, limits).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", None), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "4".to_owned(), true)
            );
        })
        .await;
}

/// An HTTP/2 upstream that gives a request's body no room for the idle bound, with no
/// answer yet, is `504`: it is the upstream not taking the upload, not the client.
#[tokio::test]
async fn an_http2_upstream_that_stops_taking_the_upload_is_answered_504() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Never reads the body, so that the room it gave runs out.
            let upstream = scripted_h2_upstream(never_answering()).await;
            let limits = quick(|limits| limits.idle = Duration::from_millis(300));
            let (front, _worker) = serving_worker_to_h2(upstream, limits).await;
            // More than a stream's initial window of 64 KiB.
            let size = 512 * 1024;
            let head = format!(
                "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
            );
            let (mut read, mut write) = TcpStream::connect(front).await.unwrap().into_split();
            let _uploading = tokio::task::spawn_local(async move {
                let _sent = write.write_all(head.as_bytes()).await;
                let _sent = write.write_all(&vec![b'x'; size]).await;
            });
            let mut answer = Vec::new();
            let _ended = within(read.read_to_end(&mut answer)).await;
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
        })
        .await;
}

/// An HTTP/2 upstream that holds every stream and says nothing for a while, then lets
/// go: bounded, so that a deadline that never runs fails the test rather than hanging it.
fn never_answering() -> Script {
    Rc::new(|_request, respond| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            drop(respond);
        })
    })
}

/// A try that ran out of time before its head is sent again when the rule says
/// `on_timeout`, and goes to the next; that one answers.
#[tokio::test]
async fn a_try_that_ran_out_of_time_is_sent_again_when_the_rule_says_so() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, asked) = upstream_silent_at_first(1, true).await;
            let retry = edgerush_config::Retry {
                on_timeout: true,
                ..retrying(1, &[], &[], 1)
            };
            let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
            let (front, worker) =
                serving_worker_with(upstream, UpstreamProtocol::Http1, Some(retry), limits).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            assert_eq!(asked.get(), 2);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                "{scrape}"
            );
        })
        .await;
}

/// Without `on_timeout`, a `502` in the rule's statuses does not cover a try that ran
/// out of time: it is answered `504`, and nothing is sent again.
#[tokio::test]
async fn a_502_does_not_stand_for_a_try_that_ran_out_of_time() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, asked) = upstream_silent_at_first(1, true).await;
            let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
            let (front, worker) = serving_worker_with(
                upstream,
                UpstreamProtocol::Http1,
                Some(retrying(2, &[502], &[], 1)),
                limits,
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert_eq!(asked.get(), 1);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
        })
        .await;
}

/// What `wants_again` says of an upstream that ran out of time or could not be reached:
/// the first is `on_timeout`'s, the second `502`'s, and neither is the other's.
#[test]
fn a_timed_out_try_is_wanted_again_only_under_on_timeout() {
    let retry = |statuses: Vec<u16>, on_timeout| CompiledRetry {
        attempts: 1,
        http_statuses: statuses,
        grpc_statuses: 0,
        on_timeout,
        backoff_base: Duration::from_millis(1),
        backoff_max: Duration::from_millis(1),
    };
    let timed_out = Err(Answer::UpstreamTimedOut);
    let unreached = Err(Answer::UpstreamFailed);
    assert!(wants_again(&retry(vec![], true), &timed_out));
    assert!(!wants_again(&retry(vec![], true), &unreached));
    assert!(!wants_again(&retry(vec![502, 504], false), &timed_out));
    assert!(wants_again(&retry(vec![502], false), &unreached));
}

/// A try that could not connect is wanted again under any retry at all, whatever it
/// names (GEP-1731); one that connected and then failed only under a `502`.
#[test]
fn a_try_that_could_not_connect_is_wanted_again_under_any_retry() {
    let retry = |statuses: Vec<u16>, on_timeout| CompiledRetry {
        attempts: 1,
        http_statuses: statuses,
        grpc_statuses: 0,
        on_timeout,
        backoff_base: Duration::from_millis(1),
        backoff_max: Duration::from_millis(1),
    };
    let unconnected = Err(Answer::Unreachable);
    assert!(wants_again(&retry(vec![503], false), &unconnected));
    assert!(wants_again(&retry(vec![], true), &unconnected));
    assert!(!wants_again(
        &retry(vec![503], false),
        &Err(Answer::UpstreamFailed)
    ));
}

/// A rule's `request` timeout of `ms` milliseconds, `0` for none.
fn request_timeout(ms: u64) -> edgerush_config::Timeouts {
    edgerush_config::Timeouts {
        request_ms: Some(ms),
        backend_request_ms: None,
        tunnel_idle_ms: None,
    }
}

/// A rule's `backend_request` timeout of `ms` milliseconds, and `request` if it says one.
fn try_timeout(ms: u64, request: Option<u64>) -> edgerush_config::Timeouts {
    edgerush_config::Timeouts {
        request_ms: request,
        backend_request_ms: Some(ms),
        tunnel_idle_ms: None,
    }
}

/// A worker for `up` at `upstream` in `protocol`, whose one rule states `timeouts`,
/// with `limits` as its bounds.
async fn serving_worker_timed(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    timeouts: edgerush_config::Timeouts,
    limits: H1Limits,
) -> (SocketAddr, Rc<Worker>) {
    serving_worker_forwarding(upstream, protocol, limits, |forward| {
        forward.timeouts = Some(timeouts);
    })
    .await
}

/// An HTTP/1 upstream that answers each request `delay` after its head has come: a
/// head saying `length` bytes, then those bytes `each` at a time, `gap` apart, one
/// request to a connection. Bounded: it stops at a connection that takes no more, and
/// the slowest answer a test asks of it is a few seconds long.
async fn upstream_answering(
    delay: Duration,
    length: usize,
    each: usize,
    gap: Duration,
) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let _serving = tokio::task::spawn_local(async move {
                let mut read = Vec::new();
                let mut chunk = [0; 16 * 1024];
                while !read.windows(4).any(|four| four == b"\r\n\r\n") {
                    match tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
                        .await
                    {
                        Ok(Ok(n)) if n > 0 => read.extend_from_slice(&chunk[..n]),
                        _ => return,
                    }
                }
                tokio::time::sleep(delay).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {length}\r\nconnection: close\r\n\r\n"
                );
                if stream.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                let mut left = length;
                while left > 0 {
                    let now = each.min(left);
                    if stream.write_all(&vec![b'x'; now]).await.is_err() {
                        return;
                    }
                    left -= now;
                    tokio::time::sleep(gap).await;
                }
            });
        }
    });
    address
}

/// Bounds whose clocks for an answer's head run out after 200 ms: what a rule's stated
/// timeouts are to take the place of.
fn head_clocks_short() -> H1Limits {
    quick(|limits| {
        limits.final_head = Duration::from_millis(200);
        limits.idle = Duration::from_millis(200);
    })
}

/// A rule's `request` timeout ends a request whose answer's head has not come: `504`,
/// counted as the deadline it is and not as the upstream failing, at the timeout and
/// not at the fixed head deadline's 60 seconds.
#[tokio::test]
async fn a_request_timeout_answers_504_when_no_head_has_come() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
            let (front, worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                request_timeout(300),
                H1Limits::default(),
            )
            .await;
            let started = Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            let took = started.elapsed();
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert!(
                took >= Duration::from_millis(300) - EARLY
                    && took < Duration::from_millis(300) + SLACK,
                "{took:?}"
            );
            let scrape = worker.proxy().metrics();
            let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"deadline_exceeded\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 0\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A stated `request` timeout takes the place of the fixed clocks for an answer's head:
/// an upstream that thinks for longer than they allow, and less than the rule does, is
/// answered. Unstated, the same upstream is out of time (the tests of `on_timeout`).
#[tokio::test]
async fn a_request_timeout_takes_the_place_of_the_fixed_head_clocks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                upstream_answering(Duration::from_millis(600), 2, 2, Duration::ZERO).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                request_timeout(3000),
                head_clocks_short(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        })
        .await;
}

/// Gateway API's `0s`: no timeout at all, and none of the fixed clocks for the head in
/// its place.
#[tokio::test]
async fn a_request_timeout_of_zero_waits_for_the_head_as_long_as_it_takes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                upstream_answering(Duration::from_millis(600), 2, 2, Duration::ZERO).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                request_timeout(0),
                head_clocks_short(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        })
        .await;
}

/// An answer still coming when the `request` timeout passes is cut off: its head has
/// gone, so the connection closes short of the length it said. It was moving all the
/// while, so no idle clock is what ended it.
#[tokio::test]
async fn an_answer_still_coming_at_the_request_timeout_is_cut_off() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                upstream_answering(Duration::ZERO, 1000, 10, Duration::from_millis(50)).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                request_timeout(400),
                H1Limits::default(),
            )
            .await;
            let started = Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            let took = started.elapsed();
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            let (_, body) = answer.split_once("\r\n\r\n").unwrap();
            assert!(!body.is_empty() && body.len() < 1000, "{}", body.len());
            assert!(took < Duration::from_millis(400) + SLACK, "{took:?}");
        })
        .await;
}

/// Over HTTP/2 the same is the stream reset, `INTERNAL_ERROR` as for any answer that
/// failed after its head: the connection and its other streams carry on.
#[tokio::test]
async fn an_http2_answer_still_coming_at_the_request_timeout_is_reset() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                upstream_answering(Duration::ZERO, 1000, 10, Duration::from_millis(50)).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                request_timeout(400),
                H1Limits::default(),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://a.test/").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            let mut body = answer.into_body();
            let ended = within(async {
                loop {
                    match body.data().await {
                        Some(Ok(chunk)) => {
                            let _ = body.flow_control().release_capacity(chunk.len());
                        }
                        Some(Err(error)) => return Some(error),
                        None => return None,
                    }
                }
            })
            .await
            .expect("the answer ended as though whole");
            assert_eq!(
                ended.reason(),
                Some(::h2::Reason::INTERNAL_ERROR),
                "{ended:?}"
            );
        })
        .await;
}

/// An HTTP/2 upstream is held to the rule's `request` timeout the same way: `504` when
/// no head has come by then, and none of the fixed clocks for the head in its place.
#[tokio::test]
async fn a_request_timeout_holds_for_an_http2_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(never_answering()).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http2,
                request_timeout(300),
                H1Limits::default(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");

            let thinking: Script = Rc::new(|_request, mut respond| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    let _sent = respond.send_response(ok_head(), true);
                })
            });
            let upstream = scripted_h2_upstream(thinking).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http2,
                request_timeout(3000),
                head_clocks_short(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        })
        .await;
}

/// A try that has no head within the rule's `backend_request` timeout is out of time:
/// `504`, counted as the upstream failing, as a try that ran out the fixed clocks is.
#[tokio::test]
async fn a_try_past_its_backend_request_timeout_is_answered_504() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
            let (front, worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                try_timeout(300, None),
                H1Limits::default(),
            )
            .await;
            let started = Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            let took = started.elapsed();
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert!(
                took >= Duration::from_millis(300) - EARLY
                    && took < Duration::from_millis(300) + SLACK,
                "{took:?}"
            );
            let scrape = worker.proxy().metrics();
            let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A stated `backend_request` timeout takes the place of the fixed clocks for an
/// answer's head, as a `request` timeout does.
#[tokio::test]
async fn a_backend_request_timeout_takes_the_place_of_the_fixed_head_clocks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                upstream_answering(Duration::from_millis(600), 2, 2, Duration::ZERO).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                try_timeout(3000, None),
                head_clocks_short(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        })
        .await;
}

/// Gateway API's conformance case: a retry of two attempts that names no status, each
/// try given 300 ms. Two slow tries and then a quick one are answered by the third;
/// three slow ones are out of time, and nothing is tried a fourth time.
#[tokio::test]
async fn tries_past_their_backend_request_timeout_are_sent_again_under_on_timeout() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for (slow, status) in [(2, "200"), (3, "504")] {
                let (upstream, asked) = upstream_silent_at_first(slow, true).await;
                let retry = edgerush_config::Retry {
                    on_timeout: true,
                    ..retrying(2, &[], &[], 1)
                };
                let (front, _worker) = serving_worker_forwarding(
                    upstream,
                    UpstreamProtocol::Http1,
                    H1Limits::default(),
                    |forward| {
                        forward.timeouts = Some(try_timeout(300, None));
                        forward.retry = Some(retry);
                    },
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(
                    answer.starts_with(&format!("HTTP/1.1 {status} ")),
                    "{slow}: {answer}"
                );
                assert_eq!(asked.get(), 3, "{slow}");
            }
        })
        .await;
}

/// The `request` timeout takes in every try: when it passes, the try in hand is given
/// up and no other is started, however many the retry had left. What ends it is the
/// request's deadline, not the upstream's.
#[tokio::test]
async fn the_request_timeout_ends_the_tries_however_many_are_left() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, asked) = upstream_silent_at_first(usize::MAX, true).await;
            let retry = edgerush_config::Retry {
                on_timeout: true,
                ..retrying(5, &[], &[], 1)
            };
            let (front, worker) = serving_worker_forwarding(
                upstream,
                UpstreamProtocol::Http1,
                H1Limits::default(),
                |forward| {
                    forward.timeouts = Some(try_timeout(300, Some(500)));
                    forward.retry = Some(retry);
                },
            )
            .await;
            let started = Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            let took = started.elapsed();
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert!(took < Duration::from_millis(500) + SLACK, "{took:?}");
            assert_eq!(asked.get(), 2);
            let scrape = worker.proxy().metrics();
            let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"deadline_exceeded\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A try's clock stops at its answer's head: an answer that takes longer than the
/// `backend_request` timeout to stream arrives whole.
#[tokio::test]
async fn a_backend_request_timeout_stops_at_the_head() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream =
                upstream_answering(Duration::ZERO, 1000, 100, Duration::from_millis(100)).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http1,
                try_timeout(300, None),
                H1Limits::default(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            let (_, body) = answer.split_once("\r\n\r\n").unwrap();
            assert_eq!(body.len(), 1000);
        })
        .await;
}

/// An HTTP/2 upstream is held to a `backend_request` timeout the same way, counted as
/// the upstream failing.
#[tokio::test]
async fn a_backend_request_timeout_holds_for_an_http2_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(never_answering()).await;
            let (front, worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http2,
                try_timeout(300, None),
                H1Limits::default(),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            let scrape = worker.proxy().metrics();
            let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A gRPC call's upstream is told the time its try has, when that is less than the
/// call has left: it is all the upstream will be waited for. A call that said no
/// `grpc-timeout` is told nothing.
#[tokio::test]
async fn a_grpc_call_s_upstream_is_told_the_time_its_try_has() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, noted) = grpc_timeouts_noted().await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http2,
                try_timeout(300, None),
                H1Limits::default(),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            for timeout in [None, Some("10S")] {
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", timeout), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "4".to_owned(), true)
                );
            }
            let noted = noted.borrow();
            assert_eq!(noted.len(), 2);
            assert_eq!(noted[0], None);
            let sent = noted[1].as_deref().unwrap();
            let left = crate::grpc::timeout::parse(sent.as_bytes()).unwrap();
            assert!(left <= Duration::from_millis(300), "{sent}");
        })
        .await;
}

/// An HTTP/2 upstream that notes the `grpc-timeout` of each request it is sent, and
/// answers none of them.
async fn grpc_timeouts_noted() -> (SocketAddr, Rc<RefCell<Vec<Option<String>>>>) {
    let noted = Rc::new(RefCell::new(Vec::new()));
    let noting = Rc::clone(&noted);
    let script: Script = Rc::new(move |request, respond| {
        noting.borrow_mut().push(
            request
                .headers()
                .get("grpc-timeout")
                .map(|timeout| timeout.to_str().unwrap().to_owned()),
        );
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            drop(respond);
        })
    });
    (scripted_h2_upstream(script).await, noted)
}

/// A gRPC call is held to the earlier of its own deadline and its rule's: ended with
/// `DEADLINE_EXCEEDED` when the rule's comes first. The upstream is told the time left
/// of that deadline when the call said a `grpc-timeout`, and nothing when it did not:
/// a rule's timeout is not the client's to have sent.
#[tokio::test]
async fn a_grpc_call_is_held_to_the_earlier_of_its_deadline_and_its_rules() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, noted) = grpc_timeouts_noted().await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http2,
                request_timeout(300),
                H1Limits::default(),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            for timeout in [None, Some("10S")] {
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", timeout), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "4".to_owned(), true)
                );
            }
            let noted = noted.borrow();
            assert_eq!(noted.len(), 2);
            assert_eq!(noted[0], None);
            let sent = noted[1].as_deref().unwrap();
            let left = crate::grpc::timeout::parse(sent.as_bytes()).unwrap();
            assert!(left <= Duration::from_millis(300), "{sent}");
        })
        .await;
}

/// A gRPC call whose answer is still coming at its rule's `request` timeout ends as
/// one past its deadline does: trailers with `DEADLINE_EXCEEDED`.
#[tokio::test]
async fn a_grpc_answer_still_coming_at_the_request_timeout_ends_exceeded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let script: Script = Rc::new(|_request, mut respond| {
                Box::pin(async move {
                    let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                        return;
                    };
                    for _ in 0..60 {
                        let message = Bytes::from_static(b"\0\0\0\0\x01x");
                        if sending.send_data(message, false).is_err() {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_timed(
                upstream,
                UpstreamProtocol::Http2,
                request_timeout(400),
                H1Limits::default(),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", None), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "4".to_owned(), false)
            );
        })
        .await;
}

/// A request head that the gateway's own fields take past the head bound on its way
/// upstream is never sent, and no connection is taken for it: it is refused as the
/// gateway's own failure, 500 under the answer reason `edits`, as an edit that does not fit
/// is (14 §6), and not counted against the upstream, which was never asked (C22).
#[tokio::test]
async fn a_request_head_grown_past_its_bound_is_refused_before_it_goes_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, opened) = counting_upstream().await;
            let (front, worker) = serving_worker_and(upstream).await;
            // Exactly as long as a head may be, before the gateway adds its forwarding
            // fields and the request's ID.
            let start = b"GET / HTTP/1.1\r\nhost: a\r\nconnection: close\r\nx-pad: ";
            let end = b"\r\n\r\n";
            let mut request = start.to_vec();
            request.resize(H1Limits::default().head - end.len(), b'v');
            request.extend_from_slice(end);
            let answer = h1_answer(front, &request).await;
            assert!(answer.starts_with("HTTP/1.1 500 "), "{answer}");
            assert_eq!(opened.load(Ordering::SeqCst), 0, "a connection was opened");
            let scrape = worker.proxy().metrics();
            for line in [
                "edgerush_upstream_failures_total{upstream=\"up\"} 0\n",
                "edgerush_listener_local_answers_total{listener=\"web\",reason=\"edits\"} 1\n",
            ] {
                assert!(scrape.contains(line), "{line}{scrape}");
            }
        })
        .await;
}

/// An answer to an HTTP/1.0 client whose length nobody knew goes out ended by the close
/// (14 §4). Cut after its head, it must not end with the ordinary close a whole one ends
/// with: "an error is never disguised as clean EOF" (13 §5), "neither outcome is a clean
/// truncation" (13 §7). Review repro A03-02.
#[tokio::test]
async fn an_http_1_0_answer_cut_after_its_head_does_not_end_as_a_whole_one_does() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (open, gate) = tokio::sync::oneshot::channel();
            let upstream = gated_upstream(
                b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n",
                gate,
                // Not a chunk: the answer's body fails here, after its head has gone.
                b"zz\r\n",
            )
            .await;
            let front = serving_worker(upstream).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            client
                .write_all(b"GET / HTTP/1.0\r\nhost: a\r\n\r\n")
                .await
                .unwrap();
            let mut received = Vec::new();
            while !received.ends_with(b"hello") {
                let mut some = [0; 512];
                let got = within(client.read(&mut some)).await.unwrap();
                assert_ne!(got, 0, "{:?}", String::from_utf8_lossy(&received));
                received.extend_from_slice(&some[..got]);
            }
            let head = String::from_utf8_lossy(&received).into_owned();
            assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
            assert!(
                !head.to_ascii_lowercase().contains("content-length")
                    && !head.to_ascii_lowercase().contains("transfer-encoding"),
                "not ended by the close: {head}"
            );
            let _ = open.send(());
            let mut rest = Vec::new();
            let ended = within(client.read_to_end(&mut rest)).await;
            assert!(
                ended.is_err(),
                "the cut answer ended as a whole one does ({ended:?}), after {:?}",
                String::from_utf8_lossy(&rest)
            );
        })
        .await;
}
