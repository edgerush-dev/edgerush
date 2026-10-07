//! Requests sent again.

use super::*;

/// A worker whose one rule retries as `retry` says, to `up` at `upstream` in `protocol`.
async fn serving_retrying_worker(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    retry: edgerush_config::Retry,
) -> (SocketAddr, Rc<Worker>) {
    serving_worker_with(upstream, protocol, Some(retry), H1Limits::default()).await
}

const POSTING_HI: &[u8] =
    b"POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi";

/// A request whose answer's status the rule names is sent again, body and all, to the
/// answer after it; one whose status it does not name, or with no tries left, is not.
#[tokio::test]
async fn a_rule_sends_a_request_again_for_a_status_it_names() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let retry = retrying(2, &[503], &[], 1);
            let (upstream, bodies) = statuses_upstream(vec![503, 200]).await;
            let (front, worker) =
                serving_retrying_worker(upstream, UpstreamProtocol::Http1, retry.clone()).await;
            let answer = h1_answer(front, POSTING_HI).await;
            assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            assert_eq!(*bodies.borrow(), vec![b"hi".to_vec(), b"hi".to_vec()]);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                "{scrape}"
            );

            // A status it does not name goes to the client as it came.
            let (upstream, bodies) = statuses_upstream(vec![500, 200]).await;
            let (front, _worker) =
                serving_retrying_worker(upstream, UpstreamProtocol::Http1, retry.clone()).await;
            let answer = h1_answer(front, POSTING_HI).await;
            assert!(answer.starts_with("HTTP/1.1 500 "), "{answer}");
            assert_eq!(bodies.borrow().len(), 1);

            // Out of tries: the last answer is the client's.
            let (upstream, bodies) = statuses_upstream(vec![503]).await;
            let (front, _worker) =
                serving_retrying_worker(upstream, UpstreamProtocol::Http1, retry).await;
            let answer = h1_answer(front, POSTING_HI).await;
            assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
            assert_eq!(bodies.borrow().len(), 3);
        })
        .await;
}

/// An upstream that could not be reached counts as a `502` for a rule that names it.
#[tokio::test]
async fn an_unreachable_upstream_is_retried_as_a_502() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_refusing, upstream) = refusing();
            let (front, worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http1,
                retrying(2, &[502], &[], 1),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 2\n"),
                "{scrape}"
            );
        })
        .await;
}

/// `up` at `endpoints`, each in turn, spoken to in `protocol`, with its one rule
/// retrying once for a `503` and nothing else it names.
fn in_turn_retrying_503(
    endpoints: Vec<SocketAddr>,
    protocol: UpstreamProtocol,
) -> edgerush_config::Config {
    let mut config = everything_config(endpoints[0]);
    let up = config.upstreams.get_mut("up").unwrap();
    up.protocol = protocol;
    up.endpoints = endpoints;
    up.load_balancer = edgerush_config::LoadBalancer::RoundRobin;
    config.routes[0].rules[0]
        .forward
        .as_mut()
        .expect("the rule forwards")
        .retry = Some(retrying(1, &[503], &[], 1));
    config
}

/// A try that could not connect is sent to another endpoint under any stated retry,
/// whatever statuses it names, as Gateway API asks (GEP-1731): nothing of the request
/// reached the first. In either protocol; and counted as the upstream failing, and as
/// what it was, when there is no retry to send it on.
#[tokio::test]
async fn a_stated_retry_sends_a_try_that_could_not_connect_elsewhere() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_held, nowhere) = refusing();
            let (h1, _) = statuses_upstream(vec![200]).await;
            let (h2, _, _) = h2_upstream(UpstreamH2::default()).await;
            for (protocol, answering) in [
                (UpstreamProtocol::Http1, h1),
                (UpstreamProtocol::Http2, h2),
            ] {
                let config = in_turn_retrying_503(vec![nowhere, answering], protocol);
                let (front, worker) = serving_config(&config).await;
                // Each endpoint in turn, a retry's turn among them: most first tries
                // are to the one that refuses, and each is sent on.
                for _ in 0..4 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 200 OK\r\n"),
                        "{protocol:?}: {answer}"
                    );
                }
                let scrape = worker.proxy().metrics();
                let retried = counted(&scrape, "edgerush_upstream_retries_total{upstream=\"up\"}");
                let failed = counted(&scrape, "edgerush_upstream_failures_total{upstream=\"up\"}");
                assert!(retried >= 1 && retried == failed, "{protocol:?}: {scrape}");
                // And the endpoint is set aside for it, whichever protocol tried.
                let line = "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 1
";
                assert!(scrape.contains(line), "{protocol:?}: {scrape}");

                let (front, worker) = serving_worker_with(
                    nowhere,
                    protocol,
                    None,
                    H1Limits::default(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{protocol:?}: {answer}");
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_unreachable\"} 1\n";
                assert!(scrape.contains(line), "{protocol:?}: {scrape}");
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
                assert!(scrape.contains(line), "{protocol:?}: {scrape}");
            }
        })
        .await;
}

/// A request with a body is sent on as well when its try could not connect: none of
/// the body went, so all of it is still there to send, whole, to the next endpoint.
#[tokio::test]
async fn a_stated_retry_sends_a_body_whose_try_could_not_connect_elsewhere() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_held, nowhere) = refusing();
            let (h1, bodies) = statuses_upstream(vec![200]).await;
            let (h2, seen, _) = h2_upstream(UpstreamH2::default()).await;
            for (protocol, answering) in
                [(UpstreamProtocol::Http1, h1), (UpstreamProtocol::Http2, h2)]
            {
                let config = in_turn_retrying_503(vec![nowhere, answering], protocol);
                let (front, worker) = serving_config(&config).await;
                for _ in 0..4 {
                    let answer = h1_answer(front, POSTING_HI).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 200 OK\r\n"),
                        "{protocol:?}: {answer}"
                    );
                }
                let scrape = worker.proxy().metrics();
                let retried = counted(&scrape, "edgerush_upstream_retries_total{upstream=\"up\"}");
                assert!(retried >= 1, "{protocol:?}: {scrape}");
            }
            // Every body arrived whole, the ones sent on among them.
            assert_eq!(*bodies.borrow(), vec![b"hi".to_vec(); 4]);
            let read: Vec<usize> = seen
                .requests
                .borrow()
                .iter()
                .map(|(_, read)| *read)
                .collect();
            assert_eq!(read, vec![2; 4]);
        })
        .await;
}

/// So is one whose client waits for leave to send it: the try sent on asks for the
/// body, and the client is told to send it then.
#[tokio::test]
async fn a_body_whose_client_waits_to_send_it_is_sent_on_as_well() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_held, nowhere) = refusing();
            let (h1, bodies) = statuses_upstream(vec![200]).await;
            let (h2, seen, _) = h2_upstream(UpstreamH2::default()).await;
            for (protocol, answering) in [
                (UpstreamProtocol::Http1, h1),
                (UpstreamProtocol::Http2, h2),
            ] {
                let config = in_turn_retrying_503(vec![nowhere, answering], protocol);
                let (front, worker) = serving_config(&config).await;
                for _ in 0..2 {
                    let mut stream = TcpStream::connect(front).await.unwrap();
                    stream
                        .write_all(b"POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nexpect: 100-continue\r\ncontent-length: 2\r\n\r\n")
                        .await
                        .unwrap();
                    let mut first = vec![0; 64];
                    let got = within(stream.read(&mut first)).await.unwrap();
                    let first = String::from_utf8_lossy(&first[..got]).into_owned();
                    assert!(first.starts_with("HTTP/1.1 100 "), "{protocol:?}: {first}");
                    stream.write_all(b"hi").await.unwrap();
                    let mut rest = Vec::new();
                    let _ = within(stream.read_to_end(&mut rest)).await;
                    let rest = String::from_utf8_lossy(&rest);
                    assert!(rest.contains("HTTP/1.1 200 OK\r\n"), "{protocol:?}: {rest}");
                }
                let scrape = worker.proxy().metrics();
                let retried = counted(&scrape, "edgerush_upstream_retries_total{upstream=\"up\"}");
                assert!(retried >= 1, "{protocol:?}: {scrape}");
            }
            assert_eq!(*bodies.borrow(), vec![b"hi".to_vec(); 2]);
            let read: Vec<usize> = seen.requests.borrow().iter().map(|(_, read)| *read).collect();
            assert_eq!(read, vec![2; 2]);
        })
        .await;
}

/// A TLS handshake that fails is a try that could not connect, as GEP-1731 names it:
/// nothing of the request was sent, and a stated retry sends it to another endpoint.
#[tokio::test]
async fn a_stated_retry_sends_a_try_whose_handshake_failed_elsewhere() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let server = certificate(&["backend.test"]);
            // The same name, on a certificate nobody trusts.
            let stranger = certificate(&["backend.test"]);
            let trusted = tls_upstream(&server, Agrees::Either).await;
            let untrusted = tls_upstream(&stranger, Agrees::Either).await;
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let mut config = in_turn_retrying_503(vec![untrusted, trusted], protocol);
                config.upstreams.get_mut("up").unwrap().tls =
                    Some(trusting("backend.test", &server));
                let (front, worker) = serving_config(&config).await;
                for _ in 0..4 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 200 OK\r\n"),
                        "{protocol:?}: {answer}"
                    );
                }
                let scrape = worker.proxy().metrics();
                let retried = counted(&scrape, "edgerush_upstream_retries_total{upstream=\"up\"}");
                let failed = counted(&scrape, "edgerush_upstream_failures_total{upstream=\"up\"}");
                assert!(retried >= 1 && retried == failed, "{protocol:?}: {scrape}");
                // TCP got through: a handshake that failed after it sets nothing aside.
                let line = "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 0\n";
                assert!(scrape.contains(line), "{protocol:?}: {scrape}");
            }
        })
        .await;
}

/// The value of the one series of `scrape` named `series`, labels and all.
fn counted(scrape: &str, series: &str) -> u64 {
    scrape
        .lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no {series} in {scrape}"))
}

/// An endpoint that took the connection, and the request with it, may have acted on the
/// request before closing without a word: that is not a try that could not connect, and
/// a retry that names no `502` does not send it again.
#[tokio::test]
async fn a_try_that_connected_is_sent_again_only_for_what_its_retry_names() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use tokio::io::AsyncReadExt;
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let closing = socket.local_addr().unwrap();
            let _accepting = tokio::task::spawn_local(async move {
                while let Ok((mut stream, _)) = socket.accept().await {
                    // The request's head read, and the connection closed on it.
                    let mut head = [0; 1024];
                    let _read = stream.read(&mut head).await;
                }
            });
            let (answering, _) = statuses_upstream(vec![200]).await;
            let config = in_turn_retrying_503(vec![closing, answering], UpstreamProtocol::Http1);
            let (front, worker) = serving_config(&config).await;
            let mut failed = 0;
            for _ in 0..4 {
                let answer = h1_answer(front, CLOSING_GET).await;
                if answer.starts_with("HTTP/1.1 502 ") {
                    failed += 1;
                } else {
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                }
            }
            // Each endpoint in turn, and nothing sent again to take a turn.
            assert_eq!(failed, 2);
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_retries_total{upstream=\"up\"} 0\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A body past what is kept goes on as it came, and is not sent again.
#[tokio::test]
async fn a_body_too_big_to_keep_is_not_sent_again() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, bodies) = statuses_upstream(vec![503, 200]).await;
            let (front, worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http1,
                retrying(2, &[503], &[], 1),
            )
            .await;
            let size = crate::retry::replay::MOST + 1;
            let mut request = format!(
                "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
            )
            .into_bytes();
            request.resize(request.len() + size, b'x');
            let answer = h1_answer(front, &request).await;
            assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
            assert_eq!(bodies.borrow().len(), 1);
            assert_eq!(bodies.borrow()[0].len(), size);
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_retries_refused_total{upstream=\"up\",reason=\"body\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// Retries stop when the upstream's budget is spent: its reserve of 100 and a fifth of
/// its requests, each retry costing five. Request `n` finds `505 - 4n` left, so the
/// 126th of an upstream that always fails is the first not sent again.
#[tokio::test]
async fn retries_stop_when_the_budget_is_spent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, bodies) = statuses_upstream(vec![503]).await;
            let (front, worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http1,
                retrying(1, &[503], &[], 1),
            )
            .await;
            for _ in 0..126 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
            }
            assert_eq!(bodies.borrow().len(), 125 * 2 + 1);
            let scrape = worker.proxy().metrics();
            let line =
                "edgerush_upstream_retries_refused_total{upstream=\"up\",reason=\"budget\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// A gRPC call whose trailers-only answer carries a status the rule names is sent
/// again, and only the answer the client gets is counted. A status in trailers after
/// a head is not retried — the head has gone to the client — and nor is a call whose
/// deadline comes before its backoff would end.
#[tokio::test]
async fn a_grpc_call_is_sent_again_for_a_status_in_its_head() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let asked = Rc::new(Cell::new(0_usize));
            let asking = Rc::clone(&asked);
            let script: Script = Rc::new(move |request, mut respond| {
                let asking = Rc::clone(&asking);
                Box::pin(async move {
                    asking.set(asking.get() + 1);
                    let mut unavailable = http::HeaderMap::new();
                    unavailable.insert("grpc-status", "14".parse().unwrap());
                    if request.uri().path() == "/pkg.Svc/Late" {
                        let mut stream = respond.send_response(grpc_head(), false).unwrap();
                        let _ = stream.send_trailers(unavailable);
                        return;
                    }
                    let first = asking.get() % 2 == 1;
                    let mut head = grpc_head();
                    let code = if first { "14" } else { "0" };
                    head.headers_mut()
                        .insert("grpc-status", code.parse().unwrap());
                    let _ = respond.send_response(head, true);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http2,
                retrying(2, &[], &["UNAVAILABLE"], 1),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", None), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "0".to_owned(), true)
            );
            assert_eq!(asked.get(), 2);
            let scrape = worker.proxy().metrics();
            assert!(
                !scrape.contains("status=\"UNAVAILABLE\"} 1"),
                "a call set aside was counted: {scrape}"
            );
            let line = "edgerush_listener_grpc_calls_total{listener=\"web\",status=\"OK\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");

            asked.set(0);
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Late", None), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "14".to_owned(), false)
            );
            assert_eq!(asked.get(), 1);

            // A backoff past the deadline: the answer there is, at once.
            let (front, _worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http2,
                retrying(2, &[], &["UNAVAILABLE"], 60_000),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            asked.set(0);
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", Some("5S")), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "14".to_owned(), true)
            );
            assert_eq!(asked.get(), 1);
        })
        .await;
}

/// A gRPC status in a head that does not end the stream is not a trailers-only answer: the
/// call's messages and its trailers follow, and it is not sent again for the status in its
/// head (03 §6: "a gRPC status named in a trailers-only head") (A08-04).
#[tokio::test]
async fn a_status_in_a_head_that_goes_on_does_not_send_a_call_again() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let asked = Rc::new(Cell::new(0_usize));
            let asking = Rc::clone(&asked);
            let script: Script = Rc::new(move |_request, mut respond| {
                let asking = Rc::clone(&asking);
                Box::pin(async move {
                    asking.set(asking.get() + 1);
                    let mut head = grpc_head();
                    head.headers_mut()
                        .insert("grpc-status", "14".parse().unwrap());
                    let Ok(mut stream) = respond.send_response(head, false) else {
                        return;
                    };
                    // One empty message, then the call's own status.
                    let _ = stream.send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false);
                    let mut ok = http::HeaderMap::new();
                    ok.insert("grpc-status", "0".parse().unwrap());
                    let _ = stream.send_trailers(ok);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http2,
                retrying(2, &[], &["UNAVAILABLE"], 1),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", None), true)
                .unwrap();
            let mut body = within(answer).await.unwrap().into_body();
            while let Some(chunk) = within(body.data()).await {
                let _ = body.flow_control().release_capacity(chunk.unwrap().len());
            }
            let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx)))
                .await
                .unwrap()
                .expect("no trailers");
            assert_eq!(trailers["grpc-status"], "0");
            assert_eq!(
                asked.get(),
                1,
                "sent again for a status in a head that went on"
            );
        })
        .await;
}

/// A retry with no place for it on the worker is not sent, takes nothing from the budget,
/// and is counted `busy` (08 §1; review A08-02): the answer it would have replaced is the
/// client's.
#[tokio::test]
async fn a_retry_with_no_place_on_the_worker_is_counted_busy() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, bodies) = statuses_upstream(vec![503, 200]).await;
            // Room for one exchange: the retry's place would be a second.
            let limits = H1Limits {
                exchanges: 1,
                ..H1Limits::default()
            };
            let retry = retrying(2, &[503], &[], 1);
            let (front, worker) =
                serving_worker_with(upstream, UpstreamProtocol::Http1, Some(retry), limits).await;
            let answer = h1_answer(front, POSTING_HI).await;
            assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
            assert_eq!(bodies.borrow().len(), 1);
            let scrape = worker.proxy().metrics();
            for (reason, count) in [("busy", 1), ("budget", 0), ("deadline", 0)] {
                let line = format!(
                    "edgerush_upstream_retries_refused_total{{upstream=\"up\",reason=\"{reason}\"}} {count}"
                );
                assert!(scrape.lines().any(|shown| shown == line), "{line}");
            }
        })
        .await;
}

/// A retry refused because its backoff would end past the call's deadline is not sent, and
/// takes nothing from the upstream's budget: after many such calls, a call with no deadline
/// whose answer the rule names is still sent again (A08-02).
#[tokio::test]
async fn a_retry_refused_for_its_deadline_spends_none_of_the_budget() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let asked = Rc::new(Cell::new(0_usize));
            let asking = Rc::clone(&asked);
            let script: Script = Rc::new(move |_request, mut respond| {
                let asking = Rc::clone(&asking);
                Box::pin(async move {
                    asking.set(asking.get() + 1);
                    let mut head = grpc_head();
                    head.headers_mut()
                        .insert("grpc-status", "14".parse().unwrap());
                    let _ = respond.send_response(head, true);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            // A backoff of half a second at the least: longer than the calls' deadlines.
            let (front, worker) = serving_retrying_worker(
                upstream,
                UpstreamProtocol::Http2,
                retrying(1, &[], &["UNAVAILABLE"], 500),
            )
            .await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            // More calls than the budget's reserve of 100 retries and a fifth of the calls
            // would pay for, were each refused retry withdrawn: none may be sent again.
            for _ in 0..126 {
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", Some("100m")), true)
                    .unwrap();
                assert_eq!(grpc_outcome(answer).await.1, "14");
            }
            assert_eq!(asked.get(), 126, "a call past its deadline was sent again");
            // A call with no deadline: its one retry is well within the budget.
            asked.set(0);
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", None), true)
                .unwrap();
            let _ = grpc_outcome(answer).await;
            let scrape = worker.proxy().metrics();
            assert_eq!(
                asked.get(),
                2,
                "a retry within the budget was refused after retries that were never sent: {}",
                scrape
                    .lines()
                    .filter(|line| line.starts_with("edgerush_upstream_retries"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            // Each refused retry counted by why (08 §1).
            let line =
                "edgerush_upstream_retries_refused_total{upstream=\"up\",reason=\"deadline\"} 126";
            assert!(scrape.lines().any(|shown| shown == line), "{line}");
        })
        .await;
}
