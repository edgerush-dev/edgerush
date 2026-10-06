//! A worker's access-log records at the end of the data plane ([21 §4] in the docs).
//!
//! [21 §4]: ../../../../../docs/21-access-logs.md

use super::*;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// At the end, a worker hands over the records it holds at once, not at its next sweep, and
/// they are written before [`Proxy::finish_logs`] returns.
#[tokio::test]
async fn the_end_has_every_worker_hand_over_its_records_at_once() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("edgerush-end-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("access.log");
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate, access_log: {{ file: '{}' }} }}
routes: []
upstreams: {{}}
"#,
        path.display()
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A sweep that would not come again for a minute: only being asked brings it.
            let limits = H1Limits {
                sweep: Duration::from_secs(60),
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(Arc::clone(&proxy), limits);
            let _maintained = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            tokio::task::yield_now().await;
            let sink = proxy.current.load().logs[0].unwrap();
            worker
                .batches
                .record(&proxy.logs, sink, |out| out.extend_from_slice(b"held\n"));
            let started = Instant::now();
            let finishing = Arc::clone(&proxy);
            let written =
                tokio::task::spawn_blocking(move || finishing.finish_logs(Duration::from_secs(10)))
                    .await
                    .unwrap();
            assert!(written);
            assert!(started.elapsed() < Duration::from_secs(5));
        })
        .await;
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "held\n");
    // Only what this test made.
    let _removed = std::fs::remove_dir_all(&directory);
}

/// A directory of the test's own, named for it.
fn scratch(test: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("edgerush-{test}-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    directory
}

/// A worker serving `config`, whose listener `web` logs to a file of the test's own, swept
/// every 50 ms so that what it logs is written within moments.
struct Recording {
    front: SocketAddr,
    worker: Rc<Worker>,
    path: PathBuf,
    directory: PathBuf,
}

async fn recording(test: &str, mut config: Config) -> Recording {
    let directory = scratch(test);
    let path = directory.join("access.log");
    config.listeners.get_mut("web").unwrap().access_log =
        Some(edgerush_config::AccessLog::File(path.clone()));
    let (front, worker) = serving_swept(compile(&config).unwrap()).await;
    Recording {
        front,
        worker,
        path,
        directory,
    }
}

impl Recording {
    /// The records written so far, once there are `count` of them.
    async fn records(&self, count: usize) -> Vec<serde_json::Value> {
        within(async {
            loop {
                let written = std::fs::read_to_string(&self.path).unwrap_or_default();
                if written.lines().count() >= count {
                    return written
                        .lines()
                        .map(|line| serde_json::from_str(line).unwrap())
                        .collect();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        // Only what this test made.
        let _removed = std::fs::remove_dir_all(&self.directory);
    }
}

/// A request that went upstream: who sent it, what it asked, where it went, what came
/// back and how long that took (08 §2).
#[tokio::test]
async fn a_proxied_request_is_logged_with_where_it_went() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let logged = recording("proxied", everything_config(upstream)).await;
            let request = b"GET /a/ok HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n";
            let answer = h1_answer(logged.front, request).await;
            assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["kind"], "request");
            assert_eq!(record["listener"], "web");
            assert_eq!(record["client"], "127.0.0.1");
            assert!(record["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
            assert_eq!(record["protocol"], "1.1");
            assert_eq!(record["method"], "GET");
            assert_eq!(record["host"], "example.test");
            assert_eq!(record["path"], "/a/ok");
            assert_eq!(record["status"], 200);
            assert!(record.get("reason").is_none(), "{record}");
            assert_eq!(record["route"], "everything");
            assert_eq!(record["rule"], 0);
            assert_eq!(record["upstream"], "up");
            assert_eq!(record["endpoint"], upstream.to_string());
            assert_eq!(record["tries"], 1);
            assert_eq!(record["bytes_in"], 0);
            assert_eq!(record["bytes_out"], 2);
            assert_eq!(record["id"].as_str().unwrap().len(), 36);
            assert!(record["time"].as_str().unwrap().ends_with('Z'));
            let took = record["duration_ms"].as_f64().unwrap();
            let upstream_took = record["upstream_ms"].as_f64().unwrap();
            assert!(upstream_took <= took, "{record}");
        })
        .await;
}

/// The data plane's own answer says why it gave it, with as much of where the request
/// was going as it got to.
#[tokio::test]
async fn the_gateways_own_answer_says_why() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = everything_config("127.0.0.1:9".parse().unwrap());
            config.upstreams.get_mut("up").unwrap().endpoints.clear();
            let logged = recording("own", config).await;
            let answer = h1_answer(logged.front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["status"], 503);
            assert_eq!(record["reason"], "no_endpoints");
            assert_eq!(record["route"], "everything");
            assert_eq!(record["upstream"], "up");
            assert!(record.get("endpoint").is_none(), "{record}");
            assert!(record.get("tries").is_none(), "{record}");
            assert!(record.get("upstream_ms").is_none(), "{record}");
        })
        .await;
}

/// The bytes of the body each way.
#[tokio::test]
async fn a_body_is_counted_each_way() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, bodies) = statuses_upstream(vec![200]).await;
            let logged = recording("bodies", everything_config(upstream)).await;
            let mut stream = TcpStream::connect(logged.front).await.unwrap();
            stream
                .write_all(b"POST /up HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\ncontent-length: 1200\r\n\r\n")
                .await
                .unwrap();
            for _ in 0..3 {
                stream.write_all(&[b'x'; 400]).await.unwrap();
            }
            let mut answer = Vec::new();
            within(stream.read_to_end(&mut answer)).await.unwrap();
            assert_eq!(bodies.borrow()[0].len(), 1200);
            let records = logged.records(1).await;
            assert_eq!(records[0]["bytes_in"], 1200);
            assert_eq!(records[0]["bytes_out"], 2);
        })
        .await;
}

/// A request sent again says how many tries it took and where the last one went.
#[tokio::test]
async fn a_retried_request_says_its_tries() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = statuses_upstream(vec![503, 200]).await;
            let mut config = everything_config(upstream);
            config.routes[0].rules[0].forward.as_mut().unwrap().retry =
                Some(retrying(1, &[503], &[], 1));
            let logged = recording("retried", config).await;
            let answer = h1_answer(logged.front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
            let records = logged.records(1).await;
            assert_eq!(records[0]["tries"], 2);
            assert_eq!(records[0]["endpoint"], upstream.to_string());
            assert_eq!(records[0]["status"], 200);
        })
        .await;
}

/// A client that leaves before its answer has a record all the same: no status, and why.
#[tokio::test]
async fn a_client_that_leaves_before_its_answer_is_logged() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let logged = recording("left", everything_config(upstream)).await;
            let mut stream = TcpStream::connect(logged.front).await.unwrap();
            stream
                .write_all(b"GET /a/held HTTP/1.1\r\nhost: example.test\r\n\r\n")
                .await
                .unwrap();
            until(|| held.borrow().len() == 1).await;
            drop(stream);
            let records = logged.records(1).await;
            let record = &records[0];
            assert!(record["status"].is_null(), "{record}");
            assert_eq!(record["reason"], "client_closed");
            assert_eq!(record["tries"], 1);
            assert!(record.get("upstream_ms").is_none(), "{record}");
        })
        .await;
}

/// A client that leaves part way through its answer keeps the status it got, and the
/// bytes that went.
#[tokio::test]
async fn a_client_that_leaves_during_its_answer_is_logged() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let logged = recording("partway", everything_config(upstream)).await;
            let mut stream = TcpStream::connect(logged.front).await.unwrap();
            stream
                .write_all(b"GET /a/endless HTTP/1.1\r\nhost: example.test\r\n\r\n")
                .await
                .unwrap();
            let mut some = [0; 64 * 1024];
            let mut read = 0;
            while read < some.len() {
                read += within(stream.read(&mut some[read..])).await.unwrap();
            }
            drop(stream);
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["status"], 200);
            assert_eq!(record["reason"], "client_closed");
            assert!(record["bytes_out"].as_u64().unwrap() > 0, "{record}");
        })
        .await;
}

/// An answer whose body stops short of its length failed upstream, after its head went.
#[tokio::test]
async fn an_answer_cut_short_upstream_is_logged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let logged = recording("short", everything_config(upstream)).await;
            let request = b"GET /a/short HTTP/1.1\r\nhost: example.test\r\n\r\n";
            let _answer = h1_answer(logged.front, request).await;
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["status"], 200);
            assert_eq!(record["reason"], "upstream_failed");
            assert_eq!(record["bytes_out"], 5);
        })
        .await;
}

/// An answer whose body is found broken before any byte of its head has gone is answered
/// 502 in its place (14 §4): its record says the status the client got, and the scrape
/// counts the 502 among the responses and the gateway's own answers, not the 200 it
/// replaced (review A10-02, C28).
#[tokio::test]
async fn an_answer_replaced_before_its_head_went_is_logged_and_counted_as_the_502_sent() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // An upstream whose answer's body is broken from its first byte: a chunk size
            // that is not one, right behind its head; then held open, so that the body fails
            // on its framing and not on a close.
            let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = backend.local_addr().unwrap();
            let _answering = tokio::task::spawn_local(async move {
                let Ok((mut stream, _)) = backend.accept().await else {
                    return;
                };
                let mut seen = Vec::new();
                let mut byte = [0; 1];
                while !seen.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte).await {
                        Ok(1) => seen.push(byte[0]),
                        _ => return,
                    }
                }
                let _said = stream
                    .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nZ\r\n")
                    .await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            });
            let logged = recording("replaced", everything_config(upstream)).await;
            let answer = h1_answer(logged.front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502"), "{answer}");
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["status"], 502, "{record}");
            assert_eq!(record["reason"], "upstream_failed", "{record}");
            let scrape = logged.worker.proxy.metrics();
            let sample = |series: &str| {
                scrape
                    .lines()
                    .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
                    .unwrap_or("absent")
                    .to_owned()
            };
            let class = |class: &str| {
                sample(&format!(
                    "edgerush_listener_responses_total{{listener=\"web\",class=\"{class}\"}}"
                ))
            };
            assert_eq!((class("5xx"), class("2xx")), ("1".into(), "0".into()));
            assert_eq!(
                sample(
                    "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_failed\"}"
                ),
                "1"
            );
            assert_eq!(
                sample("edgerush_listener_time_to_response_head_seconds_count{listener=\"web\"}"),
                "1"
            );
            // The upstream did answer 200, and its own series say so.
            assert_eq!(
                sample("edgerush_upstream_responses_total{upstream=\"up\",class=\"2xx\"}"),
                "1",
                "{scrape}"
            );
        })
        .await;
}

/// Behind a trusted proxy the client is the one the proxy names, and the peer the proxy.
#[tokio::test]
async fn a_trusted_proxys_client_is_the_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let config = forwarding_config(upstream, &["127.0.0.0/8"]);
            let logged = recording("trusted", config).await;
            let request = b"GET /a/ok HTTP/1.1\r\nhost: example.test\r\nx-forwarded-for: 203.0.113.9\r\nconnection: close\r\n\r\n";
            let _answer = h1_answer(logged.front, request).await;
            let records = logged.records(1).await;
            assert_eq!(records[0]["client"], "203.0.113.9");
            assert!(records[0]["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
        })
        .await;
}

/// A worker serving HTTP/3 for `config`, whose listener `web` logs to a file of the test's
/// own, swept every 50 ms.
async fn recording_h3(test: &str, mut config: Config) -> Recording {
    let directory = scratch(test);
    let path = directory.join("access.log");
    config.listeners.get_mut("web").unwrap().access_log =
        Some(edgerush_config::AccessLog::File(path.clone()));
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let limits = H1Limits {
        sweep: Duration::from_millis(50),
        ..H1Limits::default()
    };
    let worker = Worker::with_deadlines(proxy, limits, SHORT);
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
    let alone = Forwarding::group(1).remove(0);
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve_h3(0, socket, alone).unwrap());
    Recording {
        front,
        worker,
        path,
        directory,
    }
}

/// Reads `answer` to its end, so that its stream is done with.
async fn drained(answer: ::h2::client::ResponseFuture) -> StatusCode {
    let answer = within(answer).await.unwrap();
    let status = answer.status();
    let mut body = answer.into_body();
    while let Some(chunk) = within(body.data()).await {
        let _ = body.flow_control().release_capacity(chunk.unwrap().len());
    }
    status
}

/// An HTTP/2 request is logged as an HTTP/1.1 one is, as HTTP/2.
#[tokio::test]
async fn an_http2_request_is_logged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let logged = recording("h2", everything_config(upstream)).await;
            let mut send = h2_library_client(logged.front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://example.test/a/ok").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            assert_eq!(drained(answer).await, StatusCode::OK);
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["protocol"], "2");
            assert_eq!(record["host"], "example.test");
            assert_eq!(record["path"], "/a/ok");
            assert_eq!(record["status"], 200);
            assert_eq!(record["bytes_out"], 2);
            assert_eq!(record["upstream"], "up");
            assert!(record["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
            assert!(record.get("reason").is_none(), "{record}");
        })
        .await;
}

/// A gRPC call's record has the status it ended with: from the upstream's trailers, or
/// one the gateway gave when the upstream's stream went before them.
#[tokio::test]
async fn a_grpc_calls_record_has_the_status_it_ended_with() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let script: Script = Rc::new(|request, mut respond| {
                Box::pin(async move {
                    let path = request.uri().path().to_owned();
                    let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                        return;
                    };
                    let _ = sending.send_data(Bytes::from_static(b"\0\0\0\0\x01m"), false);
                    if path == "/pkg.Svc/Cancelled" {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        sending.send_reset(::h2::Reason::CANCEL);
                    } else {
                        let mut status = http::HeaderMap::new();
                        status.insert("grpc-status", "0".parse().unwrap());
                        let _ = sending.send_trailers(status);
                    }
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let mut config = everything_config(upstream);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            let logged = recording("grpc", config).await;
            let mut send = h2_library_client(logged.front, &::h2::client::Builder::new()).await;
            for (path, code) in [("/pkg.Svc/Ok", "0"), ("/pkg.Svc/Cancelled", "1")] {
                let (answer, _) = send.send_request(grpc_call(path, None), true).unwrap();
                assert_eq!(grpc_outcome(answer).await.1, code, "{path}");
            }
            let records = logged.records(2).await;
            for (path, code) in [("/pkg.Svc/Ok", 0), ("/pkg.Svc/Cancelled", 1)] {
                let record = records
                    .iter()
                    .find(|record| record["path"] == path)
                    .unwrap();
                assert_eq!(record["grpc_status"], code, "{record}");
                assert_eq!(record["status"], 200, "{record}");
            }
        })
        .await;
}

/// A gRPC call the gateway answers itself has the status it was told, and why.
#[tokio::test]
async fn a_grpc_call_the_gateway_answers_has_its_status() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = everything_config("127.0.0.1:9".parse().unwrap());
            config.upstreams.get_mut("up").unwrap().endpoints.clear();
            let logged = recording("grpc-own", config).await;
            let mut send = h2_library_client(logged.front, &::h2::client::Builder::new()).await;
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Any", None), true)
                .unwrap();
            assert_eq!(grpc_outcome(answer).await.1, "14");
            let records = logged.records(1).await;
            assert_eq!(records[0]["grpc_status"], 14);
            assert_eq!(records[0]["reason"], "no_endpoints");
        })
        .await;
}

/// An HTTP/2 client that resets its stream before the answer is logged as one that left.
#[tokio::test]
async fn an_http2_stream_reset_before_its_answer_is_logged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let logged = recording("h2-reset", everything_config(upstream)).await;
            let mut send = h2_library_client(logged.front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://example.test/a/held").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            until(|| held.borrow().len() == 1).await;
            // Its last handle gone, h2 resets the stream.
            drop(answer);
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["protocol"], "2");
            assert!(record["status"].is_null(), "{record}");
            assert_eq!(record["reason"], "client_closed");
        })
        .await;
}

/// An HTTP/3 request is logged as the others are, as HTTP/3, from the address and port
/// its datagrams came from.
#[tokio::test]
async fn an_http3_request_is_logged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (upstream, _) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let logged = recording_h3("h3", h3_config(upstream, http3)).await;
            let mut client = Client::connect(logged.front, "a.test").await;
            let answer = client.get("a.test", "/x").await;
            assert_eq!(answer.final_status(), Some("200"));
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["protocol"], "3");
            assert_eq!(record["host"], "a.test");
            assert_eq!(record["path"], "/x");
            assert_eq!(record["status"], 200);
            assert_eq!(record["bytes_out"], 2);
            assert!(record["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
        })
        .await;
}

/// A head the HTTP/1 server refuses before the core has a request has a record of what is
/// known: who sent it, how it was answered and why. Nothing of what it asked: nothing was
/// read of it that could be believed. A connection that sent nothing has no record.
#[tokio::test]
async fn a_refused_http1_head_is_logged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let logged = recording("refused", everything_config(upstream)).await;
            // First, so that a record of it would be the first read below.
            drop(TcpStream::connect(logged.front).await.unwrap());
            let long = format!(
                "GET /{} HTTP/1.1\r\nhost: example.test\r\n\r\n",
                "a".repeat(20_000)
            );
            let refusals: [(&[u8], u16, &str); 2] = [
                (
                    b"GET /a HTTP/1.1\r\nhost: example.test\r\ncontent-length: 1\r\ncontent-length: 1\r\n\r\n",
                    400,
                    "repeated_length",
                ),
                (long.as_bytes(), 414, "line_too_long"),
            ];
            for (request, status, _) in refusals {
                let answer = h1_answer(logged.front, request).await;
                assert!(answer.starts_with(&format!("HTTP/1.1 {status}")), "{answer}");
            }
            let records = logged.records(2).await;
            for (record, (_, status, why)) in records.iter().zip(refusals) {
                assert_eq!(record["status"], status, "{record}");
                assert_eq!(record["reason"], why, "{record}");
                assert_eq!(record["listener"], "web");
                assert_eq!(record["client"], "127.0.0.1");
                assert!(record["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
                for unknown in ["method", "host", "path", "protocol", "route", "duration_ms"] {
                    assert!(record.get(unknown).is_none(), "{unknown}: {record}");
                }
            }
        })
        .await;
}

/// A head our HTTP/1 server refuses is one of the gateway's own answers, and the scrape
/// counts it among the responses by class and among those answers by why (review A10-07,
/// C29): a refusal of the `Connection` field under the core's reason of the same name.
#[tokio::test]
async fn a_refused_http1_head_is_counted_by_class_and_why() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let logged = recording("refused-counted", everything_config(upstream)).await;
            let refusals: [&[u8]; 3] = [
                b"GET /a HTTP/1.1\r\nhost: example.test\r\ncontent-length: 1\r\ncontent-length: 1\r\n\r\n",
                b"GET /a HTTP/1.1\r\nhost: example.test\r\ncontent-length: 1\r\ncontent-length: 1\r\n\r\n",
                b"GET /a HTTP/1.1\r\nhost: example.test\r\nconnection: a b\r\n\r\n",
            ];
            for request in refusals {
                let answer = h1_answer(logged.front, request).await;
                assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
            }
            // Their records written: the server is done with them.
            let _records = logged.records(3).await;
            let scrape = logged.worker.proxy.metrics();
            let sample = |series: &str| {
                scrape
                    .lines()
                    .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
                    .unwrap_or("absent")
                    .to_owned()
            };
            let answers = |why: &str| {
                sample(&format!(
                    "edgerush_listener_local_answers_total{{listener=\"web\",reason=\"{why}\"}}"
                ))
            };
            assert_eq!(
                sample("edgerush_listener_responses_total{listener=\"web\",class=\"4xx\"}"),
                "3"
            );
            assert_eq!(answers("repeated_length"), "2");
            assert_eq!(answers("bad_connection"), "1");
            assert_eq!(answers("line_too_long"), "0");
            // No request: nothing is timed.
            assert_eq!(
                sample("edgerush_listener_time_to_response_head_seconds_count{listener=\"web\"}"),
                "0"
            );
        })
        .await;
}

/// A head over HTTP/3 too large to take, which its server answers 431 itself, has a record
/// as one over HTTP/1 does.
#[tokio::test]
async fn an_http3_head_too_large_is_logged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (upstream, _) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let logged = recording_h3("h3-refused", h3_config(upstream, http3)).await;
            let mut client = Client::connect(logged.front, "a.test").await;
            let large = "v".repeat(100 << 10);
            let mut head = get("a.test", "/big");
            head.push(("x-large", &large));
            let id = client.request(&head, true);
            assert_eq!(client.answer(id).await.final_status(), Some("431"));
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["status"], 431);
            assert_eq!(record["reason"], "head_too_long");
            assert_eq!(record["protocol"], "3");
            assert!(record["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
            assert!(record.get("path").is_none(), "{record}");
            // And counted as one over HTTP/1 is (C29).
            let scrape = logged.worker.proxy.metrics();
            for line in [
                "edgerush_listener_responses_total{listener=\"web\",class=\"4xx\"} 1",
                "edgerush_listener_local_answers_total{listener=\"web\",reason=\"head_too_long\"} 1",
            ] {
                assert!(scrape.lines().any(|shown| shown == line), "{line}");
            }
        })
        .await;
}

/// A worker serving the config `yaml` makes of where to log, its one listener's settings
/// to add, swept as [`recording`]'s is: for a `tcp` or `tls` listener's.
async fn recording_yaml(test: &str, yaml: impl FnOnce(&str) -> String) -> Recording {
    let directory = scratch(test);
    let path = directory.join("access.log");
    let logs = format!(", access_log: {{ file: '{}' }}", path.display());
    let config: Config = serde_saphyr::from_str(&yaml(&logs)).unwrap();
    let (front, worker) = serving_swept(compile(&config).unwrap()).await;
    Recording {
        front,
        worker,
        path,
        directory,
    }
}

/// A plaintext WebSocket backend: a 101 with the Accept of the key it was sent, then, for
/// each eight-byte frame that comes, a text frame `hello` for a text frame and a Close of
/// its own for a Close; once its client has finished, it finishes too.
async fn talking_websocket_backend() -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let _serving = tokio::task::spawn_local(async move {
                let mut head = Vec::new();
                let mut byte = [0; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => head.push(byte[0]),
                    }
                }
                let head = String::from_utf8(head).unwrap();
                let key = head
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                    .and_then(|key| Key::read(key.as_bytes()))
                    .unwrap();
                let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                let switched = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
                );
                stream.write_all(switched.as_bytes()).await.unwrap();
                let mut frame = [0; 8];
                while stream.read_exact(&mut frame).await.is_ok() {
                    let answer: &[u8] = match frame[0] {
                        0x81 => b"\x81\x05hello",
                        // 1000, "Normal Closure", as a server answers a Close.
                        0x88 => &[0x88, 0x02, 0x03, 0xe8],
                        _ => &[],
                    };
                    let _ = stream.write_all(answer).await;
                }
                let _ = stream.shutdown().await;
            });
        }
    });
    address
}

/// A client's text frame `hi`, masked as a client's must be, with a mask of zeros.
const HI: [u8; 8] = [0x81, 0x82, 0, 0, 0, 0, b'h', b'i'];

/// A WebSocket's handshake to `front` for `/chat`, read up to the end of its 101's head.
async fn websocket_to(front: SocketAddr) -> TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut client = TcpStream::connect(front).await.unwrap();
    client
        .write_all(
            b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
                  connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  sec-websocket-version: 13\r\n\r\n",
        )
        .await
        .unwrap();
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        within(client.read_exact(&mut byte)).await.unwrap();
        head.push(byte[0]);
    }
    assert!(head.starts_with(b"HTTP/1.1 101 "));
    client
}

/// A WebSocket has two records (08 §2): the handshake's, once its 101 has gone and while
/// the tunnel is still open, and the tunnel's, when it ends, with how it ended, what it
/// carried each way and how long it lasted.
#[tokio::test]
async fn a_websocket_is_logged_when_it_opens_and_when_it_closes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = talking_websocket_backend().await;
            let logged = recording("websocket", everything_config(upstream)).await;
            let mut client = websocket_to(logged.front).await;
            client.write_all(&HI).await.unwrap();
            let mut hello = [0; 7];
            within(client.read_exact(&mut hello)).await.unwrap();
            assert_eq!(&hello, b"\x81\x05hello");

            let opened = &logged.records(1).await[0];
            assert_eq!(opened["kind"], "websocket_open", "{opened}");
            assert_eq!(opened["status"], 101);
            assert_eq!(opened["method"], "GET");
            assert_eq!(opened["path"], "/chat");
            assert_eq!(opened["route"], "everything");
            assert_eq!(opened["upstream"], "up");
            assert_eq!(opened["endpoint"], upstream.to_string());
            assert_eq!(opened["bytes_in"], 0);
            assert_eq!(opened["bytes_out"], 0);
            assert!(opened.get("reason").is_none(), "{opened}");

            client.shutdown().await.unwrap();
            let mut rest = Vec::new();
            within(client.read_to_end(&mut rest)).await.unwrap();
            let records = logged.records(2).await;
            let closed = &records[1];
            assert_eq!(closed["kind"], "websocket_close", "{closed}");
            assert_eq!(closed["status"], 101);
            assert_eq!(closed["reason"], "closed");
            assert_eq!(closed["bytes_in"], HI.len());
            assert_eq!(closed["bytes_out"], hello.len());
            assert_eq!(closed["path"], "/chat");
            assert_eq!(closed["endpoint"], upstream.to_string());
            assert!(closed["duration_ms"].as_f64().is_some(), "{closed}");
            assert_eq!(records.len(), 2, "{records:?}");
        })
        .await;
}

/// A WebSocket a draining worker closes says so, and counts only what it carried: not the
/// Close frames the gateway sent each way, nor the answers to them it took (19 §6).
#[tokio::test]
async fn a_drained_websocket_counts_only_what_it_carried() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = talking_websocket_backend().await;
            let logged = recording("drained-websocket", everything_config(upstream)).await;
            let mut client = websocket_to(logged.front).await;
            client.write_all(&HI).await.unwrap();
            let mut hello = [0; 7];
            within(client.read_exact(&mut hello)).await.unwrap();

            logged.worker.drain();
            let mut close = [0; 4];
            within(client.read_exact(&mut close)).await.unwrap();
            assert_eq!(close, [0x88, 0x02, 0x03, 0xe9], "not a Close 1001");
            client
                .write_all(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe9])
                .await
                .unwrap();
            let mut rest = Vec::new();
            let _closed = within(client.read_to_end(&mut rest)).await;
            let records = logged.records(2).await;
            let closed = &records[1];
            assert_eq!(closed["kind"], "websocket_close", "{closed}");
            assert_eq!(closed["reason"], "drained");
            assert_eq!(closed["bytes_in"], HI.len());
            assert_eq!(closed["bytes_out"], hello.len());
        })
        .await;
}

/// The same over HTTP/2, as an extended CONNECT: its handshake's record, once its 200 has
/// gone, and its tunnel's when the client ends its stream.
#[tokio::test]
async fn an_http2_websocket_is_logged_when_it_opens_and_when_it_closes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = talking_websocket_backend().await;
            let logged = recording("h2-websocket", everything_config(upstream)).await;
            let stream = TcpStream::connect(logged.front).await.unwrap();
            let (send, connection) = ::h2::client::handshake(stream).await.unwrap();
            let _driving = tokio::task::spawn_local(async move {
                let _ended = connection.await;
            });
            let mut send = within(send.ready()).await.unwrap();
            until(|| send.is_extended_connect_protocol_enabled()).await;
            let mut request = Request::builder()
                .method(Method::CONNECT)
                .uri("http://a.test/chat")
                .header("sec-websocket-version", "13")
                .body(())
                .unwrap();
            request
                .extensions_mut()
                .insert(::h2::ext::Protocol::from_static("websocket"));
            let (response, mut stream) = send.send_request(request, false).unwrap();
            let response = within(response).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let mut body = response.into_body();
            stream.send_data(Bytes::from_static(&HI), false).unwrap();
            let mut heard = Vec::new();
            while heard.len() < 7 {
                let data = within(body.data()).await.unwrap().unwrap();
                let _released = body.flow_control().release_capacity(data.len());
                heard.extend_from_slice(&data);
            }
            assert_eq!(heard, b"\x81\x05hello");

            let opened = &logged.records(1).await[0];
            assert_eq!(opened["kind"], "websocket_open", "{opened}");
            assert_eq!(opened["protocol"], "2");
            assert_eq!(opened["status"], 200);
            assert_eq!(opened["method"], "CONNECT");

            stream.send_data(Bytes::new(), true).unwrap();
            while let Some(data) = within(body.data()).await {
                if data.is_err() {
                    break;
                }
            }
            let records = logged.records(2).await;
            let closed = &records[1];
            assert_eq!(closed["kind"], "websocket_close", "{closed}");
            assert_eq!(closed["reason"], "closed");
            assert_eq!(closed["bytes_in"], HI.len());
            assert_eq!(closed["bytes_out"], heard.len());
        })
        .await;
}

/// A `tcp` listener's connection has one record, at its end: who it came from, where it
/// went, how it ended, and what it carried each way. No HTTP status: it had none.
#[tokio::test]
async fn a_tcp_connection_is_logged_at_its_end() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = tallying_backend().await;
            let logged = recording_yaml("tcp", |logs| tcp_to(backend, logs)).await;
            let mut client = TcpStream::connect(logged.front).await.unwrap();
            let sent = vec![7_u8; 100_000];
            client.write_all(&sent).await.unwrap();
            client.shutdown().await.unwrap();
            let mut answer = String::new();
            within(client.read_to_string(&mut answer)).await.unwrap();
            assert_eq!(answer, format!("{} {}", sent.len(), 7 * sent.len()));
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["kind"], "connection");
            assert_eq!(record["listener"], "db");
            assert_eq!(record["client"], "127.0.0.1");
            assert_eq!(
                record["peer"],
                client.local_addr().unwrap().to_string(),
                "{record}"
            );
            assert_eq!(record["reason"], "closed");
            assert_eq!(record["route"], "db");
            assert_eq!(record["upstream"], "up");
            assert_eq!(record["endpoint"], backend.to_string());
            assert_eq!(record["bytes_in"], sent.len());
            assert_eq!(record["bytes_out"], answer.len());
            assert!(record["duration_ms"].as_f64().is_some(), "{record}");
            for none in ["status", "protocol", "method", "host", "path", "tries"] {
                assert!(record.get(none).is_none(), "{none}: {record}");
            }
        })
        .await;
}

/// The PROXY header a backend is told of the client is the gateway's, not the client's,
/// and is not counted among what the client sent (20 §4).
#[tokio::test]
async fn a_proxy_header_to_the_backend_is_not_counted_as_the_clients() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = tallying_backend().await;
            let logged =
                recording_yaml("tcp-told", |logs| sending(&tcp_to(backend, logs), "v1")).await;
            let mut client = TcpStream::connect(logged.front).await.unwrap();
            client.write_all(b"ping").await.unwrap();
            client.shutdown().await.unwrap();
            let mut answer = String::new();
            within(client.read_to_string(&mut answer)).await.unwrap();
            // The backend counted the header as well.
            let came: usize = answer.split(' ').next().unwrap().parse().unwrap();
            assert!(came > 4, "{answer}");
            let records = logged.records(1).await;
            assert_eq!(records[0]["bytes_in"], 4, "{}", records[0]);
        })
        .await;
}

/// A `tls` listener's connection is logged with the name its ClientHello asked for as its
/// host; one that no route has a name for is logged too, refused, with nowhere it went.
#[tokio::test]
async fn a_tls_connection_is_logged_with_the_name_it_asked_for() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let exact = tls_backend("exact").await;
            let logged = recording_yaml("tls", |logs| {
                format!(
                    "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls, proxy_protocol: off{logs} }} }}\n\
                         routes: []\n\
                         tls_routes:\n\
                         \x20 - {{ name: exact, listeners: [sni], hostnames: [{{ name: api.example.test, falls_through: true }}], backends: [{{ upstream: exact, weight: 1 }}] }}\n\
                         upstreams: {{ exact: {{ load_balancer: p2c, endpoints: [\"{exact}\"] }} }}\n"
                )
            })
            .await;
            assert_eq!(
                told_over_tls(logged.front, Some("api.example.test"))
                    .await
                    .as_deref(),
                Some("exact")
            );
            let carried = &logged.records(1).await[0];
            assert_eq!(carried["kind"], "connection");
            assert_eq!(carried["host"], "api.example.test", "{carried}");
            assert_eq!(carried["reason"], "closed");
            assert_eq!(carried["route"], "exact");
            assert_eq!(carried["upstream"], "exact");
            assert_eq!(carried["endpoint"], exact.to_string());
            assert!(carried["bytes_in"].as_u64().unwrap() > 0, "{carried}");
            assert!(carried["bytes_out"].as_u64().unwrap() > 0, "{carried}");

            assert_eq!(told_over_tls(logged.front, Some("elsewhere.test")).await, None);
            let refused = &logged.records(2).await[1];
            assert_eq!(refused["host"], "elsewhere.test", "{refused}");
            assert_eq!(refused["reason"], "refused");
            assert_eq!(refused["bytes_in"], 0);
            assert_eq!(refused["bytes_out"], 0);
            for none in ["route", "upstream", "endpoint"] {
                assert!(refused.get(none).is_none(), "{none}: {refused}");
            }
        })
        .await;
}

/// An answer the worker's own storage cut short after its head — its HTTP/2 upstream
/// connection closed to make room — is logged as that, `exhausted`, not as the upstream
/// failing (14 §8). Set up as the metrics' test of it is: one client reads its answer's
/// head and stops, another reads nothing, and the one connection the worker may open goes.
#[tokio::test]
async fn an_answer_cut_by_the_workers_shortage_is_logged_as_that() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = scripted_h2_upstream(answering_with(4 << 20)).await;
            let directory = scratch("shed");
            let path = directory.join("access.log");
            let mut config = everything_config(upstream);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            config.listeners.get_mut("web").unwrap().access_log =
                Some(edgerush_config::AccessLog::File(path.clone()));
            let limits = H1Limits {
                storage: 3 << 19,
                h2_connections: 1,
                sweep: Duration::from_millis(50),
                ..H1Limits::default()
            };
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
            let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            let logged = Recording {
                front,
                worker,
                path,
                directory,
            };

            let mut reading = h2_library_client(logged.front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://example.test/read").body(()).unwrap();
            let (answer, _) = reading.send_request(request, true).unwrap();
            let mut body = within(answer).await.unwrap().into_body();
            let mut granting_nothing = ::h2::client::Builder::new();
            granting_nothing.initial_window_size(0);
            let mut stalled = h2_library_client(logged.front, &granting_nothing).await;
            let request = Request::get("http://example.test/stalled")
                .body(())
                .unwrap();
            let (_stalled, _) = stalled.send_request(request, true).unwrap();
            until(|| logged.worker.h2_connections() == 0).await;
            while let Some(Ok(data)) = within(body.data()).await {
                let _released = body.flow_control().release_capacity(data.len());
            }
            let records = logged.records(1).await;
            let record = &records[0];
            assert_eq!(record["path"], "/read", "{record}");
            assert_eq!(record["status"], 200);
            assert_eq!(record["reason"], "exhausted", "{record}");
        })
        .await;
}
