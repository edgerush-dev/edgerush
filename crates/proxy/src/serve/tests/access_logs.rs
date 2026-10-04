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
    _worker: Rc<Worker>,
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
        _worker: worker,
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
        _worker: worker,
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
