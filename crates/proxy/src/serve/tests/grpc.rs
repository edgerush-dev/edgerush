//! gRPC calls.

use super::*;

/// A gRPC call the gateway answers itself is answered as gRPC answers a call it fails
/// before any message: `200`, and the status the cause calls for, in the head. The same
/// request that is not a gRPC call is answered in HTTP.
#[tokio::test]
async fn a_grpc_call_the_gateway_answers_itself_is_told_a_grpc_status() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = everything_config("127.0.0.1:9".parse().unwrap());
            let up = config.upstreams.get_mut("up").unwrap();
            up.endpoints.clear();
            up.protocol = UpstreamProtocol::Http2;
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Do", None), true)
                .unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert_eq!(answer.headers()["content-type"], "application/grpc");
            assert_eq!(answer.headers()["grpc-status"], "14");
            assert!(answer.headers().contains_key("grpc-message"));
            assert!(answer.body().is_end_stream());

            let plain = Request::post("http://a.test/pkg.Svc/Do").body(()).unwrap();
            let (answer, _) = send.send_request(plain, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(answer.headers().get("grpc-status").is_none());
            // The call is counted by its status; the request that was not a call is not.
            let scrape = worker.proxy().metrics();
            let line =
                "edgerush_listener_grpc_calls_total{listener=\"web\",status=\"UNAVAILABLE\"} 1
";
            assert!(scrape.contains(line), "{scrape}");

            // Connection-bound credentials, which are not sent over HTTP/2.
            let (upstream, _seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut call = grpc_call("/pkg.Svc/Do", None);
            call.headers_mut()
                .insert("authorization", "Negotiate abc".parse().unwrap());
            let (answer, _) = send.send_request(call, true).unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "12".to_owned(), true)
            );
        })
        .await;
}

/// A gRPC call's deadline bounds how long its answer is waited for, and goes up as the
/// time it has left; one already past is answered at once and never sent.
#[tokio::test]
async fn a_grpc_calls_deadline_bounds_it_and_goes_up_as_the_time_left() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let heard = Rc::new(RefCell::new(Vec::<String>::new()));
            let hearing = Rc::clone(&heard);
            let script: Script = Rc::new(move |request, _respond| {
                let hearing = Rc::clone(&hearing);
                Box::pin(async move {
                    let timeout = request
                        .headers()
                        .get("grpc-timeout")
                        .map(|value| value.to_str().unwrap().to_owned())
                        .unwrap_or_default();
                    hearing.borrow_mut().push(timeout);
                    // Never answers; keeps the stream open until it is reset.
                    let _respond = _respond;
                    std::future::pending::<()>().await;
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;

            let asked = tokio::time::Instant::now();
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Slow", Some("300m")), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "4".to_owned(), true)
            );
            let took = asked.elapsed();
            assert!(
                took + EARLY >= Duration::from_millis(300)
                    && took < Duration::from_millis(300) + SLACK,
                "answered after {took:?}"
            );
            let sent = crate::grpc::timeout::parse(heard.borrow()[0].as_bytes())
                .expect("no grpc-timeout went up");
            // What was left when the stream opened, not what the client said.
            assert!(
                sent < Duration::from_millis(300) && sent > Duration::from_millis(200),
                "sent {sent:?}"
            );

            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Late", Some("0n")), true)
                .unwrap();
            assert_eq!(
                grpc_outcome(answer).await,
                (StatusCode::OK, "4".to_owned(), true)
            );
            assert_eq!(heard.borrow().len(), 1, "a call past its deadline went up");
        })
        .await;
}

/// Once a gRPC answer has begun, whatever ends it early is told to the client as a
/// status in its trailers: the upstream cutting it off, by what gRPC makes of the
/// reason, or its deadline passing. The upstream's own status goes through once.
#[tokio::test]
async fn a_grpc_answer_always_ends_with_exactly_one_status() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let script: Script = Rc::new(|request, mut respond| {
                Box::pin(async move {
                    let path = request.uri().path().to_owned();
                    if path == "/pkg.Svc/Plain" {
                        let unavailable = Response::builder()
                            .status(503)
                            .header("content-type", "text/plain")
                            .body(())
                            .unwrap();
                        let _ = respond.send_response(unavailable, true);
                        return;
                    }
                    let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                        return;
                    };
                    let _ = sending.send_data(Bytes::from_static(b"\0\0\0\0\x01m"), false);
                    match path.as_str() {
                        "/pkg.Svc/Cancelled" => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            sending.send_reset(::h2::Reason::CANCEL);
                        }
                        "/pkg.Svc/Hangs" => std::future::pending::<()>().await,
                        _ => {
                            let mut status = http::HeaderMap::new();
                            status.insert("grpc-status", "0".parse().unwrap());
                            let _ = sending.send_trailers(status);
                        }
                    }
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            for (path, timeout, expected) in [
                ("/pkg.Svc/Ok", None, (StatusCode::OK, "0", false)),
                ("/pkg.Svc/Cancelled", None, (StatusCode::OK, "1", false)),
                ("/pkg.Svc/Hangs", Some("300m"), (StatusCode::OK, "4", false)),
            ] {
                let (answer, _) = send.send_request(grpc_call(path, timeout), true).unwrap();
                let (status, code, in_head) = grpc_outcome(answer).await;
                assert_eq!((status, code.as_str(), in_head), expected, "{path}");
            }
            // An answer that is not gRPC's goes on as it came, for the client to read.
            let (answer, _) = send
                .send_request(grpc_call("/pkg.Svc/Plain", None), true)
                .unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(answer.headers().get("grpc-status").is_none());
            let mut body = answer.into_body();
            while let Some(chunk) = within(body.data()).await {
                let _ = body.flow_control().release_capacity(chunk.unwrap().len());
            }
            let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx))).await;
            assert!(trailers.unwrap().is_none(), "a status was added to it");

            // Each call counted once, by how it ended; the one that was not answered as
            // gRPC is not counted as a call's status at all.
            let scrape = worker.proxy().metrics();
            for (status, count) in [
                ("OK", 1),
                ("CANCELLED", 1),
                ("DEADLINE_EXCEEDED", 1),
                ("UNKNOWN", 0),
                ("INTERNAL", 0),
            ] {
                let line = format!(
                    "edgerush_listener_grpc_calls_total{{listener=\"web\",status=\"{status}\"}} {count}
"
                );
                assert!(scrape.contains(&line), "{line}{scrape}");
            }
        })
        .await;
}
