//! What a worker holds and counts: places, pools, storage, and the size of a connection.

use super::*;

/// What a future `make` returns takes, without one being made.
fn size_of_made<A, F>(_make: impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

/// A connection's task is as large as its future, from accept to close, whatever the
/// connection turns out to be (14 §3). HTTP/2 and TLS hold far more than plain HTTP/1
/// does waiting between requests, and are boxed once a connection is found to be one of
/// them: the future is no larger than plain HTTP/1 needs, and what serving any
/// connection holds besides — its first request's deadline, the detector, its
/// deadlines and handles on it — which is well under what either would add.
#[test]
fn a_connection_is_no_larger_than_plain_http1_needs() {
    use crate::downstream::detect::Replay;
    type Plain = (Rc<Connection>, Rc<Client>, Rc<Cell<bool>>, Replay<Lent>);
    type Secured = (
        Rc<Connection>,
        Rc<Client>,
        Rc<Cell<bool>>,
        Deadlines,
        &'static Tls,
        Lent,
    );
    type Http2 = (
        Rc<Connection>,
        Rc<Client>,
        Rc<Cell<bool>>,
        Deadlines,
        Replay<Lent>,
    );
    let connection = size_of_made(|(worker, stream): (Rc<Worker>, TcpStream)| {
        worker.serve_connection(0, stream)
    });
    let plain = size_of_made(|(ours, client, asking, socket): Plain| {
        serve_h1(ours, client, asking, socket)
    });
    let tls = size_of_made(|(ours, client, asking, deadlines, tls, socket): Secured| {
        serve_tls(ours, client, asking, deadlines, tls, socket)
    });
    let h2 = size_of_made(|(ours, client, asking, deadlines, socket): Http2| {
        serve_h2(ours, client, asking, deadlines, socket)
    });
    assert!(
        connection <= plain + 768,
        "{connection} bytes; plain HTTP/1 {plain}, TLS {tls}, HTTP/2 {h2}"
    );
}

/// The worker's sweep lets go of the blocks a burst left parked, so that a worker that
/// has gone quiet holds only what a quiet worker keeps ([13 §7](../../docs/13-http1-upstream.md)).
#[tokio::test(start_paused = true)]
async fn a_sweep_lets_go_of_what_a_burst_left_parked() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let limits = H1Limits::default();
            let worker = Worker::with_limits(sending_to("127.0.0.1:9".parse().unwrap()), limits);
            {
                let mut blocks = worker.blocks.borrow_mut();
                let burst: Vec<_> = (0..20).map(|_| blocks.take().unwrap()).collect();
                for block in burst {
                    blocks.give(block);
                }
                assert_eq!(blocks.parked(), 20);
            }
            let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            tokio::time::sleep(limits.sweep + Duration::from_millis(1)).await;
            let blocks = worker.blocks.borrow();
            assert_eq!(
                blocks.parked(),
                blocks.sizes().kept,
                "the burst is still parked"
            );
        })
        .await;
}

/// A worker takes no more than its batch of connections before whatever else it has
/// ready runs: a backlog full of new connections does not go ahead of the rest.
#[tokio::test]
async fn a_worker_accepts_a_batch_then_lets_the_rest_run() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for (batch, other_ran) in [(1, true), (2, false)] {
                let proxy = served("127.0.0.1:9".parse().unwrap());
                let limits = H1Limits {
                    accept_batch: batch,
                    ..H1Limits::default()
                };
                let worker = Worker::with_deadlines(proxy, limits, SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let at = socket.local_addr().unwrap();
                let _first = TcpStream::connect(at).await.unwrap();
                let _second = TcpStream::connect(at).await.unwrap();
                // A connect can return before the listener has the connection in its
                // queue: on Linux the handshake's last step reaches the listening side
                // just after. Nothing can be asked about the queue without taking from
                // it, so the kernel is given the moment it needs.
                tokio::time::sleep(Duration::from_millis(50)).await;

                within(worker.accept(&socket)).await.unwrap().unwrap();
                let ran = Rc::new(Cell::new(false));
                let running = Rc::clone(&ran);
                let _other = tokio::task::spawn_local(async move { running.set(true) });
                within(worker.accept(&socket)).await.unwrap().unwrap();
                assert_eq!(ran.get(), other_ran, "batch of {batch}");
            }
        })
        .await;
}

/// A worker takes on only so many exchanges at once, whichever client carries them,
/// and answers the rest rather than opening another connection for them. The place is
/// given back by every way out of an exchange, a failed one included, so a worker that
/// has been full is not full for ever ([13 §7](../../docs/13-http1-upstream.md)).
#[test]
fn a_worker_full_of_exchanges_answers_rather_than_take_another() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let limits = H1Limits {
            exchanges: 2,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(served(upstream), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        // Two that will not come back, waited for where the worker has committed
        // to them rather than where they were sent.
        for _ in 0..2 {
            let _parked = tokio::task::spawn_local(async move {
                let _never = status_of(front, "/silent").await;
            });
        }
        until(|| held.borrow().len() == 2).await;

        // Bounded, because a worker that does not refuse it holds it for as long
        // as the upstream says nothing, which is for ever.
        let refused = tokio::time::timeout(Duration::from_secs(5), status_of(front, "/ok"))
            .await
            .unwrap_or_else(|_| panic!("took on a third exchange"));
        assert_eq!(refused, StatusCode::SERVICE_UNAVAILABLE);

        // The upstream lets both go without answering, so both exchanges fail; a
        // failure gives its place back like any other ending, and the worker
        // takes requests again.
        held.borrow_mut().clear();
        until(|| worker.places.held() == 0).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
    }));
}

/// An upstream that stops answering does not take every place a worker has: once the
/// worker is short of them, the upstream that holds more than its share is refused and
/// the healthy one beside it is still served, though it held no place when the slow one
/// filled up. Its places come back as its exchanges end, and it is served again
/// ([03 §9](../../docs/03-data-plane.md)).
#[test]
fn an_upstream_that_stops_answering_leaves_the_last_places_to_the_others() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (slow, held) = scripted_upstream().await;
        let (healthy, _) = scripted_upstream().await;
        let config = compile(&slow_and_healthy_config(slow, healthy)).unwrap();
        let proxy = Arc::new(Proxy::new(config, NonZeroUsize::MIN).unwrap());
        // Short of places from 14 of 16.
        let limits = H1Limits {
            exchanges: 16,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(proxy, limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        for _ in 0..14 {
            let _parked = tokio::task::spawn_local(async move {
                let _never = status_of(front, "/slow/silent").await;
            });
        }
        until(|| held.borrow().len() == 14).await;
        assert_eq!(worker.places.held(), 14);

        // Bounded: a worker that took it would hold it for as long as the upstream says
        // nothing, which is for ever.
        let refused =
            tokio::time::timeout(Duration::from_secs(5), status_of(front, "/slow/silent"))
                .await
                .unwrap_or_else(|_| panic!("took a fifteenth place for the slow upstream"));
        assert_eq!(refused, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        let scrape = worker.proxy().metrics();
        assert!(scrape.contains("reason=\"over_share\"} 1"), "{scrape}");
        assert!(scrape.contains("reason=\"too_busy\"} 0"), "{scrape}");

        held.borrow_mut().clear();
        until(|| worker.places.held() == 0).await;
        assert_eq!(status_of(front, "/slow/ok").await, StatusCode::OK);
    }));
}

/// The same past the metrics' last slot: upstreams whose names come after the first 4,095
/// a data plane has seen share one series, and nothing more. Each keeps its own share of the
/// places, so the healthy one is still served beside the slow one.
#[test]
fn upstreams_past_the_last_metrics_slot_keep_their_own_share_of_places() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (slow, held) = scripted_upstream().await;
        let (healthy, _) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(everything_to(healthy), NonZeroUsize::MIN).unwrap());
        // Every slot taken by names that came and went.
        for name in 0..crate::metrics::UPSTREAM_SLOTS {
            let _slot = proxy.metrics.upstream_slot(&format!("gone-{name}"));
        }
        let config = compile(&slow_and_healthy_config(slow, healthy)).unwrap();
        proxy.reload(config).unwrap();
        let limits = H1Limits {
            exchanges: 16,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(proxy, limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        for _ in 0..14 {
            let _parked = tokio::task::spawn_local(async move {
                let _never = status_of(front, "/slow/silent").await;
            });
        }
        until(|| held.borrow().len() == 14).await;
        let refused =
            tokio::time::timeout(Duration::from_secs(5), status_of(front, "/slow/silent"))
                .await
                .unwrap_or_else(|_| panic!("took a fifteenth place for the slow upstream"));
        assert_eq!(refused, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            status_of(front, "/ok").await,
            StatusCode::OK,
            "the healthy upstream refused for the slow one's places"
        );
        held.borrow_mut().clear();
        until(|| worker.places.held() == 0).await;
    }));
}

/// Requests under `/slow` to `slow`, and the rest to `healthy`.
fn slow_and_healthy_config(slow: SocketAddr, healthy: SocketAddr) -> Config {
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
          - path: {{ prefix: /slow }}
        forward: {{ backends: [{{ upstream: slow, weight: 1 }}] }}
      - matches:
          - path: {{ prefix: / }}
        forward: {{ backends: [{{ upstream: healthy, weight: 1 }}] }}
upstreams:
  slow: {{ load_balancer: p2c, endpoints: ["{slow}"] }}
  healthy: {{ load_balancer: p2c, endpoints: ["{healthy}"] }}
"#
    );
    serde_saphyr::from_str(&yaml).unwrap()
}

/// Every way out of an exchange gives its place back, whichever client carried it: an
/// upstream that could not be reached, an answer with no body, one read to its end, one
/// the client stopped reading, one that failed part way, and a client that went before
/// anything came back. A place that one of them kept would be kept for ever, and a
/// worker would fill up with exchanges nobody has in hand.
#[test]
fn every_way_out_of_an_exchange_gives_its_place_back() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let serving = |upstream| {
            let worker = Worker::with_limits(served(upstream), H1Limits::default());
            async move {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);
                (worker, front)
            }
        };
        let given_back = |worker: &Rc<Worker>, case: &str| {
            let worker = Rc::clone(worker);
            let case = case.to_owned();
            async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    while worker.places.held() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{case}: the place was kept"));
            }
        };

        // Nothing listening where the upstream should be.
        let (_held, nowhere) = refusing();
        let (worker, front) = serving(nowhere).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::BAD_GATEWAY);
        given_back(&worker, "unreachable").await;

        let (upstream, held) = scripted_upstream().await;
        let (worker, front) = serving(upstream).await;

        assert_eq!(status_of(front, "/nothing").await, StatusCode::NO_CONTENT);
        given_back(&worker, "no body").await;

        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        given_back(&worker, "a whole body").await;

        // Taken as far as the answer's first bytes, then no further.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/endless")).await.unwrap();
        let mut some = [0; 1024];
        let read = tokio::time::timeout(Duration::from_secs(5), client.read(&mut some))
            .await
            .expect("no answer");
        assert!(read.unwrap() > 0, "closed before answering");
        assert_eq!(worker.places.held(), 1, "not in hand while answering");
        drop(client);
        given_back(&worker, "a client that stopped reading").await;

        // The head says ten bytes, and five come before the upstream goes.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/short")).await.unwrap();
        let mut rest = Vec::new();
        let _ended = tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
            .await
            .expect("the failed answer was never ended");
        given_back(&worker, "a body that failed").await;

        // Asked, and gone before the upstream says anything.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;
        assert_eq!(worker.places.held(), 1, "not in hand while waiting");
        drop(client);
        given_back(&worker, "a client that went").await;
    }));
}

/// A request for `path` as a client would write it, asking for the connection to be
/// closed after the answer so that reading to the end reads the one answer.
fn asking(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n").into_bytes()
}

/// A data plane for `upstream`.
fn served(upstream: SocketAddr) -> Arc<Proxy> {
    Arc::new(Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap())
}

/// How many connections a worker opens for `requests` sent one after another, when it
/// may keep `idle_per_destination` of them.
async fn connections_for(keeping: usize, requests: usize) -> usize {
    let (upstream, opened) = counting_upstream().await;
    let limits = H1Limits {
        idle_per_destination: keeping,
        ..H1Limits::default()
    };
    let worker = Worker::with_limits(sending_to(upstream), limits);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    for _ in 0..requests {
        assert_eq!(status_over_http1(front).await, StatusCode::OK);
    }
    opened.load(std::sync::atomic::Ordering::SeqCst)
}

/// How many idle connections a worker keeps to one destination is the data plane's
/// bound and not one client's, so it holds whichever client carries the request.
/// Left to itself the engine's pool keeps as many as it likes, which is a different
/// proxy from the one [13 §7](../../docs/13-http1-upstream.md) describes.
#[test]
fn the_bound_on_idle_connections_holds_for_every_client() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        // Keeping none: every request after the first opens its own connection.
        assert_eq!(connections_for(0, 3).await, 3, "keeping none");
        // Keeping one: the first connection carries all three.
        assert_eq!(connections_for(1, 3).await, 1, "keeping one");
    }));
}

/// The worker's sweep reaches every pool it keeps: a connection left idle past its
/// time is closed by the sweep, whichever client's it is, with nothing asking for it
/// again ([13 §3](../../docs/13-http1-upstream.md)).
#[test]
fn the_sweep_closes_what_every_pool_left_idle_too_long() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _opened) = counting_upstream().await;
        let limits = H1Limits {
            idle_timeout: Duration::from_millis(100),
            sweep: Duration::from_millis(50),
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(sending_to(upstream), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
        assert_eq!(status_over_http1(front).await, StatusCode::OK);
        until(|| worker.idle_connections() == 1).await;

        let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
        tokio::time::timeout(Duration::from_secs(5), async {
            while worker.idle_connections() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("an idle connection outlived the sweep"));
    }));
}

/// What became of every connection is counted, and so is a worker's own holding, for
/// every client over this worker's pool. A benchmark that cannot tell a reused
/// connection from a fresh one is measuring the wrong thing
/// ([13 §7](../../docs/13-http1-upstream.md)).
#[test]
fn what_became_of_a_connection_is_counted() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _opened) = counting_upstream().await;
        let proxy = sending_to(upstream);
        let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        for _ in 0..3 {
            assert_eq!(status_over_http1(front).await, StatusCode::OK);
        }
        // One connection opened for the first request, and taken again for the rest.
        let scrape = proxy.metrics.render(&["web".to_owned()], &[], &[]);
        assert!(
            scrape.contains(
                "edgerush_upstream_connections_total{state=\"opened\"} 1
"
            ),
            "{scrape}"
        );
        assert!(
            scrape.contains(
                "edgerush_upstream_connections_total{state=\"reused\"} 2
"
            ),
            "{scrape}"
        );
        // And what the worker holds, which it says as it sweeps.
        let storage = worker.blocks.borrow().storage().used();
        proxy
            .metrics
            .worker()
            .holding(worker.places.held(), worker.idle_connections(), storage);
        let scrape = proxy.metrics.render(&["web".to_owned()], &[], &[]);
        assert!(
            scrape.contains(
                "edgerush_upstream_connections_idle 1
"
            ),
            "{scrape}"
        );
        assert!(storage > 0, "a connection kept holds a block");
        assert!(
            scrape.contains(&format!("\nedgerush_worker_storage_bytes {storage}\n")),
            "{scrape}"
        );
    }));
}

/// An exchange the worker could not pay for is counted as that, and not as the
/// connection failing, which is what it would pass for among the I/O failures
/// (14 §8).
#[test]
fn an_exchange_the_worker_could_not_pay_for_is_counted_as_that() {
    let exhausted = crate::storage::Storage::new(0).reserve(1).unwrap_err();
    assert_eq!(
        why_stopped(&ExchangeError::Exhausted(exhausted)),
        Stopped::Exhausted
    );
}

/// And the client is answered 503, counted under an answer reason of its own and not
/// against the upstream, which did not fail: the worker did (14 §8). Asked over HTTP/2,
/// whose server keeps storage of its own, so that the request is read and it is the
/// exchange that has nothing to pay with; our own HTTP/1 server could not read the head
/// on nothing, and would close.
#[test]
fn an_exchange_the_worker_cannot_pay_for_is_answered_503() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap();
        // Answers at once, so that a worker that could pay would be seen to.
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut asked = [0; 1024];
                    let _read = stream.read(&mut asked).await;
                    let _said = stream
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                        .await;
                });
            }
        });

        let proxy = served(upstream);
        let limits = H1Limits {
            storage: 0,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(Arc::clone(&proxy), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        assert_eq!(
            status_over_http2(front).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let up = proxy.metrics.upstream_slot("up");
        let scrape = proxy
            .metrics
            .render(&["web".to_owned()], &[("up", up)], &[]);
        assert!(
            scrape.contains(
                "edgerush_listener_local_answers_total{listener=\"web\",reason=\"exhausted\"} 1\n"
            ),
            "{scrape}"
        );
        assert!(
            scrape.contains("edgerush_upstream_failures_total{upstream=\"up\"} 0\n"),
            "{scrape}"
        );
    }));
}

/// A request whose body cannot be read is the client's fault, found after the request
/// went upstream: it is answered 400 and counted as that, not as the upstream failing,
/// which is what a 502 would have said.
#[test]
fn a_body_that_cannot_be_read_is_answered_400_and_not_held_against_the_upstream() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap();
        // Takes what it is sent and never answers: only the client can end this.
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut taken = [0; 1024];
                    while matches!(stream.read(&mut taken).await, Ok(read) if read > 0) {}
                });
            }
        });

        let proxy = served(upstream);
        let worker = Worker::new(Arc::clone(&proxy));
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        let mut client = TcpStream::connect(front).await.unwrap();
        client
            .write_all(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
            .await
            .unwrap();
        let mut answer = Vec::new();
        let _read = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client.read_to_end(&mut answer),
        )
        .await
        .unwrap();
        let answer = String::from_utf8_lossy(&answer);
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");

        let up = proxy.metrics.upstream_slot("up");
        let scrape = proxy
            .metrics
            .render(&["web".to_owned()], &[("up", up)], &[]);
        assert!(
            scrape.contains(
                "edgerush_listener_local_answers_total{listener=\"web\",reason=\"bad_body\"} 1\n"
            ),
            "{scrape}"
        );
        assert!(
            scrape.contains("edgerush_upstream_failures_total{upstream=\"up\"} 0\n"),
            "{scrape}"
        );
    }));
}

/// **Many incomplete large heads, pinned frames and slow consumers, all at once**
/// (14 §8), against a worker whose storage is a fraction of what they ask for and far
/// below its exchange cap: at every moment it is looked at, it holds no more than its
/// limit and provision; the limit, not the cap, is what stops it, since it comes within
/// a grown block of it; and once they have all gone it holds no more than a quiet worker
/// keeps, and serves again.
#[test]
fn a_worker_under_every_kind_of_load_at_once_stays_within_its_storage() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const LIMIT: usize = 1 << 20;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        // An upstream that reads no more of an upload than its head, answers `/big`
        // with more than any client here will read, and `/ok` at once.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    if head.starts_with(b"POST /upload") {
                        std::future::pending::<()>().await;
                    } else if head.starts_with(b"GET /big") {
                        // Said to close: a client's socket buffers can take the whole
                        // answer, and the connection then goes back to the pool, where
                        // a later request would wait on it for ever — this upstream
                        // answers one request a connection.
                        let length = 1 << 20;
                        let said = format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: {length}\r\n\r\n"
                        );
                        let _said = stream.write_all(said.as_bytes()).await;
                        let _said = stream.write_all(&vec![b'x'; length]).await;
                        std::future::pending::<()>().await;
                    } else {
                        let _said = stream
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                            .await;
                    }
                });
            }
        });

        let proxy = served(upstream);
        // An upload stalled on its upstream is not read, so its client going unseen until
        // the exchange's own wait runs out: shortened here, so that the test sees the end
        // of what an upload pins without waiting the usual thirty seconds for it.
        let limits = H1Limits {
            storage: LIMIT,
            idle: Duration::from_secs(3),
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(Arc::clone(&proxy), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
        let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
        let storage = Rc::clone(worker.blocks.borrow().storage());

        // Each client holds its connection until it is aborted.
        let client = |sent: Vec<u8>| {
            tokio::task::spawn_local(async move {
                let mut client = TcpStream::connect(front).await.unwrap();
                let _sent = client.write_all(&sent).await;
                std::future::pending::<()>().await;
            })
        };
        let mut clients = Vec::new();
        // Uploads whose frames the upstream never takes, pinning the blocks they were
        // cut from once the sockets' own buffers are full: longer than any of those,
        // written until the worker stops reading them.
        for _ in 0..10 {
            clients.push(tokio::task::spawn_local(async move {
                let mut client = TcpStream::connect(front).await.unwrap();
                let head = b"POST /upload HTTP/1.1\r\nhost: example.test\r\n\
                        content-length: 1073741824\r\n\r\n";
                let _sent = client.write_all(head).await;
                let piece = vec![b'u'; 64 * 1024];
                while client.write_all(&piece).await.is_ok() {}
                std::future::pending::<()>().await;
            }));
        }
        // Clients that ask for a large answer and never read it.
        for _ in 0..10 {
            clients.push(client(
                b"GET /big HTTP/1.1\r\nhost: example.test\r\n\r\n".to_vec(),
            ));
        }
        // The uploads and the slow consumers are under way, their frames pinned and
        // their answers queued, before the heads arrive to compete with them.
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Heads that never end, each longer than a small block holds. Those the worker
        // cannot pay to read are closed, and say so.
        let turned_away = Rc::new(Cell::new(0));
        for _ in 0..40 {
            let mut head = b"GET / HTTP/1.1\r\nhost: example.test\r\nx-pad: ".to_vec();
            head.extend(std::iter::repeat_n(b'a', 50 * 1024));
            let turned_away = Rc::clone(&turned_away);
            clients.push(tokio::task::spawn_local(async move {
                let mut client = TcpStream::connect(front).await.unwrap();
                let _sent = client.write_all(&head).await;
                let mut nothing = [0; 64];
                if matches!(client.read(&mut nothing).await, Ok(0) | Err(_)) {
                    turned_away.set(turned_away.get() + 1);
                }
                std::future::pending::<()>().await;
            }));
        }

        let ceiling = LIMIT + crate::storage::PROVISION;
        let (mut most, mut outlived) = (0, 0);
        for _ in 0..250 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let used = storage.used();
            assert!(used <= ceiling, "{used} held, past {ceiling}");
            most = most.max(used);
            outlived = outlived.max(storage.outlived());
        }
        let sizes = worker.blocks.borrow().sizes();
        assert!(
            most + sizes.large > LIMIT,
            "the most held was {most}: the limit was never what stopped it"
        );
        assert!(turned_away.get() > 0, "no head was turned away");
        // Memory held by frames alone, its blocks let go of by a sweep while the frames
        // wait on the upstream, was among what was counted.
        assert!(outlived > 0, "no pinned memory was ever held to account");
        assert!(worker.places.held() < limits.exchanges);

        for client in &clients {
            client.abort();
        }
        // Closes are noticed, stalled uploads given up on at their exchanges' wait, and a
        // sweep or two finds the last pinned frames gone and trims what is parked.
        tokio::time::sleep(limits.idle + limits.sweep * 3).await;
        let staging = 16 * 1024;
        let kept = sizes.kept * (sizes.small + sizes.large + staging);
        let used = storage.used();
        assert!(
            used <= kept,
            "{used} held once it was all over, past {kept}"
        );
        assert_eq!(storage.outlived(), 0, "pinned memory still charged");
        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
    }));
}

/// A local answer of the request core reaches the client from a worker that has run
/// out, as our own server writes it from the provision (14 §8): here the head that asks
/// takes the whole limit, and names no host.
#[test]
fn a_worker_that_has_run_out_still_gives_its_own_answers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (_held, nowhere) = refusing();
        let proxy = served(nowhere);
        let limits = H1Limits {
            storage: crate::upstream::h1::blocks::SMALL,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(Arc::clone(&proxy), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        let mut client = tokio::net::TcpStream::connect(front).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert!(
            received.starts_with(b"HTTP/1.1 400 "),
            "{}",
            String::from_utf8_lossy(&received)
        );
    }));
}

/// An exchange that ends without an answer says which of the named reasons it
/// was. The names are a fixed list: an upstream that fails in a new way does not
/// get to make a new series, and no error text reaches a label.
#[test]
fn why_an_exchange_stopped_is_counted_by_a_name_of_ours() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        // An upstream that waits to be asked and then says something that is not
        // an answer.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut asked = [0; 1024];
                    let _read = stream.read(&mut asked).await;
                    let _said = stream.write_all(b"nonsense\r\n\r\n").await;
                });
            }
        });

        let proxy = sending_to(upstream);
        let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        assert_eq!(status_over_http1(front).await, StatusCode::BAD_GATEWAY);
        let up = proxy.metrics.upstream_slot("up");
        let scrape = proxy
            .metrics
            .render(&["web".to_owned()], &[("up", up)], &[]);
        assert!(
            scrape.contains("edgerush_upstream_exchanges_stopped_total{reason=\"codec\"} 1\n"),
            "{scrape}"
        );
        // And the answer never arrived, so nothing counted it as a body that
        // failed part way: the two are different things.
        assert!(
            scrape.contains("edgerush_upstream_body_failures_total{upstream=\"up\"} 0\n"),
            "{scrape}"
        );
    }));
}

/// A body that fails after its head has gone is counted where nothing else
/// would notice it: the status was counted as a success and the client was told
/// so, and only the body knew otherwise
/// ([13 §7](../../docs/13-http1-upstream.md)). Both paths count it.
#[test]
fn a_body_that_fails_after_its_head_is_counted() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async move {
        // A head that promises ten bytes, five bytes, and then the end.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut asked = [0; 1024];
                    let _read = stream.read(&mut asked).await;
                    let _said = stream
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nshort")
                        .await;
                    // Long enough for the head to reach the client before
                    // the body stops. Which of the two a client sees when a
                    // body fails is the downstream server's buffering rather
                    // than anything decided here, and this test is about the
                    // counter rather than about that.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                });
            }
        });

        let proxy = sending_to(upstream);
        let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);

        // The head arrives and says the answer succeeded; the body does not.
        let stream = TcpStream::connect(front).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let _driving = tokio::task::spawn_local(async move {
            let _closed = connection.await;
        });
        let request = Request::builder()
            .uri("/")
            .header("host", "example.test")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let answer = sender.send_request(request).await.unwrap();
        assert_eq!(answer.status(), StatusCode::OK);
        assert!(
            answer.into_body().collect().await.is_err(),
            "a truncated body was made to look whole"
        );

        let up = proxy.metrics.upstream_slot("up");
        let scrape = proxy
            .metrics
            .render(&["web".to_owned()], &[("up", up)], &[]);
        assert!(
            scrape.contains("edgerush_upstream_body_failures_total{upstream=\"up\"} 1\n"),
            "{scrape}"
        );
        // And the answer was counted a success, which is why the body needed
        // a counter of its own.
        assert!(
            scrape.contains("edgerush_upstream_responses_total{upstream=\"up\",class=\"2xx\"} 1\n"),
            "{scrape}"
        );
    }));
}

/// Which client sends the body a rule keeps to send again.
#[derive(Debug, Clone, Copy)]
enum Sender {
    Http1,
    Http2,
    Http3,
}

/// A request's body kept to send again is paid for in the worker's storage whichever
/// client sent it (14 §8): over HTTP/2 the frames are h2's, whose charge ends as each
/// one's credit is given back, so the recording pays for them itself.
#[test]
fn a_body_kept_to_send_again_is_charged_over_http2() {
    kept_body_is_charged(Sender::Http2);
}

/// The same over HTTP/3, whose frames are pieces read out of quiche, charged nowhere
/// once read.
#[test]
fn a_body_kept_to_send_again_is_charged_over_http3() {
    kept_body_is_charged(Sender::Http3);
}

/// The same over HTTP/1, whose frames are cut from blocks paid for while a piece of them
/// is held: the control.
#[test]
fn a_body_kept_to_send_again_is_charged_over_http1() {
    kept_body_is_charged(Sender::Http1);
}

/// A rule retrying 503 once, an upstream that reads a request whole and holds its answer
/// until told, then answers the first try 503 and the second 200: the body, 48 KiB in three
/// frames, is kept from when it has gone until the first answer's head, and sent whole again
/// after it. Once the upstream has all of it, the worker's storage holds at least what is
/// kept.
fn kept_body_is_charged(sender: Sender) {
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const PIECE: usize = 16 * 1024;
    const BODY: usize = 3 * PIECE;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap();
        let bodies = Arc::new(AtomicUsize::new(0));
        let answer = Arc::new(tokio::sync::Notify::new());
        {
            let (bodies, answer) = (Arc::clone(&bodies), Arc::clone(&answer));
            tokio::spawn(async move {
                let mut first = true;
                while let Ok((mut stream, _)) = listener.accept().await {
                    let (bodies, answer) = (Arc::clone(&bodies), Arc::clone(&answer));
                    let first_try = std::mem::replace(&mut first, false);
                    tokio::spawn(async move {
                        let (mut seen, mut buffer) = (Vec::new(), vec![0; 64 * 1024]);
                        let body = loop {
                            match stream.read(&mut buffer).await {
                                Ok(0) | Err(_) => return,
                                Ok(read) => seen.extend_from_slice(&buffer[..read]),
                            }
                            if let Some(end) = seen.windows(4).position(|w| w == b"\r\n\r\n")
                                && seen.len() - end - 4 >= BODY
                            {
                                break seen.len() - end - 4;
                            }
                        };
                        bodies.fetch_add(body, Ordering::SeqCst);
                        let said: &[u8] = if first_try {
                            answer.notified().await;
                            b"HTTP/1.1 503 Service Unavailable\r\nconnection: close\r\ncontent-length: 0\r\n\r\n"
                        } else {
                            b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"
                        };
                        let _said = stream.write_all(said).await;
                        std::future::pending::<()>().await;
                    });
                }
            });
        }
        let mut config = match sender {
            Sender::Http3 => h3_config(
                upstream,
                edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: false,
                },
            ),
            Sender::Http1 | Sender::Http2 => everything_config(upstream),
        };
        config.routes[0].rules[0]
            .forward
            .as_mut()
            .expect("the rule forwards")
            .retry = Some(retrying(1, &[503], &[], 1));
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
        let storage = Rc::clone(worker.blocks.borrow().storage());
        let within = Duration::from_secs(10);
        let kept = || {
            let used = storage.used();
            assert!(
                used >= BODY,
                "{BODY} bytes kept to send again over {sender:?}, {used} charged"
            );
        };
        let body = Bytes::from(vec![b'x'; PIECE]);

        match sender {
            Sender::Http3 => {
                use crate::downstream::h3::testing::Client;
                let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
                let alone = Forwarding::group(1).remove(0);
                let _serving = tokio::task::spawn_local(
                    Rc::clone(&worker).serve_h3(0, socket, alone).unwrap(),
                );
                let mut client = Client::connect(front, "a.test").await;
                let length = BODY.to_string();
                let id = client.request(
                    &[
                        (":method", "POST"),
                        (":scheme", "https"),
                        (":authority", "a.test"),
                        (":path", "/"),
                        ("content-length", &length),
                    ],
                    false,
                );
                for at in 0..3 {
                    client.body(id, &body, at == 2).await;
                }
                client
                    .until(|_| bodies.load(Ordering::SeqCst) >= BODY)
                    .await;
                client.for_a_while(Duration::from_millis(100)).await;
                kept();
                answer.notify_one();
                let answered = client.answer(id).await;
                assert_eq!(answered.final_status(), Some("200"));
            }
            Sender::Http2 => {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);
                let stream = TcpStream::connect(front).await.unwrap();
                let (mut send, mut connection) = ::h2::client::handshake(stream).await.unwrap();
                let mut ping_pong = connection.ping_pong().expect("a ping-pong handle");
                let _driving = tokio::task::spawn_local(async move {
                    let _ended = connection.await;
                });
                let request = Request::builder()
                    .method("POST")
                    .uri("http://a.test/")
                    .header("content-length", BODY.to_string())
                    .body(())
                    .unwrap();
                let (answered, mut sending) = send.send_request(request, false).unwrap();
                for at in 0..3 {
                    sending.send_data(body.clone(), at == 2).unwrap();
                }
                until(|| bodies.load(Ordering::SeqCst) >= BODY).await;
                // A turn more of the server's connection, which charges what h2 holds now.
                tokio::time::timeout(within, ping_pong.ping(::h2::Ping::opaque()))
                    .await
                    .expect("no pong")
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                kept();
                answer.notify_one();
                let answered = tokio::time::timeout(within, answered)
                    .await
                    .expect("no answer")
                    .unwrap();
                assert_eq!(answered.status(), StatusCode::OK);
            }
            Sender::Http1 => {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);
                let mut client = TcpStream::connect(front).await.unwrap();
                let head =
                    format!("POST / HTTP/1.1\r\nhost: a.test\r\ncontent-length: {BODY}\r\n\r\n");
                client.write_all(head.as_bytes()).await.unwrap();
                client.write_all(&vec![b'x'; BODY]).await.unwrap();
                until(|| bodies.load(Ordering::SeqCst) >= BODY).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                kept();
                answer.notify_one();
                let mut answered = Vec::new();
                let mut byte = [0; 1];
                while !answered.ends_with(b"\r\n\r\n") {
                    tokio::time::timeout(within, client.read_exact(&mut byte))
                        .await
                        .expect("no answer")
                        .unwrap();
                    answered.push(byte[0]);
                }
                assert!(
                    answered.starts_with(b"HTTP/1.1 200"),
                    "{}",
                    String::from_utf8_lossy(&answered)
                );
            }
        }
        // Kept whole: the second try carried all of it again.
        until(|| bodies.load(Ordering::SeqCst) >= 2 * BODY).await;
    }));
}
