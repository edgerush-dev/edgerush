//! Backend protection ([25](../../../../docs/25-backend-protection.md)): each endpoint's
//! requests in flight, every worker's together, held to the upstream's bound from the pick
//! to the end of the exchange.

use super::*;

/// `everything_config`'s upstream `up` at `upstream`, protected to `cap` in flight.
fn protected(upstream: SocketAddr, cap: u32) -> Compiled {
    let mut config = everything_config(upstream);
    config.upstreams.get_mut("up").unwrap().protection = Some(edgerush_config::Protection {
        max_in_flight: std::num::NonZeroU32::new(cap).unwrap(),
    });
    compile(&config).unwrap()
}

/// How many requests `up`'s first endpoint has in flight, every worker's together, as the
/// proxy's current config counts them.
fn in_flight(proxy: &Proxy) -> u32 {
    let snapshot = proxy.current.load();
    snapshot.protected[0]
        .as_ref()
        .expect("`up` is protected")
        .counts[0]
        .in_flight()
}

/// A worker of `proxy` serving a listener of its own, and where.
async fn serving_from(proxy: &Arc<Proxy>) -> (SocketAddr, Rc<Worker>) {
    let worker = Worker::with_deadlines(Arc::clone(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// A request for `path` as a client would write it, asking for the connection to be
/// closed after the answer.
fn asking(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n").into_bytes()
}

/// An endpoint at its bound takes no more from any worker: the count is every worker's
/// together, and a request that finds it full is answered 503, never 429, named as
/// protection's (25 §5, §8). Its slot given back, the next request goes through.
#[test]
fn an_endpoint_at_its_bound_takes_nothing_more_from_any_worker() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (first, _one) = serving_from(&proxy).await;
        let (second, two) = serving_from(&proxy).await;

        // One request in flight on the first worker, which the upstream never answers.
        let mut waiting = TcpStream::connect(first).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;
        assert_eq!(in_flight(&proxy), 1);

        // The second worker has nothing in flight of its own, and is refused all the same.
        assert_eq!(
            status_of(second, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(in_flight(&proxy), 1, "a refusal takes no slot");
        let line =
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"backend_full\"} 1\n";
        let scrape = two.proxy().metrics();
        assert!(
            scrape.contains(line),
            "the refusal is protection's: {scrape}"
        );

        // The first request ends: its slot comes back, and the next request goes through.
        drop(waiting);
        held.borrow_mut().clear();
        until(|| in_flight(&proxy) == 0).await;
        assert_eq!(status_of(second, "/ok").await, StatusCode::OK);
        assert_eq!(in_flight(&proxy), 0);
    }));
}

/// Every way out of an exchange gives its slot back, as it gives its place back
/// (`resources::every_way_out_of_an_exchange_gives_its_place_back`): an upstream that
/// could not be reached, an answer with no body, one read to its end, one the client
/// stopped reading, one that failed part way, and a client that went before anything came
/// back (25 §6). A slot one of them kept would hold the endpoint full for ever.
#[test]
fn every_way_out_of_an_exchange_gives_its_slot_back() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let given_back = |proxy: &Arc<Proxy>, case: &str| {
            let proxy = Arc::clone(proxy);
            let case = case.to_owned();
            async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    while in_flight(&proxy) != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{case}: the slot was kept"));
            }
        };

        // Nothing listening where the upstream should be: a connect that failed held no
        // slot once it failed, and the next request finds the endpoint free (a 502 again,
        // not protection's 503).
        let (_refusing, nowhere) = refusing();
        let proxy = Arc::new(Proxy::new(protected(nowhere, 1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::BAD_GATEWAY);
        given_back(&proxy, "unreachable").await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::BAD_GATEWAY);

        let (upstream, held) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;

        assert_eq!(status_of(front, "/nothing").await, StatusCode::NO_CONTENT);
        given_back(&proxy, "no body").await;

        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        given_back(&proxy, "a whole body").await;

        // Taken as far as the answer's first bytes, then no further.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/endless")).await.unwrap();
        let mut some = [0; 1024];
        let read = tokio::time::timeout(Duration::from_secs(5), client.read(&mut some))
            .await
            .expect("no answer");
        assert!(read.unwrap() > 0, "closed before answering");
        assert_eq!(in_flight(&proxy), 1, "not counted while answering");
        drop(client);
        given_back(&proxy, "a client that stopped reading").await;

        // The head says ten bytes, and five come before the upstream goes.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/short")).await.unwrap();
        let mut rest = Vec::new();
        let _ended = tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
            .await
            .expect("the failed answer was never ended");
        given_back(&proxy, "a body that failed").await;

        // Asked, and gone before the upstream says anything.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;
        assert_eq!(in_flight(&proxy), 1, "not counted while waiting");
        drop(client);
        given_back(&proxy, "a client that went").await;
    }));
}

/// A client gone while its request's connect is still being made: the worker carries the
/// connect on to its end, with the request's place, but no request will go on it, so its
/// slot comes back at once (25 §6).
#[test]
fn a_client_gone_mid_connect_gives_its_slot_back_at_once() {
    use crate::upstream::dial::hold_connects;
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _) = counting_upstream().await;
        hold_connects(upstream, Some(Duration::from_secs(60)));
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (front, worker) = serving_from(&proxy).await;
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/ok")).await.unwrap();
        until(|| in_flight(&proxy) == 1).await;
        drop(client);
        until(|| worker.watcher.watched() == 1).await;
        assert_eq!(worker.places.held(), 1, "the place goes with the connect");
        assert_eq!(in_flight(&proxy), 0, "the slot does not");
        hold_connects(upstream, None);
    }));
}

/// A reload swaps the config, not the requests in flight: an endpoint the new config keeps
/// keeps its count, so the bound holds across it with no burst, and a raised bound admits
/// at once (25 §3, §7).
#[test]
fn a_reload_keeps_an_endpoints_count() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        let mut waiting = TcpStream::connect(front).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;

        // The same config again: a new snapshot, the same count.
        proxy.reload(protected(upstream, 1)).unwrap();
        assert_eq!(in_flight(&proxy), 1);
        assert_eq!(
            status_of(front, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE,
            "a reload let a second request through"
        );

        // A raised bound admits at once, counted against what is in flight.
        proxy.reload(protected(upstream, 2)).unwrap();
        let mut also = TcpStream::connect(front).await.unwrap();
        also.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 2).await;
        assert_eq!(in_flight(&proxy), 2);
        assert_eq!(
            status_of(front, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        drop((waiting, also));
        held.borrow_mut().clear();
        until(|| in_flight(&proxy) == 0).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
    }));
}

/// A pod at its bound is passed over: requests from any worker go to the one with room, and
/// only with both full is the backend refused (25 §4). Neither is set aside for being full:
/// once their requests end, both take requests again.
#[test]
fn a_full_pod_is_passed_over_for_one_with_room() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (a, held_at_a) = scripted_upstream().await;
        let (b, held_at_b) = scripted_upstream().await;
        let held = || held_at_a.borrow().len() + held_at_b.borrow().len();
        let mut config = everything_config(a);
        let up = config.upstreams.get_mut("up").unwrap();
        up.endpoints = edgerush_config::Endpoints::Listed(vec![a, b]);
        up.protection = Some(edgerush_config::Protection {
            max_in_flight: std::num::NonZeroU32::MIN,
        });
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (first, _one) = serving_from(&proxy).await;
        let (second, _two) = serving_from(&proxy).await;
        let all_in_flight = || {
            let snapshot = proxy.current.load();
            let protected = snapshot.protected[0].as_ref().expect("`up` is protected");
            protected
                .counts
                .iter()
                .map(|count| count.in_flight())
                .sum::<u32>()
        };

        // One pod holds a request from the first worker, which it never answers: each of
        // the second worker's goes to the other.
        let mut waiting = TcpStream::connect(first).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held() == 1).await;
        for _ in 0..10 {
            assert_eq!(status_of(second, "/ok").await, StatusCode::OK);
        }

        // Both full: refused, from either worker.
        let mut also = TcpStream::connect(second).await.unwrap();
        also.write_all(&asking("/silent")).await.unwrap();
        until(|| held() == 2).await;
        assert_eq!((held_at_a.borrow().len(), held_at_b.borrow().len()), (1, 1));
        for front in [first, second] {
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        assert_eq!(all_in_flight(), 2, "a refusal takes no slot");
        let snapshot = proxy.current.load();
        assert!(
            snapshot
                .destinations
                .of(0)
                .iter()
                .all(|destination| destination.serves()),
            "a full pod was set aside"
        );

        // Their requests ended, both take again.
        drop((waiting, also));
        held_at_a.borrow_mut().clear();
        held_at_b.borrow_mut().clear();
        until(|| all_in_flight() == 0).await;
        let mut again: Vec<TcpStream> = Vec::new();
        for front in [first, second] {
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(&asking("/silent")).await.unwrap();
            again.push(client);
        }
        until(|| held() == 2).await;
        assert_eq!((held_at_a.borrow().len(), held_at_b.borrow().len()), (1, 1));
    }));
}
