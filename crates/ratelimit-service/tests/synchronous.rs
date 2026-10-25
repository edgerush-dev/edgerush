//! Three synchronous service pods on real sockets, data planes deciding through their
//! streams.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_ratelimit::service::{PodId, View};
use edgerush_ratelimit_service::grpc::Client;
use edgerush_ratelimit_service::synchronous::wire::{
    Decide, Decided, FromOwner, GiveBack, Key, ToOwner, Upstream,
};
use edgerush_ratelimit_service::synchronous::{
    DecideStream, SyncPod, SyncServer, Timing, decide_stream,
};
use edgerush_ratelimit_service::wire::Contract;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

/// Faster than a deployment's, so that tests settle quickly.
fn timing() -> Timing {
    Timing {
        copy: Duration::from_millis(10),
        heartbeat: Duration::from_millis(20),
        silence: Duration::from_millis(150),
        settle: Duration::from_millis(60),
    }
}

/// Ten an hour, a bucket of ten: nothing refills while a test runs, however slowly the
/// machine runs it.
fn key(name: &[u8]) -> Key {
    Key {
        key: name.to_vec(),
        contract: Some(Contract {
            limit: 10,
            window_ns: 3_600_000_000_000,
            burst: 10,
        }),
    }
}

struct Service {
    pods: Vec<SyncPod>,
    addresses: Vec<String>,
}

/// Three synchronous pods, each serving on a port of its own, once they agree all three
/// serve.
async fn start() -> Service {
    start_with(timing()).await
}

async fn start_with(timing: Timing) -> Service {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let clients: BTreeMap<PodId, Client> = (0..3)
        .map(|pod| (pod, Client::new(&addresses[pod as usize]).unwrap()))
        .collect();
    let mut pods = Vec::new();
    for (id, listener) in (0..3).zip(listeners) {
        let peers = clients
            .iter()
            .filter(|(pod, _)| **pod != id)
            .map(|(pod, client)| (*pod, client.clone()))
            .collect();
        let pod = SyncPod::spawn(id, vec![0, 1, 2], 1, timing, peers);
        let server = SyncServer::new(pod.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(server, TcpIncoming::from(listener))
                .await
                .unwrap();
        });
        pods.push(pod);
    }
    // Agreed and settled, so that no view changes under a test: five heartbeats unchanged.
    let agreed = |pods: &[SyncPod]| {
        let (first, _) = pods[0].view();
        pods.iter()
            .all(
                |pod| matches!(pod.view(), (view, true) if view == first && view.pods == [0, 1, 2]),
            )
            .then_some(first.epoch)
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "the pods never agreed");
        if let Some(epoch) = agreed(&pods) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if agreed(&pods) == Some(epoch) {
                return Service { pods, addresses };
            }
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// A data plane's stream to one pod, numbering its requests.
struct Plane {
    stream: DecideStream,
    next: u64,
}

impl Plane {
    /// Opens a stream to the pod at `address`, and the view it is sent first.
    async fn open(address: &str) -> (Self, View) {
        let mut stream = decide_stream(address).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(1), stream.down.message())
            .await
            .expect("the view first, in time")
            .unwrap()
            .unwrap();
        let Some(FromOwner::View(view)) = first.message else {
            panic!("the view comes first, not {first:?}");
        };
        (Self { stream, next: 1 }, view.into())
    }

    /// Sends a request under `keys` and returns its number.
    async fn send(&mut self, keys: &[&[u8]]) -> u64 {
        self.send_keys(keys.iter().map(|name| key(name)).collect())
            .await
    }

    async fn send_keys(&mut self, keys: Vec<Key>) -> u64 {
        let id = self.next;
        self.next += 1;
        self.up(ToOwner::Decide(Decide { id, keys })).await;
        id
    }

    async fn up(&mut self, message: ToOwner) {
        self.stream
            .up
            .send(Upstream {
                message: Some(message),
            })
            .await
            .unwrap();
    }

    /// The next answer down the stream, past any view.
    async fn answer(&mut self) -> Decided {
        loop {
            let down = tokio::time::timeout(Duration::from_secs(5), self.stream.down.message())
                .await
                .expect("an answer in time")
                .unwrap()
                .expect("the stream still open");
            if let Some(FromOwner::Decided(decided)) = down.message {
                return decided;
            }
        }
    }

    /// Decides one request under `keys`.
    async fn decide(&mut self, keys: &[&[u8]]) -> Decided {
        self.decide_keys(keys.iter().map(|name| key(name)).collect())
            .await
    }

    async fn decide_keys(&mut self, keys: Vec<Key>) -> Decided {
        let id = self.send_keys(keys).await;
        let decided = self.answer().await;
        assert_eq!(decided.id, id);
        decided
    }

    /// How the stream ends, past any view: the status it ends with, or none.
    async fn end(&mut self) -> Option<tonic::Status> {
        loop {
            let down = tokio::time::timeout(Duration::from_secs(5), self.stream.down.message())
                .await
                .expect("the stream ended in time");
            match down {
                Ok(Some(down)) => assert!(
                    matches!(down.message, Some(FromOwner::View(_))),
                    "no answer: {down:?}"
                ),
                Ok(None) => return None,
                Err(status) => return Some(status),
            }
        }
    }

    async fn give_back(&mut self, keys: &[&[u8]]) {
        let back = GiveBack {
            keys: keys.iter().map(|name| key(name)).collect(),
        };
        self.up(ToOwner::GiveBack(back)).await;
    }
}

/// A stream hears the view first; requests sent together are answered in order, by
/// number, against one bucket, and a refusal says which key and for how long.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_hears_the_view_and_then_its_answers_by_number() {
    let service = start().await;
    let (mut plane, view) = Plane::open(&service.addresses[0]).await;
    assert_eq!(view.pods, [0, 1, 2]);
    let mut sent = Vec::new();
    for _ in 0..12 {
        sent.push(plane.send(&[b"k"]).await);
    }
    let mut answers = Vec::new();
    for _ in 0..12 {
        answers.push(plane.answer().await);
    }
    assert_eq!(answers.iter().map(|a| a.id).collect::<Vec<_>>(), sent);
    assert!(answers[..10].iter().all(|a| a.admitted));
    for refused in &answers[10..] {
        assert!(!refused.admitted);
        assert_eq!(refused.position, 0);
        assert!(refused.wait_ns > 0, "{refused:?}");
    }
}

/// Two streams to one pod, decided on whichever cores they are served on, share its one
/// bucket: ten admitted of twelve.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_to_one_pod_share_its_bucket() {
    let service = start().await;
    let (mut a, _) = Plane::open(&service.addresses[1]).await;
    let (mut b, _) = Plane::open(&service.addresses[1]).await;
    let six = async |plane: &mut Plane| {
        let mut admitted = 0;
        for _ in 0..6 {
            if plane.decide(&[b"k"]).await.admitted {
                admitted += 1;
            }
        }
        admitted
    };
    let (from_a, from_b) = tokio::join!(six(&mut a), six(&mut b));
    assert_eq!(from_a + from_b, 10);
}

/// A request is refused by the key that waits, named by its place, and charged under
/// none; a charge given back is admitted again, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_charges_nothing_and_a_charge_given_back_is_admitted_again() {
    let service = start().await;
    let (mut plane, _) = Plane::open(&service.addresses[0]).await;
    for _ in 0..10 {
        assert!(plane.decide(&[b"b"]).await.admitted);
    }
    let refused = plane.decide(&[b"a", b"b"]).await;
    assert!(!refused.admitted);
    assert_eq!(refused.position, 1);
    for _ in 0..10 {
        assert!(
            plane.decide(&[b"a"]).await.admitted,
            "nothing charged under a"
        );
    }
    plane.give_back(&[b"b"]).await;
    assert!(plane.decide(&[b"b"]).await.admitted);
    assert!(!plane.decide(&[b"b"]).await.admitted);
}

/// An owner copies what changed to the other pods on its own cycle, and a pod asked about
/// the key carries on from the copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copies_reach_the_other_pods_which_carry_on_from_them() {
    let service = start().await;
    // One an hour, a bucket of one: the first copy of the key holds its one charge, which
    // a copy of ten charges might not hold all of.
    let one = || Key {
        key: b"k".to_vec(),
        contract: Some(Contract {
            limit: 1,
            window_ns: 3_600_000_000_000,
            burst: 1,
        }),
    };
    let (mut plane, _) = Plane::open(&service.addresses[0]).await;
    assert!(plane.decide_keys(vec![one()]).await.admitted);
    let deadline = Instant::now() + Duration::from_secs(5);
    for pod in &service.pods[1..] {
        while pod.key_count() == 0 {
            assert!(Instant::now() < deadline, "the copy never came");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    // A key is made a moment before its copy is set: a cycle covers the moment.
    tokio::time::sleep(timing().copy).await;
    for address in &service.addresses[1..] {
        let (mut other, _) = Plane::open(address).await;
        assert!(
            !other.decide_keys(vec![one()]).await.admitted,
            "carried on from the copy"
        );
    }
}

/// A stream that ends hands over what it changed: with a cycle far longer than the
/// stream, nothing else would, and the key, marked changed, would never be copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_that_ends_hands_over_what_it_changed() {
    let service = start_with(Timing {
        copy: Duration::from_millis(500),
        ..timing()
    })
    .await;
    let (mut plane, _) = Plane::open(&service.addresses[0]).await;
    assert!(plane.decide(&[b"k"]).await.admitted);
    drop(plane);
    let deadline = Instant::now() + Duration::from_secs(5);
    for pod in &service.pods[1..] {
        while pod.key_count() == 0 {
            assert!(Instant::now() < deadline, "the copy never came");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// A pod stops: the others agree it is gone and send the new view down their streams, and
/// its own streams end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_view_goes_down_the_streams_when_a_pod_stops() {
    let service = start().await;
    let (mut plane, _) = Plane::open(&service.addresses[0]).await;
    let (mut gone, _) = Plane::open(&service.addresses[2]).await;
    service.pods[2].stop();
    gone.end().await;
    loop {
        let down = tokio::time::timeout(Duration::from_secs(5), plane.stream.down.message())
            .await
            .expect("a new view in time")
            .unwrap()
            .unwrap();
        if let Some(FromOwner::View(view)) = down.message {
            let view = View::from(view);
            if !view.pods.contains(&2) {
                assert_eq!(view.pods, [0, 1]);
                break;
            }
        }
    }
    assert!(
        plane.decide(&[b"k"]).await.admitted,
        "the stream carries on"
    );
    assert!(
        decide_stream(&service.addresses[2]).await.is_err(),
        "a stopped pod opens no stream"
    );
}

/// A request under no key is refused, and its stream with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_under_no_key_ends_its_stream() {
    let service = start().await;
    let (mut plane, _) = Plane::open(&service.addresses[0]).await;
    plane.send(&[]).await;
    let refused = plane.end().await.expect("refused");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
}

/// A stream that met a key and went quiet lets go of it once the pod forgets it, as every
/// stream sweeps its handles once a second, busy or quiet: a forgotten key's memory is not
/// held past the pod's cap for as long as the stream is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_stream_lets_go_of_a_key_the_pod_forgot() {
    let service = start().await;
    let (mut plane, _) = Plane::open(&service.addresses[0]).await;
    // A thousand a second, a bucket of one: full again a millisecond after its request, so
    // that the pod's sweep forgets it once it has gone a second unused.
    let brief = Key {
        key: b"brief".to_vec(),
        contract: Some(Contract {
            limit: 1_000,
            window_ns: 1_000_000_000,
            burst: 1,
        }),
    };
    assert!(plane.decide_keys(vec![brief]).await.admitted);
    let pod = &service.pods[0];
    let deadline = Instant::now() + Duration::from_secs(8);
    let wait = |what: &str, done: &dyn Fn() -> bool| {
        assert!(Instant::now() < deadline, "{what}");
        !done()
    };
    while wait("the stream never said it held it", &|| {
        pod.handles_held() == 1
    }) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    while wait("the pod never forgot it", &|| pod.key_count() == 0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    while wait("the quiet stream kept it", &|| pod.handles_held() == 0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A stream that ends takes what it said it held with it.
    assert!(plane.decide(&[b"kept"]).await.admitted);
    while wait("the stream never said it held it", &|| {
        pod.handles_held() == 1
    }) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(plane);
    while wait("an ended stream still counted", &|| pod.handles_held() == 0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
