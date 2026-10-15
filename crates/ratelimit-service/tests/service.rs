//! Three service pods on real sockets, data planes reaching them through the gRPC client.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_ratelimit::service::{Contract, DataPlane, Decision, PodId, View};
use edgerush_ratelimit_service::grpc::{Client, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

const CYCLE: Duration = Duration::from_millis(10);

/// Faster than a deployment's, so that tests settle quickly.
fn timing() -> Timing {
    Timing {
        cycle: CYCLE,
        heartbeat: Duration::from_millis(20),
        silence: Duration::from_millis(150),
        settle: Duration::from_millis(60),
    }
}

/// Ten an hour, a bucket of ten: nothing refills while a test runs, however slowly the
/// machine runs it.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(10),
        window: n(3_600_000_000_000),
        burst: n(10),
    }
}

fn view() -> View {
    View {
        epoch: 1,
        pods: vec![0, 1, 2],
    }
}

struct Service {
    pods: Vec<Pod>,
    clients: BTreeMap<PodId, Client>,
}

/// Three service pods, each serving on a port of its own, once they agree all three serve.
async fn start() -> Service {
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
        let pod = Pod::spawn(id, vec![0, 1, 2], 1, timing(), peers);
        let server = Server::new(pod.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(server, TcpIncoming::from(listener))
                .await
                .unwrap();
        });
        pods.push(pod);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    for pod in &pods {
        while !matches!(pod.view().await, Some((view, true)) if view.pods == [0, 1, 2]) {
            assert!(Instant::now() < deadline, "the pods never agreed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    // A pod taken in makes no key afresh for three heartbeats, in case one is handed to it.
    tokio::time::sleep(Duration::from_millis(80)).await;
    Service { pods, clients }
}

/// A data plane's clock: nanoseconds since the test began.
fn now(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap()
}

/// Sends what `plane` has to send and hands it the answers, until it has nothing more.
async fn exchange(plane: &mut DataPlane, service: &Service, start: Instant) {
    loop {
        let outgoing = plane.take_outgoing();
        if outgoing.is_empty() {
            return;
        }
        for (pod, batch) in outgoing {
            let answers = service.clients[&pod].exchange(batch).await.unwrap();
            plane.on_answers(answers, now(start));
        }
    }
}

/// What `pod` counts for `key`, once it has settled on `expected` (copies travel on their
/// own), or what it last said after a second.
async fn counted(pod: &Pod, key: &[u8], expected: u64) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let counted = pod.counted(key).await;
        if counted == Some(expected) || Instant::now() > deadline {
            return counted;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Two data planes holding no copy each get six requests at once: they ask, the owner
/// decides against its one bucket of ten, and copies every change to every other pod.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cold_burst_on_two_data_planes_is_held_to_one_bucket() {
    let service = start().await;
    let start = Instant::now();
    let cycle = u64::try_from(CYCLE.as_nanos()).unwrap();
    let mut planes: Vec<DataPlane> = (0..2).map(|id| DataPlane::new(id, cycle, view())).collect();
    for plane in &mut planes {
        for _ in 0..6 {
            assert!(matches!(
                plane.decide(b"k", contract(), now(start)),
                Decision::Wait(_)
            ));
        }
    }
    for plane in &mut planes {
        exchange(plane, &service, start).await;
    }
    for plane in &mut planes {
        exchange(plane, &service, start).await;
    }
    let decided: Vec<(u64, bool)> = planes
        .iter_mut()
        .flat_map(DataPlane::take_decided)
        .collect();
    assert_eq!(decided.len(), 12);
    assert_eq!(decided.iter().filter(|(_, admitted)| *admitted).count(), 10);
    for pod in &service.pods {
        assert_eq!(counted(pod, b"k", 10).await, Some(10), "owned or copied");
    }
}

/// Hits admitted locally go to the owner with the next cycle, and the copy that comes back
/// holds them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hits_admitted_locally_reach_the_owner_each_cycle() {
    let service = start().await;
    let start = Instant::now();
    let cycle = u64::try_from(CYCLE.as_nanos()).unwrap();
    let mut plane: DataPlane = DataPlane::new(7, cycle, view());
    plane.decide(b"k", contract(), now(start));
    exchange(&mut plane, &service, start).await;
    plane.take_decided();
    // A cycle after the owner's answer bounds the copy: from then on, decided here.
    plane.on_cycle(now(start));
    exchange(&mut plane, &service, start).await;
    let admitted = (0..5)
        .filter(|_| plane.decide(b"k", contract(), now(start)) == Decision::Admit)
        .count() as u64;
    assert!(admitted >= 4, "{admitted}");
    plane.on_cycle(now(start));
    exchange(&mut plane, &service, start).await;
    let owner = view().owners(b"k").0.unwrap() as usize;
    assert_eq!(
        counted(&service.pods[owner], b"k", 1 + admitted).await,
        Some(1 + admitted)
    );
}

/// Keys spread over the pods: each batch goes to the key's owner alone, which counts the
/// key and copies it to the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keys_are_counted_by_their_owners_only() {
    let service = start().await;
    let start = Instant::now();
    let cycle = u64::try_from(CYCLE.as_nanos()).unwrap();
    let mut plane: DataPlane = DataPlane::new(9, cycle, view());
    let keys: Vec<Vec<u8>> = (0..12).map(|k| format!("key-{k}").into_bytes()).collect();
    for key in &keys {
        plane.decide(key, contract(), now(start));
    }
    let sent = plane.take_outgoing();
    for (pod, batch) in &sent {
        for entry in &batch.entries {
            assert_eq!(
                view().owners(&entry.key).0,
                Some(*pod),
                "to the owner alone"
            );
        }
    }
    for (pod, batch) in sent {
        let answers = service.clients[&pod].exchange(batch).await.unwrap();
        plane.on_answers(answers, now(start));
    }
    for key in &keys {
        for pod in &service.pods {
            assert_eq!(counted(pod, key, 1).await, Some(1), "owned or copied");
        }
    }
}

/// Sends what `plane` has, ignoring pods that do not answer, and hands it the answers.
async fn send(plane: &mut DataPlane, service: &Service, start: Instant) {
    for (pod, batch) in plane.take_outgoing() {
        if let Ok(answers) = service.clients[&pod].exchange(batch).await {
            plane.on_answers(answers, now(start));
        }
    }
}

/// The owner of a key stops: the other two agree it is gone, its secondary takes over from
/// its copy, and the data plane, told the new view, sends it what the old owner never
/// confirmed. Nothing is counted twice and nothing is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_secondary_takes_over_when_the_owner_stops() {
    let service = start().await;
    let start = Instant::now();
    let cycle = u64::try_from(CYCLE.as_nanos()).unwrap();
    let mut plane: DataPlane = DataPlane::new(11, cycle, view());
    plane.decide(b"k", contract(), now(start));
    exchange(&mut plane, &service, start).await;
    plane.take_decided();
    // A cycle after the owner's answer bounds the copy: from then on, decided here.
    plane.on_cycle(now(start));
    exchange(&mut plane, &service, start).await;
    for _ in 0..3 {
        assert_eq!(plane.decide(b"k", contract(), now(start)), Decision::Admit);
    }
    plane.on_cycle(now(start));
    exchange(&mut plane, &service, start).await;
    let (owner, secondary) = view().owners(b"k");
    let (owner, secondary) = (owner.unwrap(), secondary.unwrap());
    assert_eq!(
        counted(&service.pods[secondary as usize], b"k", 4).await,
        Some(4)
    );

    service.pods[owner as usize].stop().await;
    for _ in 0..2 {
        assert_eq!(plane.decide(b"k", contract(), now(start)), Decision::Admit);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        plane.on_cycle(now(start));
        send(&mut plane, &service, start).await;
        if service.pods[secondary as usize].counted(b"k").await == Some(6) {
            break;
        }
        assert!(Instant::now() < deadline, "the secondary never took over");
        tokio::time::sleep(CYCLE).await;
    }
    let (view, serving) = service.pods[secondary as usize].view().await.unwrap();
    assert!(serving);
    assert!(!view.pods.contains(&owner));
    for _ in 0..20 {
        plane.on_cycle(now(start));
        send(&mut plane, &service, start).await;
        tokio::time::sleep(CYCLE).await;
    }
    assert_eq!(
        service.pods[secondary as usize].counted(b"k").await,
        Some(6),
        "nothing counted twice"
    );
}

/// The owner of a key restarts, holding nothing, in a new life: the others take it out of
/// the view and its secondary carries on from its copy; when it is taken in again, the pod
/// that held the key hands it back, and the data plane follows. Nothing lost, nothing
/// counted twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_owner_is_handed_its_keys_back_when_it_rejoins() {
    let service = start().await;
    let start = Instant::now();
    let cycle = u64::try_from(CYCLE.as_nanos()).unwrap();
    let mut plane: DataPlane = DataPlane::new(13, cycle, view());
    plane.decide(b"k", contract(), now(start));
    exchange(&mut plane, &service, start).await;
    plane.take_decided();
    plane.on_cycle(now(start));
    exchange(&mut plane, &service, start).await;
    for _ in 0..3 {
        assert_eq!(plane.decide(b"k", contract(), now(start)), Decision::Admit);
    }
    plane.on_cycle(now(start));
    exchange(&mut plane, &service, start).await;
    let (owner, secondary) = view().owners(b"k");
    let (owner, secondary) = (owner.unwrap() as usize, secondary.unwrap() as usize);
    assert_eq!(counted(&service.pods[owner], b"k", 4).await, Some(4));

    service.pods[owner].restart(2).await;
    assert_eq!(
        service.pods[owner].counted(b"k").await,
        None,
        "it holds nothing"
    );
    let mut admitted = 4;
    let deadline = Instant::now() + Duration::from_secs(5);
    // Keeps admitting while the pods agree, until the restarted owner serves again and
    // counts everything.
    loop {
        if plane.decide(b"k", contract(), now(start)) == Decision::Admit {
            admitted += 1;
        }
        admitted += plane.take_decided().iter().filter(|(_, a)| *a).count() as u64;
        plane.on_cycle(now(start));
        send(&mut plane, &service, start).await;
        let serving = service.pods[owner]
            .view()
            .await
            .is_some_and(|(_, serving)| serving);
        if serving && service.pods[owner].counted(b"k").await == Some(admitted) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "never back: secondary {:?}, owner {:?} of {admitted}",
            service.pods[secondary].counted(b"k").await,
            service.pods[owner].counted(b"k").await
        );
        tokio::time::sleep(CYCLE).await;
    }
    for _ in 0..20 {
        plane.on_cycle(now(start));
        send(&mut plane, &service, start).await;
        tokio::time::sleep(CYCLE).await;
    }
    assert_eq!(
        service.pods[owner].counted(b"k").await,
        Some(admitted),
        "nothing counted twice"
    );
}
