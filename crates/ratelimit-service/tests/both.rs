//! Both modes on one set of pods, on sockets on localhost (06 §4): pods of both modes formed by
//! the control plane as each mode's alone are (`tests/cluster.rs`, `tests/asynchronous.rs`), a
//! synchronous data plane asking on its stream (`Decide`) and an asynchronous one sending its
//! batches on its own (`Exchange`), to the same pods. Each is served by its keys' owner, the
//! owner's changes of both modes reach the shard's replicas' copies, each mode's keys are
//! counted apart, and a key whose bit says the other mode than its stream's is not served.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::table::shard_of;
use edgerush_cluster::{
    Authority, Config, Key, Kind, Message as StoreMessage, NodeId, Table, Time,
};
use edgerush_cluster_asynchronous::{Batch, Entry, Message as AsyncMessage, Unserved, Why};
use edgerush_cluster_modes::{Pod, Value};
use edgerush_cluster_synchronous::{Answer, Message as SyncMessage};
use edgerush_ratelimit::service::{Contract, Report};
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{
    DecideStream, ExchangeStream, StorePod, StoreServer, TICK, ask, decide_stream, exchange_stream,
};
use edgerush_ratelimit_service::cluster::wire::asynchronous::{answered_from, batch_to_wire};
use edgerush_ratelimit_service::cluster::wire::{asking, told_from};
use edgerush_ratelimit_service::grpc::Client;
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

/// How long a cluster takes to form, as the simulation holds it to.
const FORMED: Duration = Duration::from_secs(3);

/// How long a data plane waits for answers: ample on localhost.
const ANSWERED: Duration = Duration::from_secs(1);

/// How long an owner's change takes to reach its replicas' copies: the store's copy cycle
/// (10 ms) many times over.
const COPIED: Duration = Duration::from_secs(1);

/// Fewer shards than the store's default, for keys found by their owner.
const SHARDS: u16 = 16;

/// The asynchronous data plane the tests send as.
const PLANE: u64 = 77;

fn config() -> Config {
    Config {
        shards: SHARDS,
        ..Config::defaults()
    }
}

/// 100 a second, a bucket of ten.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(10),
    }
}

/// Pods of both modes on localhost, and the control plane forming them.
struct Cluster {
    pods: BTreeMap<NodeId, (StorePod<Pod>, String, JoinHandle<()>)>,
    control: JoinHandle<()>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.control.abort();
        for (_, _, server) in self.pods.values() {
            server.abort();
        }
    }
}

impl Cluster {
    /// Pods 1 to `count`, formed by the control plane into one table, each ready.
    async fn ready(count: NodeId) -> Self {
        let running = Arc::new(Mutex::new(BTreeMap::new()));
        let control = tokio::spawn(control_plane(Arc::clone(&running)));
        let mut cluster = Self {
            pods: BTreeMap::new(),
            control,
        };
        for id in 1..=count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let mut mode = Pod::new(id, address.clone(), config(), []);
            mode.set_life(1_000 + id);
            mode.set_forwarding(true);
            let pod = StorePod::start(mode, address.clone(), Dial::Plain, Vec::new());
            let server = StoreServer::new(pod.clone());
            let server = tokio::spawn(async move {
                let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
                let _ended = tonic::transport::Server::builder()
                    .serve_with_incoming(server, incoming)
                    .await;
            });
            cluster.pods.insert(id, (pod, address.clone(), server));
            running.lock().unwrap().insert(id, address);
        }
        let ready = within(FORMED, || async {
            for (pod, _, _) in cluster.pods.values() {
                if !pod.ready().await {
                    return None;
                }
            }
            Some(())
        })
        .await;
        assert!(
            ready.is_some(),
            "pods 1 to {count} not ready within {FORMED:?}"
        );
        cluster
    }

    fn pod(&self, id: NodeId) -> &StorePod<Pod> {
        &self.pods[&id].0
    }

    fn address(&self, id: NodeId) -> &str {
        &self.pods[&id].1
    }
}

/// The control plane: an `Authority` ticked as the simulation ticks it, asking the pods
/// running, each through a client of its own.
async fn control_plane(running: Arc<Mutex<BTreeMap<NodeId, String>>>) {
    let mut authority = Authority::new(&config());
    let started = Instant::now();
    let mut clients: BTreeMap<String, Client> = BTreeMap::new();
    let (answering, mut answers) = mpsc::unbounded_channel();
    let mut ticks = tokio::time::interval(TICK);
    loop {
        tokio::select! {
            _ = ticks.tick() => {
                let now = Time::try_from(started.elapsed().as_nanos()).unwrap();
                let running = running.lock().unwrap().clone();
                for (to, message) in authority.tick::<Value>(now, &running) {
                    let Some(address) = running.get(&to) else {
                        continue;
                    };
                    let client = clients
                        .entry(address.clone())
                        .or_insert_with(|| Client::new(address).unwrap())
                        .clone();
                    let answering = answering.clone();
                    tokio::spawn(async move {
                        if let Ok((_, Some(answer))) = ask::<Value>(&client, message).await {
                            let _ended = answering.send((to, answer));
                        }
                    });
                }
            }
            Some((from, answer)) = answers.recv() => authority.receive(from, answer),
        }
    }
}

/// Asks `check` every tick until it says something, for at most `most`.
async fn within<T, F: Future<Output = Option<T>>>(
    most: Duration,
    mut check: impl FnMut() -> F,
) -> Option<T> {
    let until = Instant::now() + most;
    loop {
        if let Some(found) = check().await {
            return Some(found);
        }
        if Instant::now() >= until {
            return None;
        }
        tokio::time::sleep(TICK).await;
    }
}

/// The first key of mode `kind` that `table` has `owner` own.
fn key_owned_by(table: &Table, owner: NodeId, kind: Kind) -> Key {
    (0..)
        .map(|key| kind.key(key))
        .find(|key| table.owner(shard_of(*key, SHARDS)) == Some(owner))
        .unwrap()
}

/// What `pod` holds under `key`, owned or a copy: its count, net of what was given back, if
/// the synchronous mode's; its hits counted, if the asynchronous mode's.
async fn held_at(pod: &StorePod<Pod>, key: Key) -> Option<(Kind, u64)> {
    pod.with(move |node, _| match node.value(key)? {
        Value::Synchronous(value) => Some((Kind::Synchronous, value.net())),
        Value::Asynchronous(value) => Some((Kind::Asynchronous, value.counted())),
        Value::Fresh => None,
    })
    .await
    .flatten()
}

/// A synchronous data plane's stream to pod `id`, its first word the pod's table.
async fn deciding(cluster: &Cluster, id: NodeId) -> (DecideStream, Table) {
    let mut stream = decide_stream(cluster.address(id), &Dial::Plain)
        .await
        .unwrap();
    let first = decided(&mut stream).await;
    let Some(SyncMessage::Store(StoreMessage::TableReply { table })) = first else {
        panic!("first told {first:?}");
    };
    (stream, table)
}

/// What a synchronous data plane's `stream` is told next.
async fn decided(stream: &mut DecideStream) -> Option<SyncMessage> {
    let next = tokio::time::timeout(ANSWERED, stream.down.message())
        .await
        .ok()?
        .ok()??;
    told_from(next).ok()
}

/// The answer a synchronous data plane's `stream` is told to its request `id`, the tables
/// told meanwhile passed over.
async fn decided_answer(stream: &mut DecideStream, id: u64) -> Option<Answer> {
    loop {
        if let SyncMessage::Reply {
            id: answered,
            answer,
            ..
        } = decided(stream).await?
            && answered == id
        {
            return Some(answer);
        }
    }
}

/// A synchronous data plane's request `id`, counting one under `key`.
fn request(id: u64, key: Key, table: &Table) -> SyncMessage {
    SyncMessage::Request {
        client: 0,
        id,
        key,
        stamp: table.stamp(),
        hops: 0,
        via: None,
        give_back: false,
        rule: None,
        receipt: 0,
    }
}

/// An asynchronous data plane's stream to pod `id`, its first word the pod's table.
async fn exchanging(cluster: &Cluster, id: NodeId) -> (ExchangeStream, Table) {
    let mut stream = exchange_stream(cluster.address(id), &Dial::Plain)
        .await
        .unwrap();
    let first = exchanged(&mut stream).await;
    let Some(AsyncMessage::Store(StoreMessage::TableReply { table })) = first else {
        panic!("first told {first:?}");
    };
    (stream, table)
}

/// What an asynchronous data plane's `stream` is told next.
async fn exchanged(stream: &mut ExchangeStream) -> Option<AsyncMessage> {
    let next = tokio::time::timeout(ANSWERED, stream.down.message())
        .await
        .ok()?
        .ok()??;
    answered_from(next).ok()
}

/// The pod's own answers an asynchronous data plane's `stream` is told next, the tables told
/// meanwhile passed over.
async fn exchanged_answers(
    stream: &mut ExchangeStream,
) -> Option<edgerush_cluster_asynchronous::Answers> {
    loop {
        if let AsyncMessage::Answers(answers) = exchanged(stream).await? {
            return Some(answers);
        }
    }
}

/// The data plane's batch of `count` hits under `key`, its table `table`'s.
fn batch(table: &Table, key: Key, count: u64) -> Batch {
    Batch {
        from: PLANE,
        stamp: table.stamp(),
        made: 5,
        cycle: 10_000_000,
        entries: vec![Entry {
            key,
            contract: contract(),
            report: Report::Hits {
                sequence: 1,
                count,
                seen: 0,
                age: 0,
            },
        }],
    }
}

/// A synchronous and an asynchronous data plane asking one set of pods are each answered by
/// their keys' owner; the owner's changes of both modes reach the shard's replicas' copies,
/// each copy its key's mode, and the pod counts each mode's keys apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_modes_data_planes_are_served_by_one_set_of_pods() {
    let cluster = Cluster::ready(3).await;
    let (mut deciding, table) = deciding(&cluster, 1).await;
    let (mut exchanging, _) = exchanging(&cluster, 1).await;
    let synchronous = key_owned_by(&table, 1, Kind::Synchronous);
    let asynchronous = key_owned_by(&table, 1, Kind::Asynchronous);
    deciding
        .up
        .send(asking(request(7, synchronous, &table)).unwrap())
        .await
        .unwrap();
    exchanging
        .up
        .send(batch_to_wire(batch(&table, asynchronous, 3)))
        .await
        .unwrap();
    assert_eq!(
        decided_answer(&mut deciding, 7).await,
        Some(Answer::Counted(1))
    );
    let answers = exchanged_answers(&mut exchanging).await.unwrap();
    assert!(answers.unserved.is_empty(), "{answers:?}");
    assert_eq!(answers.entries.len(), 1, "{answers:?}");
    assert_eq!(answers.entries[0].key, asynchronous);
    assert_eq!(answers.entries[0].answered.applied, 1);
    for (key, held) in [
        (synchronous, (Kind::Synchronous, 1)),
        (asynchronous, (Kind::Asynchronous, 3)),
    ] {
        let shard = &table.shards[usize::from(shard_of(key, SHARDS))];
        assert_eq!(shard.owner, Some(1));
        for id in std::iter::once(1).chain(shard.replicas.iter().copied()) {
            let copied = within(COPIED, || async {
                (held_at(cluster.pod(id), key).await == Some(held)).then_some(())
            })
            .await;
            assert!(
                copied.is_some(),
                "pod {id}'s copy of key {key:#x}: {held:?}"
            );
        }
    }
    let counted = cluster.pod(1).observed().await.unwrap();
    assert_eq!(
        (
            counted.mode.synchronous.keys,
            counted.mode.asynchronous.keys
        ),
        (1, 1),
        "{counted:?}"
    );
    assert_eq!(counted.keys, 2, "{counted:?}");
    assert_eq!(counted.mode.wrong_kind, 0, "{counted:?}");
}

/// A key whose bit says the other mode than its stream's is not served, nothing held under
/// it: a synchronous request under an asynchronous key is answered not serving, an entry of
/// a batch under a synchronous key unserved; and the pod counts both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_of_the_other_mode_than_its_streams_is_not_served() {
    let cluster = Cluster::ready(3).await;
    let (mut deciding, table) = deciding(&cluster, 1).await;
    let (mut exchanging, _) = exchanging(&cluster, 1).await;
    let asynchronous = key_owned_by(&table, 1, Kind::Asynchronous);
    let synchronous = key_owned_by(&table, 1, Kind::Synchronous);
    deciding
        .up
        .send(asking(request(7, asynchronous, &table)).unwrap())
        .await
        .unwrap();
    assert_eq!(
        decided_answer(&mut deciding, 7).await,
        Some(Answer::NotServing)
    );
    exchanging
        .up
        .send(batch_to_wire(batch(&table, synchronous, 3)))
        .await
        .unwrap();
    let answers = exchanged_answers(&mut exchanging).await.unwrap();
    assert!(answers.entries.is_empty(), "{answers:?}");
    assert_eq!(
        answers.unserved,
        vec![Unserved {
            key: synchronous,
            why: Why::NotServing
        }]
    );
    for key in [asynchronous, synchronous] {
        assert_eq!(held_at(cluster.pod(1), key).await, None, "{key:#x}");
    }
    let counted = cluster.pod(1).observed().await.unwrap();
    assert_eq!(counted.mode.wrong_kind, 2, "{counted:?}");
    assert_eq!(counted.keys, 0, "{counted:?}");
}
