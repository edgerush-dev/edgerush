//! The asynchronous mode on the cluster store, on sockets on localhost (cluster-store-transplant-
//! plan.md, "A2"): pods formed by the control plane as the synchronous mode's are
//! (`tests/cluster.rs`), each given a loopback IP of its own behind a relay the test holds, and
//! data planes sending their batches on a stream of their own (`Exchange`), as A3's data plane
//! will. A batch is applied by its keys' owner and answered; one reaching a pod not its keys'
//! owner is forwarded to it and the answers relayed back, the hits counted once; the owner's
//! changes reach its replicas' copies; an owner not serving turns the batch away and counts it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::table::shard_of;
use edgerush_cluster::{
    Authority, Config, Message as StoreMessage, NodeId, Stamp, Table, Time, ms,
};
use edgerush_cluster_asynchronous::{
    Answers, Batch, Entry, Message, Pod, Turned, TurnedAway, Unserved, Value, Why,
};
use edgerush_ratelimit::service::{Contract, Report};
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{
    ExchangeStream, StoreClient, StorePod, StoreServer, TICK, ask, exchange_stream,
};
use edgerush_ratelimit_service::cluster::wire::asynchronous::{answered_from, batch_to_wire};
use edgerush_ratelimit_service::grpc::Client;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

/// How long a cluster takes to form, as the simulation holds it to.
const FORMED: Duration = Duration::from_secs(3);

/// How long a pod cut off takes to stop serving, its lease gone.
const FAILOVER: Duration = Duration::from_secs(1);

/// How long a data plane waits for answers: ample on localhost.
const ANSWERED: Duration = Duration::from_secs(1);

/// How long an owner's change takes to reach its replicas' copies: the store's copy cycle
/// (10 ms) many times over.
const COPIED: Duration = Duration::from_secs(1);

/// The data planes' cycle, as the pods are told it.
const CYCLE: Time = ms(10);

/// Fewer shards than the store's default, for tests that read every one.
const SHARDS: u16 = 16;

/// The data plane the tests send as.
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

/// The IP pod `id` is given: one of loopback's own, apart from the 127.0.0.1 the control plane
/// and the data planes call from.
fn ip_of(id: NodeId) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 40 + u8::try_from(id).unwrap()))
}

/// A relay in front of one pod, dropping its connections, to it and from other pods, once it
/// is cut off.
struct Relay {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Relay {
    async fn to(ip: IpAddr, to: SocketAddr, cut: watch::Receiver<BTreeSet<IpAddr>>) -> Self {
        let listener = TcpListener::bind((ip, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let dropped =
            move |cut: &BTreeSet<IpAddr>, from: IpAddr| cut.contains(&ip) || cut.contains(&from);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, from)) = listener.accept().await else {
                    continue;
                };
                if dropped(&cut.borrow(), from.ip()) {
                    continue;
                }
                let mut cut = cut.clone();
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(to).await else {
                        return;
                    };
                    // As the pods' own sockets are: Nagle's algorithm against a delayed ACK
                    // holds a small message back for up to 40 ms on Linux.
                    let _nodelay = inbound.set_nodelay(true);
                    let _nodelay = outbound.set_nodelay(true);
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = cut.wait_for(|cut| dropped(cut, from.ip())) => {}
                    }
                });
            }
        });
        Self { address, task }
    }
}

/// A pod running, its server and its relay.
struct Running {
    pod: StorePod<Pod>,
    server: JoinHandle<()>,
    relay: Relay,
}

/// Pods on localhost and the control plane forming them.
struct Cluster {
    pods: BTreeMap<NodeId, Running>,
    cut: watch::Sender<BTreeSet<IpAddr>>,
    control: JoinHandle<()>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.control.abort();
        for running in self.pods.values() {
            running.server.abort();
            running.relay.task.abort();
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
            cut: watch::channel(BTreeSet::new()).0,
            control,
        };
        for id in 1..=count {
            let ip = ip_of(id);
            let listener = TcpListener::bind((ip, 0)).await.unwrap();
            let relay =
                Relay::to(ip, listener.local_addr().unwrap(), cluster.cut.subscribe()).await;
            let address = relay.address.to_string();
            let mut mode = Pod::new(id, address.clone(), config(), [], CYCLE);
            mode.set_life(1_000 + id);
            let pod = StorePod::start(mode, address.clone(), Dial::Plain, Vec::new());
            let server = StoreServer::new(pod.clone());
            let server = tokio::spawn(async move {
                let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
                let _ended = tonic::transport::Server::builder()
                    .serve_with_incoming(server, incoming)
                    .await;
            });
            cluster.pods.insert(id, Running { pod, server, relay });
            running.lock().unwrap().insert(id, address);
        }
        let ready = within(FORMED, || async {
            for running in cluster.pods.values() {
                if !running.pod.ready().await {
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
        &self.pods[&id].pod
    }

    fn address(&self, id: NodeId) -> String {
        self.pods[&id].relay.address.to_string()
    }

    /// Cuts pod `id` off from every other, both ways, and from data planes' streams, leaving
    /// it running.
    fn cut_off(&self, id: NodeId) {
        self.cut.send_modify(|cut| {
            cut.insert(ip_of(id));
        });
    }
}

/// The table `pod` has committed.
async fn committed(pod: &StorePod<Pod>) -> Option<Table> {
    pod.with(|node, _| node.committed().cloned())
        .await
        .flatten()
}

/// The hits `pod` holds counted under `key`, owned or a copy.
async fn counted_at(pod: &StorePod<Pod>, key: u64) -> Option<u64> {
    pod.with(move |node, _| node.value(key).as_ref().map(Value::counted))
        .await
        .flatten()
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

/// The first key `table` has `owner` own.
fn key_owned_by(table: &Table, owner: NodeId) -> u64 {
    (0..)
        .find(|key| table.owner(shard_of(*key, SHARDS)) == Some(owner))
        .unwrap()
}

/// The data plane's batch `sequence` of `count` hits under `key`, its table `stamp`.
fn batch(stamp: Stamp, key: u64, sequence: u64, count: u64) -> Batch {
    Batch {
        from: PLANE,
        stamp,
        made: 5,
        entries: vec![Entry {
            key,
            contract: contract(),
            report: Report::Hits {
                sequence,
                count,
                seen: sequence - 1,
                age: 0,
            },
        }],
    }
}

/// What a data plane's `stream` is told next, within `most`.
async fn told(stream: &mut ExchangeStream, most: Duration) -> Option<Message> {
    let next = tokio::time::timeout(most, stream.down.message())
        .await
        .ok()?
        .ok()??;
    answered_from(next).ok()
}

/// The answers a data plane's `stream` is told next, within `most`, the tables told meanwhile
/// passed over: the pod's own, or an owner's relayed.
async fn answers(stream: &mut ExchangeStream, most: Duration) -> Option<Told> {
    let until = Instant::now() + most;
    loop {
        let left = until.saturating_duration_since(Instant::now());
        match told(stream, left).await? {
            Message::Answers(answers) => return Some(Told::Own(answers)),
            Message::Relayed { owner, answers, .. } => {
                return Some(Told::Relayed { owner, answers });
            }
            _ => {}
        }
    }
}

/// Answers a data plane is told.
#[derive(Debug)]
enum Told {
    /// The pod's own.
    Own(Answers),
    /// An owner's, relayed by the pod.
    Relayed { owner: NodeId, answers: Answers },
}

impl Told {
    /// The pod's own answers; a panic if they were relayed.
    fn own(self) -> Answers {
        match self {
            Self::Own(answers) => answers,
            Self::Relayed { .. } => panic!("relayed, not the pod's own: {self:?}"),
        }
    }
}

/// A data plane's stream to pod `id`, its first word the pod's table: the stream and the table.
async fn opened(cluster: &Cluster, id: NodeId) -> (ExchangeStream, Table) {
    let mut stream = exchange_stream(&cluster.address(id), &Dial::Plain)
        .await
        .unwrap();
    let first = told(&mut stream, ANSWERED).await;
    let Some(Message::Store(StoreMessage::TableReply { table })) = first else {
        panic!("first told {first:?}");
    };
    (stream, table)
}

/// A data plane's stream is told the pod's table as it opens; its batch is applied by the
/// keys' owner and answered, numbered in the pod's life, and the owner's change reaches the
/// shard's replicas' copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_on_a_data_planes_stream_is_applied_answered_and_copied() {
    let cluster = Cluster::ready(3).await;
    let (mut stream, table) = opened(&cluster, 1).await;
    let named: BTreeSet<NodeId> = table.members.keys().copied().collect();
    assert_eq!(named, (1..=3).collect());
    let key = key_owned_by(&table, 1);
    let sent = batch(table.stamp(), key, 1, 3);
    stream.up.send(batch_to_wire(sent)).await.unwrap();
    let answered = answers(&mut stream, ANSWERED).await.unwrap().own();
    assert_eq!(answered.life, 1_001);
    assert_eq!(answered.made, 5, "said back");
    assert_eq!(answered.entries.len(), 1);
    assert_eq!(answered.entries[0].key, key);
    assert_eq!(answered.entries[0].answered.applied, 1);
    assert!(answered.unserved.is_empty());
    assert_eq!(counted_at(cluster.pod(1), key).await, Some(3));
    let shard = &table.shards[usize::from(shard_of(key, SHARDS))];
    for replica in shard.replicas.iter().filter(|replica| **replica != 1) {
        let copied = within(COPIED, || async {
            (counted_at(cluster.pod(*replica), key).await == Some(3)).then_some(())
        })
        .await;
        assert!(copied.is_some(), "pod {replica}'s copy of key {key}");
    }
}

/// A batch reaching a pod not its keys' owner is answered there, nothing of it applied, and
/// forwarded to the owner, whose answers come back on the same stream; sent to the owner as
/// well, as a data plane cut off from it sends, it is applied once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_at_a_pod_not_its_keys_owner_is_forwarded_and_answered_through_it() {
    let cluster = Cluster::ready(3).await;
    let (mut stream, table) = opened(&cluster, 2).await;
    let key = key_owned_by(&table, 1);
    let sent = batch(table.stamp(), key, 1, 4);
    stream.up.send(batch_to_wire(sent.clone())).await.unwrap();
    // The forwarder's own, and the owner's relayed with the owner, each numbered in its pod's
    // life, in either order.
    let (mut own, mut relayed) = (None, None);
    for _ in 0..2 {
        match answers(&mut stream, ANSWERED).await.unwrap() {
            Told::Own(answers) => own = Some(answers),
            Told::Relayed { owner, answers } => relayed = Some((owner, answers)),
        }
    }
    let own = own.unwrap();
    assert_eq!(own.life, 1_002);
    assert!(own.entries.is_empty() && own.unserved.is_empty(), "{own:?}");
    let (owner, relayed) = relayed.unwrap();
    assert_eq!(owner, 1, "relayed as the owner's");
    assert_eq!(relayed.life, 1_001);
    assert_eq!(relayed.made, 5);
    assert_eq!(relayed.entries.len(), 1);
    assert_eq!(relayed.entries[0].answered.applied, 1);
    assert_eq!(counted_at(cluster.pod(1), key).await, Some(4));
    let (mut direct, _) = opened(&cluster, 1).await;
    direct.up.send(batch_to_wire(sent)).await.unwrap();
    let again = answers(&mut direct, ANSWERED).await.unwrap().own();
    assert_eq!(again.entries[0].answered.applied, 1);
    assert_eq!(
        counted_at(cluster.pod(1), key).await,
        Some(4),
        "applied once"
    );
}

/// A batch from a data plane holding an older table is answered with the pod's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_behind_the_pods_table_is_answered_with_it() {
    let cluster = Cluster::ready(3).await;
    let (mut stream, table) = opened(&cluster, 1).await;
    let key = key_owned_by(&table, 1);
    stream
        .up
        .send(batch_to_wire(batch((0, 0), key, 1, 1)))
        .await
        .unwrap();
    let answered = answers(&mut stream, ANSWERED).await.unwrap().own();
    assert_eq!(answered.table.map(|told| told.stamp()), Some(table.stamp()));
}

/// An owner cut off, its lease gone, applies nothing of a batch and answers it not serving,
/// and counts what it turned away; its stream's client asks a batch alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_cut_off_turns_a_batch_away_and_counts_it() {
    let cluster = Cluster::ready(3).await;
    let table = committed(cluster.pod(2)).await.unwrap();
    let key = key_owned_by(&table, 2);
    cluster.cut_off(2);
    let unready = within(FAILOVER, || async {
        (!cluster.pod(2).ready().await).then_some(())
    })
    .await;
    assert!(unready.is_some(), "pod 2 still ready");
    let mut client: StoreClient<Pod> = cluster.pod(2).client().await.unwrap();
    // Not a client's to ask: dropped, so that nothing of it is turned away and counted.
    let forwarded = Message::Forwarded {
        client: client.id(),
        via: 3,
        batch: batch(table.stamp(), key, 1, 2),
    };
    assert!(client.ask(forwarded).await);
    client
        .ask(Message::Batch(batch(table.stamp(), key, 1, 2)))
        .await;
    let answered = loop {
        let told = tokio::time::timeout(ANSWERED, client.answer())
            .await
            .unwrap()
            .unwrap();
        if let Message::Answers(answers) = told {
            break answers;
        }
    };
    assert!(answered.entries.is_empty());
    assert_eq!(
        answered.unserved,
        vec![Unserved {
            key,
            why: Why::NotServing
        }]
    );
    assert_eq!(counted_at(cluster.pod(2), key).await, None);
    let observed = cluster.pod(2).observed().await.unwrap();
    assert_eq!(
        observed.mode,
        TurnedAway {
            elsewhere: Turned::default(),
            not_serving: Turned {
                entries: 1,
                hits: 2
            },
        }
    );
}
