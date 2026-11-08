//! The cluster store on sockets on localhost (cluster-store-transplant-plan.md, "T4d"): each pod
//! given a loopback IP of its own and reached through a relay the test holds there, which is
//! the address it gives. Cutting a pod off is the relays' doing: each drops the connections
//! of a pod cut off, to it and from it, known by its IP, as pods dial from theirs. The control
//! plane's `Authority` runs in-process, asking the pods as the controller will. Each scenario
//! is the simulation's, held to its bound; a rollout's, as T4d2 has it. Clients ask the pods'
//! synchronous mode in-process, through a `StoreClient` (T5a), and data planes on a stream of
//! their own (T5c), as a data plane drives them by its `Plane` (T6a); and pods formed by
//! `Forming`, as the bench's example program forms them (T6b).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::table::shard_of;
use edgerush_cluster::{
    Authority, Changed, Config, Kept, Message as StoreMessage, Node, NodeId, Stamp, Table, Time,
    generation_of, ms,
};
use edgerush_cluster_synchronous::{Answer, Message as ModeMessage, Rule, Value};
use edgerush_ratelimit_service::cluster::forming::{self, Forming};
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::plane::{Plane, Told};
use edgerush_ratelimit_service::cluster::pod::{
    DecideStream, STREAMS_IDLE, StoreClient, StorePod, StoreServer, TICK, ask, decide_stream,
};
use edgerush_ratelimit_service::cluster::wire::{Asking, asking, told_from};
use edgerush_ratelimit_service::grpc::Client;
use edgerush_ratelimit_service::scrape;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

/// Held by each test while it runs: alone by those held to timings of tens of milliseconds
/// (a rollout's unserved shards, a gathering's parts), shared by the rest. The tests of a
/// binary run at once, each with pods of its own, and on a machine of few cores those
/// alongside stretch what the tight ones measure past their bounds.
static ALONE: LazyLock<tokio::sync::RwLock<()>> = LazyLock::new(|| tokio::sync::RwLock::new(()));

/// How long a cluster takes to form, as the simulation holds it to.
const FORMED: Duration = Duration::from_secs(3);

/// How long a pod's shards take to fail over once it is gone, as the simulation holds it to.
const FAILOVER: Duration = Duration::from_secs(1);

/// How long a shard may go unserved in a rollout (decided): under the 50 ms a taker waits for
/// the old owner's last changes, so that a pod stopping without sending them fails it.
const UNSERVED: Duration = Duration::from_millis(50);

/// How long a pod told to drain may take to stop: under [`STREAMS_IDLE`], so that one whose
/// streams never end fails it.
const DRAINED: Duration = Duration::from_secs(3);
const _: () = assert!(DRAINED.as_secs() < STREAMS_IDLE.as_secs());

/// How long a client waits for an answer: ample on localhost.
const ANSWERED: Duration = Duration::from_secs(1);

/// How long a pod gathering a request under several keys waits for its parts before it answers
/// not counted, as the mode has it (15 ms), and a few ticks more.
const GATHERED: Duration = Duration::from_millis(15 + 4 * 5);

/// Fewer shards than the store's default, for tests that read every one.
const SHARDS: u16 = 16;

fn config() -> Config {
    Config {
        shards: SHARDS,
        ..Config::defaults()
    }
}

/// The IP pod `id` is given: one of loopback's own, apart from the 127.0.0.1 the control plane
/// calls from.
fn ip_of(id: NodeId) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 10 + u8::try_from(id).unwrap()))
}

/// Whether a relay in front of `ip` drops a connection from `from`, `cut` being the IPs of the
/// pods cut off.
fn dropped(cut: &BTreeSet<IpAddr>, ip: IpAddr, from: IpAddr) -> bool {
    cut.contains(&ip) || cut.contains(&from)
}

/// A relay in front of one pod.
struct Relay {
    address: SocketAddr,
    /// Where it relays to: its pod's listener, another's once the pod is restarted in place.
    /// Each change, or a mark of one, closes what it relays, as a process ending closes its
    /// connections.
    target: watch::Sender<SocketAddr>,
    /// The connections it has been dialled from pods, taken or dropped.
    dialled: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl Relay {
    /// A relay at `ip` to `to`, dropping what `cut` says, and closing what it relays once it
    /// says so.
    async fn to(ip: IpAddr, to: SocketAddr, cut: watch::Receiver<BTreeSet<IpAddr>>) -> Self {
        let listener = TcpListener::bind((ip, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let dialled = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&dialled);
        let (target, targeted) = watch::channel(to);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, from)) = listener.accept().await else {
                    continue;
                };
                if from.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) {
                    counted.fetch_add(1, Ordering::Relaxed);
                }
                if dropped(&cut.borrow(), ip, from.ip()) {
                    continue;
                }
                let mut cut = cut.clone();
                let mut targeted = targeted.clone();
                let to = *targeted.borrow_and_update();
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
                        _ = cut.wait_for(|cut| dropped(cut, ip, from.ip())) => {}
                        _ = targeted.changed() => {}
                    }
                });
            }
        });
        Self {
            address,
            target,
            dialled,
            task,
        }
    }
}

/// A pod's server's connections from `listener`, without Nagle's algorithm, as the service's
/// own listener takes them.
fn incoming(listener: TcpListener) -> TcpIncoming {
    TcpIncoming::from(listener).with_nodelay(Some(true))
}

/// A pod running, its server and its relay.
struct Running {
    pod: StorePod,
    server: JoinHandle<()>,
    relay: Relay,
}

/// Pods on localhost and the control plane forming them.
struct Cluster {
    pods: BTreeMap<NodeId, Running>,
    cut: watch::Sender<BTreeSet<IpAddr>>,
    /// What the API shows running, each pod with its address.
    running: Arc<Mutex<BTreeMap<NodeId, String>>>,
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
    fn new() -> Self {
        let running = Arc::new(Mutex::new(BTreeMap::new()));
        let control = tokio::spawn(control_plane(Arc::clone(&running)));
        Self {
            pods: BTreeMap::new(),
            cut: watch::channel(BTreeSet::new()).0,
            running,
            control,
        }
    }

    /// Starts pod `id` with `config`, asking `seeds`, and shows it running: its address.
    async fn start(&mut self, id: NodeId, config: Config, seeds: Vec<String>) -> String {
        let address = self.launch(id, config, seeds).await;
        self.show(id, &address);
        address
    }

    /// Shows pod `id`, at `address`, running, as the API would.
    fn show(&self, id: NodeId, address: &str) {
        self.running.lock().unwrap().insert(id, address.to_owned());
    }

    /// Starts pod `id` with `config`, asking `seeds`, not yet shown running: its address.
    async fn launch(&mut self, id: NodeId, config: Config, seeds: Vec<String>) -> String {
        let ip = ip_of(id);
        let listener = TcpListener::bind((ip, 0)).await.unwrap();
        let relay = Relay::to(ip, listener.local_addr().unwrap(), self.cut.subscribe()).await;
        let address = relay.address.to_string();
        let pod = StorePod::spawn(id, address.clone(), config, Dial::Plain, seeds);
        let server = StoreServer::new(pod.clone());
        let server = tokio::spawn(async move {
            let _ended = tonic::transport::Server::builder()
                .serve_with_incoming(server, incoming(listener))
                .await;
        });
        self.pods.insert(id, Running { pod, server, relay });
        address
    }

    /// Restarts pod `id` in place as member `new`, as a StatefulSet's pod restarts: its
    /// process ended and another started behind the same address, asking no seeds, its relay
    /// passing connections to the new one; not shown running.
    async fn restart_in_place(&mut self, id: NodeId, new: NodeId) {
        let old = self.pods.remove(&id).unwrap();
        old.pod.stop().await;
        old.server.abort();
        let listener = TcpListener::bind((ip_of(id), 0)).await.unwrap();
        old.relay
            .target
            .send_replace(listener.local_addr().unwrap());
        let address = old.relay.address.to_string();
        let pod = StorePod::spawn(new, address, config(), Dial::Plain, Vec::new());
        let server = StoreServer::new(pod.clone());
        let server = tokio::spawn(async move {
            let _ended = tonic::transport::Server::builder()
                .serve_with_incoming(server, incoming(listener))
                .await;
        });
        let relay = old.relay;
        self.pods.insert(new, Running { pod, server, relay });
    }

    /// Ends pod `id` with its relay, as a pod deleted: its address refuses connections.
    async fn delete(&mut self, id: NodeId) {
        let old = self.pods.remove(&id).unwrap();
        old.pod.stop().await;
        old.server.abort();
        old.relay.task.abort();
        old.relay.target.send_modify(|_| {});
    }

    /// Starts pods 1 to `count`, shown running together, and waits for the control plane to
    /// form them into one table.
    async fn formed(count: NodeId) -> Self {
        let mut cluster = Self::new();
        let mut addresses = Vec::new();
        for id in 1..=count {
            addresses.push((id, cluster.launch(id, config(), Vec::new()).await));
        }
        for (id, address) in addresses {
            cluster.show(id, &address);
        }
        let ids: BTreeSet<NodeId> = (1..=count).collect();
        let formed = within(FORMED, || cluster.one_table(&ids)).await;
        assert!(
            formed.is_some(),
            "pods 1 to {count} not formed within {FORMED:?}"
        );
        cluster
    }

    fn pod(&self, id: NodeId) -> &StorePod {
        &self.pods[&id].pod
    }

    /// Stops pod `id` as a crash does: its store, its server and every connection to it, its
    /// relay answering what is dialled to it by dropping it, and the API showing it gone.
    async fn crash(&mut self, id: NodeId) {
        self.pod(id).stop().await;
        self.gone(id);
    }

    /// Ends pod `id`'s process, its store stopped: its server and every connection to it,
    /// its relay answering what is dialled to it by dropping it, and the API showing it gone.
    fn gone(&self, id: NodeId) {
        self.pods[&id].server.abort();
        self.cut.send_modify(|cut| {
            cut.insert(ip_of(id));
        });
        self.running.lock().unwrap().remove(&id);
    }

    /// Cuts pod `id` off from every other, both ways, leaving it running.
    fn cut_off(&self, id: NodeId) {
        self.cut.send_modify(|cut| {
            cut.insert(ip_of(id));
        });
    }

    /// The table every pod of `ids` has committed, if they have one and the same, naming
    /// those alone.
    async fn one_table(&self, ids: &BTreeSet<NodeId>) -> Option<Table> {
        let mut tables = Vec::new();
        for id in ids {
            tables.push(committed(self.pod(*id)).await?);
        }
        let first = tables.first()?.clone();
        let same = tables.iter().all(|table| table.stamp() == first.stamp());
        let named: BTreeSet<NodeId> = first.members.keys().copied().collect();
        (same && named == *ids).then_some(first)
    }
}

/// The table `pod` has committed.
async fn committed(pod: &StorePod) -> Option<Table> {
    pod.with(|node, _| node.committed().cloned())
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

/// The control plane: an `Authority` ticked as the simulation ticks it, asking the pods the
/// API shows running, each through a client of its own.
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

/// Whether every one of `ids` is ready, as the store has it.
async fn all_ready(cluster: &Cluster, ids: &BTreeSet<NodeId>) -> Option<()> {
    for id in ids {
        if !cluster.pod(*id).ready().await {
            return None;
        }
    }
    Some(())
}

/// The pod of `ids` that leads, by its own say.
async fn leader(cluster: &Cluster, ids: &BTreeSet<NodeId>) -> Option<NodeId> {
    for id in ids {
        if cluster.pod(*id).with(|node, _| node.is_leader()).await? {
            return Some(*id);
        }
    }
    None
}

/// The pod of `ids` that leads, once one says so: pods that have committed one table may
/// still be electing a leader.
async fn elected(cluster: &Cluster, ids: &BTreeSet<NodeId>) -> NodeId {
    within(FORMED, || leader(cluster, ids))
        .await
        .expect("no leader elected in time")
}

/// The keys the tests write: eight in every shard.
fn keys() -> Vec<u64> {
    (0..u64::from(SHARDS) * 8).collect()
}

/// A key's value, counted `counted`.
fn count(counted: u64) -> Value {
    Value::Count {
        window: 1,
        counted,
        given: 0,
    }
}

/// What a rollout's sampling saw: the longest any shard went unserved, and the last count
/// each key was written with at a pod serving it.
struct Sampled {
    unserved: Duration,
    written: BTreeMap<u64, u64>,
}

/// Round after round until `done`, asks each pod `live` holds which shards it serves, and
/// writes every key of those shards there, counted by the round.
async fn sample(
    live: Arc<Mutex<BTreeMap<NodeId, StorePod>>>,
    done: watch::Receiver<bool>,
) -> Sampled {
    let keys = Arc::new(keys());
    let mut served_at = [Instant::now(); SHARDS as usize];
    let mut sampled = Sampled {
        unserved: Duration::ZERO,
        written: BTreeMap::new(),
    };
    let mut round = 0;
    while !*done.borrow() {
        round += 1;
        let at = Instant::now();
        let pods: Vec<StorePod> = live.lock().unwrap().values().cloned().collect();
        let mut served = BTreeSet::new();
        for pod in pods {
            let keys = Arc::clone(&keys);
            let Some((serving, written)) = pod
                .with(move |node, now| write_served(node, now, &keys, round))
                .await
            else {
                continue;
            };
            served.extend(serving);
            for key in written {
                sampled.written.insert(key, round);
            }
        }
        for (shard, since) in served_at.iter_mut().enumerate() {
            if served.contains(&u16::try_from(shard).unwrap()) {
                *since = at;
            } else {
                sampled.unserved = sampled.unserved.max(at - *since);
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    sampled
}

/// The shards `node` serves at `now`, and the keys of `keys` it took, written there counted
/// `round`.
fn write_served(
    node: &mut Node<Value>,
    now: Time,
    keys: &[u64],
    round: u64,
) -> (Vec<u16>, Vec<u64>) {
    let serving: Vec<u16> = (0..SHARDS)
        .filter(|shard| node.serves(*shard, now))
        .collect();
    let written = keys
        .iter()
        .copied()
        .filter(|key| serving.contains(&shard_of(*key, SHARDS)))
        .filter(|key| {
            node.update(*key, now, |kept: &mut Kept<Value>, _| {
                kept.value = count(round);
                kept.forget_at = Time::MAX;
                ((), Changed::Whole)
            })
            .is_some()
        })
        .collect();
    (serving, written)
}

/// Replaces pods 1 to `pods`, `step` at a time, as a Deployment surging: `step` new pods
/// started, asking the pods running, and once all are ready, `step` old ones drained, each
/// gone once it has stopped; keys written throughout at the pods serving them. Held to what
/// the simulation's rollouts are: no shard unserved but for a handoff's swap, and every key
/// held after as last written.
async fn rollout(pods: NodeId, step: usize) {
    let mut cluster = Cluster::formed(pods).await;
    let olds: Vec<NodeId> = (1..=pods).collect();
    let all: BTreeSet<NodeId> = olds.iter().copied().collect();
    let ready = within(FORMED, || all_ready(&cluster, &all)).await;
    assert!(ready.is_some(), "pods not ready within {FORMED:?}");
    let live = Arc::new(Mutex::new(
        cluster
            .pods
            .iter()
            .map(|(id, running)| (*id, running.pod.clone()))
            .collect::<BTreeMap<_, _>>(),
    ));
    let (done, until_done) = watch::channel(false);
    let sampling = tokio::spawn(sample(Arc::clone(&live), until_done));
    let mut next = pods + 1;
    for going in olds.chunks(step) {
        let seeds: Vec<String> = live
            .lock()
            .unwrap()
            .keys()
            .map(|id| cluster.pods[id].relay.address.to_string())
            .collect();
        let mut new = BTreeSet::new();
        for _ in going {
            cluster.start(next, config(), seeds.clone()).await;
            live.lock().unwrap().insert(next, cluster.pod(next).clone());
            new.insert(next);
            next += 1;
        }
        let ready = within(FORMED, || all_ready(&cluster, &new)).await;
        assert!(ready.is_some(), "pods {new:?} not ready within {FORMED:?}");
        // Ready as the store has it: holding its share of shards, so that an old pod drained
        // next has pods to hand on to that have already taken theirs.
        for id in &new {
            let table = committed(cluster.pod(*id)).await.unwrap();
            let share = usize::from(SHARDS) / table.members.len();
            let held = table.owned_by(*id).count();
            assert!(
                held >= share,
                "pod {id} ready holding {held} of {share} shards"
            );
        }
        let drains: Vec<(NodeId, JoinHandle<()>)> = going
            .iter()
            .map(|id| {
                let pod = cluster.pod(*id).clone();
                (*id, tokio::spawn(async move { pod.drain().await }))
            })
            .collect();
        for (id, drain) in drains {
            let stopped = tokio::time::timeout(DRAINED, drain).await;
            assert!(stopped.is_ok(), "pod {id} not stopped within {DRAINED:?}");
            cluster.gone(id);
            live.lock().unwrap().remove(&id);
        }
    }
    let _sent = done.send(true);
    let sampled = sampling.await.unwrap();
    assert!(
        sampled.unserved <= UNSERVED,
        "a shard unserved for {:?}",
        sampled.unserved
    );
    let news: BTreeSet<NodeId> = live.lock().unwrap().keys().copied().collect();
    let settled = within(FORMED, || async {
        let table = cluster.one_table(&news).await?;
        owned_by(&cluster, &news).await?;
        table.moves.is_empty().then_some(table)
    })
    .await;
    let table = settled.expect("the new pods' table not settled");
    for key in keys() {
        let written = sampled.written.get(&key).copied();
        assert!(written.is_some(), "key {key} never written");
        let owner = table.owner(shard_of(key, SHARDS)).unwrap();
        let held = cluster
            .pod(owner)
            .with(move |node, _| node.value(key))
            .await
            .flatten();
        assert_eq!(held, written.map(count), "key {key} at {owner}");
    }
    // Gone from every table, the old pods are forgotten, and dialled no more.
    let forgotten = within(Duration::from_secs(10), || async {
        for id in &news {
            let knows = cluster
                .pod(*id)
                .with(move |node, _| node.peers().iter().any(|peer| *peer <= pods))
                .await?;
            if knows {
                return None;
            }
        }
        Some(())
    })
    .await;
    assert!(forgotten.is_some(), "old pods not forgotten");
    let dialled = || -> Vec<u64> {
        olds.iter()
            .map(|id| cluster.pods[id].relay.dialled.load(Ordering::Relaxed))
            .collect()
    };
    let before = dialled();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(dialled(), before, "old pods dialled once forgotten");
}

/// Pods 1 to `count`, formed and ready.
async fn ready(count: NodeId) -> Cluster {
    let cluster = Cluster::formed(count).await;
    let ids: BTreeSet<NodeId> = (1..=count).collect();
    let ready = within(FORMED, || all_ready(&cluster, &ids)).await;
    assert!(ready.is_some(), "pods not ready within {FORMED:?}");
    cluster
}

/// The first key `table` has `owner` own.
fn key_owned_by(table: &Table, owner: NodeId) -> u64 {
    (0..)
        .find(|key| table.owner(shard_of(*key, SHARDS)) == Some(owner))
        .unwrap()
}

/// A request under `key` from `client`, numbered `id`, its table `stamp`, held to `rule`.
const fn request(
    client: NodeId,
    id: u64,
    key: u64,
    stamp: Stamp,
    rule: Option<Rule>,
) -> ModeMessage {
    ModeMessage::Request {
        client,
        id,
        key,
        stamp,
        hops: 0,
        via: None,
        give_back: false,
        rule,
        receipt: 0,
    }
}

/// The answer `client` is given to its request `id`, within `most`.
async fn answered(client: &mut StoreClient, id: u64, most: Duration) -> Option<Answer> {
    let answer = async {
        loop {
            if let ModeMessage::Reply {
                id: answered,
                answer,
                ..
            } = client.answer().await?
                && answered == id
            {
                return Some(answer);
            }
        }
    };
    tokio::time::timeout(most, answer).await.ok().flatten()
}

/// What a data plane's `stream` is told next, within `most`.
async fn told(stream: &mut DecideStream, most: Duration) -> Option<ModeMessage> {
    let next = tokio::time::timeout(most, stream.down.message())
        .await
        .ok()?
        .ok()??;
    told_from(next).ok()
}

/// The answer a data plane's `stream` is told to its request `id`, within `most`, the tables
/// told meanwhile passed over.
async fn told_answer(stream: &mut DecideStream, id: u64, most: Duration) -> Option<Answer> {
    let until = Instant::now() + most;
    loop {
        let left = until.saturating_duration_since(Instant::now());
        match told(stream, left).await? {
            ModeMessage::Reply {
                id: answered,
                answer,
                ..
            } if answered == id => return Some(answer),
            _ => {}
        }
    }
}

/// What `pod` holds counted under `key`, less what was given back.
async fn held_at(pod: &StorePod, key: u64) -> Option<u64> {
    pod.with(move |node, _| node.value(key).map(|value| value.net()))
        .await
        .flatten()
}

/// Whether every one of `ids` has committed a table whose every shard is owned by one of
/// them.
async fn owned_by(cluster: &Cluster, ids: &BTreeSet<NodeId>) -> Option<()> {
    for id in ids {
        let table = committed(cluster.pod(*id)).await?;
        let owned =
            (0..SHARDS).all(|shard| table.owner(shard).is_some_and(|owner| ids.contains(&owner)));
        if !owned {
            return None;
        }
    }
    Some(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_pods_are_formed_by_the_control_plane_into_one_table() {
    let _shared = ALONE.read().await;
    let cluster = Cluster::formed(3).await;
    let ids = (1..=3).collect();
    // Read again, as one: tables published as shards move reach the pods a moment apart.
    let table = within(FORMED, || cluster.one_table(&ids)).await.unwrap();
    assert!((0..SHARDS).all(|shard| table.owner(shard).is_some()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Given one pod as its seed, a new pod learns where the others are from the table, as they
/// learn where it is, and is taken in and ready.
async fn a_pod_given_a_seed_is_taken_in() {
    let mut cluster = Cluster::formed(3).await;
    let seed = cluster.pods[&1].relay.address.to_string();
    cluster.start(4, config(), vec![seed]).await;
    let ids = (1..=4).collect();
    let taken = within(FORMED, || async {
        cluster.one_table(&ids).await?;
        all_ready(&cluster, &ids).await
    })
    .await;
    assert!(
        taken.is_some(),
        "pod 4 not taken in and ready within {FORMED:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_the_leader_crashes_the_pods_left_lead_and_own_its_shards() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::formed(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let crashed = elected(&cluster, &ids).await;
    cluster.crash(crashed).await;
    let left: BTreeSet<NodeId> = ids.into_iter().filter(|id| *id != crashed).collect();
    let failed_over = within(FAILOVER, || async {
        leader(&cluster, &left).await?;
        owned_by(&cluster, &left).await
    })
    .await;
    assert!(failed_over.is_some(), "no failover within {FAILOVER:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_a_follower_crashes_its_keys_are_held_by_their_new_owners() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::formed(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let ready = within(FORMED, || all_ready(&cluster, &ids)).await;
    assert!(ready.is_some(), "pods not ready within {FORMED:?}");
    let leading = elected(&cluster, &ids).await;
    let crashed = *ids.iter().find(|id| **id != leading).unwrap();
    // A key in every shard, each counted at its owner, once that owner serves it.
    let keys: Vec<u64> = (0..u64::from(SHARDS) * 8).collect();
    for key in &keys {
        let counted = *key + 1;
        let mut owner = 0;
        let updated = within(FAILOVER, || {
            let leading = cluster.pod(leading).clone();
            let pods: BTreeMap<NodeId, StorePod> = cluster
                .pods
                .iter()
                .map(|(id, running)| (*id, running.pod.clone()))
                .collect();
            let key = *key;
            async move {
                let table = committed(&leading).await?;
                let at = table.owner(shard_of(key, SHARDS))?;
                pods[&at]
                    .with(move |node, now| {
                        node.update(key, now, |kept: &mut Kept<Value>, _| {
                            kept.value = Value::Count {
                                window: 1,
                                counted,
                                given: 0,
                            };
                            kept.forget_at = Time::MAX;
                            ((), Changed::Whole)
                        })
                    })
                    .await
                    .flatten()
                    .map(|()| at)
            }
        })
        .await;
        if let Some(at) = updated {
            owner = at;
        }
        assert!(
            updated.is_some(),
            "key {key} not updated at its owner {owner}"
        );
    }
    // Copied to every pod: each holds every key, as two replicas over three pods have it.
    let copied = within(FAILOVER, || async {
        for id in &ids {
            let held = cluster.pod(*id).with(|node, _| node.key_count()).await?;
            if held < keys.len() {
                return None;
            }
        }
        Some(())
    })
    .await;
    assert!(copied.is_some(), "keys not copied within {FAILOVER:?}");
    cluster.crash(crashed).await;
    let left: BTreeSet<NodeId> = ids.into_iter().filter(|id| *id != crashed).collect();
    let failed_over = within(FAILOVER, || owned_by(&cluster, &left)).await;
    assert!(failed_over.is_some(), "no failover within {FAILOVER:?}");
    let table = committed(cluster.pod(leading)).await.unwrap();
    for key in keys {
        let owner = table.owner(shard_of(key, SHARDS)).unwrap();
        let value = cluster
            .pod(owner)
            .with(move |node, _| node.value(key))
            .await;
        let counted = match value.flatten() {
            Some(Value::Count { counted, .. }) => counted,
            other => panic!("key {key} at {owner}: {other:?}"),
        };
        assert!(counted > key, "key {key} at {owner}: counted {counted}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pod_cut_off_stops_serving_before_the_others_take_its_shards() {
    let _shared = ALONE.read().await;
    let cluster = Cluster::formed(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    // Ready, each owning its share.
    let ready = within(FORMED, || all_ready(&cluster, &ids)).await;
    assert!(ready.is_some(), "pods not ready within {FORMED:?}");
    let leading = elected(&cluster, &ids).await;
    let cut = *ids.iter().find(|id| **id != leading).unwrap();
    let table = committed(cluster.pod(leading)).await.unwrap();
    let shards: Vec<u16> = table.owned_by(cut).collect();
    assert!(!shards.is_empty());
    cluster.cut_off(cut);
    let cut_at = Instant::now();
    let mut stopped = None;
    let mut taken = None;
    while taken.is_none() && cut_at.elapsed() < FAILOVER * 3 {
        let serves = shards.clone();
        let serving = cluster
            .pod(cut)
            .with(move |node, now| serves.iter().any(|shard| node.serves(*shard, now)))
            .await
            .unwrap();
        if !serving && stopped.is_none() {
            stopped = Some(cut_at.elapsed());
        }
        if let Some(table) = committed(cluster.pod(leading)).await
            && shards.iter().all(|shard| table.owner(*shard) != Some(cut))
        {
            taken = Some(cut_at.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let (Some(stopped), Some(taken)) = (stopped, taken) else {
        panic!("stopped serving {stopped:?}, shards taken {taken:?}");
    };
    assert!(
        stopped < taken,
        "stopped serving at {stopped:?}, its shards taken at {taken:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollout_of_three_pods_one_at_a_time_leaves_no_shard_unserved_and_loses_nothing() {
    let _alone = ALONE.write().await;
    rollout(3, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollout_of_five_pods_two_at_a_time_leaves_no_shard_unserved_and_loses_nothing() {
    let _alone = ALONE.write().await;
    rollout(5, 2).await;
}

/// Told to stop with no pod left to take their shards — a lone pod, or every pod at once as
/// deleting the service does — pods stop within [`DRAINED`], not at their grace period's kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pods_none_is_left_to_take_from_stop_when_told() {
    let _shared = ALONE.read().await;
    for count in [1, 3] {
        let cluster = ready(count).await;
        let drains: Vec<(NodeId, JoinHandle<()>)> = (1..=count)
            .map(|id| {
                let pod = cluster.pod(id).clone();
                (id, tokio::spawn(async move { pod.drain().await }))
            })
            .collect();
        for (id, drain) in drains {
            let stopped = tokio::time::timeout(DRAINED, drain).await;
            assert!(
                stopped.is_ok(),
                "pod {id} of {count} not stopped within {DRAINED:?}"
            );
        }
    }
}

/// A request reaching a pod not its key's owner, forwarding on, is forwarded to the owner and
/// answered back through it: counted once, at the owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_at_a_pod_not_its_keys_owner_is_forwarded_and_counted_once() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let table = committed(cluster.pod(1)).await.unwrap();
    let key = key_owned_by(&table, 2);
    cluster.pod(1).set_forwarding(true).await;
    let mut client = cluster.pod(1).client().await.unwrap();
    assert!(
        client
            .ask(request(client.id(), 1, key, table.stamp(), None))
            .await
    );
    let answer = answered(&mut client, 1, ANSWERED).await;
    assert_eq!(answer, Some(Answer::Counted(1)));
    assert_eq!(held_at(cluster.pod(2), key).await, Some(1));
}

/// Forwarding off, a request reaching a pod not its key's owner is told the key is elsewhere,
/// with the pod's table if the client's is older, and counted nowhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_at_a_pod_not_its_keys_owner_is_told_elsewhere() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let table = committed(cluster.pod(1)).await.unwrap();
    let key = key_owned_by(&table, 2);
    let mut client = cluster.pod(1).client().await.unwrap();
    client.ask(request(client.id(), 1, key, (0, 0), None)).await;
    let older = answered(&mut client, 1, ANSWERED).await;
    assert!(
        matches!(&older, Some(Answer::Elsewhere(Some(told))) if told.stamp() == table.stamp()),
        "{older:?}"
    );
    client
        .ask(request(client.id(), 2, key, table.stamp(), None))
        .await;
    let same = answered(&mut client, 2, ANSWERED).await;
    assert_eq!(same, Some(Answer::Elsewhere(None)));
    assert_eq!(held_at(cluster.pod(2), key).await, None);
}

/// A request past its key's limit is refused, told how long until the key would admit one:
/// within the rule's window. An hour's, so that the two requests do not fall either side of
/// a window's end, the second then rightly admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_past_its_limit_is_refused_with_how_long_to_wait() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let table = committed(cluster.pod(1)).await.unwrap();
    let key = key_owned_by(&table, 1);
    let rule = Rule::Window {
        per: 1,
        window: ms(3_600_000),
    };
    let mut client = cluster.pod(1).client().await.unwrap();
    client
        .ask(request(client.id(), 1, key, table.stamp(), Some(rule)))
        .await;
    let admitted = answered(&mut client, 1, ANSWERED).await;
    assert!(
        matches!(admitted, Some(Answer::Admitted(_))),
        "{admitted:?}"
    );
    client
        .ask(request(client.id(), 2, key, table.stamp(), Some(rule)))
        .await;
    let refused = answered(&mut client, 2, ANSWERED).await;
    assert!(
        matches!(refused, Some(Answer::Limited { wait }) if wait > 0 && wait <= ms(3_600_000)),
        "{refused:?}"
    );
}

/// A request under several keys is gathered by the pod it reaches, each part counted at its
/// key's owner, and answered counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_under_several_keys_is_gathered_and_counted_at_each_owner() {
    let _alone = ALONE.write().await;
    let cluster = ready(3).await;
    let table = committed(cluster.pod(1)).await.unwrap();
    let keys: Vec<(NodeId, u64)> = (1..=3)
        .map(|owner| (owner, key_owned_by(&table, owner)))
        .collect();
    let mut client = cluster.pod(1).client().await.unwrap();
    let gather = ModeMessage::Gather {
        client: client.id(),
        id: 1,
        keys: keys.iter().map(|(_, key)| *key).collect(),
    };
    assert!(client.ask(gather).await);
    let answer = answered(&mut client, 1, ANSWERED).await;
    assert_eq!(answer, Some(Answer::Counted(0)));
    // Once, or twice should a part's first answer have been only late, the gathering pod
    // asking it again after 3 ms: the store keeps no request numbers.
    for (owner, key) in keys {
        let held = held_at(cluster.pod(owner), key).await;
        assert!(matches!(held, Some(1 | 2)), "key {key}: {held:?}");
    }
}

/// A request under several keys, one part's owner crashed: answered not counted once the pod
/// gathering it has waited for the part, and the parts it counted given back, so that the
/// owners hold what the client was told.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_gathered_with_a_parts_owner_crashed_is_not_counted_and_given_back() {
    let _alone = ALONE.write().await;
    let mut cluster = ready(3).await;
    let table = committed(cluster.pod(1)).await.unwrap();
    let keys: Vec<(NodeId, u64)> = (1..=3)
        .map(|owner| (owner, key_owned_by(&table, owner)))
        .collect();
    cluster.crash(3).await;
    let mut client = cluster.pod(1).client().await.unwrap();
    let gather = ModeMessage::Gather {
        client: client.id(),
        id: 1,
        keys: keys.iter().map(|(_, key)| *key).collect(),
    };
    let asked = Instant::now();
    client.ask(gather).await;
    let answer = answered(&mut client, 1, ANSWERED).await;
    let took = asked.elapsed();
    assert_eq!(answer, Some(Answer::NotServing));
    assert!(took <= GATHERED, "answered after {took:?}");
    let given_back = within(FAILOVER, || async {
        for (owner, key) in &keys[..2] {
            if held_at(cluster.pod(*owner), *key).await.unwrap_or(0) != 0 {
                return None;
            }
        }
        Some(())
    })
    .await;
    assert!(given_back.is_some(), "parts counted not given back");
}

/// An owner cut off from the others stops serving once it has lost its lease, and says so to a
/// request under a key its table still has it own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_cut_off_answers_not_serving() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    cluster.cut_off(2);
    // Its lease gone, it is ready no more: shards moving just after forming may leave one
    // unserved for a moment before that, and served again.
    let unready = within(FAILOVER, || async {
        (!cluster.pod(2).ready().await).then_some(())
    })
    .await;
    assert!(unready.is_some(), "pod 2 still ready");
    let table = committed(cluster.pod(2)).await.unwrap();
    let key = key_owned_by(&table, 2);
    let mut client = cluster.pod(2).client().await.unwrap();
    client
        .ask(request(client.id(), 1, key, table.stamp(), None))
        .await;
    let answer = answered(&mut client, 1, ANSWERED).await;
    assert_eq!(answer, Some(Answer::NotServing));
}

/// A data plane's stream is told the pod's table as it opens, and answered by its own numbers:
/// a request counted at its owner, and one under several keys gathered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_planes_stream_is_told_the_table_and_answered_by_number() {
    let _alone = ALONE.write().await;
    let cluster = ready(3).await;
    let address = cluster.pods[&1].relay.address.to_string();
    let mut stream = decide_stream(&address, &Dial::Plain).await.unwrap();
    let first = told(&mut stream, ANSWERED).await;
    let Some(ModeMessage::Store(StoreMessage::TableReply { table })) = first else {
        panic!("first told {first:?}");
    };
    let named: BTreeSet<NodeId> = table.members.keys().copied().collect();
    assert_eq!(named, (1..=3).collect());
    let key = key_owned_by(&table, 1);
    // Whoever it names, a request is the stream's own.
    let asked = asking(request(42, 7, key, table.stamp(), None)).unwrap();
    stream.up.send(asked).await.unwrap();
    assert_eq!(
        told_answer(&mut stream, 7, ANSWERED).await,
        Some(Answer::Counted(1))
    );
    let others: Vec<(NodeId, u64)> = (2..=3)
        .map(|owner| (owner, key_owned_by(&table, owner)))
        .collect();
    let gather = ModeMessage::Gather {
        client: 0,
        id: 8,
        keys: others.iter().map(|(_, key)| *key).collect(),
    };
    stream.up.send(asking(gather).unwrap()).await.unwrap();
    assert_eq!(
        told_answer(&mut stream, 8, ANSWERED).await,
        Some(Answer::Counted(0))
    );
    // Once, or twice should a part's first answer have been only late, the gathering pod
    // asking it again after 3 ms: the store keeps no request numbers.
    for (owner, key) in others {
        let held = held_at(cluster.pod(owner), key).await;
        assert!(matches!(held, Some(1 | 2)), "key {key}: {held:?}");
    }
}

/// A table the pod commits reaches the data planes' streams to it: a pod taken in is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_committed_reaches_the_data_planes_streams() {
    let _shared = ALONE.read().await;
    let mut cluster = ready(3).await;
    let address = cluster.pods[&1].relay.address.to_string();
    let mut stream = decide_stream(&address, &Dial::Plain).await.unwrap();
    let seed = cluster.pods[&1].relay.address.to_string();
    cluster.start(4, config(), vec![seed]).await;
    let until = Instant::now() + FORMED;
    let mut named = false;
    while !named && Instant::now() < until {
        let left = until.saturating_duration_since(Instant::now());
        if let Some(ModeMessage::Store(StoreMessage::TableReply { table })) =
            told(&mut stream, left).await
        {
            named = table.has_member(4);
        }
    }
    assert!(named, "no table naming pod 4 told within {FORMED:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pod_of_another_shard_count_is_kept_out_and_never_ready() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::formed(3).await;
    let seeds = (1..=3)
        .map(|id| cluster.pods[&id].relay.address.to_string())
        .collect();
    let other = Config {
        shards: SHARDS * 2,
        ..config()
    };
    cluster.start(4, other, seeds).await;
    let ids: BTreeSet<NodeId> = (1..=4).collect();
    let taken = within(FORMED, || cluster.one_table(&ids)).await;
    assert!(taken.is_none(), "pod 4 taken in: {taken:?}");
    let outside = cluster
        .pod(4)
        .with(|node, now| node.committed().is_none() && !node.ready(now) && node.peers().is_empty())
        .await
        .unwrap();
    assert!(outside, "pod 4 learnt of the cluster");
    let table = committed(cluster.pod(1)).await.unwrap();
    assert!(!table.has_member(4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pod_gone_is_no_longer_dialled_once_forgotten() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::formed(3).await;
    let dialled = Arc::clone(&cluster.pods[&3].relay.dialled);
    cluster.crash(3).await;
    let crashed_at = Instant::now();
    let at_crash = dialled.load(Ordering::Relaxed);
    // A crashed voter keeps its vote until a pod takes its place, as in the simulation's
    // scenario: then out of the table, and forgotten after two silences unheard.
    let seeds = (1..=2)
        .map(|id| cluster.pods[&id].relay.address.to_string())
        .collect();
    cluster.start(4, config(), seeds).await;
    let left: BTreeSet<NodeId> = [1, 2, 4].into();
    let forgotten = within(Duration::from_secs(10), || async {
        cluster.one_table(&left).await?;
        for id in &left {
            let knows = cluster
                .pod(*id)
                .with(|node, _| node.peers().contains(&3))
                .await?;
            if knows {
                return None;
            }
        }
        Some(())
    })
    .await;
    assert!(forgotten.is_some(), "pod 3 not forgotten");
    let before = dialled.load(Ordering::Relaxed);
    assert!(before > at_crash, "pod 3 never dialled after its crash");
    // Dialled at least every second while known, backed off: none in two once forgotten.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = dialled.load(Ordering::Relaxed);
    assert_eq!(
        after,
        before,
        "pod 3 dialled {} times since forgotten, {:?} after its crash",
        after - before,
        crashed_at.elapsed()
    );
}

/// The secret the tests' data planes hash keys under.
const SECRET: [u64; 2] = [0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210];

/// A data plane on the store's streams, driven by its `Plane` as the proxy's workers are to
/// drive theirs (T6a): a stream to each member it asks, at the address its table gives, opened
/// when first needed, and whatever comes down read through the plane.
struct DataPlane {
    plane: Plane,
    streams: BTreeMap<NodeId, DecideStream>,
    next: u64,
}

impl DataPlane {
    /// One that has learnt the table from the pod at `seed`, as from `service.addresses`.
    async fn from_seed(seed: &str) -> Self {
        let mut stream = decide_stream(seed, &Dial::Plain).await.unwrap();
        let mut plane = Plane::new(SECRET);
        let first = tokio::time::timeout(ANSWERED, stream.down.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(plane.told(first), Ok(Told::Table));
        assert!(plane.table().is_some(), "no table from the seed");
        Self::with(plane)
    }

    const fn with(plane: Plane) -> Self {
        Self {
            plane,
            streams: BTreeMap::new(),
            next: 1,
        }
    }

    /// The key of a client of `owner`'s, as the data plane hashes it.
    fn key_of(&self, owner: NodeId) -> u64 {
        (0..)
            .map(|n| self.plane.key(format!("api\0client-{n}").as_bytes()))
            .find(|key| self.plane.owner(*key) == Some(owner))
            .unwrap()
    }

    /// Sends member `to` what `make` makes of the plane and a number of its own: that number.
    async fn ask(&mut self, to: NodeId, make: impl FnOnce(&Plane, u64) -> Option<Asking>) -> u64 {
        let id = self.next;
        self.next += 1;
        let asking = make(&self.plane, id).unwrap();
        if !self.streams.contains_key(&to) {
            let address = self.plane.address(to).unwrap().to_owned();
            let stream = decide_stream(&address, &Dial::Plain).await.unwrap();
            self.streams.insert(to, stream);
        }
        self.streams[&to].up.send(asking).await.unwrap();
        id
    }

    /// The answer member `from` gives request `id`, within [`ANSWERED`]: what else it tells
    /// meanwhile read by the plane, its tables taken in.
    async fn answer(&mut self, from: NodeId, id: u64) -> Option<Answer> {
        let until = Instant::now() + ANSWERED;
        loop {
            let left = until.saturating_duration_since(Instant::now());
            let stream = self.streams.get_mut(&from)?;
            let told = tokio::time::timeout(left, stream.down.message())
                .await
                .ok()?
                .ok()??;
            if let Told::Answer {
                id: answered,
                answer,
            } = self.plane.told(told).ok()?
                && answered == id
            {
                return Some(answer);
            }
        }
    }
}

/// A data plane given a seed learns the table, and asks each key of its owner at the address
/// the table gives: counted there, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_plane_learns_the_table_from_a_seed_and_asks_each_key_of_its_owner() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let seed = cluster.pods[&1].relay.address.to_string();
    let mut plane = DataPlane::from_seed(&seed).await;
    for owner in 1..=3 {
        let key = plane.key_of(owner);
        let to = plane.plane.through(key, |_| false).unwrap();
        assert_eq!(to, owner);
        let id = plane
            .ask(to, |plane, id| plane.request(id, key, None))
            .await;
        assert_eq!(plane.answer(to, id).await, Some(Answer::Counted(1)));
        assert_eq!(held_at(cluster.pod(owner), key).await, Some(1), "key {key}");
    }
}

/// A data plane whose table is behind asks a key of the pod its table names, is told the key is
/// elsewhere and the later table, and asks the owner that one names: counted there alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_plane_behind_is_told_elsewhere_and_asks_the_owner_named() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let table = committed(cluster.pod(1)).await.unwrap();
    // A table before the pods', every shard pod 1's.
    let mut behind = table.clone();
    behind.version -= 1;
    for shard in &mut behind.shards {
        shard.owner = Some(1);
    }
    let mut stale = Plane::new(SECRET);
    assert!(stale.learn(behind));
    let mut plane = DataPlane::with(stale);
    let mut later = Plane::new(SECRET);
    assert!(later.learn(table.clone()));
    let key = DataPlane::with(later).key_of(2);
    assert_eq!(plane.plane.owner(key), Some(1));
    let id = plane.ask(1, |plane, id| plane.request(id, key, None)).await;
    assert_eq!(plane.answer(1, id).await, Some(Answer::Elsewhere(None)));
    assert_eq!(plane.plane.stamp(), table.stamp());
    let to = plane.plane.through(key, |_| false).unwrap();
    assert_eq!(to, 2);
    let id = plane
        .ask(to, |plane, id| plane.request(id, key, None))
        .await;
    // Counted once: the owner's answer and what it holds say so, as only an owner counts a key.
    // Pod 1 is not looked at: as one of the shard's replicas, which every pod of three is, it
    // holds the owner's count too once the copy comes.
    assert_eq!(plane.answer(to, id).await, Some(Answer::Counted(1)));
    assert_eq!(held_at(cluster.pod(2), key).await, Some(1));
}

/// A data plane that cannot reach a key's owner — its stream down or its breaker open — asks
/// another pod, the one the key picks, which forwards it: counted once, at the owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_whose_owner_is_out_of_reach_is_forwarded_and_counted_once() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    for id in 1..=3 {
        cluster.pod(id).set_forwarding(true).await;
    }
    let seed = cluster.pods[&1].relay.address.to_string();
    let mut plane = DataPlane::from_seed(&seed).await;
    let key = plane.key_of(2);
    let to = plane.plane.through(key, |member| member == 2).unwrap();
    assert_ne!(to, 2);
    let id = plane
        .ask(to, |plane, id| plane.request(id, key, None))
        .await;
    // Counted once: the owner's answer and what it holds say so, as only an owner counts a key.
    // The pod forwarding it is not looked at: as one of the shard's replicas, which every pod of
    // three is, it holds the owner's count too once the copy comes.
    assert_eq!(plane.answer(to, id).await, Some(Answer::Counted(1)));
    assert_eq!(held_at(cluster.pod(2), key).await, Some(1));
}

/// A charge given back by a data plane, with the receipt it was admitted with, is credited at
/// the key's owner: the key admits again within its window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_charge_given_back_with_its_receipt_is_credited_at_the_owner() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let seed = cluster.pods[&1].relay.address.to_string();
    let mut plane = DataPlane::from_seed(&seed).await;
    let key = plane.key_of(3);
    let rule = Some(Rule::Window {
        per: 1,
        window: ms(60_000),
    });
    let id = plane.ask(3, |plane, id| plane.request(id, key, rule)).await;
    let Some(Answer::Admitted(receipt)) = plane.answer(3, id).await else {
        panic!("not admitted");
    };
    let id = plane.ask(3, |plane, id| plane.request(id, key, rule)).await;
    let refused = plane.answer(3, id).await;
    assert!(
        matches!(refused, Some(Answer::Limited { .. })),
        "{refused:?}"
    );
    let id = plane
        .ask(3, |plane, id| plane.give_back(id, key, rule, receipt))
        .await;
    let given = plane.answer(3, id).await;
    assert!(matches!(given, Some(Answer::Counted(_))), "{given:?}");
    let id = plane.ask(3, |plane, id| plane.request(id, key, rule)).await;
    let again = plane.answer(3, id).await;
    assert!(matches!(again, Some(Answer::Admitted(_))), "{again:?}");
}

/// A naming file of the scratch directory's, for one test.
fn naming_file(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("edgerush-forming-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join(name);
    let _ = std::fs::remove_file(&file);
    file
}

/// Pods `ids` started, not shown running to the harness's control plane, which forms nothing
/// of them: their addresses, for `Forming` to form.
async fn unformed(cluster: &mut Cluster, ids: std::ops::RangeInclusive<NodeId>) -> Vec<String> {
    let mut addresses = Vec::new();
    for id in ids {
        addresses.push(cluster.launch(id, config(), Vec::new()).await);
    }
    addresses
}

/// `Forming` from the file at `file`, run on its own against `addresses`.
fn forming(file: &Path, addresses: &[String]) -> JoinHandle<()> {
    let forming = Forming::new(&config(), file.to_owned()).unwrap();
    let addresses = addresses.to_vec();
    tokio::spawn(async move {
        forming.run(addresses).await.unwrap();
    })
}

/// The generation of the table every pod of `ids` has, if they have one and the same.
async fn formed_generation(cluster: &Cluster, ids: &BTreeSet<NodeId>) -> Option<u64> {
    cluster
        .one_table(ids)
        .await
        .map(|table| generation_of(table.term))
}

/// Pods at the addresses it is given, nothing kept, are formed by `Forming` into the first
/// generation, which it keeps in its file, the pods it named with the addresses they gave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forming_names_the_pods_at_its_addresses_and_keeps_the_naming() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::new();
    let addresses = unformed(&mut cluster, 1..=3).await;
    let file = naming_file("first");
    let task = forming(&file, &addresses);
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let formed = within(FORMED, || formed_generation(&cluster, &ids)).await;
    task.abort();
    assert_eq!(formed, Some(1), "not formed within {FORMED:?}");
    let kept = forming::read(&file).unwrap().unwrap();
    assert_eq!(kept.generation, 1);
    let named: BTreeMap<NodeId, String> = (1..=3).zip(addresses).collect();
    assert_eq!(kept.named, named);
}

/// Stopped and started again from its file, `Forming` names nothing new of a cluster that
/// runs: its pods keep the generation it formed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forming_resumed_from_its_file_names_nothing_new() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::new();
    let addresses = unformed(&mut cluster, 1..=3).await;
    let file = naming_file("resumed");
    let task = forming(&file, &addresses);
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let formed = within(FORMED, || formed_generation(&cluster, &ids)).await;
    task.abort();
    assert_eq!(formed, Some(1));
    let task = forming(&file, &addresses);
    // Several heartbeats of asking each pod and hearing it holds the table named.
    tokio::time::sleep(Duration::from_secs(2)).await;
    task.abort();
    assert_eq!(forming::read(&file).unwrap().unwrap().generation, 1);
    assert_eq!(formed_generation(&cluster, &ids).await, Some(1));
}

/// Every pod gone and new ones started in their place, `Forming` started again from its file
/// names the next generation: above the one before, which no pod running holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pods_all_replaced_are_formed_above_the_generation_kept() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::new();
    let addresses = unformed(&mut cluster, 1..=3).await;
    let file = naming_file("replaced");
    let task = forming(&file, &addresses);
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let formed = within(FORMED, || formed_generation(&cluster, &ids)).await;
    task.abort();
    assert_eq!(formed, Some(1));
    for id in 1..=3 {
        cluster.crash(id).await;
    }
    let addresses = unformed(&mut cluster, 4..=6).await;
    let task = forming(&file, &addresses);
    let ids: BTreeSet<NodeId> = (4..=6).collect();
    let formed = within(FORMED, || formed_generation(&cluster, &ids)).await;
    task.abort();
    assert_eq!(formed, Some(2), "not formed again within {FORMED:?}");
    assert_eq!(forming::read(&file).unwrap().unwrap().generation, 2);
}

/// As `Forming` runs, a pod deleted, its address refusing, runs no longer, and pods restarted
/// in place, their addresses answering as new members, run as those: formed again, the new
/// members alone, with the one deleted left out rather than waited on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_address_refusing_runs_nothing_and_one_answering_anew_runs_its_new_member() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::new();
    let addresses = unformed(&mut cluster, 1..=3).await;
    let file = naming_file("in-place");
    let task = forming(&file, &addresses);
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let formed = within(FORMED, || formed_generation(&cluster, &ids)).await;
    assert_eq!(formed, Some(1));
    cluster.delete(3).await;
    cluster.restart_in_place(1, 7).await;
    cluster.restart_in_place(2, 8).await;
    let ids: BTreeSet<NodeId> = [7, 8].into();
    // A refusal is heard within some seconds on Windows, then forming as at first.
    let formed = within(FORMED * 3, || formed_generation(&cluster, &ids)).await;
    task.abort();
    assert_eq!(formed, Some(2), "not formed again of 7 and 8");
    let kept = forming::read(&file).unwrap().unwrap();
    let named: BTreeMap<NodeId, String> = [7, 8].into_iter().zip(addresses).collect();
    assert_eq!(kept.named, named);
}

/// An address that answers nothing but does not refuse — its pod cut off — still runs: with the
/// others restarted in place, `Forming` waits on the pod cut off, which may hold the cluster's
/// latest table, rather than form a cluster without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_address_answering_nothing_still_runs_and_is_waited_on() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::new();
    let addresses = unformed(&mut cluster, 1..=3).await;
    let file = naming_file("cut-off");
    let task = forming(&file, &addresses);
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let formed = within(FORMED, || formed_generation(&cluster, &ids)).await;
    assert_eq!(formed, Some(1));
    cluster.cut_off(3);
    cluster.restart_in_place(1, 7).await;
    cluster.restart_in_place(2, 8).await;
    // Long past forming 7 and 8 had pod 3 been taken as gone (the test above: within a second
    // or two of a refusal).
    tokio::time::sleep(FORMED * 2).await;
    task.abort();
    assert_eq!(forming::read(&file).unwrap().unwrap().generation, 1);
    for id in [7, 8] {
        assert!(
            committed(cluster.pod(id)).await.is_none(),
            "pod {id} formed"
        );
    }
}

/// `GET path` of the listener at `address`, as a probe or a scrape asks: the status and the
/// body, if it answers.
async fn get(address: SocketAddr, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(address).await.ok()?;
    let asked = format!("GET {path} HTTP/1.1\r\nhost: pod\r\nconnection: close\r\n\r\n");
    stream.write_all(asked.as_bytes()).await.ok()?;
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).await.ok()?;
    let answer = String::from_utf8(answer).ok()?;
    let status = answer.split(' ').nth(1)?.parse().ok()?;
    let body = answer.split_once("\r\n\r\n")?.1.to_owned();
    Some((status, body))
}

/// A pod's readiness and scrape are served as its store has them: not ready before forming,
/// ready once formed, and not ready once cut off, its lease lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pods_readiness_and_scrape_are_served_as_its_store_has_them() {
    let _shared = ALONE.read().await;
    let mut cluster = Cluster::new();
    let addresses = unformed(&mut cluster, 1..=3).await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let at = listener.local_addr().unwrap();
    tokio::spawn(scrape::serve_store(listener, cluster.pod(1).clone()));
    assert_eq!(get(at, "/ready").await.map(|(status, _)| status), Some(503));
    let (status, scraped) = get(at, "/metrics").await.unwrap();
    assert_eq!(status, 200);
    assert!(
        scraped.contains("edgerush_rate_limit_pod_serving 0\n"),
        "{scraped}"
    );
    assert!(
        scraped.contains("edgerush_rate_limit_table_term 0\n"),
        "{scraped}"
    );
    let file = naming_file("ready");
    let task = forming(&file, &addresses);
    let ready = within(FORMED, || async {
        (get(at, "/ready").await? == (200, "ready\n".to_owned())).then_some(())
    })
    .await;
    assert!(ready.is_some(), "not ready within {FORMED:?}");
    let (_, scraped) = get(at, "/metrics").await.unwrap();
    assert!(
        scraped.contains("edgerush_rate_limit_pod_serving 1\n"),
        "{scraped}"
    );
    let term: u64 = scraped
        .lines()
        .find_map(|line| line.strip_prefix("edgerush_rate_limit_table_term "))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(generation_of(term), 1, "{scraped}");
    cluster.cut_off(1);
    let unready = within(FAILOVER, || async {
        (get(at, "/ready").await?.0 == 503).then_some(())
    })
    .await;
    task.abort();
    assert!(
        unready.is_some(),
        "still ready, cut off, after {FAILOVER:?}"
    );
    assert_eq!(get(at, "/live").await.map(|(status, _)| status), Some(404));
}

/// What a pod's scrape shows is its store's: one leader among the pods, each the table it has
/// committed, a key counted at its owner held there, and its counters as its store keeps them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_a_pods_scrape_shows_is_its_stores() {
    let _shared = ALONE.read().await;
    let cluster = ready(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let table = cluster.one_table(&ids).await.unwrap();
    let leader = elected(&cluster, &ids).await;
    let mut leaders = 0;
    for id in &ids {
        let observed = cluster.pod(*id).observed().await.unwrap();
        assert!(observed.ready, "pod {id}");
        assert_eq!(observed.table, table.stamp(), "pod {id}");
        assert_eq!(observed.keys, 0, "pod {id}");
        assert_eq!(observed.max_key_bytes, config().max_key_bytes, "pod {id}");
        leaders += u32::from(observed.leader);
    }
    assert!(cluster.pod(leader).observed().await.unwrap().leader);
    assert_eq!(leaders, 1);
    let key = key_owned_by(&table, 2);
    let mut client = cluster.pod(2).client().await.unwrap();
    client
        .ask(request(client.id(), 1, key, table.stamp(), None))
        .await;
    assert_eq!(
        answered(&mut client, 1, ANSWERED).await,
        Some(Answer::Counted(1))
    );
    let owner = cluster.pod(2).observed().await.unwrap();
    assert_eq!(owner.keys, 1);
    assert!(owner.key_bytes > 0);
    let held = cluster
        .pod(2)
        .with(|node, _| (node.key_bytes(), node.forgotten(), node.evicted()))
        .await
        .unwrap();
    assert_eq!((owner.key_bytes, owner.forgotten, owner.evicted), held);
    assert_eq!(owner.skewed, cluster.pod(2).skewed());
    assert_eq!(owner.dropped, cluster.pod(2).dropped());
}
