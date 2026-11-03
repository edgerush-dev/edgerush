//! The cluster store on sockets on localhost (cluster-store-transplant-plan.md, "T4d"): each pod
//! given a loopback IP of its own and reached through a relay the test holds there, which is
//! the address it gives. Cutting a pod off is the relays' doing: each drops the connections
//! of a pod cut off, to it and from it, known by its IP, as pods dial from theirs. The control
//! plane's `Authority` runs in-process, asking the pods as the controller will. Each scenario
//! is the simulation's, held to its bound; a rollout's, as T4d2 has it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Authority, Changed, Config, Kept, Node, NodeId, Table, Time};
use edgerush_cluster_synchronous::Value;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{STREAMS_IDLE, StorePod, StoreServer, TICK, ask};
use edgerush_ratelimit_service::grpc::Client;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

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
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(to).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = cut.wait_for(|cut| dropped(cut, ip, from.ip())) => {}
                    }
                });
            }
        });
        Self {
            address,
            dialled,
            task,
        }
    }
}

/// A pod running, its server and its relay.
struct Running {
    pod: StorePod<Value>,
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
        let pod = StorePod::<Value>::spawn(id, address.clone(), config, Dial::Plain, seeds);
        let server = StoreServer::new(pod.clone());
        let server = tokio::spawn(async move {
            let _ended = tonic::transport::Server::builder()
                .serve_with_incoming(server, TcpIncoming::from(listener))
                .await;
        });
        self.pods.insert(id, Running { pod, server, relay });
        address
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

    fn pod(&self, id: NodeId) -> &StorePod<Value> {
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
async fn committed(pod: &StorePod<Value>) -> Option<Table> {
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
                        if let Ok(Some(answer)) = ask::<Value>(&client, message).await {
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
    live: Arc<Mutex<BTreeMap<NodeId, StorePod<Value>>>>,
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
        let pods: Vec<StorePod<Value>> = live.lock().unwrap().values().cloned().collect();
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
    let mut cluster = Cluster::formed(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let crashed = leader(&cluster, &ids).await.unwrap();
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
    let mut cluster = Cluster::formed(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    let ready = within(FORMED, || all_ready(&cluster, &ids)).await;
    assert!(ready.is_some(), "pods not ready within {FORMED:?}");
    let leading = leader(&cluster, &ids).await.unwrap();
    let crashed = *ids.iter().find(|id| **id != leading).unwrap();
    // A key in every shard, each counted at its owner, once that owner serves it.
    let keys: Vec<u64> = (0..u64::from(SHARDS) * 8).collect();
    for key in &keys {
        let counted = *key + 1;
        let mut owner = 0;
        let updated = within(FAILOVER, || {
            let leading = cluster.pod(leading).clone();
            let pods: BTreeMap<NodeId, StorePod<Value>> = cluster
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
    let cluster = Cluster::formed(3).await;
    let ids: BTreeSet<NodeId> = (1..=3).collect();
    // Ready, each owning its share.
    let ready = within(FORMED, || all_ready(&cluster, &ids)).await;
    assert!(ready.is_some(), "pods not ready within {FORMED:?}");
    let leading = leader(&cluster, &ids).await.unwrap();
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
    rollout(3, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollout_of_five_pods_two_at_a_time_leaves_no_shard_unserved_and_loses_nothing() {
    rollout(5, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pod_of_another_shard_count_is_kept_out_and_never_ready() {
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
