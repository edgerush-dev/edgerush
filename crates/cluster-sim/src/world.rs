//! The simulated world: pods, links, clients and the queue of events between them.

use crate::Rng;
use crate::client::Outcome;
use crate::oracle::Oracle;
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::table::Table;
use edgerush_cluster::{Authority, Config, Held, Node, NodeId, ShardId, Time, generation_of, ms};
use edgerush_cluster_synchronous::Rule;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt::Debug;

mod synchronous;

pub use synchronous::Synchronous;

/// A rate-limiting mode as the world runs it: its pods over their stores, the messages they
/// and its clients send, and its clients, standing for data planes. The world's network,
/// clocks, faults, control plane and checks are every mode's; how a client sends, is
/// answered and is measured is the mode's own.
pub trait Mode: Sized {
    /// What its pods' stores keep under each key.
    type Value: Held;
    /// One of its pods, over its store.
    type Pod: Clone + Debug;
    /// What its pods and clients send, the store's among it.
    type Message: Clone + Debug + From<StoreMessage<Self::Value>>;
    /// One of its clients, standing for a data plane.
    type Client: Clone + Debug;

    /// Pod `id`, new, knowing how to reach `peers`, forwarding requests for shards not its own
    /// if `forwarding`, as the mode does.
    fn pod(id: NodeId, config: Config, peers: Vec<NodeId>, forwarding: bool) -> Self::Pod;
    /// A pod's store.
    fn store(pod: &Self::Pod) -> &Node<Self::Value>;
    /// A pod's store, to change.
    fn store_mut(pod: &mut Self::Pod) -> &mut Node<Self::Value>;
    /// A pod's timer at `now`, on its own clock.
    fn tick(pod: &mut Self::Pod, now: Time) -> Vec<(NodeId, Self::Message)>;
    /// `message` from `from` reaching a pod at `now`, on its own clock.
    fn receive(
        pod: &mut Self::Pod,
        from: NodeId,
        message: Self::Message,
        now: Time,
    ) -> Vec<(NodeId, Self::Message)>;
    /// The store's message `message` carries, if it is one.
    fn as_store(message: &Self::Message) -> Option<&StoreMessage<Self::Value>>;
    /// The same, taken.
    fn into_store(message: Self::Message) -> Option<StoreMessage<Self::Value>>;
    /// The name of `message`'s kind, for the bytes sent by kind.
    fn kind(message: &Self::Message) -> &'static str;
    /// About how many bytes `message` takes on the wire.
    fn size(message: &Self::Message) -> u64;
    /// What the mode watches of every message sent, before the network takes it.
    fn on_send(world: &mut Sim<Self>, from: NodeId, to: NodeId, message: &Self::Message);
    /// A client's `message` to pod `to` found unreachable, its stream down or the pod gone.
    fn unreachable(world: &mut Sim<Self>, from: NodeId, to: NodeId, message: &Self::Message);
    /// Client `id`'s turn to send.
    fn client_send(world: &mut Sim<Self>, id: NodeId);
    /// Client `id`'s check of what it has in flight, every millisecond.
    fn client_check(world: &mut Sim<Self>, id: NodeId);
    /// Client `client` told its request `id` could not reach `pod`.
    fn client_unreachable(world: &mut Sim<Self>, client: NodeId, id: u64, pod: NodeId);
    /// `message` reaching client `id`.
    fn deliver_to_client(world: &mut Sim<Self>, id: NodeId, message: Self::Message);
}

/// How long a drained pod's process goes on before it ends: longer than any request's deadline,
/// so that what it has in flight is answered.
const LINGER: Time = 50_000_000;

/// Clients' identities start here, apart from every pod's.
pub const CLIENT_BASE: NodeId = 1_000_000;

/// The control plane's identity, apart from every pod's and client's.
pub const CONTROL: NodeId = CLIENT_BASE - 1;

/// One direction of a link between two endpoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// The time every message takes at least.
    pub delay: Time,
    /// Up to this much more, at random.
    pub jitter: Time,
    /// Messages lost, per million.
    pub loss_per_million: u32,
    /// Whether nothing gets through.
    pub cut: bool,
    /// Bytes a second it carries; messages queue behind one another.
    pub bandwidth: u64,
}

impl Link {
    /// A link within one data centre: 0.3 ms, up to 0.2 ms more, nothing lost, 1 Gbit/s.
    #[must_use]
    pub const fn lan() -> Self {
        Self {
            delay: 300_000,
            jitter: 200_000,
            loss_per_million: 0,
            cut: false,
            bandwidth: 125_000_000,
        }
    }
}

/// One pod as the world runs it.
#[derive(Clone, Debug)]
struct Pod<P> {
    node: P,
    alive: bool,
    /// When it started, on the world's clock.
    started: Time,
    /// How fast its clock runs against the world's, in parts per million, plus or minus.
    rate_ppm: i64,
    /// Its clock's reading when it started.
    offset: Time,
    /// When it was told to drain, if it was.
    draining_since: Option<Time>,
    /// When it was first drained, if it has been: it ends `LINGER` after.
    drained_since: Option<Time>,
    /// How far its wall clock is from the world's, plus or minus: the clock it decides
    /// requests under a rule by.
    skew: i64,
    /// Until when it is frozen, if it is: no ticks, and what reaches it waits, in order.
    frozen_until: Option<Time>,
}

impl<P> Pod<P> {
    /// Until when it is frozen, if it is at `now`.
    fn frozen(&self, now: Time) -> Option<Time> {
        self.frozen_until.filter(|until| *until > now)
    }
}

impl<P> Pod<P> {
    /// Its own clock at the world's `now`.
    fn local(&self, now: Time) -> Time {
        let run = i128::from(now.saturating_sub(self.started));
        let scaled = run * (1_000_000 + i128::from(self.rate_ppm)) / 1_000_000;
        self.offset + u64::try_from(scaled.max(0)).unwrap_or(0)
    }
}

/// What can happen next.
#[derive(Clone, Debug)]
enum Event<Message> {
    Deliver {
        from: NodeId,
        to: NodeId,
        message: Message,
    },
    Tick(NodeId),
    ControlTick,
    ClientSend(NodeId),
    ClientCheck(NodeId),
    /// A client's request could not reach `pod`: its stream down, the pod gone.
    Unreachable {
        client: NodeId,
        id: u64,
        pod: NodeId,
    },
    Sample,
}

/// What a run has measured.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    /// Copies an owner sent a replica again: the same shard, shard term and number.
    pub copies_again: u64,
    /// Requests counted more than once: a second answer "counted" sent a client for one.
    pub counted_twice: u64,
    /// Requests a client sent that could not reach their pod.
    pub unreachable: u64,
    /// Requests sent by clients.
    pub sent: u64,
    /// Requests counted.
    pub counted: u64,
    /// Requests left to `when_unreachable` at their deadline: refused under `deny`, admitted
    /// under `allow`.
    pub refused: u64,
    /// Requests refused for a key's limit.
    pub limited: u64,
    /// The time each counted request took.
    pub latencies: Vec<Time>,
    /// When each refusal happened, on the world's clock.
    pub refused_at: Vec<Time>,
    /// The time, summed over shards, two or more pods served one shard at once.
    pub overlap_total: Time,
    /// The longest any one shard was served by two or more pods at once.
    pub overlap_longest: Time,
    /// The time, summed over shards, no live pod served a shard.
    pub unserved_total: Time,
    /// The longest any one shard went unserved.
    pub unserved_longest: Time,
    /// What must never happen, as it happened.
    pub violations: Vec<String>,
    /// Terms a leader was elected in.
    pub terms: u64,
    /// The first moments two or more pods served one shard: when, which, and who.
    pub overlaps: Vec<(Time, ShardId, Vec<NodeId>)>,
    /// Bytes sent, every message counted by its size.
    pub bytes: u64,
    /// Messages pods sent to pods gone: in a real network each a connection tried and failed.
    pub to_gone: u64,
    /// Shards a committed table handed, by plan, to a pod already gone: unserved until it is
    /// failed over again.
    pub moved_to_gone: u64,
    /// Bytes sent by kind of message.
    pub bytes_by_kind: BTreeMap<&'static str, u64>,
    /// Sends of give-backs, each give-back sent once or more.
    pub give_back_sends: u64,
}

impl Stats {
    /// The latency below which `share` of counted requests fell.
    #[must_use]
    pub fn latency(&self, share: f64) -> Time {
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        if sorted.is_empty() {
            return 0;
        }
        let last = sorted.len() - 1;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "an index into the sorted latencies"
        )]
        let index = ((last as f64) * share).round() as usize;
        sorted[index.min(last)]
    }
}

/// The world, running mode `M`.
pub struct Sim<M: Mode> {
    now: Time,
    rng: Rng,
    queue: BinaryHeap<Reverse<(Time, u64)>>,
    events: BTreeMap<u64, Event<M::Message>>,
    seq: u64,
    pods: BTreeMap<NodeId, Pod<M::Pod>>,
    clients: BTreeMap<NodeId, M::Client>,
    default_link: Link,
    links: BTreeMap<(NodeId, NodeId), Link>,
    last_arrival: BTreeMap<(NodeId, NodeId), Time>,
    /// When each link's lane is next free to start sending: bulk copies apart from the rest, as
    /// separate streams over one connection, so that heartbeats and handoffs never queue
    /// behind a replica's snapshot.
    link_free: BTreeMap<(NodeId, NodeId, bool), Time>,
    /// Whether bulk copies have a lane of their own; one lane for everything if not.
    pub lanes: bool,
    config: Config,
    next_pod: NodeId,
    next_client: NodeId,
    /// The time between a pod's ticks.
    pub tick: Time,
    /// The time between samples of who serves each shard.
    pub sample: Time,
    forwarding: bool,
    /// What it measured.
    pub stats: Stats,
    /// Admissions per key, as clients were told.
    truth: BTreeMap<u64, u64>,
    /// Every key a client has asked under.
    asked: BTreeSet<u64>,
    leaders: BTreeMap<u64, NodeId>,
    tables: BTreeMap<(u64, u64), Table>,
    /// Each pod's table as last checked.
    held: BTreeMap<NodeId, (u64, u64)>,
    /// The rule clients' requests are held to, and whether `when_unreachable` admits.
    limit: Option<(Rule, bool)>,
    /// Under a rule, what clients were told, judged by the rule's reference.
    oracle: Option<Oracle>,
    /// The same, each decision judged at the true time it was made: what a skewed clock
    /// costs as a client sees it.
    true_time: Option<Oracle>,
    /// When each request a client made was decided by its key's owner, by that owner's wall
    /// clock — judged in the windows it decided in, so that a skewed clock shows where two
    /// owners disagree, not in every window it shifts — and by the world's.
    decided: BTreeMap<(NodeId, u64), (Time, Time)>,
    overlap_since: BTreeMap<ShardId, Time>,
    unserved_since: BTreeMap<ShardId, Time>,
    sampling: bool,
    /// Whether clients have stopped sending.
    clients_stopped: bool,
    /// Every copy sent: owner, replica, shard, shard term and number.
    copies: BTreeSet<(NodeId, NodeId, ShardId, u64, u64)>,
    /// Every request a client has been answered "counted" for: the client and its number.
    answered_counted: BTreeSet<(NodeId, u64)>,
    /// Every give-back a client has sent, which it may send more than once.
    give_backs: BTreeSet<(NodeId, u64)>,
    /// Pods stopped, when, and whether they drained first.
    pub stopped: Vec<(NodeId, Time, bool)>,
    /// How long a pod told to drain is given before it is killed.
    pub grace: Time,
    /// The control plane's part in forming the cluster.
    authority: Authority,
    /// Whether the control plane is down: it neither hears nor tells anyone.
    control_down: bool,
    /// How long the API goes on showing a pod running after it stopped.
    pub api_lag: Time,
    /// Pods the control plane judges gone while they run: Terminating on a node not ready.
    judged_gone: BTreeSet<NodeId>,
}

/// The world running the synchronous mode.
pub type World = Sim<Synchronous>;

impl<M: Mode> Sim<M> {
    /// An empty world from `seed`, its pods to be started with `config`.
    #[must_use]
    pub fn new(seed: u64, config: Config) -> Self {
        let mut world = Self {
            now: 0,
            rng: Rng::new(seed),
            queue: BinaryHeap::new(),
            events: BTreeMap::new(),
            seq: 0,
            pods: BTreeMap::new(),
            clients: BTreeMap::new(),
            default_link: Link::lan(),
            links: BTreeMap::new(),
            last_arrival: BTreeMap::new(),
            link_free: BTreeMap::new(),
            lanes: true,
            config,
            next_pod: 1,
            next_client: CLIENT_BASE,
            tick: ms(5),
            sample: ms(1),
            forwarding: false,
            stats: Stats::default(),
            truth: BTreeMap::new(),
            asked: BTreeSet::new(),
            leaders: BTreeMap::new(),
            tables: BTreeMap::new(),
            held: BTreeMap::new(),
            limit: None,
            oracle: None,
            true_time: None,
            decided: BTreeMap::new(),
            overlap_since: BTreeMap::new(),
            unserved_since: BTreeMap::new(),
            sampling: false,
            clients_stopped: false,
            copies: BTreeSet::new(),
            answered_counted: BTreeSet::new(),
            give_backs: BTreeSet::new(),
            stopped: Vec::new(),
            grace: ms(30_000),
            authority: Authority::new(&config),
            control_down: false,
            api_lag: ms(1_000),
            judged_gone: BTreeSet::new(),
        };
        world.schedule(0, Event::ControlTick);
        world
    }

    /// Takes the control plane down, or brings it back up.
    pub fn set_control_down(&mut self, down: bool) {
        self.control_down = down;
    }

    /// Restarts the control plane: it forgets all but the generation last formed.
    pub fn restart_control(&mut self) {
        self.authority.restart();
    }

    /// The generation the control plane last formed.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.authority.generation()
    }

    /// Has the control plane judge `id` gone though it runs, as a pod Terminating on a node not
    /// ready beyond the timeout.
    pub fn judge_gone(&mut self, id: NodeId) {
        self.judged_gone.insert(id);
    }

    /// The pods the API shows running: alive and not judged gone, or stopped within `api_lag`.
    fn api_running(&self) -> BTreeSet<NodeId> {
        let stopped: BTreeMap<NodeId, Time> =
            self.stopped.iter().map(|(id, at, _)| (*id, *at)).collect();
        self.pods
            .iter()
            .filter(|(id, pod)| {
                if self.judged_gone.contains(id) {
                    return false;
                }
                pod.alive
                    || stopped
                        .get(id)
                        .is_some_and(|at| self.now.saturating_sub(*at) < self.api_lag)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// The world's clock.
    #[must_use]
    pub const fn now(&self) -> Time {
        self.now
    }

    /// The pods' config.
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    fn schedule(&mut self, at: Time, event: Event<M::Message>) {
        self.seq += 1;
        self.events.insert(self.seq, event);
        self.queue.push(Reverse((at, self.seq)));
    }

    /// `n` new pods, started together, for the control plane to name to form the cluster.
    pub fn start_cluster(&mut self, n: usize) -> Vec<NodeId> {
        let ids: Vec<NodeId> = (0..n)
            .map(|_| {
                let id = self.next_pod;
                self.next_pod += 1;
                id
            })
            .collect();
        for id in &ids {
            self.spawn(*id, self.config, ids.iter().copied());
        }
        ids
    }

    /// One new pod, knowing every pod alive.
    pub fn add_pod(&mut self) -> NodeId {
        let id = self.next_pod;
        self.next_pod += 1;
        let peers: Vec<NodeId> = self.alive().collect();
        self.spawn(id, self.config, peers);
        id
    }

    fn spawn(&mut self, id: NodeId, config: Config, peers: impl IntoIterator<Item = NodeId>) {
        let node = M::pod(id, config, peers.into_iter().collect(), self.forwarding);
        // A clock started anywhere in its first second.
        let offset = self.rng.below(ms(1_000));
        self.pods.insert(
            id,
            Pod {
                node,
                alive: true,
                started: self.now,
                rate_ppm: 0,
                offset,
                draining_since: None,
                drained_since: None,
                frozen_until: None,
                skew: 0,
            },
        );
        let first = self.now + self.rng.below(self.tick);
        self.schedule(first, Event::Tick(id));
    }

    /// Clients added from now have every request held to `rule` by its key's owner, and
    /// `when_unreachable` admits, if `allow`, or refuses.
    pub fn set_limit(&mut self, rule: Rule, allow: bool) {
        self.limit = Some((rule, allow));
        self.oracle = Some(Oracle::new(rule));
        self.true_time = Some(Oracle::new(rule));
    }

    /// Sets `id`'s wall clock `skew` off the world's, ahead or behind.
    pub fn set_wall_skew(&mut self, id: NodeId, skew: i64) {
        if let Some(pod) = self.pods.get_mut(&id) {
            pod.skew = skew;
        }
    }

    /// Admissions the rule's reference would have refused, as clients were told.
    #[must_use]
    pub fn overshoot(&self) -> u64 {
        self.oracle.as_ref().map_or(0, Oracle::overshoot)
    }

    /// The most a key was admitted beyond the rule at once by its owners, what `allow`
    /// admitted without a count aside: two owners each admitting a whole window make one
    /// limit's worth.
    #[must_use]
    pub fn overshoot_peak(&self) -> u64 {
        self.oracle.as_ref().map_or(0, Oracle::overshoot_peak)
    }

    /// Requests refused under the rule while every key they were under had room: refusals
    /// within the rate.
    #[must_use]
    pub fn shaved(&self) -> u64 {
        self.oracle.as_ref().map_or(0, Oracle::shaved)
    }

    /// Requests `when_unreachable` refused, under `deny`, while their keys had room.
    #[must_use]
    pub fn refused_with_room(&self) -> u64 {
        self.oracle.as_ref().map_or(0, Oracle::refused_with_room)
    }

    /// What clients were told, each decision judged at the true time it was made rather
    /// than by its owner's wall clock: with clocks skewed, what the skew costs as clients
    /// see it, beside what [`World::shaved`] and the rest say owners disagreed.
    #[must_use]
    pub const fn in_true_time(&self) -> Option<&Oracle> {
        self.true_time.as_ref()
    }

    /// Every client stops sending: the keys go quiet, what is in flight still answered.
    pub fn stop_clients(&mut self) {
        self.clients_stopped = true;
    }

    /// Starts measuring who serves each shard, every `sample`.
    pub fn start_sampling(&mut self) {
        if !self.sampling {
            self.sampling = true;
            self.schedule(self.now + self.sample, Event::Sample);
        }
    }

    /// The pods alive.
    pub fn alive(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.pods
            .iter()
            .filter(|(_, pod)| pod.alive)
            .map(|(id, _)| *id)
    }

    /// The pod `id`'s node.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&Node<M::Value>> {
        self.pods.get(&id).map(|pod| M::store(&pod.node))
    }

    /// Whether `id` is alive.
    #[must_use]
    pub fn is_alive(&self, id: NodeId) -> bool {
        self.pods.get(&id).is_some_and(|pod| pod.alive)
    }

    /// The live leader of the highest term, if any.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.pods
            .iter()
            .filter(|(_, pod)| pod.alive && M::store(&pod.node).is_leader())
            .max_by_key(|(_, pod)| M::store(&pod.node).term())
            .map(|(id, _)| *id)
    }

    /// The latest committed table any live pod holds.
    #[must_use]
    pub fn table(&self) -> Option<&Table> {
        self.pods
            .values()
            .filter(|pod| pod.alive)
            .filter_map(|pod| M::store(&pod.node).committed())
            .max_by_key(|table| table.stamp())
    }

    /// Every table committed so far, in order.
    pub fn history(&self) -> impl Iterator<Item = &Table> + '_ {
        self.tables.values()
    }

    /// Kills `id` without warning.
    pub fn crash(&mut self, id: NodeId) {
        if let Some(pod) = self.pods.get_mut(&id)
            && pod.alive
        {
            pod.alive = false;
            self.stopped.push((id, self.now, false));
        }
    }

    /// Freezes `id` for `for_`, as a process stopped by CPU throttling or a stalled node is:
    /// its clock runs on, but it does nothing, and what is sent to it waits, then reaches it
    /// all at once, in order.
    pub fn freeze(&mut self, id: NodeId, for_: Time) {
        let until = self.now + for_;
        if let Some(pod) = self.pods.get_mut(&id) {
            pod.frozen_until = Some(until);
        }
    }

    /// Tells `id` to stop: it drains, and is stopped once drained, or killed after `grace`.
    pub fn drain(&mut self, id: NodeId) {
        if let Some(pod) = self.pods.get_mut(&id)
            && pod.alive
        {
            M::store_mut(&mut pod.node).drain();
            pod.draining_since = Some(self.now);
        }
    }

    /// Sets `id`'s clock running `ppm` parts per million fast, or slow if negative.
    pub fn set_clock_rate(&mut self, id: NodeId, ppm: i64) {
        let now = self.now;
        if let Some(pod) = self.pods.get_mut(&id) {
            let reading = pod.local(now);
            pod.offset = reading;
            pod.started = now;
            pod.rate_ppm = ppm;
        }
    }

    /// The link from `from` to `to`.
    fn link(&self, from: NodeId, to: NodeId) -> Link {
        self.links
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_link)
    }

    /// Every link not set apart becomes `link`.
    pub fn set_default_link(&mut self, link: Link) {
        self.default_link = link;
    }

    /// The link from `from` to `to` becomes `link`.
    pub fn set_link(&mut self, from: NodeId, to: NodeId, link: Link) {
        self.links.insert((from, to), link);
    }

    /// Nothing from `from` reaches `to`.
    pub fn cut_one_way(&mut self, from: NodeId, to: NodeId) {
        let mut link = self.link(from, to);
        link.cut = true;
        self.set_link(from, to, link);
    }

    /// Nothing between `a` and `b`, either way.
    pub fn cut(&mut self, a: NodeId, b: NodeId) {
        self.cut_one_way(a, b);
        self.cut_one_way(b, a);
    }

    /// `pod` cut off from every other pod, both ways; clients still reach it.
    pub fn isolate(&mut self, pod: NodeId) {
        let others: Vec<NodeId> = self
            .pods
            .keys()
            .copied()
            .filter(|other| *other != pod)
            .collect();
        for other in others {
            self.cut(pod, other);
        }
    }

    /// Every client's identity.
    pub fn client_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.clients.keys().copied()
    }

    /// Every client cut off from `pod`, both ways; the pods still hear each other.
    pub fn cut_clients_from(&mut self, pod: NodeId) {
        let clients: Vec<NodeId> = self.clients.keys().copied().collect();
        for client in clients {
            self.cut(client, pod);
        }
    }

    /// Every pod cut off from every other; clients still reach them all.
    pub fn cut_pods_apart(&mut self) {
        let pods: Vec<NodeId> = self.pods.keys().copied().collect();
        for a in &pods {
            for b in &pods {
                if a != b {
                    self.cut_one_way(*a, *b);
                }
            }
        }
    }

    /// Every link back as the default.
    pub fn heal(&mut self) {
        self.links.clear();
    }

    /// Runs for `duration`.
    pub fn run_for(&mut self, duration: Time) {
        let until = self.now + duration;
        self.run_until(until);
    }

    /// Runs until the world's clock reads `until`.
    pub fn run_until(&mut self, until: Time) {
        while let Some(Reverse((at, seq))) = self.queue.peek().copied() {
            if at > until {
                break;
            }
            self.queue.pop();
            self.now = at;
            if let Some(event) = self.events.remove(&seq) {
                self.handle(event);
            }
        }
        self.now = until;
    }

    /// Runs until `done` holds, checked every 10 ms, for at most `limit`; how long it took.
    pub fn run_while_not(
        &mut self,
        limit: Time,
        mut done: impl FnMut(&Self) -> bool,
    ) -> Option<Time> {
        let start = self.now;
        while self.now - start < limit {
            if done(self) {
                return Some(self.now - start);
            }
            self.run_for(ms(10));
        }
        done(self).then(|| self.now - start)
    }

    /// Whether the cluster is settled: one live leader, every live pod a member on its table,
    /// no moves outstanding, and every shard owned by a live member.
    #[must_use]
    pub fn settled(&self) -> bool {
        let Some(leader) = self.leader() else {
            return false;
        };
        let Some(table) = self.node(leader).and_then(Node::committed) else {
            return false;
        };
        let members_on_it = table.members.iter().all(|member| {
            self.node(*member)
                .and_then(Node::committed)
                .is_some_and(|theirs| theirs.stamp() == table.stamp())
                && self.is_alive(*member)
        });
        members_on_it
            && self.alive().all(|pod| table.has_member(pod))
            && table.moves.is_empty()
            && table
                .shards
                .iter()
                .all(|shard| shard.owner.is_some_and(|owner| self.is_alive(owner)))
    }

    /// Give-backs sent, by clients and by pods gathering, each counted once however often it
    /// was sent.
    #[must_use]
    pub fn give_backs(&self) -> u64 {
        self.give_backs.len() as u64
    }

    fn handle(&mut self, event: Event<M::Message>) {
        match event {
            Event::Deliver { from, to, message } => self.deliver(from, to, message),
            Event::Tick(id) => self.on_tick(id),
            Event::ControlTick => self.on_control_tick(),
            Event::ClientSend(id) => M::client_send(self, id),
            Event::ClientCheck(id) => M::client_check(self, id),
            Event::Unreachable { client, id, pod } => M::client_unreachable(self, client, id, pod),
            Event::Sample => self.on_sample(),
        }
    }

    fn send(&mut self, from: NodeId, to: NodeId, message: M::Message) {
        if let Some(StoreMessage::Replicate {
            shard,
            term,
            version,
            ..
        }) = M::as_store(&message)
            && !self.copies.insert((from, to, *shard, *term, *version))
        {
            self.stats.copies_again += 1;
        }
        M::on_send(self, from, to, &message);
        let link = self.link(from, to);
        if link.cut {
            // A client knows its stream to a pod is down, as a data plane does from a connect
            // that fails or a stream that breaks.
            M::unreachable(self, from, to, &message);
            return;
        }
        if self.rng.chance(link.loss_per_million) {
            return;
        }
        // Sent once the link has sent what it already had, taking its size's time.
        let bytes = M::size(&message);
        let sending = bytes.saturating_mul(1_000_000_000) / link.bandwidth.max(1);
        let bulk =
            self.lanes && matches!(M::as_store(&message), Some(StoreMessage::Replicate { .. }));
        let free = self.link_free.get(&(from, to, bulk)).copied().unwrap_or(0);
        let sent = self.now.max(free) + sending;
        self.link_free.insert((from, to, bulk), sent);
        self.stats.bytes += bytes;
        *self
            .stats
            .bytes_by_kind
            .entry(M::kind(&message))
            .or_insert(0) += bytes;
        let delay = link.delay + self.rng.below(link.jitter.max(1));
        // In order along one lane, as along one stream.
        let lane = (from, to.wrapping_add(if bulk { 1 << 62 } else { 0 }));
        let last = self.last_arrival.get(&lane).copied().unwrap_or(0);
        let at = (sent + delay).max(last);
        self.last_arrival.insert(lane, at);
        self.schedule(at, Event::Deliver { from, to, message });
    }

    fn on_control_tick(&mut self) {
        if !self.control_down {
            let running = self.api_running();
            let out = self.authority.tick::<M::Value>(self.now, &running);
            for (to, message) in out {
                self.send(CONTROL, to, message.into());
            }
        }
        let next = self.now + self.tick;
        self.schedule(next, Event::ControlTick);
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, message: M::Message) {
        if to == CONTROL {
            if !self.control_down
                && let Some(message) = M::into_store(message)
            {
                self.authority.receive(from, message);
            }
            return;
        }
        if to >= CLIENT_BASE {
            M::deliver_to_client(self, to, message);
            return;
        }
        let now = self.now;
        if !self.pods.get(&to).is_some_and(|pod| pod.alive) {
            if from < CONTROL {
                self.stats.to_gone += 1;
            }
            // A pod gone resets its streams.
            M::unreachable(self, from, to, &message);
            return;
        }
        let Some(pod) = self.pods.get_mut(&to) else {
            return;
        };
        if let Some(until) = pod.frozen(now) {
            self.schedule(until, Event::Deliver { from, to, message });
            return;
        }
        let local = pod.local(now);
        M::store_mut(&mut pod.node).set_wall_offset(wall_offset(now, local, pod.skew));
        let out = M::receive(&mut pod.node, from, message, local);
        self.after(to, out);
    }

    fn on_tick(&mut self, id: NodeId) {
        let now = self.now;
        let grace = self.grace;
        let Some(pod) = self.pods.get_mut(&id) else {
            return;
        };
        if !pod.alive {
            return;
        }
        if let Some(until) = pod.frozen(now) {
            self.schedule(until, Event::Tick(id));
            return;
        }
        let local = pod.local(now);
        M::store_mut(&mut pod.node).set_wall_offset(wall_offset(now, local, pod.skew));
        let out = M::tick(&mut pod.node, local);
        // A drained pod's process ends once what it has in flight is answered — requests it
        // forwarded, their answers coming back through it — as a graceful shutdown does.
        let drained = M::store(&pod.node).drained()
            && now.saturating_sub(*pod.drained_since.get_or_insert(now)) >= LINGER;
        let overdue = pod
            .draining_since
            .is_some_and(|since| now.saturating_sub(since) > grace);
        self.after(id, out);
        if drained || overdue {
            if let Some(pod) = self.pods.get_mut(&id) {
                pod.alive = false;
            }
            self.stopped.push((id, now, drained));
            return;
        }
        let next = now + self.tick;
        self.schedule(next, Event::Tick(id));
    }

    /// Sends what pod `id` gave back, then checks what must never happen.
    fn after(&mut self, id: NodeId, out: Vec<(NodeId, M::Message)>) {
        for (to, message) in out {
            self.send(id, to, message);
        }
        self.check(id);
    }

    fn check(&mut self, id: NodeId) {
        let Some(pod) = self.pods.get(&id) else {
            return;
        };
        if M::store(&pod.node).is_leader() {
            let term = M::store(&pod.node).term();
            match self.leaders.get(&term) {
                Some(other) if *other != id => {
                    self.stats
                        .violations
                        .push(format!("term {term}: leaders {other} and {id}"));
                }
                Some(_) => {}
                None => {
                    self.leaders.insert(term, id);
                    self.stats.terms += 1;
                }
            }
        }
        if let Some(table) = M::store(&pod.node).committed()
            && !self.tables.contains_key(&table.stamp())
        {
            let stamp = table.stamp();
            let accepted = |member: NodeId| {
                self.pods
                    .get(&member)
                    .is_some_and(|pod| M::store(&pod.node).accepted() >= stamp)
            };
            // The table before, in the same generation: a generation's first needs only those
            // the control plane named, its own voting set.
            let before = self
                .tables
                .range(..stamp)
                .next_back()
                .filter(|(before, _)| generation_of(before.0) == generation_of(stamp.0))
                .map(|(_, before)| before.voting.clone());
            let own = edgerush_cluster::table::is_quorum(&table.voting, accepted);
            let old =
                before.is_none_or(|voting| edgerush_cluster::table::is_quorum(&voting, accepted));
            if !own || !old {
                self.stats.violations.push(format!(
                    "{stamp:?} committed without a majority of {}",
                    if own {
                        "the voting set before"
                    } else {
                        "its voting set"
                    }
                ));
            }
        }
        let Some(pod) = self.pods.get(&id) else {
            return;
        };
        if let Some(table) = M::store(&pod.node).committed() {
            match self.tables.get(&table.stamp()) {
                Some(seen) if seen != table => {
                    let stamp = table.stamp();
                    self.stats
                        .violations
                        .push(format!("two tables committed as {stamp:?}"));
                }
                Some(_) => {}
                None => {
                    let stamp = table.stamp();
                    if let Some(base) = self.base_of(stamp) {
                        let base = &self.tables[&base];
                        let gone = |pod: NodeId| !self.pods.get(&pod).is_some_and(|pod| pod.alive);
                        self.stats.moved_to_gone += (0..table.shards.len())
                            .filter_map(|index| ShardId::try_from(index).ok())
                            .filter(|shard| {
                                let to = table.owner(*shard);
                                to != base.owner(*shard)
                                    && to.is_some_and(gone)
                                    && to == base.move_of(*shard)
                            })
                            .count() as u64;
                        if let Some(broken) = moved_on_badly(base, table) {
                            self.stats
                                .violations
                                .push(format!("{stamp:?} after {:?}: {broken}", base.stamp()));
                        }
                    }
                    self.tables.insert(stamp, table.clone());
                }
            }
            self.held.insert(id, table.stamp());
        }
    }

    /// The table one committed as `stamp` follows: the one before in its generation; or, the
    /// first of its generation, the latest held by a pod alive — the cluster formed again from
    /// what it left — and none if every pod holding one is gone.
    fn base_of(&self, stamp: (u64, u64)) -> Option<(u64, u64)> {
        let before = self
            .tables
            .range(..stamp)
            .next_back()
            .map(|(before, _)| *before)?;
        if generation_of(before.0) == generation_of(stamp.0) {
            return Some(before);
        }
        self.held
            .iter()
            .filter(|(pod, held)| self.pods.get(pod).is_some_and(|pod| pod.alive) && **held < stamp)
            .map(|(_, held)| *held)
            .max()
    }

    fn live_pods(&self) -> Vec<NodeId> {
        self.alive().collect()
    }

    /// How a request client `client` made ended, each key with its part's number: the truth
    /// as clients were told it, and under a rule, to the oracle, each part at the time its
    /// owner decided it — by its wall clock, and to the true-time oracle by the world's — or
    /// for one left to `when_unreachable`, the time then.
    fn record(&mut self, client: NodeId, outcome: Outcome, parts: &[(u64, u64)]) {
        let now = self.now;
        let (own, world): (Timed, Timed) = parts
            .iter()
            .map(|(key, id)| {
                let (own, world) = match outcome {
                    Outcome::Refused => (now, now),
                    _ => self
                        .decided
                        .get(&(client, *id))
                        .copied()
                        .unwrap_or((now, now)),
                };
                ((*key, own), (*key, world))
            })
            .unzip();
        match outcome {
            Outcome::Counted(took) => {
                self.stats.counted += 1;
                self.stats.latencies.push(took);
                if self.oracle.is_none() {
                    for (key, _) in parts {
                        *self.truth.entry(*key).or_insert(0) += 1;
                    }
                }
            }
            Outcome::Refused => {
                self.stats.refused += 1;
                self.stats.refused_at.push(now);
            }
            Outcome::Limited => self.stats.limited += 1,
        }
        let allow = self.limit.is_some_and(|(_, allow)| allow);
        for (oracle, timed) in [(&mut self.oracle, own), (&mut self.true_time, world)] {
            if let Some(oracle) = oracle {
                tell(oracle, outcome, allow, timed);
            }
        }
    }

    fn on_sample(&mut self) {
        let now = self.now;
        let step = self.sample;
        for shard in 0..self.config.shards {
            let servers: Vec<NodeId> = self
                .pods
                .iter()
                .filter(|(_, pod)| {
                    pod.alive
                        && pod.frozen(now).is_none()
                        && M::store(&pod.node).serves(shard, pod.local(now))
                })
                .map(|(id, _)| *id)
                .collect();
            let serving = servers.len();
            if serving > 1 {
                if !self.overlap_since.contains_key(&shard) && self.stats.overlaps.len() < 64 {
                    self.stats.overlaps.push((now, shard, servers.clone()));
                }
                let since = *self.overlap_since.entry(shard).or_insert(now);
                self.stats.overlap_total += step;
                self.stats.overlap_longest = self.stats.overlap_longest.max(now - since + step);
            } else {
                self.overlap_since.remove(&shard);
            }
            if serving == 0 {
                let since = *self.unserved_since.entry(shard).or_insert(now);
                self.stats.unserved_total += step;
                self.stats.unserved_longest = self.stats.unserved_longest.max(now - since + step);
            } else {
                self.unserved_since.remove(&shard);
            }
        }
        self.schedule(now + step, Event::Sample);
    }

    /// The shards no live pod serves now, each with the owner the latest table names.
    #[must_use]
    pub fn unserved_now(&self) -> Vec<(ShardId, Option<NodeId>)> {
        let now = self.now;
        let table = self.table();
        (0..self.config.shards)
            .filter(|shard| {
                !self
                    .pods
                    .values()
                    .any(|pod| pod.alive && M::store(&pod.node).serves(*shard, pod.local(now)))
            })
            .map(|shard| (shard, table.and_then(|table| table.owner(shard))))
            .collect()
    }

    /// Whether `id` is alive and ready, as its readiness probe would say, by its own clock.
    #[must_use]
    pub fn ready(&self, id: NodeId) -> bool {
        self.pods
            .get(&id)
            .is_some_and(|pod| pod.alive && M::store(&pod.node).ready(pod.local(self.now)))
    }

    /// The control plane's namings without the latest table, having waited it out.
    #[must_use]
    pub const fn named_without_the_latest(&self) -> u64 {
        self.authority.named_without_the_latest()
    }

    /// The pods it has started, alive or not.
    #[must_use]
    pub fn pods(&self) -> BTreeSet<NodeId> {
        self.pods.keys().copied().collect()
    }
}

/// The name of a store's message's kind.
const fn store_kind<V>(message: &StoreMessage<V>) -> &'static str {
    match message {
        StoreMessage::Heartbeat { .. } => "heartbeat",
        StoreMessage::TableRequest => "table request",
        StoreMessage::TableReply { .. } => "table reply",
        StoreMessage::PreVote { .. } | StoreMessage::PreVoteReply { .. } => "pre-vote",
        StoreMessage::StartJoin { .. } | StoreMessage::Join { .. } => "election",
        StoreMessage::Publish { .. } | StoreMessage::PublishChanges { .. } => "publish",
        StoreMessage::PublishAck { .. } | StoreMessage::Commit { .. } => "commit",
        StoreMessage::JoinCluster | StoreMessage::Drain => "join or drain",
        StoreMessage::Probe { .. } | StoreMessage::Report { .. } | StoreMessage::Form { .. } => {
            "control plane"
        }
        StoreMessage::Replicate { .. }
        | StoreMessage::Replicated { .. }
        | StoreMessage::Lacking { .. } => "replicate",
        StoreMessage::Handoff { .. }
        | StoreMessage::CaughtUp { .. }
        | StoreMessage::HandedOff { .. } => "handoff",
    }
}

/// About how many bytes a store's message takes on the wire.
fn store_size<V: Held>(message: &StoreMessage<V>) -> u64 {
    // Each key with its value, as the store sizes it, and each key dropped.
    let entries = |entries: &[(u64, edgerush_cluster::Kept<V>)], dropped: &[u64]| {
        entries.iter().map(|(_, kept)| kept.bytes()).sum::<u64>() + 8 * dropped.len() as u64
    };
    32 + match message {
        StoreMessage::Heartbeat { hears, .. } => 8 + 16 * hears.len() as u64,
        StoreMessage::TableReply { table } | StoreMessage::Publish { table } => {
            8 * (table.members.len() + table.voting.len()) as u64
                + table
                    .shards
                    .iter()
                    .map(|shard| 16 + 8 * shard.replicas.len() as u64)
                    .sum::<u64>()
        }
        // The table it changes, the lists whole and each shard changed with its number.
        StoreMessage::PublishChanges { changes, .. } => {
            16 + 8 * (changes.members.len() + changes.voting.len()) as u64
                + changes
                    .shards
                    .iter()
                    .map(|(_, shard)| 18 + 8 * shard.replicas.len() as u64)
                    .sum::<u64>()
        }
        StoreMessage::Replicate {
            entries: list,
            dropped,
            ..
        }
        | StoreMessage::Handoff {
            entries: list,
            dropped,
            ..
        } => entries(list, dropped),
        _ => 0,
    }
}

/// How `next` breaks the rules for a table following `base`, if it does: a shard's term never
/// falls, and its owner never changes without it rising — else two owners could each count a
/// key under one term, or a table formed again from nothing could drop what the pods held.
fn moved_on_badly(base: &Table, next: &Table) -> Option<String> {
    base.shards
        .iter()
        .zip(&next.shards)
        .enumerate()
        .find_map(|(index, (was, now))| {
            if now.term < was.term {
                Some(format!("shard {index}'s term fell"))
            } else if now.owner != was.owner && now.term == was.term {
                Some(format!("shard {index}'s owner changed in its term"))
            } else {
                None
            }
        })
}

/// A request's keys, each with the time its part was decided.
type Timed = Vec<(u64, Time)>;

/// Tells `oracle` how a request ended, each key with the time it was decided.
fn tell(oracle: &mut Oracle, outcome: Outcome, allow: bool, timed: Timed) {
    match outcome {
        Outcome::Counted(_) => {
            for (key, at) in timed {
                oracle.admitted(key, at, false);
            }
        }
        // Under `allow`, admitted, though counted nowhere.
        Outcome::Refused if allow => {
            for (key, at) in timed {
                oracle.admitted(key, at, true);
            }
        }
        Outcome::Refused => oracle.refused(timed),
        Outcome::Limited => oracle.limited(timed),
    }
}

/// The offset that puts a pod's wall clock `skew` off the world's at `now`, its own reading
/// `local`.
fn wall_offset(now: Time, local: Time, skew: i64) -> i64 {
    let gap = i128::from(now) - i128::from(local) + i128::from(skew);
    i64::try_from(gap).unwrap_or(0)
}
