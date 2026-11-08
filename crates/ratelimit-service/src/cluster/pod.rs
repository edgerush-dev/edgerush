//! A pod of the cluster store on sockets (cluster-store-transplant-plan.md, "T4d"): the
//! synchronous mode's [`Pod`], the store's [`Node`] within it, owned by one task, ticked every
//! [`TICK`] and on every message, on real time, with
//! what it sends going to its peers through a link each ([`link`]), and what they send it coming in
//! on the streams they dial to it, served by a [`StoreServer`].
//!
//! The store names pods by member id; the pod keeps where each is reached, from four places:
//! the members of its table, the voters of a `Form`, the hellos it is greeted with, and its
//! seeds — addresses only, asked who answers there until it is a member. A pod whose cluster
//! has another shard count is refused, asking or asked, and so is never taken in. A peer the
//! store forgets has its link dropped.
//!
//! A pod told to drain hands its shards on and leaves the voting set, as its store does; once
//! drained it stops, but first sends what it has queued — the last changes of the shards it
//! handed on among them — for at most [`STREAMS_IDLE`] (T4d2).
//!
//! A peer whose wall clock its store finds off from the pod's past `skew_warning` is told of
//! in the log, as it goes past and every [`SKEW_TOLD_AGAIN`] while it stays, and counted in
//! [`StorePod::skewed`] (T4d3): nothing corrects for skew.
//!
//! Clients ask it through a [`StoreClient`], in-process, as data planes will through a stream
//! for them (T5c): a request reaching a pod not its key's owner is forwarded one hop to the
//! owner, when forwarding is on, and answered back through it; a request under several keys
//! is gathered by the pod it reaches, each part asked of its owner (T5a). Data planes ask on a
//! stream of their own, `Decide`, each a client, told the pod's table as it opens and on each
//! commit ([`decide_stream`], T5c); the pods' methods are the pods' alone, and this one the data
//! planes'.

use super::link::{
    self, CONTROL, Counts, DECIDE, Dial, Dropped, GREET, HELLO, Link, Received, SERVICE, Tasks,
};
use super::wire::{
    self, Asking, Empty, Hello, Invalid, Joining, Outgoing, Piece, Synchronous, Telling, ToCopy,
    ToPeer, Value,
};
use crate::grpc::{Boxed, Client, MAX_MESSAGE, Prost, limited};
use crate::secure::{Caller, refused};
use edgerush_cluster::{Config, Message, Node, NodeId, Stamp, Table, Time};
use edgerush_cluster_synchronous::{Message as ModeMessage, Outbox, Pod, Value as SyncValue};
use http::uri::PathAndQuery;
use prost::Message as _;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, watch};
use tonic::server::NamedService;
use tonic::{Request, Response, Status, Streaming};

/// How often the pod's store is ticked, as the simulation ticks it.
pub const TICK: Duration = Duration::from_millis(5);

/// How often a pod that is no member asks its seeds who answers there.
const ASK_SEEDS: Duration = Duration::from_secs(1);

/// How long a drained pod waits for its streams to send what they hold before it stops
/// anyway (decided): what is left then, to a peer slow or cut off, is failed over as a crash's
/// is. Well inside the 30 s Kubernetes grants a pod to stop by default, beside the drain itself.
pub const STREAMS_IDLE: Duration = Duration::from_secs(5);

/// How often a peer whose wall clock stays off past `skew_warning` is told of again in the
/// log (decided): once a minute, so that a skew lasting is seen in recent lines.
pub const SKEW_TOLD_AGAIN: Duration = Duration::from_secs(60);

/// The control plane, as a pod's store names it: no pod's member id.
pub const CONTROL_PLANE: NodeId = NodeId::MAX;

/// Where clients' ids start, as a pod numbers those asking it: apart from every pod's member
/// id, as the simulation keeps its clients apart, and below the control plane's.
pub const CLIENTS: NodeId = 1 << 63;

/// How many answers wait for a client before more are dropped, as messages lost: a client
/// reading them slowly holds up no pod. Tables do not wait among them: a client is told only
/// the newest (#252).
const ANSWERS_WAITING: usize = 1_024;

/// How many messages received wait for the pod's task, across its streams: past it, each
/// stream waits to read on, and its sender's queue fills.
const RECEIVED_WAITING: usize = 64;

/// How many messages wait to be written to a data plane's stream: room for the answers of
/// several turns of the pod's task, so that they go out together, a write for many. One at a
/// time, every answer cost a write of its own, and the pod reached a third of the old pod's
/// rate at three keys a request (#253).
const TELLING_WAITING: usize = 64;

/// Something run on a pod's mode, its store within, in its task.
type Run = Box<dyn FnOnce(&mut Pod, Time) + Send>;

enum Command {
    Receive(NodeId, ModeMessage),
    Greeted(Hello),
    Control(
        Message<SyncValue>,
        oneshot::Sender<Option<Message<SyncValue>>>,
    ),
    With(Run),
    /// A client to be answered: its id, and where what it is told goes.
    Client(oneshot::Sender<(NodeId, Told)>),
    /// A client's request.
    Ask(NodeId, ModeMessage),
    /// Drain, and say so by dropping the sender once stopped.
    Drain(oneshot::Sender<()>),
    Stop,
}

/// What a store pod's scrape shows of it (`crate::scrape`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Observed {
    /// Whether it is ready, as its store has it.
    pub ready: bool,
    /// Whether it leads.
    pub leader: bool,
    /// The number of the table it has committed, `(0, 0)` for none.
    pub table: Stamp,
    /// Keys it holds, owned and copies.
    pub keys: u64,
    /// About the bytes they take.
    pub key_bytes: u64,
    /// What those bytes are capped at, if anything.
    pub max_key_bytes: Option<u64>,
    /// Keys forgotten, their time come.
    pub forgotten: u64,
    /// Keys evicted past the cap.
    pub evicted: u64,
    /// Peers whose wall clocks are off past `skew_warning` now.
    pub skewed: usize,
    /// What its sending has dropped.
    pub dropped: Dropped,
    /// Requests it decided from its copy, their owner unheard.
    pub decided_from_copy: u64,
}

/// A handle on a pod of the store, running. Cheap to clone.
pub struct StorePod {
    commands: mpsc::Sender<Command>,
    id: NodeId,
    hello: Arc<Hello>,
    counts: Arc<Counts>,
    skewed: Arc<AtomicUsize>,
}

impl Clone for StorePod {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            id: self.id,
            hello: Arc::clone(&self.hello),
            counts: Arc::clone(&self.counts),
            skewed: Arc::clone(&self.skewed),
        }
    }
}

impl StorePod {
    /// Pod `id`, reached at `address`, started with `config`, dialling its peers as `dial`
    /// says and asking `seeds` who answers there until it is a member.
    #[must_use]
    pub fn spawn(
        id: NodeId,
        address: String,
        config: Config,
        dial: Dial,
        seeds: Vec<String>,
    ) -> Self {
        let hello = Arc::new(Hello {
            from: id,
            address: address.clone(),
            shards: u32::from(config.shards),
        });
        let (commands, received) = mpsc::channel(RECEIVED_WAITING);
        let counts = Arc::new(Counts::default());
        let skewed = Arc::new(AtomicUsize::new(0));
        let pod = Self {
            commands: commands.clone(),
            id,
            hello: Arc::clone(&hello),
            counts: Arc::clone(&counts),
            skewed: Arc::clone(&skewed),
        };
        let running = Running {
            node: Pod::new(id, address, config, []),
            started: Instant::now(),
            hello,
            dial,
            seeds,
            asked_at: None,
            addresses: BTreeMap::new(),
            links: BTreeMap::new(),
            counts,
            commands: commands.downgrade(),
            drains: Vec::new(),
            skews: SkewWarnings::default(),
            skewed,
            clients: BTreeMap::new(),
            next_client: CLIENTS,
            told: (0, 0),
        };
        tokio::spawn(running.run(received));
        pod
    }

    /// Its member id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// What its sending has dropped, by why.
    #[must_use]
    pub fn dropped(&self) -> Dropped {
        self.counts.dropped()
    }

    /// How many peers' wall clocks its store finds off from its own past `skew_warning` now:
    /// zero while all agree.
    #[must_use]
    pub fn skewed(&self) -> usize {
        self.skewed.load(Ordering::Relaxed)
    }

    /// What its scrape shows of it, read at once between messages: `None` once it has stopped.
    pub async fn observed(&self) -> Option<Observed> {
        let (skewed, dropped) = (self.skewed(), self.dropped());
        self.with_pod(move |pod, now| {
            let node = pod.store();
            Observed {
                ready: node.ready(now),
                leader: node.is_leader(),
                table: node.committed_stamp(),
                keys: u64::try_from(node.key_count()).unwrap_or(u64::MAX),
                key_bytes: node.key_bytes(),
                max_key_bytes: node.config().max_key_bytes,
                forgotten: node.forgotten(),
                evicted: node.evicted(),
                skewed,
                dropped,
                decided_from_copy: pod.decided_from_copy(),
            }
        })
        .await
    }

    /// Runs `with` on its store, at its time, between messages: to read it or update keys,
    /// the copies going on its next tick. `None` once it has stopped.
    pub async fn with<R: Send + 'static>(
        &self,
        with: impl FnOnce(&mut Node<SyncValue>, Time) -> R + Send + 'static,
    ) -> Option<R> {
        self.with_pod(move |pod, now| with(pod.store_mut(), now))
            .await
    }

    /// Runs `with` on the mode's pod, as [`with`](Self::with) on its store.
    async fn with_pod<R: Send + 'static>(
        &self,
        with: impl FnOnce(&mut Pod, Time) -> R + Send + 'static,
    ) -> Option<R> {
        let (answer, answered) = oneshot::channel();
        let command = Command::With(Box::new(move |pod, now| {
            let _gone = answer.send(with(pod, now));
        }));
        self.commands.send(command).await.ok()?;
        answered.await.ok()
    }

    /// Whether it forwards a request for a key not its own to the owner its table names, one
    /// hop at most, rather than answer that the key is elsewhere: off as it starts.
    pub async fn set_forwarding(&self, forwarding: bool) {
        let command = Command::With(Box::new(move |pod, _| pod.set_forwarding(forwarding)));
        let _stopped = self.commands.send(command).await;
    }

    /// Whether, forwarding, it decides a data plane's request itself from its copy when it is
    /// a replica of the key's shard holding its lease and has not heard the owner for
    /// `stop_serving`: off as it starts.
    pub async fn set_forwarder_decides(&self, decides: bool) {
        let command = Command::With(Box::new(move |pod, _| pod.set_forwarder_decides(decides)));
        let _stopped = self.commands.send(command).await;
    }

    /// A client asking it in-process, answered through the handle: `None` once it has
    /// stopped.
    pub async fn client(&self) -> Option<StoreClient> {
        let (given, giving) = oneshot::channel();
        self.commands.send(Command::Client(given)).await.ok()?;
        let (id, told) = giving.await.ok()?;
        Some(StoreClient {
            id,
            commands: self.commands.clone(),
            told,
        })
    }

    /// Whether it is ready, as its store has it ([`Node::ready`]): never once it has stopped.
    pub async fn ready(&self) -> bool {
        self.with(|node, now| node.ready(now))
            .await
            .unwrap_or(false)
    }

    /// Drains it: its store hands its shards on and leaves the voting set, and once drained
    /// the pod stops, its streams first sending what they hold, for at most
    /// [`STREAMS_IDLE`]. Returns once it has stopped.
    pub async fn drain(&self) {
        let (stopped, stopping) = oneshot::channel();
        if self.commands.send(Command::Drain(stopped)).await.is_ok() {
            let _stopped = stopping.await;
        }
    }

    /// Stops it, as a pod gone: its store ticks and answers no more, and its links are
    /// dropped.
    pub async fn stop(&self) {
        let _stopped = self.commands.send(Command::Stop).await;
    }

    /// Whether `hello` is from a pod of its cluster's shard count, which another pod's is not
    /// (6a); if so, its sender's address is kept.
    async fn greet(&self, hello: &Hello) -> Result<(), Status> {
        may_be_peer(&self.hello, hello)?;
        let _stopped = self.commands.send(Command::Greeted(hello.clone())).await;
        Ok(())
    }

    /// Hands it a message from `from`, waiting while too many wait.
    async fn receive(&self, from: NodeId, message: ModeMessage) -> bool {
        self.commands
            .send(Command::Receive(from, message))
            .await
            .is_ok()
    }

    /// Hands it the control plane's message: its answer, if it has one.
    async fn control(&self, message: Message<SyncValue>) -> Option<Message<SyncValue>> {
        let (answer, answered) = oneshot::channel();
        self.commands
            .send(Command::Control(message, answer))
            .await
            .ok()?;
        answered.await.ok().flatten()
    }
}

/// A client asking a pod in-process, as data planes do through the pod's stream for them
/// (T5c): what it asks names it, and its answers come back here.
pub struct StoreClient {
    id: NodeId,
    commands: mpsc::Sender<Command>,
    told: Told,
}

/// Where a pod's task sends what it tells one client: its answers, in order, and beside them
/// the newest table it was sent, which takes the place of one not yet told (#252). A pod commits
/// tables in bursts while shards move — fifteen in ten milliseconds as one drains — and a client
/// needs only the last: told every one, in line with its answers, it found them late.
struct ToClient {
    answers: mpsc::Sender<ModeMessage>,
    table: watch::Sender<Option<Table>>,
}

/// A client's side of [`ToClient`].
struct Told {
    answers: mpsc::Receiver<ModeMessage>,
    table: watch::Receiver<Option<Table>>,
}

impl StoreClient {
    /// Its id, as what it asks names it.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Asks the pod `message`: a request, or a request to gather, naming this client; anything
    /// else is dropped. False once the pod has stopped.
    pub async fn ask(&self, message: ModeMessage) -> bool {
        self.commands
            .send(Command::Ask(self.id, message))
            .await
            .is_ok()
    }

    /// What it is told next: an answer, or the newest table it was sent, if it has not been
    /// told that one. An answer waiting goes first, so that tables delay none; a table waits
    /// only while answers do, a client behind meanwhile told a newer one by any pod it asks
    /// for a key that has moved ([`Answer::Elsewhere`]). `None` once the pod has stopped.
    ///
    /// Cancel-safe, as a stream serving the client needs it to be: a table is taken only in
    /// the poll that returns it.
    ///
    /// [`Answer::Elsewhere`]: edgerush_cluster_synchronous::Answer::Elsewhere
    pub async fn answer(&mut self) -> Option<ModeMessage> {
        loop {
            tokio::select! {
                biased;
                answer = self.told.answers.recv() => return answer,
                changed = self.told.table.changed() => {
                    changed.ok()?;
                    if let Some(table) = self.told.table.borrow_and_update().clone() {
                        return Some(ModeMessage::Store(Message::TableReply { table }));
                    }
                }
            }
        }
    }
}

/// The pod's task: its mode and store, and how it reaches its peers and clients.
struct Running {
    node: Pod,
    started: Instant,
    hello: Arc<Hello>,
    dial: Dial,
    seeds: Vec<String>,
    asked_at: Option<Instant>,
    /// Where each peer is reached, as last learnt.
    addresses: BTreeMap<NodeId, String>,
    links: BTreeMap<NodeId, Link>,
    counts: Arc<Counts>,
    /// For seeds' answers, which come back as hellos.
    commands: mpsc::WeakSender<Command>,
    /// Those waiting for it to stop, drained.
    drains: Vec<oneshot::Sender<()>>,
    skews: SkewWarnings,
    /// The peers skewed now, as [`StorePod::skewed`] says.
    skewed: Arc<AtomicUsize>,
    /// The clients asking it, each with where what it tells them goes.
    clients: BTreeMap<NodeId, ToClient>,
    next_client: NodeId,
    /// The table last told its clients, by its number.
    told: Stamp,
}

/// When each peer whose wall clock is off past `skew_warning` was last told of in the log.
#[derive(Debug, Default)]
struct SkewWarnings {
    told: BTreeMap<NodeId, Instant>,
}

impl SkewWarnings {
    /// Of `skewed`, those to tell of at `now`: each as it goes past, and again every
    /// [`SKEW_TOLD_AGAIN`] while it stays; one back within is forgotten, so that it is told
    /// of at once should it go past again.
    fn due(&mut self, skewed: &BTreeMap<NodeId, i64>, now: Instant) -> Vec<(NodeId, i64)> {
        self.told.retain(|peer, _| skewed.contains_key(peer));
        let mut due = Vec::new();
        for (peer, skew) in skewed {
            let told = self.told.get(peer);
            if told.is_none_or(|at| now.duration_since(*at) >= SKEW_TOLD_AGAIN) {
                self.told.insert(*peer, now);
                due.push((*peer, *skew));
            }
        }
        due
    }
}

impl Running {
    /// Its time, as its store reads it, with its wall clock set from the system's.
    fn now(&mut self) -> Time {
        let now = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(Time::MAX);
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            });
        let offset = i128::from(wall) - i128::from(now);
        self.node
            .store_mut()
            .set_wall_offset(i64::try_from(offset).unwrap_or(i64::MAX));
        now
    }

    async fn run(mut self, mut received: mpsc::Receiver<Command>) {
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticks.tick() => {
                    let now = self.now();
                    let out = self.node.tick(now);
                    self.send(out);
                    self.follow();
                    self.ask_seeds();
                    self.warn_skews();
                }
                command = received.recv() => match command {
                    None | Some(Command::Stop) => return,
                    Some(Command::Receive(from, message)) => {
                        let now = self.now();
                        let out = self.node.receive(from, message, now);
                        self.send(out);
                    }
                    Some(Command::Greeted(hello)) => self.greeted(hello),
                    Some(Command::Control(message, answer)) => {
                        if let Message::Form { voters, .. } = &message {
                            for (voter, address) in voters {
                                self.learn(*voter, address, false);
                            }
                        }
                        let now = self.now();
                        let mut out =
                            self.node.receive(CONTROL_PLANE, ModeMessage::Store(message), now);
                        let to_control = out.iter().position(|(to, _)| *to == CONTROL_PLANE);
                        let answered = to_control.and_then(|at| match out.remove(at).1 {
                            ModeMessage::Store(answered) => Some(answered),
                            _ => None,
                        });
                        let _gone = answer.send(answered);
                        self.send(out);
                    }
                    Some(Command::With(with)) => {
                        let now = self.now();
                        with(&mut self.node, now);
                    }
                    Some(Command::Client(given)) => self.client(given),
                    Some(Command::Ask(client, message)) => {
                        if asks_as(&message, client) {
                            let now = self.now();
                            let out = self.node.receive(client, message, now);
                            self.send(out);
                        }
                    }
                    Some(Command::Drain(stopped)) => {
                        self.node.store_mut().drain();
                        self.drains.push(stopped);
                    }
                },
            }
            self.tell_tables();
            if self.node.store().drained() {
                break;
            }
        }
        // Its store stopped, nothing more reaches it; what it sent still goes.
        drop(received);
        self.stop_drained().await;
    }

    /// Stops it, drained, once its links have sent what they hold, or after
    /// [`STREAMS_IDLE`] whatever is left, and then tells those waiting.
    async fn stop_drained(mut self) {
        let mut closing: Vec<Tasks> = std::mem::take(&mut self.links)
            .into_values()
            .map(Link::close)
            .collect();
        let idle = async {
            for tasks in &mut closing {
                tasks.ended().await;
            }
        };
        let _late = tokio::time::timeout(STREAMS_IDLE, idle).await;
        drop(closing);
        drop(self);
    }

    /// Sends `out`, each message to its client, or to its peer's link, made if it has none:
    /// dropped if no address is known for the peer.
    fn send(&mut self, out: Outbox) {
        for (to, message) in out {
            if (CLIENTS..CONTROL_PLANE).contains(&to) {
                self.answer(to, message);
                continue;
            }
            if !self.links.contains_key(&to) {
                let made = self.addresses.get(&to).and_then(|address| {
                    Link::new(address, &self.hello, &self.dial, &self.counts).ok()
                });
                match made {
                    Some(link) => {
                        self.links.insert(to, link);
                    }
                    None => {
                        self.counts
                            .lost
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                }
            }
            if let (Some(link), Some(outgoing)) = (
                self.links.get(&to),
                wire::mode_wire(self.hello.from, message),
            ) {
                link.send(&outgoing);
            }
        }
    }

    /// Hands `message` to client `client`, if it is still there, and forgets the client once
    /// it is gone: a table in place of an older one it has not been told, and never after a
    /// newer; anything else after its answers so far, dropped past [`ANSWERS_WAITING`], as a
    /// message lost.
    fn answer(&mut self, client: NodeId, message: ModeMessage) {
        let Some(to) = self.clients.get(&client) else {
            return;
        };
        let gone = match message {
            ModeMessage::Store(Message::TableReply { table }) => {
                to.table.send_if_modified(|held| {
                    let newer = held
                        .as_ref()
                        .is_none_or(|held| table.stamp() > held.stamp());
                    if newer {
                        *held = Some(table);
                    }
                    newer
                });
                to.table.is_closed()
            }
            message => matches!(
                to.answers.try_send(message),
                Err(mpsc::error::TrySendError::Closed(_))
            ),
        };
        if gone {
            self.clients.remove(&client);
        }
    }

    /// A new client: its id, the next from [`CLIENTS`], and where what it is told goes, the
    /// table to tell it first if there is one yet.
    fn client(&mut self, given: oneshot::Sender<(NodeId, Told)>) {
        let id = self.next_client;
        self.next_client = self.next_client.saturating_add(1);
        let (answers, answered) = mpsc::channel(ANSWERS_WAITING);
        let (table, tables) = watch::channel(None);
        let told = Told {
            answers: answered,
            table: tables,
        };
        if given.send((id, told)).is_ok() {
            if let Some(committed) = self.node.store().committed() {
                table.send_replace(Some(committed.clone()));
            }
            self.clients.insert(id, ToClient { answers, table });
        }
    }

    /// Tells every client the table it has committed, as it commits one: in place of an older
    /// one the client has not yet been told.
    fn tell_tables(&mut self) {
        let Some(table) = self.node.store().committed() else {
            return;
        };
        if table.stamp() == self.told {
            return;
        }
        self.told = table.stamp();
        let table = table.clone();
        let clients: Vec<NodeId> = self.clients.keys().copied().collect();
        for client in clients {
            let told = ModeMessage::Store(Message::TableReply {
                table: table.clone(),
            });
            self.answer(client, told);
        }
    }

    /// Keeps where `peer` is reached: `heard`, from the peer itself, it takes the address
    /// from any other id there, a pod restarted at the same address being a new member;
    /// otherwise, from a table or a naming, only an address no other id holds.
    fn learn(&mut self, peer: NodeId, address: &str, heard: bool) {
        if peer == self.hello.from || peer >= CLIENTS {
            return;
        }
        let others: Vec<NodeId> = self
            .addresses
            .iter()
            .filter(|(other, held)| **other != peer && *held == address)
            .map(|(other, _)| *other)
            .collect();
        if !heard && !others.is_empty() {
            return;
        }
        for other in others {
            self.addresses.remove(&other);
            self.links.remove(&other);
        }
        if self
            .addresses
            .get(&peer)
            .is_some_and(|held| held != address)
        {
            self.links.remove(&peer);
        }
        self.addresses.insert(peer, address.to_owned());
        self.node.store_mut().add_peer(peer);
    }

    fn greeted(&mut self, hello: Hello) {
        self.learn(hello.from, &hello.address, true);
    }

    /// Keeps the addresses its table names, and lets go of the peers its store has
    /// forgotten.
    fn follow(&mut self) {
        let members: Vec<(NodeId, String)> =
            self.node
                .store()
                .committed()
                .map_or_else(Vec::new, |table| {
                    table
                        .members
                        .iter()
                        .filter(|(member, address)| self.addresses.get(member) != Some(*address))
                        .map(|(member, address)| (*member, address.clone()))
                        .collect()
                });
        for (member, address) in members {
            self.learn(member, &address, false);
        }
        let peers = self.node.store().peers();
        self.links.retain(|peer, _| peers.contains(peer));
        self.addresses.retain(|peer, _| peers.contains(peer));
    }

    /// Tells of the peers whose wall clocks its store finds off past `skew_warning`: in the
    /// log, as [`SkewWarnings`] has it, and in how many there are.
    fn warn_skews(&mut self) {
        let skewed = self.node.store().skewed();
        self.skewed.store(skewed.len(), Ordering::Relaxed);
        for (peer, skew) in self.skews.due(&skewed, Instant::now()) {
            let way = if skew > 0 { "ahead of" } else { "behind" };
            eprintln!(
                "edgerush-ratelimit-service: pod {peer}'s wall clock is {} ms {way} this pod's",
                skew.unsigned_abs() / 1_000_000
            );
        }
    }

    /// Asks each seed who answers there, every [`ASK_SEEDS`] while it is no member: the
    /// answers come back as hellos.
    fn ask_seeds(&mut self) {
        let member = self
            .node
            .store()
            .committed()
            .is_some_and(|table| table.has_member(self.hello.from));
        let due = self
            .asked_at
            .is_none_or(|asked| asked.elapsed() >= ASK_SEEDS);
        if member || !due || self.seeds.is_empty() {
            return;
        }
        self.asked_at = Some(Instant::now());
        for seed in &self.seeds {
            let client = match &self.dial {
                Dial::Plain => Client::new(seed),
                Dial::Secured(tls) => Client::secured(seed, tls.clone()),
            };
            let (Ok(client), Some(commands)) = (client, self.commands.upgrade()) else {
                continue;
            };
            let hello = Hello::clone(&self.hello);
            tokio::spawn(async move {
                let answer: Result<Hello, _> = client.call(hello.clone(), GREET).await;
                if let Ok(answer) = answer
                    && may_be_peer(&hello, &answer).is_ok()
                {
                    let _stopped = commands.send(Command::Greeted(answer)).await;
                }
            });
        }
    }
}

/// Whether `message`, asked by client `client`, is one a client asks, naming it: a request or a
/// request to gather, not one forwarded.
const fn asks_as(message: &ModeMessage, client: NodeId) -> bool {
    match message {
        ModeMessage::Request {
            client: asker,
            via: None,
            ..
        }
        | ModeMessage::Gather { client: asker, .. } => *asker == client,
        _ => false,
    }
}

/// Whether the pod that says `other` may be a peer of the one that says `own`, asking or
/// asked: of the same shard count, as a pod of another cluster is not (6a), and neither that
/// pod nor a client or the control plane.
fn may_be_peer(own: &Hello, other: &Hello) -> Result<(), Status> {
    if other.shards != own.shards {
        return Err(Status::failed_precondition(format!(
            "a cluster of {} shards, not {}",
            own.shards, other.shards
        )));
    }
    if other.from >= CLIENTS || other.from == own.from {
        return Err(Status::invalid_argument("a member id not a peer's"));
    }
    Ok(())
}

/// Asks the pod at `address` the control plane's `message` (`Probe` or `Form`), as the
/// controller does: the member id of the pod that answered, which every answer carries, and
/// its answer, if it has one.
///
/// # Errors
///
/// When the call fails or its answer cannot be used, and for a message that is not the
/// control plane's.
pub async fn ask<V: Value>(
    client: &Client,
    message: Message<V>,
) -> Result<(NodeId, Option<Message<V>>), crate::grpc::Error>
where
    V::Wire: Send + 'static,
{
    let Outgoing::Peer(asked) = wire::wire(CONTROL_PLANE, message) else {
        return Err(Status::invalid_argument("not the control plane's").into());
    };
    let answer: ToPeer<V::Wire> = client.call(asked, CONTROL).await?;
    let from = answer.from;
    if answer.message.is_none() {
        return Ok((from, None));
    }
    // An answer the store cannot use, as the service's others are refused (`crate::wire`).
    let (_, message) = wire::from_peer::<V>(answer)
        .map_err(|invalid| Status::invalid_argument(invalid.to_string()))?;
    Ok((from, Some(message)))
}

/// A data plane's stream to a pod: what it asks goes up, in order, and what the pod tells it —
/// its table, and each answer — comes down.
#[derive(Debug)]
pub struct DecideStream {
    /// Requests and requests to gather.
    pub up: mpsc::Sender<Asking>,
    /// The pod's table and its answers, as they come.
    pub down: Streaming<Telling>,
}

/// Opens a data plane's stream to the pod at `address` (`host:port`), dialled as `dial` says,
/// pinged as the service's other clients are, and held open for as long as both sides keep
/// it.
///
/// # Errors
///
/// When `address` cannot be dialled, or the stream cannot be opened.
pub async fn decide_stream(address: &str, dial: &Dial) -> Result<DecideStream, crate::grpc::Error> {
    let channel = link::channel(address, dial, None)?;
    let mut grpc = tonic::client::Grpc::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    grpc.ready()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let (up, asked) = mpsc::channel(ANSWERS_WAITING);
    let response = grpc
        .streaming(
            Request::new(Received(asked)),
            PathAndQuery::from_static(DECIDE),
            Prost::<Asking, Telling>::default(),
        )
        .await?;
    Ok(DecideStream {
        up,
        down: response.into_inner(),
    })
}

/// The store's service as a gRPC server, in front of its pod.
pub struct StoreServer {
    pod: StorePod,
}

impl Clone for StoreServer {
    fn clone(&self) -> Self {
        Self {
            pod: self.pod.clone(),
        }
    }
}

impl StoreServer {
    /// Serves `pod`.
    #[must_use]
    pub const fn new(pod: StorePod) -> Self {
        Self { pod }
    }
}

impl NamedService for StoreServer {
    const NAME: &'static str = SERVICE;
}

/// Which stream a peer opened.
#[derive(Clone, Copy)]
enum Kind {
    Peer,
    Copy,
}

struct StreamCall {
    pod: StorePod,
    kind: Kind,
}

impl tonic::server::StreamingService<Piece> for StreamCall {
    type Response = Empty;
    type ResponseStream = Received<Result<Empty, Status>>;
    type Future = Boxed<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Streaming<Piece>>) -> Self::Future {
        let pod = self.pod.clone();
        let kind = self.kind;
        Box::pin(async move {
            let hello = request
                .metadata()
                .get_bin(HELLO)
                .and_then(|said| said.to_bytes().ok())
                .and_then(|said| Hello::decode(said).ok())
                .ok_or_else(|| Status::invalid_argument("a stream with no hello"))?;
            pod.greet(&hello).await?;
            let (ended, ending) = mpsc::channel(1);
            tokio::spawn(take_all(pod, kind, hello.from, request.into_inner(), ended));
            Ok(Response::new(Received(ending)))
        })
    }
}

/// Takes the messages on one stream from `from`, each put together from its pieces and handed
/// to the pod, until the stream ends, or carries a message longer than a receiver takes or
/// from another sender, which ends it.
async fn take_all(
    pod: StorePod,
    kind: Kind,
    from: NodeId,
    mut pieces: Streaming<Piece>,
    ended: mpsc::Sender<Result<Empty, Status>>,
) {
    let mut joining = Joining::new(MAX_MESSAGE);
    while let Ok(Some(piece)) = pieces.message().await {
        let encoded = match joining.take(&piece) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => continue,
            Err(too_long) => {
                let _gone = ended
                    .send(Err(Status::resource_exhausted(too_long.to_string())))
                    .await;
                return;
            }
        };
        let decoded: Result<(NodeId, ModeMessage), Invalid> = match kind {
            Kind::Peer => ToPeer::<Synchronous>::decode(encoded.as_slice())
                .map_err(|_| Invalid::Missing)
                .and_then(wire::mode_from_peer),
            Kind::Copy => ToCopy::<Synchronous>::decode(encoded.as_slice())
                .map_err(|_| Invalid::Missing)
                .and_then(wire::mode_from_copy),
        };
        // A message the mode or its store cannot use is dropped whole, as a message lost.
        let Ok((sender, message)) = decoded else {
            continue;
        };
        if sender != from {
            let _gone = ended
                .send(Err(Status::invalid_argument(
                    "a message from another sender",
                )))
                .await;
            return;
        }
        if !pod.receive(from, message).await {
            return;
        }
    }
}

/// A data plane's stream: one client of the pod's.
struct DecideCall(StorePod);

impl tonic::server::StreamingService<Asking> for DecideCall {
    type Response = Telling;
    type ResponseStream = Received<Result<Telling, Status>>;
    type Future = Boxed<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Streaming<Asking>>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let client = pod
                .client()
                .await
                .ok_or_else(|| Status::unavailable("the pod has stopped"))?;
            Ok(Response::new(data_plane_stream(
                client,
                request.into_inner(),
            )))
        })
    }
}

/// A data plane's stream served as `client`, `asked` what it asks: what is to be written to
/// it, at most [`TELLING_WAITING`] waiting.
fn data_plane_stream<A>(client: StoreClient, asked: A) -> Received<Result<Telling, Status>>
where
    A: futures_core::Stream<Item = Result<Asking, Status>> + Unpin + Send + 'static,
{
    let (telling, told) = mpsc::channel(TELLING_WAITING);
    tokio::spawn(serve_data_plane(client, asked, telling));
    Received(told)
}

/// Serves one data plane's stream as `client`: what it asks handed to the pod, and what the
/// pod tells the client sent down, until either side ends it. Its requests are read only as the
/// pod takes them, and its answers wait in the client's queue while the data plane reads
/// slowly.
async fn serve_data_plane<A>(
    mut client: StoreClient,
    mut asked: A,
    telling: mpsc::Sender<Result<Telling, Status>>,
) where
    A: futures_core::Stream<Item = Result<Asking, Status>> + Unpin,
{
    loop {
        tokio::select! {
            asking = std::future::poll_fn(|cx| std::pin::Pin::new(&mut asked).poll_next(cx)) => {
                let Some(Ok(asking)) = asking else {
                    return;
                };
                // One the mode cannot use is dropped, as a message lost.
                if let Ok(message) = wire::asked_from(asking, client.id())
                    && !client.ask(message).await
                {
                    return;
                }
            }
            told = client.answer() => {
                let Some(told) = told else {
                    return;
                };
                if let Some(told) = wire::telling(told)
                    && telling.send(Ok(told)).await.is_err()
                {
                    return;
                }
            }
        }
    }
}

struct GreetCall(StorePod);

impl tonic::server::UnaryService<Hello> for GreetCall {
    type Response = Hello;
    type Future = Boxed<Result<Response<Hello>, Status>>;

    fn call(&mut self, request: Request<Hello>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            pod.greet(request.get_ref()).await?;
            Ok(Response::new(Hello::clone(&pod.hello)))
        })
    }
}

struct ControlCall(StorePod);

impl tonic::server::UnaryService<ToPeer<Synchronous>> for ControlCall {
    type Response = ToPeer<Synchronous>;
    type Future = Boxed<Result<Response<ToPeer<Synchronous>>, Status>>;

    fn call(&mut self, request: Request<ToPeer<Synchronous>>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let (_, message) = wire::from_peer::<SyncValue>(request.into_inner())
                .map_err(|invalid| Status::invalid_argument(invalid.to_string()))?;
            if !matches!(message, Message::Probe { .. } | Message::Form { .. }) {
                return Err(Status::invalid_argument("not the control plane's"));
            }
            let message = match pod.control(message).await {
                Some(answer) => match wire::wire(pod.id, answer) {
                    Outgoing::Peer(answer) => answer.message,
                    Outgoing::Copy(_) => None,
                },
                None => None,
            };
            Ok(Response::new(ToPeer {
                from: pod.id,
                message,
            }))
        })
    }
}

impl tower_service::Service<http::Request<tonic::body::Body>> for StoreServer {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let pod = self.pod.clone();
        Box::pin(async move {
            // The data planes' stream is theirs; every other method the pods'.
            let caller = if request.uri().path() == DECIDE {
                Caller::DataPlane
            } else {
                Caller::Pod
            };
            if let Some(status) = refused(&request, caller) {
                return Ok(status.into_http());
            }
            Ok(match request.uri().path() {
                super::link::PEER => {
                    let mut grpc = limited(Prost::<Empty, Piece>::default());
                    let call = StreamCall {
                        pod,
                        kind: Kind::Peer,
                    };
                    grpc.streaming(call, request).await
                }
                super::link::COPY => {
                    let mut grpc = limited(Prost::<Empty, Piece>::default());
                    let call = StreamCall {
                        pod,
                        kind: Kind::Copy,
                    };
                    grpc.streaming(call, request).await
                }
                DECIDE => {
                    let mut grpc = limited(Prost::<Telling, Asking>::default());
                    grpc.streaming(DecideCall(pod), request).await
                }
                GREET => {
                    let mut grpc = limited(Prost::<Hello, Hello>::default());
                    grpc.unary(GreetCall(pod), request).await
                }
                CONTROL => {
                    let mut grpc =
                        limited(Prost::<ToPeer<Synchronous>, ToPeer<Synchronous>>::default());
                    grpc.unary(ControlCall(pod), request).await
                }
                _ => Status::unimplemented("no such method").into_http(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::ms;

    fn hello(from: NodeId, shards: u32) -> Hello {
        Hello {
            from,
            address: format!("pod-{from}:7000"),
            shards,
        }
    }

    /// A pod's task as it starts, before it runs.
    fn running() -> Running {
        let config = Config::defaults();
        let (commands, _) = mpsc::channel(1);
        Running {
            node: Pod::new(1, "pod-1:7000".to_owned(), config, []),
            started: Instant::now(),
            hello: Arc::new(hello(1, u32::from(config.shards))),
            dial: Dial::Plain,
            seeds: Vec::new(),
            asked_at: None,
            addresses: BTreeMap::new(),
            links: BTreeMap::new(),
            counts: Arc::default(),
            commands: commands.downgrade(),
            drains: Vec::new(),
            skews: SkewWarnings::default(),
            skewed: Arc::default(),
            clients: BTreeMap::new(),
            next_client: CLIENTS,
            told: (0, 0),
        }
    }

    /// A table numbered `version`, in term 1.
    fn table(version: u64) -> Table {
        Table {
            term: 1,
            version,
            members: BTreeMap::new(),
            voting: Vec::new(),
            draining: Vec::new(),
            shards: Vec::new(),
            moves: Vec::new(),
        }
    }

    fn told_table(version: u64) -> ModeMessage {
        ModeMessage::Store(Message::TableReply {
            table: table(version),
        })
    }

    fn reply(id: u64) -> ModeMessage {
        ModeMessage::Reply {
            client: CLIENTS,
            id,
            answer: edgerush_cluster_synchronous::Answer::Counted(id),
        }
    }

    /// A client that has not read is told the newest of the tables sent it, once, never an
    /// older one after it, and every answer waiting before it: a burst of commits costs its
    /// answers no wait behind tables it would only replace (#252).
    #[tokio::test]
    async fn a_client_is_told_its_answers_first_and_the_newest_table_only() {
        let mut running = running();
        let (given, giving) = oneshot::channel();
        running.client(given);
        let (id, told) = giving.await.unwrap();
        let (commands, _) = mpsc::channel(1);
        let mut client = StoreClient { id, commands, told };
        let next = Duration::from_millis(50);
        for version in [2, 3, 1] {
            running.answer(id, told_table(version));
        }
        running.answer(id, reply(1));
        assert_eq!(client.answer().await, Some(reply(1)));
        assert_eq!(client.answer().await, Some(told_table(3)));
        let more = tokio::time::timeout(next, client.answer()).await;
        assert!(more.is_err(), "told {more:?} after the newest table");
        // A table and an answer both waiting: the answer, every time.
        for round in 0..20 {
            running.answer(id, told_table(10 + round));
            running.answer(id, reply(round));
            assert_eq!(client.answer().await, Some(reply(round)), "round {round}");
            assert_eq!(client.answer().await, Some(told_table(10 + round)));
        }
    }

    /// A data plane that asks nothing more: its stream of asks never ends.
    struct Quiet;

    impl futures_core::Stream for Quiet {
        type Item = Result<Asking, Status>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    /// Every answer waiting for a data plane is handed to its stream's writer at once, before
    /// the writer takes any: the answers of one turn of the pod's go out in one write, not one
    /// write each (#253).
    #[tokio::test]
    async fn a_data_planes_answers_waiting_are_handed_to_its_writer_together() {
        let mut running = running();
        let (given, giving) = oneshot::channel();
        running.client(given);
        let (id, told) = giving.await.unwrap();
        let (commands, _commands) = mpsc::channel(1);
        let client = StoreClient { id, commands, told };
        let waiting = u64::try_from(TELLING_WAITING).unwrap();
        for answer in 0..waiting {
            running.answer(id, reply(answer));
        }
        let Received(written) = data_plane_stream(client, Quiet);
        let handed = tokio::time::timeout(Duration::from_secs(1), async {
            while written.len() < TELLING_WAITING {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            handed.is_ok(),
            "{} of {TELLING_WAITING} answers handed to the writer",
            written.len()
        );
    }

    #[test]
    fn the_peers_its_store_finds_skewed_are_counted_and_none_once_back() {
        let mut running = running();
        // Its own heartbeat sent at 950 ms echoed at 1 s, and at 1.1 s.
        let beat = |sent, wall| Message::Heartbeat {
            term: 0,
            leader: None,
            committed: (0, 0),
            fresh: true,
            sent,
            wall,
            hears: vec![(1, ms(950))],
            leased: false,
        };
        let _ = running
            .node
            .store_mut()
            .receive(2, beat(ms(10), ms(1_275)), ms(1_000));
        running.warn_skews();
        assert_eq!(running.skewed.load(Ordering::Relaxed), 1);
        let _ = running
            .node
            .store_mut()
            .receive(2, beat(ms(20), ms(1_025)), ms(1_100));
        running.warn_skews();
        assert_eq!(running.skewed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_peer_skewed_is_told_of_as_it_goes_past_and_every_minute_while_it_stays() {
        let mut warnings = SkewWarnings::default();
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let skewed = BTreeMap::from([(2, 300_000_000), (3, -400_000_000)]);
        assert_eq!(
            warnings.due(&skewed, at(0)),
            vec![(2, 300_000_000), (3, -400_000_000)]
        );
        assert!(warnings.due(&skewed, at(59)).is_empty());
        // Pod 3 back within: forgotten, and told of at once going past again.
        let only = BTreeMap::from([(2, 300_000_000)]);
        assert!(warnings.due(&only, at(59)).is_empty());
        assert_eq!(warnings.due(&skewed, at(59)), vec![(3, -400_000_000)]);
        assert_eq!(warnings.due(&skewed, at(60)), vec![(2, 300_000_000)]);
    }

    #[test]
    fn a_pod_heard_at_another_ids_address_takes_it_and_a_table_does_not() {
        let mut running = running();
        running.learn(2, "pod-2:7000", false);
        // A table still naming a pod gone takes nothing from one heard there since.
        running.learn(3, "pod-2:7000", true);
        running.learn(2, "pod-2:7000", false);
        assert_eq!(
            running.addresses.get(&3).map(String::as_str),
            Some("pod-2:7000")
        );
        assert!(!running.addresses.contains_key(&2));
        // Nor does a naming take another's.
        running.learn(4, "pod-2:7000", false);
        assert!(!running.addresses.contains_key(&4));
        assert!(running.node.store().peers().contains(&3));
    }

    #[test]
    fn a_client_asks_only_requests_naming_it_and_not_forwarded() {
        let request = |client, via| ModeMessage::Request {
            client,
            id: 1,
            key: 7,
            stamp: (0, 0),
            hops: 0,
            via,
            give_back: false,
            rule: None,
            receipt: 0,
        };
        let client = CLIENTS + 4;
        assert!(asks_as(&request(client, None), client));
        assert!(!asks_as(&request(client + 1, None), client));
        assert!(!asks_as(&request(client, Some(2)), client));
        let gather = |asker| ModeMessage::Gather {
            client: asker,
            id: 1,
            keys: vec![7, 8],
        };
        assert!(asks_as(&gather(client), client));
        assert!(!asks_as(&gather(2), client));
        let reply = ModeMessage::Reply {
            client,
            id: 1,
            answer: edgerush_cluster_synchronous::Answer::Counted(1),
        };
        assert!(!asks_as(&reply, client));
    }

    #[test]
    fn a_pod_of_the_same_shard_count_may_be_a_peer_and_no_other() {
        let own = hello(1, 16);
        assert!(may_be_peer(&own, &hello(2, 16)).is_ok());
        let other = may_be_peer(&own, &hello(2, 32)).unwrap_err();
        assert_eq!(other.code(), tonic::Code::FailedPrecondition);
        assert!(may_be_peer(&own, &hello(1, 16)).is_err());
        assert!(may_be_peer(&own, &hello(CONTROL_PLANE, 16)).is_err());
        assert!(may_be_peer(&own, &hello(CLIENTS, 16)).is_err());
    }
}
