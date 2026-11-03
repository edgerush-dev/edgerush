//! A pod of the cluster store on sockets (cluster-store-transplant-plan.md, "T4d"): its
//! [`Node`] owned by one task, ticked every [`TICK`] and on every message, on real time, with
//! what it sends going to its peers through a link each ([`link`](super::link)), and what they send it coming in
//! on the streams they dial to it, served by a [`StoreServer`].
//!
//! The store names pods by member id; the pod keeps where each is reached, from four places:
//! the members of its table, the voters of a `Form`, the hellos it is greeted with, and its
//! seeds — addresses only, asked who answers there until it is a member. A pod whose cluster
//! has another shard count is refused, asking or asked, and so is never taken in. A peer the
//! store forgets has its link dropped.

use super::link::{CONTROL, Counts, Dial, Dropped, GREET, HELLO, Link, SERVICE};
use super::wire::{self, Empty, Hello, Invalid, Joining, Outgoing, Piece, ToCopy, ToPeer, Value};
use crate::grpc::{Boxed, Client, MAX_MESSAGE, Prost, limited};
use crate::secure::{Caller, refused};
use crate::synchronous::Received;
use edgerush_cluster::{Config, Message, Node, NodeId, Outbox, Time};
use prost::Message as _;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tonic::server::NamedService;
use tonic::{Request, Response, Status, Streaming};

/// How often the pod's store is ticked, as the simulation ticks it.
pub const TICK: Duration = Duration::from_millis(5);

/// How often a pod that is no member asks its seeds who answers there.
const ASK_SEEDS: Duration = Duration::from_secs(1);

/// The control plane, as a pod's store names it: no pod's member id.
pub const CONTROL_PLANE: NodeId = NodeId::MAX;

/// How many messages received wait for the pod's task, across its streams: past it, each
/// stream waits to read on, and its sender's queue fills.
const RECEIVED_WAITING: usize = 64;

/// Something run on a pod's store, in its task.
type Run<V> = Box<dyn FnOnce(&mut Node<V>, Time) + Send>;

enum Command<V> {
    Receive(NodeId, Message<V>),
    Greeted(Hello),
    Control(Message<V>, oneshot::Sender<Option<Message<V>>>),
    With(Run<V>),
    Stop,
}

/// A handle on a pod of the store, running. Cheap to clone.
pub struct StorePod<V> {
    commands: mpsc::Sender<Command<V>>,
    id: NodeId,
    hello: Arc<Hello>,
    counts: Arc<Counts>,
}

impl<V> Clone for StorePod<V> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            id: self.id,
            hello: Arc::clone(&self.hello),
            counts: Arc::clone(&self.counts),
        }
    }
}

impl<V: Value + Send + 'static> StorePod<V>
where
    V::Wire: Send,
{
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
        let pod = Self {
            commands: commands.clone(),
            id,
            hello: Arc::clone(&hello),
            counts: Arc::clone(&counts),
        };
        let running = Running {
            node: Node::new(id, address, config, []),
            started: Instant::now(),
            hello,
            dial,
            seeds,
            asked_at: None,
            addresses: BTreeMap::new(),
            links: BTreeMap::new(),
            counts,
            commands: commands.downgrade(),
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

    /// Runs `with` on its store, at its time, between messages: to read it or update keys,
    /// the copies going on its next tick. `None` once it has stopped.
    pub async fn with<R: Send + 'static>(
        &self,
        with: impl FnOnce(&mut Node<V>, Time) -> R + Send + 'static,
    ) -> Option<R> {
        let (answer, answered) = oneshot::channel();
        let command = Command::With(Box::new(move |node, now| {
            let _gone = answer.send(with(node, now));
        }));
        self.commands.send(command).await.ok()?;
        answered.await.ok()
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
    async fn receive(&self, from: NodeId, message: Message<V>) -> bool {
        self.commands
            .send(Command::Receive(from, message))
            .await
            .is_ok()
    }

    /// Hands it the control plane's message: its answer, if it has one.
    async fn control(&self, message: Message<V>) -> Option<Message<V>> {
        let (answer, answered) = oneshot::channel();
        self.commands
            .send(Command::Control(message, answer))
            .await
            .ok()?;
        answered.await.ok().flatten()
    }
}

/// The pod's task: its store, and how it reaches its peers.
struct Running<V> {
    node: Node<V>,
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
    commands: mpsc::WeakSender<Command<V>>,
}

impl<V: Value + Send + 'static> Running<V>
where
    V::Wire: Send,
{
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
            .set_wall_offset(i64::try_from(offset).unwrap_or(i64::MAX));
        now
    }

    async fn run(mut self, mut received: mpsc::Receiver<Command<V>>) {
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
                        let mut out = self.node.receive(CONTROL_PLANE, message, now);
                        let to_control = out.iter().position(|(to, _)| *to == CONTROL_PLANE);
                        let _gone = answer.send(to_control.map(|at| out.remove(at).1));
                        self.send(out);
                    }
                    Some(Command::With(with)) => {
                        let now = self.now();
                        with(&mut self.node, now);
                    }
                },
            }
        }
    }

    /// Sends `out`, each message to its peer's link, made if it has none: dropped if no
    /// address is known for the peer.
    fn send(&mut self, out: Outbox<V>) {
        for (to, message) in out {
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
            if let Some(link) = self.links.get(&to) {
                link.send(&wire::wire(self.hello.from, message));
            }
        }
    }

    /// Keeps where `peer` is reached: `heard`, from the peer itself, it takes the address
    /// from any other id there, a pod restarted at the same address being a new member;
    /// otherwise, from a table or a naming, only an address no other id holds.
    fn learn(&mut self, peer: NodeId, address: &str, heard: bool) {
        if peer == self.hello.from || peer == CONTROL_PLANE {
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
        self.node.add_peer(peer);
    }

    fn greeted(&mut self, hello: Hello) {
        self.learn(hello.from, &hello.address, true);
    }

    /// Keeps the addresses its table names, and lets go of the peers its store has
    /// forgotten.
    fn follow(&mut self) {
        let members: Vec<(NodeId, String)> = self.node.committed().map_or_else(Vec::new, |table| {
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
        let peers = self.node.peers();
        self.links.retain(|peer, _| peers.contains(peer));
        self.addresses.retain(|peer, _| peers.contains(peer));
    }

    /// Asks each seed who answers there, every [`ASK_SEEDS`] while it is no member: the
    /// answers come back as hellos.
    fn ask_seeds(&mut self) {
        let member = self
            .node
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

/// Whether the pod that says `other` may be a peer of the one that says `own`, asking or
/// asked: of the same shard count, as a pod of another cluster is not (6a), and neither that
/// pod nor the control plane.
fn may_be_peer(own: &Hello, other: &Hello) -> Result<(), Status> {
    if other.shards != own.shards {
        return Err(Status::failed_precondition(format!(
            "a cluster of {} shards, not {}",
            own.shards, other.shards
        )));
    }
    if other.from == CONTROL_PLANE || other.from == own.from {
        return Err(Status::invalid_argument("a member id not a peer's"));
    }
    Ok(())
}

/// Asks the pod at `address` the control plane's `message` (`Probe` or `Form`), as the
/// controller does: its answer, if it has one.
///
/// # Errors
///
/// When the call fails or its answer cannot be used.
pub async fn ask<V: Value>(
    client: &Client,
    message: Message<V>,
) -> Result<Option<Message<V>>, crate::grpc::Error>
where
    V::Wire: Send + 'static,
{
    let Outgoing::Peer(asked) = wire::wire(CONTROL_PLANE, message) else {
        return Ok(None);
    };
    let answer: ToPeer<V::Wire> = client.call(asked, CONTROL).await?;
    if answer.message.is_none() {
        return Ok(None);
    }
    // An answer the store cannot use, as the service's others are refused (`crate::wire`).
    let (_, message) = wire::from_peer::<V>(answer)
        .map_err(|invalid| Status::invalid_argument(invalid.to_string()))?;
    Ok(Some(message))
}

/// The store's service as a gRPC server, in front of its pod.
pub struct StoreServer<V> {
    pod: StorePod<V>,
}

impl<V> Clone for StoreServer<V> {
    fn clone(&self) -> Self {
        Self {
            pod: self.pod.clone(),
        }
    }
}

impl<V> StoreServer<V> {
    /// Serves `pod`.
    #[must_use]
    pub const fn new(pod: StorePod<V>) -> Self {
        Self { pod }
    }
}

impl<V> NamedService for StoreServer<V> {
    const NAME: &'static str = SERVICE;
}

/// Which stream a peer opened.
#[derive(Clone, Copy)]
enum Kind {
    Peer,
    Copy,
}

struct StreamCall<V> {
    pod: StorePod<V>,
    kind: Kind,
}

impl<V: Value + Send + 'static> tonic::server::StreamingService<Piece> for StreamCall<V>
where
    V::Wire: Send,
{
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
async fn take_all<V: Value + Send + 'static>(
    pod: StorePod<V>,
    kind: Kind,
    from: NodeId,
    mut pieces: Streaming<Piece>,
    ended: mpsc::Sender<Result<Empty, Status>>,
) where
    V::Wire: Send,
{
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
        let decoded: Result<(NodeId, Message<V>), Invalid> = match kind {
            Kind::Peer => ToPeer::<V::Wire>::decode(encoded.as_slice())
                .map_err(|_| Invalid::Missing)
                .and_then(wire::from_peer),
            Kind::Copy => ToCopy::<V::Wire>::decode(encoded.as_slice())
                .map_err(|_| Invalid::Missing)
                .and_then(wire::from_copy),
        };
        // A message the store cannot use is dropped whole, as a message lost.
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

struct GreetCall<V>(StorePod<V>);

impl<V: Value + Send + 'static> tonic::server::UnaryService<Hello> for GreetCall<V>
where
    V::Wire: Send,
{
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

struct ControlCall<V>(StorePod<V>);

impl<V: Value + Send + 'static> tonic::server::UnaryService<ToPeer<V::Wire>> for ControlCall<V>
where
    V::Wire: Send,
{
    type Response = ToPeer<V::Wire>;
    type Future = Boxed<Result<Response<ToPeer<V::Wire>>, Status>>;

    fn call(&mut self, request: Request<ToPeer<V::Wire>>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let (_, message) = wire::from_peer::<V>(request.into_inner())
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

impl<V: Value + Send + 'static> tower_service::Service<http::Request<tonic::body::Body>>
    for StoreServer<V>
where
    V::Wire: Send,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let pod = self.pod.clone();
        Box::pin(async move {
            if let Some(status) = refused(&request, Caller::Pod) {
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
                GREET => {
                    let mut grpc = limited(Prost::<Hello, Hello>::default());
                    grpc.unary(GreetCall(pod), request).await
                }
                CONTROL => {
                    let mut grpc = limited(Prost::<ToPeer<V::Wire>, ToPeer<V::Wire>>::default());
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

    fn hello(from: NodeId, shards: u32) -> Hello {
        Hello {
            from,
            address: format!("pod-{from}:7000"),
            shards,
        }
    }

    /// A pod's task as it starts, before it runs.
    fn running() -> Running<edgerush_cluster_synchronous::Value> {
        let config = Config::defaults();
        let (commands, _) = mpsc::channel(1);
        Running {
            node: Node::new(1, "pod-1:7000".to_owned(), config, []),
            started: Instant::now(),
            hello: Arc::new(hello(1, u32::from(config.shards))),
            dial: Dial::Plain,
            seeds: Vec::new(),
            asked_at: None,
            addresses: BTreeMap::new(),
            links: BTreeMap::new(),
            counts: Arc::default(),
            commands: commands.downgrade(),
        }
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
        assert!(running.node.peers().contains(&3));
    }

    #[test]
    fn a_pod_of_the_same_shard_count_may_be_a_peer_and_no_other() {
        let own = hello(1, 16);
        assert!(may_be_peer(&own, &hello(2, 16)).is_ok());
        let other = may_be_peer(&own, &hello(2, 32)).unwrap_err();
        assert_eq!(other.code(), tonic::Code::FailedPrecondition);
        assert!(may_be_peer(&own, &hello(1, 16)).is_err());
        assert!(may_be_peer(&own, &hello(CONTROL_PLANE, 16)).is_err());
    }
}
