//! A service pod running: the protocol's [`ServicePod`] and its part in agreeing the view
//! ([`Membership`]), owned by one task, on real time, with copies and heartbeats sent on to
//! the other pods.
//!
//! Everything the pod does goes through its task, one message at a time, as the state
//! machines want; the gRPC server and the tests reach it through a [`Pod`] handle. Copies
//! to each other pod go out in the order they were made, by a task per pod, so that a
//! pod never takes an older copy over a newer; what piles up while one is on its way goes
//! as one message, the latest copy of each key.

use crate::grpc::{Client, Error};
use edgerush_ratelimit::service::membership::{Heartbeat, Membership, Sight};
use edgerush_ratelimit::service::{self, PodId, ServicePod, View};
use std::collections::BTreeMap;
use std::hash::RandomState;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// How many requests may wait for the pod's task.
const QUEUE: usize = 4_096;

/// How many messages of copies may wait for one other pod. Beyond it a copy is dropped:
/// the key's next change sends it whole again, and a pod taking over from a copy that
/// missed some is what the data planes' `seen` marks are for.
const COPIES_WAITING: usize = 1_024;

/// How the pods watch each other.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// The time between a data plane's batches.
    pub cycle: Duration,
    /// The time between a pod's heartbeats.
    pub heartbeat: Duration,
    /// How long a pod goes unheard before it is suspected.
    pub silence: Duration,
    /// How long a pod is heard before it is taken in.
    pub settle: Duration,
    /// How long a pod goes without hearing a majority before it stops serving: two
    /// heartbeats shorter than `silence` at least, so that one cut off from the others stops
    /// before they take it out, whenever each side heard the other last and looked.
    pub stop_serving: Duration,
}

impl Timing {
    /// A cycle of `cycle`, heartbeats every 100 ms, suspicion after 600 ms, taken in after
    /// 300 ms, and no serving after 400 ms without a majority.
    #[must_use]
    pub fn with_cycle(cycle: Duration) -> Self {
        Self {
            cycle,
            heartbeat: Duration::from_millis(100),
            silence: Duration::from_millis(600),
            settle: Duration::from_millis(300),
            stop_serving: Duration::from_millis(400),
        }
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

enum Command {
    Batch(service::Batch, oneshot::Sender<service::Answers>),
    Replicas(service::Replicas),
    Heartbeat(Heartbeat),
    Counted(Vec<u8>, oneshot::Sender<Option<u64>>),
    View(oneshot::Sender<(View, bool)>),
    Capacity(u64),
    Usage(oneshot::Sender<Usage>),
    Sight(oneshot::Sender<(bool, Sight)>),
    Restart(u64),
    Stop,
}

/// What a pod's keys hold, as of its last timer (06 §8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Their bytes, as an estimate of their own data.
    pub key_bytes: u64,
    /// How many there are, owned and copies.
    pub keys: usize,
    /// How many it has evicted to stay within what they may hold, all told, in this life.
    pub evicted: u64,
    /// Copies not sent to another pod, each alone longer than a message takes, all told.
    pub copies_too_long: u64,
}

/// A handle on a running service pod. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Pod {
    commands: mpsc::Sender<Command>,
}

impl Pod {
    /// Starts service pod `me` among `members` (itself too), in its life `incarnation`,
    /// reaching the others through `peers`. Runs until stopped or every handle is dropped.
    #[must_use]
    pub fn spawn(
        me: PodId,
        members: Vec<PodId>,
        incarnation: u64,
        timing: Timing,
        peers: BTreeMap<PodId, Client>,
    ) -> Self {
        let (commands, received) = mpsc::channel(QUEUE);
        let start = Instant::now();
        let (node, membership) = fresh(me, &members, incarnation, timing, 0);
        let copies_too_long = Arc::new(AtomicU64::new(0));
        let senders = peers
            .into_iter()
            .map(|(pod, client)| (pod, Sender::spawn(client, Arc::clone(&copies_too_long))))
            .collect();
        let running = Running {
            me,
            members,
            timing,
            node,
            membership,
            senders,
            start,
            capacity: u64::MAX,
            copies_too_long,
        };
        tokio::spawn(running.run(received));
        Self { commands }
    }

    /// Restarts it in place, as a process restarted on the same address would: everything
    /// it held is gone, and it comes back as `incarnation`, a new life, serving nothing
    /// until the others take it in again. For tests, where a port cannot be counted on to
    /// be free again at once.
    pub async fn restart(&self, incarnation: u64) {
        let _stopped = self.commands.send(Command::Restart(incarnation)).await;
    }

    async fn ask<T>(&self, command: impl FnOnce(oneshot::Sender<T>) -> Command) -> Option<T> {
        let (answer, answered) = oneshot::channel();
        self.commands.send(command(answer)).await.ok()?;
        answered.await.ok()
    }

    /// A data plane's batch; `None` if the pod has stopped.
    pub async fn batch(&self, batch: service::Batch) -> Option<service::Answers> {
        self.ask(|answer| Command::Batch(batch, answer)).await
    }

    /// Copies from another pod.
    pub async fn replicas(&self, replicas: service::Replicas) {
        let _stopped = self.commands.send(Command::Replicas(replicas)).await;
    }

    /// Another pod's heartbeat.
    pub async fn heartbeat(&self, heartbeat: Heartbeat) {
        let _stopped = self.commands.send(Command::Heartbeat(heartbeat)).await;
    }

    /// Hits the count of `key` holds, if the pod holds the key: for tests and inspection.
    pub async fn counted(&self, key: &[u8]) -> Option<u64> {
        self.ask(|answer| Command::Counted(key.to_vec(), answer))
            .await
            .flatten()
    }

    /// The view it holds, and whether it serves.
    pub async fn view(&self) -> Option<(View, bool)> {
        self.ask(Command::View).await
    }

    /// The bytes its keys may hold, in this life and after a restart: past them its timer
    /// evicts the least recently changed (06 §8). No bound unless set.
    pub async fn set_capacity(&self, bytes: u64) {
        let _stopped = self.commands.send(Command::Capacity(bytes)).await;
    }

    /// What its keys hold; `None` if it has stopped.
    pub async fn usage(&self) -> Option<Usage> {
        self.ask(Command::Usage).await
    }

    /// Whether it serves, and how often and how long it has gone without hearing a
    /// majority, in this life; none once it is stopped.
    pub async fn sight(&self) -> Option<(bool, Sight)> {
        self.ask(Command::Sight).await
    }

    /// Stops it, as a pod gone.
    pub async fn stop(&self) {
        let _stopped = self.commands.send(Command::Stop).await;
    }
}

/// A pod's state from nothing, in life `incarnation`, at `now`.
fn fresh(
    me: PodId,
    members: &[PodId],
    incarnation: u64,
    timing: Timing,
    now: u64,
) -> (ServicePod<RandomState>, Membership) {
    let membership = Membership::new(
        me,
        incarnation,
        members.to_vec(),
        nanos(timing.silence),
        nanos(timing.settle),
        nanos(timing.stop_serving),
        now,
    );
    let mut node = ServicePod::new(me, nanos(timing.cycle), membership.view().clone());
    node.set_serving(false);
    // Its answers carry the life, which differs from every earlier one's.
    node.set_life(incarnation);
    // A view reaches every pod within a heartbeat or so, and the handovers it starts soon
    // after.
    node.set_join_grace(nanos(timing.heartbeat).saturating_mul(3));
    (node, membership)
}

struct Running {
    me: PodId,
    members: Vec<PodId>,
    timing: Timing,
    node: ServicePod<RandomState>,
    membership: Membership,
    senders: BTreeMap<PodId, Sender>,
    start: Instant,
    /// The bytes its keys may hold, kept for a life after a restart.
    capacity: u64,
    /// Copies its senders found too long to send.
    copies_too_long: Arc<AtomicU64>,
}

impl Running {
    fn now(&self) -> u64 {
        nanos(self.start.elapsed())
    }

    /// The view changed: the node follows it, and serves if it took this pod in.
    fn follow(&mut self, view: Option<View>) {
        if let Some(view) = view {
            let now = self.now();
            self.node.on_view(view, now);
        }
        self.node.set_serving(self.membership.serving());
    }

    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        let timing = self.timing;
        let mut cycle = tokio::time::interval(timing.cycle);
        cycle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat = tokio::time::interval(timing.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    None | Some(Command::Stop) => return,
                    Some(Command::Restart(incarnation)) => {
                        let now = self.now();
                        let (mut node, membership) =
                            fresh(self.me, &self.members, incarnation, self.timing, now);
                        node.set_capacity(self.capacity);
                        self.node = node;
                        self.membership = membership;
                    }
                    Some(Command::Batch(batch, answer)) => {
                        let now = self.now();
                        let _gone = answer.send(self.node.on_batch(batch, now));
                    }
                    Some(Command::Replicas(replicas)) => {
                        let now = self.now();
                        self.node.on_replicas(replicas, now);
                    }
                    Some(Command::Heartbeat(beat)) => {
                        let now = self.now();
                        let view = self.membership.on_heartbeat(beat, now);
                        self.follow(view);
                    }
                    Some(Command::Counted(key, answer)) => {
                        let _gone = answer.send(self.node.counted(&key));
                    }
                    Some(Command::View(answer)) => {
                        let _gone = answer.send((
                            self.membership.view().clone(),
                            self.membership.serving(),
                        ));
                    }
                    Some(Command::Capacity(bytes)) => {
                        self.capacity = bytes;
                        self.node.set_capacity(bytes);
                    }
                    Some(Command::Sight(answer)) => {
                        let now = self.now();
                        let _gone = answer
                            .send((self.membership.serving(), self.membership.sight(now)));
                    }
                    Some(Command::Usage(answer)) => {
                        let _gone = answer.send(Usage {
                            key_bytes: self.node.holding(),
                            keys: self.node.key_count(),
                            evicted: self.node.evicted(),
                            copies_too_long: self.copies_too_long.load(Ordering::Relaxed),
                        });
                    }
                },
                _ = cycle.tick() => {
                    let now = self.now();
                    self.node.on_timer(now);
                }
                _ = heartbeat.tick() => {
                    let now = self.now();
                    let view = self.membership.on_tick(now);
                    self.follow(view);
                    let beat = self.membership.heartbeat(now);
                    for sender in self.senders.values() {
                        sender.heartbeat(beat.clone());
                    }
                }
            }
            for (pod, replicas) in self.node.take_replicas() {
                if let Some(sender) = self.senders.get(&pod) {
                    sender.send(replicas);
                }
            }
        }
    }
}

/// Sends one pod its copies, in order, coalescing what waits and cutting it into pieces
/// ([`service::Replicas::into_pieces`]); and its heartbeats, which need no order and are
/// not worth waiting for.
struct Sender {
    queue: mpsc::Sender<service::Replicas>,
    client: Client,
}

impl Sender {
    /// Counting in `too_long` the copies too long to send.
    fn spawn(client: Client, too_long: Arc<AtomicU64>) -> Self {
        let (queue, mut waiting) = mpsc::channel::<service::Replicas>(COPIES_WAITING);
        let sending = client.clone();
        tokio::spawn(async move {
            while let Some(first) = waiting.recv().await {
                // The latest of each key, of everything waiting, copies and handovers apart.
                let from = first.from;
                let mut epoch = first.epoch;
                let mut latest: BTreeMap<(bool, Box<[u8]>), service::Replica> = BTreeMap::new();
                let mut take = |replicas: service::Replicas| {
                    epoch = epoch.max(replicas.epoch);
                    for replica in replicas.entries {
                        latest.insert((replicas.handover, replica.key.clone()), replica);
                    }
                };
                take(first);
                while let Ok(more) = waiting.try_recv() {
                    take(more);
                }
                for handover in [true, false] {
                    let entries: Vec<service::Replica> = latest
                        .iter()
                        .filter(|((h, _), _)| *h == handover)
                        .map(|(_, replica)| replica.clone())
                        .collect();
                    if entries.is_empty() {
                        continue;
                    }
                    let replicas = service::Replicas {
                        from,
                        entries,
                        handover,
                        epoch,
                    };
                    // A piece at a time, in order, as a message takes so much. Lost if the
                    // call fails: the next change sends the key again, and a pod taking
                    // over from a copy that missed some is what the data planes' `seen`
                    // marks are for.
                    for piece in replicas.into_pieces() {
                        if let Err(Error::TooLong(_)) = sending.replicate(piece).await {
                            too_long.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        Self { queue, client }
    }

    fn send(&self, replicas: service::Replicas) {
        let _full_or_stopped = self.queue.try_send(replicas);
    }

    fn heartbeat(&self, beat: Heartbeat) {
        let client = self.client.clone();
        tokio::spawn(async move {
            let _unheard = client.heartbeat(beat).await;
        });
    }
}
