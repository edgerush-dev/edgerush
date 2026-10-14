//! A service pod running: the protocol's [`ServicePod`] and its part in agreeing the view
//! ([`Membership`]), owned by one task, on real time, with copies and heartbeats sent on to
//! the other pods.
//!
//! Everything the pod does goes through its task, one message at a time, as the state
//! machines want; the gRPC server and the tests reach it through a [`Pod`] handle. Copies
//! to each other pod go out in the order they were made, by a task per pod, so that a
//! secondary never takes an older copy over a newer; what piles up while one is on its way
//! goes as one message, the latest copy of each key.

use crate::grpc::Client;
use edgerush_ratelimit::service::membership::{Heartbeat, Membership};
use edgerush_ratelimit::service::{self, PodId, ServicePod, View};
use std::collections::BTreeMap;
use std::hash::RandomState;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// How many requests may wait for the pod's task.
const QUEUE: usize = 4_096;

/// How many messages of copies may wait for one other pod. Beyond it a copy is dropped:
/// the key's next change sends it whole again, and a secondary that missed copies is what
/// the data planes' `seen` marks are for.
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
}

impl Timing {
    /// A cycle of `cycle`, heartbeats every 100 ms, suspicion after 500 ms, taken in after
    /// 300 ms.
    #[must_use]
    pub fn with_cycle(cycle: Duration) -> Self {
        Self {
            cycle,
            heartbeat: Duration::from_millis(100),
            silence: Duration::from_millis(500),
            settle: Duration::from_millis(300),
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
    Stop,
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
        let membership = Membership::new(
            me,
            incarnation,
            members,
            nanos(timing.silence),
            nanos(timing.settle),
            0,
        );
        let mut node: ServicePod<RandomState> =
            ServicePod::new(me, nanos(timing.cycle), membership.view().clone());
        node.set_serving(false);
        let senders = peers
            .into_iter()
            .map(|(pod, client)| (pod, Sender::spawn(client)))
            .collect();
        let running = Running {
            node,
            membership,
            senders,
            start,
        };
        tokio::spawn(running.run(received, timing));
        Self { commands }
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

    /// Stops it, as a pod gone.
    pub async fn stop(&self) {
        let _stopped = self.commands.send(Command::Stop).await;
    }
}

struct Running {
    node: ServicePod<RandomState>,
    membership: Membership,
    senders: BTreeMap<PodId, Sender>,
    start: Instant,
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

    async fn run(mut self, mut commands: mpsc::Receiver<Command>, timing: Timing) {
        let mut cycle = tokio::time::interval(timing.cycle);
        cycle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat = tokio::time::interval(timing.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    None | Some(Command::Stop) => return,
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

/// Sends one pod its copies, in order, coalescing what waits; and its heartbeats, which
/// need no order and are not worth waiting for.
struct Sender {
    queue: mpsc::Sender<service::Replicas>,
    client: Client,
}

impl Sender {
    fn spawn(client: Client) -> Self {
        let (queue, mut waiting) = mpsc::channel::<service::Replicas>(COPIES_WAITING);
        let sending = client.clone();
        tokio::spawn(async move {
            while let Some(first) = waiting.recv().await {
                // The latest of each key, of everything waiting, copies and handovers apart.
                let from = first.from;
                let mut latest: BTreeMap<(bool, Box<[u8]>), service::Replica> = BTreeMap::new();
                let mut take = |replicas: service::Replicas| {
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
                    };
                    // Lost if the call fails: the next change sends the key again, and a
                    // secondary that missed copies is what the data planes' `seen` marks
                    // are for.
                    let _lost = sending.replicate(replicas).await;
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
