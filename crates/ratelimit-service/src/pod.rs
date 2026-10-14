//! A service pod running: the protocol's [`ServicePod`] owned by one task, on real time,
//! with its copies sent on to the other pods.
//!
//! Everything the pod does goes through its task, one message at a time, as the state
//! machine wants; the gRPC server and the tests reach it through a [`Pod`] handle. Copies
//! to each other pod go out in the order they were made, by a task per pod, so that a
//! secondary never takes an older copy over a newer; what piles up while one is on its way
//! goes as one message, the latest copy of each key.

use crate::grpc::Client;
use edgerush_ratelimit::service::{self, PodId, ServicePod, View};
use std::collections::BTreeMap;
use std::hash::RandomState;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// How many requests may wait for the pod's task.
const QUEUE: usize = 4_096;

enum Command {
    Batch(service::Batch, oneshot::Sender<service::Answers>),
    Replicas(service::Replicas),
    Counted(Vec<u8>, oneshot::Sender<Option<u64>>),
}

/// A handle on a running service pod. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Pod {
    commands: mpsc::Sender<Command>,
}

impl Pod {
    /// Starts service pod `me` of `view`, its data planes sending every `cycle`, its copies
    /// going to the other pods through `peers`. Runs until every handle is dropped.
    #[must_use]
    pub fn spawn(me: PodId, cycle: Duration, view: View, peers: BTreeMap<PodId, Client>) -> Self {
        let (commands, received) = mpsc::channel(QUEUE);
        let cycle_ns = u64::try_from(cycle.as_nanos()).unwrap_or(u64::MAX);
        let node: ServicePod<RandomState> = ServicePod::new(me, cycle_ns, view);
        let senders = peers
            .into_iter()
            .map(|(pod, client)| (pod, Sender::spawn(client)))
            .collect();
        tokio::spawn(run(node, received, cycle, senders));
        Self { commands }
    }

    /// A data plane's batch; `None` if the pod has stopped.
    pub async fn batch(&self, batch: service::Batch) -> Option<service::Answers> {
        let (answer, answered) = oneshot::channel();
        self.commands
            .send(Command::Batch(batch, answer))
            .await
            .ok()?;
        answered.await.ok()
    }

    /// An owner's copies.
    pub async fn replicas(&self, replicas: service::Replicas) {
        let _stopped = self.commands.send(Command::Replicas(replicas)).await;
    }

    /// Hits the count of `key` holds, if the pod holds the key: for tests and inspection.
    pub async fn counted(&self, key: &[u8]) -> Option<u64> {
        let (answer, answered) = oneshot::channel();
        self.commands
            .send(Command::Counted(key.to_vec(), answer))
            .await
            .ok()?;
        answered.await.ok().flatten()
    }
}

async fn run(
    mut node: ServicePod<RandomState>,
    mut commands: mpsc::Receiver<Command>,
    cycle: Duration,
    senders: BTreeMap<PodId, Sender>,
) {
    let start = Instant::now();
    let now = || u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let mut timer = tokio::time::interval(cycle);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                None => return,
                Some(Command::Batch(batch, answer)) => {
                    let _gone = answer.send(node.on_batch(batch, now()));
                }
                Some(Command::Replicas(replicas)) => node.on_replicas(replicas, now()),
                Some(Command::Counted(key, answer)) => {
                    let _gone = answer.send(node.counted(&key));
                }
            },
            _ = timer.tick() => node.on_timer(now()),
        }
        for (pod, replicas) in node.take_replicas() {
            if let Some(sender) = senders.get(&pod) {
                sender.send(replicas);
            }
        }
    }
}

/// Sends one pod its copies, in order, coalescing what waits.
struct Sender {
    queue: mpsc::UnboundedSender<service::Replicas>,
}

impl Sender {
    fn spawn(client: Client) -> Self {
        let (queue, mut waiting) = mpsc::unbounded_channel::<service::Replicas>();
        tokio::spawn(async move {
            while let Some(first) = waiting.recv().await {
                let from = first.from;
                // The latest copy of each key, of everything waiting.
                let mut latest: BTreeMap<Box<[u8]>, service::Replica> = BTreeMap::new();
                let mut take = |replicas: service::Replicas| {
                    for replica in replicas.entries {
                        latest.insert(replica.key.clone(), replica);
                    }
                };
                take(first);
                while let Ok(more) = waiting.try_recv() {
                    take(more);
                }
                let replicas = service::Replicas {
                    from,
                    entries: latest.into_values().collect(),
                };
                // Lost if the call fails: the next change sends the key again, and a
                // secondary that missed copies is what the data planes' `seen` marks are for.
                let _lost = client.replicate(replicas).await;
            }
        });
        Self { queue }
    }

    fn send(&self, replicas: service::Replicas) {
        let _stopped = self.queue.send(replicas);
    }
}
