//! The rate-limiting service as the prototype runs it: the protocol's own state machines
//! (`edgerush_ratelimit::service`), one data plane per pod and three service pods, with the
//! simulator carrying their messages. Its results should match [`Service`](crate::service)
//! with the same choices, which it was taken from; where they differ, the nodes are what is
//! built. Requests can be under other limits beside the run's ([`Beside`]).

use crate::{Beside, Cluster, Contract, FaultEvent, Message, Outbox};
use edgerush_ratelimit::service::pooled::{
    self, Decision as Pooled, Entry, Levers, Local, Participant, Registry,
};
use edgerush_ratelimit::service::{
    self, Answers, Batch, DataPlane, Decision, FixedState, PlaneId, PodId, ServicePod, View,
};
use std::collections::HashMap;
use std::sync::Arc;

/// The service's pods.
const SERVICE_PODS: u32 = 3;

/// The key of the run's contract, which every request is under.
const KEY: &[u8] = b"key";

/// A fleet of data planes sharing keys through the protocol's nodes.
pub(crate) struct Nodes {
    /// Every key requests are under, with its contract: the run's first.
    keys: Vec<(Box<[u8]>, service::Contract)>,
    /// The limits of each kind of request, as positions in `keys`, smallest bucket first:
    /// request `n` is of the kind at `n` modulo their number.
    kinds: Vec<Vec<usize>>,
    cycle: u64,
    /// How long a pooled participant waits for an exchange's answer; 0 for four cycles.
    exchange_timeout: u64,
    view: View,
    planes: Vec<Plane>,
    /// How many times each data plane has restarted: its id is its index and its life.
    lives: Vec<u64>,
    pods: Vec<ServicePod>,
    /// The service pods' lives begun, all told: each pod's answers carry its own.
    pod_lives: u64,
    /// Service pods gone for good.
    gone: Vec<bool>,
    /// Decisions for requests that waited, not yet taken.
    decided: Vec<(usize, bool)>,
    /// Requests admitted anywhere, under each key.
    admitted: Vec<u64>,
    /// The workers each participant shares its keys with, if pooled.
    pool: Option<Pool>,
    /// By data plane: until when its background work is held (`Fault::Pause`), 0 if not;
    /// the answers that reached it meanwhile, and — pooled — what its workers asked.
    paused: Vec<u64>,
    held: Vec<Vec<(u64, Answers)>>,
    pending: Vec<Vec<Asked>>,
    /// Pooled, by process: requests deferred (`Local::Defer`) or handed back to decide
    /// again, with the worker that decides them, retried at the process's next arrival,
    /// answer or cycle.
    deferred: Vec<Vec<(usize, usize)>>,
    /// The nodes before the service pods, as the run numbers them: the data planes, or,
    /// pooled, their workers.
    front: usize,
}

/// Each data plane one participant for its process (`service::pooled`), its workers
/// deciding warm requests from the shared entries they hold handles to, the participant
/// (the process's transport) deciding the rest, sending the cycle's batches and taking the
/// answers. In one process: a worker's decision is a function call on shared state.
struct Pool {
    workers: usize,
    /// What its participants give up of accuracy for throughput.
    levers: Levers,
    /// Whether its participants visit only the keys that need it each cycle.
    scheduled: bool,
    /// By worker: its handles to the entries its participant gave it.
    handles: Vec<HashMap<Box<[u8]>, Arc<Entry>>>,
    /// By process: its participant's registry, where a worker finds an entry it holds no
    /// handle to without asking.
    registries: Vec<Arc<Registry<FixedState>>>,
}

/// A data plane of the protocol: one worker's own (`DataPlane`), or a process's shared by
/// its workers (`pooled::Participant`).
enum Plane {
    Data(DataPlane),
    Pooled(Box<Participant<FixedState>>),
}

impl Plane {
    fn decide_all(&mut self, keys: &[(&[u8], service::Contract)], now: u64) -> Pooled {
        match self {
            Self::Data(plane) => match plane.decide_all(keys, now) {
                Decision::Admit => Pooled::Admit,
                Decision::Refuse => Pooled::Refuse,
            },
            Self::Pooled(participant) => participant.decide_all(keys, now),
        }
    }

    fn on_cycle(&mut self, now: u64) {
        match self {
            Self::Data(plane) => plane.on_cycle(now),
            Self::Pooled(participant) => participant.on_cycle(now),
        }
    }

    /// Answers to `exchange`, as [`Self::take_exchanges`] numbered it.
    fn on_answers(&mut self, exchange: u64, answers: Answers, now: u64) {
        match self {
            Self::Data(plane) => plane.on_answers(answers, now),
            Self::Pooled(participant) => participant.on_answers_to(exchange, answers, now),
        }
    }

    fn on_view(&mut self, view: View, now: u64) {
        match self {
            Self::Data(plane) => plane.on_view(view, now),
            Self::Pooled(participant) => participant.on_view(view, now),
        }
    }

    /// Messages to send, each with the exchange it starts: pooled, numbered by the
    /// participant; a `DataPlane`, which takes every answer, 0.
    fn take_exchanges(&mut self) -> Vec<(PodId, u64, Batch)> {
        match self {
            Self::Data(plane) => plane
                .take_outgoing()
                .into_iter()
                .map(|(pod, batch)| (pod, 0, batch))
                .collect(),
            Self::Pooled(participant) => participant.take_exchanges(),
        }
    }
}

impl Nodes {
    /// Pooled, request `request` comes to worker `worker` at `now`: decided now, or later
    /// (its process held up, or deferred).
    fn pooled_arrival(
        &mut self,
        request: usize,
        worker: usize,
        now: u64,
        outbox: &mut Outbox,
    ) -> Option<bool> {
        let pool = self.pool.as_ref()?;
        let process = worker / pool.workers;
        let limits: Vec<(Box<[u8]>, service::Contract)> = self
            .kind(request)
            .iter()
            .map(|&key| (self.keys[key].0.clone(), self.keys[key].1))
            .collect();
        let keys: Vec<(&[u8], service::Contract)> =
            limits.iter().map(|(key, c)| (&**key, *c)).collect();
        // A key it holds no handle to is looked for in its process's registry first, made
        // there if its process has none, and adopted.
        let cycle = self.cycle;
        let pool = self.pool.as_mut()?;
        let mut made = Vec::new();
        for (key, contract) in &keys {
            let held = pool.handles[worker]
                .get(*key)
                .is_some_and(|entry| entry.contract() == *contract);
            if held {
                continue;
            }
            let found = pool.registries[process]
                .find_or_make(key, *contract, cycle, now)
                .map(|(entry, fresh)| {
                    if fresh {
                        made.push(Arc::clone(&entry));
                    }
                    entry
                });
            if let Some(entry) = found {
                pool.handles[worker].insert((*key).into(), entry);
            }
        }
        if let Plane::Pooled(participant) = &mut self.planes[process] {
            for entry in made {
                participant.adopt(entry);
            }
        }
        let handles = &self.pool.as_ref()?.handles[worker];
        let held: Option<Vec<&Arc<Entry>>> = keys
            .iter()
            .map(|(key, contract)| {
                handles
                    .get(*key)
                    .filter(|entry| entry.contract() == *contract)
            })
            .collect();
        if let Some(held) = held {
            let entries: Vec<&Entry> = held.iter().map(|entry| &***entry).collect();
            let outcome = pooled::decide(&entries, now);
            // Signals reach the process at once, as the proxy's do within a tick.
            if outcome.signal != 0
                && let Plane::Pooled(participant) = &mut self.planes[process]
            {
                let signalled = held
                    .iter()
                    .enumerate()
                    .filter(|(at, _)| outcome.signal & (1 << at) != 0)
                    .map(|(_, entry)| Arc::clone(entry));
                participant.on_signals(signalled);
            }
            match outcome.local {
                Local::Admit => {
                    self.count(request);
                    return Some(true);
                }
                Local::Refuse => return Some(false),
                Local::Defer => return self.defer(process, request, worker),
                Local::Ask => {}
            }
        }
        self.resume(process, now, outbox);
        if self.paused[process] > now {
            // Its process is held: the ask waits in its queue, as on the process
            // thread's channel.
            self.pending[process].push((request, worker, limits));
            return None;
        }
        let decision = self.planes[process].decide_all(&keys, now);
        self.give_entries(process, worker, &keys);
        let decided = match decision {
            Pooled::Admit => {
                self.count(request);
                Some(true)
            }
            Pooled::Refuse => Some(false),
            Pooled::Defer => self.defer(process, request, worker),
        };
        self.flush_plane(process, outbox);
        decided
    }

    /// Pooled, request `request` deferred by `worker` of `process`, to decide again; one
    /// with no number, which nothing can come back to, refused.
    fn defer(&mut self, process: usize, request: usize, worker: usize) -> Option<bool> {
        if request == usize::MAX {
            return Some(false);
        }
        self.deferred[process].push((request, worker));
        None
    }
}

/// The protocol's contract for the simulator's.
fn contract_of(contract: &Contract) -> service::Contract {
    service::Contract {
        limit: contract.limit,
        window: contract.window,
        burst: contract.burst,
    }
}

impl Nodes {
    pub(crate) fn new(
        contract: &Contract,
        planes: usize,
        cycle: u64,
        exchange_timeout: u64,
        beside: Option<Beside>,
    ) -> Self {
        let view = View {
            epoch: 1,
            pods: (0..SERVICE_PODS).collect(),
        };
        // A key named after `name`, owned by another pod than the run's key.
        let apart = |name: &str| -> Box<[u8]> {
            (0_u32..)
                .map(|n| format!("{name}-{n}").into_bytes())
                .find(|key| view.owners(key).0 != view.owners(KEY).0)
                .unwrap_or_default()
                .into_boxed_slice()
        };
        let run = contract_of(contract);
        let mut keys = vec![(Box::from(KEY), run)];
        // The limits of each kind of request, its own first: every limit is decided from
        // the data plane's copy, so the order changes nothing.
        let kinds = match beside {
            None => vec![vec![0]],
            Some(Beside::Global(global)) => {
                keys.push((apart("global"), contract_of(&global)));
                vec![vec![0, 1]]
            }
            Some(Beside::Routes(each)) => {
                keys.push((apart("route-a"), contract_of(&each)));
                keys.push((apart("route-b"), contract_of(&each)));
                vec![vec![1, 0], vec![2, 0]]
            }
        };
        Self {
            admitted: vec![0; keys.len()],
            keys,
            kinds,
            cycle,
            exchange_timeout,
            planes: (0..planes)
                .map(|plane| Plane::Data(DataPlane::new(id(plane, 0), cycle, view.clone())))
                .collect(),
            lives: vec![0; planes],
            pods: (0..SERVICE_PODS)
                .map(|pod| {
                    let mut node = ServicePod::new(pod, cycle, view.clone());
                    node.set_life(u64::from(pod) + 1);
                    node
                })
                .collect(),
            pod_lives: u64::from(SERVICE_PODS),
            gone: vec![false; SERVICE_PODS as usize],
            decided: Vec::new(),
            view,
            pool: None,
            paused: vec![0; planes],
            held: vec![Vec::new(); planes],
            pending: vec![Vec::new(); planes],
            deferred: vec![Vec::new(); planes],
            front: planes,
        }
    }

    /// The data planes of the pod whose first worker is `pod`, as the run numbers them;
    /// none for any other worker, so that a fault striking every worker in turn strikes
    /// each pod once.
    fn pod_planes(&self, pod: usize) -> std::ops::Range<usize> {
        let (workers, per_plane) = match &self.pool {
            Some(pool) => (pool.workers, pool.workers),
            None => (1, 1),
        };
        if !pod.is_multiple_of(workers) {
            return 0..0;
        }
        let first = pod / per_plane;
        let count = workers / per_plane;
        first.min(self.planes.len())..(first + count).min(self.planes.len())
    }

    /// Ends a pause of data plane `plane` that is over at `now`: the answers it held taken,
    /// and, pooled, what its workers asked decided.
    fn resume(&mut self, plane: usize, now: u64, outbox: &mut Outbox) {
        if self.paused[plane] == 0 || now < self.paused[plane] {
            return;
        }
        self.paused[plane] = 0;
        for (exchange, answers) in std::mem::take(&mut self.held[plane]) {
            self.planes[plane].on_answers(exchange, answers, now);
        }
        for (request, worker, limits) in std::mem::take(&mut self.pending[plane]) {
            let keys: Vec<(&[u8], service::Contract)> =
                limits.iter().map(|(key, c)| (&**key, *c)).collect();
            let decision = self.planes[plane].decide_all(&keys, now);
            self.give_entries(plane, worker, &keys);
            match decision {
                Pooled::Admit => {
                    self.count(request);
                    self.decided.push((request, true));
                }
                Pooled::Refuse => self.decided.push((request, false)),
                Pooled::Defer => self.deferred[plane].push((request, worker)),
            }
        }
        self.flush_plane(plane, outbox);
    }

    /// Pooled, every request process `process` deferred, decided again at `now`.
    fn again(&mut self, process: usize, now: u64, outbox: &mut Outbox) {
        for (request, worker) in std::mem::take(&mut self.deferred[process]) {
            if let Some(admitted) = self.pooled_arrival(request, worker, now, outbox) {
                self.decided.push((request, admitted));
            }
        }
    }

    /// Pooled, worker `worker` holds the entries participant `plane` has of `keys`.
    fn give_entries(&mut self, plane: usize, worker: usize, keys: &[(&[u8], service::Contract)]) {
        if let Plane::Pooled(participant) = &self.planes[plane] {
            let given = participant.entries(keys);
            if let Some(pool) = &mut self.pool {
                for entry in given {
                    pool.handles[worker].insert(entry.key().into(), entry);
                }
            }
        }
    }

    /// The same, each data plane a process's one participant, its `workers` deciding from
    /// shared entries; requests arrive at the workers, `workers` to a process.
    pub(crate) fn pooled(self, workers: usize) -> Self {
        self.pooled_with(workers, Levers::default(), false)
    }

    /// The same, its participants' levers set, each visiting only the keys that need it
    /// each cycle if `scheduled`.
    pub(crate) fn pooled_with(mut self, workers: usize, levers: Levers, scheduled: bool) -> Self {
        let workers = workers.max(1);
        for (at, plane) in self.planes.iter_mut().enumerate() {
            *plane = pooled_plane(
                id(at, 0),
                self.cycle,
                self.exchange_timeout,
                self.view.clone(),
                levers,
                scheduled,
            );
        }
        let registries = self.planes.iter().map(registry_of).collect();
        self.pool = Some(Pool {
            workers,
            levers,
            scheduled,
            handles: vec![HashMap::new(); self.planes.len() * workers],
            registries,
        });
        self.front = self.planes.len() * workers;
        self
    }

    /// The node data plane `plane` sends from and is reached at, as the network places
    /// it: pooled, its process's first worker, so that a partition cuts it where its
    /// workers are.
    fn node_of(&self, plane: usize) -> usize {
        let workers = self.pool.as_ref().map_or(1, |pool| pool.workers);
        plane * workers
    }

    /// The data plane at node `node`, if one is.
    fn plane_at(&self, node: usize) -> Option<usize> {
        let workers = self.node_of(1);
        (node.is_multiple_of(workers) && node / workers < self.planes.len()).then(|| node / workers)
    }

    /// The node a service pod is reached at: after the data planes.
    fn address(&self, pod: PodId) -> usize {
        self.front + pod as usize
    }

    /// Sends what data plane `plane` has to send.
    fn flush_plane(&mut self, plane: usize, outbox: &mut Outbox) {
        let from = self.node_of(plane);
        for (pod, exchange, batch) in self.planes[plane].take_exchanges() {
            outbox.send(
                self.address(pod),
                Message::Batch {
                    from,
                    exchange,
                    batch,
                },
            );
        }
    }

    /// The limits of request `request`, as positions in `keys`.
    fn kind(&self, request: usize) -> &[usize] {
        &self.kinds[request % self.kinds.len()]
    }

    /// Request `request` admitted: one more under each of its keys.
    fn count(&mut self, request: usize) {
        let kind = request % self.kinds.len();
        for &key in &self.kinds[kind] {
            self.admitted[key] += 1;
        }
    }

    /// Sends the copies service pod `pod` has to send.
    fn flush_pod(&mut self, pod: usize, outbox: &mut Outbox) {
        let from = self.front + pod;
        for (to, replicas) in self.pods[pod].take_replicas() {
            outbox.send(self.address(to), Message::Replicas { from, replicas });
        }
    }

    /// The pods agree on a new view: every node learns of it at once, as everywhere in the
    /// simulator.
    fn change_view(&mut self, without: PodId, now: u64, outbox: &mut Outbox) {
        self.view = View {
            epoch: self.view.epoch + 1,
            pods: self
                .view
                .pods
                .iter()
                .copied()
                .filter(|p| *p != without)
                .collect(),
        };
        for pod in 0..self.pods.len() {
            if !self.gone[pod] {
                self.pods[pod].on_view(self.view.clone(), now);
                self.flush_pod(pod, outbox);
            }
        }
        for plane in 0..self.planes.len() {
            self.planes[plane].on_view(self.view.clone(), now);
            self.flush_plane(plane, outbox);
        }
    }

    fn owner(&self) -> Option<usize> {
        self.view.owners(KEY).0.map(|pod| pod as usize)
    }

    /// For each key, what was admitted under it and what its owner counted.
    fn tallies(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.keys
            .iter()
            .zip(&self.admitted)
            .map(|((key, _), admitted)| {
                let owner = self.view.owners(key).0.map(|pod| pod as usize);
                let counted = owner.and_then(|owner| self.pods[owner].counted(key));
                (*admitted, counted.unwrap_or(0))
            })
    }
}

impl Nodes {
    /// Data plane `plane` restarts: a new life, with a new id; what waited on it is
    /// refused, and, pooled, what its workers held is gone with it.
    fn restart(&mut self, plane: usize) {
        for (request, _, _) in std::mem::take(&mut self.pending[plane]) {
            self.decided.push((request, false));
        }
        for (request, _) in std::mem::take(&mut self.deferred[plane]) {
            self.decided.push((request, false));
        }
        self.held[plane].clear();
        self.paused[plane] = 0;
        self.lives[plane] += 1;
        let id = id(plane, self.lives[plane]);
        self.planes[plane] = match &mut self.pool {
            Some(pool) => {
                let workers = plane * pool.workers..(plane + 1) * pool.workers;
                for handles in &mut pool.handles[workers] {
                    handles.clear();
                }
                let fresh = pooled_plane(
                    id,
                    self.cycle,
                    self.exchange_timeout,
                    self.view.clone(),
                    pool.levers,
                    pool.scheduled,
                );
                pool.registries[plane] = registry_of(&fresh);
                fresh
            }
            None => Plane::Data(DataPlane::new(id, self.cycle, self.view.clone())),
        };
    }
}

/// A plane's registry of entries: a pooled participant's, or an empty one.
fn registry_of(plane: &Plane) -> Arc<Registry<FixedState>> {
    match plane {
        Plane::Pooled(participant) => participant.registry(),
        Plane::Data(_) => Arc::new(Registry::default()),
    }
}

/// An ask a pooled worker made of its process while it was held: the request, the worker,
/// and the request's keys.
type Asked = (usize, usize, Vec<(Box<[u8]>, service::Contract)>);

/// A pooled participant `id`, waiting `exchange_timeout` for an exchange's answer (0: four
/// cycles).
/// The simulator runs where 128-bit atomics are native; the protocol's `DataPlane` is
/// what it falls back to only where they are not, so that a run still runs.
fn pooled_plane(
    id: PlaneId,
    cycle: u64,
    exchange_timeout: u64,
    view: View,
    levers: Levers,
    scheduled: bool,
) -> Plane {
    match Participant::new(id, cycle, view.clone(), FixedState::default()) {
        Some(mut participant) => {
            if exchange_timeout > 0 {
                participant.set_exchange_timeout(exchange_timeout);
            }
            // As the proxy runs it: one exchange in flight to each service pod.
            participant.set_one_in_flight(true);
            participant.set_levers(levers);
            participant.set_scheduled(scheduled);
            Plane::Pooled(Box::new(participant))
        }
        None => Plane::Data(DataPlane::new(id, cycle, view)),
    }
}

/// A data plane's id: its index and its life.
fn id(plane: usize, life: u64) -> PlaneId {
    (life << 32) | plane as u64
}

impl Cluster for Nodes {
    fn decide(&mut self, plane: usize, now: u64, outbox: &mut Outbox) -> bool {
        self.arrive(usize::MAX, plane, now, outbox) == Some(true)
    }

    fn arrive(
        &mut self,
        request: usize,
        plane: usize,
        now: u64,
        outbox: &mut Outbox,
    ) -> Option<bool> {
        // Pooled: the request comes to a worker, which decides it from the entries it
        // holds, or asks its participant, and holds the entries it gives.
        if let Some(pool) = &self.pool {
            let process = plane / pool.workers;
            self.again(process, now, outbox);
            return self.pooled_arrival(request, plane, now, outbox);
        }
        let limits: Vec<(&[u8], service::Contract)> = self
            .kind(request)
            .iter()
            .map(|&key| (&*self.keys[key].0, self.keys[key].1))
            .collect();
        let decision = self.planes[plane].decide_all(&limits, now);
        let decided = match decision {
            Pooled::Admit => {
                self.count(request);
                Some(true)
            }
            // A data plane never defers.
            Pooled::Refuse | Pooled::Defer => Some(false),
        };
        self.flush_plane(plane, outbox);
        decided
    }

    fn take_decided(&mut self) -> Vec<(usize, bool)> {
        std::mem::take(&mut self.decided)
    }

    fn cycle(&self) -> Option<u64> {
        Some(self.cycle)
    }

    fn helpers(&self) -> usize {
        SERVICE_PODS as usize
    }

    fn on_timer(&mut self, node: usize, now: u64, outbox: &mut Outbox) {
        if node < self.front {
            // Pooled, a process's cycle comes with its first worker's timer.
            let workers = self.pool.as_ref().map(|pool| pool.workers);
            let node = match workers {
                None => node,
                Some(workers) if node.is_multiple_of(workers) => node / workers,
                Some(_) => return,
            };
            self.resume(node, now, outbox);
            if self.paused[node] > now {
                return;
            }
            self.planes[node].on_cycle(now);
            self.flush_plane(node, outbox);
            if self.pool.is_some() {
                self.again(node, now, outbox);
            }
        } else {
            let pod = node - self.front;
            if !self.gone[pod] {
                self.pods[pod].on_timer(now);
            }
        }
    }

    fn on_message(&mut self, to: usize, message: Message, now: u64, outbox: &mut Outbox) {
        match message {
            Message::Batch {
                from,
                exchange,
                batch,
            } if to >= self.front => {
                let pod = to - self.front;
                if self.gone[pod] {
                    return;
                }
                let plane = batch.from;
                let answers = self.pods[pod].on_batch(batch, now);
                outbox.send(
                    from,
                    Message::Answers {
                        from: to,
                        plane,
                        exchange,
                        answers,
                    },
                );
                self.flush_pod(pod, outbox);
            }
            Message::Replicas { replicas, .. } if to >= self.front => {
                let pod = to - self.front;
                if !self.gone[pod] {
                    self.pods[pod].on_replicas(replicas, now);
                }
            }
            Message::Answers {
                plane,
                exchange,
                answers,
                ..
            } if to < self.front => {
                let Some(to) = self.plane_at(to) else {
                    return;
                };
                // Answers to a life of the data plane before this one went with the task
                // that made the call.
                if plane != id(to, self.lives[to]) {
                    return;
                }
                if self.paused[to] > now {
                    self.held[to].push((exchange, answers));
                    return;
                }
                self.planes[to].on_answers(exchange, answers, now);
                self.flush_plane(to, outbox);
                if self.pool.is_some() {
                    self.again(to, now, outbox);
                }
            }
            _ => {}
        }
    }

    fn on_fault(&mut self, event: FaultEvent, now: u64, outbox: &mut Outbox) {
        match event {
            // The owning service pod goes for good, or restarts holding nothing: either way
            // the others agree it is gone and the next in rank takes over from its copy.
            FaultEvent::OwnerLost | FaultEvent::OwnerRestarts => {
                let Some(owner) = self.owner() else {
                    return;
                };
                self.gone[owner] = true;
                if event == FaultEvent::OwnerRestarts {
                    self.pods[owner] =
                        ServicePod::new(owner as PodId, self.cycle, self.view.clone());
                    self.pod_lives += 1;
                    self.pods[owner].set_life(self.pod_lives);
                }
                self.change_view(owner as PodId, now, outbox);
            }
            // A data plane restarts: a new life, with a new id; what waited on it is refused.
            FaultEvent::Restart { pod } => {
                for plane in self.pod_planes(pod) {
                    self.restart(plane);
                }
            }
            // A pooled process's background held; one without (apart, or one data plane)
            // has none apart from its deciding, and goes on.
            FaultEvent::Pause { pod, until } if self.pool.is_some() => {
                for plane in self.pod_planes(pod) {
                    self.paused[plane] = until;
                }
            }
            _ => {}
        }
    }

    fn unapplied(&self) -> u64 {
        self.tallies()
            .map(|(admitted, counted)| admitted.saturating_sub(counted))
            .sum()
    }

    fn counted_twice(&self) -> u64 {
        self.tallies()
            .map(|(admitted, counted)| counted.saturating_sub(admitted))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Approach, Beside, Fault, Network, Run, Shape, Workload, run};
    use edgerush_ratelimit::service::pooled::Levers;
    use std::num::NonZeroU64;

    const SECOND: u64 = 1_000_000_000;

    fn bursts(fault: Fault, seed: u64) -> Run {
        let n = |n| NonZeroU64::new(n).unwrap();
        Run {
            contract: crate::Contract {
                limit: n(100),
                window: n(SECOND),
                burst: n(100),
            },
            pods: 4,
            workload: Workload {
                shape: Shape::Bursts { each: 100 },
                span: 6 * SECOND,
                conforming: false,
            },
            approach: Approach::Nodes { cycle: 10_000_000 },
            network: Network::reliable(1_000_000, 500_000),
            fault,
            seed,
        }
    }

    /// What a run of `span` on `planes` data planes may admit under `contract` (06 §1): its
    /// rate over the whole run and 5% more, and a bucket for each data plane.
    fn allowed(contract: &crate::Contract, span: u64, planes: u64) -> u64 {
        let rate = contract.limit.get() * span / contract.window.get();
        rate + rate / 20 + planes * contract.burst.get()
    }

    /// Pooled, a pod's process one participant: what a pod admits
    /// does not depend on how many workers it has — the same traffic on 1, 4 and 8 workers
    /// a pod comes out the same — bursts on every worker of 4 processes are held to what is
    /// allowed, and a conforming
    /// client moving between a pod's workers is never refused, its process's background
    /// held for 100 ms across the move or not. Every admission is counted once.
    #[test]
    fn a_pooled_pod_decides_alike_whatever_its_workers_and_refuses_nothing_conforming() {
        let pooled = |workers| Approach::Pooled {
            cycle: 10_000_000,
            exchange_timeout: 0,
            workers,
        };
        for seed in 0..3 {
            let spread = |workers: usize| {
                run(&Run {
                    pods: 4 * workers,
                    approach: pooled(workers),
                    workload: Workload {
                        shape: Shape::Spread { per_window: 300 },
                        span: 6 * SECOND,
                        conforming: false,
                    },
                    ..bursts(Fault::None, seed)
                })
            };
            let one = spread(1);
            for workers in [4, 8] {
                let many = spread(workers);
                assert_eq!(
                    (many.admitted, many.refused, many.messages),
                    (one.admitted, one.refused, one.messages),
                    "{seed}: {workers} workers"
                );
            }
            let burst = run(&Run {
                pods: 32,
                approach: pooled(8),
                workload: Workload {
                    shape: Shape::Bursts { each: 100 / 8 },
                    span: 6 * SECOND,
                    conforming: false,
                },
                ..bursts(Fault::None, seed)
            });
            let central = run(&Run {
                pods: 32,
                approach: Approach::Central,
                workload: burst_workload(),
                ..bursts(Fault::None, seed)
            });
            assert!(
                burst.admitted <= allowed(&bursts(Fault::None, seed).contract, 6 * SECOND, 4),
                "{seed}: {} against {}",
                burst.admitted,
                central.admitted
            );
            assert_eq!(burst.counted_twice, 0);
            let span = 6 * SECOND;
            let mut waited = Vec::new();
            for fault in [
                Fault::None,
                Fault::Pause {
                    at: 2 * span / 3 - 50_000_000,
                    length: 100_000_000,
                    pod: 3 * 8,
                },
            ] {
                let moving = run(&Run {
                    pods: 32,
                    approach: pooled(8),
                    workload: Workload {
                        // Spread over half a pod's workers, then on one, then on one it
                        // had not reached: an ask of its process, held or not.
                        shape: Shape::Migrating {
                            per_window: 100,
                            last: 4,
                            to: 8,
                        },
                        span,
                        conforming: true,
                    },
                    fault,
                    ..bursts(Fault::None, seed)
                });
                assert_eq!(moving.refused, 0, "{seed} {fault:?}");
                assert_eq!(
                    (moving.lost, moving.counted_twice),
                    (0, 0),
                    "{seed} {fault:?}"
                );
                waited.push((moving.waited, moving.waited_for));
            }
            // The worker it moved to finds the pod's entry itself: the process held up
            // holds up none of its requests.
            assert_eq!(waited[1], waited[0], "{seed}: waits with the process held");
        }
    }

    /// Pooled visiting only the keys that need it each cycle keeps pooled's guarantees,
    /// with the protocol's levers and with quiet keys not polled: bursts on every pod
    /// admitted as when every key is visited, within 5%, nothing counted twice; a conforming
    /// client moving between a pod's workers never refused, nothing lost; steady overload
    /// admitted as when every key is visited, within 2%, with no more messages.
    #[test]
    fn a_scheduled_pooled_pod_keeps_pooleds_guarantees() {
        let no_poll = Levers { poll_quiet: false };
        for levers in [Levers::default(), no_poll] {
            let pooled = |scheduled| Approach::PooledWith {
                cycle: 10_000_000,
                exchange_timeout: 0,
                workers: 8,
                levers,
                scheduled,
            };
            for seed in 0..3 {
                let burst = run(&Run {
                    pods: 32,
                    approach: pooled(true),
                    workload: burst_workload(),
                    ..bursts(Fault::None, seed)
                });
                let visited = run(&Run {
                    pods: 32,
                    approach: pooled(false),
                    workload: burst_workload(),
                    ..bursts(Fault::None, seed)
                });
                assert!(
                    burst.admitted <= visited.admitted + visited.admitted / 20,
                    "{levers:?} {seed}: {} against {}",
                    burst.admitted,
                    visited.admitted
                );
                assert_eq!(burst.counted_twice, 0, "{levers:?} {seed}");
                let moving = run(&Run {
                    pods: 32,
                    approach: pooled(true),
                    workload: Workload {
                        shape: Shape::Migrating {
                            per_window: 100,
                            last: 4,
                            to: 8,
                        },
                        span: 6 * SECOND,
                        conforming: true,
                    },
                    ..bursts(Fault::None, seed)
                });
                assert_eq!(moving.refused, 0, "{levers:?} {seed}");
                assert_eq!(
                    (moving.lost, moving.counted_twice),
                    (0, 0),
                    "{levers:?} {seed}"
                );
                let overload = |scheduled| {
                    run(&Run {
                        pods: 32,
                        approach: pooled(scheduled),
                        workload: Workload {
                            shape: Shape::Spread { per_window: 300 },
                            span: 6 * SECOND,
                            conforming: false,
                        },
                        ..bursts(Fault::None, seed)
                    })
                };
                let (scheduled, every) = (overload(true), overload(false));
                assert!(
                    scheduled.admitted.abs_diff(every.admitted) <= every.admitted / 50,
                    "{levers:?} {seed}: {} against {}",
                    scheduled.admitted,
                    every.admitted
                );
                assert!(
                    scheduled.messages <= every.messages,
                    "{levers:?} {seed}: {} messages against {}",
                    scheduled.messages,
                    every.messages
                );
            }
        }
    }

    /// 200 a second with a bucket of 100, over a minute, on 4 data planes and on 4
    /// processes of 8 workers: spread, uneven, on one data plane, over one process's
    /// workers and in bursts on every data plane over 300 ms, at the limit, half over it and
    /// three times it, and a whole bucket on every data plane at once each second, no more
    /// gets through than is allowed, and no less than one bucket admits, but for the few a
    /// data plane refuses while its copy has yet to hear what others gave back. A minute,
    /// so that a leak past 5% shows beside the fixed bucket per data plane.
    #[test]
    fn a_limit_held_over_a_minute_lets_through_no_more_than_allowed() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = crate::Contract {
            limit: n(200),
            window: n(SECOND),
            burst: n(100),
        };
        let shapes: [fn(u64, u64) -> Shape; 6] = [
            |per, _| Shape::Spread { per_window: per },
            |per, _| Shape::Uneven {
                per_window: per,
                hot: 50,
            },
            |per, _| Shape::Skewed { per_window: per },
            |per, _| Shape::Within {
                per_window: per,
                last: 8,
            },
            |per, pods| Shape::SpreadBursts {
                each: per / pods,
                over: 300_000_000,
            },
            // The same at every load: a bucket a data plane, its workers sharing it.
            |_, pods| Shape::Bursts {
                each: 100 / (pods / 4),
            },
        ];
        let approaches = [
            (4, Approach::Nodes { cycle: 10_000_000 }),
            (
                32,
                Approach::PooledWith {
                    cycle: 10_000_000,
                    exchange_timeout: 0,
                    workers: 8,
                    levers: Levers { poll_quiet: false },
                    scheduled: true,
                },
            ),
        ];
        for seed in 0..2 {
            for (index, shape) in shapes.iter().enumerate() {
                for per_second in [200, 300, 600] {
                    for (pods, approach) in approaches {
                        let base = Run {
                            contract,
                            pods,
                            workload: Workload {
                                shape: shape(per_second, pods as u64),
                                span: 60 * SECOND,
                                conforming: false,
                            },
                            approach,
                            network: Network::reliable(1_000_000, 500_000),
                            fault: Fault::None,
                            seed,
                        };
                        let outcome = run(&base);
                        let central = run(&Run {
                            approach: Approach::Central,
                            ..base
                        });
                        let said = format!(
                            "{seed} shape {index} {per_second}/s {pods} pods: {} against {}",
                            outcome.admitted, central.admitted
                        );
                        assert!(
                            outcome.admitted <= allowed(&contract, 60 * SECOND, 4),
                            "{said}"
                        );
                        assert!(outcome.admitted * 100 >= central.admitted * 99, "{said}");
                        assert_eq!(outcome.counted_twice, 0, "{said}");
                    }
                }
            }
        }
    }

    fn burst_workload() -> Workload {
        Workload {
            shape: Shape::Bursts { each: 100 / 8 },
            span: 6 * SECOND,
            conforming: false,
        }
    }

    /// A whole bucket on every pod at once, every second: the nodes admit no more than is
    /// allowed — with the owner lost half way too.
    #[test]
    fn the_nodes_hold_bursts_on_every_pod_to_what_is_allowed() {
        for fault in [Fault::None, Fault::OwnerLost { at: 3 * SECOND }] {
            for seed in 0..3 {
                let outcome = run(&bursts(fault, seed));
                let central = run(&Run {
                    approach: Approach::Central,
                    ..bursts(fault, seed)
                });
                assert!(
                    outcome.admitted <= allowed(&bursts(fault, seed).contract, 6 * SECOND, 4),
                    "{fault:?} {seed}: {} against {}",
                    outcome.admitted,
                    central.admitted
                );
                assert_eq!(outcome.counted_twice, 0, "{fault:?}");
            }
        }
    }

    /// A request under a limit over the whole data plane too is decided under both: the
    /// tighter binds, whichever of the two is the run's, and every admission is counted once
    /// under each. Messages take a fixed delay: a second key, owned elsewhere, sends twice
    /// as many, and with jitter drawn from one stream for all of them the runs compared
    /// would not see the same network.
    #[test]
    fn a_request_under_a_limit_over_the_data_plane_too_is_held_to_the_tighter() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let limit = |limit, burst| crate::Contract {
            limit: n(limit),
            window: n(SECOND),
            burst: n(burst),
        };
        for seed in 0..3 {
            let base = Run {
                contract: limit(20, 20),
                workload: Workload {
                    shape: Shape::Bursts { each: 20 },
                    span: 6 * SECOND,
                    conforming: false,
                },
                network: Network::reliable(1_000_000, 0),
                ..bursts(Fault::None, seed)
            };
            let alone = run(&base);
            let under = |global| {
                run(&Run {
                    approach: Approach::NodesUnder {
                        cycle: 10_000_000,
                        beside: Beside::Global(global),
                    },
                    ..base
                })
            };
            let wider = under(limit(200, 200));
            assert_eq!(wider.admitted, alone.admitted, "{seed}: the run's binds");
            assert_eq!((wider.lost, wider.counted_twice), (0, 0));
            let tighter = under(limit(10, 10));
            let global_alone = run(&Run {
                contract: limit(10, 10),
                ..base
            });
            assert_eq!(
                tighter.admitted, global_alone.admitted,
                "{seed}: the global binds, as it would alone"
            );
            assert_eq!((tighter.lost, tighter.counted_twice), (0, 0));
        }
    }

    /// Two routes under one limit over the data plane, their own limits smaller, bursts on
    /// every pod at once, cold each time: every limit decided from the data plane's copies,
    /// the shared limit is held as it would be alone. Every admission is counted once under
    /// each limit.
    #[test]
    fn routes_sharing_a_limit_hold_it_as_alone() {
        let route = crate::Contract {
            limit: NonZeroU64::new(80).unwrap(),
            window: NonZeroU64::new(SECOND).unwrap(),
            burst: NonZeroU64::new(80).unwrap(),
        };
        for seed in 0..3 {
            let base = Run {
                pods: 2,
                ..bursts(Fault::None, seed)
            };
            let alone = run(&base);
            let under = run(&Run {
                approach: Approach::NodesUnder {
                    cycle: 10_000_000,
                    beside: Beside::Routes(route),
                },
                ..base
            });
            assert_eq!(under.worst_excess, alone.worst_excess, "{seed}");
            assert_eq!((under.lost, under.counted_twice), (0, 0));
        }
    }

    /// A whole bucket on every pod arriving over 30 ms, as a real client's burst does,
    /// spans three of a data plane's cycles: a share given out at one cycle shows in the
    /// others' copies only after the next, which must not share out the copy again, nor
    /// let them past what is allowed.
    #[test]
    fn bursts_arriving_over_a_few_cycles_hold_to_what_is_allowed() {
        let (mut admitted, mut central) = (0, 0);
        for seed in 0..10 {
            let run = Run {
                pods: 8,
                workload: Workload {
                    shape: Shape::SpreadBursts {
                        each: 100,
                        over: 30_000_000,
                    },
                    span: 10 * SECOND,
                    conforming: false,
                },
                ..bursts(Fault::None, seed)
            };
            admitted += crate::run(&run).admitted;
            central += crate::run(&Run {
                approach: Approach::Central,
                ..run
            })
            .admitted;
        }
        assert!(
            admitted <= 10 * allowed(&bursts(Fault::None, 0).contract, 10 * SECOND, 8),
            "{admitted} against {central}"
        );
    }

    /// Over TCP a lost message holds back what follows it on its connection for a
    /// retransmission timeout: a data plane whose batches are held up must still count as
    /// admitting, or every other one's share grows while it is stalled. At one packet in
    /// ten lost, whole buckets arriving at once on every pod still stay within what is
    /// allowed: each pod spends at most its full copy.
    /// Traffic spread over time is another matter: copies mostly stale, every data plane
    /// spends what its copy refills meanwhile, and the overshoot grows with the time the
    /// loss lasts (06 §4).
    #[test]
    fn bursts_hold_with_one_packet_in_ten_lost_over_tcp() {
        let tcp = Network {
            loss_per_mille: 100,
            retransmit: Some(200_000_000),
            ..Network::reliable(1_000_000, 500_000)
        };
        let (mut admitted, mut central) = (0, 0);
        for seed in 0..10 {
            let run = Run {
                pods: 8,
                network: tcp,
                approach: Approach::Nodes { cycle: 10_000_000 },
                ..bursts(Fault::None, seed)
            };
            admitted += crate::run(&run).admitted;
            central += crate::run(&Run {
                approach: Approach::Central,
                ..run
            })
            .admitted;
        }
        assert!(
            admitted <= 10 * allowed(&bursts(Fault::None, 0).contract, 6 * SECOND, 8),
            "{admitted} against {central}"
        );
    }
}
