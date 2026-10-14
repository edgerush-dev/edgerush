//! The rate-limiting service as the prototype runs it: the protocol's own state machines
//! (`edgerush_ratelimit::service`), one data plane per pod and three service pods, with the
//! simulator carrying their messages. Its results should match [`Service`](crate::service)
//! with the same choices, which it was taken from; where they differ, the nodes are what is
//! built.

use crate::{Cluster, Contract, FaultEvent, Message, Outbox};
use edgerush_ratelimit::service::{
    self, DataPlane, Decision, PlaneId, PodId, ServicePod, Ticket, View,
};
use std::collections::BTreeMap;

/// The service's pods.
const SERVICE_PODS: u32 = 3;

/// The key every request is for.
const KEY: &[u8] = b"key";

/// A fleet of data planes sharing one key through the protocol's nodes.
pub(crate) struct Nodes {
    contract: service::Contract,
    cycle: u64,
    /// How long a data plane waits for an ask's answer; 0 for four cycles.
    ask_timeout: u64,
    view: View,
    planes: Vec<DataPlane>,
    /// How many times each data plane has restarted: its id is its index and its life.
    lives: Vec<u64>,
    pods: Vec<ServicePod>,
    /// Service pods gone for good.
    gone: Vec<bool>,
    /// For each data plane, the request each of its tickets stands for.
    tickets: Vec<BTreeMap<Ticket, usize>>,
    /// Decisions for requests that waited, not yet taken.
    decided: Vec<(usize, bool)>,
    /// Hits admitted anywhere.
    admitted: u64,
}

impl Nodes {
    pub(crate) fn new(contract: &Contract, planes: usize, cycle: u64, ask_timeout: u64) -> Self {
        let view = View {
            epoch: 1,
            pods: (0..SERVICE_PODS).collect(),
        };
        Self {
            contract: service::Contract {
                limit: contract.limit,
                window: contract.window,
                burst: contract.burst,
            },
            cycle,
            ask_timeout,
            planes: (0..planes)
                .map(|plane| plane_of(id(plane, 0), cycle, ask_timeout, view.clone()))
                .collect(),
            lives: vec![0; planes],
            pods: (0..SERVICE_PODS)
                .map(|pod| ServicePod::new(pod, cycle, view.clone()))
                .collect(),
            gone: vec![false; SERVICE_PODS as usize],
            tickets: vec![BTreeMap::new(); planes],
            decided: Vec::new(),
            admitted: 0,
            view,
        }
    }

    /// The node a service pod is reached at: after the data planes.
    fn address(&self, pod: PodId) -> usize {
        self.planes.len() + pod as usize
    }

    /// Sends what data plane `plane` has to send, and takes its decisions.
    fn flush_plane(&mut self, plane: usize, outbox: &mut Outbox) {
        for (pod, batch) in self.planes[plane].take_outgoing() {
            outbox.send(self.address(pod), Message::Batch { from: plane, batch });
        }
        for (ticket, admitted) in self.planes[plane].take_decided() {
            if let Some(request) = self.tickets[plane].remove(&ticket) {
                self.admitted += u64::from(admitted);
                self.decided.push((request, admitted));
            }
        }
    }

    /// Sends the copies service pod `pod` has to send.
    fn flush_pod(&mut self, pod: usize, outbox: &mut Outbox) {
        let from = self.planes.len() + pod;
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
}

/// A data plane `id`, waiting `ask_timeout` for an ask's answer (0: four cycles).
fn plane_of(id: PlaneId, cycle: u64, ask_timeout: u64, view: View) -> DataPlane {
    let mut plane = DataPlane::new(id, cycle, view);
    if ask_timeout > 0 {
        plane.set_ask_timeout(ask_timeout);
    }
    plane
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
        let decision = self.planes[plane].decide(KEY, self.contract, now);
        let decided = match decision {
            Decision::Admit => {
                self.admitted += 1;
                Some(true)
            }
            Decision::Refuse => Some(false),
            Decision::Wait(ticket) => {
                self.tickets[plane].insert(ticket, request);
                None
            }
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
        if node < self.planes.len() {
            self.planes[node].on_cycle(now);
            self.flush_plane(node, outbox);
        } else {
            let pod = node - self.planes.len();
            if !self.gone[pod] {
                self.pods[pod].on_timer(now);
            }
        }
    }

    fn on_message(&mut self, to: usize, message: Message, now: u64, outbox: &mut Outbox) {
        match message {
            Message::Batch { from, batch } if to >= self.planes.len() => {
                let pod = to - self.planes.len();
                if self.gone[pod] {
                    return;
                }
                let answers = self.pods[pod].on_batch(batch, now);
                outbox.send(from, Message::Answers { from: to, answers });
                self.flush_pod(pod, outbox);
            }
            Message::Replicas { replicas, .. } if to >= self.planes.len() => {
                let pod = to - self.planes.len();
                if !self.gone[pod] {
                    self.pods[pod].on_replicas(replicas, now);
                }
            }
            Message::Answers { answers, .. } if to < self.planes.len() => {
                self.planes[to].on_answers(answers, now);
                self.flush_plane(to, outbox);
            }
            _ => {}
        }
    }

    fn on_fault(&mut self, event: FaultEvent, now: u64, outbox: &mut Outbox) {
        match event {
            // The owning service pod goes for good, or restarts holding nothing: either way
            // the others agree it is gone and its secondary takes over from its copy.
            FaultEvent::OwnerLost | FaultEvent::OwnerRestarts => {
                let Some(owner) = self.owner() else {
                    return;
                };
                self.gone[owner] = true;
                if event == FaultEvent::OwnerRestarts {
                    self.pods[owner] =
                        ServicePod::new(owner as PodId, self.cycle, self.view.clone());
                }
                self.change_view(owner as PodId, now, outbox);
            }
            // A data plane restarts: a new life, with a new id; what waited on it is refused.
            FaultEvent::Restart { pod } if pod < self.planes.len() => {
                for ticket in self.planes[pod].abandon() {
                    if let Some(request) = self.tickets[pod].remove(&ticket) {
                        self.decided.push((request, false));
                    }
                }
                self.lives[pod] += 1;
                self.planes[pod] = plane_of(
                    id(pod, self.lives[pod]),
                    self.cycle,
                    self.ask_timeout,
                    self.view.clone(),
                );
            }
            _ => {}
        }
    }

    fn unapplied(&self) -> u64 {
        let counted = self.owner().and_then(|owner| self.pods[owner].counted(KEY));
        self.admitted.saturating_sub(counted.unwrap_or(0))
    }

    fn counted_twice(&self) -> u64 {
        let counted = self.owner().and_then(|owner| self.pods[owner].counted(KEY));
        counted.unwrap_or(0).saturating_sub(self.admitted)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Approach, Fault, Network, Run, Shape, Workload, run};
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
            approach: Approach::Nodes {
                cycle: 10_000_000,
                ask_timeout: 0,
            },
            network: Network::reliable(1_000_000, 500_000),
            fault,
            seed,
        }
    }

    /// A whole bucket on every pod at once, every second: the nodes hold the fleet to one
    /// bucket, as the simulator's own service does — with the owner lost half way too.
    #[test]
    fn the_nodes_hold_bursts_on_every_pod_to_one_bucket() {
        for fault in [Fault::None, Fault::OwnerLost { at: 3 * SECOND }] {
            for seed in 0..3 {
                let outcome = run(&bursts(fault, seed));
                let central = run(&Run {
                    approach: Approach::Central,
                    ..bursts(fault, seed)
                });
                assert!(
                    outcome.admitted <= central.admitted + central.admitted / 50,
                    "{fault:?} {seed}: {} against {}",
                    outcome.admitted,
                    central.admitted
                );
                assert_eq!(outcome.counted_twice, 0, "{fault:?}");
            }
        }
    }

    /// Over TCP a lost message holds back what follows it on its connection for a
    /// retransmission timeout: a data plane whose batches are held up must still count as
    /// admitting, or every other one's share grows while it is stalled; and an ask must
    /// wait longer than one retransmission, or what waits is decided from a copy that has
    /// heard nothing.
    #[test]
    fn bursts_hold_with_one_packet_in_ten_lost_over_tcp() {
        let tcp = Network {
            loss_per_mille: 100,
            retransmit: Some(200_000_000),
            ..Network::reliable(1_000_000, 500_000)
        };
        let (mut admitted, mut central) = (0, 0);
        for seed in 0..10 {
            // As the proxy runs it: an ask waits 300 ms, one retransmission.
            let run = Run {
                pods: 8,
                network: tcp,
                approach: Approach::Nodes {
                    cycle: 10_000_000,
                    ask_timeout: 300_000_000,
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
            admitted * 100 <= central * 110,
            "{admitted} against {central}"
        );
    }
}
