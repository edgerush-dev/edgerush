use super::*;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;

/// 100 a second, a bucket of ten: a refill time of 100 ms.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000 * MS),
        burst: n(10),
    }
}

fn view(pods: &[PodId], epoch: u64) -> View {
    View {
        epoch,
        pods: pods.to_vec(),
    }
}

/// The owner and secondary of `key` among all three pods, and the third.
fn roles(key: &[u8]) -> (PodId, PodId, PodId) {
    let (owner, secondary) = view(&[0, 1, 2], 1).owners(key);
    let (owner, secondary) = (owner.unwrap(), secondary.unwrap());
    let third = (0..3).find(|p| *p != owner && *p != secondary).unwrap();
    (owner, secondary, third)
}

fn without(pod: PodId) -> View {
    let pods: Vec<PodId> = (0..3).filter(|p| *p != pod).collect();
    view(&pods, 2)
}

fn hits(sequence: u64, count: u64, seen: u64) -> Report {
    Report::Hits {
        sequence,
        count,
        seen,
        age: 0,
    }
}

fn batch(from: PlaneId, report: Report) -> Batch {
    Batch {
        from,
        entries: vec![Entry {
            key: b"k"[..].into(),
            contract: contract(),
            report,
        }],
        made: 0,
    }
}

/// Data planes and three service pods, and what is on its way between them.
struct Net {
    planes: Vec<DataPlane>,
    pods: Vec<ServicePod>,
    to_pods: Vec<(PodId, Batch)>,
    replicas: Vec<(PodId, Replicas)>,
}

impl Net {
    fn new(planes: usize) -> Self {
        let view = view(&[0, 1, 2], 1);
        Self {
            planes: (0..planes)
                .map(|p| DataPlane::new(100 + p as u64, CYCLE, view.clone()))
                .collect(),
            pods: (0..3)
                .map(|p| ServicePod::new(p, CYCLE, view.clone()))
                .collect(),
            to_pods: Vec::new(),
            replicas: Vec::new(),
        }
    }

    /// Collects what every node has to send.
    fn collect(&mut self) {
        for plane in &mut self.planes {
            self.to_pods.extend(plane.take_outgoing());
        }
        for pod in &mut self.pods {
            self.replicas.extend(pod.take_replicas());
        }
    }

    /// What is on its way arrives at `now` — batches, their answers at once, and copies —
    /// and what that sends is collected.
    fn hop(&mut self, now: u64) {
        self.collect();
        for (pod, batch) in std::mem::take(&mut self.to_pods) {
            let plane = self
                .planes
                .iter()
                .position(|p| p.id() == batch.from)
                .unwrap();
            let answers = self.pods[pod as usize].on_batch(batch, now);
            self.planes[plane].on_answers(answers, now);
        }
        for (pod, replicas) in std::mem::take(&mut self.replicas) {
            self.pods[pod as usize].on_replicas(replicas, now);
        }
        self.collect();
    }

    /// Hops until nothing is on its way.
    fn settle(&mut self, now: u64) {
        for _ in 0..10 {
            self.hop(now);
            if self.to_pods.is_empty() && self.replicas.is_empty() {
                return;
            }
        }
    }

    fn admits(&mut self, plane: usize, key: &[u8], count: u64, now: u64) -> u64 {
        (0..count)
            .filter(|_| self.planes[plane].decide(key, contract(), now) == Decision::Admit)
            .count() as u64
    }

    fn owner(key: &[u8]) -> usize {
        roles(key).0 as usize
    }

    fn copy(&self, plane: usize, key: &[u8]) -> &PlaneKey {
        &self.planes[plane].keys[key]
    }

    fn set_view(&mut self, view: &View, now: u64) {
        for pod in &mut self.pods {
            pod.on_view(view.clone(), now);
        }
        for plane in &mut self.planes {
            plane.on_view(view.clone(), now);
        }
    }
}

/// A data plane decides its first request from a full bucket, and a cycle sends its hit to
/// the owner, whose answer the copy takes.
fn warmed(net: &mut Net, plane: usize, key: &[u8], now: u64) {
    net.planes[plane].decide(key, contract(), now);
    net.planes[plane].on_cycle(now);
    net.settle(now);
}

#[test]
fn hits_go_to_the_owner_and_a_copy_comes_back() {
    let mut net = Net::new(2);
    warmed(&mut net, 0, b"k", 0);
    warmed(&mut net, 1, b"k", 0);
    assert_eq!(net.admits(1, b"k", 4, 0), 4);
    net.planes[1].on_cycle(0);
    let sent = net.planes[1].take_outgoing();
    assert_eq!(sent.len(), 1, "one batch");
    assert_eq!(sent[0].0 as usize, Net::owner(b"k"), "to the owner only");
    net.to_pods.extend(sent);
    net.settle(0);
    assert_eq!(
        net.pods[Net::owner(b"k")].counted(b"k"),
        Some(6),
        "two warmed, four reported"
    );
    assert_eq!(net.copy(1, b"k").bucket.remaining(0), 4);
    assert_eq!(
        net.copy(0, b"k").bucket.remaining(0),
        9,
        "a data plane that sent nothing hears nothing"
    );
}

#[test]
fn one_batch_a_cycle_carries_every_key_its_owner_holds() {
    let mut net = Net::new(1);
    let keys: Vec<Vec<u8>> = (0..30).map(|k| format!("key-{k}").into_bytes()).collect();
    for key in &keys {
        warmed(&mut net, 0, key, 0);
        net.admits(0, key, 1, 0);
    }
    net.planes[0].on_cycle(0);
    let sent = net.planes[0].take_outgoing();
    let owners: std::collections::BTreeSet<usize> = keys.iter().map(|k| Net::owner(k)).collect();
    assert_eq!(sent.len(), owners.len(), "one batch per owner");
    let entries: usize = sent.iter().map(|(_, batch)| batch.entries.len()).sum();
    assert_eq!(entries, 30);
}

#[test]
fn a_pod_leaving_moves_only_the_keys_it_owned() {
    let all = view(&[0, 1, 2], 1);
    let mut owned = [0; 3];
    for k in 0..3_000 {
        let key = format!("k{k}").into_bytes();
        let (owner, secondary) = all.owners(&key);
        let (owner, secondary) = (owner.unwrap(), secondary.unwrap());
        assert_ne!(owner, secondary);
        owned[owner as usize] += 1;
        let after = without(1).owners(&key);
        if owner == 1 {
            assert_eq!(after.0, Some(secondary), "its secondary takes it over");
        } else {
            assert_eq!(after.0, Some(owner), "the rest stay where they were");
        }
    }
    assert!(
        owned.iter().all(|&n| (900..1_100).contains(&n)),
        "{owned:?}"
    );
}

/// With nothing to report, a data plane polls while the key is in use — a refill time
/// after its last request — and then stops.
#[test]
fn a_data_plane_polls_while_the_key_is_in_use() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.planes[0].on_cycle(100 * MS);
    let sent = net.planes[0].take_outgoing();
    let owner = Net::owner(b"k") as PodId;
    let to_owner: Vec<&Batch> = sent
        .iter()
        .filter(|(pod, batch)| *pod == owner && !batch.entries.is_empty())
        .map(|(_, batch)| batch)
        .collect();
    assert!(
        matches!(to_owner.as_slice(), [batch] if batch.entries[0].report == Report::Poll),
        "{sent:?}"
    );
    net.planes[0].on_cycle(100 * MS + 1);
    assert!(net.planes[0].take_outgoing().is_empty());
}

/// Its own hits stay in a data plane's copy until an answer shows them applied; a batch
/// that came ahead of a lost one waits, and both apply in order once it is sent again.
#[test]
fn a_copy_keeps_its_own_hits_until_the_owner_has_them() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 4, 0);
    net.planes[0].on_cycle(0);
    net.planes[0].take_outgoing(); // lost
    net.admits(0, b"k", 2, 0);
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    let owner = Net::owner(b"k");
    assert_eq!(net.pods[owner].counted(b"k"), Some(1), "the second waits");
    assert_eq!(
        net.copy(0, b"k").bucket.remaining(CYCLE),
        4,
        "the owner's ten, less its own six"
    );
    net.planes[0].on_cycle(4 * CYCLE);
    net.settle(4 * CYCLE);
    assert_eq!(net.pods[owner].counted(b"k"), Some(7), "both, in order");
    assert!(net.copy(0, b"k").unacknowledged.is_empty());
}

/// Hits that do not fit are owed while they would be paid back within a refill time, and
/// forgiven beyond: twenty-five in a bucket of ten leave it owing ten.
#[test]
fn the_owner_owes_excess_up_to_a_refill_time() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    pod.on_batch(batch(1, hits(1, 25, 0)), 0);
    assert_eq!(pod.counted(b"k"), Some(25));
    assert_eq!(pod.keys[&b"k"[..]].bucket().retry_at(0), Some(110 * MS));
}

/// A report of any number of hits is charged in one step, owed and forgiven as twenty-five
/// are: what a batch costs a pod grows with its size, not with the counts in it.
#[test]
fn a_report_of_any_number_of_hits_is_charged_at_once() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    pod.on_batch(batch(1, hits(1, u64::MAX, 0)), 0);
    assert_eq!(pod.counted(b"k"), Some(u64::MAX));
    assert_eq!(pod.keys[&b"k"[..]].bucket().retry_at(0), Some(110 * MS));
    pod.on_batch(batch(1, hits(2, u64::MAX, 0)), 0);
    assert_eq!(
        pod.counted(b"k"),
        Some(u64::MAX),
        "counted as many as it can hold"
    );
}

/// Reports kept waiting are added to the one they waited for without overflowing, however
/// many hits each carries: counted as many as the count can hold, not wrapped round to none.
#[test]
fn reports_that_waited_are_taken_in_with_no_overflow() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    let mut both = batch(1, hits(2, u64::MAX, 0));
    both.entries.extend(batch(1, hits(1, 1, 0)).entries);
    pod.on_batch(both, 0);
    assert_eq!(pod.counted(b"k"), Some(u64::MAX));
    assert_eq!(pod.keys[&b"k"[..]].planes.get(1).unwrap().applied, 2);
    assert_eq!(pod.keys[&b"k"[..]].bucket().retry_at(0), Some(110 * MS));
}

proptest::proptest! {
    /// Hits charged at once are charged as one by one: each owed while the bucket would
    /// admit again within a refill time, the rest forgiven.
    #[test]
    fn hits_charged_at_once_are_charged_as_one_by_one(
        reports in proptest::collection::vec((0u64..300, 0u64..40), 1..20),
        limit in 1u64..8,
        window in 1u64..2_000,
        burst in 1u64..12,
    ) {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract { limit: n(limit), window: n(window), burst: n(burst) };
        let most = contract.refill_time();
        let mut owned = Owned::new(contract, 0);
        let mut one_by_one = contract.bucket();
        let mut now = 0;
        for (gap, count) in reports {
            now += gap;
            owned.apply(count, now, now);
            for _ in 0..count {
                if one_by_one.retry_at(now).is_none_or(|retry| retry - now <= most) {
                    one_by_one.charge(now);
                }
            }
            proptest::prop_assert_eq!(owned.bucket(), one_by_one.clone());
        }
    }

}

#[test]
fn a_batch_unseen_for_a_refill_time_is_emptied_and_holds_nothing_back() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 4, 0);
    net.planes[0].on_cycle(0);
    net.planes[0].take_outgoing(); // lost
    net.planes[0].on_cycle(100 * MS);
    assert_eq!(net.copy(0, b"k").unacknowledged[0].count, 4, "still held");
    net.planes[0].take_outgoing();
    net.planes[0].on_cycle(100 * MS + 1);
    assert_eq!(net.copy(0, b"k").unacknowledged[0].count, 0, "emptied");
    net.planes[0].take_outgoing();
    net.admits(0, b"k", 2, 110 * MS);
    net.planes[0].on_cycle(140 * MS);
    net.settle(140 * MS);
    assert_eq!(
        net.pods[Net::owner(b"k")].counted(b"k"),
        Some(3),
        "the empty one, then the two"
    );
    assert!(net.copy(0, b"k").unacknowledged.is_empty());
}

/// The owner counts the others admitting as of the latest admission it knows of, not as of
/// now: data planes that burst together and fell quiet count together.
#[test]
fn the_owner_counts_those_admitting_as_of_its_latest_traffic() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    pod.on_batch(batch(1, hits(1, 1, 0)), 0);
    let answer = pod.on_batch(batch(2, hits(1, 1, 0)), 0);
    assert_eq!(answer.entries[0].others, 1);
    let later = 5_000 * MS;
    let answer = pod.on_batch(batch(3, hits(1, 1, 0)), later);
    assert_eq!(answer.entries[0].others, 0, "the two before, long ago");
    let answer = pod.on_batch(batch(1, hits(2, 1, 0)), later + CYCLE);
    assert_eq!(answer.entries[0].others, 1, "the third, a cycle before");
    let answer = pod.on_batch(batch(2, Report::Poll), later + 100 * CYCLE);
    assert_eq!(
        answer.entries[0].others, 2,
        "nothing since: the two last admitting"
    );
}

/// A report sent again, its batch applied already, keeps its data plane counted with those
/// admitting as of the latest admission, and no later: arriving long after a burst, it
/// leaves the data planes that admitted together counted together. One that brings a batch
/// not applied yet is an admission then, however late it comes.
#[test]
fn a_report_sent_again_admits_no_later_than_the_latest() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    for plane in 1..=3 {
        pod.on_batch(batch(plane, hits(1, 1, 0)), 0);
    }
    // Its answer lost, the first data plane's batch is sent again, and comes late.
    let answer = pod.on_batch(batch(1, hits(1, 1, 0)), 400 * MS);
    assert_eq!(answer.entries[0].others, 2, "the three still together");
    let answer = pod.on_batch(batch(1, hits(2, 1, 1)), 400 * MS);
    assert_eq!(
        answer.entries[0].others, 0,
        "a new batch, the others long before"
    );

    // While another admits, one whose batch comes again is counted with it.
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    pod.on_batch(batch(1, hits(1, 1, 0)), 0);
    pod.on_batch(batch(2, hits(1, 1, 0)), 0);
    pod.on_batch(batch(2, hits(2, 1, 1)), 250 * MS);
    pod.on_batch(batch(1, hits(1, 1, 0)), 250 * MS);
    let answer = pod.on_batch(batch(2, hits(3, 1, 2)), 500 * MS);
    assert_eq!(answer.entries[0].others, 1, "the first, sent again lately");
}

/// A data plane may admit a cycle its share of what its copy has left and the cycle
/// refills, divided among it and the others admitting; one request more if it left one
/// unspent the cycle before.
#[test]
fn a_data_plane_is_bounded_to_its_share_and_carries_one_over() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let at = net.planes[0].keys.get_mut(&b"k"[..]).unwrap();
    at.others = 2;
    // A first cycle: nothing carried from one before, nothing shared.
    at.allowance = None;
    at.shared = None;
    net.planes[0].on_cycle(0);
    // Nine left, one refilled, among three.
    assert_eq!(net.copy(0, b"k").allowance, Some(3));
    assert_eq!(net.admits(0, b"k", 5, 0), 3);
    net.planes[0].on_cycle(CYCLE);
    net.planes[0].on_cycle(2 * CYCLE);
    let at = net.copy(0, b"k");
    let share = (at.bucket.remaining(2 * CYCLE) + 1) / 3;
    assert_eq!(
        at.allowance,
        Some(share + 1),
        "one carried, not the whole share left unspent"
    );
    net.planes[0].keys.get_mut(&b"k"[..]).unwrap().others = 50;
    // The cycle after a share is held back; the one after that shares afresh.
    net.planes[0].on_cycle(3 * CYCLE);
    net.planes[0].on_cycle(4 * CYCLE);
    assert_eq!(
        net.copy(0, b"k").allowance,
        Some(2),
        "at least one, plus one carried"
    );
}

/// What the others admit under a share shows in the copy only a cycle later: the cycle
/// after a share gets its share of the refill and keeps what it left unspent, and the one
/// after that shares out the copy afresh.
#[test]
fn the_cycle_after_a_share_gets_the_refill_and_what_was_left() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let at = net.planes[0].keys.get_mut(&b"k"[..]).unwrap();
    at.others = 1;
    at.allowance = None;
    at.shared = None;
    net.planes[0].on_cycle(0);
    // Nine left, one refilled, between two.
    assert_eq!(net.copy(0, b"k").allowance, Some(5));
    assert_eq!(net.admits(0, b"k", 2, 0), 2);
    net.planes[0].on_cycle(CYCLE);
    assert_eq!(
        net.copy(0, b"k").allowance,
        Some(1 + 3),
        "half the one refilled, at least one, and the three left; not (8 + 1) / 2 + 1"
    );
    net.planes[0].on_cycle(2 * CYCLE);
    let at = net.copy(0, b"k");
    // What is left and one refilled, between two.
    let sees = at.bucket.remaining(2 * CYCLE) + 1;
    assert_eq!(
        at.allowance,
        Some(sees / 2 + 1),
        "shared afresh, one carried"
    );
}

/// Alone in admitting the key, a data plane has no one else's hits to wait for: every cycle
/// shares out the copy.
#[test]
fn a_data_plane_admitting_alone_shares_every_cycle() {
    let mut net = Net::new(1);
    // Warmed, it has shared out the copy at 0.
    warmed(&mut net, 0, b"k", 0);
    assert_eq!(net.admits(0, b"k", 4, 0), 4);
    net.planes[0].on_cycle(CYCLE);
    let at = net.copy(0, b"k");
    assert_eq!(at.others, 0);
    let sees = at.bucket.remaining(CYCLE) + 1;
    assert_eq!(
        at.allowance,
        Some(sees + 1),
        "the copy shared afresh, one carried; not the refill and the six left"
    );
}

/// Cut off from the owner, a data plane holds back no more than every other cycle, and what
/// it carries does not build up while it is quiet: quiet cycles do not leave it a bucket's
/// worth to admit at once.
#[test]
fn a_cut_off_data_plane_does_not_build_up_what_it_carries() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let at = net.planes[0].keys.get_mut(&b"k"[..]).unwrap();
    at.others = 7;
    at.allowance = None;
    at.shared = None;
    net.planes[0].on_cycle(0);
    // Heard just after the share, and never again.
    net.planes[0].keys.get_mut(&b"k"[..]).unwrap().heard = Some(1);
    let mut most = 0;
    // Twenty cycles: the key is forgotten after two refill times without a request.
    for cycle in 1..20 {
        net.planes[0].on_cycle(cycle * CYCLE);
        net.planes[0].take_outgoing();
        most = most.max(net.copy(0, b"k").allowance.unwrap_or(0));
    }
    // A share of a full bucket among eight, one carried, and one held-back cycle's floor.
    assert!(most <= 11 / 8 + 1 + 1, "{most}");
}

/// The owner copies every change to the key's secondary, which takes over from that copy
/// when the owner leaves: data planes send it again the batches not seen applied, numbered
/// as before, and it applies only those its copy lacks.
#[test]
fn the_secondary_takes_over_from_its_copy() {
    let (owner, secondary, _) = roles(b"k");
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 3, 0);
    net.planes[0].on_cycle(0);
    net.settle(0);
    assert_eq!(
        net.pods[secondary as usize].counted(b"k"),
        Some(4),
        "copied"
    );
    net.admits(0, b"k", 2, CYCLE);
    net.planes[0].on_cycle(CYCLE);
    // The batch reaches the owner and is copied, but its answer is lost.
    for (_, batch) in net.planes[0].take_outgoing() {
        net.pods[owner as usize].on_batch(batch, CYCLE);
    }
    net.collect();
    net.hop(CYCLE);
    assert_eq!(net.pods[secondary as usize].counted(b"k"), Some(6));
    net.set_view(&without(owner), 2 * CYCLE);
    net.settle(2 * CYCLE);
    assert_eq!(
        net.pods[secondary as usize].counted(b"k"),
        Some(6),
        "nothing counted twice"
    );
    let copy = net.copy(0, b"k");
    assert!(
        copy.unacknowledged.is_empty(),
        "seen applied by the new owner"
    );
    assert_eq!(copy.owner, Some(secondary));
}

/// A new owner whose copy missed batches skips to what the data plane saw applied.
#[test]
fn a_new_owner_skips_the_batches_its_copy_missed() {
    let (owner, secondary, _) = roles(b"k");
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 3, 0);
    net.planes[0].on_cycle(0);
    net.hop(0);
    net.replicas.clear(); // the copy is lost
    net.admits(0, b"k", 2, CYCLE);
    net.planes[0].on_cycle(CYCLE);
    net.planes[0].take_outgoing(); // and so is the next batch
    net.set_view(&without(owner), 2 * CYCLE);
    net.settle(2 * CYCLE);
    assert_eq!(
        net.pods[secondary as usize].counted(b"k"),
        Some(1 + 2),
        "the ask copied, the first batch lost with its copy, the second applied"
    );
    assert!(net.copy(0, b"k").unacknowledged.is_empty());
}

#[test]
fn a_copy_from_a_pod_that_no_longer_owns_the_key_is_ignored() {
    let (owner, secondary, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(secondary, CYCLE, without(owner));
    let replica = Replica {
        key: b"k"[..].into(),
        contract: contract(),
        behind: 0,
        counted: 5,
        planes: Vec::new(),
    };
    pod.on_replicas(
        Replicas {
            from: owner,
            entries: vec![replica],
            handover: false,
            epoch: 0,
        },
        0,
    );
    assert_eq!(pod.counted(b"k"), None);
}

#[test]
fn every_change_is_copied_to_every_other_pod() {
    let (owner, secondary, third) = roles(b"k");
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 3, 0);
    net.planes[0].on_cycle(0);
    net.settle(0);
    for pod in [secondary, third] {
        assert_eq!(net.pods[pod as usize].counted(b"k"), Some(4), "pod {pod}");
    }
    assert_eq!(net.pods[owner as usize].counted(b"k"), Some(4));
}

/// A pod joining holds no copies: the owners send it one of each key at once.
#[test]
fn a_pod_joining_gets_a_copy_of_every_key_at_once() {
    let (owner, secondary, third) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, without(third));
    pod.on_batch(batch(1, hits(1, 2, 0)), 0);
    assert!(matches!(pod.take_replicas().as_slice(), [(to, _)] if *to == secondary));
    pod.on_view(view(&[0, 1, 2], 3), CYCLE);
    let copies = pod.take_replicas();
    assert!(
        matches!(copies.as_slice(), [(to, r)] if *to == third && !r.handover && r.entries[0].counted == 2),
        "{copies:?}"
    );
}

/// With a copy on every pod, the owner lost and then the pod that took over lost too
/// leaves the last pod going on from its own copy: nothing had to be sent between.
#[test]
fn the_owner_lost_twice_in_turn_loses_nothing() {
    let (owner, _, third) = roles(b"k");
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 3, 0);
    net.planes[0].on_cycle(0);
    net.settle(0);
    net.set_view(&without(owner), CYCLE);
    // The second pod goes before anything was sent at the change.
    net.collect();
    net.replicas.clear();
    net.set_view(&view(&[third], 3), CYCLE);
    net.settle(CYCLE);
    assert_eq!(net.pods[third as usize].counted(b"k"), Some(4));
    net.admits(0, b"k", 2, 2 * CYCLE);
    net.planes[0].on_cycle(2 * CYCLE);
    net.settle(2 * CYCLE);
    assert_eq!(net.pods[third as usize].counted(b"k"), Some(6));
    assert_eq!(net.copy(0, b"k").owner, Some(third));
}

#[test]
fn an_answer_from_a_pod_that_is_not_the_owner_is_ignored() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let (_, not_owner, _) = roles(b"k");
    net.planes[0].on_answers(
        Answers {
            from: not_owner,
            entries: vec![Answer {
                key: b"k"[..].into(),
                behind: 0,
                applied: 0,
                others: 5,
            }],
            view: None,
            life: 0,
            number: 0,
            made: CYCLE,
        },
        CYCLE,
    );
    assert_eq!(net.copy(0, b"k").others, 0);
}

#[test]
fn a_changed_contract_starts_the_key_afresh() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.admits(0, b"k", 5, 0);
    let mut bigger = contract();
    bigger.burst = NonZeroU64::new(20).unwrap();
    assert_eq!(
        net.planes[0].decide(b"k", bigger, 0),
        Decision::Admit,
        "a new key, from a full bucket"
    );
    net.planes[0].on_cycle(0);
    net.settle(0);
    assert_eq!(
        net.pods[Net::owner(b"k")].counted(b"k"),
        Some(1),
        "the owner too starts afresh"
    );
}

#[test]
fn quiet_keys_are_forgotten() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.planes[0].on_cycle(200 * MS);
    assert!(
        net.planes[0].keys.contains_key(&b"k"[..]),
        "two refill times: kept"
    );
    net.planes[0].on_cycle(200 * MS + 1);
    assert!(net.planes[0].keys.is_empty(), "the data plane's copy");
    let owner = Net::owner(b"k");
    net.pods[owner].on_timer(100 * MS);
    assert_eq!(
        net.pods[owner].counted(b"k"),
        Some(1),
        "a refill time: kept"
    );
    net.pods[owner].on_timer(100 * MS + 1);
    assert_eq!(net.pods[owner].counted(b"k"), None, "the owner's");
}

#[test]
fn a_bucket_set_from_how_far_it_is_from_full_reads_the_same() {
    let mut bucket = contract().bucket();
    for at in [0, 0, 0, 3 * MS, 4 * MS] {
        bucket.charge(at);
    }
    let mut copy = contract().bucket();
    copy.set_behind(7_000 * MS, bucket.behind(5 * MS));
    for later in [0, MS, 30 * MS, 100 * MS] {
        assert_eq!(
            copy.remaining(7_000 * MS + later),
            bucket.remaining(5 * MS + later)
        );
    }
}

/// Only the owner's copies are kept: one from another pod is ignored.
#[test]
fn a_pod_keeps_copies_only_from_the_owner() {
    let (owner, secondary, third) = roles(b"k");
    let replica = |from| Replicas {
        from,
        entries: vec![Replica {
            key: b"k"[..].into(),
            contract: contract(),
            behind: 0,
            counted: 5,
            planes: Vec::new(),
        }],
        handover: false,
        epoch: 0,
    };
    let mut pod: ServicePod = ServicePod::new(third, CYCLE, view(&[0, 1, 2], 1));
    pod.on_replicas(replica(secondary), 0);
    assert_eq!(pod.counted(b"k"), None);
    pod.on_replicas(replica(owner), 0);
    assert_eq!(pod.counted(b"k"), Some(5), "the third pod keeps one too");
}

/// A batch the owner has applied, arriving again, is answered and nothing more: it is not
/// kept waiting.
#[test]
fn a_batch_applied_arriving_again_is_not_kept() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 1));
    pod.on_batch(batch(1, hits(1, 2, 0)), 0);
    let answer = pod.on_batch(batch(1, hits(1, 2, 0)), CYCLE);
    assert_eq!(answer.entries[0].applied, 1);
    assert_eq!(pod.counted(b"k"), Some(2));
    assert!(pod.keys[&b"k"[..]].planes.get(1).unwrap().waiting.is_none());
}

/// Batches arriving in order hold no room for batches kept waiting; one arriving ahead of
/// a batch still missing is kept, and the room given back once the missing one comes.
#[test]
fn only_batches_ahead_of_a_missing_one_hold_room() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let room = |pod: &ServicePod| {
        pod.keys[&b"k"[..]]
            .planes
            .get(1)
            .unwrap()
            .waiting
            .as_ref()
            .map_or(0, |waiting| waiting.capacity())
    };
    pod.on_batch(batch(1, hits(1, 1, 0)), 0);
    pod.on_batch(batch(1, hits(2, 1, 1)), CYCLE);
    assert_eq!(room(&pod), 0, "in order");
    pod.on_batch(batch(1, hits(4, 1, 2)), 2 * CYCLE);
    assert_eq!(pod.counted(b"k"), Some(2), "the fourth waits for the third");
    assert!(room(&pod) > 0);
    pod.on_batch(batch(1, hits(3, 1, 2)), 3 * CYCLE);
    assert_eq!(pod.counted(b"k"), Some(4));
    assert_eq!(room(&pod), 0, "given back");
}

/// A pod that leaves and comes back owns its keys again: the pod that held them meanwhile
/// hands them over, so the count goes on where it was.
#[test]
fn a_pod_back_in_the_view_is_handed_its_keys() {
    let (owner, secondary, _) = roles(b"k");
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    net.set_view(&without(owner), CYCLE);
    net.pods[owner as usize] = ServicePod::new(owner, CYCLE, without(owner)); // restarted
    net.admits(0, b"k", 3, CYCLE);
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    assert_eq!(net.pods[secondary as usize].counted(b"k"), Some(4));
    let all = view(&[0, 1, 2], 3);
    net.set_view(&all, 2 * CYCLE);
    net.settle(2 * CYCLE);
    assert_eq!(
        net.pods[owner as usize].counted(b"k"),
        Some(4),
        "handed over by the pod that held it"
    );
    net.admits(0, b"k", 2, 3 * CYCLE);
    net.planes[0].on_cycle(3 * CYCLE);
    net.settle(3 * CYCLE);
    assert_eq!(net.pods[owner as usize].counted(b"k"), Some(6));
    assert_eq!(net.copy(0, b"k").owner, Some(owner));
}

/// Hits that reach the new owner before the handover are charged on top of what it is
/// handed, not lost under it.
#[test]
fn a_handover_arriving_late_keeps_what_the_new_owner_applied() {
    let (owner, secondary, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 3));
    pod.on_batch(batch(9, hits(1, 2, 0)), 0);
    let handed = Replica {
        key: b"k"[..].into(),
        contract: contract(),
        behind: contract().bucket().behind(0) + 5 * u128::from(1_000 * MS),
        counted: 5,
        planes: vec![PlaneRecord {
            plane: 9,
            applied: 0,
            admitted_ago: Some(0),
        }],
    };
    pod.on_replicas(
        Replicas {
            from: secondary,
            entries: vec![handed],
            handover: true,
            epoch: 0,
        },
        0,
    );
    assert_eq!(pod.counted(b"k"), Some(7));
    assert_eq!(
        pod.keys[&b"k"[..]].bucket().remaining(0),
        3,
        "ten less five less two"
    );
    assert_eq!(pod.keys[&b"k"[..]].planes.get(9).unwrap().applied, 1);
}

/// A handover reaching a pod that does not own the key is ignored.
#[test]
fn a_handover_to_a_pod_that_does_not_own_the_key_is_ignored() {
    let (_, secondary, third) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(third, CYCLE, view(&[0, 1, 2], 3));
    pod.on_replicas(
        Replicas {
            from: secondary,
            entries: vec![Replica {
                key: b"k"[..].into(),
                contract: contract(),
                behind: 0,
                counted: 5,
                planes: Vec::new(),
            }],
            handover: true,
            epoch: 0,
        },
        0,
    );
    assert_eq!(pod.counted(b"k"), None);
}

/// What a pod owning `k` would hold of it after `batches` from data plane 9, as a copy.
fn held_after(batches: &[Report]) -> Replica {
    let mut pod: ServicePod = ServicePod::new(roles(b"k").0, CYCLE, view(&[0, 1, 2], 3));
    for report in batches {
        pod.on_batch(batch(9, *report), 0);
    }
    pod.keys[&b"k"[..]].replica(b"k", 0)
}

/// The owner of `k`, having applied one hit, is left out of the view and taken back in —
/// seeing the view without it or not — and is handed `k` back by the pod that held it
/// meanwhile, which applied one more: then, before the handback, it applies `fresh`.
fn retaken(saw_itself_left_out: bool, fresh: Option<Report>) -> ServicePod {
    let (owner, secondary, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 3));
    pod.on_batch(batch(9, hits(1, 1, 0)), 0);
    if saw_itself_left_out {
        let pods: Vec<PodId> = (0..3).filter(|p| *p != owner).collect();
        pod.on_view(view(&pods, 4), 0);
    }
    pod.on_view(view(&[0, 1, 2], 5), 0);
    if let Some(report) = fresh {
        pod.on_batch(batch(9, report), 0);
    }
    pod.on_replicas(
        Replicas {
            from: secondary,
            entries: vec![held_after(&[hits(1, 1, 0), hits(2, 1, 1)])],
            handover: true,
            epoch: 5,
        },
        0,
    );
    pod
}

/// A pod taken back into the view still holding a key is handed it back holding all it
/// held before, and more: what it held is not charged again on top.
#[test]
fn a_key_handed_back_to_a_pod_that_still_holds_it_is_counted_once() {
    for saw_itself_left_out in [true, false] {
        let pod = retaken(saw_itself_left_out, None);
        assert_eq!(pod.counted(b"k"), Some(2), "{saw_itself_left_out}");
        assert_eq!(
            pod.keys[&b"k"[..]].bucket().remaining(0),
            8,
            "{saw_itself_left_out}: ten less two"
        );
    }
}

/// Hits it applies once taken back in, before the handback, are charged on top of it.
#[test]
fn a_key_handed_back_keeps_what_its_pod_applied_since_it_was_taken_back() {
    for saw_itself_left_out in [true, false] {
        let pod = retaken(saw_itself_left_out, Some(hits(3, 1, 2)));
        assert_eq!(pod.counted(b"k"), Some(3), "{saw_itself_left_out}");
        assert_eq!(
            pod.keys[&b"k"[..]].bucket().remaining(0),
            7,
            "{saw_itself_left_out}: ten less three"
        );
        assert_eq!(pod.keys[&b"k"[..]].planes.get(9).unwrap().applied, 3);
    }
}

/// A key handed over twice, the same, is counted as it was handed, once.
#[test]
fn a_key_handed_over_twice_is_counted_once() {
    let (owner, secondary, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 3));
    for _ in 0..2 {
        pod.on_replicas(
            Replicas {
                from: secondary,
                entries: vec![held_after(&[hits(1, 1, 0), hits(2, 1, 1)])],
                handover: true,
                epoch: 3,
            },
            0,
        );
    }
    assert_eq!(pod.counted(b"k"), Some(2));
    assert_eq!(pod.keys[&b"k"[..]].bucket().remaining(0), 8);
}

/// A pod not yet taken into the view answers with its view and decides nothing.
#[test]
fn a_pod_not_serving_answers_with_its_view_alone() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 4));
    pod.set_serving(false);
    let answers = pod.on_batch(batch(1, hits(1, 3, 0)), 0);
    assert!(answers.entries.is_empty());
    assert_eq!(answers.view, Some(view(&[0, 1, 2], 4)));
    assert_eq!(pod.counted(b"k"), None);
    pod.set_serving(true);
    let answers = pod.on_batch(batch(1, hits(1, 3, 0)), 0);
    assert_eq!(answers.entries.len(), 1);
    assert_eq!(pod.counted(b"k"), Some(3));
}

/// A data plane takes over a later view from any answer, and moves its keys with it.
#[test]
fn a_data_plane_takes_a_later_view_from_an_answer() {
    let (owner, secondary, _) = roles(b"k");
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let later = without(owner);
    for pod in &mut net.pods {
        pod.on_view(later.clone(), CYCLE);
    }
    // The plane hears of it only in an answer about another key, to its report.
    let other = (0..100)
        .map(|k| format!("o{k}").into_bytes())
        .find(|k| roles(k).0 == secondary)
        .unwrap();
    net.planes[0].decide(&other, contract(), CYCLE);
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    assert_eq!(net.planes[0].view, later);
    assert_eq!(net.copy(0, b"k").owner, Some(secondary));
}

/// Cut off from an owner — a key in use it has heard nothing about for four cycles — a data
/// plane asks the pods in turn for their view: an empty batch to each. The key is pod 2's,
/// so that the polls, to pods 0 and 1, go alone rather than beside what it sends its owner.
#[test]
fn a_data_plane_cut_off_asks_the_pods_for_their_view() {
    let mut net = Net::new(1);
    let key = (0..100)
        .map(|k| format!("c{k}").into_bytes())
        .find(|k| roles(k).0 == 2)
        .unwrap();
    net.planes[0].decide(&key, contract(), 0);
    net.planes[0].on_cycle(4 * CYCLE);
    let first = net.planes[0].take_outgoing();
    net.planes[0].on_cycle(5 * CYCLE);
    let second = net.planes[0].take_outgoing();
    let polled = |sent: &[(PodId, Batch)]| {
        sent.iter()
            .filter(|(_, batch)| batch.entries.is_empty())
            .map(|(pod, _)| *pod)
            .collect::<Vec<_>>()
    };
    assert_eq!(polled(&first).len(), 1);
    assert_eq!(polled(&second).len(), 1);
    assert_ne!(polled(&first), polled(&second), "in turn");
}

/// An owner that has stopped answering a key the data plane has been hearing about cuts it
/// off too: it asks the pods for their view.
#[test]
fn an_owner_silent_for_four_cycles_sends_the_data_plane_asking_for_the_view() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let empty = |sent: &[(PodId, Batch)]| sent.iter().filter(|(_, b)| b.entries.is_empty()).count();
    for cycle in 0..4 {
        net.admits(0, b"k", 1, cycle * CYCLE);
        net.planes[0].on_cycle(cycle * CYCLE);
        assert_eq!(
            empty(&net.planes[0].take_outgoing()),
            0,
            "cycle {cycle}: still in time"
        );
    }
    net.planes[0].on_cycle(4 * CYCLE);
    assert_eq!(empty(&net.planes[0].take_outgoing()), 1);
}

/// A handover can reach a pod joining before the view that takes it in: it is kept, and
/// taken once the pod holds that view.
#[test]
fn a_handover_ahead_of_the_view_is_kept_until_the_view_comes() {
    let (owner, secondary, _) = roles(b"k");
    let mut joining: ServicePod = ServicePod::new(owner, CYCLE, without(owner));
    let handover = Replicas {
        from: secondary,
        entries: vec![Replica {
            key: b"k"[..].into(),
            contract: contract(),
            behind: 0,
            counted: 8,
            planes: Vec::new(),
        }],
        handover: true,
        epoch: 3,
    };
    joining.on_replicas(handover, 0);
    assert_eq!(joining.counted(b"k"), None, "not its key yet");
    joining.on_view(view(&[0, 1, 2], 3), CYCLE);
    assert_eq!(joining.counted(b"k"), Some(8), "taken with the view");
}

/// A pod just joined makes no key afresh for a while: what it is to own may still be on its
/// way to it. Keys it holds it answers for at once.
#[test]
fn a_pod_just_joined_waits_before_making_a_key_afresh() {
    let (owner, _, _) = roles(b"k");
    let mut joining: ServicePod = ServicePod::new(owner, CYCLE, without(owner));
    joining.set_join_grace(5 * CYCLE);
    joining.on_view(view(&[0, 1, 2], 3), 0);
    let answers = joining.on_batch(batch(1, Report::Poll), 4 * CYCLE);
    assert!(answers.entries.is_empty(), "within the grace: nothing made");
    assert_eq!(joining.counted(b"k"), None);
    let answers = joining.on_batch(batch(1, Report::Poll), 5 * CYCLE);
    assert_eq!(answers.entries.len(), 1, "after it: made afresh");
}

/// Two keys owned by different pods.
fn apart() -> (Vec<u8>, Vec<u8>) {
    let a = b"limit-a".to_vec();
    let b = (0..)
        .map(|n| format!("limit-b{n}").into_bytes())
        .find(|b| roles(b).0 != roles(&a).0)
        .unwrap();
    (a, b)
}

fn under<'a>(keys: &[&'a [u8]]) -> Vec<(&'a [u8], Contract)> {
    keys.iter().map(|key| (*key, contract())).collect()
}

/// Limits whose keys it holds fresh copies of are decided here: one refusing refuses, and
/// the others are not charged.
#[test]
fn several_limits_decided_here_are_charged_to_all_or_none() {
    let (a, b) = apart();
    let mut net = Net::new(1);
    warmed(&mut net, 0, &a, 0);
    warmed(&mut net, 0, &b, 0);
    assert_eq!(net.admits(0, &b, 9, 0), 9);
    let before = net.copy(0, &a).bucket.remaining(0);
    assert_eq!(
        net.planes[0].decide_all(&under(&[&a, &b]), 0),
        Decision::Refuse
    );
    assert_eq!(net.copy(0, &a).bucket.remaining(0), before, "a not charged");
    net.planes[0].on_cycle(CYCLE);
    net.planes[0].take_outgoing();
    assert_eq!(
        net.planes[0].decide_all(&under(&[&a, &b]), CYCLE),
        Decision::Admit
    );
    assert_eq!(net.copy(0, &a).bucket.remaining(CYCLE), before, "a charged");
    assert_eq!(net.copy(0, &b).bucket.remaining(CYCLE), 0, "b charged");
}

/// Any number of limits is decided all or none, and the same key named twice — a caller's
/// mistake — is decided rather than refused by the map.
#[test]
fn any_number_of_limits_is_decided_and_a_key_named_twice_is_no_panic() {
    let keys: Vec<Vec<u8>> = (0..5).map(|k| format!("limit-{k}").into_bytes()).collect();
    let mut net = Net::new(1);
    for key in &keys {
        warmed(&mut net, 0, key, 0);
    }
    for count in 2..=5 {
        let list: Vec<&[u8]> = keys[..count].iter().map(Vec::as_slice).collect();
        assert_eq!(
            net.planes[0].decide_all(&under(&list), 0),
            Decision::Admit,
            "{count}"
        );
    }
    assert_eq!(net.copy(0, &keys[4]).bucket.remaining(0), 8);
    assert_eq!(net.copy(0, &keys[0]).bucket.remaining(0), 5);
    assert_eq!(net.admits(0, &keys[4], 8, 0), 8);
    let list: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    assert_eq!(net.planes[0].decide_all(&under(&list), 0), Decision::Refuse);
    assert_eq!(net.copy(0, &keys[0]).bucket.remaining(0), 5, "none charged");
    let twice = [&keys[1][..], &keys[1][..]];
    assert_eq!(net.planes[0].decide_all(&under(&twice), 0), Decision::Admit);
}

/// A refused request waits as long as the longest of its keys: its bucket's refill to one,
/// the latest when several refuse, whatever their order; that key is named, the first of
/// those that wait as long.
#[test]
fn a_refused_request_waits_for_the_latest_of_its_keys() {
    let (a, b) = apart();
    // Ten a second: one request back every 100 ms.
    let slow = Contract {
        limit: NonZeroU64::new(10).unwrap(),
        ..contract()
    };
    let mut net = Net::new(1);
    warmed(&mut net, 0, &a, 0);
    net.planes[0].decide(&b, slow, 0);
    net.planes[0].on_cycle(0);
    net.planes[0].take_outgoing();
    let both = [(&a[..], contract()), (&b[..], slow)];
    let plane = &mut net.planes[0];
    assert_eq!(plane.retry_after(&both, 0), None, "both admit");
    // A hundred a second: one request back every 10 ms.
    while plane.decide(&a, contract(), 0) == Decision::Admit {}
    assert_eq!(plane.retry_after(&both, 0), Some((0, 10 * MS)));
    let twice = [(&a[..], contract()), (&a[..], contract())];
    assert_eq!(
        plane.retry_after(&twice, 0),
        Some((0, 10 * MS)),
        "the first"
    );
    while plane.decide(&b, slow, 0) == Decision::Admit {}
    assert_eq!(
        plane.retry_after(&both, 0),
        Some((1, 100 * MS)),
        "the latest"
    );
    let reversed = [(&b[..], slow), (&a[..], contract())];
    assert_eq!(plane.retry_after(&reversed, 0), Some((0, 100 * MS)));
}

/// A key with room whose share of the cycle is spent waits for the next cycle.
#[test]
fn a_key_refused_for_its_share_waits_for_the_next_cycle() {
    // A bucket of a hundred, which two data planes admitting share half each.
    let big = Contract {
        burst: NonZeroU64::new(100).unwrap(),
        ..contract()
    };
    let mut net = Net::new(2);
    for plane in 0..2 {
        net.planes[plane].decide(b"k", big, 0);
    }
    net.planes[1].decide(b"k", big, 0);
    net.planes[1].on_cycle(CYCLE);
    net.settle(CYCLE);
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    // The cycle after a share keeps what was left; the one after that shares afresh,
    // halving what the copy holds between the two data planes.
    net.planes[0].on_cycle(2 * CYCLE);
    net.settle(2 * CYCLE);
    net.planes[0].on_cycle(3 * CYCLE);
    net.planes[0].take_outgoing();
    let plane = &mut net.planes[0];
    while plane.decide(b"k", big, 3 * CYCLE) == Decision::Admit {}
    let copy = &plane.keys[&b"k"[..]];
    assert!(
        copy.bucket.admits(3 * CYCLE),
        "room in the bucket: refused for its share"
    );
    assert_eq!(
        plane.retry_after(&[(b"k", big)], 3 * CYCLE),
        Some((0, CYCLE))
    );
}

/// Keys `k0`… `k{n-1}`, each decided once by `plane` and the cycle's batch acknowledged,
/// the `n`th a millisecond after the one before.
fn used_in_turn(net: &mut Net, plane: usize, n: u64) -> Vec<Vec<u8>> {
    (0..n)
        .map(|at| {
            let key = format!("k{at}").into_bytes();
            let now = at * MS;
            net.planes[plane].decide(&key, contract(), now);
            net.planes[plane].on_cycle(now);
            net.settle(now);
            key
        })
        .collect()
}

/// Past its capacity, a data plane evicts the keys it used least recently down to nine
/// tenths of it, even one with hits still to be acknowledged, which are forgiven.
#[test]
fn a_data_plane_past_its_capacity_evicts_the_least_recently_used() {
    let mut net = Net::new(1);
    let keys = used_in_turn(&mut net, 0, 5);
    let each = plane_key_bytes(&keys[0]);
    assert_eq!(net.planes[0].holding(), 5 * each);
    // Room for three, nine tenths of that for two: the two used last stay.
    net.planes[0].set_capacity(3 * each);
    net.planes[0].on_cycle(5 * MS);
    let kept = |plane: &DataPlane| -> Vec<bool> {
        keys.iter()
            .map(|key| plane.keys.contains_key(&key[..]))
            .collect()
    };
    assert_eq!(kept(&net.planes[0]), [false, false, false, true, true]);
    assert_eq!(net.planes[0].holding(), 2 * each);
    assert_eq!(net.planes[0].evicted(), 3);
    assert_eq!(net.planes[0].forgiven(), 0, "nothing outstanding");
    // The newest has two hits not yet acknowledged: room for less than one, and it goes,
    // its hits forgiven.
    net.admits(0, &keys[4], 2, 6 * MS);
    net.planes[0].set_capacity(each / 2);
    net.planes[0].on_cycle(6 * MS);
    assert_eq!(kept(&net.planes[0]), [false; 5]);
    assert_eq!(net.planes[0].evicted(), 5);
    assert_eq!(net.planes[0].forgiven(), 2);
    assert_eq!(net.planes[0].holding(), 0);
}

/// A key evicted for capacity with a batch lost on its way is counted afresh by its owner
/// when made again: its first report says every earlier batch was seen, so the owner does
/// not hold it back waiting for the one that will never come.
#[test]
fn a_key_evicted_with_hits_outstanding_is_counted_afresh() {
    let mut net = Net::new(1);
    let owner = Net::owner(b"k");
    warmed(&mut net, 0, b"k", 0);
    assert_eq!(net.admits(0, b"k", 2, MS), 2);
    net.planes[0].on_cycle(MS);
    let lost = net.planes[0].take_outgoing();
    assert_eq!(lost.len(), 1);
    net.planes[0].set_capacity(1);
    net.planes[0].on_cycle(2 * MS);
    assert!(net.planes[0].keys.is_empty(), "evicted");
    assert_eq!(net.planes[0].forgiven(), 2);
    net.planes[0].set_capacity(u64::MAX);
    warmed(&mut net, 0, b"k", 3 * MS);
    assert_eq!(
        net.pods[owner].counted(b"k"),
        Some(2),
        "the first life's warming hit and the new one; the lost two forgiven"
    );
}

/// A key evicted and made again numbers its reports past those its earlier life sent: its
/// owner, which still holds that life's numbers, counts them rather than taking them for
/// reports it has applied.
#[test]
fn a_key_made_again_is_counted_from_its_first_report() {
    let mut net = Net::new(1);
    let owner = Net::owner(b"k");
    warmed(&mut net, 0, b"k", 0);
    assert_eq!(net.admits(0, b"k", 2, MS), 2);
    net.planes[0].on_cycle(MS);
    net.settle(MS);
    assert_eq!(net.pods[owner].counted(b"k"), Some(3));
    net.planes[0].set_capacity(1);
    net.planes[0].on_cycle(2 * MS);
    assert!(net.planes[0].keys.is_empty(), "evicted");
    net.planes[0].set_capacity(u64::MAX);
    warmed(&mut net, 0, b"k", 3 * MS);
    assert_eq!(net.pods[owner].counted(b"k"), Some(4), "asked about");
    assert_eq!(net.admits(0, b"k", 2, 4 * MS), 2);
    net.planes[0].on_cycle(4 * MS);
    net.settle(4 * MS);
    assert_eq!(net.pods[owner].counted(b"k"), Some(6), "reported");
}

/// Cut off from its owner, a data plane keeps at most `MOST_UNACKNOWLEDGED` reports out for
/// a key, and later hits wait in its ledger; once the owner is back, every hit is counted,
/// once.
#[test]
fn a_data_plane_keeps_a_bounded_number_of_reports_out() {
    // A refill time of a second: no report grows older than it within the test.
    let roomy = Contract {
        limit: NonZeroU64::new(1_000).unwrap(),
        burst: NonZeroU64::new(1_000).unwrap(),
        ..contract()
    };
    let mut net = Net::new(1);
    let owner = Net::owner(b"k");
    for cycle in 1..=50 {
        let now = cycle * CYCLE;
        assert_eq!(net.planes[0].decide(b"k", roomy, now), Decision::Admit);
        net.planes[0].on_cycle(now);
        let _lost = net.planes[0].take_outgoing();
    }
    let copy = net.copy(0, b"k");
    assert_eq!(copy.unacknowledged.len(), MOST_UNACKNOWLEDGED);
    assert_eq!(copy.unsent, 50 - MOST_UNACKNOWLEDGED as u64, "waiting");
    for cycle in 51..=60 {
        net.planes[0].on_cycle(cycle * CYCLE);
        net.settle(cycle * CYCLE);
    }
    assert_eq!(net.pods[owner].counted(b"k"), Some(50));
    assert!(net.copy(0, b"k").unacknowledged.is_empty());
}

/// Past its capacity, a service pod evicts the keys that changed least recently down to
/// nine tenths of it.
#[test]
fn a_service_pod_past_its_capacity_evicts_the_least_recently_changed() {
    // The only pod: it owns every key.
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let keys: Vec<Vec<u8>> = (0..5_u64).map(|at| format!("k{at}").into_bytes()).collect();
    for (at, key) in (0_u64..).zip(&keys) {
        let mut hit = batch(100, hits(1, 1, 0));
        hit.entries[0].key = key[..].into();
        pod.on_batch(hit, at * MS);
    }
    let now = 5 * MS;
    pod.on_timer(now);
    let bytes: Vec<u64> = keys
        .iter()
        .map(|key| pod_key_bytes(key, &pod.keys[&key[..]]))
        .collect();
    assert_eq!(pod.holding(), bytes.iter().sum::<u64>());
    // Room for the newest three; nine tenths of that holds the newest two.
    pod.set_capacity(bytes[2..].iter().sum());
    pod.on_timer(now);
    let kept: Vec<bool> = keys
        .iter()
        .map(|key| pod.keys.contains_key(&key[..]))
        .collect();
    assert_eq!(kept, [false, false, false, true, true]);
    assert_eq!(pod.holding(), bytes[3] + bytes[4]);
    assert_eq!(pod.evicted(), 3);
}

/// A service pod counts room for as many records as a key's data planes need, not a
/// B-tree node's eleven: one for the usual key, admitted by one data plane, and growing
/// one at a time, then doubling.
#[test]
fn a_service_pod_counts_room_for_the_records_a_key_has() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let entry = size_of::<(Box<[u8]>, Owned)>() + 1 + b"k".len();
    let record = size_of::<(PlaneId, Record)>();
    for (plane, room) in [(7, 1), (3, 2), (9, 4), (5, 4), (1, 8)] {
        pod.on_batch(batch(plane, hits(1, 1, 0)), 0);
        let bytes = pod_key_bytes(b"k", &pod.keys[&b"k"[..]]);
        assert_eq!(bytes, (entry + room * record) as u64, "plane {plane}");
    }
}

/// A service pod's key takes 128 bytes in its table, which holds up to twice the room its
/// keys take, and its record of a data plane 32: its bucket kept as the bucket's state, its
/// terms the key's contract, batches waiting, which rarely do, kept apart, and the data
/// plane's last admission in eight bytes.
#[test]
fn a_service_pods_key_is_small() {
    assert_eq!(size_of::<(Box<[u8]>, Owned)>(), 128);
    assert_eq!(size_of::<(PlaneId, Record)>(), 32);
}

/// A data plane's last admission, kept in eight bytes, reads back as it was set and orders
/// as an option of it does: never before any time, time 0 among them.
#[test]
fn a_last_admission_reads_back_and_orders_as_an_option() {
    for at in [None, Some(0), Some(1), Some(5 * MS)] {
        assert_eq!(Admitted::from(at).get(), at);
    }
    assert!(Admitted::from(None) < Admitted::at(0));
    assert!(Admitted::at(0) < Admitted::at(1));
    assert_eq!(Admitted::default(), Admitted::from(None));
}

proptest::proptest! {
    /// A key's records behave as a map from data plane to record: each data plane's found,
    /// made when first wanted, and gone through in order of data plane.
    #[test]
    fn records_are_a_map_of_data_planes(
        uses in proptest::collection::vec((0u64..24, 1u64..100), 0..64)
    ) {
        let mut records = Records::default();
        let mut model: BTreeMap<PlaneId, u64> = BTreeMap::new();
        for (plane, applied) in uses {
            records.entry(plane).applied += applied;
            *model.entry(plane).or_default() += applied;
        }
        for plane in 0..25 {
            proptest::prop_assert_eq!(
                records.get(plane).map(|record| record.applied),
                model.get(&plane).copied()
            );
        }
        let seen: Vec<(PlaneId, u64)> =
            records.iter().map(|(plane, record)| (plane, record.applied)).collect();
        proptest::prop_assert_eq!(seen, model.into_iter().collect::<Vec<_>>());
    }

    /// Records made from a copy's list, in any order, are kept in order of data plane, the
    /// last of a data plane's taken.
    #[test]
    fn records_from_a_list_are_a_map_of_its_last(
        list in proptest::collection::vec((0u64..24, 1u64..100), 0..64)
    ) {
        let records: Records = list
            .iter()
            .map(|&(plane, applied)| (plane, Record { applied, ..Record::default() }))
            .collect();
        let model: BTreeMap<PlaneId, u64> = list.into_iter().collect();
        let seen: Vec<(PlaneId, u64)> =
            records.iter().map(|(plane, record)| (plane, record.applied)).collect();
        proptest::prop_assert_eq!(seen, model.into_iter().collect::<Vec<_>>());
    }
}

/// A data plane's share of the cycle, admitting with others: what its copy has left and the
/// cycle refills, divided among them.
#[test]
fn a_share_of_the_cycle_divides_what_the_copy_has_among_those_admitting() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let at = net.planes[0].keys.get_mut(&b"k"[..]).unwrap();
    at.others = 2;
    at.allowance = None;
    at.shared = None;
    net.planes[0].on_cycle(0);
    // Nine left, one refilled, among three.
    assert_eq!(net.copy(0, b"k").allowance, Some(3));
}

/// A hit is charged as of when it was admitted, its age counted back from the batch's
/// arrival: one admitted 30 ms before, at 100 a second, has refilled by the time it arrives.
#[test]
fn a_hit_is_charged_as_of_when_it_was_admitted() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let mut late = batch(100, hits(1, 1, 0));
    late.made = 7;
    if let Report::Hits { age, .. } = &mut late.entries[0].report {
        *age = 30 * MS;
    }
    let answers = pod.on_batch(late, 1_000 * MS);
    assert_eq!(answers.made, 7, "said back");
    let bucket = pod.keys[&b"k"[..]].bucket();
    assert_eq!(bucket.remaining(1_000 * MS), 10, "refilled since");
    assert_eq!(answers.entries[0].behind, 0);
}

/// The owner charges back to the earliest hit however long ago, past the key's refill time
/// too: 15 hits said to be 150 ms old, at 100 a second with a bucket of 10, are charged
/// from then and have all refilled as they arrive.
#[test]
fn hits_are_charged_back_to_the_earliest_past_a_refill_time() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let mut old = batch(100, hits(1, 15, 0));
    if let Report::Hits { age, .. } = &mut old.entries[0].report {
        *age = 150 * MS;
    }
    pod.on_batch(old, 1_000 * MS);
    assert_eq!(pod.keys[&b"k"[..]].bucket().remaining(1_000 * MS), 10);
}

/// A batch's ages count back from when it was made, the latest anything joined it: entries
/// that joined earlier age with it.
#[test]
fn a_batch_ages_what_joined_it_earlier() {
    let mut queued = Queued::default();
    queued.at(10 * MS).entries.push(Entry {
        key: b"k"[..].into(),
        contract: contract(),
        report: Unseen {
            sequence: 1,
            count: 1,
            admitted: 4 * MS,
            first: 10 * MS,
            last: 10 * MS,
        }
        .report(0, 10 * MS),
    });
    queued.at(25 * MS);
    queued.at(20 * MS);
    let batch = queued.batch(100);
    assert_eq!(batch.made, 25 * MS);
    assert!(matches!(batch.entries[0].report, Report::Hits { age, .. } if age == 21 * MS));
}

/// One data plane and its pod, batches taking `there` to reach the pod and answers `back`
/// to return, a cycle every `CYCLE`: requests at `times`, each decided at once from the
/// copy. Whether each was admitted.
fn decided_over_round_trips(
    c: Contract,
    times: &[u64],
    there: u64,
    back: u64,
) -> Vec<Option<bool>> {
    let mut plane: DataPlane = DataPlane::new(100, CYCLE, view(&[0], 1));
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let mut decided = vec![None; times.len()];
    // In order of arrival: answers, and when. Each takes as long, so sent in order, they
    // arrive in order.
    let mut answers: VecDeque<(u64, Answers)> = VecDeque::new();
    let mut next_cycle = CYCLE;
    let last = times.last().copied().unwrap_or(0);
    let mut at = 0;
    loop {
        let request = times.get(at).copied();
        let answer = answers.front().map(|(when, _)| *when);
        // Requests first at the same instant, as the harder order.
        let now = [request, Some(next_cycle), answer]
            .into_iter()
            .flatten()
            .min()
            .unwrap();
        if request == Some(now) {
            decided[at] = Some(plane.decide(b"k", c, now) == Decision::Admit);
            at += 1;
        } else if answer == Some(now) {
            let (_, answer) = answers.pop_front().unwrap();
            plane.on_answers(answer, now);
        } else {
            if next_cycle > last + 4 * CYCLE && at == times.len() && answers.is_empty() {
                return decided;
            }
            plane.on_cycle(now);
            next_cycle += CYCLE;
        }
        for (_, batch) in plane.take_outgoing() {
            let answer = pod.on_batch(batch, now + there);
            answers.push_back((now + there + back, answer));
        }
    }
}

proptest::proptest! {
    /// Traffic a plain token bucket admits all of, over many rates and bursts, is admitted
    /// all by one data plane however long its messages take: its hits are charged as of
    /// when it admitted them, its copy taken as of when it sent them, and requests that
    /// wait — the copy cold between answers — decided as of when they came. One request
    /// warms the key a refill time and a round trip before the traffic, and no gap is
    /// longer than a refill time, after which a bucket is full however long the gap. The
    /// bucket holds two cycles of its rate or more, as config insists, and a round trip
    /// fits in a cycle. A bucket that refills faster refuses a client within its rate: a
    /// copy answered full charges hits that waited from the answer's batch rather than from
    /// when they came (issue #54); so does a cycle shorter than the round trip, whose
    /// answers are taken as of a cycle before they come (issue #56).
    #[test]
    fn what_a_plain_bucket_admits_a_data_plane_admits_over_round_trips(
        limit in 1u64..200, interval in 1u64..50, burst in 1u64..12,
        gaps in proptest::collection::vec(
            proptest::prop_oneof![2 => proptest::strategy::Just(1u64), 1 => 0u64..3, 1 => 0u64..40],
            1..80,
        ),
        jitter in proptest::collection::vec(0u64..1_000_000, 80),
        there in 0u64..5_000_000, back in 0u64..5_000_000,
    ) {
        let interval = interval * MS + 1;
        let burst = burst.max((2 * CYCLE).div_ceil(interval));
        let n = |n| NonZeroU64::new(n).unwrap();
        let c = Contract { limit: n(limit), window: n(limit * interval), burst: n(burst) };
        // Gaps of whole intervals, exactly at the edge, some a little more.
        let refill = burst * interval;
        let mut plain = c.bucket();
        proptest::prop_assert!(plain.decide(0));
        let mut times = vec![0];
        let mut now = refill + there + back;
        for (gap, jitter) in gaps.iter().zip(&jitter) {
            now += (gap * interval + if *gap > 2 { *jitter } else { 0 }).min(refill);
            if plain.decide(now) {
                times.push(now);
            }
        }
        let decided = decided_over_round_trips(c, &times, there, back);
        let refused: Vec<(u64, Option<bool>)> = times
            .iter()
            .zip(&decided)
            .filter(|(_, admitted)| **admitted != Some(true))
            .map(|(at, admitted)| (*at, *admitted))
            .collect();
        proptest::prop_assert!(refused.is_empty(), "refused {:?} of {:?}", refused, times);
    }
}

/// The issue's case: a client at exactly one a second, a bucket of one, its hits reaching
/// the owner a cycle and a millisecond on, the answers a millisecond after.
#[test]
fn a_client_at_exactly_its_rate_is_admitted_after_a_round_trip() {
    let n = |n| NonZeroU64::new(n).unwrap();
    let c = Contract {
        limit: n(1),
        window: n(1_000 * MS),
        burst: n(1),
    };
    let times: Vec<u64> = (0..5).map(|k| k * 1_000 * MS).collect();
    let decided = decided_over_round_trips(c, &times, MS, MS);
    assert_eq!(decided, [Some(true); 5]);
}

/// What the copy has left and what the cycle refills are counted together and rounded once:
/// half a request left and 6.67 refilled, one request every 1.5 ms, is a share of 7 — not
/// 0 and 6, which would refuse a client at exactly its rate.
#[test]
fn a_share_rounds_what_is_left_and_the_refill_together() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let at = net.planes[0].keys.get_mut(&b"k"[..]).unwrap();
    let n = |n| NonZeroU64::new(n).unwrap();
    at.contract = Contract {
        limit: n(2),
        window: n(3 * MS),
        burst: n(1),
    };
    at.bucket = at.contract.bucket();
    at.bucket.charge_many(0, 1);
    at.others = 0;
    at.allowance = None;
    at.shared = None;
    net.planes[0].on_cycle(3 * MS / 4);
    assert_eq!(net.copy(0, b"k").allowance, Some(7));
}

/// Exchanges overlapping, a bucket of one that refills faster than a round trip: requests
/// at exactly the rate ask, and the owner holds what it admits for their hits. Its answers
/// say what it holds rather than taking it off the bucket, which a partial refill since
/// would leave short, so the copy is not left further behind than the owner.
#[test]
fn a_client_at_exactly_its_rate_is_admitted_with_exchanges_overlapping() {
    let n = |n| NonZeroU64::new(n).unwrap();
    let interval = 11 * MS + 1;
    let c = Contract {
        limit: n(1),
        window: n(interval),
        burst: n(1),
    };
    let times = [
        interval,
        2 * interval,
        3 * interval,
        4 * interval,
        176_494_783,
        309_000_000,
        309_000_000 + interval,
    ];
    let decided = decided_over_round_trips(c, &times, 4_773_750, 266_466);
    assert_eq!(decided, [Some(true); 7]);
}

/// The issue's case (#49): a bucket of one refilling faster than a round trip, so the copy
/// is cold for every request and each asks. The requests that come while the first ask is
/// out go in one ask after it; the owner decides them at one instant, against a bucket of
/// one, though they came an interval apart.
#[test]
fn a_client_at_exactly_its_rate_is_admitted_when_every_request_asks() {
    let n = |n| NonZeroU64::new(n).unwrap();
    let interval = MS + 1;
    let c = Contract {
        limit: n(1),
        window: n(interval),
        burst: n(1),
    };
    let times = [interval, 2 * interval, 3 * interval];
    let decided = decided_over_round_trips(c, &times, 3_270_000, 1_620_000);
    assert_eq!(decided, [Some(true); 3]);
}

/// Answers from a pod that does not say back when their batch was made — `made` 0 — come
/// from one that charges hits as they arrive: their buckets are taken as of when the
/// answers came, not as of time 0 and refilled since.
#[test]
fn answers_that_do_not_say_when_are_taken_as_they_come() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let (owner, _, _) = roles(b"k");
    let seen = net.copy(0, b"k").seen;
    net.planes[0].on_answers(
        Answers {
            from: owner,
            entries: vec![Answer {
                key: b"k"[..].into(),
                behind: 5 * u128::from(1_000 * MS),
                applied: seen,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 0,
        },
        500 * MS,
    );
    assert_eq!(net.copy(0, b"k").bucket.remaining(500 * MS), 5);
}

/// Answers whose batch was made long before they came — a lost packet waiting for TCP to
/// send it again — are taken as of a cycle before they came, not as of then: the trip's
/// refill counted no further than a cycle.
#[test]
fn answers_held_up_are_taken_as_of_no_more_than_a_cycle_before() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    let (owner, _, _) = roles(b"k");
    let seen = net.copy(0, b"k").seen;
    let behind = 5 * u128::from(1_000 * MS);
    net.planes[0].on_answers(
        Answers {
            from: owner,
            entries: vec![Answer {
                key: b"k"[..].into(),
                behind,
                applied: seen,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 200 * MS,
        },
        500 * MS,
    );
    // A cycle's refill at 100 a second, in the bucket's units.
    let refill = u128::from(CYCLE) * 100;
    assert_eq!(net.copy(0, b"k").bucket.behind(500 * MS), behind - refill);
}

/// What is queued for a pod goes a piece at a time: its entries in the order they were
/// queued, each piece no bigger than [`PIECE_BYTES`], and everything once.
#[test]
fn a_piece_takes_no_more_than_fits() {
    let key = |n: usize| -> Box<[u8]> {
        let mut key = vec![b'k'; 1_000];
        key[..8].copy_from_slice(&(n as u64).to_be_bytes());
        key.into()
    };
    let mut queued = Queued::default();
    let entries: Vec<Entry> = (0..3_000)
        .map(|n| Entry {
            key: key(n),
            contract: contract(),
            report: hits(1, 1, 0),
        })
        .collect();
    for entry in entries.iter().cloned() {
        queued.at(10 * MS).push_entry(entry);
    }
    let whole: usize = entries.iter().map(Entry::bytes).sum();
    assert!(whole > 3 * PIECE_BYTES, "{whole}: more than three pieces");
    let mut pieces = Vec::new();
    while !queued.is_empty() {
        let piece = queued.take_piece(100);
        assert!(piece.bytes() <= PIECE_BYTES, "{}", piece.bytes());
        assert_eq!((piece.from, piece.made), (100, 10 * MS));
        pieces.push(piece);
    }
    assert_eq!(pieces.len(), 4);
    let sent: Vec<Entry> = pieces.into_iter().flat_map(|piece| piece.entries).collect();
    assert_eq!(sent, entries, "every entry once, in order");
}

/// A piece takes at least one thing, however big: an entry bigger than a piece goes alone,
/// not never.
#[test]
fn a_piece_takes_an_entry_bigger_than_itself_alone() {
    let mut queued = Queued::default();
    for key in [vec![b'a'; 2 * PIECE_BYTES], vec![b'b'; 10]] {
        queued.push_entry(Entry {
            key: key.into(),
            contract: contract(),
            report: Report::Poll,
        });
    }
    let first = queued.take_piece(100);
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].key.len(), 2 * PIECE_BYTES);
    let second = queued.take_piece(100);
    assert_eq!(&*second.entries[0].key, b"bbbbbbbbbb");
    assert!(queued.is_empty());
}

/// Copies go a piece at a time: in order, every copy once, each piece no bigger than
/// [`PIECE_BYTES`] but for a copy bigger than a piece, which goes alone; each piece from
/// the same pod, under the same view, copies or handed over as the whole was.
#[test]
fn copies_go_a_piece_at_a_time() {
    let replica = |n: u64, key_bytes: usize, planes: u64| Replica {
        key: {
            let mut key = vec![b'k'; key_bytes];
            key[..8].copy_from_slice(&n.to_be_bytes());
            key.into()
        },
        contract: contract(),
        behind: u128::from(n),
        counted: n,
        planes: (0..planes)
            .map(|plane| PlaneRecord {
                plane,
                applied: 1,
                admitted_ago: Some(0),
            })
            .collect(),
    };
    let mut entries: Vec<Replica> = (0..3_000).map(|n| replica(n, 1_000, 2)).collect();
    entries.insert(1_500, replica(3_000, 2 * PIECE_BYTES, 0));
    let whole = Replicas {
        from: 2,
        entries: entries.clone(),
        handover: true,
        epoch: 9,
    };
    let pieces = whole.into_pieces();
    assert_eq!(pieces.len(), 5, "two either side of the one alone");
    for piece in &pieces {
        assert_eq!((piece.from, piece.handover, piece.epoch), (2, true, 9));
        assert!(
            piece.bytes() <= PIECE_BYTES || piece.entries.len() == 1,
            "{}",
            piece.bytes()
        );
    }
    let sent: Vec<Replica> = pieces.into_iter().flat_map(|piece| piece.entries).collect();
    assert_eq!(sent, entries);
    let empty = Replicas {
        from: 2,
        entries: Vec::new(),
        handover: false,
        epoch: 9,
    };
    assert!(empty.into_pieces().is_empty(), "nothing to send");
}
