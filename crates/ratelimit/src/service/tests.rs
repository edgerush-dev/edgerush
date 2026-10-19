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

fn asks(batches: &[(PodId, Batch)]) -> usize {
    batches.iter().map(|(_, batch)| batch.asks.len()).sum()
}

fn hits(sequence: u64, count: u64, seen: u64) -> Report {
    Report::Hits {
        sequence,
        count,
        seen,
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
        asks: Vec::new(),
    }
}

/// An ask from `from` about `count` requests under `keys`.
fn ask(from: PlaneId, id: u64, count: u64, keys: &[&[u8]]) -> Batch {
    Batch {
        from,
        entries: Vec::new(),
        asks: vec![Ask {
            id,
            count,
            keys: keys.iter().map(|key| ((*key).into(), contract())).collect(),
        }],
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

/// A data plane asks about its first request and hears back, and a cycle bounds the copy:
/// warm, it decides at once. What the cycle has to send is left unsent.
fn warmed(net: &mut Net, plane: usize, key: &[u8], now: u64) {
    net.planes[plane].decide(key, contract(), now);
    net.settle(now);
    net.planes[plane].take_decided();
    net.planes[plane].on_cycle(now);
    net.planes[plane].take_outgoing();
}

fn waits(decision: Decision) -> bool {
    matches!(decision, Decision::Wait(_))
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
        "two asked about, four reported"
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

/// An ask about any number of requests is decided in one step: as many admitted as every
/// key has room for.
#[test]
fn an_ask_about_any_number_of_requests_is_decided_at_once() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let answers = pod.on_batch(ask(1, 1, u64::MAX, &[b"a", b"b"]), 0);
    assert_eq!(answers.decided, [(1, 10)]);
    assert_eq!(pod.counted(b"a"), Some(10));
    assert_eq!(pod.keys[&b"b"[..]].bucket().remaining(0), 0);
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
            owned.apply(count, now);
            for _ in 0..count {
                if one_by_one.retry_at(now).is_none_or(|retry| retry - now <= most) {
                    one_by_one.charge(now);
                }
            }
            proptest::prop_assert_eq!(owned.bucket, one_by_one.state());
        }
    }

    /// Requests asked about are decided at once as one by one: each admitted while every
    /// one of its keys has room, and charged to all of them.
    #[test]
    fn asks_decided_at_once_are_decided_as_one_by_one(
        asks in proptest::collection::vec((0u64..50, 0u64..30, 1usize..8), 1..20),
    ) {
        const KEYS: [&[u8]; 3] = [b"a", b"b", b"c"];
        let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
        let mut one_by_one: Vec<TokenBucket> = KEYS.iter().map(|_| contract().bucket()).collect();
        let mut now = 0;
        for (id, (gap, count, which)) in (1..).zip(asks) {
            now += gap * MS;
            let under: Vec<usize> = (0..KEYS.len()).filter(|at| which & (1 << at) != 0).collect();
            let keys: Vec<&[u8]> = under.iter().map(|&at| KEYS[at]).collect();
            let mut admitted = 0;
            while admitted < count && under.iter().all(|&at| one_by_one[at].admits(now)) {
                for &at in &under {
                    one_by_one[at].charge(now);
                }
                admitted += 1;
            }
            let answers = pod.on_batch(ask(1, id, count, &keys), now);
            proptest::prop_assert_eq!(answers.decided, vec![(id, admitted)]);
            for &at in &under {
                proptest::prop_assert_eq!(pod.keys[KEYS[at]].bucket, one_by_one[at].state());
            }
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
    assert_eq!(net.copy(0, b"k").unacknowledged[0].1, 4, "still held");
    net.planes[0].take_outgoing();
    net.planes[0].on_cycle(100 * MS + 1);
    assert_eq!(net.copy(0, b"k").unacknowledged[0].1, 0, "emptied");
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

/// Two data planes holding no copy each get six requests at once: they wait, the owner
/// decides them against its one bucket of ten, and ten are admitted in all.
#[test]
fn requests_for_a_cold_key_wait_and_the_owner_decides_them() {
    let mut net = Net::new(2);
    let waiting = (0..2)
        .flat_map(|plane| std::iter::repeat_n(plane, 6))
        .filter(|&plane| waits(net.planes[plane].decide(b"k", contract(), 0)))
        .count();
    assert_eq!(waiting, 12, "all wait");
    net.settle(0);
    let decided: Vec<(Ticket, bool)> = net
        .planes
        .iter_mut()
        .flat_map(DataPlane::take_decided)
        .collect();
    assert_eq!(decided.len(), 12, "every request decided");
    assert_eq!(decided.iter().filter(|(_, a)| *a).count(), 10);
    assert_eq!(net.pods[Net::owner(b"k")].counted(b"k"), Some(10));
    assert!(
        waits(net.planes[0].decide(b"k", contract(), 0)),
        "answered, but not yet bounded by a cycle: still asked about"
    );
    net.settle(0);
    net.planes[0].on_cycle(0);
    assert!(
        !waits(net.planes[0].decide(b"k", contract(), 0)),
        "answered and bounded: decided here"
    );
}

/// Requests coming faster than a round trip: the first asks, those during it are asked
/// about once more, and once a cycle has bounded the copy requests are decided here.
#[test]
fn a_busy_data_plane_stops_asking_after_the_cold_burst() {
    let mut net = Net::new(1);
    assert!(waits(net.planes[0].decide(b"k", contract(), 0)));
    net.collect();
    assert!(waits(net.planes[0].decide(b"k", contract(), MS)));
    net.hop(2 * MS);
    assert_eq!(net.planes[0].take_decided().len(), 1, "the first answered");
    assert!(
        waits(net.planes[0].decide(b"k", contract(), 3 * MS)),
        "the second is asked about"
    );
    net.planes[0].on_cycle(3 * MS);
    net.hop(4 * MS);
    assert_eq!(
        net.planes[0].take_decided().len(),
        2,
        "the second answered, the third decided here"
    );
    assert!(
        !waits(net.planes[0].decide(b"k", contract(), 5 * MS)),
        "and the next at once"
    );
}

#[test]
fn a_copy_goes_cold_a_refill_time_after_the_last_answer() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    assert!(!waits(net.planes[0].decide(b"k", contract(), 100 * MS)));
    assert!(waits(net.planes[0].decide(b"k", contract(), 100 * MS + 1)));
}

/// An ask unanswered for four cycles: the requests waiting are decided from the copy, and
/// so is every request until the owner is heard from again. Its answer, late, decides
/// nothing.
#[test]
fn an_unanswered_ask_is_given_up_after_four_cycles() {
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    net.planes[0].decide(b"k", contract(), 0);
    let lost = net.planes[0].take_outgoing();
    net.planes[0].on_cycle(3 * CYCLE);
    net.planes[0].take_outgoing();
    assert!(net.planes[0].take_decided().is_empty(), "still waiting");
    net.planes[0].on_cycle(4 * CYCLE);
    assert_eq!(net.planes[0].take_decided(), vec![(0, true), (1, true)]);
    assert!(net.copy(0, b"k").unreachable);
    assert_eq!(
        net.planes[0].decide(b"k", contract(), 4 * CYCLE),
        Decision::Admit
    );
    net.to_pods = lost;
    net.settle(4 * CYCLE);
    assert!(
        net.planes[0].take_decided().is_empty(),
        "the late answer decides nothing"
    );
    assert!(!net.copy(0, b"k").unreachable, "heard from again");
}

/// An ask whose answer is lost is sent again once, half way to giving it up, and the owner
/// answers it alike, deciding nothing twice.
#[test]
fn an_ask_is_sent_again_once_and_decided_once() {
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    let owner = Net::owner(b"k");
    for (_, batch) in net.planes[0].take_outgoing() {
        net.pods[owner].on_batch(batch, 0); // its answer lost
    }
    assert_eq!(net.pods[owner].counted(b"k"), Some(1));
    net.planes[0].on_cycle(CYCLE);
    assert_eq!(asks(&net.planes[0].take_outgoing()), 0, "not yet");
    net.planes[0].on_cycle(2 * CYCLE);
    let again = net.planes[0].take_outgoing();
    assert_eq!(asks(&again), 1, "sent again");
    net.to_pods = again;
    net.settle(2 * CYCLE);
    assert_eq!(net.pods[owner].counted(b"k"), Some(1), "decided once");
    assert_eq!(net.planes[0].take_decided(), vec![(0, true)]);
    net.planes[0].on_cycle(3 * CYCLE);
    assert_eq!(asks(&net.planes[0].take_outgoing()), 0, "only once");
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
                owned: true,
            }],
            decided: Vec::new(),
            view: None,
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
    assert!(
        waits(net.planes[0].decide(b"k", bigger, 0)),
        "a new key, so cold"
    );
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
fn a_data_plane_going_away_gives_up_the_requests_waiting() {
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    net.planes[0].decide(b"k", contract(), 0);
    assert_eq!(net.planes[0].abandon(), vec![0, 1]);
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

/// An ask sent again whose answer is lost too is not sent a third time: the next step is
/// giving it up.
#[test]
fn an_ask_is_sent_again_no_more_than_once() {
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    net.planes[0].take_outgoing();
    net.planes[0].on_cycle(2 * CYCLE);
    assert_eq!(asks(&net.planes[0].take_outgoing()), 1);
    net.planes[0].on_cycle(3 * CYCLE);
    assert_eq!(asks(&net.planes[0].take_outgoing()), 0);
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
            last_ask: (0, 0),
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

/// A pod not yet taken into the view answers with its view and decides nothing.
#[test]
fn a_pod_not_serving_answers_with_its_view_alone() {
    let (owner, _, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 4));
    pod.set_serving(false);
    let answers = pod.on_batch(ask(1, 1, 3, &[b"k"]), 0);
    assert!(answers.entries.is_empty());
    assert!(answers.decided.is_empty());
    assert_eq!(answers.view, Some(view(&[0, 1, 2], 4)));
    assert_eq!(pod.counted(b"k"), None);
    pod.set_serving(true);
    let answers = pod.on_batch(ask(1, 1, 3, &[b"k"]), 0);
    assert_eq!(answers.decided, vec![(1, 3)]);
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
    // The plane hears of it only in an answer about another key.
    let other = (0..100)
        .map(|k| format!("o{k}").into_bytes())
        .find(|k| roles(k).0 == secondary)
        .unwrap();
    net.planes[0].decide(&other, contract(), CYCLE);
    net.settle(CYCLE);
    assert_eq!(net.planes[0].view, later);
    assert_eq!(net.copy(0, b"k").owner, Some(secondary));
}

/// Cut off from an owner, a data plane asks the pods in turn for their view.
#[test]
fn a_data_plane_cut_off_asks_the_pods_for_their_view() {
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    net.planes[0].take_outgoing();
    net.planes[0].on_cycle(4 * CYCLE);
    assert!(net.copy(0, b"k").unreachable);
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

/// An owner that has stopped answering a key in use, with no ask in flight, cuts the data
/// plane off as surely as an ask given up: it asks the pods for their view.
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

/// A burst arrives over a few milliseconds, not at once: the first ask is answered while
/// the rest is still coming. Until a cycle has shared out the copy among the data planes
/// admitting the key, every request goes to the owner: two data planes each holding the
/// copy the first answer brought would each spend it.
#[test]
fn a_burst_arriving_after_the_first_answer_is_held_to_one_bucket() {
    let mut net = Net::new(2);
    let mut admitted = 0;
    let mut now = 0;
    // One request on each, answered before the next arrives; then the rest, one at a time,
    // each answered before the next. A cycle runs at once, before any answer, as a new
    // data plane's first does: it bounds nothing.
    for plane in &mut net.planes {
        plane.decide(b"k", contract(), now);
        plane.on_cycle(now);
    }
    net.settle(now);
    admitted += net
        .planes
        .iter_mut()
        .flat_map(DataPlane::take_decided)
        .filter(|(_, a)| *a)
        .count();
    for _ in 0..8 {
        for plane in 0..2 {
            if net.planes[plane].decide(b"k", contract(), now) == Decision::Admit {
                admitted += 1;
            }
            net.settle(now);
        }
        now += MS / 2;
    }
    let waited: usize = net
        .planes
        .iter_mut()
        .flat_map(DataPlane::take_decided)
        .filter(|(_, admitted)| *admitted)
        .count();
    assert_eq!(admitted + waited, 10, "one bucket, not two");
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

/// An ask's timeout can be set longer than four cycles: until it passes, what waits waits.
#[test]
fn an_ask_waits_as_long_as_its_timeout_says() {
    let mut net = Net::new(1);
    net.planes[0].set_ask_timeout(30 * CYCLE);
    net.planes[0].decide(b"k", contract(), 0);
    net.planes[0].take_outgoing(); // lost
    net.planes[0].on_cycle(29 * CYCLE);
    assert!(net.planes[0].take_decided().is_empty(), "still waiting");
    net.planes[0].on_cycle(30 * CYCLE);
    assert_eq!(
        net.planes[0].take_decided(),
        vec![(0, true)],
        "given up, decided here"
    );
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

/// A request with one cold limit: the warm one is decided here and held, and the cold
/// one's owner is asked about it alone; admitted, the held hit goes to its own owner.
#[test]
fn a_warm_limit_is_held_while_the_cold_ones_owner_decides() {
    let (a, b) = apart();
    let mut net = Net::new(1);
    warmed(&mut net, 0, &a, 0);
    let before = net.copy(0, &a).bucket.remaining(0);
    assert!(waits(net.planes[0].decide_all(&under(&[&a, &b]), 0)));
    assert_eq!(net.copy(0, &a).bucket.remaining(0), before - 1, "held");
    let sent = net.planes[0].take_outgoing();
    assert_eq!(asks(&sent), 1);
    let (pod, batch) = sent.iter().find(|(_, b)| !b.asks.is_empty()).unwrap();
    assert_eq!(*pod as usize, Net::owner(&b));
    assert_eq!(batch.asks[0].keys.len(), 1, "the cold key alone");
    net.to_pods.extend(sent);
    net.settle(0);
    assert_eq!(net.planes[0].take_decided(), vec![(1, true)]);
    assert_eq!(net.pods[Net::owner(&b)].counted(&b), Some(1));
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    assert_eq!(
        net.pods[Net::owner(&a)].counted(&a),
        Some(2),
        "the first request's ask, and the held hit"
    );
    assert_eq!(
        net.pods[Net::owner(&b)].counted(&b),
        Some(1),
        "counted by its owner when it decided: not sent again"
    );
}

/// The cold limit's owner refusing gives the held charge back: nothing is sent for it.
#[test]
fn a_refusal_from_the_cold_ones_owner_gives_the_held_charge_back() {
    let (a, b) = apart();
    let mut net = Net::new(2);
    warmed(&mut net, 0, &a, 0);
    for _ in 0..10 {
        net.planes[1].decide(&b, contract(), 0);
    }
    net.settle(0);
    assert_eq!(net.pods[Net::owner(&b)].counted(&b), Some(10), "b spent");
    let before = net.copy(0, &a).bucket.remaining(0);
    assert!(waits(net.planes[0].decide_all(&under(&[&a, &b]), 0)));
    net.settle(0);
    assert_eq!(net.planes[0].take_decided(), vec![(1, false)]);
    assert_eq!(net.copy(0, &a).bucket.remaining(0), before, "given back");
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    assert_eq!(
        net.pods[Net::owner(&a)].counted(&a),
        Some(1),
        "nothing sent"
    );
}

/// Cold limits are asked about together, of the owner of the first in their order; it
/// decides its own key and charges its copy of the other, whose hit the data plane then
/// sends that key's owner.
#[test]
fn cold_limits_are_asked_together_of_the_first_ones_owner() {
    let (a, b) = apart();
    let mut net = Net::new(2);
    for (plane, keys) in [(0, [&a[..], &b[..]]), (1, [&b[..], &a[..]])] {
        assert!(waits(net.planes[plane].decide_all(&under(&keys), 0)));
        let sent = net.planes[plane].take_outgoing();
        assert_eq!(sent.len(), 1, "one pod asked");
        assert_eq!(sent[0].0 as usize, Net::owner(keys[0]), "the first's owner");
        assert_eq!(sent[0].1.asks[0].keys.len(), 2);
        if plane == 0 {
            net.to_pods.extend(sent);
        }
    }
    net.settle(0);
    assert_eq!(net.planes[0].take_decided(), vec![(0, true)]);
    assert_eq!(net.pods[Net::owner(&a)].counted(&a), Some(1));
    assert_eq!(
        net.pods[Net::owner(&b)].counted(&b),
        None,
        "decided on a's owner's copy"
    );
    net.planes[0].on_cycle(CYCLE);
    net.settle(CYCLE);
    assert_eq!(
        net.pods[Net::owner(&b)].counted(&b),
        Some(1),
        "sent its owner"
    );
    assert_eq!(net.pods[Net::owner(&a)].counted(&a), Some(1), "once");
}

/// A pod decides an ask's requests each under all its keys or none, charging its copies,
/// so that the asks it decides after see the requests it admitted.
#[test]
fn a_pod_decides_an_ask_whole_and_its_copies_see_each_ask() {
    let (a, b) = apart();
    let mut pod: ServicePod = ServicePod::new(roles(&a).0, CYCLE, view(&[0, 1, 2], 1));
    let answers = pod.on_batch(ask(1, 1, 6, &[&a, &b]), 0);
    assert_eq!(answers.decided, vec![(1, 6)]);
    let owned: Vec<bool> = answers.entries.iter().map(|answer| answer.owned).collect();
    assert_eq!(owned, vec![true, false]);
    let answers = pod.on_batch(ask(2, 1, 6, &[&b]), 0);
    assert_eq!(answers.decided, vec![(1, 4)], "b's copy holds the six");
    let answers = pod.on_batch(ask(3, 1, 3, &[&a, &b]), 0);
    assert_eq!(answers.decided, vec![(1, 0)], "b's copy empty");
    assert_eq!(
        pod.keys[&a[..]].bucket().remaining(0),
        4,
        "a charged six only"
    );
    assert_eq!(pod.counted(&a), Some(6));
}

/// An ask given up: what waits is decided here under every limit, the held ones admitted
/// with the rest.
#[test]
fn an_ask_given_up_decides_its_requests_here_under_every_limit() {
    let (a, b) = apart();
    let mut net = Net::new(1);
    warmed(&mut net, 0, &a, 0);
    assert!(waits(net.planes[0].decide_all(&under(&[&a, &b]), 0)));
    net.planes[0].take_outgoing(); // lost
    net.planes[0].on_cycle(4 * CYCLE);
    assert_eq!(net.planes[0].take_decided(), vec![(1, true)]);
    assert_eq!(net.copy(0, &a).held, 0);
    let sent: Vec<(Vec<u8>, Report)> = net.planes[0]
        .take_outgoing()
        .into_iter()
        .flat_map(|(_, batch)| batch.entries)
        .map(|entry| (entry.key.to_vec(), entry.report))
        .collect();
    assert!(
        sent.contains(&(a.clone(), hits(1, 1, 0))) && sent.contains(&(b.clone(), hits(1, 1, 0))),
        "both sent their owners in the same cycle: {sent:?}"
    );
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

/// A pod answering an ask from its copy of a key it does not own may hold a copy fresher
/// than the owner's own state — after a failover, the new owner took over from an older
/// one — so what it says was applied is not taken: only the owner's word lets a batch go.
#[test]
fn a_copy_answering_an_ask_does_not_say_which_batches_were_applied() {
    let (a, b) = apart();
    let mut net = Net::new(1);
    warmed(&mut net, 0, &b, 0);
    net.admits(0, &b, 2, 0);
    net.planes[0].on_cycle(CYCLE);
    net.planes[0].take_outgoing(); // the batch's answer never comes
    assert_eq!(net.copy(0, &b).unacknowledged.len(), 1);
    let copy = Answer {
        key: b.clone().into_boxed_slice(),
        behind: 0,
        applied: 1,
        others: 0,
        owned: false,
    };
    let not_owner = roles(&a).0;
    assert_ne!(Some(not_owner), roles(&b).0.into());
    net.planes[0].on_answers(
        Answers {
            from: not_owner,
            entries: vec![copy],
            decided: Vec::new(),
            view: None,
        },
        CYCLE,
    );
    assert_eq!(
        net.copy(0, &b).unacknowledged.len(),
        1,
        "still waiting for the owner to say it applied it"
    );
    assert_eq!(net.copy(0, &b).seen, 0);
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
    net.settle(0);
    net.planes[0].take_decided();
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
        net.settle(0);
        net.planes[plane].take_decided();
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

/// A pod that decided an ask but that the data plane no longer takes for the key's owner
/// refuses it: the copy does not take the pod's state, and still shows room, yet the
/// decision names the key that refused, with its wait, as the pod held it.
#[test]
fn a_refusal_names_its_key_as_the_deciding_pod_held_it() {
    let mut net = Net::new(2);
    // Another data plane spends the whole bucket through the owner.
    for _ in 0..12 {
        net.planes[1].decide(b"k", contract(), 0);
    }
    net.settle(0);
    net.planes[1].take_decided();
    assert_eq!(net.pods[Net::owner(b"k")].counted(b"k"), Some(10));
    // This one, cold, asks the owner; while the answer is on its way, a view comes that
    // leaves the owner out.
    let ticket = match net.planes[0].decide(b"k", contract(), 0) {
        Decision::Wait(ticket) => ticket,
        other => panic!("{other:?}"),
    };
    let owner = Net::owner(b"k");
    let (pod, batch) = net.planes[0]
        .take_outgoing()
        .into_iter()
        .find(|(pod, batch)| *pod as usize == owner && !batch.asks.is_empty())
        .unwrap();
    let answers = net.pods[pod as usize].on_batch(batch, 0);
    net.planes[0].on_view(without(pod), 0);
    net.planes[0].on_answers(answers, 0);
    assert!(net.copy(0, b"k").bucket.admits(0), "the copy took nothing");
    let decided = net.planes[0].take_decisions();
    assert_eq!(
        decided,
        [Decided {
            ticket,
            admitted: false,
            // A hundred a second: one back in 10 ms.
            refused_by: [(0, 10 * MS)].into(),
        }]
    );
}

/// Requests whose ask goes unanswered are decided here, from the copy, and those refused
/// name the key that refused them, as the copy has it.
#[test]
fn a_refusal_decided_here_names_its_key_as_the_copy_has_it() {
    let mut net = Net::new(1);
    for _ in 0..12 {
        net.planes[0].decide(b"k", contract(), 0);
    }
    net.planes[0].take_outgoing();
    net.planes[0].on_cycle(3 * CYCLE);
    net.planes[0].take_outgoing();
    net.planes[0].on_cycle(4 * CYCLE);
    let decided = net.planes[0].take_decisions();
    assert_eq!(decided.len(), 12);
    let refused: Vec<&Decided> = decided.iter().filter(|d| !d.admitted).collect();
    assert!(!refused.is_empty(), "{decided:?}");
    for decided in refused {
        // A hundred a second: one back in 10 ms.
        assert_eq!(&*decided.refused_by, [(0, 10 * MS)], "{decided:?}");
    }
    assert!(
        decided
            .iter()
            .filter(|d| d.admitted)
            .all(|d| d.refused_by.is_empty())
    );
}

/// Keys `k0`… `k{n-1}`, each decided once by `plane`, its ask answered and the cycle's
/// batch acknowledged, the `n`th a millisecond after the one before.
fn used_in_turn(net: &mut Net, plane: usize, n: u64) -> Vec<Vec<u8>> {
    (0..n)
        .map(|at| {
            let key = format!("k{at}").into_bytes();
            let now = at * MS;
            net.planes[plane].decide(&key, contract(), now);
            net.settle(now);
            net.planes[plane].take_decided();
            net.planes[plane].on_cycle(now);
            net.settle(now);
            key
        })
        .collect()
}

/// Past its capacity, a data plane evicts the keys it used least recently down to nine
/// tenths of it, never one with hits still to be acknowledged.
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
    // The newest has a hit not yet acknowledged: room for less than one, and it stays.
    net.planes[0].decide(&keys[4], contract(), 6 * MS);
    net.planes[0].set_capacity(each / 2);
    net.planes[0].on_cycle(6 * MS);
    assert_eq!(kept(&net.planes[0]), [false, false, false, false, true]);
    assert_eq!(net.planes[0].evicted(), 4);
    // Acknowledged and quiet past twice its refill time, it is forgotten, and gives its
    // bytes back.
    net.settle(6 * MS);
    net.planes[0].on_cycle(10_000 * MS);
    assert_eq!(kept(&net.planes[0]), [false; 5]);
    assert_eq!(net.planes[0].holding(), 0);
}

/// Past its capacity, a service pod evicts the keys that changed least recently down to
/// nine tenths of it.
#[test]
fn a_service_pod_past_its_capacity_evicts_the_least_recently_changed() {
    // The only pod: it owns every key.
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let keys: Vec<Vec<u8>> = (0..5_u64).map(|at| format!("k{at}").into_bytes()).collect();
    for (at, key) in (0_u64..).zip(&keys) {
        pod.on_batch(ask(100, at, 1, &[key]), at * MS);
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

/// A service pod's key takes 112 bytes in its table, which holds up to twice the room its
/// keys take, and its record of a data plane 56: its bucket kept as the bucket's state, its
/// terms the key's contract, and batches waiting, which rarely do, kept apart.
#[test]
fn a_service_pods_key_is_small() {
    assert_eq!(size_of::<(Box<[u8]>, Owned)>(), 112);
    assert_eq!(size_of::<(PlaneId, Record)>(), 56);
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
