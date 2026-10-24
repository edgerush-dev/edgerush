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
        asked: 0,
        refused: 0,
        seen,
        age: 0,
    }
}

/// Hits that are all requests an ask decided.
fn asked_hits(sequence: u64, count: u64, seen: u64) -> Report {
    Report::Hits {
        sequence,
        count,
        asked: count,
        refused: 0,
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
        asks: Vec::new(),
        oldest_ask: 0,
        made: 0,
    }
}

/// An ask from `from` about `count` requests under `keys`, the oldest its data plane has
/// in flight.
fn ask(from: PlaneId, id: u64, count: u64, keys: &[&[u8]]) -> Batch {
    Batch {
        from,
        entries: Vec::new(),
        asks: vec![Ask {
            id,
            count,
            age: 0,
            span: 0,
            timeout: 4 * CYCLE,
            keys: keys.iter().map(|key| ((*key).into(), contract())).collect(),
        }],
        oldest_ask: id,
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

/// A data plane asks about its first request and hears back, and a cycle bounds the copy:
/// warm, it decides at once. What the cycle sends — the request's hit, settling what the
/// owner held for it — is delivered and answered.
fn warmed(net: &mut Net, plane: usize, key: &[u8], now: u64) {
    net.planes[plane].decide(key, contract(), now);
    net.settle(now);
    net.planes[plane].take_decided();
    net.planes[plane].on_cycle(now);
    net.settle(now);
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
            owned.apply(count, now, now);
            for _ in 0..count {
                if one_by_one.retry_at(now).is_none_or(|retry| retry - now <= most) {
                    one_by_one.charge(now);
                }
            }
            proptest::prop_assert_eq!(owned.bucket(), one_by_one.clone());
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
                proptest::prop_assert_eq!(pod.keys[KEYS[at]].bucket(), one_by_one[at].clone());
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

/// A batch emptied unseen still says what it did of requests refused: that charges nothing,
/// and frees what the owner holds for them.
#[test]
fn an_emptied_batch_still_says_what_was_refused() {
    let mut unseen = Unseen {
        sequence: 1,
        count: 3,
        asked: 2,
        refused: 1,
        first: 0,
        last: 0,
        admitted: 0,
    };
    let said = |unseen: &Unseen| unseen.report(0, 0);
    unseen.empty_if_stale(100 * MS, 100 * MS);
    assert_eq!(
        said(&unseen),
        Report::Hits {
            sequence: 1,
            count: 3,
            asked: 2,
            refused: 1,
            seen: 0,
            age: 0,
        },
        "not yet"
    );
    unseen.empty_if_stale(100 * MS, 100 * MS + 1);
    assert_eq!(
        said(&unseen),
        Report::Hits {
            sequence: 1,
            count: 0,
            asked: 0,
            refused: 1,
            seen: 0,
            age: 0,
        }
    );
}

/// A key with refused requests of an ask given up still to report is neither evicted for
/// room nor idle: its owner holds them, and counts them, until it hears.
#[test]
fn a_key_with_refusals_to_report_is_kept() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    // Its owner gone, nothing is sent.
    net.set_view(&view(&[], 2), 0);
    net.planes[0]
        .keys
        .get_mut(&b"k"[..])
        .unwrap()
        .unsent_refused = 1;
    net.planes[0].set_capacity(1);
    net.planes[0].on_cycle(CYCLE);
    assert!(net.planes[0].keys.contains_key(&b"k"[..]), "not evicted");
    net.planes[0].set_capacity(u64::MAX);
    net.planes[0].on_cycle(1_000 * MS);
    assert!(net.planes[0].keys.contains_key(&b"k"[..]), "not idle");
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

/// An ask its owner decided, whose answer is lost and whose sending again is lost too, is
/// given up and its request decided here: the hit it is sent as settles what the owner held
/// for it, and the request is counted once.
#[test]
fn an_ask_given_up_after_its_owner_decided_it_is_counted_once() {
    let mut net = Net::new(1);
    let owner = Net::owner(b"k");
    net.planes[0].decide(b"k", contract(), 0);
    for (_, batch) in net.planes[0].take_outgoing() {
        net.pods[owner].on_batch(batch, 0); // its answer lost
    }
    net.planes[0].on_cycle(2 * CYCLE);
    net.planes[0].take_outgoing(); // sent again, lost
    net.planes[0].on_cycle(4 * CYCLE);
    assert_eq!(
        net.planes[0].take_decided(),
        vec![(0, true)],
        "given up, decided here"
    );
    net.settle(4 * CYCLE);
    assert_eq!(net.pods[owner].counted(b"k"), Some(1));
}

/// An ask whose first sending arrives after a later ask was sent is not decided again: the
/// later one said the data plane was done with it.
#[test]
fn an_ask_arriving_after_a_later_one_is_not_decided_again() {
    let mut net = Net::new(1);
    let owner = Net::owner(b"k");
    net.planes[0].decide(b"k", contract(), 0);
    let late = net.planes[0].take_outgoing(); // held up on the way
    net.planes[0].on_cycle(2 * CYCLE);
    net.settle(2 * CYCLE); // sent again, and answered
    assert_eq!(net.planes[0].take_decided(), vec![(0, true)]);
    net.planes[0].on_cycle(3 * CYCLE);
    net.settle(3 * CYCLE);
    // Quiet for two refill times: forgotten here, made again, and asked about anew.
    net.planes[0].on_cycle(300 * MS);
    assert!(net.planes[0].keys.is_empty());
    net.planes[0].decide(b"k", contract(), 300 * MS);
    net.settle(300 * MS);
    assert_eq!(net.planes[0].take_decided(), vec![(1, true)]);
    net.planes[0].on_cycle(310 * MS);
    net.settle(310 * MS);
    assert_eq!(net.pods[owner].counted(b"k"), Some(2));
    // The first sending arrives, an ask long over.
    net.to_pods = late;
    net.settle(310 * MS);
    assert_eq!(net.pods[owner].counted(b"k"), Some(2), "not decided again");
    assert!(net.planes[0].take_decided().is_empty());
    assert_eq!(net.admits(0, b"k", 1, 320 * MS), 1);
    net.planes[0].on_cycle(320 * MS);
    net.settle(320 * MS);
    assert_eq!(
        net.pods[owner].counted(b"k"),
        Some(3),
        "three admitted, each counted once"
    );
}

/// An ask says how long its data plane waits for the answer, and every batch the oldest of
/// its asks still in flight: one sent while another is out leaves that one open, and once
/// both are given up, the next batch says both are over.
#[test]
fn a_batch_says_which_asks_are_over_and_an_ask_how_long_it_waits() {
    let mut net = Net::new(1);
    net.planes[0].set_ask_timeout(300 * MS);
    let sent = |net: &mut Net| -> (Vec<(u64, u64)>, Vec<u64>) {
        let batches = net.planes[0].take_outgoing();
        let marks = batches.iter().map(|(_, batch)| batch.oldest_ask).collect();
        let asks = batches
            .into_iter()
            .flat_map(|(_, batch)| batch.asks)
            .map(|ask| (ask.id, ask.timeout))
            .collect();
        (asks, marks)
    };
    net.planes[0].decide(b"k", contract(), 0);
    assert_eq!(sent(&mut net), (vec![(1, 300 * MS)], vec![1]));
    net.planes[0].decide(b"j", contract(), 0);
    assert_eq!(sent(&mut net), (vec![(2, 300 * MS)], vec![1]));
    net.planes[0].on_cycle(300 * MS);
    let (_, marks) = sent(&mut net);
    assert!(
        !marks.is_empty() && marks.iter().all(|mark| *mark == 3),
        "{marks:?}"
    );
}

/// A batch says which of its data plane's asks are over: one before its mark is not decided
/// again however late it comes — queued behind an exchange, even in that very batch, after
/// the hit it was given up and decided as. A pod forgets what it knows of a data plane's
/// asks a minute after it last heard from it.
#[test]
fn an_ask_over_is_not_decided_and_is_forgotten_a_minute_on() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let mut late = ask(100, 1, 1, &[b"k"]);
    late.oldest_ask = 2;
    late.entries = vec![Entry {
        key: b"k"[..].into(),
        contract: contract(),
        report: asked_hits(1, 1, 0),
    }];
    assert!(pod.on_batch(late, 0).decided.is_empty(), "over");
    assert_eq!(pod.counted(b"k"), Some(1), "once");
    // Decided, then over: what was decided of it is let go. A batch that went the long
    // way round, its mark older, moves nothing back.
    assert_eq!(pod.on_batch(ask(100, 2, 1, &[b"k"]), 0).decided, [(2, 1)]);
    let mut on = batch(100, Report::Poll);
    on.oldest_ask = 3;
    pod.on_batch(on, 0);
    assert!(pod.asks[&100].decided.is_empty());
    let mut behind = ask(100, 2, 1, &[b"k"]);
    behind.oldest_ask = 1;
    assert!(pod.on_batch(behind, 0).decided.is_empty(), "still over");
    assert_eq!(pod.counted(b"k"), Some(2));
    // Heard from half way through the minute: kept a minute from then.
    let kept = ASKS_KEPT_CYCLES * CYCLE;
    pod.on_batch(batch(100, Report::Poll), kept / 2);
    pod.on_timer(kept + 1);
    assert!(pod.asks.contains_key(&100));
    pod.on_timer(kept / 2 + kept + 1);
    assert!(pod.asks.is_empty());
}

/// A later ask holds what is held for its data plane as long as its own: the hits of both
/// may come together.
#[test]
fn a_later_ask_holds_what_is_held_as_long_as_its_own() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    pod.set_least_hold(0);
    // Refilling in 100 ms, asks waiting 40: each held 140 ms.
    pod.on_batch(ask(100, 1, 1, &[b"k"]), 0);
    pod.on_batch(ask(100, 2, 1, &[b"k"]), 100 * MS);
    pod.on_batch(batch(100, asked_hits(1, 2, 0)), 200 * MS);
    assert_eq!(pod.counted(b"k"), Some(2));
}

/// An asked request's hit whose batch went unseen for a refill time comes emptied: what is
/// held for it is left to lapse, and an admission after it, its batch waiting behind that
/// one, is charged and counted rather than settling it.
#[test]
fn an_admission_after_an_asked_hit_emptied_is_counted() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    assert_eq!(pod.on_batch(ask(100, 1, 1, &[b"k"]), 0).decided, [(1, 1)]);
    // The asked request's batch lost; one admitted after it reported, kept waiting. Charged
    // as of when it was admitted, under an interval before the check below, where it shows.
    pod.on_batch(batch(100, hits(2, 1, 0)), 105 * MS);
    pod.on_batch(batch(100, asked_hits(1, 0, 0)), 110 * MS);
    assert_eq!(pod.counted(b"k"), Some(2), "two admitted");
    assert_eq!(pod.keys[&b"k"[..]].bucket().remaining(110 * MS), 9);
}

/// An ask sent after another is over says so: a copy of the first arriving late, behind it,
/// is not decided again, and each request is counted once.
#[test]
fn a_late_copy_of_an_ask_over_is_not_decided_again() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    assert_eq!(pod.on_batch(ask(100, 1, 1, &[b"k"]), 0).decided, [(1, 1)]);
    assert_eq!(
        pod.on_batch(ask(100, 2, 1, &[b"k"]), CYCLE).decided,
        [(2, 1)]
    );
    assert!(
        pod.on_batch(ask(100, 1, 1, &[b"k"]), 2 * CYCLE)
            .decided
            .is_empty(),
        "over"
    );
    pod.on_batch(batch(100, asked_hits(1, 2, 0)), 3 * CYCLE);
    assert_eq!(pod.counted(b"k"), Some(2), "two admitted");
}

/// The hits of an ask its data plane gave up come as long after it was decided as the data
/// plane waited, and a refill time more as it sends them: they still find its hold.
#[test]
fn the_hits_of_an_ask_given_up_find_its_hold() {
    let n = |n| NonZeroU64::new(n).unwrap();
    // Refilling in a second, the data plane waiting 300 ms: held for 1.3 s.
    let slow = Contract {
        limit: n(10),
        window: n(1_000 * MS),
        burst: n(10),
    };
    let ask = |id| Batch {
        from: 100,
        oldest_ask: id,
        entries: Vec::new(),
        asks: vec![Ask {
            id,
            count: 1,
            age: 0,
            span: 0,
            timeout: 300 * MS,
            keys: vec![(b"k"[..].into(), slow)],
        }],
        made: 0,
    };
    let hits = |sequence| Batch {
        from: 100,
        oldest_ask: 0,
        entries: vec![Entry {
            key: b"k"[..].into(),
            contract: slow,
            report: asked_hits(sequence, 1, sequence - 1),
        }],
        asks: Vec::new(),
        made: 0,
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    pod.set_least_hold(0);
    pod.on_batch(ask(1), 0);
    pod.on_batch(hits(1), 1_299 * MS);
    assert_eq!(pod.counted(b"k"), Some(1), "settled");
    pod.on_batch(ask(2), 1_299 * MS);
    pod.on_batch(hits(2), 2_599 * MS);
    assert_eq!(pod.counted(b"k"), Some(3), "lapsed, charged");
}

/// An ask its owner decided, given up by its data plane, which then refused its request
/// itself: told so, the owner drops it from what it holds and counts, and leaves its bucket
/// charged. Told of one it holds nothing for, it changes nothing.
#[test]
fn a_request_of_an_ask_given_up_and_refused_is_not_counted() {
    let refused = |sequence| Report::Hits {
        sequence,
        count: 0,
        asked: 0,
        refused: 1,
        seen: sequence - 1,
        age: 0,
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    assert_eq!(pod.on_batch(ask(100, 1, 1, &[b"k"]), 0).decided, [(1, 1)]);
    pod.on_batch(batch(100, refused(1)), 0);
    assert_eq!(pod.counted(b"k"), Some(0), "none admitted");
    assert_eq!(
        pod.keys[&b"k"[..]].bucket().remaining(0),
        9,
        "still charged"
    );
    pod.on_batch(batch(100, refused(2)), 0);
    assert_eq!(pod.counted(b"k"), Some(0));
}

/// What an ask admits is held for its data plane's asked hits, which settle it rather than
/// being charged again — other hits are charged as ever — for the key's refill time, or the
/// pod's least hold if that is longer, and the time the data plane waits for an answer
/// beside: then it lapses, still charged and counted, and asked hits after it are charged.
#[test]
fn a_hold_lapses_charged_once_its_time_is_over() {
    let n = |n| NonZeroU64::new(n).unwrap();
    // Counted after each step, for a key under `contract`, the pod holding at least
    // `least_hold`, the data plane waiting four cycles for an answer.
    let steps = |contract: Contract, least_hold: u64| -> Vec<u64> {
        let hits = |sequence, count, asked| Batch {
            from: 100,
            oldest_ask: 0,
            entries: vec![Entry {
                key: b"k"[..].into(),
                contract,
                report: Report::Hits {
                    sequence,
                    count,
                    asked,
                    refused: 0,
                    seen: sequence - 1,
                    age: 0,
                },
            }],
            asks: Vec::new(),
            made: 0,
        };
        let ask = |id, count| Batch {
            from: 100,
            oldest_ask: id,
            entries: Vec::new(),
            asks: vec![Ask {
                id,
                count,
                age: 0,
                span: 0,
                timeout: 4 * CYCLE,
                keys: vec![(b"k"[..].into(), contract)],
            }],
            made: 0,
        };
        let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
        pod.set_least_hold(least_hold);
        let mut counted = Vec::new();
        let mut step = |pod: &ServicePod| counted.push(pod.counted(b"k").unwrap_or(0));
        // Held for 90 ms: 50 and the 40 the data plane waits.
        assert_eq!(pod.on_batch(ask(1, 3), 0).decided, [(1, 3)]);
        step(&pod);
        pod.on_batch(hits(1, 1, 1), 4 * CYCLE);
        step(&pod);
        // A hit no ask decided: charged, the hold left as it is.
        pod.on_batch(hits(2, 1, 0), 5 * CYCLE);
        step(&pod);
        pod.on_batch(hits(3, 1, 1), 8 * CYCLE);
        step(&pod);
        // The hold's time over: what is left of it lapses, still charged.
        pod.on_batch(hits(4, 1, 1), 9 * CYCLE);
        step(&pod);
        assert_eq!(pod.on_batch(ask(2, 2), 9 * CYCLE).decided, [(2, 2)]);
        step(&pod);
        pod.on_batch(hits(5, 2, 2), 10 * CYCLE);
        step(&pod);
        counted
    };
    let want = [3, 3, 4, 4, 5, 7, 7];
    // Refilling in 50 ms, longer than the least hold: held as long as that.
    let refilling_in = |time: u64| Contract {
        limit: n(10),
        window: n(time),
        burst: n(10),
    };
    assert_eq!(
        steps(refilling_in(5 * CYCLE), CYCLE),
        want,
        "its refill time"
    );
    // Refilling in 10 ms, shorter than the least hold of 50 ms: held as long as that.
    assert_eq!(
        steps(refilling_in(CYCLE), 5 * CYCLE),
        want,
        "the least hold"
    );
}

/// What the owner holds for a data plane's hits is copied with the key: the secondary
/// taking over settles it with those hits, charging each request once.
#[test]
fn a_hold_is_settled_by_the_secondary_taking_over() {
    let (owner, secondary, _) = roles(b"k");
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    net.settle(0);
    assert_eq!(net.planes[0].take_decided(), vec![(0, true)]);
    let secondary = secondary as usize;
    assert_eq!(net.pods[secondary].counted(b"k"), Some(1), "copied, held");
    net.set_view(&without(owner), 0);
    net.planes[0].on_cycle(0);
    net.settle(0);
    assert_eq!(net.pods[secondary].counted(b"k"), Some(1), "settled");
    let bucket = net.pods[secondary].keys[&b"k"[..]].bucket();
    assert_eq!(bucket.remaining(0), 9, "charged once");
}

/// What the owner holds for an asked request is left out of the bucket it sends the
/// asking data plane, which carries the request itself until it sees its hit applied: its
/// copy has the request once, from the ask's answer and from any answer after it.
#[test]
fn the_asking_data_plane_has_its_asked_request_once() {
    let mut net = Net::new(1);
    net.planes[0].decide(b"k", contract(), 0);
    net.settle(0);
    assert_eq!(net.planes[0].take_decided(), vec![(0, true)]);
    assert_eq!(
        net.copy(0, b"k").bucket.remaining(0),
        9,
        "from the ask's answer"
    );
    net.planes[0].on_cycle(0);
    net.planes[0].take_outgoing(); // the asked request's hit, lost
    assert_eq!(net.admits(0, b"k", 2, 0), 2);
    net.planes[0].on_cycle(0);
    net.settle(0); // the next batch, kept waiting for the lost one, answered
    assert_eq!(
        net.copy(0, b"k").bucket.remaining(0),
        7,
        "from the next batch's answer"
    );
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
                held: 0,
                applied: 0,
                others: 5,
                owned: true,
            }],
            decided: Vec::new(),
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
            held: 0,
            held_for: 0,
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

/// What the new owner holds for asks it decided before the handover reached it stays held,
/// on top of what it is handed: the hits that come after settle it, each request counted
/// and charged once.
#[test]
fn a_handover_arriving_late_keeps_what_the_new_owner_held() {
    let (owner, secondary, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 3));
    assert_eq!(pod.on_batch(ask(9, 1, 2, &[b"k"]), 0).decided, [(1, 2)]);
    let handed = Replica {
        key: b"k"[..].into(),
        contract: contract(),
        behind: contract().bucket().behind(0) + 5 * u128::from(1_000 * MS),
        counted: 5,
        planes: Vec::new(),
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
    pod.on_batch(batch(9, asked_hits(1, 2, 0)), 0);
    assert_eq!(pod.counted(b"k"), Some(7), "settled, not counted again");
    assert_eq!(
        pod.keys[&b"k"[..]].bucket().remaining(0),
        3,
        "ten less five less two"
    );
}

/// A key handed over holds what its data plane's asks admitted as long as the longer of
/// the two holds: the old owner's, copied with it, and the new owner's own.
#[test]
fn a_handover_holds_for_the_longer_of_the_two() {
    let (owner, secondary, _) = roles(b"k");
    let mut pod: ServicePod = ServicePod::new(owner, CYCLE, view(&[0, 1, 2], 3));
    pod.set_least_hold(0);
    // Held 140 ms here; 250 more by the old owner as it hands the key over.
    assert_eq!(pod.on_batch(ask(9, 2, 1, &[b"k"]), 0).decided, [(2, 1)]);
    let handed = Replica {
        key: b"k"[..].into(),
        contract: contract(),
        behind: contract().bucket().behind(0) + u128::from(100 * MS),
        counted: 1,
        planes: vec![PlaneRecord {
            plane: 9,
            applied: 0,
            admitted_ago: Some(0),
            last_ask: (1, 1),
            held: 1,
            held_for: 250 * MS,
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
    assert_eq!(pod.counted(b"k"), Some(2));
    pod.on_batch(batch(9, asked_hits(1, 2, 0)), 200 * MS);
    assert_eq!(pod.counted(b"k"), Some(2), "settled");
}

/// Two asks out on one key at once — two groups of limits sharing it — both answers lost,
/// both sent again: each copy is answered as its ask was, and each request counted once.
#[test]
fn two_asks_out_on_one_key_sent_again_are_each_decided_once() {
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let with_oldest = |id, oldest| {
        let mut batch = ask(100, id, 1, &[b"k"]);
        batch.oldest_ask = oldest;
        batch
    };
    assert_eq!(pod.on_batch(with_oldest(8, 8), 0).decided, [(8, 1)]);
    assert_eq!(pod.on_batch(with_oldest(9, 8), 0).decided, [(9, 1)]);
    assert_eq!(pod.on_batch(with_oldest(8, 8), 2 * CYCLE).decided, [(8, 1)]);
    assert_eq!(pod.on_batch(with_oldest(9, 8), 2 * CYCLE).decided, [(9, 1)]);
    assert_eq!(pod.counted(b"k"), Some(2), "each once");
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
    // Each after the first batch the data plane numbered, and seen applied: the cold
    // limit's a request of the ask, which its owner may have decided and holds for it. Both
    // admitted as the request came, four cycles before.
    let aged = |report| match report {
        Report::Hits {
            sequence,
            count,
            asked,
            refused,
            seen,
            ..
        } => Report::Hits {
            sequence,
            count,
            asked,
            refused,
            seen,
            age: 4 * CYCLE,
        },
        report => report,
    };
    assert!(
        sent.contains(&(a.clone(), aged(hits(2, 1, 1))))
            && sent.contains(&(b.clone(), aged(asked_hits(2, 1, 1)))),
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
    // The first batch, the asked request's hit, the owner said it applied; the second's
    // answer never came.
    assert_eq!(net.copy(0, &b).seen, 1);
    let copy = Answer {
        key: b.clone().into_boxed_slice(),
        behind: 0,
        held: 0,
        applied: 2,
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
            life: 0,
            number: 0,
            made: CYCLE,
        },
        CYCLE,
    );
    assert_eq!(
        net.copy(0, &b).unacknowledged.len(),
        1,
        "still waiting for the owner to say it applied it"
    );
    assert_eq!(net.copy(0, &b).seen, 1);
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

/// Requests whose ask goes unanswered are decided here, from the copy as of when they came,
/// and those refused name the key that refused them, with its wait as the copy has it now:
/// none, as it refilled while they waited.
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
        assert_eq!(&*decided.refused_by, [(0, 0)], "{decided:?}");
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
/// terms the key's contract, batches waiting, which rarely do, kept apart, and the data
/// plane's last admission in eight bytes, room for what is held for it.
#[test]
fn a_service_pods_key_is_small() {
    assert_eq!(size_of::<(Box<[u8]>, Owned)>(), 112);
    assert_eq!(size_of::<(PlaneId, Record)>(), 56);
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
            asked: 0,
            refused: 0,
            admitted: 4 * MS,
            first: 10 * MS,
            last: 10 * MS,
        }
        .report(0, 10 * MS),
    });
    queued.at(25 * MS);
    queued.at(20 * MS);
    let batch = queued.batch(100, 1);
    assert_eq!(batch.made, 25 * MS);
    assert!(matches!(batch.entries[0].report, Report::Hits { age, .. } if age == 21 * MS));
}

/// One data plane and its pod, batches taking `there` to reach the pod and answers `back`
/// to return, a cycle every `CYCLE`, and asks sent as they are made, as the proxy's hub
/// sends them: requests at `times`. Whether each was admitted, `None` if never decided.
fn decided_over_round_trips(
    c: Contract,
    times: &[u64],
    there: u64,
    back: u64,
) -> Vec<Option<bool>> {
    let mut plane: DataPlane = DataPlane::new(100, CYCLE, view(&[0], 1));
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let mut decided = vec![None; times.len()];
    let mut waiting: BTreeMap<Ticket, usize> = BTreeMap::new();
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
            match plane.decide(b"k", c, now) {
                Decision::Wait(ticket) => {
                    waiting.insert(ticket, at);
                }
                decision => decided[at] = Some(decision == Decision::Admit),
            }
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
        for (ticket, admitted) in plane.take_decided() {
            decided[waiting.remove(&ticket).unwrap()] = Some(admitted);
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
    /// bucket refills in two round trips or more: in less, a copy answered full charges
    /// hits that waited from the answer's batch rather than from when they came, and
    /// refuses a client within its rate (issue #54).
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
        let burst = burst.max((2 * (there + back)).div_ceil(interval));
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

/// An owner decides an ask's requests as of when they came, by the ask's ages: two that came
/// an interval apart fit a bucket of one; two at one instant do not; and an ask that does
/// not say when is decided as it arrives.
#[test]
fn an_ask_is_decided_between_its_requests_arrivals() {
    let n = |n| NonZeroU64::new(n).unwrap();
    let interval = 10 * MS;
    let c = Contract {
        limit: n(1),
        window: n(interval),
        burst: n(1),
    };
    let decided = |age, span| {
        let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
        let batch = Batch {
            from: 100,
            entries: Vec::new(),
            asks: vec![Ask {
                id: 1,
                count: 2,
                age,
                span,
                timeout: 4 * CYCLE,
                keys: vec![(b"k"[..].into(), c)],
            }],
            oldest_ask: 1,
            made: 50 * MS,
        };
        pod.on_batch(batch, 100 * MS).decided
    };
    assert_eq!(
        decided(3 * interval, interval),
        [(1, 2)],
        "an interval apart"
    );
    assert_eq!(decided(3 * interval, 0), [(1, 1)], "at one instant");
    assert_eq!(decided(0, 0), [(1, 1)], "as it arrives");
}

/// An ask that, by its ages, came before requests its owner has already charged is decided
/// as if it came no earlier than them: two data planes' batches arrive offset differently —
/// a batch waiting to be sent counts its wait as trip — and decided as of before them, it
/// would find their charges ahead of it, as if out of order, and lose room. A bucket of ten
/// refilling ten an hour admits ten of twelve, in any order.
#[test]
fn an_ask_is_decided_no_earlier_than_what_its_owner_charged() {
    let n = |n| NonZeroU64::new(n).unwrap();
    let c = Contract {
        limit: n(10),
        window: n(3_600_000 * MS),
        burst: n(10),
    };
    let ask = |from, count, age| Batch {
        from,
        entries: Vec::new(),
        asks: vec![Ask {
            id: 1,
            count,
            age,
            span: 0,
            timeout: 4 * CYCLE,
            keys: vec![(b"k"[..].into(), c)],
        }],
        oldest_ask: 1,
        made: 50 * MS,
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    assert_eq!(pod.on_batch(ask(100, 6, 0), 50 * MS).decided, [(1, 6)]);
    assert_eq!(
        pod.on_batch(ask(101, 6, 10 * MS), 51 * MS).decided,
        [(1, 4)],
        "as of 41 ms, before the six charged as of 50"
    );
    // So too after another data plane's hits, charged as of when they were admitted.
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view(&[0], 1));
    let mut hits = batch(102, hits(1, 6, 0));
    hits.entries[0].contract = c;
    hits.made = 50 * MS;
    pod.on_batch(hits, 50 * MS);
    assert_eq!(
        pod.on_batch(ask(101, 6, 10 * MS), 51 * MS).decided,
        [(1, 4)],
        "as of 41 ms, before the six hits admitted at 50"
    );
}

/// An ask's ages count back from when its batch was made, as its hits' do.
#[test]
fn a_batch_ages_its_asks_with_its_hits() {
    let mut queued = Queued::default();
    let arrived = Arrived::and(Some(Arrived::at(4 * MS)), 6 * MS);
    let made = queued.at(10 * MS).made;
    queued
        .asks
        .push(arrived.ask(made, 1, 2, 4 * CYCLE, vec![(b"k"[..].into(), contract())]));
    queued.at(25 * MS);
    let batch = queued.batch(100, 1);
    assert_eq!((batch.asks[0].age, batch.asks[0].span), (21 * MS, 2 * MS));
}

/// An ask's batch carries first what the data plane admitted of its keys and has not yet
/// sent: the owner charges those before it decides the ask, as they came before it.
#[test]
fn an_asks_batch_carries_its_keys_unsent_hits_ahead_of_it() {
    let mut net = Net::new(1);
    warmed(&mut net, 0, b"k", 0);
    assert_eq!(
        net.planes[0].decide(b"k", contract(), 10 * MS),
        Decision::Admit
    );
    // A refill time on, unheard from since, the copy is cold: the next request asks.
    assert!(waits(net.planes[0].decide(b"k", contract(), 150 * MS)));
    let owner = Net::owner(b"k") as PodId;
    let sent = net.planes[0].take_outgoing();
    let (_, batch) = sent.iter().find(|(pod, _)| *pod == owner).unwrap();
    assert_eq!(batch.asks.len(), 1);
    assert!(
        batch.entries.iter().any(|entry| matches!(
            entry.report,
            Report::Hits { count: 1, age, .. } if age == 140 * MS
        )),
        "the hit admitted at 10 ms, sent with the ask: {:?}",
        batch.entries
    );
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
                held: 0,
                applied: seen,
                others: 0,
                owned: true,
            }],
            decided: Vec::new(),
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
                held: 0,
                applied: seen,
                others: 0,
                owned: true,
            }],
            decided: Vec::new(),
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
