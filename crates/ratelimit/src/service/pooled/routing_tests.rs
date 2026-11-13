//! Tests of the participant through its routing: a key sent through another pod while its
//! owner leaves its batches unanswered, the wait ended by the owner's own reply, a relayed
//! reply taken as the owner's, a later routing moving a key at once, and today's pods sent
//! nothing through another.

use super::*;
use crate::service::{FixedState, PodId, Standing};
use std::num::NonZeroU64;

const CYCLE: u64 = 10_000_000;

/// Every key owned by `owner`, gone through `through`, among `pods`; later by `epoch`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fixed {
    epoch: u64,
    owner: Option<u64>,
    through: Option<u64>,
    pods: Vec<u64>,
}

impl Routing for Fixed {
    type Pod = u64;
    type Batch = Batch;

    fn owner(&self, _: &[u8], _: u64) -> Option<u64> {
        self.owner
    }

    fn through(&self, _: u64, _: u64) -> Option<u64> {
        self.through
    }

    fn polled(&self, turn: u64) -> Option<u64> {
        let at = usize::try_from(turn).ok()? % self.pods.len().max(1);
        self.pods.get(at).copied()
    }

    fn later(&self, than: &Self) -> bool {
        self.epoch > than.epoch
    }

    fn names(&self, pod: u64) -> bool {
        self.pods.contains(&pod)
    }

    fn batch(&self, _: u64, batch: Batch, _: impl Fn(&[u8]) -> u64) -> Batch {
        batch
    }
}

fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000_000_000),
        burst: n(10),
    }
}

/// Owned by 1, gone through 2.
fn routed() -> Fixed {
    Fixed {
        epoch: 1,
        owner: Some(1),
        through: Some(2),
        pods: vec![1, 2],
    }
}

fn participant(routing: Fixed) -> Participant<FixedState, Fixed> {
    Participant::new(7, CYCLE, routing, FixedState::default()).unwrap()
}

const KEY: &[u8] = b"k";

/// A hit on the key just before cycle `at`, then the cycle: the pods sent entries, and how
/// many each; the empty batches polling a pod in turn, while the key goes unheard, left out.
fn cycle(participant: &mut Participant<FixedState, Fixed>, at: u64) -> Vec<(u64, usize)> {
    let decided = participant.decide_all(&[(KEY, contract())], at * CYCLE - 1);
    assert_eq!(decided, Decision::Admit);
    participant.on_cycle(at * CYCLE);
    sent(participant.take_outgoing())
}

/// The pods `batches` send entries to, and how many each.
fn sent(batches: Vec<(u64, Batch)>) -> Vec<(u64, usize)> {
    batches
        .into_iter()
        .filter(|(_, batch)| !batch.entries.is_empty())
        .map(|(pod, batch)| (pod, batch.entries.len()))
        .collect()
}

/// The owner's own reply about nothing, at `now`.
fn direct(from: u64) -> Reply<Fixed> {
    Reply {
        from,
        via: None,
        life: 0,
        number: 0,
        made: 0,
        entries: Vec::new(),
        routing: None,
    }
}

/// A key whose owner leaves its batches unanswered for four cycles is sent through the
/// routing's other pod as well, the same entries both ways, and the owner shows as
/// unanswered; once the owner answers for itself, the key goes to it alone again.
#[test]
fn an_owner_unanswered_four_cycles_has_its_keys_sent_through_another_pod_until_it_answers() {
    let mut participant = participant(routed());
    assert_eq!(cycle(&mut participant, 1), vec![(1, 1)]);
    for at in 2..=4 {
        assert_eq!(cycle(&mut participant, at), vec![(1, 1)], "cycle {at}");
        assert_eq!(participant.unanswered(at * CYCLE).count(), 0);
    }
    // Four cycles since the first batch: this cycle's report and the first sent again, both
    // ways.
    assert_eq!(cycle(&mut participant, 5), vec![(1, 2), (2, 2)]);
    assert_eq!(
        participant.unanswered(5 * CYCLE).collect::<Vec<_>>(),
        vec![1]
    );
    participant.on_reply(direct(1), 5 * CYCLE + 1);
    assert_eq!(participant.unanswered(5 * CYCLE + 1).count(), 0);
    // To the owner alone: this cycle's report, and the second sent again.
    assert_eq!(cycle(&mut participant, 6), vec![(1, 2)]);
}

/// The pods unanswered are those past four cycles alone, each from its first batch left
/// unanswered.
#[test]
fn a_pod_is_unanswered_from_four_cycles_after_its_first_batch_left_so() {
    let mut participant = participant(Fixed {
        through: None,
        ..routed()
    });
    let _sent = cycle(&mut participant, 1);
    for at in 2..=4 {
        let _sent = cycle(&mut participant, at);
    }
    assert_eq!(participant.unanswered(5 * CYCLE - 1).count(), 0);
    assert_eq!(
        participant.unanswered(5 * CYCLE).collect::<Vec<_>>(),
        vec![1]
    );
}

/// A reply relayed through another pod is the owner's: it acknowledges the key's reports and
/// marks its copy heard, but the owner is still waited on, its own reply not come.
#[test]
fn a_relayed_reply_acknowledges_as_the_owners_and_ends_no_wait() {
    let mut participant = participant(routed());
    for at in 1..=5 {
        let _sent = cycle(&mut participant, at);
    }
    let hash = participant.hash(KEY);
    let sent = participant.keys.get(KEY).unwrap().sent;
    let reply = |from| Reply {
        via: Some(2),
        entries: vec![Replied {
            key: hash,
            standing: Standing::Bucket { behind: 0 },
            applied: sent,
            others: 0,
        }],
        ..direct(from)
    };
    // From a pod the key's owner is not: taken as nothing about it.
    participant.on_reply(reply(3), 5 * CYCLE + 1);
    assert!(!participant.keys.get(KEY).unwrap().unacknowledged.is_empty());
    participant.on_reply(reply(1), 5 * CYCLE + 2);
    let tracked = participant.keys.get(KEY).unwrap();
    assert!(tracked.unacknowledged.is_empty(), "acknowledged by hash");
    assert_eq!(tracked.heard.get(), Some(5 * CYCLE + 2));
    assert!(participant.unanswered(5 * CYCLE + 2).any(|pod| pod == 1));
}

/// With one exchange in flight to each pod, the owner's still out: its key goes through the
/// other pod alone once the owner has gone four cycles unanswered, rather than waiting for the
/// exchange to be given up.
#[test]
fn with_the_owners_exchange_in_flight_its_keys_go_through_the_other_pod_alone() {
    let mut participant = participant(routed());
    participant.set_one_in_flight(true);
    participant.set_exchange_timeout(100 * CYCLE);
    assert_eq!(cycle(&mut participant, 1), vec![(1, 1)]);
    for at in 2..=4 {
        assert_eq!(
            cycle(&mut participant, at),
            Vec::new(),
            "the owner's exchange in flight"
        );
    }
    assert_eq!(cycle(&mut participant, 5), vec![(2, 2)]);
}

/// A later routing naming another owner sends the key's reports not yet seen applied to it
/// at once; a pod it names no longer is waited on no more; an earlier routing changes nothing.
#[test]
fn a_later_routing_moves_a_key_at_once_and_forgets_a_pod_it_no_longer_names() {
    let mut participant = participant(Fixed {
        through: None,
        ..routed()
    });
    for at in 1..=5 {
        let _sent = cycle(&mut participant, at);
    }
    assert_eq!(
        participant.unanswered(5 * CYCLE).collect::<Vec<_>>(),
        vec![1]
    );
    participant.on_routing(routed(), 5 * CYCLE + 1);
    assert!(
        participant.take_outgoing().is_empty(),
        "not later: nothing moved"
    );
    let moved = Fixed {
        epoch: 2,
        owner: Some(3),
        through: None,
        pods: vec![2, 3],
    };
    participant.on_routing(moved.clone(), 5 * CYCLE + 2);
    assert_eq!(participant.routing(), &moved);
    let unseen = participant
        .keys
        .get(KEY)
        .unwrap()
        .unacknowledged
        .as_slice()
        .len();
    assert_eq!(sent(participant.take_outgoing()), vec![(3, unseen)]);
    assert!(!participant.unanswered(u64::MAX).any(|pod| pod == 1));
}

/// Today's pods forward nothing: a key whose owner never answers goes to it alone.
#[test]
fn todays_pods_are_sent_nothing_through_another() {
    let view = View {
        epoch: 1,
        pods: vec![0, 1],
    };
    let owner: PodId = view.owners(KEY).0.unwrap();
    let mut participant = Participant::new(7, CYCLE, view, FixedState::default()).unwrap();
    for at in 1..=20 {
        let decided = participant.decide_all(&[(KEY, contract())], at * CYCLE - 1);
        assert_eq!(decided, Decision::Admit);
        participant.on_cycle(at * CYCLE);
        for (pod, batch) in participant.take_outgoing() {
            assert!(batch.entries.is_empty() || pod == owner, "cycle {at}");
        }
    }
    assert_eq!(
        participant.unanswered(20 * CYCLE).collect::<Vec<_>>(),
        vec![owner]
    );
}
