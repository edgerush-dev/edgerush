//! The synchronous owner: deciding, giving back, and carrying on from copies.

use super::*;
use crate::service::FixedState;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(SECOND),
        burst: n(burst),
    }
}

fn owner() -> Owner<FixedState> {
    Owner::new(FixedState::default(), 1)
}

fn handles() -> Handles<FixedState> {
    Handles::new(FixedState::default())
}

fn request(id: u64, keys: &[(&[u8], Contract)]) -> Decide {
    Decide {
        id,
        keys: keys.iter().map(|(key, c)| ((*key).into(), *c)).collect(),
    }
}

/// How many of `count` requests under `key` alone `owner` admits at `now`.
fn admitted(
    owner: &Owner<FixedState>,
    handles: &mut Handles<FixedState>,
    key: &[u8],
    c: Contract,
    count: u64,
    now: u64,
) -> u64 {
    (0..count)
        .filter(|id| {
            owner
                .decide(handles, &request(*id, &[(key, c)]), now)
                .decision
                == Decision::Admit
        })
        .count() as u64
}

#[test]
fn an_answer_names_its_request() {
    let owner = owner();
    let mut handles = handles();
    let c = contract(10, 1);
    let answer = owner.decide(&mut handles, &request(41, &[(b"k", c)]), 0);
    assert_eq!(
        answer,
        Decided {
            id: 41,
            decision: Decision::Admit
        }
    );
    let answer = owner.decide(&mut handles, &request(42, &[(b"k", c)]), 0);
    assert_eq!(answer.id, 42);
    assert!(matches!(
        answer.decision,
        Decision::Refuse { position: 0, .. }
    ));
}

/// A request one owner admitted and another refused is given back to the first: its bucket
/// is as if the request never came.
#[test]
fn a_charge_given_back_leaves_the_room_it_took() {
    let owner = owner();
    let mut handles = handles();
    let c = contract(10, 3);
    assert_eq!(
        owner
            .decide(&mut handles, &request(1, &[(b"k", c)]), 0)
            .decision,
        Decision::Admit
    );
    owner.give_back(
        &mut handles,
        &GiveBack {
            keys: vec![(b"k"[..].into(), c)],
        },
        0,
    );
    assert_eq!(admitted(&owner, &mut handles, b"k", c, 5, 0), 3);
}

/// A give-back never makes a bucket fuller than full: one that refilled meanwhile admits its
/// bucket and no more.
#[test]
fn a_give_back_never_fills_past_full() {
    let owner = owner();
    let mut handles = handles();
    let c = contract(10, 3);
    assert_eq!(
        owner
            .decide(&mut handles, &request(1, &[(b"k", c)]), 0)
            .decision,
        Decision::Admit
    );
    // Full again by 100 ms; the give-back arrives later.
    let back = GiveBack {
        keys: vec![(b"k"[..].into(), c)],
    };
    owner.give_back(&mut handles, &back, SECOND);
    owner.give_back(&mut handles, &back, SECOND);
    assert_eq!(admitted(&owner, &mut handles, b"k", c, 5, SECOND), 3);
}

/// Only keys changed since their last copy are copied, each once whichever connections
/// changed it, and again once changed again.
#[test]
fn a_cycle_copies_what_changed_since_the_last() {
    let owner = Owner::new(FixedState::default(), 2);
    let (mut one, mut other) = (handles(), handles());
    let c = contract(10, 5);
    owner.decide(&mut one, &request(1, &[(b"a", c)]), 0);
    owner.decide(&mut other, &request(2, &[(b"a", c)]), 0);
    owner.decide(&mut other, &request(3, &[(b"b", c)]), 0);
    let changed: Vec<Shared> = one
        .take_changed()
        .into_iter()
        .chain(other.take_changed())
        .collect();
    let mut keys: Vec<Box<[u8]>> = owner
        .copies(&changed, 0)
        .into_iter()
        .map(|copy| copy.key)
        .collect();
    keys.sort();
    assert_eq!(keys, [b"a"[..].into(), b"b"[..].into()]);
    assert!(
        one.take_changed().is_empty() && other.take_changed().is_empty(),
        "nothing since"
    );
    owner.decide(&mut one, &request(4, &[(b"b", c)]), 0);
    let changed = one.take_changed();
    assert_eq!(owner.copies(&changed, 0).len(), 1);
    // A refusal changes nothing.
    let tight = contract(10, 1);
    owner.decide(&mut one, &request(5, &[(b"t", tight)]), 0);
    let _ = owner.copies(&one.take_changed(), 0);
    owner.decide(&mut one, &request(6, &[(b"t", tight)]), 0);
    assert!(one.take_changed().is_empty());
}

/// A pod taking over carries on from the owner's last copy: what the owner admitted before it
/// is still spent, and what it admitted after it — at most a cycle of decisions — is not.
#[test]
fn a_pod_taking_over_carries_on_from_the_last_copy() {
    let (owner, standby) = (self::owner(), self::owner());
    let (mut at_owner, mut at_standby) = (handles(), handles());
    let c = contract(1, 5);
    assert_eq!(admitted(&owner, &mut at_owner, b"k", c, 3, 0), 3);
    let copies = owner.copies(&at_owner.take_changed(), 0);
    standby.install(&copies, 0);
    // One more after the copy, which the standby never hears of.
    assert_eq!(admitted(&owner, &mut at_owner, b"k", c, 1, 0), 1);
    assert_eq!(admitted(&standby, &mut at_standby, b"k", c, 5, 0), 2);
}

/// A copy never makes a bucket emptier (#88): one from an owner the view has moved on from,
/// arriving late, leaves what the new owner charged since, and the fuller of the two holds.
#[test]
fn a_late_copy_leaves_what_was_charged_since() {
    let (old, new) = (self::owner(), self::owner());
    let (mut at_old, mut at_new) = (handles(), handles());
    let c = contract(1, 5);
    assert_eq!(admitted(&old, &mut at_old, b"k", c, 1, 0), 1);
    let late = old.copies(&at_old.take_changed(), 0);
    assert_eq!(admitted(&new, &mut at_new, b"k", c, 3, 0), 3);
    new.install(&late, 0);
    assert_eq!(admitted(&new, &mut at_new, b"k", c, 5, 0), 2);
    // A copy fuller than the bucket is still taken in.
    assert_eq!(admitted(&old, &mut at_old, b"k", c, 4, 0), 4);
    new.install(&old.copies(&at_old.take_changed(), 0), 0);
    assert_eq!(admitted(&new, &mut at_new, b"k", c, 5, 0), 0);
}

/// A key quiet for longer than its clock takes to come round is full, whatever its word
/// says: a copy of a full bucket taken in leaves it full, rather than keeping the word as
/// the fuller of the two.
#[test]
fn a_copy_of_a_full_bucket_leaves_a_quiet_key_full() {
    let standby = owner();
    let mut at_standby = handles();
    // One a nanosecond: the 63-bit clock comes round every 2^47 ns, when the word reads a
    // request's interval ahead.
    let one = NonZeroU64::new(1).unwrap();
    let c = Contract {
        limit: one,
        window: one,
        burst: one,
    };
    let round = 1 << 47;
    assert_eq!(admitted(&standby, &mut at_standby, b"k", c, 1, 0), 1);
    let full = vec![Copy {
        key: b"k"[..].into(),
        contract: c,
        full_in: 0,
    }];
    standby.install(&full, round);
    assert_eq!(admitted(&standby, &mut at_standby, b"k", c, 1, round), 1);
}

/// A copy says how long until full, not when: taken in on a clock far from the owner's, the
/// bucket is where it was.
#[test]
fn a_copy_reads_the_same_on_another_clock() {
    let (owner, standby) = (self::owner(), self::owner());
    let (mut at_owner, mut at_standby) = (handles(), handles());
    let c = contract(1, 5);
    assert_eq!(admitted(&owner, &mut at_owner, b"k", c, 4, 7 * SECOND), 4);
    let copies = owner.copies(&at_owner.take_changed(), 7 * SECOND);
    assert_eq!(copies[0].full_in, 4 * SECOND);
    let elsewhere = 1_000 * SECOND;
    standby.install(&copies, elsewhere);
    assert_eq!(
        admitted(&standby, &mut at_standby, b"k", c, 5, elsewhere),
        1
    );
    // And refills from there as the owner's would.
    assert_eq!(
        admitted(
            &standby,
            &mut at_standby,
            b"k",
            c,
            5,
            elsewhere + 2 * SECOND
        ),
        2
    );
}

/// A copy of a key under another contract replaces it, as a changed contract starts a key
/// afresh anywhere.
#[test]
fn a_copy_under_a_new_contract_replaces_the_key() {
    let (owner, standby) = (self::owner(), self::owner());
    let (mut at_owner, mut at_standby) = (handles(), handles());
    let (old, new) = (contract(1, 5), contract(2, 5));
    assert_eq!(admitted(&standby, &mut at_standby, b"k", old, 5, 0), 5);
    assert_eq!(admitted(&owner, &mut at_owner, b"k", new, 1, 0), 1);
    standby.install(&owner.copies(&at_owner.take_changed(), 0), 0);
    assert_eq!(admitted(&standby, &mut at_standby, b"k", new, 5, 0), 4);
}

/// A request under several keys, and a give-back, change each of their keys, which are
/// gathered for the next copy as one key's decision is.
#[test]
fn several_keys_and_give_backs_are_gathered_too() {
    let owner = owner();
    let mut handles = handles();
    let c = contract(10, 5);
    let both = request(1, &[(b"a", c), (b"b", c)]);
    assert_eq!(
        owner.decide(&mut handles, &both, 0).decision,
        Decision::Admit
    );
    let mut keys: Vec<Box<[u8]>> = owner
        .copies(&handles.take_changed(), 0)
        .into_iter()
        .map(|copy| copy.key)
        .collect();
    keys.sort();
    assert_eq!(keys, [b"a"[..].into(), b"b"[..].into()]);
    owner.give_back(
        &mut handles,
        &GiveBack {
            keys: vec![(b"a"[..].into(), c)],
        },
        0,
    );
    let copies = owner.copies(&handles.take_changed(), 0);
    assert_eq!(copies.len(), 1);
    assert_eq!((&*copies[0].key, copies[0].full_in), (&b"a"[..], 0));
}
