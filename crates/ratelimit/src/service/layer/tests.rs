use super::*;
use crate::service::{FixedState, ServicePod, View};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;

/// 100 a second, a bucket of ten.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(100),
        window: n(1_000 * MS),
        burst: n(10),
    }
}

/// A process of `workers` syncing every millisecond, and the one service pod, which owns
/// every key.
fn process(workers: usize) -> (Process<FixedState>, ServicePod) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let plane = DataPlane::new(1, CYCLE, view.clone());
    (
        Process::new(plane, workers, MS, FixedState::default()),
        ServicePod::new(0, CYCLE, view),
    )
}

/// What the process's data plane has to send, answered, until nothing is left.
fn settle(process: &mut Process<FixedState>, pod: &mut ServicePod, now: u64) {
    for _ in 0..10 {
        let outgoing = process.plane().take_outgoing();
        if outgoing.is_empty() {
            return;
        }
        for (_, batch) in outgoing {
            let answers = pod.on_batch(batch, now);
            process.plane().on_answers(answers, now);
        }
    }
}

/// The process with `k` warm: one request asked about and answered, and a cycle that
/// bounds the copy.
fn warm(process: &mut Process<FixedState>, pod: &mut ServicePod, worker: usize) -> Answer {
    warm_key(process, pod, worker, (b"k", contract()), 0)
}

/// The process with `key` under its contract warm at `now`, as [`warm`] has `k`.
fn warm_key(
    process: &mut Process<FixedState>,
    pod: &mut ServicePod,
    worker: usize,
    key: (&[u8], Contract),
    now: u64,
) -> Answer {
    let keys = [key];
    assert_eq!(
        process.on_ask(worker, 1, &keys, now),
        None,
        "cold: it waits"
    );
    settle(process, pod, now);
    let answers = process.take_answers(now);
    process.plane().on_cycle(now);
    settle(process, pod, now);
    let (_, id, answer) = answers.into_iter().next().unwrap();
    assert_eq!(id, 1);
    answer
}

fn worker() -> Worker<FixedState> {
    Worker::with_hasher(FixedState::default())
}

/// A worker admits from its slice on its own, refuses on its own while told there is no
/// room, and asks otherwise; a sync sends what it used and its slices lapse.
#[test]
fn a_worker_decides_from_its_slice_and_asks_without_one() {
    let keys = [(&b"k"[..], contract())];
    let mut one = worker();
    assert_eq!(one.decide(&keys, 0), Local::Ask, "nothing held");
    one.on_slices(vec![Slice {
        key: b"k"[..].into(),
        contract: contract(),
        allowance: 2,
        refuse_until: None,
    }]);
    assert_eq!(one.decide(&keys, 0), Local::Admit);
    assert_eq!(one.decide(&keys, 0), Local::Admit);
    assert_eq!(one.decide(&keys, 0), Local::Ask, "the slice spent");
    let used = one.take_used(0);
    assert_eq!(used.len(), 1);
    assert_eq!(used[0].count, 2);
    assert!(one.take_used(0).is_empty(), "nothing since: forgotten");
    // A slice not spent lapses at the sync: what is left of it is the process's again.
    one.on_slices(vec![Slice {
        key: b"k"[..].into(),
        contract: contract(),
        allowance: 3,
        refuse_until: None,
    }]);
    assert_eq!(one.decide(&keys, 0), Local::Admit);
    assert_eq!(one.take_used(0)[0].count, 1);
    assert_eq!(one.decide(&keys, 0), Local::Ask, "the two left lapsed");
    one.on_slices(vec![Slice {
        key: b"k"[..].into(),
        contract: contract(),
        allowance: 0,
        refuse_until: Some(5 * MS),
    }]);
    assert_eq!(one.decide(&keys, MS), Local::Refuse, "no room until 5 ms");
    assert_eq!(one.waits(&keys, MS).collect::<Vec<_>>(), [(0, 4 * MS)]);
    assert_eq!(
        one.decide(&keys, 5 * MS),
        Local::Ask,
        "then for the process"
    );
    // Under two keys, admitted only if both have slice left.
    let both = [(&b"k"[..], contract()), (&b"j"[..], contract())];
    one.on_slices(vec![Slice {
        key: b"k"[..].into(),
        contract: contract(),
        allowance: 1,
        refuse_until: None,
    }]);
    assert_eq!(one.decide(&both, 6 * MS), Local::Ask);
    assert_eq!(
        one.decide(&keys, 6 * MS),
        Local::Admit,
        "the first untouched"
    );
}

/// The process shares a key's room equally among the workers that came to it lately —
/// a worker alone has all of it — never giving out more than the room; an ask is admitted
/// only past what is given out.
#[test]
fn the_process_gives_equal_slices_and_never_more_than_its_room() {
    let (mut process, mut pod) = process(4);
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let k = |count| Used {
        key: b"k"[..].into(),
        contract: contract(),
        count,
    };
    // Room nine. Worker 0 alone has come to it: all eight left after its request.
    let first = process.on_ask(0, 2, &keys, 0).unwrap();
    assert!(first.admitted);
    assert_eq!(first.slices[0].allowance, 8);
    // Worker 1 comes: nothing left until worker 0's slice lapses — but no refusal: the
    // room is there, in worker 0's slice.
    assert_eq!(
        process.on_ask(1, 3, &keys, MS),
        None,
        "it waits for worker 0's sync"
    );
    assert!(process.take_answers(MS).is_empty());
    // A millisecond on, worker 0 syncs, one used: half of the seven left is its, and
    // worker 1's request, waiting, is admitted from what lapsed, with a slice of its own.
    assert_eq!(process.on_used(0, vec![k(1)], 2 * MS)[0].allowance, 3);
    let answers = process.take_answers(2 * MS);
    assert_eq!(answers.len(), 1);
    let (asker, id, answer) = &answers[0];
    assert_eq!((*asker, *id, answer.admitted), (1, 3, true));
    assert_eq!(
        answer.slices[0].allowance, 3,
        "six left, less worker 0's three"
    );
    // Worker 1 syncs, its three spent: what room is left is in worker 0's slice.
    let late = process.on_used(1, vec![k(3)], 3 * MS);
    assert_eq!(late[0].allowance, 0);
    assert_eq!(late[0].refuse_until, None, "room given out is no refusal");
    // Four workers, a request each a millisecond, under a key with room to spare: never
    // more given out than the room, however they arrive, and settled, a quarter each.
    let n = |n| NonZeroU64::new(n).unwrap();
    let roomy = Contract {
        limit: n(10_000),
        window: n(1_000 * MS),
        burst: n(400),
    };
    warm_key(&mut process, &mut pod, 0, (b"r", roomy), 3 * MS);
    let r = [(&b"r"[..], roomy)];
    let mut workers: Vec<_> = (0..4).map(|_| worker()).collect();
    let mut id = 10;
    for round in 4..12 {
        let now = round * MS;
        for (at, one) in workers.iter_mut().enumerate() {
            if one.decide(&r, now) == Local::Ask {
                id += 1;
                if let Some(answer) = process.on_ask(at, id, &r, now) {
                    one.on_slices(answer.slices);
                }
            }
            let used = one.take_used(now);
            one.on_slices(process.on_used(at, used, now));
            // The service answers within the round, as it does within a sync or two.
            settle(&mut process, &mut pod, now);
            let room = process.plane().room(b"r", roomy, now).unwrap();
            let out: u64 = process.given[&b"r"[..]].slices.iter().sum();
            assert!(out <= room, "round {round}: {out} of {room}");
        }
        for (at, _, answer) in process.take_answers(now) {
            workers[at].on_slices(answer.slices);
        }
    }
    let held: Vec<u64> = workers
        .iter()
        .map(|one| one.keys[&b"r"[..]].allowance)
        .collect();
    // A quarter each of the room at a sync: the full 400, less the request just taken.
    assert_eq!(held, [99; 4]);
}

/// A key's room spent comes back with until when it has room again; the worker told so
/// refuses on its own till then.
#[test]
fn no_room_comes_back_with_until_when() {
    let (mut process, mut pod) = process(1);
    warm(&mut process, &mut pod, 0);
    let k = |count| Used {
        key: b"k"[..].into(),
        contract: contract(),
        count,
    };
    let slices = process.on_used(0, vec![k(9)], MS);
    assert_eq!(slices[0].allowance, 0);
    // A hundred a second: one back 10 ms after the last was taken.
    let until = slices[0].refuse_until.unwrap();
    assert!(until > MS && until <= MS + 10 * MS, "{until}");
    let mut one = worker();
    one.on_slices(slices);
    let keys = [(&b"k"[..], contract())];
    assert_eq!(one.decide(&keys, MS), Local::Refuse);
    assert_eq!(one.decide(&keys, until), Local::Ask);
}

/// What workers admit on their own is charged to the process's data plane at their sync
/// and goes to the owner with its next batch: counted once.
#[test]
fn what_workers_admit_reaches_the_owner_once() {
    let (mut process, mut pod) = process(2);
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, MS).unwrap();
    assert!(answer.admitted);
    let mut zero = worker();
    zero.on_slices(answer.slices);
    let admitted = (0..20)
        .filter(|_| zero.decide(&keys, MS) == Local::Admit)
        .count() as u64;
    assert!(admitted > 0);
    process.on_used(0, zero.take_used(MS), 2 * MS);
    process.plane().on_cycle(CYCLE);
    settle(&mut process, &mut pod, CYCLE);
    assert_eq!(
        pod.counted(b"k"),
        Some(2 + admitted),
        "the two asked and the worker's"
    );
}

/// An ask waiting for a holder's sync is answered at that sync however late it comes: a
/// worker waiting counts among those come to the key, so the room is shared with it.
#[test]
fn a_waiting_ask_is_answered_however_late_the_holders_sync() {
    let (mut process, mut pod) = process(2);
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, MS).unwrap();
    assert_eq!(answer.slices[0].allowance, 8, "nine, less the one asked");
    assert_eq!(
        process.on_ask(1, 7, &keys, MS),
        None,
        "the room is worker 0's"
    );
    // Worker 0's next sync, one used, long after two of them would have come.
    let k = Used {
        key: b"k"[..].into(),
        contract: contract(),
        count: 1,
    };
    process.on_used(0, vec![k], 50 * MS);
    let answers = process.take_answers(50 * MS);
    assert_eq!(answers.len(), 1, "answered at the holder's sync");
    assert_eq!(
        (answers[0].0, answers[0].1, answers[0].2.admitted),
        (1, 7, true)
    );
}

/// A slice a worker leaves unused is given back at its next sync and none given in its
/// place: an idle worker sends nothing after, and the room goes to the workers still coming.
#[test]
fn an_unused_slice_lapses_and_is_not_given_again() {
    let (mut process, mut pod) = process(2);
    let mut zero = worker();
    warm(&mut process, &mut pod, 0);
    // Its next request, the copy fresh: admitted, with a slice.
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, 0).unwrap();
    assert!(answer.admitted);
    zero.on_slices(answer.slices);
    assert_eq!(zero.decide(&keys, 0), Local::Admit);
    // Its first sync: one used, a new slice.
    let slices = process.on_used(0, zero.take_used(MS), MS);
    assert!(slices[0].allowance > 0, "{slices:?}");
    zero.on_slices(slices);
    // Its next, nothing used: the slice given back, none given.
    let used = zero.take_used(2 * MS);
    assert_eq!(used.len(), 1, "given back once");
    assert!(process.on_used(0, used, 2 * MS).is_empty(), "none given");
    assert!(zero.take_used(3 * MS).is_empty(), "idle: nothing sent");
    // Worker 1, coming later, is given the whole room.
    let answer = process.on_ask(1, 3, &keys, 10 * MS).unwrap();
    assert!(answer.admitted);
    let room = process.plane().room(b"k", contract(), 10 * MS).unwrap();
    assert_eq!(answer.slices[0].allowance, room);
}

/// Kept (`keeping`), a slice a worker leaves unused is given again at each sync until the
/// worker last came to the key longer ago than the keep: a quiet key's next request is
/// decided on the spot rather than asked.
#[test]
fn a_kept_slice_is_given_again_until_its_keep_runs_out() {
    let (process, mut pod) = process(2);
    let mut process = process.keeping(5 * MS);
    let mut zero = worker();
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, 0).unwrap();
    zero.on_slices(answer.slices);
    // Nothing used: given again, to 5 ms after the sync that carried the ask.
    for sync in 1..=6 {
        let used = zero.take_used(sync * MS);
        let slices = process.on_used(0, used, sync * MS);
        assert_eq!(slices.len(), 1, "kept at {sync} ms");
        assert!(slices[0].allowance > 0, "at {sync} ms: {slices:?}");
        zero.on_slices(slices);
        // A kept slice counts its worker in the share: another's ask, finding the room all
        // given out, is answered at the kept worker's next sync.
        if sync == 3 {
            assert_eq!(process.on_ask(1, 9, &keys, 3 * MS), None);
        }
        if sync == 4 {
            let answers = process.take_answers(4 * MS);
            assert_eq!(answers.len(), 1, "answered at the kept worker's sync");
            assert!(answers[0].2.admitted);
        }
    }
    let used = zero.take_used(7 * MS);
    assert!(
        process.on_used(0, used, 7 * MS).is_empty(),
        "the keep run out"
    );
    assert!(zero.take_used(8 * MS).is_empty());
    // Asked once more, it has a slice again.
    assert_eq!(zero.decide(&keys, 9 * MS), Local::Ask);
    zero.on_slices(process.on_ask(0, 3, &keys, 9 * MS).unwrap().slices);
    assert_eq!(zero.decide(&keys, 9 * MS), Local::Admit);
}
