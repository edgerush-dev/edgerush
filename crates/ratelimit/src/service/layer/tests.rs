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
    let keys = [(&b"k"[..], contract())];
    assert_eq!(process.on_ask(worker, 1, &keys, 0), None, "cold: it waits");
    settle(process, pod, 0);
    let answers = process.take_answers(0);
    process.plane().on_cycle(0);
    settle(process, pod, 0);
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
    let first = warm(&mut process, &mut pod, 0);
    assert!(first.admitted);
    let k = |count| Used {
        key: b"k"[..].into(),
        contract: contract(),
        count,
    };
    let sync = |process: &mut Process<FixedState>, worker, now| {
        process.on_used(worker, vec![k(0)], now)[0].allowance
    };
    // Room nine. Worker 0 alone has come to it: all nine.
    assert_eq!(sync(&mut process, 0, MS), 9);
    // Worker 1 comes: nothing left until worker 0's slice lapses.
    let late = process.on_used(1, vec![k(0)], MS);
    assert_eq!(late[0].allowance, 0);
    assert!(late[0].refuse_until.is_some(), "all of the room given out");
    let keys = [(&b"k"[..], contract())];
    assert!(
        !process.on_ask(1, 2, &keys, MS).unwrap().admitted,
        "the room is worker 0's until its sync"
    );
    // A millisecond on, both sync: half each, rounded down.
    assert_eq!(sync(&mut process, 0, 2 * MS), 4);
    assert_eq!(sync(&mut process, 1, 2 * MS), 4);
    // All four come: never more given out than the room, however they arrive.
    let mut out = [0; 4];
    for round in 3..8 {
        for worker in 0..4 {
            out[worker] = sync(&mut process, worker, round * MS);
            let room = process.plane().room(b"k", contract(), round * MS).unwrap();
            assert!(
                out.iter().sum::<u64>() <= room,
                "round {round}: {out:?} of {room}"
            );
        }
    }
    // Settled, each has a quarter: nine among four, two each.
    assert_eq!(out, [2; 4]);
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
    let k = |count| Used {
        key: b"k"[..].into(),
        contract: contract(),
        count,
    };
    let slices = process.on_used(0, vec![k(0)], MS);
    let mut zero = worker();
    zero.on_slices(slices);
    let keys = [(&b"k"[..], contract())];
    let admitted = (0..20)
        .filter(|_| zero.decide(&keys, MS) == Local::Admit)
        .count() as u64;
    assert!(admitted > 0);
    process.on_used(0, zero.take_used(MS), 2 * MS);
    process.plane().on_cycle(CYCLE);
    settle(&mut process, &mut pod, CYCLE);
    assert_eq!(
        pod.counted(b"k"),
        Some(1 + admitted),
        "the asked one and the worker's"
    );
}
