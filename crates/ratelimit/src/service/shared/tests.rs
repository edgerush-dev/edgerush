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

/// A process, and the one service pod, which owns every key.
fn process() -> (Process<FixedState>, ServicePod) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let plane = DataPlane::new(1, CYCLE, view.clone());
    (
        Process::new(plane, FixedState::default()),
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

/// `k` warm at `now`: one request asked about and answered, and a cycle that bounds the
/// copy; the answer.
fn warm(process: &mut Process<FixedState>, pod: &mut ServicePod, now: u64) -> Answer {
    let keys = [(&b"k"[..], contract())];
    assert!(process.on_ask(0, 1, &keys, now).is_none(), "cold: it waits");
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

/// A worker takes from a room it holds, all of whose takes the process sees at its sync;
/// with nothing left it asks; told the bucket is empty it refuses on its own.
#[test]
fn a_worker_takes_from_the_room_and_asks_once_it_is_empty() {
    let (mut process, mut pod) = process();
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let mut one = worker();
    assert_eq!(one.decide(&keys, 0), Local::Ask, "nothing held");
    let answer = process.on_ask(0, 2, &keys, 0).unwrap();
    assert!(answer.admitted);
    one.on_rooms(answer.rooms);
    // Nine left after the warm one: the ask took one, the worker the other eight.
    for _ in 0..8 {
        assert_eq!(one.decide(&keys, 0), Local::Admit);
    }
    assert_eq!(one.decide(&keys, 0), Local::Ask, "the room empty");
    // The process folds it: the bucket empty, every worker refuses till it has room.
    process.on_sync(MS);
    assert_eq!(one.decide(&keys, MS), Local::Refuse);
    let waits: Vec<_> = one.waits(&keys, MS).collect();
    assert_eq!(waits.len(), 1);
    assert!(waits[0].1 > 0 && waits[0].1 <= 10 * MS, "{waits:?}");
    let until = MS + waits[0].1;
    assert_eq!(one.decide(&keys, until), Local::Ask, "then for the process");
    // At its next sync past then, the room has one again.
    process.on_sync(until);
    assert_eq!(one.decide(&keys, until), Local::Admit);
}

/// What the workers take is charged to the process's data plane when it folds the room,
/// and goes to the owner with its next batch: counted once.
#[test]
fn what_workers_take_reaches_the_owner_once() {
    let (mut process, mut pod) = process();
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, MS).unwrap();
    let (mut zero, mut one) = (worker(), worker());
    zero.on_rooms(answer.rooms.clone());
    one.on_rooms(answer.rooms);
    let mut admitted = 0;
    for _ in 0..3 {
        admitted += u64::from(zero.decide(&keys, MS) == Local::Admit);
        admitted += u64::from(one.decide(&keys, MS) == Local::Admit);
    }
    assert_eq!(admitted, 6);
    process.on_sync(2 * MS);
    process.plane().on_cycle(CYCLE);
    settle(&mut process, &mut pod, CYCLE);
    assert_eq!(
        pod.counted(b"k"),
        Some(2 + admitted),
        "the two asked and the workers'"
    );
}

/// However many threads take from a room at once, no more is taken than was put in it.
#[test]
fn threads_taking_at_once_never_take_more_than_the_room() {
    let room = Room::default();
    room.left.store(10_000, Ordering::Release);
    let taken: u64 = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let mut taken = 0;
                    while room.take(0) == Take::Taken {
                        taken += 1;
                    }
                    taken
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    });
    assert_eq!(taken, 10_000);
    assert_eq!(room.left.load(Ordering::Acquire), 0);
}

/// A request under two keys is admitted only if both rooms have some left; the first given
/// back if the second has none.
#[test]
fn two_keys_are_taken_all_or_none() {
    let full = Arc::new(Room::default());
    full.left.store(5, Ordering::Release);
    let empty = Arc::new(Room::default());
    let mut one = worker();
    one.on_rooms(vec![
        Handed {
            key: b"a"[..].into(),
            contract: contract(),
            room: Arc::clone(&full),
        },
        Handed {
            key: b"b"[..].into(),
            contract: contract(),
            room: Arc::clone(&empty),
        },
    ]);
    let both = [(&b"a"[..], contract()), (&b"b"[..], contract())];
    assert_eq!(one.decide(&both, 0), Local::Ask);
    assert_eq!(full.left.load(Ordering::Acquire), 5, "given back");
    empty.left.store(1, Ordering::Release);
    assert_eq!(one.decide(&both, 0), Local::Admit);
    assert_eq!(
        (
            full.left.load(Ordering::Acquire),
            empty.left.load(Ordering::Acquire)
        ),
        (4, 0)
    );
}

/// A room the process has closed is asked about rather than taken from, and dropped; a
/// key nobody comes to for long is closed.
#[test]
fn a_closed_room_is_asked_about_and_an_idle_key_closed() {
    let (mut process, mut pod) = process();
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, 0).unwrap();
    let room = Arc::clone(&answer.rooms[0].room);
    let mut one = worker();
    one.on_rooms(answer.rooms);
    assert_eq!(process.len(), 1);
    // The ask's take folded at the next sync, and then nothing for longer than IDLE.
    process.on_sync(MS);
    process.on_sync(IDLE + MS);
    assert!(!room.closed(), "not yet");
    process.on_sync(IDLE + 2 * MS);
    assert!(room.closed());
    assert!(process.is_empty(), "forgotten");
    assert_eq!(one.decide(&keys, IDLE + 2 * MS), Local::Ask);
    assert!(one.is_empty(), "dropped");
}

/// An ask that finds a room emptied by the workers after its fold folds again, so that
/// what they took is charged before it is decided: never past the room.
#[test]
fn an_ask_never_admits_past_what_the_workers_took() {
    let (mut process, mut pod) = process();
    warm(&mut process, &mut pod, 0);
    let keys = [(&b"k"[..], contract())];
    let answer = process.on_ask(0, 2, &keys, 0).unwrap();
    let mut one = worker();
    one.on_rooms(answer.rooms);
    while one.decide(&keys, 0) == Local::Admit {}
    // The room is empty, untouched by any sync: the ask folds it, finds the bucket empty.
    let answer = process.on_ask(1, 3, &keys, 0).unwrap();
    assert!(!answer.admitted);
    assert_eq!(
        answer.refused_by.len(),
        1,
        "the bucket empty: when to come back"
    );
}
