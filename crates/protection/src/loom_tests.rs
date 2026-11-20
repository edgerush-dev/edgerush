//! Every interleaving of workers racing on one pod's count, under loom
//! (`RUSTFLAGS="--cfg loom" cargo test --release -p edgerush-protection --lib loom`): the
//! last slot taken by one of them alone, and a slot given back while another takes.

#![cfg(loom)]

use super::*;
use loom::thread;

fn cap(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

#[test]
fn loom_two_workers_racing_for_the_last_slot_take_it_once() {
    loom::model(|| {
        let pod = PodCount::new();
        let held = PodCount::take(&pod, cap(2)).unwrap();
        let other = Arc::clone(&pod);
        // Each keeps what it took until both have tried.
        let racer = thread::spawn(move || PodCount::take(&other, cap(2)));
        let mine = PodCount::take(&pod, cap(2));
        let theirs = racer.join().unwrap();
        assert!(
            mine.is_some() != theirs.is_some(),
            "exactly one of them takes the last slot"
        );
        assert_eq!(pod.in_flight(), 2);
        drop((held, mine, theirs));
        assert_eq!(pod.in_flight(), 0);
    });
}

#[test]
fn loom_three_workers_never_pass_a_cap_of_one() {
    loom::model(|| {
        let pod = PodCount::new();
        let racers: Vec<_> = (0..2)
            .map(|_| {
                let pod = Arc::clone(&pod);
                thread::spawn(move || {
                    let slot = PodCount::take(&pod, cap(1));
                    if slot.is_some() {
                        assert_eq!(pod.in_flight(), 1);
                    }
                    slot.is_some()
                })
            })
            .collect();
        let mine = PodCount::take(&pod, cap(1));
        if mine.is_some() {
            assert_eq!(pod.in_flight(), 1);
        }
        drop(mine);
        for racer in racers {
            racer.join().unwrap();
        }
        assert_eq!(pod.in_flight(), 0, "every slot came back");
    });
}

#[test]
fn loom_a_slot_given_back_while_another_worker_takes_is_never_lost() {
    loom::model(|| {
        let pod = PodCount::new();
        let held = PodCount::take(&pod, cap(1)).unwrap();
        let other = Arc::clone(&pod);
        let taker = thread::spawn(move || PodCount::take(&other, cap(1)));
        drop(held);
        let taken = taker.join().unwrap();
        // Taken after the give-back or refused before it: the count says which.
        assert_eq!(pod.in_flight(), u32::from(taken.is_some()));
        drop(taken);
        assert_eq!(pod.in_flight(), 0);
    });
}
