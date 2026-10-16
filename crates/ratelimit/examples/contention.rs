//! What sharing a pod's keys among its workers would cost a decision (12 item 3): a key as
//! one atomic GCRA state — its theoretical arrival time, moved by compare-and-swap, as the
//! governor crate keeps one — charged from 1 to 16 threads at once, against the plain state
//! a worker keeps for itself today.
//!
//! `cargo run --release -p edgerush-ratelimit --example contention [millions of decisions
//! a thread]`
//!
//! Every decision admits, so that every one writes: the worst for contention. Cases: one
//! key every thread charges (the hot key); a key a thread, each on a line of its own
//! (atomics, no contention); two hot keys charged all or none, the first given back when
//! the second refuses; and the plain state of today, a thread's own. Nanoseconds a decision,
//! the median of five runs, wall time over all threads' decisions together and a thread's
//! own.

use std::hint::black_box;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A key's state, alone on a cache line so that no other key's writes move it.
#[repr(align(128))]
struct Key(AtomicU64);

impl Key {
    fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// GCRA: admitted if the theoretical arrival time after this request is no further
    /// ahead of `now` than `tolerance`; then charged by `interval`.
    fn decide(&self, now: u64, interval: u64, tolerance: u64) -> bool {
        let mut tat = self.0.load(Ordering::Relaxed);
        loop {
            let next = tat.max(now) + interval;
            if next - now > tolerance {
                return false;
            }
            match self
                .0
                .compare_exchange_weak(tat, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(seen) => tat = seen,
            }
        }
    }

    /// Gives back a charge of `interval`, for an all-or-none decision another key refused.
    fn give_back(&self, interval: u64) {
        self.0.fetch_sub(interval, Ordering::AcqRel);
    }
}

/// The plain state a worker keeps of a key for itself.
struct Plain(u64);

impl Plain {
    fn decide(&mut self, now: u64, interval: u64, tolerance: u64) -> bool {
        let next = self.0.max(now) + interval;
        if next - now > tolerance {
            return false;
        }
        self.0 = next;
        true
    }
}

/// Every decision admits: one nanosecond a request, a tolerance no run reaches.
const INTERVAL: u64 = 1;
const TOLERANCE: u64 = u64::MAX / 4;

#[derive(Clone, Copy)]
enum Case {
    HotKey,
    KeyEach,
    TwoHotKeys,
    Plain,
}

/// Nanoseconds a decision, wall time over all `threads`' `each` decisions together, and a
/// thread's own.
fn measure(case: Case, threads: usize, each: u64) -> (f64, f64) {
    let hot = Key::new();
    let second = Key::new();
    let own: Vec<Key> = (0..threads).map(|_| Key::new()).collect();
    let start = Barrier::new(threads + 1);
    let began = Instant::now();
    let clock = || u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let mut wall = 0.0;
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let (hot, second, own, start) = (&hot, &second, &own[thread], &start);
            scope.spawn(move || {
                start.wait();
                let mut plain = Plain(0);
                let mut admitted = 0_u64;
                let mut now = clock();
                for decision in 0..each {
                    // The clock read once every 64 decisions, as a worker's request handling
                    // would amortise it.
                    if decision % 64 == 0 {
                        now = clock();
                    }
                    let yes = match case {
                        Case::HotKey => hot.decide(now, INTERVAL, TOLERANCE),
                        Case::KeyEach => own.decide(now, INTERVAL, TOLERANCE),
                        Case::TwoHotKeys => {
                            hot.decide(now, INTERVAL, TOLERANCE)
                                && (second.decide(now, INTERVAL, TOLERANCE) || {
                                    hot.give_back(INTERVAL);
                                    false
                                })
                        }
                        Case::Plain => plain.decide(now, INTERVAL, TOLERANCE),
                    };
                    admitted += u64::from(black_box(yes));
                }
                black_box(admitted);
            });
        }
        start.wait();
        let timed = Instant::now();
        // The scope's end waits for every thread.
        let _ = &timed;
        wall = timed.elapsed().as_nanos() as f64;
    });
    let total = began.elapsed().as_nanos() as f64;
    let _ = wall;
    let decisions = (threads as u64 * each) as f64;
    (total / decisions, total / each as f64)
}

fn main() {
    let millions: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(10);
    let each = millions * 1_000_000;
    let threads = [1_usize, 2, 4, 8, 16];
    let cases = [
        ("plain, a worker's own (today)", Case::Plain),
        ("atomic, a key a thread", Case::KeyEach),
        ("atomic, one hot key", Case::HotKey),
        ("atomic, two hot keys, all or none", Case::TwoHotKeys),
    ];
    println!(
        "{} hardware threads; {millions} million decisions a thread; ns a decision, all \
         threads together / a thread's own; median of five.\n",
        std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get)
    );
    print!("| Case |");
    for t in threads {
        print!(" {t} thread{} |", if t == 1 { "" } else { "s" });
    }
    println!();
    println!("|---|{}", "---|".repeat(threads.len()));
    for (name, case) in cases {
        print!("| {name} |");
        for t in threads {
            let mut runs: Vec<(f64, f64)> = (0..5).map(|_| measure(case, t, each)).collect();
            runs.sort_by(|a, b| a.1.total_cmp(&b.1));
            let (together, own) = runs[2];
            print!(" {together:.1} / {own:.1} |");
        }
        println!();
    }
}
