//! Cost of disabled, active and exhausted h2 deadline checkpoints. Linux/Valgrind:
//! `cargo bench -p edgerush-proxy --bench h2_turn`.

#![allow(missing_docs, reason = "benchmark macro entry points")]

// Measure the exact internal checkpoint without adding a public instrumentation API.
#[path = "../../../vendor/h2/src/turn.rs"]
#[allow(
    unreachable_pub,
    reason = "the included module's public API belongs to h2"
)]
mod turn;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::task::{Context, Waker};
use std::time::{Duration, Instant};
use turn::{Turn, TurnBudget};

fn installed(mode: u8) -> Turn {
    let mut turn = Turn::default();
    turn.set(match mode {
        0 => None,
        1 => Some(TurnBudget::new(Instant::now() + Duration::from_secs(3600))),
        _ => Some(TurnBudget::new(Instant::now())),
    });
    turn
}

#[library_benchmark]
#[bench::disabled(installed(0))]
#[bench::active(installed(1))]
#[bench::exhausted(installed(2))]
fn checkpoints(mut turn: Turn) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        let _ = black_box(turn.poll(&mut cx));
    }
    black_box(turn.exhausted())
}

library_benchmark_group!(name = h2_turn; benchmarks = checkpoints);
main!(library_benchmark_groups = h2_turn);
