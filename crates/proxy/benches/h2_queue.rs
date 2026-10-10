//! Ready-queue work for completed and refused HTTP/2 requests. Run on Linux with
//! `cargo bench -p edgerush-proxy --bench h2_queue` (valgrind required).

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

// Exercise the private queue without adding a public benchmark API to the proxy.
#[path = "../src/downstream/h2/queue.rs"]
#[cfg_attr(
    test,
    allow(
        dead_code,
        unused_imports,
        reason = "the included module's unit tests run in the library, not this benchmark harness"
    )
)]
mod queue;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use queue::Queue;
use std::future::{Ready, ready};
use std::hint::black_box;
use std::task::Waker;

const ENTRIES: usize = 100;
const BATCH: usize = 16;

fn waiting() -> Queue<usize, Ready<()>> {
    let mut queue = Queue::new();
    for entry in 0..ENTRIES {
        queue.push(entry);
    }
    queue
}

// Construction and final destruction are outside the measured region; completing the
// entries, releasing their slots and enforcing the batch allowance are measured.
#[library_benchmark]
#[bench::hundred(waiting())]
fn completed(mut queue: Queue<usize, Ready<()>>) -> (Queue<usize, Ready<()>>, usize) {
    let mut examined = 0;
    for _ in 0..ENTRIES.div_ceil(BATCH) {
        examined += queue.run_while(
            Waker::noop(),
            black_box(BATCH),
            |entry| {
                black_box(entry);
                Some(ready(()))
            },
            || true,
        );
    }
    (queue, black_box(examined))
}

#[library_benchmark]
#[bench::hundred(waiting())]
fn refused(mut queue: Queue<usize, Ready<()>>) -> (Queue<usize, Ready<()>>, usize) {
    let mut examined = 0;
    for _ in 0..ENTRIES.div_ceil(BATCH) {
        examined += queue.run_while(
            Waker::noop(),
            black_box(BATCH),
            |entry| {
                black_box(entry);
                None
            },
            || true,
        );
    }
    (queue, black_box(examined))
}

#[library_benchmark]
#[bench::hundred(waiting())]
fn deadline(mut queue: Queue<usize, Ready<()>>) -> (Queue<usize, Ready<()>>, usize) {
    let budget =
        h2::TurnBudget::new(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let examined = queue.run_while(
        Waker::noop(),
        black_box(BATCH),
        |_| Some(ready(())),
        || !budget.expired(),
    );
    (queue, black_box(examined))
}

library_benchmark_group!(name = h2_queue; benchmarks = completed, refused, deadline);
main!(library_benchmark_groups = h2_queue);
