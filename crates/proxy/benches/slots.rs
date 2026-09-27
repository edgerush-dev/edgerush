//! Instruction counts for running a request's future in a slot the worker lends, against
//! holding it inline ([14 §3](../../../docs/14-downstream-server.md)): the one is what every
//! HTTP/1 request now does, the other what the connection's own future did.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench slots`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::slots::Slots;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::future::Future;
use std::hint::black_box;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

/// Requests run, one after another, as on a busy connection.
const REQUESTS: u8 = 100;

/// A future as large as a request's, 3,840 bytes, whose state is made only once it runs:
/// what a move of it copies is all of that, whatever it holds when moved.
async fn request(seed: u8) -> u8 {
    let state = [seed; 3_800];
    Yield(false).await;
    black_box(&state)[usize::from(seed) % 3_800]
}

/// Pending once, then ready: a request that waits for something, once.
struct Yield(bool);

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            Poll::Pending
        }
    }
}

fn run<F: Future<Output = u8>>(mut future: Pin<&mut F>) -> u8 {
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(done) = future.as_mut().poll(&mut context) {
            return done;
        }
    }
}

// Each request's future made in a slot of the worker's, run, and dropped where it stands.
#[library_benchmark]
fn in_a_slot() -> u32 {
    let slots = Slots::default();
    let mut sum = 0_u32;
    for seed in 0..REQUESTS {
        let mut slot = slots.start(|| request(seed));
        sum += u32::from(run(Pin::new(&mut slot)));
    }
    sum
}

// The same, each held inline, as in the connection's future.
#[library_benchmark]
fn inline() -> u32 {
    let mut sum = 0_u32;
    for seed in 0..REQUESTS {
        let future = pin!(request(seed));
        sum += u32::from(run(future));
    }
    sum
}

library_benchmark_group!(
    name = slots;
    benchmarks = in_a_slot, inline
);
main!(library_benchmark_groups = slots);
