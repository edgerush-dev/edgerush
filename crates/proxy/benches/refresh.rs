//! Instruction counts for a worker's balancing state brought up to a new config
//! ([03 §6](../../../docs/03-data-plane.md)): what the first request on each worker pays
//! after a reload, on its own thread, while every connection of the worker waits. At 10,
//! 1,000 and 10,000 upstreams of four endpoints each, when nothing of the upstreams changed
//! and when one upstream's endpoints did.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench refresh`, see the repository
//! README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_config::{Compiled, Config, compile};
use edgerush_proxy::upstream::balancing::Balancing;
use edgerush_proxy::upstream::destination::{Destinations, Keys};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::fmt::Write;
use std::hint::black_box;

/// A config of `upstreams` upstreams of four endpoints each, the one at `moved` on four
/// other addresses.
fn config(upstreams: usize, moved: Option<usize>) -> Compiled {
    let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
    for upstream in 0..upstreams {
        let port = if moved == Some(upstream) { 2 } else { 1 };
        let (high, low) = (upstream / 256, upstream % 256);
        let endpoints: Vec<String> = (1..=4)
            .map(|host| format!("\"10.{high}.{low}.{host}:{port}\""))
            .collect();
        let _written = writeln!(
            yaml,
            "  u{upstream}: {{ load_balancer: p2c, endpoints: [{}] }}",
            endpoints.join(", ")
        );
    }
    let config: Config = serde_saphyr::from_str(&yaml).expect("valid YAML");
    compile(&config).expect("valid config")
}

/// A worker's balancing state made for one config, and the next config to bring it up to.
pub struct Reloaded {
    balancing: Balancing,
    config: Compiled,
    destinations: Destinations,
}

fn reloaded(upstreams: usize, one_moved: bool) -> Reloaded {
    let keys = Keys::default();
    let before = config(upstreams, None);
    let was = Destinations::reconcile_plain(&before, &Destinations::default(), &keys);
    let mut balancing = Balancing::default();
    balancing.refresh(1, &before, &was, &[]);
    let config = config(upstreams, one_moved.then_some(0));
    let destinations = Destinations::reconcile_plain(&config, &was, &keys);
    Reloaded {
        balancing,
        config,
        destinations,
    }
}

#[library_benchmark]
#[bench::unchanged_10(reloaded(10, false))]
#[bench::unchanged_1000(reloaded(1_000, false))]
#[bench::unchanged_10000(reloaded(10_000, false))]
#[bench::one_moved_1000(reloaded(1_000, true))]
#[bench::one_moved_10000(reloaded(10_000, true))]
fn refresh(mut reloaded: Reloaded) -> Reloaded {
    reloaded
        .balancing
        .refresh(2, &reloaded.config, &reloaded.destinations, &[]);
    black_box(reloaded)
}

library_benchmark_group!(name = reload; benchmarks = refresh);

main!(library_benchmark_groups = reload);
