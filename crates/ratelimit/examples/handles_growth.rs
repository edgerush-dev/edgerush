//! What one decision costs at worst as a worker meets key after key (GitHub issue #111): a
//! worker's handles, and the registry's shards, grow as they fill, and a decision that
//! makes one grow pays for the whole move. KEYS new keys (2,000,000 unless said) decided
//! one after another through one worker's handles, each timed; prints the slowest ten with
//! the handles held then, and the total. Release builds only:
//! `cargo run --release -p edgerush-ratelimit --example handles_growth -- [KEYS]`.

use edgerush_ratelimit::local::{Contract, Handles, Local};
use std::hash::RandomState;
use std::num::NonZeroU64;
use std::time::Instant;

fn main() {
    let keys: usize = std::env::args()
        .nth(1)
        .and_then(|keys| keys.parse().ok())
        .unwrap_or(2_000_000);
    let nz = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    // Ten an hour, a bucket of a million: no key is ever full, so none is refused.
    let contract = Contract {
        limit: nz(10),
        window: nz(3_600_000_000_000),
        burst: nz(1_000_000),
    };
    let hasher = RandomState::new();
    let local = Local::new(hasher.clone(), 1);
    let mut handles = Handles::new(hasher);
    let names: Vec<Vec<u8>> = (0..keys)
        .map(|i| format!("client-{i}").into_bytes())
        .collect();
    let mut slowest: Vec<(u128, usize)> = Vec::with_capacity(keys);
    let began = Instant::now();
    for (at, name) in names.iter().enumerate() {
        let one = Instant::now();
        let _decision = local.decide(&mut handles, &[(name, contract)], at as u64);
        slowest.push((one.elapsed().as_nanos(), handles.len()));
    }
    let took = began.elapsed();
    slowest.sort_unstable_by_key(|slow| std::cmp::Reverse(slow.0));
    println!(
        "{keys} keys in {:.0} ms; the slowest decisions:",
        took.as_secs_f64() * 1e3
    );
    for (nanos, held) in slowest.iter().take(10) {
        println!("  {:>8.2} ms with {held} handles", *nanos as f64 / 1e6);
    }
}
