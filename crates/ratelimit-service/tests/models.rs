//! The service's models against local mode's and the references (cluster-store-transplant-plan.md,
//! question 2): the same limits and the same requests fed to each. Held to what was found
//! (decided): the service's fixed window and bucket decide every request as the references do;
//! its rolling window, counting in a thousandth of the window, and local mode's bucket, its
//! arrival time in 1/65,536 of a request's interval, admit as many over a run, though not always
//! the same requests, and those are printed. Neither side is changed to match the other.
//!
//! The service decides by `edgerush_cluster_synchronous::Rule`; local mode, the request path's
//! limiter, has the bucket alone; the references (`edgerush_ratelimit::reference`) are the
//! slow, obvious models of docs/06 §3. A service fixed window is on a phase of its own for each
//! key, so the reference, aligned to the clock, is asked at times shifted by that phase.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::table::mix;
use edgerush_cluster::{Time, ms};
use edgerush_cluster_synchronous::{Rule, Value};
use edgerush_ratelimit::local::{Decision, Handles, Local};
use edgerush_ratelimit::reference::{Alignment, FixedWindow, Model, RollingWindow, TokenBucket};
use edgerush_ratelimit::service::Contract;
use std::hash::RandomState;
use std::num::NonZeroU64;

/// A limit: `per` every `window`, `burst` at once for a bucket.
#[derive(Clone, Copy, Debug)]
struct Limit {
    per: u64,
    window: Time,
    burst: u64,
}

const LIMITS: [Limit; 4] = [
    Limit {
        per: 10,
        window: ms(1_000),
        burst: 10,
    },
    Limit {
        per: 10,
        window: ms(1_000),
        burst: 1,
    },
    Limit {
        per: 100,
        window: ms(1_000),
        burst: 20,
    },
    Limit {
        per: 3,
        window: ms(500),
        burst: 3,
    },
];

/// How long each run of requests lasts, in windows.
const WINDOWS: u64 = 20;

/// Times of requests under `limit`, from 1 s on: steady at `rate` times the limit, each a
/// little early or late by a generator of its own, or in bursts of `burst` every window.
fn arrivals(limit: Limit, pattern: Pattern, seed: u64) -> Vec<Time> {
    let start = ms(1_000);
    let end = start + limit.window * WINDOWS;
    let mut state = seed | 1;
    let mut jitter = |most: Time| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % most.max(1)
    };
    let mut times = Vec::new();
    match pattern {
        Pattern::Steady(tenths) => {
            let gap = limit.window * 10 / (limit.per * tenths).max(1);
            let mut at = start;
            while at < end {
                times.push(at + jitter(gap / 2));
                at += gap;
            }
        }
        Pattern::Bursts(each) => {
            let mut at = start;
            while at < end {
                let spread = jitter(limit.window);
                times.extend((0..each).map(|n| at + spread + n));
                at += limit.window;
            }
        }
    }
    times.sort_unstable();
    times
}

#[derive(Clone, Copy, Debug)]
enum Pattern {
    /// Requests at this many tenths of the limit's rate.
    Steady(u64),
    /// This many requests at once, every window.
    Bursts(u64),
}

const PATTERNS: [Pattern; 7] = [
    Pattern::Steady(5),
    Pattern::Steady(10),
    Pattern::Steady(15),
    Pattern::Steady(30),
    Pattern::Steady(100),
    Pattern::Bursts(2),
    Pattern::Bursts(50),
];

fn nonzero(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

/// What each decided, request by request.
struct Run {
    name: &'static str,
    admitted: Vec<bool>,
}

/// How many a run admitted.
fn admitted(run: &Run) -> usize {
    run.admitted.iter().filter(|admitted| **admitted).count()
}

/// Where two runs of the same requests first differ, and by how many admissions in all.
fn compare(times: &[Time], first: &Run, second: &Run) -> Option<String> {
    let at = first
        .admitted
        .iter()
        .zip(&second.admitted)
        .position(|(one, other)| one != other)?;
    Some(format!(
        "{} admitted {}, {} {} of {}; first apart at request {at} ({} ms in)",
        first.name,
        admitted(first),
        second.name,
        admitted(second),
        times.len(),
        (times[at] - times[0]) / ms(1),
    ))
}

const KEY: u64 = 7;

fn service(rule: Rule, times: &[Time]) -> Run {
    let mut value = Value::default();
    Run {
        name: "service",
        admitted: times
            .iter()
            .map(|at| rule.admit(KEY, &mut value, *at).is_some())
            .collect(),
    }
}

fn reference(name: &'static str, mut model: impl Model, times: &[Time], shift: Time) -> Run {
    Run {
        name,
        admitted: times.iter().map(|at| model.decide(at + shift)).collect(),
    }
}

fn local(limit: Limit, times: &[Time]) -> Run {
    let store = Local::new(RandomState::new(), 1);
    let mut handles = Handles::new(RandomState::new());
    let contract = Contract {
        limit: nonzero(limit.per),
        window: nonzero(limit.window),
        burst: nonzero(limit.burst),
    };
    Run {
        name: "local",
        admitted: times
            .iter()
            .map(|at| {
                matches!(
                    store.decide(&mut handles, &[(b"key", contract)], *at),
                    Decision::Admit
                )
            })
            .collect(),
    }
}

/// Every limit under every pattern through each model: the service's fixed window and bucket as
/// the references decide, request by request; its rolling window and local mode's bucket
/// admitting as many as the references over each run, the requests they differ on printed.
#[test]
fn the_services_models_decide_as_the_references_and_local_mode_admits_as_many() {
    let mut lines = Vec::new();
    let mut wrong = Vec::new();
    for limit in LIMITS {
        for pattern in PATTERNS {
            for seed in 1..=3 {
                let times = arrivals(limit, pattern, seed);
                let case = format!("{limit:?} {pattern:?} seed {seed}");
                // The bucket: the service's, local mode's and the reference's.
                let bucket = Rule::Bucket {
                    per: limit.per,
                    period: limit.window,
                    burst: limit.burst,
                };
                let served = service(bucket, &times);
                let local = local(limit, &times);
                let exact = reference(
                    "reference",
                    TokenBucket::new(
                        nonzero(limit.per),
                        nonzero(limit.window),
                        nonzero(limit.burst),
                    ),
                    &times,
                    0,
                );
                if let Some(apart) = compare(&times, &served, &exact) {
                    wrong.push(format!("bucket {case}: {apart}"));
                }
                if let Some(apart) = compare(&times, &local, &exact) {
                    if admitted(&local) == admitted(&exact) {
                        lines.push(format!("bucket {case}: {apart}"));
                    } else {
                        wrong.push(format!("bucket {case}: {apart}"));
                    }
                }
                // The fixed window, the reference shifted by the service's phase for the key.
                let window = Rule::Window {
                    per: limit.per,
                    window: limit.window,
                };
                let phase = mix(KEY) % limit.window;
                let served = service(window, &times);
                let exact = reference(
                    "reference",
                    FixedWindow::new(nonzero(limit.per), nonzero(limit.window), Alignment::Clock),
                    &times,
                    phase,
                );
                if let Some(apart) = compare(&times, &served, &exact) {
                    wrong.push(format!("fixed window {case}: {apart}"));
                }
                // The rolling window: the service's in a thousand slots, the reference exact.
                let rolling = Rule::Rolling {
                    per: limit.per,
                    window: limit.window,
                };
                let served = service(rolling, &times);
                let exact = reference(
                    "reference",
                    RollingWindow::new(nonzero(limit.per), nonzero(limit.window)),
                    &times,
                    0,
                );
                if let Some(apart) = compare(&times, &served, &exact) {
                    if admitted(&served) == admitted(&exact) {
                        lines.push(format!("rolling window {case}: {apart}"));
                    } else {
                        wrong.push(format!("rolling window {case}: {apart}"));
                    }
                }
            }
        }
    }
    eprintln!(
        "{} runs admitting as many, some requests apart:\n{}",
        lines.len(),
        lines.join("\n")
    );
    assert!(wrong.is_empty(), "{wrong:#?}");
}
