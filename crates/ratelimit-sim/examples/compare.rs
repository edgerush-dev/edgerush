//! Ways of limiting one key across pods, side by side over many seeds, printed as Markdown:
//! the baselines (one bucket for the fleet, which keeps the contract exactly; pods sharing
//! nothing; pods with a share each) and the ways of sharing counts: ownership, per-pod
//! counters, and a rate-limiting service of its own.
//!
//! `cargo run --release -p edgerush-ratelimit-sim --example compare [seeds] [lost in 1,000]
//! [none|owner-lost|partition|restart|owner-restart|rolling] [delay in µs] [pods, as 2,4,8]
//! [contract, as limit/window in ms/burst] [clock skew in µs] [service cycle in ms]
//! [debt cap, % of a refill time]`; `ROWS=service;central` keeps only matching rows.
//!
//! The workloads scale with the contract — its limit a window, three times it, its whole
//! bucket at once — and a run lasts ten windows.

use edgerush_ratelimit_sim::{
    Approach, Bound, Contract, Counting, Excess, Fault, Network, OnOwnerChange, Replicate, Run,
    Serving, Shape, Sharing, Workload, run,
};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

fn owned(cycle: u64, excess: Excess) -> Approach {
    Approach::Ownership(Sharing {
        cycle,
        excess,
        ledger: false,
        bound: Bound::Unbounded,
        on_owner_change: OnOwnerChange::Drop,
    })
}

fn ledger(cycle: u64, excess: Excess) -> Approach {
    Approach::Ownership(Sharing {
        cycle,
        excess,
        ledger: true,
        bound: Bound::Unbounded,
        on_owner_change: OnOwnerChange::Drop,
    })
}

fn bounded(cycle: u64, excess: Excess, bound: Bound) -> Approach {
    Approach::Ownership(Sharing {
        cycle,
        excess,
        ledger: true,
        bound,
        on_owner_change: OnOwnerChange::Drop,
    })
}

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(200);
    let loss_per_mille: u64 = std::env::args()
        .nth(2)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(0);
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    let terms: Vec<u64> = std::env::args()
        .nth(6)
        .map(|arg| arg.split('/').filter_map(|n| n.parse().ok()).collect())
        .unwrap_or_default();
    let (limit, window_ms, burst) = match terms.as_slice() {
        [limit, window, burst] => (*limit, *window, *burst),
        _ => (100, 1_000, 100),
    };
    let contract = Contract {
        limit: n(limit),
        window: n(window_ms * MS),
        burst: n(burst),
    };
    // Ten windows a run; faults strike at its middle, a partition from 3 to 7 tenths.
    let span = 10 * contract.window.get();
    // Debt is owed up to one bucket's refill time.
    // As a percentage of that, if given: how sensitive the results are to the cap.
    let cap_percent: u64 = std::env::args()
        .nth(9)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(100);
    let cap = burst * contract.window.get() / limit.max(1) * cap_percent / 100;
    // `owner-lost`: the pod owning the key goes away half way through each run; with
    // counters, which have no owner, the first pod.
    // `partition`: half the pods are cut off from the others from 3 s to 7 s.
    // `restart`: the last pod restarts half way through; `owner-restart`: the owner does —
    // with counters the first pod, with the service the service pod owning the key.
    let fault_name = std::env::args().nth(3).unwrap_or_default();
    let fault = |pods: usize| match fault_name.as_str() {
        "owner-lost" => Fault::OwnerLost { at: span / 2 },
        "partition" => Fault::Partition {
            from: span * 3 / 10,
            until: span * 7 / 10,
            split: pods / 2,
        },
        "restart" => Fault::Restart {
            at: span / 2,
            pod: pods - 1,
        },
        "owner-restart" => Fault::OwnerRestart { at: span / 2 },
        // `rolling`: every pod restarts in turn between 3 and 7 tenths of the run.
        "rolling" => Fault::Rolling {
            from: span * 3 / 10,
            every: span * 4 / 10 / pods as u64,
        },
        _ => Fault::None,
    };
    // How long a message takes, give or take half as much again.
    let delay_us: u64 = std::env::args()
        .nth(4)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(1_000);

    let network = Network {
        loss_per_mille,
        ..Network::reliable(delay_us * 1_000, delay_us * 500)
    };
    let mut approaches = [
        ("central", Approach::Central),
        ("sharing nothing", Approach::Independent),
        ("a share each", Approach::Split),
        ("ownership, drain, 100 ms", owned(100 * MS, Excess::Drain)),
        ("ownership, drain, 10 ms", owned(10 * MS, Excess::Drain)),
        ("ownership, debt, 100 ms", owned(100 * MS, Excess::Debt)),
        ("ownership, debt, 10 ms", owned(10 * MS, Excess::Debt)),
        (
            "ownership, debt up to 1 s, 100 ms",
            owned(100 * MS, Excess::DebtUpTo(cap)),
        ),
        (
            "ownership, debt up to 1 s, 10 ms",
            owned(10 * MS, Excess::DebtUpTo(cap)),
        ),
        ("ledger, drain, 100 ms", ledger(100 * MS, Excess::Drain)),
        ("ledger, drain, 10 ms", ledger(10 * MS, Excess::Drain)),
        (
            "ledger, debt up to 1 s, 100 ms",
            ledger(100 * MS, Excess::DebtUpTo(cap)),
        ),
        (
            "ledger, debt up to 1 s, 10 ms",
            ledger(10 * MS, Excess::DebtUpTo(cap)),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, new pods bounded",
            bounded(10 * MS, Excess::DebtUpTo(cap), Bound::NewPods),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, every pod bounded",
            bounded(10 * MS, Excess::DebtUpTo(cap), Bound::Every),
        ),
        (
            "ledger, drain, 10 ms, every pod bounded",
            bounded(10 * MS, Excess::Drain, Bound::Every),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, every pod bounded, sent again to a new owner",
            Approach::Ownership(Sharing {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                ledger: true,
                bound: Bound::Every,
                on_owner_change: OnOwnerChange::SendAgain,
            }),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, every pod bounded carrying, sent again to a new owner",
            Approach::Ownership(Sharing {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                ledger: true,
                bound: Bound::EveryCarrying,
                on_owner_change: OnOwnerChange::SendAgain,
            }),
        ),
        (
            "counters, debt up to 1 s, 10 ms",
            Approach::Counters(Counting {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Unbounded,
            }),
        ),
        (
            "counters, debt up to 1 s, 10 ms, every pod bounded",
            Approach::Counters(Counting {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Every,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Unbounded,
                ask_when_cold: false,
                replicate: Replicate::None,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms, every pod bounded",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Every,
                ask_when_cold: false,
                replicate: Replicate::None,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 100 ms, every pod bounded",
            Approach::Service(Serving {
                cycle: 100 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Every,
                ask_when_cold: false,
                replicate: Replicate::None,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms, every pod bounded, the owner asked when cold",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Every,
                ask_when_cold: true,
                replicate: Replicate::None,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms, every pod bounded, the owner asked when cold, copied to a secondary every cycle",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Every,
                ask_when_cold: true,
                replicate: Replicate::EveryCycle,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms, every pod bounded, the owner asked when cold, copied to a secondary with every change",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Every,
                ask_when_cold: true,
                replicate: Replicate::EveryChange,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms, every pod bounded carrying, the owner asked when cold, copied to a secondary with every change",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::EveryCarrying,
                ask_when_cold: true,
                replicate: Replicate::EveryChange,
                skew: 0,
            }),
        ),
        (
            "service, debt up to 1 s, 10 ms, every pod granted by the owner, the owner asked when cold, copied to a secondary with every change",
            Approach::Service(Serving {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(cap),
                bound: Bound::Granted,
                ask_when_cold: true,
                replicate: Replicate::EveryChange,
                skew: 0,
            }),
        ),
    ];
    // The data planes' clocks off the service's by up to this many µs, either way.
    let skew_us: u64 = std::env::args()
        .nth(7)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(0);
    // The service's cycle instead of the rows' own, if given, in ms.
    let service_cycle: Option<u64> = std::env::args().nth(8).and_then(|arg| arg.parse().ok());
    for (_, approach) in &mut approaches {
        if let Approach::Service(serving) = approach {
            serving.skew = skew_us * 1_000;
            if let Some(cycle) = service_cycle {
                serving.cycle = cycle * MS;
            }
        }
    }
    let workloads = [
        (
            "Within the contract, spread",
            Shape::Spread { per_window: limit },
            true,
        ),
        (
            "Within the contract, all on one pod",
            Shape::Skewed { per_window: limit },
            true,
        ),
        (
            "On the contract's edge, spread (three times the limit, kept to what one bucket admits)",
            Shape::Spread {
                per_window: 3 * limit,
            },
            true,
        ),
        (
            "On the contract's edge, all on one pod",
            Shape::Skewed {
                per_window: 3 * limit,
            },
            true,
        ),
        (
            "On the contract's edge, uneven (half on one pod, the rest spread)",
            Shape::Uneven {
                per_window: 3 * limit,
                hot: 50,
            },
            true,
        ),
        (
            "Three times the limit, spread",
            Shape::Spread {
                per_window: 3 * limit,
            },
            false,
        ),
        (
            "A whole bucket on every pod at once, every window",
            Shape::Bursts { each: burst },
            false,
        ),
        (
            "One pod at the limit, then such bursts",
            Shape::SkewedThenBursts {
                per_window: limit,
                each: burst,
            },
            false,
        ),
    ];
    let pods: Vec<usize> = std::env::args()
        .nth(5)
        .map(|arg| arg.split(',').filter_map(|n| n.parse().ok()).collect())
        .filter(|pods: &Vec<usize>| !pods.is_empty())
        .unwrap_or_else(|| vec![2, 4, 8]);

    println!(
        "Contract: {limit} every {window_ms} ms, burst {burst}. Runs of ten windows, {seeds} \
         seeds each; messages take \
         {delay} ms, give or take {jitter} ms, {loss_per_mille} in 1,000 lost. Each cell: \
         share of \
         requests refused (for conforming traffic every refusal is false) / admitted against \
         one bucket for the fleet / the most over the contract in any span, worst seed / \
         share of admitted requests the fleet's count never saw / share it counted twice / \
         messages a second for each pod (all a run sends, a service's answers among them, \
         over its pods and its length) / share of requests that waited for their \
         decision / how long they waited, on average. \
         Fault: {fault_name}.\n",
        delay = delay_us as f64 / 1_000.0,
        jitter = delay_us as f64 / 2_000.0,
    );
    for (title, shape, conforming) in workloads {
        println!("**{title}**\n");
        print!("| Approach |");
        for &p in &pods {
            print!(" {p} pods |");
        }
        println!();
        println!("|---|{}", "---|".repeat(pods.len()));
        // `ROWS=service;central` keeps only the rows whose name holds one of these.
        let rows = std::env::var("ROWS").unwrap_or_default();
        let wanted =
            |name: &str| rows.is_empty() || rows.split(';').any(|part| name.contains(part));
        for (name, approach) in approaches.iter().copied().filter(|(name, _)| wanted(name)) {
            print!("| {name} |");
            for &p in &pods {
                let (mut sent, mut refused, mut admitted, mut central, mut over) =
                    (0, 0, 0, 0, i64::MIN);
                let (mut lost, mut twice, mut messages) = (0, 0, 0);
                let (mut waited, mut waited_for) = (0, 0);
                for seed in 0..seeds {
                    let workload = Workload {
                        shape,
                        span,
                        conforming,
                    };
                    let this = Run {
                        contract,
                        pods: p,
                        workload,
                        approach,
                        network,
                        fault: fault(p),
                        seed,
                    };
                    let outcome = run(&this);
                    let baseline = run(&Run {
                        approach: Approach::Central,
                        ..this
                    });
                    sent += outcome.sent;
                    refused += outcome.refused;
                    admitted += outcome.admitted;
                    central += baseline.admitted;
                    over = over.max(outcome.worst_excess);
                    lost += outcome.lost;
                    twice += outcome.counted_twice;
                    messages += outcome.messages;
                    waited += outcome.waited;
                    waited_for += outcome.waited_for;
                }
                let refused = 100.0 * refused as f64 / sent.max(1) as f64;
                let ratio = admitted as f64 / central.max(1) as f64;
                let lost = 100.0 * lost as f64 / admitted.max(1) as f64;
                let twice = 100.0 * twice as f64 / admitted.max(1) as f64;
                // Messages a second for each pod, over the run.
                let seconds = span as f64 / SECOND as f64;
                let rate = messages as f64 / (seeds.max(1) * p as u64) as f64 / seconds;
                let wait = 100.0 * waited as f64 / sent.max(1) as f64;
                let mean = waited_for as f64 / waited.max(1) as f64 / MS as f64;
                print!(
                    " {refused:.1}% / {ratio:.2}× / {over} / {lost:.1}% / {twice:.1}% / {rate:.0} \
                     / {wait:.1}% / {mean:.1} ms |"
                );
            }
            println!();
        }
        println!();
    }
}
