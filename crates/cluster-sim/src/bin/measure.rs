//! Runs every scenario over a number of seeds and prints what each cost, as a table.
//!
//! `measure [seeds] [shards] [forwarding]` — 5 seeds, 256 shards and no forwarding unless said.

use edgerush_cluster::{Time, ms};
use edgerush_cluster_sim::client::Multi;
use edgerush_cluster_sim::scenario::{self, Report, Shape, Trouble};

fn millis(time: Time) -> String {
    #[allow(clippy::cast_precision_loss, reason = "a time printed in milliseconds")]
    let shown = time as f64 / 1e6;
    format!("{shown:.1}")
}

fn main() {
    let mut args = std::env::args().skip(1);
    let seeds: u64 = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(5);
    let shards: u16 = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(256);
    let forwarding = args.next().is_some_and(|arg| arg == "forwarding");
    let shape = Shape {
        shards,
        forwarding,
        ..Shape::defaults()
    };
    let five = Shape { pods: 5, ..shape };
    let seven = Shape { pods: 7, ..shape };
    let forwarding_on = Shape {
        forwarding: true,
        ..shape
    };
    // A million keys over 256 shards.
    let many = Shape {
        preload: 1_000_000 / u64::from(shards.max(1)),
        ..shape
    };
    type Run = Box<dyn Fn(u64) -> Report>;
    let runs: Vec<Run> = vec![
        Box::new(move |seed| scenario::steady(seed, shape)),
        Box::new(move |seed| scenario::crash_follower(seed, shape)),
        Box::new(move |seed| scenario::crash_leader(seed, shape)),
        Box::new(move |seed| scenario::isolate_follower(seed, shape, ms(3_000))),
        Box::new(move |seed| scenario::isolate_leader(seed, shape, ms(3_000))),
        Box::new(move |seed| scenario::one_way(seed, shape, ms(3_000))),
        Box::new(move |seed| scenario::pods_apart(seed, shape, ms(1_000))),
        Box::new(move |seed| scenario::rollout(seed, shape)),
        Box::new(move |seed| scenario::rollout(seed, five)),
        Box::new(move |seed| scenario::scale_up(seed, shape, 5)),
        Box::new(move |seed| scenario::scale_down(seed, five, 3)),
        Box::new(move |seed| scenario::scale_down_killed(seed, five, 3)),
        Box::new(move |seed| scenario::majority_lost(seed, shape)),
        Box::new(move |seed| scenario::lossy(seed, shape, 20_000, ms(2))),
        Box::new(move |seed| scenario::clients_cut_from_pod(seed, shape, ms(3_000))),
        Box::new(move |seed| scenario::clients_cut_from_pod(seed, forwarding_on, ms(3_000))),
        Box::new(move |seed| scenario::rollout(seed, many)),
        Box::new(move |seed| scenario::crash_follower(seed, many)),
        Box::new(move |seed| scenario::scale_up(seed, many, 5)),
        Box::new(move |seed| scenario::rollout_with_crash(seed, shape)),
        Box::new(move |seed| scenario::rollout_lossy(seed, shape)),
        Box::new(move |seed| scenario::scale_down(seed, five, 2)),
        Box::new(move |seed| scenario::scale_down(seed, five, 1)),
        Box::new(move |seed| scenario::multi_key(seed, shape, Multi::Split, Trouble::None)),
        Box::new(move |seed| scenario::multi_key(seed, shape, Multi::Gather, Trouble::None)),
        Box::new(move |seed| scenario::multi_key(seed, shape, Multi::Split, Trouble::Crash)),
        Box::new(move |seed| scenario::multi_key(seed, shape, Multi::Gather, Trouble::Crash)),
        Box::new(move |seed| scenario::multi_key(seed, shape, Multi::Split, Trouble::ClientsCut)),
        Box::new(move |seed| scenario::multi_key(seed, shape, Multi::Gather, Trouble::ClientsCut)),
        Box::new(move |seed| {
            scenario::multi_key(seed, forwarding_on, Multi::Split, Trouble::Crash)
        }),
        Box::new(move |seed| {
            scenario::multi_key(seed, forwarding_on, Multi::Split, Trouble::ClientsCut)
        }),
        Box::new(move |seed| scenario::scale_down_at_once(seed, five, 2)),
        Box::new(move |seed| scenario::scale_down_at_once(seed, five, 1)),
        Box::new(move |seed| scenario::scale_down_at_once(seed, seven, 3)),
        Box::new(move |seed| scenario::rollout_surge(seed, five, 2)),
        Box::new(move |seed| scenario::zone_cut(seed, five, 2, ms(3_000))),
        Box::new(move |seed| scenario::zone_cut(seed, seven, 3, ms(3_000))),
        Box::new(move |seed| scenario::frozen(seed, shape, false, ms(300))),
        Box::new(move |seed| scenario::frozen(seed, shape, false, ms(2_000))),
        Box::new(move |seed| scenario::frozen(seed, shape, true, ms(300))),
        Box::new(move |seed| scenario::frozen(seed, shape, true, ms(2_000))),
        Box::new(move |seed| scenario::drain_cut_off(seed, shape, ms(3_000))),
    ];
    println!(
        "| scenario | seed | sent | counted | refused | p50 ms | p99 ms | p99.9 ms | lost | overlap ms (sum/longest) | unserved ms (sum/longest) | terms | took ms | settled | violations | overcount | orphaned | MB sent |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for run in &runs {
        for seed in 1..=seeds {
            let report = run(seed);
            println!(
                "| {} | {seed} | {} | {} | {} | {} | {} | {} | {} | {}/{} | {}/{} | {} | {} | {} | {} | {} | {} | {:.1} |",
                report.name,
                report.sent,
                report.counted,
                report.refused,
                millis(report.p50),
                millis(report.p99),
                millis(report.p999),
                report.lost,
                millis(report.overlap_total),
                millis(report.overlap_longest),
                millis(report.unserved_total),
                millis(report.unserved_longest),
                report.terms,
                report.took.map_or_else(|| "-".to_owned(), millis),
                report.settled,
                report.violations.len(),
                report.overcount,
                report.orphaned,
                report.bytes as f64 / 1e6,
            );
        }
    }
}
