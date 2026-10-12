//! The same clients through every admission model at "100 a second", printed as Markdown
//! tables: how many requests each model admits, and the most it admits in any one second.
//!
//! `cargo run -p edgerush-ratelimit --example timelines`

use edgerush_ratelimit::reference::{
    Alignment, ApproximateRollingWindow, FixedWindow, Model, RollingWindow, TokenBucket,
};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
/// Clients start part-way through a second of the clock, as real ones do.
const START: u64 = 10 * SECOND + 300 * MS;

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
}

fn models() -> Vec<(&'static str, Box<dyn Model>)> {
    let (limit, window) = (nz(100), nz(SECOND));
    let bucket =
        |capacity| Box::new(TokenBucket::new(limit, window, nz(capacity))) as Box<dyn Model>;
    vec![
        (
            "Fixed, clock",
            Box::new(FixedWindow::new(limit, window, Alignment::Clock)),
        ),
        (
            "Fixed, first request",
            Box::new(FixedWindow::new(limit, window, Alignment::FirstRequest)),
        ),
        ("Rolling", Box::new(RollingWindow::new(limit, window))),
        (
            "Approx. rolling",
            Box::new(ApproximateRollingWindow::new(limit, window)),
        ),
        ("Bucket 1", bucket(1)),
        ("Bucket 20", bucket(20)),
        ("Bucket 100", bucket(100)),
        ("Bucket 120", bucket(120)),
    ]
}

/// `count` requests together at each of `times`.
fn bunches(times: impl IntoIterator<Item = u64>, count: usize) -> Vec<u64> {
    times
        .into_iter()
        .flat_map(|at| std::iter::repeat_n(at, count))
        .collect()
}

/// One request every `interval` for `span`.
fn evenly(interval: u64, span: u64) -> Vec<u64> {
    (0..span / interval).map(|i| START + i * interval).collect()
}

fn clients() -> Vec<(&'static str, Vec<u64>)> {
    vec![
        ("150 at once", bunches([START], 150)),
        (
            "Browser: 6 at once every 100 ms, 2 s (60/s)",
            bunches((0..20).map(|i| START + i * 100 * MS), 6),
        ),
        (
            "Exactly 100/s, one every 10 ms, 5 s",
            evenly(10 * MS, 5 * SECOND),
        ),
        ("100/s in pairs 2 ms apart every 20 ms, 5 s", {
            (0..250)
                .flat_map(|i| [START + i * 20 * MS, START + i * 20 * MS + 2 * MS])
                .collect()
        }),
        (
            "100/s as 100 at once every second, 5 s",
            bunches((0..5).map(|i| START + i * SECOND), 100),
        ),
        (
            "150/s, one every 6.67 ms, 5 s",
            (0..750).map(|i| START + i * 20 * MS / 3).collect(),
        ),
        ("100 at 10.999 s and 100 at 11.000 s", {
            let mut times = bunches([11 * SECOND - MS], 100);
            times.extend(bunches([11 * SECOND], 100));
            times
        }),
        ("1 at 10.000 s, 99 at 10.999 s, 100 at 11.000 s", {
            let mut times = vec![10 * SECOND];
            times.extend(bunches([11 * SECOND - MS], 99));
            times.extend(bunches([11 * SECOND], 100));
            times
        }),
        (
            "150 at once, then 150 at once a minute later",
            bunches([START, START + 60 * SECOND], 150),
        ),
    ]
}

/// The most admissions inside any half-open second.
fn most_in_a_second(admitted: &[u64]) -> usize {
    let mut most = 0;
    let mut first = 0;
    for last in 0..admitted.len() {
        while admitted[last] - admitted[first] >= SECOND {
            first += 1;
        }
        most = most.max(last - first + 1);
    }
    most
}

fn main() {
    let names: Vec<&str> = models().iter().map(|(name, _)| *name).collect();
    let header = format!("| Client | Sent | {} |", names.join(" | "));
    let rule = format!("|---|---|{}", "---|".repeat(names.len()));
    let mut admitted_rows = Vec::new();
    let mut busiest_rows = Vec::new();
    for (client, times) in clients() {
        let mut admitted_cells = Vec::new();
        let mut busiest_cells = Vec::new();
        for (_, mut model) in models() {
            let admitted: Vec<u64> = times
                .iter()
                .copied()
                .filter(|&at| model.decide(at))
                .collect();
            admitted_cells.push(admitted.len().to_string());
            busiest_cells.push(most_in_a_second(&admitted).to_string());
        }
        admitted_rows.push(format!(
            "| {client} | {} | {} |",
            times.len(),
            admitted_cells.join(" | ")
        ));
        busiest_rows.push(format!(
            "| {client} | {} | {} |",
            times.len(),
            busiest_cells.join(" | ")
        ));
    }
    println!("Admitted, at 100 a second\n\n{header}\n{rule}");
    admitted_rows.iter().for_each(|row| println!("{row}"));
    println!("\nMost admitted in any one second\n\n{header}\n{rule}");
    busiest_rows.iter().for_each(|row| println!("{row}"));
}
