//! The asynchronous mode on the store under workloads: data planes of many workers deciding
//! bursts, skewed and moving traffic and requests under two limits from the entries they share,
//! held to what the contract allows — its rate over the run and 5% more, and a bucket for each
//! data plane — and every admission counted once by its key's owner.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::{Config, Time, ms};
use edgerush_cluster_sim::{
    Asynchronous, Beside, Fleet, Link, Rng, Shape, Sim, Workload, one_bucket_admits, worst_excess,
};
use edgerush_ratelimit::service::Contract;
use std::num::NonZeroU64;

type World = Sim<Asynchronous>;

const SECOND: u64 = 1_000_000_000;

fn limit(limit: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(SECOND),
        burst: n(burst),
    }
}

/// What a run of `span` on `planes` data planes may admit under `contract` (06 §1): its rate
/// over the whole run and 5% more, and a bucket for each data plane.
fn allowed(contract: &Contract, span: u64, planes: u64) -> u64 {
    let rate = contract.limit.get() * span / contract.window.get();
    rate + rate / 20 + planes * contract.burst.get()
}

/// Links taking a millisecond, give or take half of one.
fn link(jitter: Time) -> Link {
    Link {
        delay: ms(1),
        jitter,
        ..Link::lan()
    }
}

/// 100 a second with a bucket of 100, `planes` data planes of `workers` workers, `workload`
/// landing on them, the store's pods a millisecond away.
fn fleet(planes: usize, workers: usize, workload: Workload, seed: u64) -> Fleet {
    Fleet {
        planes,
        workers,
        contract: limit(100, 100),
        beside: None,
        workload,
        seed,
        scheduled: true,
        poll_quiet: false,
    }
}

fn workload(shape: Shape, span: u64) -> Workload {
    Workload {
        shape,
        span,
        conforming: false,
    }
}

/// What a run did.
#[derive(Debug)]
struct Run {
    /// Requests admitted under each of its keys, the run's first.
    admitted: Vec<u64>,
    /// Requests refused.
    refused: u64,
    /// Batches the data planes sent.
    batches: u64,
    /// The most any key admitted over its own contract, in any span: under the run's key first.
    worst_excess: Vec<i64>,
    /// Hits each key's owner applied, each once: as many as were admitted, once the run is over.
    counted: Vec<u64>,
    /// Requests whose decision waited for a data plane's participant, and their waits in all.
    waited: (u64, Time),
    /// What one bucket holding the run's contract would admit of the same traffic.
    one_bucket: u64,
}

/// What can happen while a run goes.
#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    /// The run's key's owner crashes at `at` after the traffic starts.
    OwnerLost {
        at: Time,
    },
    /// Data plane `plane`'s participant is held for `length` at `at` after the traffic starts.
    Pause {
        at: Time,
        length: Time,
        plane: usize,
    },
}

/// `fleet` run on three pods, every link `link`, with `fault`; every request decided, and
/// measured once every batch has been applied.
fn run(fleet: &Fleet, link: Link, fault: Fault) -> Run {
    let mut config = Config::defaults();
    config.shards = 64;
    let mut world = World::new(fleet.seed, config);
    world.start_cluster(3);
    assert!(
        world.run_while_not(ms(10_000), World::settled).is_some(),
        "never formed"
    );
    world.set_default_link(link);
    let keys = world.add_fleet(fleet);
    let start = world.now();
    match fault {
        Fault::None => {}
        Fault::OwnerLost { at } => {
            world.run_until(start + at);
            let owner = world.owner_of(keys[0]).expect("an owner");
            world.crash(owner);
        }
        Fault::Pause { at, length, plane } => {
            world.run_until(start + at);
            let id = world.plane_ids()[plane];
            world.pause_plane(id, length);
        }
    }
    let arrivals: Vec<u64> = fleet
        .workload
        .arrivals(
            &fleet.contract,
            fleet.planes * fleet.workers,
            &mut Rng::new(fleet.seed),
        )
        .iter()
        .map(|arrival| arrival.at)
        .collect();
    // Half a second after the last arrival, for every batch to be applied: before a key quiet
    // for a refill time, and full again, is forgotten, and its count with it.
    let last = arrivals.last().copied().unwrap_or(0);
    world.run_until(start + last + SECOND / 2);
    assert_eq!(
        world.decided().0,
        arrivals.len() as u64,
        "every request decided"
    );
    let admissions = world.admissions();
    let times = |key| admissions.get(&key).cloned().unwrap_or_default();
    let contracts = world_contracts(fleet, &keys);
    Run {
        admitted: keys.iter().map(|key| times(*key).len() as u64).collect(),
        refused: world.decided().1,
        batches: world.batches(),
        worst_excess: keys
            .iter()
            .zip(&contracts)
            .map(|(key, contract)| worst_excess(contract, &times(*key)))
            .collect(),
        counted: keys
            .iter()
            .map(|key| world.counted(*key).unwrap_or(0))
            .collect(),
        waited: world.waited(),
        one_bucket: one_bucket_admits(&fleet.contract, &arrivals),
    }
}

/// The contract of each of a run's keys, in the order `add_fleet` gave them.
fn world_contracts(fleet: &Fleet, keys: &[u64]) -> Vec<Contract> {
    keys.iter()
        .enumerate()
        .map(|(n, _)| match (n, fleet.beside) {
            (0, _) | (_, None) => fleet.contract,
            (_, Some(Beside::Global(other) | Beside::Routes(other))) => other,
        })
        .collect()
}

/// Every admission counted once by its key's owner.
fn counted_once(run: &Run, said: &str) {
    assert_eq!(
        run.counted, run.admitted,
        "{said}: counted against admitted"
    );
}

/// A data plane decides alike whatever its workers — the same traffic on 1, 4 and 8 workers a
/// data plane comes out the same — bursts on every worker of 4 data planes are held to what is
/// allowed, and a conforming client moving between a data plane's workers is never refused, its
/// participant held for 100 ms across the move or not, the move holding up none of its
/// requests. Requests that find their key's entry closed while their participant is held wait
/// for it, and are decided once it goes on. Every admission is counted once, where the owner
/// still holds the count.
#[test]
fn a_data_plane_decides_alike_whatever_its_workers_and_refuses_nothing_conforming() {
    for seed in 0..3 {
        let spread = |workers| {
            run(
                &fleet(
                    4,
                    workers,
                    workload(Shape::Spread { per_window: 300 }, 6 * SECOND),
                    seed,
                ),
                link(500_000),
                Fault::None,
            )
        };
        let one = spread(1);
        counted_once(&one, "spread");
        for workers in [4, 8] {
            let many = spread(workers);
            assert_eq!(
                (&many.admitted, many.refused, many.batches),
                (&one.admitted, one.refused, one.batches),
                "{seed}: {workers} workers"
            );
        }
        let bursts = run(
            &fleet(
                4,
                8,
                workload(Shape::Bursts { each: 100 / 8 }, 6 * SECOND),
                seed,
            ),
            link(500_000),
            Fault::None,
        );
        assert!(
            bursts.admitted[0] <= allowed(&limit(100, 100), 6 * SECOND, 4),
            "{seed}: {bursts:?}"
        );
        counted_once(&bursts, "bursts");
        let span = 6 * SECOND;
        let mut waited = Vec::new();
        for fault in [
            Fault::None,
            Fault::Pause {
                at: 2 * span / 3 - 50_000_000,
                length: 100_000_000,
                plane: 3,
            },
        ] {
            let moving = fleet(
                4,
                8,
                Workload {
                    // Spread over half a data plane's workers, then on one, then on one it had
                    // not reached: an ask of its participant, held or not.
                    shape: Shape::Migrating {
                        per_window: 100,
                        last: 4,
                        to: 8,
                    },
                    span,
                    conforming: true,
                },
                seed,
            );
            let moving = run(&moving, link(500_000), fault);
            assert_eq!(moving.refused, 0, "{seed} {fault:?}");
            counted_once(&moving, &format!("{seed} {fault:?}"));
            waited.push(moving.waited);
        }
        // The worker it moved to finds the data plane's entry itself: the participant held up
        // holds up none of its requests.
        assert_eq!(
            waited[1], waited[0],
            "{seed}: waits with the participant held"
        );
        // Refilled in a tenth of a second, the key goes quiet between bursts a second apart and
        // its entry closes: the third burst, reaching a data plane while its participant is
        // held, waits for it, and is decided once it goes on. (Its owner forgets a key so soon
        // refilled before the run is measured: no counts to compare.)
        let bursts = Fleet {
            contract: limit(1_000, 100),
            ..fleet(4, 8, workload(Shape::Bursts { each: 3 }, 4 * SECOND), seed)
        };
        let first = bursts
            .workload
            .arrivals(&bursts.contract, 4 * 8, &mut Rng::new(seed))[0]
            .at;
        let closed = run(
            &bursts,
            link(500_000),
            Fault::Pause {
                at: first + 2 * SECOND - 100_000_000,
                length: 300_000_000,
                plane: 0,
            },
        );
        assert!(closed.waited.0 > 0, "{seed}: {closed:?}");
    }
}

/// Visiting only the keys that need it each cycle, as the proxy does unless `schedule_keys` is
/// off, keeps the guarantees of visiting every key, with quiet keys polled or not: bursts on
/// every data plane admitted as when every key is visited, within 5%; a conforming client moving
/// between a data plane's workers never refused; steady overload admitted as when every key is
/// visited, within 2%. Every admission is counted once.
#[test]
fn visiting_only_the_keys_that_need_it_keeps_the_guarantees_of_visiting_every_key() {
    for poll_quiet in [true, false] {
        let with = |scheduled, workload, seed| Fleet {
            scheduled,
            poll_quiet,
            ..fleet(4, 8, workload, seed)
        };
        for seed in 0..3 {
            let bursts = workload(Shape::Bursts { each: 100 / 8 }, 6 * SECOND);
            let scheduled = run(&with(true, bursts, seed), link(500_000), Fault::None);
            let every = run(&with(false, bursts, seed), link(500_000), Fault::None);
            assert!(
                scheduled.admitted[0] <= every.admitted[0] + every.admitted[0] / 20,
                "{poll_quiet} {seed}: {} against {}",
                scheduled.admitted[0],
                every.admitted[0]
            );
            counted_once(&scheduled, "bursts");
            let moving = Workload {
                shape: Shape::Migrating {
                    per_window: 100,
                    last: 4,
                    to: 8,
                },
                span: 6 * SECOND,
                conforming: true,
            };
            let moving = run(&with(true, moving, seed), link(500_000), Fault::None);
            assert_eq!(moving.refused, 0, "{poll_quiet} {seed}");
            counted_once(&moving, "moving");
            let overload = workload(Shape::Spread { per_window: 300 }, 6 * SECOND);
            let scheduled = run(&with(true, overload, seed), link(500_000), Fault::None);
            let every = run(&with(false, overload, seed), link(500_000), Fault::None);
            assert!(
                scheduled.admitted[0].abs_diff(every.admitted[0]) <= every.admitted[0] / 50,
                "{poll_quiet} {seed}: {} against {}",
                scheduled.admitted[0],
                every.admitted[0]
            );
        }
    }
}

/// Visiting only the keys that need it sends no more batches than visiting every key, under
/// steady overload, with quiet keys polled or not.
#[test]
#[ignore = "a few more batches with quiet keys not polled; whether it holds at all is #416"]
fn visiting_only_the_keys_that_need_it_sends_no_more_batches_than_visiting_every_key() {
    for poll_quiet in [true, false] {
        for seed in 0..3 {
            let overload = |scheduled| Fleet {
                scheduled,
                poll_quiet,
                ..fleet(
                    4,
                    8,
                    workload(Shape::Spread { per_window: 300 }, 6 * SECOND),
                    seed,
                )
            };
            let scheduled = run(&overload(true), link(500_000), Fault::None);
            let every = run(&overload(false), link(500_000), Fault::None);
            assert!(
                scheduled.batches <= every.batches,
                "{poll_quiet} {seed}: {} batches against {}",
                scheduled.batches,
                every.batches
            );
        }
    }
}

/// 200 a second with a bucket of 100, over a minute, on 4 data planes and on 4 data planes of 8
/// workers: spread, uneven, on one data plane's worker, over one data plane's workers and in
/// bursts on every worker over 300 ms, at the limit, half over it and three times it, and a
/// whole bucket on every data plane at once each second: no more gets through than is allowed,
/// and no less than one bucket admits, but for the few a data plane refuses while its copy has
/// yet to hear what others gave back. A minute, so that a leak past 5% shows beside the fixed
/// bucket per data plane.
#[test]
fn a_limit_held_over_a_minute_lets_through_no_more_than_allowed() {
    let contract = limit(200, 100);
    let shapes: [fn(u64, u64) -> Shape; 6] = [
        |per, _| Shape::Spread { per_window: per },
        |per, _| Shape::Uneven {
            per_window: per,
            hot: 50,
        },
        |per, _| Shape::Skewed { per_window: per },
        |per, _| Shape::Within {
            per_window: per,
            last: 8,
        },
        |per, workers| Shape::SpreadBursts {
            each: per / workers,
            over: 300_000_000,
        },
        // The same at every load: a bucket a data plane, its workers sharing it.
        |_, workers| Shape::Bursts {
            each: 100 / (workers / 4),
        },
    ];
    for seed in 0..2 {
        for (index, shape) in shapes.iter().enumerate() {
            for per_second in [200, 300, 600] {
                for workers in [1, 8] {
                    let outcome = run(
                        &Fleet {
                            contract,
                            ..fleet(
                                4,
                                workers,
                                workload(shape(per_second, 4 * workers as u64), 60 * SECOND),
                                seed,
                            )
                        },
                        link(500_000),
                        Fault::None,
                    );
                    let said = format!(
                        "{seed} shape {index} {per_second}/s {workers} workers: {} against {}",
                        outcome.admitted[0], outcome.one_bucket
                    );
                    assert!(
                        outcome.admitted[0] <= allowed(&contract, 60 * SECOND, 4),
                        "{said}"
                    );
                    assert!(
                        outcome.admitted[0] * 100 >= outcome.one_bucket * 99,
                        "{said}"
                    );
                    counted_once(&outcome, &said);
                }
            }
        }
    }
}

/// A whole bucket on every data plane at once, every second, quiet keys polled: no more admitted
/// than is allowed, and every admission counted once — with the key's owner lost half way too.
#[test]
fn bursts_on_every_data_plane_hold_to_what_is_allowed() {
    bursts_on_every_data_plane(true);
}

/// As [`bursts_on_every_data_plane_hold_to_what_is_allowed`], at the proxy's levers: quiet keys
/// not polled, four data planes of one worker admit some 1,053 against the 1,030 allowed (#137),
/// where polled they admit about 805.
#[test]
#[ignore = "over what is allowed with quiet keys not polled, the proxy's default (#137)"]
fn bursts_on_every_data_plane_hold_to_what_is_allowed_with_quiet_keys_not_polled() {
    bursts_on_every_data_plane(false);
}

/// Four data planes of one worker each taking a whole bucket at once every second, quiet keys
/// polled or not, held to what is allowed and every admission counted once, with the key's owner
/// lost half way and without.
fn bursts_on_every_data_plane(poll_quiet: bool) {
    for fault in [Fault::None, Fault::OwnerLost { at: 3 * SECOND }] {
        for seed in 0..3 {
            let outcome = run(
                &Fleet {
                    poll_quiet,
                    ..fleet(
                        4,
                        1,
                        workload(Shape::Bursts { each: 100 }, 6 * SECOND),
                        seed,
                    )
                },
                link(500_000),
                fault,
            );
            assert!(
                outcome.admitted[0] <= allowed(&limit(100, 100), 6 * SECOND, 4),
                "{fault:?} {seed}: {outcome:?}"
            );
            // The owner lost too: the store copies a key to its replicas with every change, and
            // hits sent while no pod owns it are sent again to the next (06 §4), so the new
            // owner counts every admission still.
            counted_once(&outcome, &format!("{fault:?} {seed}"));
        }
    }
}

/// A request under a limit over every data plane too is decided under both: the tighter binds,
/// whichever of the two is the run's, and every admission is counted once under each. Links
/// take a fixed delay: the second key, owned by another pod, sends more messages, and the runs
/// compared must see the same network.
#[test]
fn a_request_under_a_second_limit_is_held_to_the_tighter() {
    for seed in 0..3 {
        let base = Fleet {
            contract: limit(20, 20),
            ..fleet(4, 1, workload(Shape::Bursts { each: 20 }, 6 * SECOND), seed)
        };
        let alone = run(&base, link(0), Fault::None);
        let under = |other| {
            run(
                &Fleet {
                    beside: Some(Beside::Global(other)),
                    ..base
                },
                link(0),
                Fault::None,
            )
        };
        let wider = under(limit(200, 200));
        assert_eq!(
            wider.admitted[0], alone.admitted[0],
            "{seed}: the run's binds"
        );
        counted_once(&wider, "wider");
        let tighter = under(limit(10, 10));
        let other_alone = run(
            &Fleet {
                contract: limit(10, 10),
                ..base
            },
            link(0),
            Fault::None,
        );
        assert_eq!(
            tighter.admitted[1], other_alone.admitted[0],
            "{seed}: the other binds, as it would alone"
        );
        counted_once(&tighter, "tighter");
    }
}

/// Two routes under one limit over every data plane, their own limits smaller, bursts on every
/// data plane at once: every limit decided from the data plane's copies, the shared limit is
/// held as it would be alone. Every admission is counted once under each limit.
#[test]
fn routes_sharing_a_limit_hold_it_as_alone() {
    for seed in 0..3 {
        let base = fleet(
            2,
            1,
            workload(Shape::Bursts { each: 100 }, 6 * SECOND),
            seed,
        );
        let alone = run(&base, link(500_000), Fault::None);
        let under = run(
            &Fleet {
                beside: Some(Beside::Routes(limit(80, 80))),
                ..base
            },
            link(500_000),
            Fault::None,
        );
        assert_eq!(under.worst_excess[0], alone.worst_excess[0], "{seed}");
        counted_once(&under, "routes");
    }
}

/// A whole bucket on every data plane arriving over 30 ms, as a real client's burst does, spans
/// three of a data plane's cycles: a share given out at one cycle shows in the others' copies
/// only after the next, which must not share out the copy again, nor let them past what is
/// allowed.
#[test]
fn bursts_arriving_over_a_few_cycles_hold_to_what_is_allowed() {
    let (mut admitted, mut one_bucket) = (0, 0);
    for seed in 0..10 {
        let outcome = run(
            &fleet(
                8,
                1,
                workload(
                    Shape::SpreadBursts {
                        each: 100,
                        over: 30_000_000,
                    },
                    10 * SECOND,
                ),
                seed,
            ),
            link(500_000),
            Fault::None,
        );
        admitted += outcome.admitted[0];
        one_bucket += outcome.one_bucket;
    }
    assert!(
        admitted <= 10 * allowed(&limit(100, 100), 10 * SECOND, 8),
        "{admitted} against {one_bucket}"
    );
}
