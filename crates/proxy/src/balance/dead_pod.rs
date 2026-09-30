//! What setting aside an endpoint that cannot be connected to saves (03 §6), measured by
//! simulation rather than argued: one pod of four dies while the endpoint list still holds
//! it, and the workers' `p2c` goes on drawing it until the list drops it.
//!
//! Two deaths. A process gone, so that a connect is refused at once, and the list drops the
//! pod a few seconds later; and a node gone, so that a connect hangs until its timeout, and
//! the list takes the better part of a minute. Four ways of meeting it: `p2c` alone; active
//! checks; the pod set aside by the first try that cannot connect, for every worker, and
//! brought back only by a connect probe that gets through, which to a dead pod none does;
//! and both.
//!
//! Deterministic: the same seed gives the same numbers. `cargo test -p edgerush-proxy --lib
//! dead_pod -- --nocapture` prints them.

use super::simulation::Random;
use super::{Candidates, Share, Tried, p2c};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

/// Gateway workers.
const WORKERS: usize = 8;
/// Pods behind the upstream.
const PODS: usize = 4;
/// Requests a pod serves at once; the rest wait in its queue.
const SERVERS: usize = 4;
/// A request's mean service time, exponentially distributed, in microseconds.
const SERVICE_US: f64 = 10_000.0;
/// When the pod dies, in microseconds: the first ten seconds settle the queues.
const DIES_US: u64 = 10_000_000;
/// The pod that dies.
const DEAD: usize = 0;
/// Active checks: how often, how long a probe may take, and how many failed in a row
/// take a pod out.
const INTERVAL_US: u64 = 5_000_000;
const PROBE_TIMEOUT_US: u64 = 1_000_000;
const FALL: u32 = 2;
/// How long a pod set aside waits for its connect probe: the data plane's default.
const SET_ASIDE_US: u64 = 5_000_000;

/// How the pod dies.
#[derive(Debug, Clone, Copy)]
struct Death {
    name: &'static str,
    /// How long a connect to it takes to fail.
    connect_us: u64,
    /// How long after it dies the endpoint list drops it.
    listed_us: u64,
}

/// Its process gone: refused at once; the list drops it within seconds.
const REFUSED: Death = Death {
    name: "process gone, refused at once, listed 5 s more",
    connect_us: 1_000,
    listed_us: 5_000_000,
};

/// Its node gone: a connect hangs until the 5 s connect timeout; Kubernetes takes 40–50 s
/// to notice a node that has gone.
const HANGS: Death = Death {
    name: "node gone, a connect hangs 5 s, listed 45 s more",
    connect_us: 5_000_000,
    listed_us: 45_000_000,
};

/// What meets the dead pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Guard {
    /// `p2c` alone.
    Nothing,
    /// Active checks.
    Checks,
    /// The pod set aside by a try that cannot connect.
    SetAside,
    /// Both.
    Both,
}

impl Guard {
    fn checks(self) -> bool {
        matches!(self, Self::Checks | Self::Both)
    }

    fn sets_aside(self) -> bool {
        matches!(self, Self::SetAside | Self::Both)
    }
}

/// What a run found, of the requests that came while the dead pod was listed.
#[derive(Debug, Clone, Copy, Default)]
struct Outcome {
    /// Sent to the dead pod: each a request that failed, late by the connect's time.
    dead_tries: usize,
    /// All of them.
    requests: usize,
}

/// Pods as one worker's balancer sees them.
struct View<'a> {
    /// The pods the endpoint list holds, by position.
    listed: &'a [usize],
    in_flight: &'a [u32],
    serves: &'a [bool; PODS],
}

impl Candidates for View<'_> {
    fn count(&self) -> usize {
        self.listed.len()
    }
    fn serves(&self, at: usize) -> bool {
        self.serves[self.listed[at]]
    }
    fn in_flight(&self, at: usize) -> u32 {
        self.in_flight[self.listed[at]]
    }
    fn share(&self, _: usize) -> Share {
        Share::FULL
    }
}

#[derive(Debug, Clone, Copy)]
enum Event {
    /// A try ends: answered, or, at the dead pod, failed to connect. `served` if it held
    /// one of the pod's servers.
    Ends {
        pod: usize,
        worker: usize,
        came: u64,
        failed: bool,
        served: bool,
    },
    /// An active check's probe of `pod` starts.
    Probe { pod: usize },
    /// It ends.
    Probed { pod: usize, passed: bool },
    /// A pod set aside is tried with a connect.
    Reconnect { pod: usize },
}

/// Events by when, in order of their making where two fall at once.
#[derive(Default)]
struct Events {
    queue: BinaryHeap<Reverse<(u64, usize)>>,
    made: Vec<Event>,
}

impl Events {
    fn at(&mut self, when: u64, event: Event) {
        self.made.push(event);
        self.queue.push(Reverse((when, self.made.len() - 1)));
    }

    fn peek(&self) -> Option<u64> {
        self.queue.peek().map(|Reverse((when, _))| *when)
    }

    fn next(&mut self) -> Option<(u64, Event)> {
        let Reverse((when, made)) = self.queue.pop()?;
        Some((when, self.made[made]))
    }
}

/// The pods, their queues, and the dead one's fate.
struct Pods {
    death: Death,
    busy: [usize; PODS],
    /// Tries waiting for a server: when their request came, from which worker, its work.
    queues: [VecDeque<(u64, usize, u64)>; PODS],
}

impl Pods {
    fn dead(&self, pod: usize, now: u64) -> bool {
        pod == DEAD && now >= DIES_US
    }

    /// A try of `work` at `pod` starting at `now`, or waiting for a server there.
    fn start(
        &mut self,
        events: &mut Events,
        now: u64,
        pod: usize,
        worker: usize,
        came: u64,
        work: u64,
    ) {
        if self.dead(pod, now) {
            let failed = Event::Ends {
                pod,
                worker,
                came,
                failed: true,
                served: false,
            };
            events.at(now + self.death.connect_us, failed);
        } else if self.busy[pod] < SERVERS {
            self.busy[pod] += 1;
            self.serve(events, now, pod, worker, came, work);
        } else {
            self.queues[pod].push_back((came, worker, work));
        }
    }

    /// A try given a server: answered, unless the pod dies under it.
    fn serve(
        &self,
        events: &mut Events,
        now: u64,
        pod: usize,
        worker: usize,
        came: u64,
        work: u64,
    ) {
        let ends = now + work;
        let event = if pod == DEAD && ends > DIES_US {
            // Nothing more comes of it: it fails as a connect to the dead pod would.
            (DIES_US.max(now) + self.death.connect_us, true)
        } else {
            (ends, false)
        };
        let (when, failed) = event;
        events.at(
            when,
            Event::Ends {
                pod,
                worker,
                came,
                failed,
                served: true,
            },
        );
    }

    /// A server of `pod` let go of at `now`: the next try waiting there takes it.
    fn free(&mut self, events: &mut Events, now: u64, pod: usize) {
        match self.queues[pod].pop_front() {
            Some((came, worker, work)) => self.serve(events, now, pod, worker, came, work),
            None => self.busy[pod] -= 1,
        }
    }
}

/// One run at `load`, the fraction of the pods' capacity asked of them.
fn run(guard: Guard, death: Death, load: f64, seed: u64) -> Outcome {
    let mut random = Random(seed);
    let rate_per_us = load * (PODS * SERVERS) as f64 / SERVICE_US;
    let dropped = DIES_US + death.listed_us;
    let mut events = Events::default();
    let mut pods = Pods {
        death,
        busy: [0; PODS],
        queues: std::array::from_fn(|_| VecDeque::new()),
    };
    let mut in_flight = [[0_u32; PODS]; WORKERS];
    let mut healthy = [true; PODS];
    let mut failed_probes = [0_u32; PODS];
    let mut set_aside = [false; PODS];
    let mut outcome = Outcome::default();
    if guard.checks() {
        for pod in 0..PODS {
            events.at(random.next() % INTERVAL_US, Event::Probe { pod });
        }
    }
    let listed = |now: u64| -> Vec<usize> {
        (0..PODS)
            .filter(|&pod| pod != DEAD || now < dropped)
            .collect()
    };

    let mut arrives = random.exponential(1.0 / rate_per_us) as u64;
    loop {
        if arrives < dropped && events.peek().is_none_or(|when| arrives < when) {
            let now = arrives;
            let worker = (random.next() % WORKERS as u64) as usize;
            let listing = listed(now);
            let serves = std::array::from_fn(|pod| healthy[pod] && !set_aside[pod]);
            let view = View {
                listed: &listing,
                in_flight: &in_flight[worker],
                serves: &serves,
            };
            let pod = listing[p2c(&view, &Tried::default(), &mut || random.next()).unwrap()];
            in_flight[worker][pod] += 1;
            let work = random.exponential(SERVICE_US) as u64;
            pods.start(&mut events, now, pod, worker, now, work);
            arrives = now + random.exponential(1.0 / rate_per_us) as u64;
            continue;
        }
        let Some((now, event)) = events.next() else {
            break;
        };
        match event {
            Event::Ends {
                pod,
                worker,
                came,
                failed,
                served,
            } => {
                in_flight[worker][pod] -= 1;
                if served {
                    pods.free(&mut events, now, pod);
                }
                if failed && guard.sets_aside() && !set_aside[pod] {
                    set_aside[pod] = true;
                    events.at(now + SET_ASIDE_US, Event::Reconnect { pod });
                }
                if (DIES_US..dropped).contains(&came) {
                    outcome.requests += 1;
                    outcome.dead_tries += usize::from(failed);
                }
            }
            Event::Reconnect { pod } => {
                // A connect probe gets through only to a pod that is alive; one that does
                // not leaves it aside for another wait, once it has failed.
                if pods.dead(pod, now) {
                    if now < dropped {
                        events.at(
                            now + death.connect_us + SET_ASIDE_US,
                            Event::Reconnect { pod },
                        );
                    }
                } else {
                    set_aside[pod] = false;
                }
            }
            Event::Probe { pod } => {
                if pod == DEAD && now >= dropped {
                    continue;
                }
                let probed = if pods.dead(pod, now) {
                    (now + death.connect_us.min(PROBE_TIMEOUT_US), false)
                } else {
                    (now + 1_000, true)
                };
                events.at(
                    probed.0,
                    Event::Probed {
                        pod,
                        passed: probed.1,
                    },
                );
                // Nothing is counted once the list has dropped the pod: the run winds down.
                if now + INTERVAL_US < dropped {
                    events.at(now + INTERVAL_US, Event::Probe { pod });
                }
            }
            Event::Probed { pod, passed } => {
                if passed {
                    failed_probes[pod] = 0;
                } else {
                    failed_probes[pod] += 1;
                    if failed_probes[pod] >= FALL {
                        healthy[pod] = false;
                    }
                }
            }
        }
    }
    outcome
}

/// Each guard's outcome, summed over a few seeds.
fn table(death: Death, load: f64) -> Vec<(Guard, Outcome)> {
    println!(
        "\n{} — load {:.0}% of capacity; {WORKERS} workers, {PODS} pods of {SERVERS}",
        death.name,
        load * 100.0
    );
    println!("{:<10} {:>14} {:>10}", "guard", "to the dead", "requests");
    [Guard::Nothing, Guard::Checks, Guard::SetAside, Guard::Both]
        .into_iter()
        .map(|guard| {
            let mut sum = Outcome::default();
            for seed in 0..3 {
                let one = run(guard, death, load, 0x5EED + seed);
                sum.dead_tries += one.dead_tries;
                sum.requests += one.requests;
            }
            println!(
                "{:<10} {:>14} {:>10}",
                format!("{guard:?}"),
                sum.dead_tries,
                sum.requests
            );
            (guard, sum)
        })
        .collect()
}

fn of(outcomes: &[(Guard, Outcome)], guard: Guard) -> Outcome {
    outcomes.iter().find(|(g, _)| *g == guard).unwrap().1
}

/// A pod that refuses at once draws at least its share under `p2c`, more under load — it
/// has the fewest in flight — and active checks, two failed probes five seconds apart, are
/// slower than the list; setting it aside at the first refusal leaves it a handful of tries
/// in all.
#[test]
fn a_pod_that_refuses_is_set_aside_by_its_first_refusal() {
    for load in [0.2, 0.6] {
        let outcomes = table(REFUSED, load);
        let nothing = of(&outcomes, Guard::Nothing);
        let checks = of(&outcomes, Guard::Checks);
        let set_aside = of(&outcomes, Guard::SetAside);
        // At least about its share, a quarter, and more than that under load.
        assert!(
            nothing.dead_tries * (PODS + 1) > nothing.requests,
            "{nothing:?}"
        );
        assert!(
            checks.dead_tries * 2 > nothing.dead_tries,
            "{checks:?} against {nothing:?}"
        );
        // At most a try or two from each worker that drew it in the same instant, over
        // three runs.
        assert!(set_aside.dead_tries <= 3 * WORKERS, "{set_aside:?}");
    }
}

/// A pod whose node is gone is kept to a little of the traffic by `p2c` alone — its tries
/// pile up while they hang — and active checks cut that further; set aside after its first
/// failed connect, it is sent less than either, and checks beside it change nothing.
#[test]
fn a_pod_whose_connects_hang_is_set_aside_by_its_first_timeout() {
    for load in [0.2, 0.6] {
        let outcomes = table(HANGS, load);
        let nothing = of(&outcomes, Guard::Nothing);
        let checks = of(&outcomes, Guard::Checks);
        let set_aside = of(&outcomes, Guard::SetAside);
        let both = of(&outcomes, Guard::Both);
        assert!(nothing.dead_tries * 20 < nothing.requests, "{nothing:?}");
        assert!(
            set_aside.dead_tries < checks.dead_tries,
            "{set_aside:?} against {checks:?}"
        );
        assert!(
            set_aside.dead_tries * 2 < nothing.dead_tries,
            "{set_aside:?} against {nothing:?}"
        );
        // Checks add nothing to it: it has acted before their second failed probe.
        assert!(
            both.dead_tries < checks.dead_tries,
            "{both:?} against {checks:?}"
        );
    }
}
