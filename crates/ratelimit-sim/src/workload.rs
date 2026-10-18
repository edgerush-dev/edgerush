//! The traffic a run is played with.

use crate::{Contract, Rng};
use edgerush_ratelimit::reference::{CountingBucket, Model};

/// A request: when it arrives, and at which pod.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Arrival {
    /// Nanoseconds from the start of the run.
    pub at: u64,
    /// The pod it lands on.
    pub pod: usize,
}

/// The shape of the traffic (10 §2 names the ones that matter).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// `per_window` requests a window on average, each to a pod chosen at random: traffic
    /// spread over the fleet.
    Spread {
        /// Requests a window, on average.
        per_window: u64,
    },
    /// The same, `hot` in a hundred of them to the last pod and the rest at random:
    /// uneven traffic, one large client on a few connections among smaller ones.
    Uneven {
        /// Requests a window, on average.
        per_window: u64,
        /// Of every hundred, how many go to the last pod.
        hot: u64,
    },
    /// The same, all to the last pod: maximal skew, one client on a few long-lived
    /// connections. The last, because a way of sharing that gives the first pod a part of
    /// its own (an owner) must not be measured at its best.
    Skewed {
        /// Requests a window, on average.
        per_window: u64,
    },
    /// The same, each to one of the last `last` pods at random: one client on many
    /// connections to one data-plane pod, which land on its workers, each a data plane of
    /// the protocol — so `last` is the pod's workers.
    Within {
        /// Requests a window, on average.
        per_window: u64,
        /// Among how many of the last pods.
        last: usize,
    },
    /// `each` requests on every pod at the same instant, once a window: synchronised
    /// bursts.
    Bursts {
        /// Requests on each pod at each burst.
        each: u64,
    },
    /// `each` requests on every pod once a window, as `Bursts`, but arriving evenly over
    /// `over` nanoseconds rather than in one instant: as a real client's burst comes, a few
    /// milliseconds from first to last.
    SpreadBursts {
        /// Requests on each pod at each burst.
        each: u64,
        /// How long, in nanoseconds, each burst takes to arrive.
        over: u64,
    },
    /// One client moving: until a third of the span spread over the last `last` pods
    /// (some of one data-plane pod's workers), then to two thirds all on the last, then all
    /// on the pod `to` from the end (1 the last) — traffic concentrating and moving, as connections
    /// close and open again elsewhere: to a worker it had not reached, or to another pod.
    Migrating {
        /// Requests a window, on average.
        per_window: u64,
        /// Among how many of the last pods it starts spread.
        last: usize,
        /// The pod it ends on, counted from the end.
        to: usize,
    },
    /// `Skewed` for the first half of the run, then `Bursts`: the case that sets a bound on
    /// bursts against skewed traffic.
    SkewedThenBursts {
        /// Requests a window, on average, in the first half.
        per_window: u64,
        /// Requests on each pod at each burst, in the second half.
        each: u64,
    },
}

/// The traffic of a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Workload {
    /// Its shape.
    pub shape: Shape,
    /// How long it lasts, in nanoseconds.
    pub span: u64,
    /// Whether to keep only what one limiter holding the contract would admit, so that the
    /// traffic keeps the contract: then every refusal is a false one.
    pub conforming: bool,
}

impl Workload {
    /// The requests, in the order they arrive.
    #[must_use]
    pub fn arrivals(&self, contract: &Contract, pods: usize, rng: &mut Rng) -> Vec<Arrival> {
        let window = contract.window.get();
        let pods = pods.max(1);
        let start = rng.below(window);
        let mut arrivals = Vec::new();
        let span = start + self.span;
        let last = Target::Only(pods - 1);
        match self.shape {
            Shape::Spread { per_window } => {
                let any = Target::Any(pods);
                spread(&mut arrivals, rng, start, span, window, per_window, any);
            }
            Shape::Skewed { per_window } => {
                spread(&mut arrivals, rng, start, span, window, per_window, last);
            }
            Shape::Uneven { per_window, hot } => {
                let uneven = Target::Uneven(pods, hot);
                spread(&mut arrivals, rng, start, span, window, per_window, uneven);
            }
            Shape::Within { per_window, last } => {
                let within = Target::Last(pods, last.clamp(1, pods));
                spread(&mut arrivals, rng, start, span, window, per_window, within);
            }
            Shape::Migrating {
                per_window,
                last: among,
                to,
            } => {
                // The moves at fixed times, a third and two thirds of the span, whatever
                // the start: a fault can be set across one.
                let (first, second) = (self.span / 3, 2 * self.span / 3);
                let within = Target::Last(pods, among.clamp(1, pods));
                let other = Target::Only(pods.saturating_sub(to.clamp(1, pods)));
                spread(&mut arrivals, rng, start, first, window, per_window, within);
                spread(&mut arrivals, rng, first, second, window, per_window, last);
                spread(&mut arrivals, rng, second, span, window, per_window, other);
            }
            Shape::Bursts { each } => {
                bursts(&mut arrivals, start, span, window, each, pods);
            }
            Shape::SpreadBursts { each, over } => {
                let mut at = start;
                while at < span {
                    for k in 0..each {
                        for pod in 0..pods {
                            let offset = over * k / each.max(1);
                            arrivals.push(Arrival {
                                at: at + offset,
                                pod,
                            });
                        }
                    }
                    at += window;
                }
            }
            Shape::SkewedThenBursts { per_window, each } => {
                let middle = start + self.span / 2;
                spread(&mut arrivals, rng, start, middle, window, per_window, last);
                bursts(&mut arrivals, middle, span, window, each, pods);
            }
        }
        if self.conforming {
            let mut reference =
                CountingBucket::new(contract.limit, contract.window, contract.burst);
            arrivals.retain(|arrival| reference.decide(arrival.at));
        }
        arrivals
    }
}

/// Which pods traffic lands on.
#[derive(Clone, Copy)]
enum Target {
    /// Any of the first this many, at random.
    Any(usize),
    /// Only this one.
    Only(usize),
    /// The last of this many in so many of every hundred, any of them otherwise.
    Uneven(usize, u64),
    /// Of this many, any of the last so many, at random.
    Last(usize, usize),
}

/// About `per_window` requests a window from `from` to `until`, each gap between half and
/// one and a half times the mean, landing as `target` says. Integers only, so that every
/// machine draws the same traffic.
fn spread(
    arrivals: &mut Vec<Arrival>,
    rng: &mut Rng,
    from: u64,
    until: u64,
    window: u64,
    per_window: u64,
    target: Target,
) {
    if per_window == 0 {
        return;
    }
    let mean = (window / per_window).max(1);
    let mut at = from;
    while at < until {
        let pod = match target {
            Target::Any(pods) => usize::try_from(rng.below(pods as u64)).unwrap_or(0),
            Target::Only(pod) => pod,
            Target::Uneven(pods, hot) => {
                if rng.below(100) < hot {
                    pods - 1
                } else {
                    usize::try_from(rng.below(pods as u64)).unwrap_or(0)
                }
            }
            Target::Last(pods, last) => {
                pods - last + usize::try_from(rng.below(last as u64)).unwrap_or(0)
            }
        };
        arrivals.push(Arrival { at, pod });
        at += mean / 2 + rng.below(mean + 1);
    }
}

/// `each` requests on every pod at once, every `window` from `from` to `until`.
fn bursts(arrivals: &mut Vec<Arrival>, from: u64, until: u64, window: u64, each: u64, pods: usize) {
    let mut at = from;
    while at < until {
        for pod in 0..pods {
            for _ in 0..each {
                arrivals.push(Arrival { at, pod });
            }
        }
        at += window;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_ratelimit::reference::TokenBucket;
    use std::num::NonZeroU64;

    const SECOND: u64 = 1_000_000_000;

    fn contract() -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(100),
            window: n(SECOND),
            burst: n(20),
        }
    }

    fn workload(shape: Shape, conforming: bool) -> Workload {
        Workload {
            shape,
            span: 10 * SECOND,
            conforming,
        }
    }

    #[test]
    fn spread_traffic_comes_at_about_its_rate_to_every_pod() {
        let arrivals = workload(Shape::Spread { per_window: 100 }, false).arrivals(
            &contract(),
            4,
            &mut Rng::new(3),
        );
        assert!((900..=1100).contains(&arrivals.len()), "{}", arrivals.len());
        assert!(arrivals.windows(2).all(|pair| pair[0].at <= pair[1].at));
        for pod in 0..4 {
            let count = arrivals.iter().filter(|a| a.pod == pod).count();
            assert!((150..=350).contains(&count), "pod {pod}: {count}");
        }
    }

    /// Uneven traffic sends about `hot` in a hundred to the last pod, and the rest at
    /// random to any.
    #[test]
    fn uneven_traffic_sends_its_share_to_the_last_pod() {
        let arrivals = workload(
            Shape::Uneven {
                per_window: 400,
                hot: 50,
            },
            false,
        )
        .arrivals(&contract(), 4, &mut Rng::new(3));
        let last = arrivals.iter().filter(|a| a.pod == 3).count() * 100 / arrivals.len();
        // Half, plus a quarter of the other half landing there at random.
        assert!((57..=68).contains(&last), "{last}%");
        for pod in 0..3 {
            let count = arrivals.iter().filter(|a| a.pod == pod).count() * 100 / arrivals.len();
            assert!((9..=16).contains(&count), "pod {pod}: {count}%");
        }
    }

    /// Traffic within the last pods lands on each of them and on no other.
    #[test]
    fn traffic_within_the_last_pods_lands_on_each_of_them_alone() {
        let arrivals = workload(
            Shape::Within {
                per_window: 400,
                last: 4,
            },
            false,
        )
        .arrivals(&contract(), 16, &mut Rng::new(3));
        assert!(arrivals.iter().all(|a| (12..16).contains(&a.pod)));
        for pod in 12..16 {
            let count = arrivals.iter().filter(|a| a.pod == pod).count() * 100 / arrivals.len();
            assert!((20..=30).contains(&count), "pod {pod}: {count}%");
        }
    }

    #[test]
    fn skewed_traffic_lands_on_one_pod() {
        let arrivals = workload(Shape::Skewed { per_window: 100 }, false).arrivals(
            &contract(),
            4,
            &mut Rng::new(3),
        );
        assert!(!arrivals.is_empty() && arrivals.iter().all(|a| a.pod == 3));
    }

    #[test]
    fn spread_bursts_arrive_evenly_over_their_span_on_every_pod() {
        let sent = workload(Shape::SpreadBursts { each: 4, over: 40 }, false).arrivals(
            &contract(),
            2,
            &mut Rng::new(3),
        );
        let first: Vec<u64> = sent.iter().take(8).map(|a| a.at - sent[0].at).collect();
        assert_eq!(first, vec![0, 0, 10, 10, 20, 20, 30, 30]);
        assert_eq!(sent.iter().filter(|a| a.pod == 1).count(), sent.len() / 2);
    }

    #[test]
    fn bursts_hit_every_pod_at_the_same_instant() {
        let arrivals =
            workload(Shape::Bursts { each: 5 }, false).arrivals(&contract(), 3, &mut Rng::new(3));
        assert_eq!(arrivals.len(), 10 * 3 * 5);
        let first = arrivals[0].at;
        let at_first: Vec<usize> = arrivals
            .iter()
            .filter(|a| a.at == first)
            .map(|a| a.pod)
            .collect();
        assert_eq!(at_first.len(), 15);
        assert!((0..3).all(|pod| at_first.iter().filter(|p| **p == pod).count() == 5));
    }

    #[test]
    fn conforming_traffic_is_all_admitted_by_the_other_bucket_too() {
        for shape in [
            Shape::Spread { per_window: 300 },
            Shape::Bursts { each: 40 },
            Shape::SkewedThenBursts {
                per_window: 200,
                each: 30,
            },
        ] {
            let arrivals = workload(shape, true).arrivals(&contract(), 4, &mut Rng::new(9));
            assert!(!arrivals.is_empty());
            let c = contract();
            let mut gcra = TokenBucket::new(c.limit, c.window, c.burst);
            assert!(arrivals.iter().all(|a| gcra.decide(a.at)), "{shape:?}");
        }
    }

    #[test]
    fn the_same_seed_draws_the_same_traffic() {
        let draw = |seed| {
            workload(Shape::Spread { per_window: 100 }, false).arrivals(
                &contract(),
                4,
                &mut Rng::new(seed),
            )
        };
        assert_eq!(draw(5), draw(5));
        assert_ne!(draw(5), draw(6));
    }
}
