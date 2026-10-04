//! A worker's side of load balancing ([03 §6](../../../docs/03-data-plane.md)): what it has
//! in flight to each endpoint and whose turn it is, per upstream of the config it serves,
//! for [`crate::balance`] to choose with.
//!
//! **A worker's own.** It counts only its own exchanges, and no request writes where another
//! core writes: a stalled pod shows in every worker's count, since every worker's exchanges
//! to it stop coming back.

use crate::balance::{self, Candidates, RoundRobin, Share, Tried};
use crate::random::random;
use crate::upstream::destination::{Destinations, ReuseIdentity};
use edgerush_config::{Compiled, LoadBalancer};
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// One exchange in flight to an endpoint, counted from its pick until it is let go of:
/// with the place its exchange holds, which goes with the answer's body to its end, or with
/// a tunnel until it closes.
#[derive(Debug)]
pub(crate) struct InFlight(Rc<Cell<u32>>);

impl InFlight {
    fn new(count: &Rc<Cell<u32>>) -> Self {
        count.set(count.get().saturating_add(1));
        Self(Rc::clone(count))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

/// A worker's balancing state for every upstream of one config.
#[derive(Debug, Default)]
pub(crate) struct Balancing {
    /// The config it was made for; none until the first request.
    generation: Option<u64>,
    /// By position of the upstream.
    upstreams: Vec<Rc<Upstream>>,
}

/// A worker's balancing state for one upstream: its balancer, a count for each endpoint, and
/// whose turn it is.
#[derive(Debug)]
pub(crate) struct Upstream {
    balancer: LoadBalancer,
    /// Its slow start's window in milliseconds, if it has one.
    window: Option<u64>,
    /// What each endpoint's destination is filed under, by position: how a new config finds
    /// the counts and the turn it keeps.
    keys: Box<[u64]>,
    /// By position of the endpoint. Each count is an exchange's to hold, so that one begun
    /// on an earlier config still counts where it went, and still comes off, whatever
    /// configs come in the meantime.
    counts: Box<[Rc<Cell<u32>>]>,
    round_robin: Cell<RoundRobin>,
}

impl Balancing {
    /// Made again for the config numbered `generation`, if it is not the one this was made
    /// for: counts are kept for every destination that is the same one, and the turn for
    /// every upstream whose endpoints are all the same, in the same order; an upstream with a
    /// new list starts its turns at a place drawn at random. Once per worker per config.
    pub(crate) fn refresh(
        &mut self,
        generation: u64,
        config: &Compiled,
        destinations: &Destinations,
    ) {
        if self.generation == Some(generation) {
            return;
        }
        let mut counts: HashMap<u64, Rc<Cell<u32>>> = HashMap::new();
        let mut turns: HashMap<Box<[u64]>, RoundRobin> = HashMap::new();
        for upstream in self.upstreams.drain(..) {
            for (key, count) in upstream.keys.iter().zip(upstream.counts.iter()) {
                counts.insert(*key, Rc::clone(count));
            }
            turns.insert(upstream.keys.clone(), upstream.round_robin.get());
        }
        self.upstreams = config
            .upstreams()
            .iter()
            .enumerate()
            .map(|(position, upstream)| {
                let keys: Box<[u64]> = destinations
                    .of(position)
                    .iter()
                    .map(|destination| destination.key())
                    .collect();
                let counts = keys
                    .iter()
                    .map(|key| counts.remove(key).unwrap_or_default())
                    .collect();
                let round_robin = turns.remove(&keys).unwrap_or_else(|| {
                    // Any `usize` will do: it is taken modulo the endpoints.
                    RoundRobin::starting_at(random() as usize)
                });
                Rc::new(Upstream {
                    balancer: upstream.load_balancer,
                    window: upstream.slow_start.map(|slow_start| slow_start.window_ms),
                    keys,
                    counts,
                    round_robin: Cell::new(round_robin),
                })
            })
            .collect();
        self.generation = Some(generation);
    }

    /// The state of the upstream at `position` of the config last refreshed for.
    pub(crate) fn upstream(&self, position: usize) -> Option<&Rc<Upstream>> {
        self.upstreams.get(position)
    }
}

/// The endpoints of one upstream as its balancer sees them from one worker.
struct Seen<'a> {
    destinations: &'a [Arc<ReuseIdentity>],
    counts: &'a [Rc<Cell<u32>>],
    window: Option<u64>,
}

impl Candidates for Seen<'_> {
    fn count(&self) -> usize {
        self.destinations.len().min(self.counts.len())
    }

    fn serves(&self, at: usize) -> bool {
        self.destinations
            .get(at)
            .is_some_and(|destination| destination.serves())
    }

    fn in_flight(&self, at: usize) -> u32 {
        self.counts.get(at).map_or(0, |count| count.get())
    }

    fn share(&self, at: usize) -> Share {
        self.destinations
            .get(at)
            .map_or(Share::FULL, |destination| destination.share(self.window))
    }
}

impl Upstream {
    /// The endpoint an exchange goes to, among `destinations` — this upstream's, of the
    /// config this state was made for — keeping away from those in `tried`, and the count it
    /// holds while in flight. `None` only if there are no endpoints.
    pub(crate) fn pick(
        &self,
        destinations: &[Arc<ReuseIdentity>],
        tried: &Tried,
    ) -> Option<(usize, InFlight)> {
        let seen = Seen {
            destinations,
            counts: &self.counts,
            window: self.window,
        };
        let at = match self.balancer {
            LoadBalancer::P2c => balance::p2c(&seen, tried, &mut random),
            LoadBalancer::RoundRobin => {
                let mut round_robin = self.round_robin.get();
                let at = round_robin.pick(&seen, tried, &mut random);
                self.round_robin.set(round_robin);
                at
            }
        }?;
        Some((at, InFlight::new(self.counts.get(at)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::destination::Keys;
    use edgerush_config::{Config, compile};

    fn compiled(upstreams: &[(&str, &str, &[&str])]) -> Compiled {
        let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
        for (name, balancer, addresses) in upstreams {
            let listed: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
            yaml += &format!(
                "  {name}: {{ load_balancer: {balancer}, endpoints: [{}] }}\n",
                listed.join(", ")
            );
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
    }

    /// A worker's state for `config`, reconciled against `previous`.
    fn state(
        config: &Compiled,
        previous: &Destinations,
        keys: &Keys,
        balancing: &mut Balancing,
        generation: u64,
    ) -> Destinations {
        let destinations = Destinations::reconcile(config, previous, keys, &[None, None]);
        balancing.refresh(generation, config, &destinations);
        destinations
    }

    #[test]
    fn an_exchange_counts_from_its_pick_until_it_is_let_go_of() {
        let config = compiled(&[("u", "p2c", &["10.0.0.1:80", "10.0.0.2:80"])]);
        let keys = Keys::default();
        let mut balancing = Balancing::default();
        let destinations = state(&config, &Destinations::default(), &keys, &mut balancing, 0);
        let upstream = Rc::clone(balancing.upstream(0).unwrap());
        let (first, held) = upstream
            .pick(destinations.of(0), &Tried::default())
            .unwrap();
        assert_eq!(upstream.counts[first].get(), 1);
        // The other has none in flight, so it is the pick while the first is held.
        let (second, _also) = upstream
            .pick(destinations.of(0), &Tried::default())
            .unwrap();
        assert_ne!(first, second);
        drop(held);
        assert_eq!(upstream.counts[first].get(), 0);
    }

    #[test]
    fn a_count_outlives_a_reload_that_keeps_its_destination() {
        let before = compiled(&[("u", "p2c", &["10.0.0.1:80"])]);
        let after = compiled(&[("u", "p2c", &["10.0.0.1:80", "10.0.0.2:80"])]);
        let keys = Keys::default();
        let mut balancing = Balancing::default();
        let first = state(&before, &Destinations::default(), &keys, &mut balancing, 0);
        let upstream = Rc::clone(balancing.upstream(0).unwrap());
        let (_, held) = upstream.pick(first.of(0), &Tried::default()).unwrap();

        let second = state(&after, &first, &keys, &mut balancing, 1);
        let reloaded = balancing.upstream(0).unwrap();
        // The same destination, the same count: what was in flight before still is.
        assert_eq!(reloaded.counts[0].get(), 1);
        let (at, _) = reloaded.pick(second.of(0), &Tried::default()).unwrap();
        assert_eq!(at, 1, "the endpoint with nothing in flight");
        drop(held);
        assert_eq!(reloaded.counts[0].get(), 0);
    }

    #[test]
    fn the_turn_is_kept_while_the_endpoints_are_and_drawn_again_when_they_change() {
        let three = compiled(&[(
            "u",
            "round_robin",
            &["10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"],
        )]);
        let keys = Keys::default();
        let mut balancing = Balancing::default();
        let destinations = state(&three, &Destinations::default(), &keys, &mut balancing, 0);
        let upstream = Rc::clone(balancing.upstream(0).unwrap());
        let (at, _) = upstream
            .pick(destinations.of(0), &Tried::default())
            .unwrap();

        // The same list under a new config: the turn goes on from where it was.
        let same = state(&three, &destinations, &keys, &mut balancing, 1);
        let (next, _) = balancing
            .upstream(0)
            .unwrap()
            .pick(same.of(0), &Tried::default())
            .unwrap();
        assert_eq!(next, (at + 1) % 3);

        // A new list: a turn of its own, from wherever it was drawn to start.
        let four = compiled(&[(
            "u",
            "round_robin",
            &["10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80", "10.0.0.4:80"],
        )]);
        let grown = state(&four, &same, &keys, &mut balancing, 2);
        let upstream = balancing.upstream(0).unwrap();
        let round: Vec<usize> = (0..4)
            .map(|_| upstream.pick(grown.of(0), &Tried::default()).unwrap().0)
            .collect();
        let mut sorted = round.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, [0, 1, 2, 3], "{round:?}");
    }

    #[test]
    fn round_robin_workers_do_not_start_in_lock_step() {
        // Each worker draws where to start: of sixteen workers over eight endpoints, not all
        // start at the same one (all would, once in 8¹⁵).
        let config = compiled(&[(
            "u",
            "round_robin",
            &[
                "10.0.0.1:80",
                "10.0.0.2:80",
                "10.0.0.3:80",
                "10.0.0.4:80",
                "10.0.0.5:80",
                "10.0.0.6:80",
                "10.0.0.7:80",
                "10.0.0.8:80",
            ],
        )]);
        let keys = Keys::default();
        let destinations =
            Destinations::reconcile(&config, &Destinations::default(), &keys, &[None]);
        let firsts: std::collections::BTreeSet<usize> = (0..16)
            .map(|_| {
                let mut balancing = Balancing::default();
                balancing.refresh(0, &config, &destinations);
                let upstream = balancing.upstream(0).unwrap();
                upstream
                    .pick(destinations.of(0), &Tried::default())
                    .unwrap()
                    .0
            })
            .collect();
        assert!(firsts.len() > 1, "{firsts:?}");
    }

    #[test]
    fn each_upstream_is_balanced_as_it_says() {
        let config = compiled(&[
            ("a", "p2c", &["10.0.0.1:80", "10.0.0.2:80"]),
            ("b", "round_robin", &["10.0.0.3:80", "10.0.0.4:80"]),
        ]);
        let keys = Keys::default();
        let mut balancing = Balancing::default();
        let destinations = state(&config, &Destinations::default(), &keys, &mut balancing, 0);
        assert_eq!(balancing.upstream(0).unwrap().balancer, LoadBalancer::P2c);
        // Round-robin takes its turns whatever is in flight: holding every pick, it still
        // alternates.
        let b = Rc::clone(balancing.upstream(1).unwrap());
        let held: Vec<(usize, InFlight)> = (0..4)
            .map(|_| b.pick(destinations.of(1), &Tried::default()).unwrap())
            .collect();
        let order: Vec<usize> = held.iter().map(|(at, _)| *at).collect();
        assert_ne!(order[0], order[1]);
        assert_eq!(order[0], order[2]);
        // P2C with one of two held always takes the other.
        let a = Rc::clone(balancing.upstream(0).unwrap());
        let (first, _keep) = a.pick(destinations.of(0), &Tried::default()).unwrap();
        for _ in 0..20 {
            assert_ne!(
                a.pick(destinations.of(0), &Tried::default()).unwrap().0,
                first
            );
        }
    }
}
