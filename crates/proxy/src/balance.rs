//! Which endpoint of an upstream takes an exchange ([03 §6](../../docs/03-data-plane.md)):
//! `p2c`, two different endpoints drawn at random and the one with fewer exchanges in flight,
//! or `round_robin`, each endpoint in turn.
//!
//! Pure: what the endpoints are like — whether they serve, what is in flight to them, how far
//! into slow start they are — is asked of [`Candidates`], and every random number of a
//! caller's generator. Both balancers share three rules:
//!
//! - **Health**: an endpoint failing its checks is not taken, unless fewer than half serve,
//!   when health is ignored and any may be (Envoy's panic threshold at its default): probes
//!   that fail most of an upstream are likelier wrong, or about to put the rest under a load
//!   that fails them too, than a reason to answer every request 503.
//! - **Tried**: a retry takes no endpoint already tried, unless every one health allows has
//!   been, when any may be taken again (NGINX's tried set).
//! - **Slow start acts on the draw**: an endpoint in slow start, drawn or reached in turn, is
//!   kept with the probability of its [`Share`] and otherwise drawn again or passed over, so
//!   that its part follows the ramp whatever the load. After [`REFUSALS`] refusals the one in
//!   hand is kept: when most of what may be taken is ramping, refusing for ever would only
//!   spin. An upstream of one endpoint never ramps.
//!
//! What the common case costs — every endpoint serving, none ramping, a first try — is one
//! look at each endpoint drawn: no endpoint is counted, and no list is made, unless one drawn
//! may not be taken.

// What is here is `pub` so that the benchmarks, which are a crate of their own, can name it.
// The module is public only when they are being built, so in an ordinary build none of this
// is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

#[cfg(test)]
mod dead_pod;
#[cfg(test)]
mod simulation;

/// How often an endpoint in slow start may be refused, for one pick, before the one in hand
/// is kept. Where one old endpoint is left beside nine new ones at a tenth, it takes about
/// 45% of the requests rather than the 53% its share is; more refusals come nearer, and cost
/// a random number each.
pub const REFUSALS: u32 = 8;

/// What part of a full endpoint's share one takes, in 65,536ths: [`Share::FULL`] for one not
/// in slow start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Share(u32);

impl Share {
    /// All of it: an endpoint not in slow start.
    pub const FULL: Self = Self(1 << 16);

    /// `parts` 65,536ths of a full share, at most all of it.
    pub fn of(parts: u32) -> Self {
        Self(parts.min(Self::FULL.0))
    }

    /// Where slow start has an endpoint `elapsed` milliseconds into a ramp of `window`: a
    /// tenth of a full share at its start, rising linearly to all of it at its end (Envoy's
    /// curve at its defaults). The tenth at the start is so that a pod just ready is used at
    /// once, if lightly, rather than hardly at all for the first part of the window.
    pub fn ramped(elapsed: u64, window: u64) -> Self {
        if elapsed >= window {
            return Self::FULL;
        }
        let full = u128::from(Self::FULL.0);
        let floor = full.div_ceil(10);
        // Below `window`, so below `full` and within a `u32`.
        let parts = floor + (full - floor) * u128::from(elapsed) / u128::from(window);
        Self::of(u32::try_from(parts).unwrap_or(Self::FULL.0))
    }

    /// Whether an endpoint of this share, drawn, is kept, given a number uniform over `u64`.
    /// Its top sixteen bits are uniform over 0..65,536, so a share of `p` is kept with the
    /// probability `p / 65,536`, and a full one always.
    fn keeps(self, random: u64) -> bool {
        (random >> 48) < u64::from(self.0)
    }
}

/// The endpoints of one upstream, as a balancer sees them, by position.
pub trait Candidates {
    /// How many there are.
    fn count(&self) -> usize;
    /// Whether the endpoint at `at` passes its checks, or has none.
    fn serves(&self, at: usize) -> bool;
    /// How many exchanges this worker has in flight to it.
    fn in_flight(&self, at: usize) -> u32;
    /// How far into slow start it is: [`Share::FULL`] for one that is not.
    fn share(&self, at: usize) -> Share;
}

/// The endpoints a request has already been sent to, by position, for its retries to keep
/// away from.
#[derive(Debug, Clone, Default)]
pub struct Tried {
    at: [usize; Self::MOST],
    len: usize,
}

impl Tried {
    /// A request's first try and its most retries (five: a rule's `attempts`, 07 §1).
    const MOST: usize = 6;

    /// Adds `at` to what has been tried. Past [`Self::MOST`], which no request reaches, the
    /// oldest are what is kept.
    pub fn add(&mut self, at: usize) {
        if let Some(slot) = self.at.get_mut(self.len) {
            *slot = at;
            self.len += 1;
        }
    }

    fn contains(&self, at: usize) -> bool {
        self.at
            .get(..self.len)
            .is_some_and(|tried| tried.contains(&at))
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Which endpoints may be taken for one pick, worked out only once one drawn may not be.
#[derive(Debug, Clone, Copy)]
struct Rule {
    /// Fewer than half serve: any may be taken, whatever its checks say.
    health_ignored: bool,
    /// Every endpoint health allows has been tried: any of them may be taken again.
    tried_ignored: bool,
    /// How many may be taken.
    count: usize,
}

impl Rule {
    fn of(candidates: &impl Candidates, tried: &Tried) -> Self {
        let endpoints = candidates.count();
        let serving = (0..endpoints).filter(|&at| candidates.serves(at)).count();
        let health_ignored = serving * 2 < endpoints;
        let allowed = if health_ignored { endpoints } else { serving };
        let untried = if tried.is_empty() {
            allowed
        } else {
            (0..endpoints)
                .filter(|&at| (health_ignored || candidates.serves(at)) && !tried.contains(at))
                .count()
        };
        let tried_ignored = untried == 0;
        Self {
            health_ignored,
            tried_ignored,
            count: if tried_ignored { allowed } else { untried },
        }
    }

    fn allows(&self, candidates: &impl Candidates, tried: &Tried, at: usize) -> bool {
        (self.health_ignored || candidates.serves(at))
            && (self.tried_ignored || !tried.contains(at))
    }
}

/// One pick's view of the endpoints: its rule, once it is needed.
struct Pick<'a, C, R> {
    candidates: &'a C,
    tried: &'a Tried,
    rule: Option<Rule>,
    random: &'a mut R,
}

impl<C: Candidates, R: FnMut() -> u64> Pick<'_, C, R> {
    fn random(&mut self) -> u64 {
        (self.random)()
    }

    /// Whether the endpoint at `at` may be taken. A serving endpoint not yet tried always may,
    /// whatever the rule, so the rule is worked out only for one that is neither.
    fn allows(&mut self, at: usize) -> bool {
        if self.candidates.serves(at) && !self.tried.contains(at) {
            return true;
        }
        let rule = self.rule();
        rule.allows(self.candidates, self.tried, at)
    }

    fn rule(&mut self) -> Rule {
        *self
            .rule
            .get_or_insert_with(|| Rule::of(self.candidates, self.tried))
    }

    /// One endpoint that may be taken, other than `besides`, each as likely as any other;
    /// `None` if there is none.
    ///
    /// A first draw over every endpoint is kept if it may be taken; otherwise a second is
    /// made over those that may. Each that may is then as likely as any other: `1/n` from the
    /// first draw, plus `(1 - m/n)/m` from the second, which is `1/m`.
    fn draw(&mut self, besides: Option<usize>) -> Option<usize> {
        let endpoints = self.candidates.count();
        let slots = endpoints - usize::from(besides.is_some());
        let at = skip(index(self.random(), slots)?, besides);
        if self.allows(at) {
            return Some(at);
        }
        let rule = self.rule();
        let left = rule.count.saturating_sub(usize::from(besides.is_some()));
        let nth = index(self.random(), left)?;
        (0..endpoints)
            .filter(|&at| Some(at) != besides && rule.allows(self.candidates, self.tried, at))
            .nth(nth)
    }

    /// [`Self::draw`], drawn again while slow start refuses what it drew, [`REFUSALS`] times
    /// at most.
    fn draw_ramped(&mut self, besides: Option<usize>) -> Option<usize> {
        let mut at = self.draw(besides)?;
        // One endpoint never ramps: there is nothing to send its share to instead.
        if self.candidates.count() == 1 {
            return Some(at);
        }
        for _ in 0..REFUSALS {
            let share = self.candidates.share(at);
            if share == Share::FULL || share.keeps(self.random()) {
                break;
            }
            at = self.draw(besides)?;
        }
        Some(at)
    }
}

/// `random` brought into `0..slots`; `None` if there are no slots.
fn index(random: u64, slots: usize) -> Option<usize> {
    let slots = u64::try_from(slots).ok()?;
    usize::try_from(random.checked_rem(slots)?).ok()
}

/// The `at`th slot of a range that leaves `besides` out, as a position in the whole range.
fn skip(at: usize, besides: Option<usize>) -> usize {
    match besides {
        Some(besides) if at >= besides => at + 1,
        _ => at,
    }
}

/// `p2c`: two different endpoints that may be taken, drawn at random, and of them the one
/// with fewer exchanges in flight; the first drawn on a tie. The one that may be taken if
/// there is only one; `None` only if there are no endpoints.
///
/// Two *different* endpoints: two draws that may land on the same one would give the busier
/// of two pods a quarter of the requests (Envoy says so of its own).
pub fn p2c(
    candidates: &impl Candidates,
    tried: &Tried,
    random: &mut impl FnMut() -> u64,
) -> Option<usize> {
    let mut pick = Pick {
        candidates,
        tried,
        rule: None,
        random,
    };
    let first = pick.draw_ramped(None)?;
    if candidates.count() == 1 {
        return Some(first);
    }
    let Some(second) = pick.draw_ramped(Some(first)) else {
        return Some(first);
    };
    if candidates.in_flight(second) < candidates.in_flight(first) {
        Some(second)
    } else {
        Some(first)
    }
}

/// `round_robin`, for one upstream in one worker: each endpoint that may be taken in turn.
#[derive(Debug, Clone, Copy, Default)]
pub struct RoundRobin {
    /// Where the next turn starts looking: one past the endpoint last taken. An endpoint
    /// passed over gives its turn to none: the next one taken is the next that may be, and
    /// the turn after that starts after it, so every endpoint that may be taken is taken once
    /// in each round.
    next: usize,
}

impl RoundRobin {
    /// Starting at the endpoint at `start`, taken modulo however many there are. A worker
    /// draws it at random, and again for each new list of endpoints: started at the first,
    /// every worker, and every gateway pod, would send its first request to the same pod and
    /// go on in lock step.
    pub fn starting_at(start: usize) -> Self {
        Self { next: start }
    }

    /// The endpoint whose turn it is; `None` only if there are no endpoints. Random numbers
    /// are drawn only for an endpoint in slow start.
    pub fn pick(
        &mut self,
        candidates: &impl Candidates,
        tried: &Tried,
        random: &mut impl FnMut() -> u64,
    ) -> Option<usize> {
        let endpoints = candidates.count();
        let mut at = self.next.checked_rem(endpoints)?;
        let mut pick = Pick {
            candidates,
            tried,
            rule: None,
            random,
        };
        let mut refusals = 0;
        // Some endpoint may always be taken, so each round of them finds one, and each
        // refusal costs at most a round: this many steps always find one.
        for _ in 0..endpoints * (REFUSALS as usize + 1) {
            if pick.allows(at) {
                let share = candidates.share(at);
                if endpoints == 1
                    || refusals == REFUSALS
                    || share == Share::FULL
                    || share.keeps(pick.random())
                {
                    self.next = at + 1;
                    return Some(at);
                }
                refusals += 1;
            }
            at = (at + 1) % endpoints;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    /// Endpoints as a test states them.
    #[derive(Debug, Clone)]
    struct Endpoints {
        serves: Vec<bool>,
        in_flight: Vec<u32>,
        share: Vec<Share>,
    }

    impl Endpoints {
        fn serving(count: usize) -> Self {
            Self {
                serves: vec![true; count],
                in_flight: vec![0; count],
                share: vec![Share::FULL; count],
            }
        }

        fn with_health(serves: &[bool]) -> Self {
            Self {
                serves: serves.to_vec(),
                ..Self::serving(serves.len())
            }
        }
    }

    impl Candidates for Endpoints {
        fn count(&self) -> usize {
            self.serves.len()
        }
        fn serves(&self, at: usize) -> bool {
            self.serves[at]
        }
        fn in_flight(&self, at: usize) -> u32 {
            self.in_flight[at]
        }
        fn share(&self, at: usize) -> Share {
            self.share[at]
        }
    }

    /// SplitMix64, as the data plane's own generator: seeded, so every run draws the same.
    fn generator(seed: u64) -> impl FnMut() -> u64 {
        let mut state = seed;
        move || {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mixed = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            mixed ^ (mixed >> 31)
        }
    }

    fn tried(positions: &[usize]) -> Tried {
        let mut tried = Tried::default();
        for &at in positions {
            tried.add(at);
        }
        tried
    }

    /// The naive reference: which endpoints may be taken, by the rules as 03 §6 states them,
    /// worked out in full every time.
    fn may_take(endpoints: &Endpoints, tried: &[usize]) -> BTreeSet<usize> {
        let all: BTreeSet<usize> = (0..endpoints.count()).collect();
        let serving: BTreeSet<usize> = all
            .iter()
            .copied()
            .filter(|&at| endpoints.serves[at])
            .collect();
        let allowed = if serving.len() * 2 < all.len() {
            all
        } else {
            serving
        };
        let untried: BTreeSet<usize> = allowed
            .iter()
            .copied()
            .filter(|at| !tried.contains(at))
            .collect();
        if untried.is_empty() { allowed } else { untried }
    }

    /// How often each endpoint is picked in `picks` picks.
    fn tally(
        endpoints: usize,
        picks: usize,
        mut pick: impl FnMut() -> Option<usize>,
    ) -> Vec<usize> {
        let mut counts = vec![0; endpoints];
        for _ in 0..picks {
            counts[pick().unwrap()] += 1;
        }
        counts
    }

    #[test]
    fn no_endpoints_is_no_pick() {
        let none = Endpoints::serving(0);
        let mut random = generator(1);
        assert_eq!(p2c(&none, &Tried::default(), &mut random), None);
        assert_eq!(
            RoundRobin::starting_at(3).pick(&none, &Tried::default(), &mut random),
            None
        );
    }

    #[test]
    fn one_endpoint_is_always_the_pick_even_while_it_ramps() {
        let mut one = Endpoints::serving(1);
        one.share[0] = Share::of(0);
        let mut random = generator(2);
        let mut round_robin = RoundRobin::starting_at(7);
        for _ in 0..100 {
            assert_eq!(p2c(&one, &Tried::default(), &mut random), Some(0));
            assert_eq!(
                round_robin.pick(&one, &Tried::default(), &mut random),
                Some(0)
            );
        }
    }

    #[test]
    fn p2c_takes_the_one_with_fewer_in_flight_of_the_two_it_draws() {
        // Two endpoints: both are always drawn, so the less busy always wins.
        let mut two = Endpoints::serving(2);
        two.in_flight = vec![5, 3];
        let mut random = generator(3);
        for _ in 0..100 {
            assert_eq!(p2c(&two, &Tried::default(), &mut random), Some(1));
        }
    }

    #[test]
    fn p2c_spreads_evenly_over_endpoints_equally_busy() {
        let four = Endpoints::serving(4);
        let mut random = generator(4);
        let counts = tally(4, 40_000, || p2c(&four, &Tried::default(), &mut random));
        for count in counts {
            assert!((9_500..=10_500).contains(&count), "{count}");
        }
    }

    #[test]
    fn p2c_sends_a_stalled_endpoint_nothing_while_another_may_be_taken() {
        let mut three = Endpoints::serving(3);
        three.in_flight = vec![0, 40, 0];
        let mut random = generator(5);
        let counts = tally(3, 3_000, || p2c(&three, &Tried::default(), &mut random));
        assert_eq!(counts[1], 0);
    }

    #[test]
    fn p2c_favours_the_least_busy_about_twice_over() {
        // With distinct pairs out of four, the least busy is in half the pairs and wins them
        // all; the busiest wins none.
        let mut four = Endpoints::serving(4);
        four.in_flight = vec![1, 2, 3, 4];
        let mut random = generator(6);
        let counts = tally(4, 60_000, || p2c(&four, &Tried::default(), &mut random));
        assert!((29_000..=31_000).contains(&counts[0]), "{counts:?}");
        assert_eq!(counts[3], 0, "{counts:?}");
    }

    #[test]
    fn p2c_spreads_evenly_over_those_that_serve_when_some_do_not() {
        // A first draw that lands on one failing its checks is drawn again among those that
        // serve, so that none of them comes up more often for sitting after it.
        let six = Endpoints::with_health(&[true, false, true, true, false, true]);
        let mut random = generator(15);
        let counts = tally(6, 40_000, || p2c(&six, &Tried::default(), &mut random));
        for at in [0, 2, 3, 5] {
            assert!((9_500..=10_500).contains(&counts[at]), "{counts:?}");
        }
    }

    #[test]
    fn round_robin_takes_each_endpoint_once_a_round_from_its_start() {
        let four = Endpoints::serving(4);
        let mut random = generator(7);
        let mut round_robin = RoundRobin::starting_at(6);
        let picked: Vec<usize> = (0..8)
            .map(|_| {
                round_robin
                    .pick(&four, &Tried::default(), &mut random)
                    .unwrap()
            })
            .collect();
        assert_eq!(picked, [2, 3, 0, 1, 2, 3, 0, 1]);
    }

    #[test]
    fn round_robin_passes_over_an_endpoint_failing_its_checks_without_doubling_the_next() {
        let four = Endpoints::with_health(&[true, true, false, true]);
        let mut random = generator(8);
        let mut round_robin = RoundRobin::starting_at(0);
        let picked: Vec<usize> = (0..6)
            .map(|_| {
                round_robin
                    .pick(&four, &Tried::default(), &mut random)
                    .unwrap()
            })
            .collect();
        assert_eq!(picked, [0, 1, 3, 0, 1, 3]);
    }

    #[test]
    fn round_robin_draws_no_random_number_while_nothing_ramps() {
        let four = Endpoints::with_health(&[true, false, true, true]);
        let mut drawn = 0;
        let mut random = || {
            drawn += 1;
            0
        };
        let mut round_robin = RoundRobin::starting_at(0);
        for _ in 0..12 {
            round_robin.pick(&four, &Tried::default(), &mut random);
        }
        assert_eq!(drawn, 0);
    }

    #[test]
    fn health_is_ignored_below_half_serving() {
        // One of three serves: any may be taken.
        let mostly_failing = Endpoints::with_health(&[false, true, false]);
        let mut random = generator(9);
        let counts = tally(3, 3_000, || {
            p2c(&mostly_failing, &Tried::default(), &mut random)
        });
        assert!(counts.iter().all(|&count| count > 0), "{counts:?}");
        let mut round_robin = RoundRobin::starting_at(0);
        let picked: Vec<usize> = (0..3)
            .map(|_| {
                round_robin
                    .pick(&mostly_failing, &Tried::default(), &mut random)
                    .unwrap()
            })
            .collect();
        assert_eq!(picked, [0, 1, 2]);
    }

    #[test]
    fn half_serving_is_enough_to_keep_to_those_that_serve() {
        let half = Endpoints::with_health(&[false, true, false, true]);
        let mut random = generator(10);
        let counts = tally(4, 4_000, || p2c(&half, &Tried::default(), &mut random));
        assert_eq!((counts[0], counts[2]), (0, 0), "{counts:?}");
    }

    #[test]
    fn a_retry_keeps_away_from_every_endpoint_tried() {
        let four = Endpoints::serving(4);
        let already = tried(&[0, 2]);
        let mut random = generator(11);
        let counts = tally(4, 4_000, || p2c(&four, &already, &mut random));
        assert_eq!((counts[0], counts[2]), (0, 0), "{counts:?}");
        let mut round_robin = RoundRobin::starting_at(0);
        for _ in 0..8 {
            let at = round_robin.pick(&four, &already, &mut random).unwrap();
            assert!(at == 1 || at == 3);
        }
    }

    #[test]
    fn once_every_endpoint_that_serves_is_tried_any_of_them_may_be_again() {
        let three = Endpoints::with_health(&[true, true, false]);
        let already = tried(&[0, 1]);
        let mut random = generator(12);
        let counts = tally(3, 3_000, || p2c(&three, &already, &mut random));
        assert!(counts[0] > 0 && counts[1] > 0, "{counts:?}");
        assert_eq!(counts[2], 0, "{counts:?}");
    }

    #[test]
    fn a_ramping_endpoint_is_drawn_in_proportion_to_its_share() {
        // Four endpoints, one at a quarter: its part is a quarter of a full one's, 1/13 of
        // the picks, whether nothing is in flight or everything is.
        let mut four = Endpoints::serving(4);
        four.share[3] = Share::of(1 << 14);
        let mut random = generator(13);
        let counts = tally(4, 130_000, || p2c(&four, &Tried::default(), &mut random));
        assert!((9_000..=11_000).contains(&counts[3]), "{counts:?}");
        let mut round_robin = RoundRobin::starting_at(0);
        let counts = tally(4, 130_000, || {
            round_robin.pick(&four, &Tried::default(), &mut random)
        });
        assert!((9_000..=11_000).contains(&counts[3]), "{counts:?}");
    }

    #[test]
    fn one_old_endpoint_beside_nine_ramping_takes_what_refusals_allow() {
        // Its share is 1/1.9, 53%; eight refusals at most leave it 45% (see `REFUSALS`).
        let mut ten = Endpoints::serving(10);
        ten.share = vec![Share::of(6_554); 10];
        ten.share[0] = Share::FULL;
        let mut random = generator(16);
        let counts = tally(10, 100_000, || p2c(&ten, &Tried::default(), &mut random));
        assert!((43_500..=45_500).contains(&counts[0]), "{counts:?}");
    }

    #[test]
    fn when_everything_may_be_refused_a_pick_is_still_made() {
        let mut three = Endpoints::serving(3);
        three.share = vec![Share::of(0); 3];
        let mut random = generator(14);
        let mut round_robin = RoundRobin::starting_at(0);
        for _ in 0..100 {
            assert!(p2c(&three, &Tried::default(), &mut random).is_some());
            assert!(
                round_robin
                    .pick(&three, &Tried::default(), &mut random)
                    .is_some()
            );
        }
    }

    #[test]
    fn a_share_is_kept_in_proportion() {
        assert!(Share::FULL.keeps(u64::MAX));
        assert!(!Share::of(0).keeps(0));
        assert!(Share::of(1).keeps(0));
        assert!(!Share::of(1).keeps(1 << 48));
        assert_eq!(Share::of(u32::MAX), Share::FULL);
    }

    #[test]
    fn a_ramp_rises_linearly_from_a_tenth_to_all() {
        assert_eq!(Share::ramped(0, 30_000), Share::of(6_554));
        assert_eq!(Share::ramped(15_000, 30_000), Share::of(6_554 + 58_982 / 2));
        assert_eq!(Share::ramped(29_999, 30_000), Share::of(65_534));
        assert_eq!(Share::ramped(30_000, 30_000), Share::FULL);
        assert_eq!(Share::ramped(u64::MAX, 30_000), Share::FULL);
        assert_eq!(Share::ramped(u64::MAX - 1, u64::MAX), Share::of(65_535));
        assert_eq!(Share::ramped(0, 1), Share::of(6_554));
    }

    #[test]
    fn one_endpoint_draws_nothing_for_its_ramp() {
        let mut one = Endpoints::serving(1);
        one.share[0] = Share::of(0);
        let mut drawn = 0;
        let mut random = || {
            drawn += 1;
            0
        };
        assert_eq!(p2c(&one, &Tried::default(), &mut random), Some(0));
        assert_eq!(drawn, 1, "the one draw of which endpoint");
    }

    #[test]
    fn tried_holds_a_first_try_and_five_retries() {
        let mut all = Tried::default();
        for at in 0..8 {
            all.add(at);
        }
        assert!((0..6).all(|at| all.contains(at)));
        assert!(!all.contains(6));
    }

    fn endpoints() -> impl Strategy<Value = (Endpoints, Vec<usize>)> {
        (1_usize..10).prop_flat_map(|count| {
            (
                prop::collection::vec(any::<bool>(), count),
                prop::collection::vec(0_u32..5, count),
                prop::collection::vec(
                    prop_oneof![Just(Share::FULL), (0_u32..=65_536).prop_map(Share::of)],
                    count,
                ),
                prop::collection::vec(0..count, 0..6),
            )
                .prop_map(|(serves, in_flight, share, tried)| {
                    (
                        Endpoints {
                            serves,
                            in_flight,
                            share,
                        },
                        tried,
                    )
                })
        })
    }

    proptest! {
        #[test]
        fn a_ramp_never_falls_and_never_passes_all(
            window in 1_u64..,
            elapsed in any::<u64>(),
            later in any::<u64>(),
        ) {
            let now = Share::ramped(elapsed, window);
            let then = Share::ramped(elapsed.saturating_add(later), window);
            prop_assert!(now.0 <= then.0);
            prop_assert!(then.0 <= Share::FULL.0);
            prop_assert!(now.0 >= 6_554);
        }

        #[test]
        fn p2c_takes_only_what_the_reference_allows_and_never_the_strictly_busiest(
            (endpoints, already) in endpoints(),
            seed in any::<u64>(),
        ) {
            let allowed = may_take(&endpoints, &already);
            let busiest = allowed.iter().copied().max_by_key(|&at| endpoints.in_flight[at]).unwrap();
            let strictly = allowed.iter().filter(|&&at| endpoints.in_flight[at] == endpoints.in_flight[busiest]).count() == 1;
            let mut random = generator(seed);
            for _ in 0..50 {
                let at = p2c(&endpoints, &tried(&already), &mut random).unwrap();
                prop_assert!(allowed.contains(&at), "{at} not in {allowed:?}");
                if allowed.len() > 1 && strictly {
                    prop_assert_ne!(at, busiest);
                }
            }
        }

        #[test]
        fn p2c_can_reach_every_endpoint_the_reference_allows(
            (mut endpoints, already) in endpoints(),
            seed in any::<u64>(),
        ) {
            // Equally busy and none ramping, so that every allowed endpoint can win.
            endpoints.in_flight.fill(0);
            endpoints.share.fill(Share::FULL);
            let allowed = may_take(&endpoints, &already);
            let mut random = generator(seed);
            let seen: BTreeSet<usize> = (0..2_000)
                .map(|_| p2c(&endpoints, &tried(&already), &mut random).unwrap())
                .collect();
            prop_assert_eq!(seen, allowed);
        }

        #[test]
        fn round_robin_takes_each_allowed_endpoint_once_a_round(
            (mut endpoints, already) in endpoints(),
            start in any::<usize>(),
            seed in any::<u64>(),
        ) {
            // Nothing ramping: then a round is exact.
            endpoints.share.fill(Share::FULL);
            let allowed = may_take(&endpoints, &already);
            let mut random = generator(seed);
            let mut round_robin = RoundRobin::starting_at(start);
            for _ in 0..3 {
                let round: Vec<usize> = (0..allowed.len())
                    .map(|_| round_robin.pick(&endpoints, &tried(&already), &mut random).unwrap())
                    .collect();
                let once: BTreeSet<usize> = round.iter().copied().collect();
                prop_assert_eq!(round.len(), once.len(), "{:?}", round);
                prop_assert_eq!(&once, &allowed);
            }
        }

        #[test]
        fn round_robin_takes_only_what_the_reference_allows(
            (endpoints, already) in endpoints(),
            start in any::<usize>(),
            seed in any::<u64>(),
        ) {
            let allowed = may_take(&endpoints, &already);
            let mut random = generator(seed);
            let mut round_robin = RoundRobin::starting_at(start);
            for _ in 0..50 {
                let at = round_robin.pick(&endpoints, &tried(&already), &mut random).unwrap();
                prop_assert!(allowed.contains(&at), "{at} not in {allowed:?}");
            }
        }
    }
}
