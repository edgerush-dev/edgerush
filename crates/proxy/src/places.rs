//! The places a worker has for exchanges, and which upstreams hold them
//! ([03 §9](../../../docs/03-data-plane.md), [13 §7](../../../docs/13-http1-upstream.md)).
//!
//! A worker has a fixed number of places, and an exchange holds one from before a
//! connection is looked for until its answer's body is let go of. Past the last place every
//! request is refused, as before. Short of it, the places would go to whoever asks first, so
//! one upstream that answers slowly would take them all: the number it holds is its request
//! rate times how long it takes to answer, and a hung upstream's grows until nothing is
//! left for the healthy ones beside it.
//!
//! So once a worker holds seven eighths of its places, it is short of them, and an upstream
//! that already holds its fair share is refused ([`crate::share`]: the same rule shares a
//! worker's connections among listeners). The fair share is the places split equally
//! among the upstreams holding any, the one asking counted — and never among fewer than two
//! while the config has another upstream. A healthy upstream that answers in a millisecond
//! holds a place only for that millisecond, so most of the time it holds none: counted only
//! by what it holds, it would leave the slow one alone, free to take every place, and be
//! refused the moment it asked. With the floor, the last eighth stays for whoever else
//! comes. An upstream under its share is never refused while a place is free, and the only
//! upstream a config has may take every place. Nothing is sized to a backend, so nothing
//! goes stale when one scales (03 §6), and a worker with room does nothing but count.
//!
//! What an upstream holds is a count of its own, kept with the rest of the worker's state
//! for it ([`crate::upstream::balancing`]), which a reload carries over by the upstream's
//! name and lets go of when no config has it and nothing holds a place of it. A request
//! reaches it through the state it was directed with: no lookup, no table to outgrow. Its
//! metrics slot is no part of it, so upstreams past the last slot, which share a series,
//! still hold their places apart.
//!
//! One per worker, and it never leaves it. Nothing here does I/O or reads a clock.

#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::share::over_share;
use std::cell::Cell;
use std::rc::Rc;

/// One worker's places, and how many upstreams hold them.
#[derive(Debug)]
pub struct Places {
    limit: usize,
    /// Places held, by anyone.
    held: Cell<usize>,
    /// How many upstreams hold at least one place.
    holding: Cell<usize>,
}

/// Why a place was not given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// Every place is held.
    Full,
    /// The worker is short of places and the upstream already holds its fair share.
    OverShare,
}

impl Places {
    /// A worker's `limit` places, none held.
    pub fn new(limit: usize) -> Rc<Self> {
        Rc::new(Self {
            limit,
            held: Cell::new(0),
            holding: Cell::new(0),
        })
    }

    /// A place for an exchange with the upstream whose count of places held is `holds`, if
    /// one is going to it. `alone` says the config it was directed by has no other upstream.
    ///
    /// # Errors
    ///
    /// [`Refused::Full`] when every place is held; [`Refused::OverShare`] when the worker is
    /// short of places and the upstream holds its fair share already.
    pub fn take(self: &Rc<Self>, holds: &Rc<Cell<usize>>, alone: bool) -> Result<Place, Refused> {
        let held = self.held.get();
        if held >= self.limit {
            return Err(Refused::Full);
        }
        let own = holds.get();
        if over_share(self.limit, held, own, self.holding.get(), alone) {
            return Err(Refused::OverShare);
        }
        if own == 0 {
            self.holding.set(self.holding.get() + 1);
        }
        holds.set(own + 1);
        self.held.set(held + 1);
        Ok(Place {
            places: Rc::clone(self),
            holds: Rc::clone(holds),
        })
    }

    /// How many places are held.
    #[must_use]
    pub fn held(&self) -> usize {
        self.held.get()
    }

    fn give_back(&self, holds: &Cell<usize>) {
        if holds.get() > 0 {
            let own = holds.get() - 1;
            holds.set(own);
            if own == 0 {
                self.holding.set(self.holding.get().saturating_sub(1));
            }
            self.held.set(self.held.get().saturating_sub(1));
        }
    }
}

/// A place held. Dropping it is the only way to give it back, so every way an exchange ends
/// gives it back — to its upstream's count, whatever reloads came in the meantime.
#[derive(Debug)]
pub struct Place {
    places: Rc<Places>,
    holds: Rc<Cell<usize>>,
}

impl Drop for Place {
    fn drop(&mut self) {
        self.places.give_back(&self.holds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// What the configs of most tests have: other upstreams.
    const SEVERAL: bool = false;

    /// Counts enough for every upstream the tests name.
    const SLOTS: usize = 8;

    /// A count of places held for each upstream the tests name.
    fn counts() -> Vec<Rc<Cell<usize>>> {
        (0..SLOTS).map(|_| Rc::new(Cell::new(0))).collect()
    }

    fn take(places: &Rc<Places>, upstream: &Rc<Cell<usize>>, times: usize) -> Vec<Place> {
        (0..times)
            .map(|_| places.take(upstream, SEVERAL).expect("a place"))
            .collect()
    }

    #[test]
    fn the_only_upstream_of_a_config_may_take_every_place() {
        let places = Places::new(16);
        let up = counts();
        let held: Vec<Place> = (0..16)
            .map(|_| places.take(&up[3], true).expect("a place"))
            .collect();
        assert_eq!(places.held(), 16);
        assert_eq!(up[3].get(), 16);
        assert_eq!(places.take(&up[3], true).err(), Some(Refused::Full));
        drop(held);
        assert_eq!(places.held(), 0);
    }

    #[test]
    fn nobody_is_refused_a_place_before_the_worker_is_short_of_them() {
        // 16 places: short from 14. One upstream takes 13, so the 14th place is still given
        // whoever asks, however much the asker already holds.
        let places = Places::new(16);
        let up = counts();
        let _slow = take(&places, &up[1], 13);
        let _fourteenth = places.take(&up[1], SEVERAL).expect("not short yet");
        assert_eq!(places.held(), 14);
    }

    #[test]
    fn once_short_an_upstream_over_its_share_is_refused_and_the_others_are_not() {
        let places = Places::new(16);
        let up = counts();
        let _slow = take(&places, &up[1], 14);
        // Nobody else holds a place, but the config has others: 8 each at the least.
        assert_eq!(places.take(&up[1], SEVERAL).err(), Some(Refused::OverShare));
        let _healthy = take(&places, &up[2], 2);
        assert_eq!(places.held(), 16);
        assert_eq!(places.take(&up[2], SEVERAL).err(), Some(Refused::Full));
    }

    #[test]
    fn a_share_is_of_the_upstreams_holding_places_now() {
        let places = Places::new(16);
        let up = counts();
        let a = take(&places, &up[1], 7);
        let _b = take(&places, &up[2], 7);
        // Short; two hold places, so 8 each, and a third asking makes it 5 each.
        assert_eq!(places.held(), 14);
        let a_eighth = places.take(&up[1], SEVERAL).expect("7 of a share of 8");
        assert_eq!(places.take(&up[1], SEVERAL).err(), Some(Refused::OverShare));
        let _c = places
            .take(&up[3], SEVERAL)
            .expect("holds none of a share of 5");
        // Once the first lets go of all it held, there is room again.
        drop(a);
        drop(a_eighth);
        assert_eq!(places.held(), 8);
        assert!(places.take(&up[2], SEVERAL).is_ok(), "not short any more");
    }

    #[test]
    fn letting_a_place_go_frees_it_for_its_upstream_and_the_count() {
        let places = Places::new(16);
        let up = counts();
        let mut slow = take(&places, &up[1], 14);
        assert_eq!(places.take(&up[1], SEVERAL).err(), Some(Refused::OverShare));
        slow.truncate(13);
        assert_eq!(places.held(), 13);
        assert_eq!(up[1].get(), 13);
        assert!(
            places.take(&up[1], SEVERAL).is_ok(),
            "not short with 13 of 16 held"
        );
    }

    #[test]
    fn a_worker_with_no_places_refuses_everyone_as_full() {
        let places = Places::new(0);
        let up = counts();
        assert_eq!(places.take(&up[0], SEVERAL).err(), Some(Refused::Full));
    }

    /// A place goes back to the count it was taken from, whoever else holds that count by
    /// then — a worker's state for its upstream made again by a reload shares it.
    #[test]
    fn a_place_goes_back_to_the_count_it_was_taken_from() {
        let places = Places::new(16);
        let before = Rc::new(Cell::new(0));
        let place = places.take(&before, SEVERAL).expect("a place");
        let after = Rc::clone(&before);
        drop(before);
        assert_eq!(after.get(), 1);
        drop(place);
        assert_eq!(after.get(), 0);
        assert_eq!(places.held(), 0);
    }

    #[test]
    fn a_worker_with_fewer_than_eight_places_is_never_short_before_it_is_full() {
        let places = Places::new(7);
        let up = counts();
        let _all = take(&places, &up[1], 7);
        assert_eq!(places.take(&up[2], SEVERAL).err(), Some(Refused::Full));
    }

    #[derive(Debug, Clone)]
    enum Step {
        Take(usize),
        /// Lets go of the place at this position among those held, wrapped.
        Release(usize),
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            3 => (0usize..6).prop_map(Step::Take),
            1 => any::<usize>().prop_map(Step::Release),
        ]
    }

    /// The rule as it is said, counting everything afresh from the places held.
    fn reference(
        limit: usize,
        alive: &[usize],
        upstream: usize,
        upstreams: usize,
    ) -> Result<(), Refused> {
        let held = alive.len();
        if held >= limit {
            return Err(Refused::Full);
        }
        if held < limit - limit / 8 {
            return Ok(());
        }
        let own = alive.iter().filter(|held| **held == upstream).count();
        let mut sharing: Vec<usize> = alive.to_vec();
        sharing.push(upstream);
        sharing.sort_unstable();
        sharing.dedup();
        let sharing = if upstreams > 1 {
            sharing.len().max(2)
        } else {
            sharing.len()
        };
        if own >= limit / sharing {
            Err(Refused::OverShare)
        } else {
            Ok(())
        }
    }

    proptest! {
        /// Against the rule counted afresh on every step, whatever is taken and let go of in
        /// whatever order: every place is given or refused as the rule says, and what is
        /// held, in all and by each upstream, is what the places alive say.
        #[test]
        fn places_are_given_exactly_as_the_rule_says(
            limit in 0usize..=40,
            upstreams in 1usize..=6,
            steps in proptest::collection::vec(step(), 0..160),
        ) {
            let places = Places::new(limit);
        let up = counts();
            let mut alive: Vec<(usize, Place)> = Vec::new();
            for step in steps {
                match step {
                    Step::Take(upstream) => {
                        let upstream = upstream % upstreams;
                        let holders: Vec<usize> = alive.iter().map(|(u, _)| *u).collect();
                        let expected = reference(limit, &holders, upstream, upstreams);
                        let given = places.take(&up[upstream], upstreams < 2);
                        prop_assert_eq!(given.as_ref().map(|_| ()).map_err(|e| *e), expected);
                        if let Ok(place) = given {
                            alive.push((upstream, place));
                        }
                    }
                    Step::Release(at) => {
                        if !alive.is_empty() {
                            let at = at % alive.len();
                            alive.swap_remove(at);
                        }
                    }
                }
                prop_assert!(places.held() <= limit);
                prop_assert_eq!(places.held(), alive.len());
                for (upstream, count) in up.iter().enumerate().take(6) {
                    let own = alive.iter().filter(|(u, _)| *u == upstream).count();
                    prop_assert_eq!(count.get(), own);
                }
            }
            drop(alive);
            prop_assert_eq!(places.held(), 0);
        }

        /// Whatever the others hold, an upstream under its fair share is given a place
        /// while any is free, and nobody is refused while the worker is not short.
        #[test]
        fn under_its_share_or_with_room_an_upstream_is_never_refused(
            limit in 1usize..=64,
            holdings in proptest::collection::vec(0usize..12, 1..6),
            asker in 0usize..6,
        ) {
            let places = Places::new(limit);
        let up = counts();
            let mut alive = Vec::new();
            for (upstream, count) in holdings.iter().enumerate() {
                for _ in 0..*count {
                    if let Ok(place) = places.take(&up[upstream], false) {
                        alive.push(place);
                    }
                }
            }
            let held = places.held();
            let own = up[asker].get();
            let mut sharing = (0..6).filter(|u| up[*u].get() > 0).count();
            if own == 0 {
                sharing += 1;
            }
            let sharing = sharing.max(2);
            let given = places.take(&up[asker], false);
            if held < limit - limit / 8 || (held < limit && own < limit / sharing) {
                prop_assert!(given.is_ok(), "refused with {held} of {limit}, {own} of a share");
            }
        }

        /// However long one upstream keeps asking, with others in the config it never
        /// holds more than the worker's first seven eighths, and whatever is left is there
        /// for the others when they come.
        #[test]
        fn one_upstream_never_takes_the_last_eighth_from_the_others(
            limit in 0usize..=2048,
            upstreams in 2usize..=6,
        ) {
            let places = Places::new(limit);
        let up = counts();
            let mut held = Vec::new();
            while let Ok(place) = places.take(&up[0], false) {
                held.push(place);
            }
            let short = limit - limit / 8;
            prop_assert!(held.len() <= short.max(limit / 2), "{} of {limit}", held.len());
            for (other, count) in up.iter().enumerate().take(upstreams).skip(1) {
                if places.held() < limit {
                    let given = places.take(count, false);
                    prop_assert!(given.is_ok(), "upstream {other} refused");
                    held.extend(given.ok());
                }
            }
        }
    }
}
