//! What clients were told under a rule, judged by the rule's own reference: each admission the
//! reference, fed what clients were told before it, would refuse is an overshoot; each
//! refusal it would admit is a refusal within the rate. Every decision is judged at the time
//! its key's owner made it, by that owner's wall clock.

use edgerush_cluster::Time;
use edgerush_cluster_synchronous::Rule;
use std::collections::BTreeMap;

/// The decisions under one rule.
#[derive(Clone, Debug)]
pub struct Oracle {
    rule: Rule,
    /// Each key's admissions as clients were told, when, and whether `when_unreachable`
    /// admitted it without a count.
    admitted: BTreeMap<u64, Vec<(Time, bool)>>,
    /// Requests refused under the rule, each key with when its owner refused or admitted it.
    limited: Vec<Vec<(u64, Time)>>,
    /// Requests `when_unreachable` refused, each key with the time.
    refused: Vec<Vec<(u64, Time)>>,
}

impl Oracle {
    /// An oracle of `rule`, nothing told yet.
    #[must_use]
    pub const fn new(rule: Rule) -> Self {
        Self {
            rule,
            admitted: BTreeMap::new(),
            limited: Vec::new(),
            refused: Vec::new(),
        }
    }

    /// `key` admitted at `at`, counted by its owner or, `uncounted`, by nobody.
    pub fn admitted(&mut self, key: u64, at: Time, uncounted: bool) {
        self.admitted.entry(key).or_default().push((at, uncounted));
    }

    /// A request refused under the rule, each key with its decision's time.
    pub fn limited(&mut self, parts: Vec<(u64, Time)>) {
        self.limited.push(parts);
    }

    /// A request `when_unreachable` refused, each key with the time.
    pub fn refused(&mut self, parts: Vec<(u64, Time)>) {
        self.refused.push(parts);
    }

    /// `key`'s admissions in time order.
    fn history(&self, key: u64) -> Vec<(Time, bool)> {
        let mut history = self.admitted.get(&key).cloned().unwrap_or_default();
        history.sort_unstable();
        history
    }

    /// Whether the rule's reference, fed every admission told under `key` before `at`, would
    /// admit one at `at`.
    fn room(&self, key: u64, at: Time) -> bool {
        let before: Vec<Time> = self
            .history(key)
            .into_iter()
            .map(|(time, _)| time)
            .filter(|time| *time < at)
            .collect();
        self.admits(key, &before, at)
    }

    /// Whether the rule's reference, having admitted `before` under `key`, in time order,
    /// admits one at `at`.
    fn admits(&self, key: u64, before: &[Time], at: Time) -> bool {
        let before = before.iter().copied();
        match self.rule {
            Rule::Window { per, .. } => {
                let window = self.rule.window_of(key, at);
                let held = before
                    .filter(|time| self.rule.window_of(key, *time) == window)
                    .count();
                (held as u64) < per
            }
            Rule::Rolling { per, window } => {
                let held = before.filter(|time| *time + window > at).count();
                (held as u64) < per
            }
            Rule::Bucket { per, period, burst } => {
                let (interval, tolerance) = Rule::interval_and_tolerance(per, period, burst);
                let due = before.fold(0, |due: Time, time| due.max(time) + interval);
                due.max(at) - at <= tolerance
            }
        }
    }

    /// What the rule's reference would admit if asked every request clients asked, each at
    /// the time it was decided, against what clients were told was admitted, summed over keys:
    /// the allowance clients lost, and what they were admitted beyond it, what
    /// `when_unreachable` admitted without a count aside. Each part of a request counts as
    /// asked under its key, so it is a measure for requests under one key each.
    #[must_use]
    pub fn allowance(&self) -> (u64, u64) {
        let counted = |admitted: &Vec<(Time, bool)>| {
            admitted
                .iter()
                .filter(|(_, uncounted)| !uncounted)
                .map(|(time, _)| *time)
                .collect::<Vec<_>>()
        };
        let mut asked: BTreeMap<u64, Vec<Time>> = BTreeMap::new();
        for (key, admitted) in &self.admitted {
            asked.entry(*key).or_default().extend(counted(admitted));
        }
        for (key, at) in self.limited.iter().chain(&self.refused).flatten() {
            asked.entry(*key).or_default().push(*at);
        }
        asked
            .into_iter()
            .fold((0, 0), |(lost, beyond), (key, mut asked)| {
                asked.sort_unstable();
                let mut promised = Vec::new();
                for at in asked {
                    if self.admits(key, &promised, at) {
                        promised.push(at);
                    }
                }
                let promised = promised.len() as u64;
                let told = self.admitted.get(&key).map_or(0, |all| counted(all).len()) as u64;
                (
                    lost + promised.saturating_sub(told),
                    beyond + told.saturating_sub(promised),
                )
            })
    }

    /// Requests refused under the rule while every key they were under had room: refusals
    /// within the rate.
    #[must_use]
    pub fn shaved(&self) -> u64 {
        self.within(&self.limited)
    }

    /// Requests `when_unreachable` refused while every key they were under had room.
    #[must_use]
    pub fn refused_with_room(&self) -> u64 {
        self.within(&self.refused)
    }

    fn within(&self, requests: &[Vec<(u64, Time)>]) -> u64 {
        requests
            .iter()
            .filter(|parts| !parts.is_empty() && parts.iter().all(|(key, at)| self.room(*key, *at)))
            .count() as u64
    }

    /// Admissions the reference would have refused, fed those before them.
    #[must_use]
    pub fn overshoot(&self) -> u64 {
        self.admitted
            .keys()
            .map(|key| self.beyond(*key, true).0)
            .sum()
    }

    /// The most a key was admitted beyond the rule at any one time by its owners, what
    /// `when_unreachable` admitted without a count aside: for a window, beyond its limit in a
    /// window; for a rolling window, beyond its limit in any span of 99.9% of the window, the
    /// slots rounding down; for a bucket, admitted ahead of its burst.
    #[must_use]
    pub fn overshoot_peak(&self) -> u64 {
        self.admitted
            .keys()
            .map(|key| self.beyond(*key, false).1)
            .max()
            .unwrap_or(0)
    }

    /// For `key`: admissions the reference refuses, and the most beyond the rule at once —
    /// counting what `when_unreachable` admitted, if `all`.
    fn beyond(&self, key: u64, all: bool) -> (u64, u64) {
        let history: Vec<Time> = self
            .history(key)
            .into_iter()
            .filter(|(_, uncounted)| all || !uncounted)
            .map(|(time, _)| time)
            .collect();
        match self.rule {
            Rule::Window { per, .. } => {
                let mut by_window: BTreeMap<u64, u64> = BTreeMap::new();
                for time in &history {
                    *by_window
                        .entry(self.rule.window_of(key, *time))
                        .or_insert(0) += 1;
                }
                let over = by_window.values().map(|held| held.saturating_sub(per));
                (over.clone().sum(), over.max().unwrap_or(0))
            }
            Rule::Rolling { per, window } => {
                // Within 99.9% of the window, as the slots, rounding down, promise.
                let span = window - window / 1_000;
                let mut beyond = 0;
                let mut most = 0;
                for (index, time) in history.iter().enumerate() {
                    let held = history[..=index]
                        .iter()
                        .filter(|earlier| **earlier + span > *time)
                        .count() as u64;
                    if held > per {
                        beyond += 1;
                        most = most.max(held - per);
                    }
                }
                (beyond, most)
            }
            Rule::Bucket { per, period, burst } => {
                let (interval, tolerance) = Rule::interval_and_tolerance(per, period, burst);
                let mut due: Time = 0;
                let mut beyond = 0;
                let mut most = 0;
                for time in history {
                    let next = due.max(time);
                    if next - time > tolerance {
                        beyond += 1;
                        most = most.max((next - time - tolerance).div_ceil(interval.max(1)));
                    }
                    due = next + interval;
                }
                (beyond, most)
            }
        }
    }

    /// What a key's owner holds beyond what clients were told, for a window: the requests it
    /// counted in its window `window`, `held`, less those told in it.
    #[must_use]
    pub fn beyond_told(&self, key: u64, window: u64, held: u64) -> u64 {
        let told = self
            .history(key)
            .iter()
            .filter(|(time, _)| self.rule.window_of(key, *time) == window)
            .count() as u64;
        held.saturating_sub(told)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::ms;

    /// The oracle judging `refusals` after `admissions`, all under key 1: how many it counts
    /// refused within the rule.
    fn shaved(rule: Rule, admissions: &[Time], refusals: &[Time]) -> u64 {
        let mut oracle = Oracle::new(rule);
        for at in admissions {
            oracle.admitted(1, *at, false);
        }
        for at in refusals {
            oracle.limited(vec![(1, *at)]);
        }
        oracle.shaved()
    }

    /// Under a fixed window, a refusal in a full window is no shave; one in the next is.
    #[test]
    fn a_refusal_in_the_next_window_is_a_shave() {
        let rule = Rule::Window {
            per: 2,
            window: ms(1_000),
        };
        let start = (0..ms(1_000))
            .find(|at| rule.window_of(1, *at) != rule.window_of(1, 0))
            .unwrap();
        let full = [start, start + ms(10)];
        assert_eq!(shaved(rule, &full, &[start + ms(500)]), 0);
        assert_eq!(shaved(rule, &full, &[start + ms(1_000)]), 1);
    }

    /// Under a rolling window, a refusal while the window holds its limit is no shave; one
    /// once the oldest admission has left the window is.
    #[test]
    fn a_refusal_once_an_admission_leaves_a_rolling_window_is_a_shave() {
        let rule = Rule::Rolling {
            per: 2,
            window: ms(1_000),
        };
        let full = [ms(10), ms(500)];
        assert_eq!(shaved(rule, &full, &[ms(1_009)]), 0);
        assert_eq!(shaved(rule, &full, &[ms(1_011)]), 1);
    }

    /// Under a bucket, a refusal before a request has dripped back is no shave; one once it
    /// has is.
    #[test]
    fn a_refusal_once_a_bucket_has_dripped_back_is_a_shave() {
        let rule = Rule::Bucket {
            per: 2,
            period: ms(1_000),
            burst: 2,
        };
        let full = [ms(5_000), ms(5_000)];
        assert_eq!(shaved(rule, &full, &[ms(5_499)]), 0);
        assert_eq!(shaved(rule, &full, &[ms(5_500)]), 1);
    }

    /// One request refused with room is one admission of allowance lost, however many
    /// refusals follow it, and one admitted beyond the burst is one beyond it, what
    /// `when_unreachable` admitted without a count aside.
    #[test]
    fn allowance_counts_admissions_lost_or_beyond_once() {
        let rule = Rule::Bucket {
            per: 2,
            period: ms(1_000),
            burst: 2,
        };
        let mut oracle = Oracle::new(rule);
        for at in [ms(5_000), ms(5_000), ms(6_000), ms(6_600), ms(7_000)] {
            oracle.admitted(1, at, false);
        }
        for at in [ms(5_500), ms(6_100), ms(7_100)] {
            oracle.limited(vec![(1, at)]);
        }
        assert_eq!(oracle.shaved(), 3);
        assert_eq!(oracle.allowance(), (1, 0));
        let mut oracle = Oracle::new(rule);
        for _ in 0..3 {
            oracle.admitted(1, ms(5_000), false);
        }
        oracle.admitted(1, ms(5_100), true);
        assert_eq!(oracle.allowance(), (0, 1));
    }
}
