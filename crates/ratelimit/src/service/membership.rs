//! Which service pods are serving, as a majority of them agree (the rate-limiting service
//! note: one broken link must not make two owners).
//!
//! Every pod sends every other a [`Heartbeat`] a few times a second: who it is in this life
//! (its incarnation, new at every start), the view it holds, and which pods it hears from.
//! A pod unheard for `silence` is suspected. Only a side that hears from a majority of the
//! configured pods may change the view, and on it only the lowest-numbered pod — the leader
//! — does, when every pod it hears agrees: a pod leaves when none of them hears it, and
//! joins when all of them have heard it, in this life, for `settle`. A pod that restarted
//! leaves first and joins again, so that the pods holding its keys hand them over; until a
//! view takes it in, it serves nothing.
//!
//! Nor does a pod serve while it hears no majority (06 §4): one that has heard none for
//! `stop_serving`, two heartbeats shorter than `silence` at least, stops, and serves again
//! once it hears a majority. Cut off from the rest, it stops before they can take it out of the view and give
//! its keys to another; that narrows the time two pods serve one key, and does not rule it
//! out: one whose own heartbeats are lost while it still hears a majority is taken out while
//! it serves, and stops only once it hears of it. Alone, it cannot tell being cut off from
//! every pod being cut off, so when no pod hears a majority, none serves. Each time it loses sight of a majority, finds one
//! again, and the time it goes without, are counted ([`Sight`]).

use super::{PodId, View};
use std::collections::BTreeMap;

/// A view, and the life of each of its pods it took in: a pod restarted since is not the
/// pod it took in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Agreed {
    /// The view, as data planes are given it.
    pub view: View,
    /// For each of its pods, the incarnation taken in.
    pub incarnations: Vec<(PodId, u64)>,
}

/// What a pod tells every other, a few times a second.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heartbeat {
    /// The pod.
    pub from: PodId,
    /// Its life: new at every start.
    pub incarnation: u64,
    /// The view it holds.
    pub agreed: Agreed,
    /// The pods it hears from, in the lives it hears them in, itself among them.
    pub hears: Vec<(PodId, u64)>,
}

/// What a pod last heard of another.
struct Heard {
    incarnation: u64,
    /// When last heard from.
    last: u64,
    /// Since when heard from in this life.
    since: u64,
    hears: Vec<(PodId, u64)>,
}

/// How often a pod has gone without hearing a majority, and for how long.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sight {
    /// The times it lost sight of a majority, after hearing one: it stopped serving.
    pub lost: u64,
    /// The times it heard a majority again, after losing sight of one.
    pub regained: u64,
    /// The time it has gone without, in nanoseconds: from each loss, when it stopped, to
    /// the majority heard again, or to now.
    pub without: u64,
}

/// One pod's part in agreeing on the view.
pub struct Membership {
    me: PodId,
    incarnation: u64,
    started: u64,
    members: Vec<PodId>,
    silence: u64,
    settle: u64,
    stop_serving: u64,
    agreed: Agreed,
    heard: BTreeMap<PodId, Heard>,
    /// Whether it heard a majority within `stop_serving` when it last looked.
    majority: bool,
    sight: Sight,
    /// Since when it has gone without a majority, after hearing one.
    without_since: Option<u64>,
}

impl Membership {
    /// Pod `me` in its life `incarnation`, started at `now`, among the configured `members`
    /// (itself too); others are suspected after `silence` unheard, and taken in after being
    /// heard for `settle`; it stops serving after `stop_serving` without hearing a
    /// majority, which is to be two heartbeats shorter than `silence` at least: each side
    /// may have heard the other last up to a heartbeat apart, and look up to one late. It holds no view yet: every member,
    /// at epoch 0, which serves nothing until a leader makes the first.
    #[must_use]
    pub fn new(
        me: PodId,
        incarnation: u64,
        members: Vec<PodId>,
        silence: u64,
        settle: u64,
        stop_serving: u64,
        now: u64,
    ) -> Self {
        let view = View {
            epoch: 0,
            pods: members.clone(),
        };
        Self {
            me,
            incarnation,
            started: now,
            members,
            silence,
            settle,
            stop_serving,
            agreed: Agreed {
                view,
                incarnations: Vec::new(),
            },
            heard: BTreeMap::new(),
            majority: false,
            sight: Sight::default(),
            without_since: None,
        }
    }

    /// The view it holds.
    #[must_use]
    pub fn view(&self) -> &View {
        &self.agreed.view
    }

    /// The pods the view it holds took in, each with the life it took it in.
    #[must_use]
    pub fn lives(&self) -> &[(PodId, u64)] {
        &self.agreed.incarnations
    }

    /// Whether it serves: the view it holds took it in, in this life, and it heard a
    /// majority within `stop_serving` when it last looked, at its last heartbeat in or tick.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.majority
            && self
                .agreed
                .incarnations
                .contains(&(self.me, self.incarnation))
    }

    /// How often it has gone without hearing a majority, and for how long, as of `now`.
    #[must_use]
    pub fn sight(&self, now: u64) -> Sight {
        let mut sight = self.sight;
        if let Some(since) = self.without_since {
            sight.without = sight.without.saturating_add(now.saturating_sub(since));
        }
        sight
    }

    /// Whether it hears a majority at `now`, within `stop_serving`, counted as it changes.
    fn look(&mut self, now: u64) {
        let heard =
            self.members
                .iter()
                .filter(|pod| {
                    **pod == self.me
                        || self.heard.get(pod).is_some_and(|heard| {
                            now.saturating_sub(heard.last) <= self.stop_serving
                        })
                })
                .count();
        let majority = heard * 2 > self.members.len();
        match (self.majority, majority) {
            (true, false) => {
                self.sight.lost += 1;
                self.without_since = Some(now);
            }
            (false, true) => {
                // The first majority it hears is no regaining: it never had one to lose.
                if let Some(since) = self.without_since.take() {
                    self.sight.regained += 1;
                    self.sight.without =
                        self.sight.without.saturating_add(now.saturating_sub(since));
                }
            }
            _ => {}
        }
        self.majority = majority;
    }

    fn alive(&self, pod: PodId, now: u64) -> bool {
        pod == self.me
            || self
                .heard
                .get(&pod)
                .is_some_and(|heard| now.saturating_sub(heard.last) <= self.silence)
    }

    /// The pods it hears from at `now`, in their lives, itself first.
    fn hears(&self, now: u64) -> Vec<(PodId, u64)> {
        std::iter::once((self.me, self.incarnation))
            .chain(
                self.heard
                    .iter()
                    .filter(|(pod, _)| self.alive(**pod, now))
                    .map(|(pod, heard)| (*pod, heard.incarnation)),
            )
            .collect()
    }

    /// Its heartbeat at `now`, for every other member.
    #[must_use]
    pub fn heartbeat(&self, now: u64) -> Heartbeat {
        Heartbeat {
            from: self.me,
            incarnation: self.incarnation,
            agreed: self.agreed.clone(),
            hears: self.hears(now),
        }
    }

    /// Another pod's heartbeat reaches it at `now`. A later view than its own is taken
    /// over, and returned.
    pub fn on_heartbeat(&mut self, heartbeat: Heartbeat, now: u64) -> Option<View> {
        if heartbeat.from == self.me || !self.members.contains(&heartbeat.from) {
            return None;
        }
        let heard = self.heard.entry(heartbeat.from).or_insert(Heard {
            incarnation: heartbeat.incarnation,
            last: now,
            since: now,
            hears: Vec::new(),
        });
        // A new life, or silence long enough to be suspected: heard afresh from now.
        if heard.incarnation != heartbeat.incarnation
            || now.saturating_sub(heard.last) > self.silence
        {
            heard.incarnation = heartbeat.incarnation;
            heard.since = now;
        }
        heard.last = now;
        heard.hears = heartbeat.hears;
        self.look(now);
        if heartbeat.agreed.view.epoch > self.agreed.view.epoch {
            self.agreed = heartbeat.agreed;
            return Some(self.agreed.view.clone());
        }
        None
    }

    /// Its timer at `now`: as the leader of a majority, it changes the view if every pod it
    /// hears agrees; the new view is returned.
    pub fn on_tick(&mut self, now: u64) -> Option<View> {
        self.look(now);
        let alive: Vec<PodId> = self
            .members
            .iter()
            .copied()
            .filter(|pod| self.alive(*pod, now))
            .collect();
        if alive.len() * 2 <= self.members.len() || alive.iter().min() != Some(&self.me) {
            return None;
        }
        let others: Vec<&Heard> = alive
            .iter()
            .filter(|pod| **pod != self.me)
            .filter_map(|pod| self.heard.get(pod))
            .collect();
        let life = |pod: PodId| {
            if pod == self.me {
                Some((self.incarnation, self.started))
            } else {
                self.heard
                    .get(&pod)
                    .map(|heard| (heard.incarnation, heard.since))
            }
        };
        let mut incarnations = Vec::new();
        for &pod in &self.members {
            let taken = self
                .agreed
                .incarnations
                .iter()
                .find(|(p, _)| *p == pod)
                .map(|(_, incarnation)| *incarnation);
            let keep = match (taken, life(pod)) {
                // In the view, in the life it took in, and heard: stays.
                (Some(taken), Some((incarnation, _)))
                    if taken == incarnation && self.alive(pod, now) =>
                {
                    true
                }
                // Restarted since: leaves now, to join again later.
                (Some(taken), Some((incarnation, _))) if taken != incarnation => false,
                // Unheard: leaves only if none of the majority hears it either.
                (Some(taken), _) => others
                    .iter()
                    .any(|heard| heard.hears.contains(&(pod, taken))),
                // Not in the view: joins once heard by all for long enough.
                (None, Some((incarnation, since))) => {
                    self.alive(pod, now)
                        && now.saturating_sub(since) >= self.settle
                        && others
                            .iter()
                            .all(|heard| heard.hears.contains(&(pod, incarnation)))
                }
                (None, None) => false,
            };
            if keep {
                let incarnation = taken
                    .filter(|t| life(pod).is_none_or(|(i, _)| i == *t))
                    .or(life(pod).map(|(incarnation, _)| incarnation));
                if let Some(incarnation) = incarnation {
                    incarnations.push((pod, incarnation));
                }
            }
        }
        if incarnations == self.agreed.incarnations && self.agreed.view.epoch > 0 {
            return None;
        }
        if incarnations.is_empty() {
            return None;
        }
        // Numbered to keep rising across the service's lives, not only within one: data
        // planes take only a higher epoch than they hold, so a service restarted whole and
        // counting from 1 again would go unheard. At least the leader's life, its start on
        // the wall clock, in milliseconds: the new service's leader started after every pod
        // of the old one, which made fewer views than it lived milliseconds, a view waiting
        // out `settle` at the least.
        let started_ms = self.incarnation / 1_000_000;
        self.agreed = Agreed {
            view: View {
                epoch: (self.agreed.view.epoch + 1).max(started_ms),
                pods: incarnations.iter().map(|(pod, _)| *pod).collect(),
            },
            incarnations,
        };
        Some(self.agreed.view.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;
    const SILENCE: u64 = 500 * MS;
    const SETTLE: u64 = 300 * MS;
    const STOP_SERVING: u64 = 250 * MS;

    /// Three pods and which links between them carry heartbeats.
    struct Cluster {
        pods: Vec<Membership>,
        up: Vec<bool>,
        cut: Vec<(PodId, PodId)>,
    }

    impl Cluster {
        fn new() -> Self {
            Self::living(1)
        }

        /// Three pods started together, each in its life `incarnation`.
        fn living(incarnation: u64) -> Self {
            Self {
                pods: (0..3)
                    .map(|p| {
                        Membership::new(
                            p,
                            incarnation,
                            vec![0, 1, 2],
                            SILENCE,
                            SETTLE,
                            STOP_SERVING,
                            0,
                        )
                    })
                    .collect(),
                up: vec![true; 3],
                cut: Vec::new(),
            }
        }

        fn linked(&self, a: PodId, b: PodId) -> bool {
            self.up[a as usize]
                && self.up[b as usize]
                && !self.cut.contains(&(a, b))
                && !self.cut.contains(&(b, a))
        }

        /// Heartbeats and ticks every 100 ms from `from` to `until`.
        fn run(&mut self, from: u64, until: u64) {
            let mut now = from;
            while now <= until {
                let beats: Vec<Heartbeat> = self.pods.iter().map(|p| p.heartbeat(now)).collect();
                for (to, pod) in self.pods.iter_mut().enumerate() {
                    for beat in &beats {
                        if beat.from != to as PodId && self.up[to] && {
                            let (a, b) = (beat.from, to as PodId);
                            self.up[a as usize]
                                && self.up[b as usize]
                                && !self.cut.contains(&(a, b))
                                && !self.cut.contains(&(b, a))
                        } {
                            pod.on_heartbeat(beat.clone(), now);
                        }
                    }
                }
                for (index, pod) in self.pods.iter_mut().enumerate() {
                    if self.up[index] {
                        pod.on_tick(now);
                    }
                }
                now += 100 * MS;
            }
        }

        fn view(&self, pod: usize) -> (u64, Vec<PodId>) {
            let view = self.pods[pod].view();
            (view.epoch, view.pods.clone())
        }

        fn restart(&mut self, pod: usize, incarnation: u64, now: u64) {
            self.pods[pod] = Membership::new(
                pod as PodId,
                incarnation,
                vec![0, 1, 2],
                SILENCE,
                SETTLE,
                STOP_SERVING,
                now,
            );
        }
    }

    /// A service restarted whole numbers its views above anything the old one made: a
    /// data plane, which takes only a higher epoch than it holds, takes the new service's
    /// views at once, and never the old service's again. Lives are start times, in
    /// nanoseconds of the wall clock, as the service pods give them.
    #[test]
    fn a_service_restarted_whole_numbers_its_views_above_the_old_one() {
        let started = 1_790_000_000_000_000_000; // some day in 2026
        let mut old = Cluster::living(started);
        old.run(0, 600 * MS);
        // Some views later: a pod goes and comes back.
        old.up[2] = false;
        old.run(700 * MS, 2_000 * MS);
        old.up[2] = true;
        old.restart(2, started + 2_000 * MS, 2_000 * MS);
        old.run(2_100 * MS, 4_000 * MS);
        let (last, pods) = old.view(0);
        assert_eq!(pods, [0, 1, 2]);
        // The whole service again, ten seconds later, every pod new.
        let mut new = Cluster::living(started + 14_000 * MS);
        new.run(0, 600 * MS);
        let (first, pods) = new.view(0);
        assert_eq!(pods, [0, 1, 2]);
        assert!(
            first > last,
            "the new service's first view {first} after the old one's last {last}"
        );
    }

    #[test]
    fn three_pods_starting_together_agree_on_all_three() {
        let mut cluster = Cluster::new();
        cluster.run(0, 200 * MS);
        assert_eq!(
            cluster.view(0).0,
            0,
            "not before they have heard each other for a while"
        );
        assert!(!cluster.pods[0].serving());
        cluster.run(300 * MS, 600 * MS);
        for pod in 0..3 {
            assert_eq!(cluster.view(pod), (1, vec![0, 1, 2]));
            assert!(cluster.pods[pod].serving());
        }
    }

    #[test]
    fn a_pod_gone_leaves_the_view_once_the_majority_agrees() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.up[0] = false;
        cluster.run(700 * MS, 1_100 * MS);
        assert_eq!(
            cluster.view(1),
            (1, vec![0, 1, 2]),
            "silent for less than 500 ms"
        );
        cluster.run(1_200 * MS, 1_400 * MS);
        assert_eq!(cluster.view(1), (2, vec![1, 2]), "the next lowest leads");
        assert_eq!(cluster.view(2), (2, vec![1, 2]));
    }

    #[test]
    fn one_broken_link_changes_nothing() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.cut.push((0, 2));
        cluster.run(700 * MS, 3_000 * MS);
        for pod in 0..3 {
            assert_eq!(cluster.view(pod), (1, vec![0, 1, 2]), "pod {pod}");
        }
        assert!(!cluster.linked(0, 2));
    }

    #[test]
    fn a_pod_cut_off_alone_changes_nothing_on_its_side() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.cut.extend([(0, 1), (0, 2)]);
        cluster.run(700 * MS, 3_000 * MS);
        assert_eq!(cluster.view(0), (1, vec![0, 1, 2]), "a minority: no change");
        assert_eq!(
            cluster.view(1),
            (2, vec![1, 2]),
            "the majority takes it out"
        );
        cluster.cut.clear();
        cluster.run(3_100 * MS, 4_000 * MS);
        for pod in 0..3 {
            assert_eq!(
                cluster.view(pod),
                (3, vec![0, 1, 2]),
                "back in once heard again"
            );
        }
    }

    /// A pod cut off from the others stops serving before they take it out of the view
    /// and give its keys to another. Heard again, it serves again, and is counted as having
    /// gone without a majority once.
    #[test]
    fn a_pod_cut_off_stops_serving_before_the_others_take_it_out() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.cut.extend([(0, 1), (0, 2)]);
        let (mut stopped, mut taken_out) = (None, None);
        for step in 7..=30 {
            let now = step * 100 * MS;
            cluster.run(now, now);
            if stopped.is_none() && !cluster.pods[0].serving() {
                stopped = Some(now);
            }
            if taken_out.is_none() && !cluster.view(1).1.contains(&0) {
                taken_out = Some(now);
            }
        }
        let (stopped, taken_out) = (stopped.unwrap(), taken_out.unwrap());
        assert!(
            stopped < taken_out,
            "stopped at {stopped}, taken out at {taken_out}"
        );
        assert!(cluster.pods[1].serving() && cluster.pods[2].serving());
        cluster.cut.clear();
        cluster.run(3_100 * MS, 4_000 * MS);
        assert_eq!(cluster.view(0), (3, vec![0, 1, 2]));
        assert!(cluster.pods[0].serving());
        let sight = cluster.pods[0].sight(4_000 * MS);
        assert_eq!((sight.lost, sight.regained), (1, 1));
        // From when it stopped to the first heartbeat heard again.
        assert_eq!(sight.without, 3_100 * MS - stopped);
        assert_eq!(cluster.pods[1].sight(4_000 * MS), Sight::default());
    }

    /// With every link down, no pod hears a majority and none serves; nor can any change
    /// the view. Once heartbeats come again, every pod serves the view it held, at once.
    #[test]
    fn every_pod_cut_off_stops_serving_and_serves_again_in_the_same_view() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.cut.extend([(0, 1), (0, 2), (1, 2)]);
        cluster.run(700 * MS, 800 * MS);
        for pod in 0..3 {
            assert!(cluster.pods[pod].serving(), "pod {pod}: not 250 ms yet");
        }
        cluster.run(900 * MS, 2_000 * MS);
        for pod in 0..3 {
            assert!(!cluster.pods[pod].serving(), "pod {pod}");
            assert_eq!(cluster.view(pod), (1, vec![0, 1, 2]), "pod {pod}");
            // Still without, as of now: from the stop, at 900 ms, to 2 s.
            let sight = cluster.pods[pod].sight(2_000 * MS);
            assert_eq!((sight.lost, sight.regained), (1, 0), "pod {pod}");
            assert_eq!(sight.without, 1_100 * MS, "pod {pod}");
        }
        cluster.cut.clear();
        cluster.run(2_100 * MS, 2_100 * MS);
        for pod in 0..3 {
            assert!(cluster.pods[pod].serving(), "pod {pod}");
            assert_eq!(cluster.view(pod), (1, vec![0, 1, 2]), "pod {pod}");
        }
    }

    #[test]
    fn a_restarted_pod_leaves_and_joins_again_and_serves_only_then() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.restart(2, 2, 700 * MS);
        cluster.run(700 * MS, 700 * MS);
        assert!(!cluster.pods[2].serving(), "a new life, not yet taken in");
        cluster.run(800 * MS, 800 * MS);
        assert_eq!(cluster.view(0), (2, vec![0, 1]), "left at once");
        cluster.run(900 * MS, 1_500 * MS);
        assert_eq!(
            cluster.view(0),
            (3, vec![0, 1, 2]),
            "joined again after settling"
        );
        assert!(cluster.pods[2].serving());
    }

    #[test]
    fn two_pods_gone_leave_the_last_unable_to_change_anything() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.up[0] = false;
        cluster.up[1] = false;
        cluster.run(700 * MS, 3_000 * MS);
        assert_eq!(cluster.view(2), (1, vec![0, 1, 2]), "no majority");
        assert!(!cluster.pods[2].serving(), "alone, it serves nothing");
    }

    /// Only the lowest-numbered pod of a majority changes the view: the other, alone in
    /// its view of things, leaves it as it is.
    #[test]
    fn only_the_leader_changes_the_view() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.up[0] = false;
        // Pod 2 alone ticks, long after pod 0 went silent: pod 1 leads, not it.
        let now = 1_500 * MS;
        let beat = cluster.pods[1].heartbeat(now);
        cluster.pods[2].on_heartbeat(beat, now);
        assert_eq!(cluster.pods[2].on_tick(now), None);
        assert_eq!(cluster.view(2), (1, vec![0, 1, 2]));
    }

    /// A pod joins only when every pod of the majority hears it, not the leader alone.
    #[test]
    fn a_pod_joins_only_when_all_of_the_majority_hear_it() {
        let mut cluster = Cluster::new();
        cluster.run(0, 600 * MS);
        cluster.restart(2, 2, 700 * MS);
        cluster.cut.push((1, 2));
        cluster.run(700 * MS, 3_000 * MS);
        assert_eq!(
            cluster.view(0),
            (2, vec![0, 1]),
            "left, and not back while 1 cannot hear it"
        );
        cluster.cut.clear();
        cluster.run(3_100 * MS, 4_000 * MS);
        assert_eq!(cluster.view(0), (3, vec![0, 1, 2]));
    }

    /// A view of the same epoch is not taken over: only a later one is.
    #[test]
    fn a_view_of_the_same_epoch_is_not_taken_over() {
        let mut pod = Membership::new(0, 1, vec![0, 1, 2], SILENCE, SETTLE, STOP_SERVING, 0);
        let beat = |epoch, pods: Vec<PodId>| Heartbeat {
            from: 1,
            incarnation: 1,
            agreed: Agreed {
                view: View {
                    epoch,
                    pods: pods.clone(),
                },
                incarnations: pods.iter().map(|p| (*p, 1)).collect(),
            },
            hears: vec![(1, 1)],
        };
        assert!(pod.on_heartbeat(beat(1, vec![0, 1]), 0).is_some());
        assert_eq!(pod.on_heartbeat(beat(1, vec![1, 2]), MS), None);
        assert_eq!(pod.view().pods, vec![0, 1]);
        assert!(pod.on_heartbeat(beat(2, vec![1, 2]), 2 * MS).is_some());
    }
}
