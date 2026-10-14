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

/// One pod's part in agreeing on the view.
pub struct Membership {
    me: PodId,
    incarnation: u64,
    started: u64,
    members: Vec<PodId>,
    silence: u64,
    settle: u64,
    agreed: Agreed,
    heard: BTreeMap<PodId, Heard>,
}

impl Membership {
    /// Pod `me` in its life `incarnation`, started at `now`, among the configured `members`
    /// (itself too); others are suspected after `silence` unheard, and taken in after being
    /// heard for `settle`. It holds no view yet: every member, at epoch 0, which serves
    /// nothing until a leader makes the first.
    #[must_use]
    pub fn new(
        me: PodId,
        incarnation: u64,
        members: Vec<PodId>,
        silence: u64,
        settle: u64,
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
            agreed: Agreed {
                view,
                incarnations: Vec::new(),
            },
            heard: BTreeMap::new(),
        }
    }

    /// The view it holds.
    #[must_use]
    pub fn view(&self) -> &View {
        &self.agreed.view
    }

    /// Whether the view it holds took it in, in this life: until then it serves nothing.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.agreed
            .incarnations
            .contains(&(self.me, self.incarnation))
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
        if heartbeat.agreed.view.epoch > self.agreed.view.epoch {
            self.agreed = heartbeat.agreed;
            return Some(self.agreed.view.clone());
        }
        None
    }

    /// Its timer at `now`: as the leader of a majority, it changes the view if every pod it
    /// hears agrees; the new view is returned.
    pub fn on_tick(&mut self, now: u64) -> Option<View> {
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
        self.agreed = Agreed {
            view: View {
                epoch: self.agreed.view.epoch + 1,
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

    /// Three pods and which links between them carry heartbeats.
    struct Cluster {
        pods: Vec<Membership>,
        up: Vec<bool>,
        cut: Vec<(PodId, PodId)>,
    }

    impl Cluster {
        fn new() -> Self {
            Self {
                pods: (0..3)
                    .map(|p| Membership::new(p, 1, vec![0, 1, 2], SILENCE, SETTLE, 0))
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
                now,
            );
        }
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
        let mut pod = Membership::new(0, 1, vec![0, 1, 2], SILENCE, SETTLE, 0);
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
