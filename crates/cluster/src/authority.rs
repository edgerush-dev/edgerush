//! The control plane's part in the cluster: the one place that may say a cluster is to be
//! formed, and by whom — at first, and again once no pod running has applied a table a
//! majority of whose voting set runs, as after the whole service restarted
//! (`membership-design.md`, Q1 and Q2).
//!
//! It keeps where it outlives the control plane the generation last named and the pods named
//! to form it (in the custom resource's status, written before anyone is told: a naming sent
//! may yet form after a restart, and is waited on as any other), and learns the rest again
//! after a restart: which pods run, from the API, and which table each has applied, by asking
//! them. It decides only from answers to questions asked after it first found a cluster
//! needed, never from what it heard before or what reaches it late: a pod running that has
//! not answered may hold a cluster that runs. Each generation is above every one before it,
//! and is the upper half of every term in it, so a cluster formed again outnumbers the old one
//! in every term and table: data planes, and old pods that come back, take the new one's
//! tables over the old.
//!
//! Pods never form a cluster on their own. With the control plane down, a cluster that runs
//! goes on — serving, failing over, letting pods in and out — and forming one waits.

use crate::message::{Message, Stamp};
use crate::node::Outbox;
use crate::table::is_quorum;
use crate::{Config, NodeId, Time, generation_of};
use std::collections::{BTreeMap, BTreeSet};

/// What a pod last answered the control plane, and to which question.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Reported {
    round: u64,
    committed: Stamp,
    voting: Vec<NodeId>,
}

/// The control plane, as far as forming the cluster goes.
#[derive(Clone, Debug)]
pub struct Authority {
    heartbeat: Time,
    form_wait: Time,
    /// The generation last named, 0 for none, the pods named to form it and the latest table
    /// any of them had then: kept across restarts, as a naming sent may form after one.
    generation: u64,
    named: Vec<NodeId>,
    latest: Stamp,
    /// This life of the control plane, apart from every other's, and its last question.
    life: u64,
    round: u64,
    /// Each pod's latest answer in this life.
    reports: BTreeMap<NodeId, Reported>,
    /// The last question asked before a cluster was found needed, while it still is.
    needed_since: Option<u64>,
    /// When it last asked, and told those named.
    asked_at: Option<Time>,
    /// When it named those it names, or, restarted, first ticked since: what forming is waited
    /// on from.
    named_at: Option<Time>,
    /// The namings it made without requiring the latest table, having waited it out.
    without_the_latest: u64,
}

impl Authority {
    /// For a cluster of pods started with `config`, never formed.
    #[must_use]
    pub const fn new(config: &Config) -> Self {
        Self {
            heartbeat: config.heartbeat,
            form_wait: config.form_wait,
            generation: 0,
            life: 0,
            round: 0,
            reports: BTreeMap::new(),
            needed_since: None,
            named: Vec::new(),
            latest: (0, 0),
            asked_at: None,
            named_at: None,
            without_the_latest: 0,
        }
    }

    /// As after a restart: everything forgotten but the generation last named and those named
    /// to form it, in a life of its own, so that answers to the last life's questions count for
    /// nothing.
    pub fn restart(&mut self) {
        self.life += 1;
        self.round = 0;
        self.reports.clear();
        self.needed_since = None;
        self.asked_at = None;
        self.named_at = None;
    }

    /// The generation last named, 0 for none.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// The namings it made without requiring the latest table, having waited `form_wait` for
    /// one that did to form: each formed without the counts of a pod cut off.
    #[must_use]
    pub const fn named_without_the_latest(&self) -> u64 {
        self.without_the_latest
    }

    /// A message from pod `from`.
    pub fn receive<V>(&mut self, from: NodeId, message: Message<V>) {
        let Message::Report {
            life,
            round,
            committed,
            voting,
        } = message
        else {
            return;
        };
        let later = self
            .reports
            .get(&from)
            .is_none_or(|report| round > report.round);
        if life == self.life && later {
            self.reports.insert(
                from,
                Reported {
                    round,
                    committed,
                    voting,
                },
            );
        }
    }

    /// Its timer at `now`, `running` being the pods the API shows running. A pod's life is over
    /// once its pod is deleted, its container restarted, or it has been Terminating on a node
    /// not ready for longer than a timeout.
    pub fn tick<V: Clone>(&mut self, now: Time, running: &BTreeSet<NodeId>) -> Outbox<V> {
        if !self.named.is_empty() {
            let _ = self.named_at.get_or_insert(now);
        }
        let needed = self.needed(running);
        let waited_out = !needed && self.waited_out(now, running);
        if needed || waited_out {
            let since = *self.needed_since.get_or_insert(self.round);
            // Every pod running has answered a question asked since: what it said was so then.
            let answered = running.iter().all(|pod| {
                self.reports
                    .get(pod)
                    .is_some_and(|report| report.round > since)
            });
            if answered {
                self.generation += 1;
                self.named = running.iter().copied().collect();
                self.latest = if waited_out {
                    self.without_the_latest += 1;
                    (0, 0)
                } else {
                    running
                        .iter()
                        .filter_map(|pod| self.reports.get(pod))
                        .map(|report| report.committed)
                        .max()
                        .unwrap_or((0, 0))
                };
                self.needed_since = None;
                self.asked_at = None;
                self.named_at = Some(now);
            }
        } else {
            self.needed_since = None;
        }
        let mut out = Outbox::new();
        if self
            .asked_at
            .is_none_or(|at| now.saturating_sub(at) >= self.heartbeat)
        {
            self.asked_at = Some(now);
            self.round += 1;
            let probe = Message::Probe {
                life: self.life,
                round: self.round,
            };
            out.extend(running.iter().map(|pod| (*pod, probe.clone())));
            let form = Message::Form {
                generation: self.generation,
                voters: self.named.clone(),
                latest: self.latest,
            };
            // Told again until each has a table of the generation, or is gone.
            out.extend(
                self.named
                    .iter()
                    .filter(|pod| {
                        running.contains(pod)
                            && self.reports.get(pod).is_none_or(|report| {
                                generation_of(report.committed.0) < self.generation
                            })
                    })
                    .map(|pod| (*pod, form.clone())),
            );
        }
        out
    }

    /// Whether a cluster is to be formed, every pod running having answered: none ever was;
    /// or, the generation named not yet formed, a majority of those named are gone, or every
    /// one of them holding the latest table any had, which alone may stand; or no pod running
    /// has applied a table a majority of whose voting set runs. (An election needs a
    /// majority of the applied table's voting set too, so a cluster none of whose pods has one
    /// can never change its table again.)
    fn needed(&self, running: &BTreeSet<NodeId>) -> bool {
        if running.is_empty() || !running.iter().all(|pod| self.reports.contains_key(pod)) {
            return false;
        }
        if self.generation == 0 {
            return true;
        }
        let gone = |voters: &[NodeId]| !is_quorum(voters, |pod| running.contains(&pod));
        let reports = || running.iter().filter_map(|pod| self.reports.get(pod));
        let formed = reports().any(|report| generation_of(report.committed.0) >= self.generation);
        if !formed && !self.named.is_empty() {
            let holder = self.named.iter().any(|pod| {
                running.contains(pod)
                    && self
                        .reports
                        .get(pod)
                        .is_some_and(|report| report.committed >= self.latest)
            });
            return gone(&self.named) || !holder;
        }
        reports().all(|report| gone(&report.voting))
    }

    /// Whether the generation named has gone `form_wait` without forming though it required
    /// the latest table, which a pod running holds: one cut off from the rest of those named
    /// would hold forming up until the cut healed, the service down meanwhile. A rate limiter
    /// loses less by forming without its counts.
    fn waited_out(&self, now: Time, running: &BTreeSet<NodeId>) -> bool {
        let formed = running
            .iter()
            .filter_map(|pod| self.reports.get(pod))
            .any(|report| generation_of(report.committed.0) >= self.generation);
        !formed
            && self.latest > (0, 0)
            && running.iter().all(|pod| self.reports.contains_key(pod))
            && self
                .named_at
                .is_some_and(|at| now.saturating_sub(at) >= self.form_wait)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kept::Counts;
    use crate::{first_term, ms};

    type Outbox = crate::node::Outbox<Counts>;

    /// Pods in `pods` answering the questions in `out` put to them, with `committed` and
    /// `voting`.
    fn answer(
        authority: &mut Authority,
        out: &Outbox,
        pods: &[NodeId],
        committed: Stamp,
        voting: &[NodeId],
    ) {
        for (to, message) in out {
            if let Message::Probe { life, round } = message
                && pods.contains(to)
            {
                authority.receive::<Counts>(
                    *to,
                    Message::Report {
                        life: *life,
                        round: *round,
                        committed,
                        voting: voting.to_vec(),
                    },
                );
            }
        }
    }

    fn named(out: &Outbox) -> Vec<(NodeId, u64, Vec<NodeId>)> {
        out.iter()
            .filter_map(|(to, message)| match message {
                Message::Form {
                    generation, voters, ..
                } => Some((*to, *generation, voters.clone())),
                _ => None,
            })
            .collect()
    }

    fn running(pods: &[NodeId]) -> BTreeSet<NodeId> {
        pods.iter().copied().collect()
    }

    /// What each group of pods answers: who, the table's number and its voting set.
    type Answers<'a> = &'a [(&'a [NodeId], Stamp, &'a [NodeId])];

    /// Ticks once a heartbeat, from heartbeat `from` to `to`, with `pods` running, each group
    /// in `answers` answering every question at once; all it sent.
    fn run(
        authority: &mut Authority,
        from: u64,
        to: u64,
        pods: &[NodeId],
        answers: Answers,
    ) -> Outbox {
        let mut all = Outbox::new();
        for beat in from..to {
            let out = authority.tick(ms(100 * beat), &running(pods));
            for (who, committed, voting) in answers {
                answer(authority, &out, who, *committed, voting);
            }
            all.extend(out);
        }
        all
    }

    /// The first generation names every pod running once each has answered, and never names
    /// a second while the first runs.
    #[test]
    fn it_names_the_pods_of_the_first_generation_once() {
        let mut authority = Authority::new(&Config::defaults());
        let out = run(
            &mut authority,
            1,
            5,
            &[1, 2, 3, 4],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        assert_eq!(authority.generation(), 0, "pod 4 runs and has not answered");
        assert!(named(&out).is_empty());
        let out = run(
            &mut authority,
            5,
            8,
            &[1, 2, 3, 4],
            &[(&[1, 2, 3, 4], (0, 0), &[])],
        );
        assert_eq!(authority.generation(), 1);
        assert!(
            named(&out)
                .iter()
                .all(|(_, generation, voters)| *generation == 1 && *voters == vec![1, 2, 3, 4])
        );
        let formed = (first_term(1), 1);
        let answers: Answers = &[
            (&[1, 2, 3, 4], formed, &[1, 2, 3, 4]),
            (&[5, 6], (0, 0), &[]),
        ];
        let _ = run(&mut authority, 8, 30, &[1, 2, 3, 4, 5, 6], answers);
        assert_eq!(authority.generation(), 1);
    }

    /// A majority of the voting set gone, as the API shows it, the pods left and new ones are
    /// named to form the next generation; one voter gone is not enough.
    #[test]
    fn it_forms_again_once_a_majority_of_the_voting_set_is_gone() {
        let mut authority = Authority::new(&Config::defaults());
        let _ = run(
            &mut authority,
            1,
            4,
            &[1, 2, 3],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        assert_eq!(authority.generation(), 1);
        let formed = (first_term(1), 4);
        let answers: Answers = &[(&[2, 3], formed, &[1, 2, 3]), (&[4], (0, 0), &[])];
        let _ = run(&mut authority, 4, 30, &[2, 3, 4], answers);
        assert_eq!(authority.generation(), 1, "one voter of three gone");
        let answers: Answers = &[(&[3], formed, &[1, 2, 3]), (&[4, 5], (0, 0), &[])];
        let out = run(&mut authority, 30, 40, &[3, 4, 5], answers);
        assert_eq!(authority.generation(), 2);
        assert!(
            named(&out)
                .iter()
                .any(|(_, generation, voters)| *generation == 2 && *voters == vec![3, 4, 5])
        );
    }

    /// The latest table any pod named had, in each naming of `generation`.
    fn latest(out: &Outbox, generation: u64) -> BTreeSet<Stamp> {
        out.iter()
            .filter_map(|(_, message)| match message {
                Message::Form {
                    generation: named,
                    latest,
                    ..
                } if *named == generation => Some(*latest),
                _ => None,
            })
            .collect()
    }

    /// Naming pods to form again, it tells them the latest table any of them had, which alone
    /// may stand; every pod holding it gone before the generation forms, it names again, from
    /// what is latest then, though a majority of those named still runs.
    #[test]
    fn it_names_again_once_every_pod_holding_the_latest_table_is_gone() {
        let mut authority = Authority::new(&Config::defaults());
        let all = [1, 2, 3, 4, 5];
        let _ = run(&mut authority, 1, 4, &all, &[(&all, (0, 0), &[])]);
        assert_eq!(authority.generation(), 1);
        // Pods 1 to 3 gone, three fresh in their place; 4 had applied a later table than 5.
        let (ahead, behind) = ((first_term(1), 9), (first_term(1), 7));
        let answers: Answers = &[
            (&[4], ahead, &all),
            (&[5], behind, &all),
            (&[6, 7, 8], (0, 0), &[]),
        ];
        let out = run(&mut authority, 4, 30, &[4, 5, 6, 7, 8], answers);
        assert_eq!(authority.generation(), 2);
        assert_eq!(latest(&out, 2), [ahead].into_iter().collect());
        let _ = run(&mut authority, 30, 40, &[4, 5, 6, 7, 8], answers);
        assert_eq!(authority.generation(), 2, "the pod holding it runs");
        let out = run(&mut authority, 40, 60, &[5, 6, 7, 8], &answers[1..]);
        assert_eq!(authority.generation(), 3, "four of the five named run");
        assert_eq!(latest(&out, 3), [behind].into_iter().collect());
    }

    /// Named to form again from the latest table, which only a pod cut off from the rest holds:
    /// once `form_wait` has gone by without the generation forming, it names them again without
    /// requiring that table, once, counted; a naming that required none, as the first, is never
    /// waited out, as naming again would change nothing.
    #[test]
    fn it_names_again_without_the_latest_table_once_forming_has_waited() {
        let mut authority = Authority::new(&Config::defaults());
        let all = [1, 2, 3];
        let _ = run(&mut authority, 1, 80, &all, &[(&all, (0, 0), &[])]);
        assert_eq!(authority.generation(), 1, "the first naming waited out");
        // 2 and 3 gone; 1 holds the latest table and forms nothing, 4 and 5 are fresh.
        let held = (first_term(1), 9);
        let answers: Answers = &[(&[1], held, &all), (&[4, 5], (0, 0), &[])];
        let out = run(&mut authority, 80, 90, &[1, 4, 5], answers);
        assert_eq!(authority.generation(), 2);
        assert_eq!(latest(&out, 2), [held].into_iter().collect());
        let _ = run(&mut authority, 90, 125, &[1, 4, 5], answers);
        assert_eq!(
            authority.generation(),
            2,
            "named again before the wait was out"
        );
        let out = run(&mut authority, 125, 150, &[1, 4, 5], answers);
        assert_eq!(authority.generation(), 3);
        assert_eq!(latest(&out, 3), [(0, 0)].into_iter().collect());
        let _ = run(&mut authority, 150, 300, &[1, 4, 5], answers);
        assert_eq!(authority.generation(), 3);
        assert_eq!(authority.named_without_the_latest(), 1);
    }

    /// A pod running that has not answered may hold a cluster that runs: nothing is formed
    /// until every pod running has, however gone the voters of what was heard before.
    #[test]
    fn it_forms_nothing_while_a_pod_running_has_not_answered() {
        let mut authority = Authority::new(&Config::defaults());
        let _ = run(
            &mut authority,
            1,
            4,
            &[1, 2, 3],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        let answers: Answers = &[(&[1, 2, 3], (first_term(1), 2), &[1, 2, 3])];
        let _ = run(&mut authority, 4, 6, &[1, 2, 3], answers);
        // Pods 1 and 2 drained and gone; 3, 4 and 5 vote now, and do not answer; 6 is new.
        let _ = run(&mut authority, 6, 40, &[3, 4, 5, 6], &[(&[6], (0, 0), &[])]);
        assert_eq!(authority.generation(), 1);
    }

    /// An answer to a question asked before a cluster was found needed, reaching it late,
    /// counts for nothing: only one to a question asked since does.
    #[test]
    fn it_decides_only_from_answers_to_questions_asked_since() {
        let mut authority = Authority::new(&Config::defaults());
        let _ = run(
            &mut authority,
            1,
            4,
            &[1, 2, 3],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        let answers: Answers = &[(&[1, 2, 3], (first_term(1), 2), &[1, 2, 3])];
        let _ = run(&mut authority, 4, 6, &[1, 2, 3], answers);
        let old = authority.tick(ms(600), &running(&[1, 2, 3]));
        // Pod 3 answers that question only now, after 1 and 2 are gone and 4 has come: its
        // answer is all that was heard of it since, and it was written before.
        let _ = authority.tick::<Counts>(ms(700), &running(&[3, 4]));
        answer(&mut authority, &old, &[3], (first_term(1), 2), &[1, 2, 3]);
        let _ = run(&mut authority, 8, 30, &[3, 4], &[(&[4], (0, 0), &[])]);
        assert_eq!(authority.generation(), 1, "decided on a stale answer");
    }

    /// Restarted, answers to its last life's questions reaching it late count for nothing,
    /// though they bear higher numbers than its new questions: written before, they would show
    /// a voting set since replaced, now mostly gone, beside a cluster that runs.
    #[test]
    fn restarted_it_takes_no_answers_to_its_last_life() {
        let mut authority = Authority::new(&Config::defaults());
        let _ = run(
            &mut authority,
            1,
            4,
            &[1, 2, 3],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        let old = (first_term(1), 2);
        let answers: Answers = &[(&[1, 2, 3], old, &[1, 2, 3])];
        let _ = run(&mut authority, 4, 40, &[1, 2, 3], answers);
        let last = authority.tick(ms(4_000), &running(&[1, 2, 3]));
        authority.restart();
        // Since then 1 and 2 have drained and 4 and 5 vote in their place. The last life's
        // answers arrive now, before any to the new life's questions.
        let now = (first_term(1), 9);
        for beat in 41..60 {
            let out = authority.tick(ms(100 * beat), &running(&[3, 4, 5]));
            if beat == 43 {
                answer(&mut authority, &last, &[3], old, &[1, 2, 3]);
                for pod in [4, 5] {
                    authority.receive::<Counts>(
                        pod,
                        Message::Report {
                            life: 0,
                            round: 40,
                            committed: old,
                            voting: vec![1, 2, 3],
                        },
                    );
                }
            }
            if beat > 50 {
                answer(&mut authority, &out, &[3, 4, 5], now, &[3, 4, 5]);
            }
        }
        assert_eq!(
            authority.generation(),
            1,
            "named beside a cluster that runs"
        );
    }

    /// Restarted while the pods it named may still be forming — its namings sent, none of them
    /// with a table yet — it waits on that formation as on any in progress: fresh pods and
    /// those named all answering "no table" is no cause to name another.
    #[test]
    fn restarted_it_waits_on_the_formation_it_named() {
        let mut authority = Authority::new(&Config::defaults());
        let _ = run(
            &mut authority,
            1,
            4,
            &[1, 2, 3],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        assert_eq!(authority.generation(), 1);
        authority.restart();
        let pods = [1, 2, 3, 4, 5, 6, 7];
        let _ = run(&mut authority, 4, 30, &pods, &[(&pods, (0, 0), &[])]);
        assert_eq!(
            authority.generation(),
            1,
            "named another while the first may form"
        );
    }

    /// Restarted, it keeps the generation; answers to its last life's questions count for
    /// nothing, and it forms again only once no pod running has a table whose voting set mostly
    /// runs.
    #[test]
    fn restarted_it_keeps_the_generation_and_forms_again_only_from_nothing() {
        let mut authority = Authority::new(&Config::defaults());
        let _ = run(
            &mut authority,
            1,
            4,
            &[1, 2, 3],
            &[(&[1, 2, 3], (0, 0), &[])],
        );
        let before = authority.tick(ms(400), &running(&[1, 2, 3]));
        authority.restart();
        assert_eq!(authority.generation(), 1);
        answer(&mut authority, &before, &[1, 2, 3], (0, 0), &[]);
        let answers: Answers = &[
            (&[1, 2], (first_term(1), 2), &[1, 2, 3]),
            (&[4], (0, 0), &[]),
        ];
        let _ = run(&mut authority, 5, 20, &[1, 2, 4], answers);
        assert_eq!(authority.generation(), 1);
        authority.restart();
        let _ = run(
            &mut authority,
            20,
            40,
            &[4, 5, 6],
            &[(&[4, 5, 6], (0, 0), &[])],
        );
        assert_eq!(authority.generation(), 2);
    }
}
