//! What the participant's tests run it against: a fixed routing, and a pod that applies each
//! report with the real owner ([`owner::apply`]), as the store's pods do, answering in the
//! participant's protocol.

use super::super::{Batch, PodId, owner};
use super::reference::{Answer, Answers};
use super::{Participant, Replied, Reply, Routing};
use std::collections::BTreeMap;
use std::hash::BuildHasher;

/// Every key owned by `owner`, if any, but those `keyed` names another, among `pods`; later by
/// `epoch`. Nothing is sent through another pod; with keys unheard, `pods` are polled in turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Pods {
    pub(super) epoch: u64,
    pub(super) owner: Option<PodId>,
    pub(super) keyed: BTreeMap<Box<[u8]>, PodId>,
    pub(super) pods: Vec<PodId>,
}

impl Pods {
    /// Pod 0 owning every key.
    pub(super) fn one() -> Self {
        Self {
            epoch: 1,
            owner: Some(0),
            keyed: BTreeMap::new(),
            pods: vec![0],
        }
    }

    /// No pods: a key has no owner, and is decided where it is met.
    pub(super) fn none() -> Self {
        Self {
            epoch: 1,
            owner: None,
            keyed: BTreeMap::new(),
            pods: Vec::new(),
        }
    }
}

impl Routing for Pods {
    type Pod = PodId;
    type Batch = Batch;

    fn owner(&self, bytes: &[u8], _: u64) -> Option<PodId> {
        self.keyed.get(bytes).copied().or(self.owner)
    }

    fn through(&self, _: u64, _: PodId) -> Option<PodId> {
        None
    }

    fn polled(&self, turn: u64) -> Option<PodId> {
        let at = usize::try_from(turn % self.pods.len().max(1) as u64).unwrap_or(0);
        self.pods.get(at).copied()
    }

    fn later(&self, than: &Self) -> bool {
        self.epoch > than.epoch
    }

    fn names(&self, pod: PodId) -> bool {
        self.pods.contains(&pod)
    }

    fn batch(&self, _: PodId, batch: Batch, _: impl Fn(&[u8]) -> u64) -> Batch {
        batch
    }
}

/// `answers` as the participant takes them, each key hashed by `hash`, carrying no routing.
pub(super) fn reply(answers: Answers, hash: impl Fn(&[u8]) -> u64) -> Reply<Pods> {
    Reply {
        from: answers.from,
        via: None,
        life: answers.life,
        number: answers.number,
        made: answers.made,
        entries: answers
            .entries
            .iter()
            .map(|answer| Replied {
                key: hash(&answer.key),
                behind: answer.behind,
                applied: answer.applied,
                others: answer.others,
            })
            .collect(),
        routing: None,
    }
}

impl<S: BuildHasher + Clone> Participant<S, Pods> {
    /// A pod's answers to the latest exchange started with it reach it at `now`.
    pub(super) fn on_answers(&mut self, answers: Answers, now: u64) {
        let reply = reply(answers, |bytes| self.hash(bytes));
        self.on_reply(reply, now);
    }

    /// A pod's answers to exchange `exchange` reach it at `now`.
    pub(super) fn on_answers_to(&mut self, exchange: u64, answers: Answers, now: u64) {
        let reply = reply(answers, |bytes| self.hash(bytes));
        self.on_reply_to(exchange, reply, now);
    }
}

/// A pod owning every key it is sent, applying each report with the real owner, as the store's
/// pods do: each key's value by its bytes, never forgotten.
pub(super) struct Owner {
    id: PodId,
    cycle: u64,
    keys: BTreeMap<Box<[u8]>, owner::Value>,
    life: u64,
    answered: u64,
    serving: bool,
}

impl Owner {
    /// Pod `id`, its data planes' cycle `cycle`, serving, its answers unnumbered.
    pub(super) fn new(id: PodId, cycle: u64) -> Self {
        Self {
            id,
            cycle,
            keys: BTreeMap::new(),
            life: 0,
            answered: 0,
            serving: true,
        }
    }

    /// Its life, never 0 when set, which its answers carry: 0, the default, leaves them
    /// unnumbered, each taken as it comes.
    pub(super) fn set_life(&mut self, life: u64) {
        self.life = life;
    }

    /// Whether it serves: one that does not answers a batch with nothing of its keys.
    pub(super) fn set_serving(&mut self, serving: bool) {
        self.serving = serving;
    }

    /// Hits applied to `key`, each once, if it holds the key.
    pub(super) fn counted(&self, key: &[u8]) -> Option<u64> {
        self.keys.get(key).map(owner::Value::counted)
    }

    /// A data plane's batch reaches it at `now`: each entry applied, and answered.
    pub(super) fn on_batch(&mut self, batch: Batch, now: u64) -> Answers {
        let mut entries = Vec::new();
        if self.serving {
            for entry in batch.entries {
                let value = self.keys.entry(entry.key.clone()).or_default();
                let applied = owner::apply(
                    value,
                    batch.from,
                    entry.contract,
                    entry.report,
                    self.cycle,
                    now,
                );
                entries.push(Answer {
                    key: entry.key,
                    behind: applied.answered.behind,
                    applied: applied.answered.applied,
                    others: applied.answered.others,
                });
            }
        }
        self.answered = self.answered.wrapping_add(1);
        Answers {
            from: self.id,
            entries,
            view: None,
            life: self.life,
            number: self.answered,
            made: batch.made,
        }
    }
}
