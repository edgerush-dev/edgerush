//! Where a participant's batches go: the seam between its routing-free work and the service
//! it reaches. Today's pods route by the [`View`], their batches and answers keyed by the
//! key's bytes; the cluster store's route by its table, keyed by the key's hash
//! (`edgerush-cluster-asynchronous`). The participant asks its routing for a key's owner,
//! for a pod to send a key through when the owner has gone unanswered, for a pod to poll,
//! and for the batch a pod is sent; answers reach it as a [`Reply`] whichever it is.

use super::super::{Answers, Batch, PodId, View};

/// How a participant reaches the service's pods.
pub trait Routing: Sized {
    /// A pod's id.
    type Pod: Copy + Ord + std::fmt::Debug;
    /// What a pod is sent: a batch of the participant's entries, made into the service's
    /// terms.
    type Batch;

    /// The owner of the key of `bytes`, `hash` being the participant's for it
    /// ([`Participant::hash`](super::Participant::hash)), if it names one.
    fn owner(&self, bytes: &[u8], hash: u64) -> Option<Self::Pod>;

    /// A pod other than `owner` to send the key of `hash` through as well, while `owner`
    /// leaves its batches unanswered: one that forwards them to it. `None` where pods forward
    /// nothing.
    fn through(&self, hash: u64, owner: Self::Pod) -> Option<Self::Pod>;

    /// The pod to poll at `turn`, counted up from 0, when keys in use have gone unheard, with
    /// an empty batch, its answer bringing the pod's routing if later: each in turn. `None`
    /// with no pod known, or where the routing reaches the participant unasked.
    fn polled(&self, turn: u64) -> Option<Self::Pod>;

    /// Whether it is later than `than`, and so replaces it.
    fn later(&self, than: &Self) -> bool;

    /// Whether it names `pod` among the service's pods.
    fn names(&self, pod: Self::Pod) -> bool;

    /// `batch` as `pod` is sent it, each entry's key hashed by `hash` where the service
    /// takes the hash.
    fn batch(&self, pod: Self::Pod, batch: Batch, hash: impl Fn(&[u8]) -> u64) -> Self::Batch;
}

/// A pod's answers to a batch, in the participant's terms, whichever service it reaches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply<R: Routing> {
    /// The pod that answered: the owner of the keys it answers about.
    pub from: R::Pod,
    /// The pod the answers were relayed through, if not sent by `from` itself: the batch went
    /// to it, and it forwarded the entries.
    pub via: Option<R::Pod>,
    /// `from`'s life, never 0 when set; 0 for a pod that does not number its answers
    /// ([`Answers::life`]).
    pub life: u64,
    /// Counted up, wrapping, with each answer `from` gives in this life
    /// ([`Answers::number`]).
    pub number: u64,
    /// The batch's `made`, said back ([`Answers::made`]).
    pub made: u64,
    /// One for each entry of a key `from` serves; nothing of one it does not.
    pub entries: Vec<Replied>,
    /// The routing `from` holds, if later than the batch's.
    pub routing: Option<R>,
}

/// The owner's answer about one key, the key by its hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Replied {
    /// The key's hash ([`Participant::hash`](super::Participant::hash)).
    pub key: u64,
    /// The bucket as the owner holds it: how far from full ([`Answer::behind`]).
    ///
    /// [`Answer::behind`]: super::super::Answer::behind
    pub behind: u128,
    /// The highest of the data plane's batches applied, every one before it too.
    pub applied: u64,
    /// How many other data planes the owner has seen admit the key lately.
    pub others: u64,
}

/// Today's pods: the view's, its keys owned by their rank, its batches and answers keyed by
/// the key's bytes.
impl Routing for View {
    type Pod = PodId;
    type Batch = Batch;

    fn owner(&self, bytes: &[u8], _: u64) -> Option<PodId> {
        self.owners(bytes).0
    }

    /// Today's pods forward nothing.
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

impl Reply<View> {
    /// Today's pod's `answers`, each key hashed by `hash`.
    pub fn of(answers: Answers, hash: impl Fn(&[u8]) -> u64) -> Self {
        Self {
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
            routing: answers.view,
        }
    }
}
