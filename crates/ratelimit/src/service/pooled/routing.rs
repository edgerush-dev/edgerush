//! Where a participant's batches go: the seam between its routing-free work and the service
//! it reaches. The cluster store's pods route by its table, keyed by the key's hash
//! (`edgerush-cluster-asynchronous`'s `Routed`). The participant asks its routing for a key's
//! owner, for a pod to send a key through when the owner has gone unanswered, for a pod to
//! poll, and for the batch a pod is sent; answers reach it as a [`Reply`] whichever it is.

use super::super::Batch;

/// How a participant reaches the service's pods.
pub trait Routing: Sized {
    /// A pod's id.
    type Pod: Copy + Ord + std::fmt::Debug;
    /// What a pod is sent: a batch of the participant's entries, made into the service's
    /// terms.
    type Batch;
    /// What it keeps of a key beside the participant's hash of it ([`Self::key`]): made once,
    /// as the participant first keeps the key, and handed back with the hash wherever the
    /// key is routed. `()` where the hash is all it needs; the cluster store's routing keeps
    /// the second half of the store's 128-bit key.
    type Key: Copy;

    /// What it keeps of the key of `bytes`, `hash` being the participant's for it
    /// ([`Participant::hash`](super::Participant::hash)): alike under every routing the
    /// participant is told, as it is made once.
    fn key(&self, bytes: &[u8], hash: u64) -> Self::Key;

    /// The owner of the key of `bytes`, `hash` being the participant's for it and `key` what
    /// the routing keeps of it, if it names one.
    fn owner(&self, bytes: &[u8], hash: u64, key: Self::Key) -> Option<Self::Pod>;

    /// A pod other than `owner` to send the key of `hash` and `key` through as well, while
    /// `owner` leaves its batches unanswered: one that forwards them to it. `None` where pods
    /// forward nothing.
    fn through(&self, hash: u64, key: Self::Key, owner: Self::Pod) -> Option<Self::Pod>;

    /// The pod to poll at `turn`, counted up from 0, when keys in use have gone unheard, with
    /// an empty batch, its answer bringing the pod's routing if later: each in turn. `None`
    /// with no pod known, or where the routing reaches the participant unasked.
    fn polled(&self, turn: u64) -> Option<Self::Pod>;

    /// Whether it is later than `than`, and so replaces it.
    fn later(&self, than: &Self) -> bool;

    /// Whether it names `pod` among the service's pods.
    fn names(&self, pod: Self::Pod) -> bool;

    /// `batch` as `pod` is sent it, `keys` beside its entries, by position: each one's hash
    /// and what the routing keeps of it, where the service takes those rather than the bytes.
    fn batch(&self, pod: Self::Pod, batch: Batch, keys: &[(u64, Self::Key)]) -> Self::Batch;
}

/// A pod's answers to a batch, in the participant's terms, whichever service it reaches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply<R: Routing> {
    /// The pod that answered: the owner of the keys it answers about.
    pub from: R::Pod,
    /// The pod the answers were relayed through, if not sent by `from` itself: the batch went
    /// to it, and it forwarded the entries.
    pub via: Option<R::Pod>,
    /// `from`'s life, never 0 when set; 0 for a pod that does not number its answers, each
    /// then taken as it comes.
    pub life: u64,
    /// Counted up, wrapping, with each answer `from` gives in this life: the order in which
    /// it handled their batches.
    pub number: u64,
    /// The batch's [`made`](Batch::made), said back: the copy takes the buckets as of then.
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
    /// The bucket as the owner holds it: how far from full, as
    /// [`TokenBucket::behind`](crate::reference::TokenBucket::behind) says.
    pub behind: u128,
    /// The highest of the data plane's batches applied, every one before it too.
    pub applied: u64,
    /// How many other data planes the owner has seen admit the key lately.
    pub others: u64,
}
