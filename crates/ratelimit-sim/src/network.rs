//! Messages between pods: what they are, and how the network carries them.

use crate::Rng;
use edgerush_ratelimit::reference::TokenBucket;

/// How the network between the pods behaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Network {
    /// The least time a message takes, in nanoseconds.
    pub delay: u64,
    /// Up to this much more, drawn for each message: messages can overtake each other.
    pub jitter: u64,
    /// Of every thousand messages, how many are lost.
    pub loss_per_mille: u64,
}

impl Network {
    /// A network that delivers everything after `delay`, give or take `jitter`.
    #[must_use]
    pub fn reliable(delay: u64, jitter: u64) -> Self {
        Self {
            delay,
            jitter,
            loss_per_mille: 0,
        }
    }

    /// When a message sent at `now` arrives, or `None` if it is lost.
    pub(crate) fn carry(&self, now: u64, rng: &mut Rng) -> Option<u64> {
        let lost = rng.below(1000) < self.loss_per_mille;
        let at = now + self.delay + rng.below(self.jitter + 1);
        (!lost).then_some(at)
    }
}

/// What one pod tells another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Hits a pod admitted, for the owner of the key to apply.
    Hits {
        /// The pod that admitted them.
        from: usize,
        /// Which life of that pod: a restart begins another (10 §2, identity safety).
        instance: u64,
        /// The batch's number among the sender's batches, counted from 1.
        sequence: u64,
        /// How many.
        count: u64,
        /// The highest of its batches the sender has seen applied: a new owner taking over
        /// from a copy older than that skips to it rather than wait for batches the sender
        /// will not send again. Zero where unused.
        seen: u64,
    },
    /// The owner's bucket as it stands, for every other pod to take over.
    State {
        /// The owner that sent it.
        from: usize,
        /// The owner's bucket.
        bucket: TokenBucket,
        /// For each pod, the highest batch of its hits the bucket has applied.
        applied: Vec<u64>,
        /// For each pod, whether the owner has seen it admit the key lately.
        active: Vec<bool>,
        /// Hits the owner's count holds.
        counted: u64,
        /// To a data plane that asked: which of its asks this answers, and how many of the
        /// requests it was about the owner admitted, the first so many; the rest it
        /// refused.
        decided: Option<(u64, u64)>,
        /// With [`Bound::Granted`](crate::Bound::Granted): how many the receiver may admit
        /// a cycle until the next answer.
        grant: Option<u64>,
    },
    /// A pod's total of admissions, for every other pod.
    Counts {
        /// The pod that admitted them.
        from: usize,
        /// Which life of that pod.
        instance: u64,
        /// How many it has admitted in that life.
        total: u64,
    },
    /// A data plane holding no fresh copy of a key asks the owner to decide the requests
    /// waiting for it.
    Ask {
        /// The data plane asking.
        from: usize,
        /// Which life of it.
        instance: u64,
        /// The ask's number among that data plane's asks, counted from 1.
        sequence: u64,
        /// How many requests wait.
        count: u64,
    },
    /// The service pod owning a key, to its secondary: what it holds of the key.
    Replica {
        /// The owner that sent it.
        from: usize,
        /// Its bucket.
        bucket: TokenBucket,
        /// For each data plane, the highest batch of its hits applied.
        applied: Vec<u64>,
        /// Hits its count holds.
        counted: u64,
        /// For each data plane, the latest of its lives heard from.
        instances: Vec<u64>,
        /// For each data plane, when it was last seen to admit the key.
        last_admitted: Vec<Option<u64>>,
    },
    /// A data plane with no hits to report asks the rate-limiting service for a copy.
    Poll {
        /// The data plane asking.
        from: usize,
        /// Which life of it.
        instance: u64,
    },
}

impl Message {
    /// The pod that sent it.
    #[must_use]
    pub fn sender(&self) -> usize {
        match self {
            Self::Hits { from, .. }
            | Self::State { from, .. }
            | Self::Counts { from, .. }
            | Self::Ask { from, .. }
            | Self::Replica { from, .. }
            | Self::Poll { from, .. } => *from,
        }
    }
}

/// What a pod sends while handling an event, collected for the network to carry.
#[derive(Debug, Default)]
pub struct Outbox {
    pub(crate) sent: Vec<(usize, Message)>,
}

impl Outbox {
    /// Sends `message` to pod `to`.
    pub fn send(&mut self, to: usize, message: Message) {
        self.sent.push((to, message));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_takes_its_delay_and_at_most_its_jitter_more() {
        let network = Network::reliable(1_000, 500);
        let mut rng = Rng::new(4);
        let delays: Vec<u64> = (0..1000)
            .map(|_| network.carry(10, &mut rng).unwrap() - 10)
            .collect();
        assert!(delays.iter().all(|d| (1_000..=1_500).contains(d)));
        assert!(delays.iter().any(|d| *d < 1_010));
        assert!(delays.iter().any(|d| *d > 1_490));
    }

    #[test]
    fn about_the_share_lost_is_lost() {
        let network = Network {
            delay: 0,
            jitter: 0,
            loss_per_mille: 200,
        };
        let mut rng = Rng::new(4);
        let lost = (0..10_000)
            .filter(|_| network.carry(0, &mut rng).is_none())
            .count();
        assert!((1_800..=2_200).contains(&lost), "{lost}");
        let none = Network {
            loss_per_mille: 0,
            ..network
        };
        assert!((0..1000).all(|_| none.carry(0, &mut rng).is_some()));
    }
}
