//! Each pod's count found again in the next config (25 §3): a reload swaps the config, not
//! the requests in flight, so a pod in the old config and the new has one count, and a new
//! config never starts at zero beside the old one's requests. So is each backend's queue
//! (25 §7): requests waiting in it keep their places whatever configs come.

use crate::{PodCount, Queue};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Weak};

/// Every pod's count still held — by a config's upstreams or by a request in flight — by
/// what the data plane files it under, `K`; and every backend's queue, by what it files a
/// backend under, `B`.
#[derive(Debug)]
pub struct Pods<B, K> {
    known: HashMap<K, Weak<PodCount>>,
    queues: HashMap<B, Weak<Queue>>,
}

impl<B, K> Default for Pods<B, K> {
    fn default() -> Self {
        Self {
            known: HashMap::new(),
            queues: HashMap::new(),
        }
    }
}

impl<B: Hash + Eq + Clone, K: Hash + Eq + Clone> Pods<B, K> {
    /// The pods and queues `previous` knew that something still holds: a pod a config no
    /// longer has is kept while its requests are in flight, so that one back again before
    /// they end counts them, and a queue while a pod of its backend or a request in it is.
    /// Made by the thread that builds a config, once per config.
    #[must_use]
    pub fn after(previous: Option<&Self>) -> Self {
        let known = previous
            .into_iter()
            .flat_map(|previous| &previous.known)
            .filter(|(_, count)| count.strong_count() > 0)
            .map(|(key, count)| (key.clone(), Weak::clone(count)))
            .collect();
        let queues = previous
            .into_iter()
            .flat_map(|previous| &previous.queues)
            .filter(|(_, queue)| queue.strong_count() > 0)
            .map(|(backend, queue)| (backend.clone(), Weak::clone(queue)))
            .collect();
        Self { known, queues }
    }

    /// The count of the pod filed under `key`, of `backend`: the one it had, if anything
    /// still holds it, or a new one at zero.
    pub fn count(&mut self, backend: &B, key: K) -> Arc<PodCount> {
        if let Some(count) = self.known.get(&key).and_then(Weak::upgrade) {
            return count;
        }
        let count = PodCount::of(&self.queue(backend));
        self.known.insert(key, Arc::downgrade(&count));
        count
    }

    /// The queue of `backend`: the one it had, if anything still holds it, or an empty one.
    pub fn queue(&mut self, backend: &B) -> Arc<Queue> {
        if let Some(queue) = self.queues.get(backend).and_then(Weak::upgrade) {
            return queue;
        }
        let queue = Queue::new();
        self.queues.insert(backend.clone(), Arc::downgrade(&queue));
        queue
    }

    /// How many pods it knows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.known.len()
    }

    /// Whether it knows none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.known.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    const CAP: NonZeroU32 = NonZeroU32::new(10).unwrap();

    #[test]
    fn a_pod_kept_by_the_next_config_keeps_its_count() {
        let mut first = Pods::after(None);
        let count = first.count(&"search", "a");
        let slot = PodCount::take(&count, CAP).unwrap();
        let mut second = Pods::after(Some(&first));
        let again = second.count(&"search", "a");
        assert!(Arc::ptr_eq(&count, &again));
        assert_eq!(again.in_flight(), 1);
        drop(slot);
        assert_eq!(again.in_flight(), 0);
    }

    #[test]
    fn a_pod_gone_from_the_config_is_kept_while_its_requests_are_in_flight() {
        let mut first = Pods::after(None);
        let slot = PodCount::take(&first.count(&"search", "a"), CAP).unwrap();
        // The next config has no `a`: nothing but the slot holds its count.
        let second = Pods::after(Some(&first));
        drop(first);
        let mut third = Pods::after(Some(&second));
        assert_eq!(
            third.count(&"search", "a").in_flight(),
            1,
            "back before its request ended"
        );
        drop(slot);
    }

    #[test]
    fn a_pod_nothing_holds_is_forgotten_and_starts_again_at_zero() {
        let mut first = Pods::after(None);
        drop(PodCount::take(&first.count(&"search", "a"), CAP));
        let second = Pods::after(Some(&first));
        assert!(second.is_empty());
        let mut third = Pods::after(Some(&second));
        assert_eq!(third.count(&"search", "a").in_flight(), 0);
        assert_eq!(third.len(), 1);
    }

    #[test]
    fn two_keys_are_two_counts() {
        let mut pods = Pods::after(None);
        let a = pods.count(&"search", "a");
        let b = pods.count(&"search", "b");
        let _slot = PodCount::take(&a, CAP);
        assert_eq!(b.in_flight(), 0);
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn a_backends_pods_share_its_queue_and_another_backends_have_their_own() {
        let mut pods = Pods::after(None);
        let a = pods.count(&"search", "a");
        let b = pods.count(&"search", "b");
        let c = pods.count(&"cart", "c");
        assert!(Arc::ptr_eq(&a.queue, &b.queue));
        assert!(Arc::ptr_eq(&a.queue, &pods.queue(&"search")));
        assert!(!Arc::ptr_eq(&a.queue, &c.queue));
    }

    /// Requests waiting keep their places across a reload (25 §7): the queue a pod of the
    /// backend or a request in it holds is the one the next config finds.
    #[test]
    fn a_queue_held_is_found_again_and_one_nothing_holds_starts_empty() {
        let size = NonZeroU32::new(5).unwrap();
        let mut first = Pods::after(None);
        let queue = first.queue(&"search");
        let joined = Queue::join(&queue, size).unwrap();
        drop(queue);
        let mut second = Pods::after(Some(&first));
        drop(first);
        let again = second.queue(&"search");
        assert_eq!(again.waiting(), 1, "its request still waits in it");
        // A pod made now is of the same backend, and calls the same line.
        assert!(Arc::ptr_eq(&second.count(&"search", "a").queue, &again));
        drop((joined, again));
        let third = Pods::after(Some(&second));
        drop(second);
        let mut fourth = Pods::after(Some(&third));
        assert_eq!(fourth.queue(&"search").waiting(), 0);
    }
}
