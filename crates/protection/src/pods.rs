//! Each pod's count found again in the next config (25 §3): a reload swaps the config, not
//! the requests in flight, so a pod in the old config and the new has one count, and a new
//! config never starts at zero beside the old one's requests.

use crate::PodCount;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Weak};

/// Every pod's count still held — by a config's upstreams or by a request in flight — by
/// what the data plane files it under.
#[derive(Debug)]
pub struct Pods<K> {
    known: HashMap<K, Weak<PodCount>>,
}

impl<K> Default for Pods<K> {
    fn default() -> Self {
        Self {
            known: HashMap::new(),
        }
    }
}

impl<K: Hash + Eq> Pods<K> {
    /// The pods `previous` knew that something still holds: a pod a config no longer has
    /// is kept while its requests are in flight, so that one back again before they end
    /// counts them. Made by the thread that builds a config, once per config.
    #[must_use]
    pub fn after(previous: Option<&Self>) -> Self
    where
        K: Clone,
    {
        let known = previous
            .into_iter()
            .flat_map(|previous| &previous.known)
            .filter(|(_, count)| count.strong_count() > 0)
            .map(|(key, count)| (key.clone(), Weak::clone(count)))
            .collect();
        Self { known }
    }

    /// The count of the pod filed under `key`: the one it had, if anything still holds it,
    /// or a new one at zero.
    pub fn count(&mut self, key: K) -> Arc<PodCount> {
        if let Some(count) = self.known.get(&key).and_then(Weak::upgrade) {
            return count;
        }
        let count = PodCount::new();
        self.known.insert(key, Arc::downgrade(&count));
        count
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
        let count = first.count("a");
        let slot = PodCount::take(&count, CAP).unwrap();
        let mut second = Pods::after(Some(&first));
        let again = second.count("a");
        assert!(Arc::ptr_eq(&count, &again));
        assert_eq!(again.in_flight(), 1);
        drop(slot);
        assert_eq!(again.in_flight(), 0);
    }

    #[test]
    fn a_pod_gone_from_the_config_is_kept_while_its_requests_are_in_flight() {
        let mut first = Pods::after(None);
        let slot = PodCount::take(&first.count("a"), CAP).unwrap();
        // The next config has no `a`: nothing but the slot holds its count.
        let second = Pods::after(Some(&first));
        drop(first);
        let mut third = Pods::after(Some(&second));
        assert_eq!(
            third.count("a").in_flight(),
            1,
            "back before its request ended"
        );
        drop(slot);
    }

    #[test]
    fn a_pod_nothing_holds_is_forgotten_and_starts_again_at_zero() {
        let mut first = Pods::after(None);
        drop(PodCount::take(&first.count("a"), CAP));
        let second = Pods::after(Some(&first));
        assert!(second.is_empty());
        let mut third = Pods::after(Some(&second));
        assert_eq!(third.count("a").in_flight(), 0);
        assert_eq!(third.len(), 1);
    }

    #[test]
    fn two_keys_are_two_counts() {
        let mut pods = Pods::after(None);
        let a = pods.count("a");
        let b = pods.count("b");
        let _slot = PodCount::take(&a, CAP);
        assert_eq!(b.in_flight(), 0);
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
