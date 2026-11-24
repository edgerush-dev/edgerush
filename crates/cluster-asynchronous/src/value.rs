//! What the asynchronous mode keeps under each key, as the store holds it: the owner's value
//! (`edgerush_ratelimit::service::owner`), whose parts only rise, so that two copies merge by
//! keeping the larger of each, in whatever order they meet.

use edgerush_cluster::Held;
use edgerush_ratelimit::service::Contract;
use edgerush_ratelimit::service::owner;
use std::ops::{Deref, DerefMut};

pub use owner::{Parts, PlaneParts};

/// A key's state as its owner holds it, and every replica a copy of it: the owner's value, as
/// the store keeps it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Value(owner::Value);

impl Value {
    /// A key made afresh under `contract`: a full bucket, nothing counted, no data plane known.
    #[must_use]
    pub fn new(contract: Contract) -> Self {
        Self(owner::Value::new(contract))
    }

    /// The value from its parts, as a wire carried them: `None` if they could not have come
    /// from a value ([`owner::Value::from_parts`]).
    #[must_use]
    pub fn from_parts(parts: Parts) -> Option<Self> {
        owner::Value::from_parts(parts).map(Self)
    }
}

impl Deref for Value {
    type Target = owner::Value;

    fn deref(&self) -> &owner::Value {
        &self.0
    }
}

impl DerefMut for Value {
    fn deref_mut(&mut self) -> &mut owner::Value {
        &mut self.0
    }
}

impl Held for Value {
    fn merge(&mut self, other: &Self) -> bool {
        self.0.merge(&other.0)
    }

    fn bytes(&self) -> u64 {
        self.0.bytes()
    }

    fn heap_bytes(&self) -> u64 {
        self.0.heap_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner::apply;
    use edgerush_cluster::Kept;
    use edgerush_ratelimit::service::Report;
    use std::num::NonZeroU64;

    /// 100 a second, a bucket of ten.
    fn contract() -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(100),
            window: n(1_000_000_000),
            burst: n(10),
        }
    }

    /// The store merges a copy and sizes a value as the owner's value does: a copy behind
    /// another takes it in and says so, and says nothing changed when taken again.
    #[test]
    fn the_store_merges_and_sizes_a_value_as_the_owners() {
        let mut owner = Kept::<Value>::default();
        let hits = |sequence| Report::Hits {
            sequence,
            count: 1,
            seen: sequence - 1,
            age: 0,
        };
        apply(&mut owner, 1, contract(), hits(1), 10_000_000, 0);
        let mut replica = owner.value.clone();
        apply(&mut owner, 1, contract(), hits(2), 10_000_000, 1_000);
        apply(&mut owner, 2, contract(), hits(1), 10_000_000, 2_000);
        assert!(Held::merge(&mut replica, &owner.value));
        assert_eq!(replica, owner.value);
        assert!(!Held::merge(&mut replica, &owner.value));
        assert_eq!(Held::bytes(&owner.value), owner.value.bytes());
        assert_eq!(Held::bytes(&owner.value), 56 + 2 * 24);
    }
}
