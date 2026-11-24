//! What a pod of both modes keeps under each key: either mode's value, merged, sized and cut
//! into parts as that mode's own, the mode its key's bit says (`edgerush_cluster::Kind`).

use edgerush_cluster::Held;
use edgerush_cluster_asynchronous as asynchronous;
use edgerush_cluster_synchronous as synchronous;
use std::collections::BTreeSet;

/// A key's value: either mode's, or, a key new to the store, none yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Value {
    /// New to the store, not yet given to a mode: what a key is made as, for the mode that
    /// asks for it, or a copy coming in, to take.
    #[default]
    Fresh,
    /// The synchronous mode's.
    Synchronous(synchronous::Value),
    /// The asynchronous mode's.
    Asynchronous(asynchronous::Value),
}

impl Held for Value {
    /// Of the same mode, as that mode merges; a fresh value takes the copy whole. Two modes'
    /// values never merge: the one held is kept, as a key's bit keeps them from ever meeting.
    fn merge(&mut self, other: &Self) -> bool {
        match (&mut *self, other) {
            (Self::Synchronous(held), Self::Synchronous(copy)) => held.merge(copy),
            (Self::Asynchronous(held), Self::Asynchronous(copy)) => Held::merge(held, copy),
            (Self::Fresh, Self::Fresh) => false,
            (Self::Fresh, copy) => {
                *self = copy.clone();
                true
            }
            (_, Self::Fresh)
            | (Self::Synchronous(_), Self::Asynchronous(_))
            | (Self::Asynchronous(_), Self::Synchronous(_)) => false,
        }
    }

    /// The mode's own count: nothing added, so that a pod of one mode's keys counts as that
    /// mode's pod does.
    fn bytes(&self) -> u64 {
        match self {
            Self::Fresh => 0,
            Self::Synchronous(value) => Held::bytes(value),
            Self::Asynchronous(value) => Held::bytes(value),
        }
    }

    /// The mode's own, as [`bytes`](Held::bytes) is.
    fn heap_bytes(&self) -> u64 {
        match self {
            Self::Fresh => 0,
            Self::Synchronous(value) => value.heap_bytes(),
            Self::Asynchronous(value) => value.heap_bytes(),
        }
    }

    fn only(&self, parts: &BTreeSet<u64>) -> Self {
        match self {
            Self::Fresh => Self::Fresh,
            Self::Synchronous(value) => Self::Synchronous(value.only(parts)),
            Self::Asynchronous(value) => Self::Asynchronous(value.only(parts)),
        }
    }

    /// 0 for the synchronous mode's, 1 for the asynchronous mode's: how the store counts its
    /// keys apart. A fresh value counts as the first, and is never held past the change that
    /// made it.
    fn kind(&self) -> usize {
        match self {
            Self::Fresh | Self::Synchronous(_) => 0,
            Self::Asynchronous(_) => 1,
        }
    }
}

impl synchronous::Holds for Value {
    fn synchronous_mut(&mut self) -> &mut synchronous::Value {
        // A key new to the store becomes this mode's. One of the other mode's, which a key's
        // bit keeps from being asked of this one, is made afresh rather than read as this
        // mode's.
        if !matches!(self, Self::Synchronous(_)) {
            *self = Self::Synchronous(synchronous::Value::default());
        }
        match self {
            Self::Synchronous(value) => value,
            // Made the synchronous mode's just above.
            Self::Fresh | Self::Asynchronous(_) => unreachable!("made synchronous above"),
        }
    }
}

impl asynchronous::Holds for Value {
    fn asynchronous_mut(&mut self) -> &mut asynchronous::Value {
        // As for the synchronous mode's.
        if !matches!(self, Self::Asynchronous(_)) {
            *self = Self::Asynchronous(asynchronous::Value::default());
        }
        match self {
            Self::Asynchronous(value) => value,
            // Made the asynchronous mode's just above.
            Self::Fresh | Self::Synchronous(_) => unreachable!("made asynchronous above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster_asynchronous::Holds as _;
    use edgerush_cluster_synchronous::Holds as _;
    use proptest::prelude::*;

    /// A fresh value becomes the mode's that asks for it, and counts as that mode's; one of
    /// the other mode's is made afresh, never read as this one's.
    #[test]
    fn a_fresh_value_becomes_the_mode_that_asks_for_it() {
        let mut value = Value::Fresh;
        value.synchronous_mut().count_one();
        assert_eq!(value, Value::Synchronous(synchronous::Value::counted(1)));
        assert_eq!(value.kind(), 0);
        let _ = value.asynchronous_mut();
        assert_eq!(value, Value::Asynchronous(asynchronous::Value::default()));
        assert_eq!(value.kind(), 1);
    }

    /// Copies of one synchronous key: of one shape, as one rule keeps a key, its counts or its
    /// bucket each a copy of some moment of it.
    fn copies_of_one_key() -> impl Strategy<Value = Vec<synchronous::Value>> {
        let counts = (0..3_u64, 0..5_u64, 0..3_u64).prop_map(|(window, counted, given)| {
            synchronous::Value::Count {
                window,
                counted,
                given,
            }
        });
        let buckets = (0..100_u64, 0..3_u64)
            .prop_map(|(due, given)| synchronous::Value::Bucket { due, given });
        prop_oneof![
            proptest::collection::vec(counts, 1..6),
            proptest::collection::vec(buckets, 1..6),
        ]
    }

    proptest! {
        /// Copies of a synchronous key merge as that mode merges them, in either order, a fresh value taking the first whole; a copy of the other mode's, or a fresh
        /// one, changes nothing.
        #[test]
        fn copies_merge_as_their_modes_do_and_never_across_modes(
            copies in copies_of_one_key(),
        ) {
            let (mut held, mut reversed) = (Value::Fresh, Value::Fresh);
            for copy in &copies {
                held.merge(&Value::Synchronous(copy.clone()));
            }
            for copy in copies.iter().rev() {
                reversed.merge(&Value::Synchronous(copy.clone()));
            }
            let mut alone = copies[0].clone();
            for copy in &copies[1..] {
                alone.merge(copy);
            }
            prop_assert_eq!(&held, &Value::Synchronous(alone.clone()));
            prop_assert_eq!(&reversed, &held);
            prop_assert_eq!(held.bytes(), Held::bytes(&alone));
            let before = held.clone();
            prop_assert!(!held.merge(&Value::Asynchronous(asynchronous::Value::default())));
            prop_assert!(!held.merge(&Value::Fresh));
            prop_assert_eq!(held, before);
        }
    }
}
