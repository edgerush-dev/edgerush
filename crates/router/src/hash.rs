//! The hash map the indexes are built on.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};

/// A map from bytes with a fixed-key hasher instead of the randomly seeded default: a pure
/// crate takes no randomness, and benchmarks that count instructions need identical runs.
/// Random seeding defends against chosen colliding keys, but the keys stored here come from
/// configuration, not from requests.
pub(crate) type Map<V> = HashMap<Box<[u8]>, V, BuildHasherDefault<DefaultHasher>>;
