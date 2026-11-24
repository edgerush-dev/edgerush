//! What data takes in memory, worked out from its types' sizes rather than asked of the
//! allocator: as Linux's C library (glibc) allocates on a 64-bit target, and as std's
//! B-trees and hashbrown's tables (std's `HashMap` and `HashSet` among them) lay entries out.
//! For counts that follow the memory really taken, kept as the data changes — the
//! rate-limit keys' caps (06 §8) — where asking the allocator would need `unsafe`.
//!
//! A B-tree's entries share its nodes, and how full those are depends on the order entries
//! came in and went, so an entry's share is an average over the tree's life, not what any
//! one entry takes. A hash table is one block, counted whole as it grows.

/// What glibc's `malloc` takes for `bytes`: the request and an 8-byte header, rounded up to
/// 16, at least 32. Nothing for nothing: Rust allocates nothing for a size of 0. A block
/// past glibc's mmap threshold (128 KiB at first) is rounded to pages instead, a difference
/// too small against such a block to count.
#[must_use]
pub const fn allocation(bytes: usize) -> u64 {
    if bytes == 0 {
        return 0;
    }
    let chunk = bytes.saturating_add(8 + 15) & !15;
    // Widening: a `usize` fits in a `u64` on every target EdgeRush builds for.
    (if chunk < 32 { 32 } else { chunk }) as u64
}

/// What a `Vec<T>` with room for `capacity` takes beside itself: one block.
#[must_use]
pub const fn vec<T>(capacity: usize) -> u64 {
    allocation(capacity.saturating_mul(size_of::<T>()))
}

/// What an `Arc<T>` takes beside itself: one block, its two counts and `T`.
#[must_use]
pub const fn arc<T>() -> u64 {
    allocation(2 * size_of::<usize>() + size_of::<T>())
}

/// The most entries a std B-tree node holds.
const NODE_ENTRIES: usize = 11;

/// Entries a B-tree node holds on average, with its share of the internal nodes above it,
/// keys coming in no order: 7.3 to 7.4 in a tree filled, 7.0 to 7.1 in one turning over
/// at a steady size, 6.8 with keys also moved, as a schedule's are, and 6.9 just after a
/// tenth was taken out (measured with glibc's sizes, two million entries). The middle, so
/// that a count is at most some 6% over or 3% under.
const ENTRIES_PER_NODE: u64 = 7;

/// The larger of two sizes.
const fn larger(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

/// The alignment of a node of a `BTreeMap<K, V>`.
const fn node_align<K, V>() -> usize {
    larger(
        align_of::<usize>(),
        larger(align_of::<K>(), align_of::<V>()),
    )
}

/// The size of a leaf node of a `BTreeMap<K, V>`: its parent's pointer, its place among the
/// parent's children and its length, two `u16`s, and room for 11 keys and 11 values, padded
/// to its alignment (std's `LeafNode`, whose fields Rust lays out with padding only at the
/// end).
const fn leaf<K, V>() -> usize {
    let bytes = size_of::<usize>()
        + 2 * size_of::<u16>()
        + NODE_ENTRIES * size_of::<K>()
        + NODE_ENTRIES * size_of::<V>();
    bytes.next_multiple_of(node_align::<K, V>())
}

/// The size of an internal node: a leaf and its 12 children's pointers (std's
/// `InternalNode`, the leaf first).
const fn internal<K, V>() -> usize {
    (leaf::<K, V>() + (NODE_ENTRIES + 1) * size_of::<usize>())
        .next_multiple_of(node_align::<K, V>())
}

/// What an entry of a `BTreeMap<K, V>` (or a `BTreeSet<K>`, `V` being `()`) takes: its share
/// of the nodes, keys coming in no order.
#[must_use]
pub const fn btree_entry<K, V>() -> u64 {
    allocation(leaf::<K, V>()) / ENTRIES_PER_NODE
}

/// What a whole `BTreeMap<K, V>` of `len` entries takes, its keys coming in rising order, as
/// a rolling window's slots do: a full node splits leaving six in the older half, and the
/// newer fills again, so that every node but the newest of a level holds six. Exact for a
/// tree so made; one whose oldest go as its newest come stays within a node of it.
#[must_use]
pub const fn btree_ascending<K, V>(len: usize) -> u64 {
    if len == 0 {
        return 0;
    }
    let leaves = if len <= NODE_ENTRIES {
        1
    } else {
        1 + (len - 5) / 7
    };
    let (mut below, mut internals) = (leaves, 0);
    while below > 1 {
        let level = if below <= NODE_ENTRIES + 1 {
            1
        } else {
            1 + (below - 6) / 7
        };
        internals += level;
        below = level;
    }
    // Widening: counts of nodes fit in a `u64`.
    (leaves as u64).saturating_mul(allocation(leaf::<K, V>()))
        + (internals as u64).saturating_mul(allocation(internal::<K, V>()))
}

/// The bytes of a hashbrown table's group of control bytes, read at once: 16 with SSE2 on
/// x86, 8 elsewhere (NEON on Arm, a word in the generic code).
const GROUP_WIDTH: usize = if cfg!(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "sse2"
)) {
    16
} else {
    8
};

/// What a hashbrown table of `T` (std's `HashMap` and `HashSet` among them, of `(K, V)` and
/// `K`) takes with room for `capacity`, as its `capacity()` says: one block, a bucket and a
/// control byte for each of its buckets and a group of control bytes besides, the buckets
/// one more than its capacity below 8 and eight-sevenths of it from there, as hashbrown
/// lays a table out. Nothing for a table that has never held anything. A table grows by
/// doubling and never shrinks: one left larger by keys since gone holds its room.
#[must_use]
pub const fn hash_table<T>(capacity: usize) -> u64 {
    if capacity == 0 || size_of::<T>() == 0 {
        return 0;
    }
    let buckets = if capacity < 8 {
        capacity + 1
    } else {
        capacity / 7 * 8
    };
    let control_align = larger(align_of::<T>(), GROUP_WIDTH);
    let control = size_of::<T>()
        .saturating_mul(buckets)
        .next_multiple_of(control_align);
    allocation(control.saturating_add(buckets + GROUP_WIDTH))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value of the store's keys' size and alignment: 112 bytes, aligned to 16.
    type Value112 = [u128; 7];

    #[test]
    fn an_allocation_is_its_request_and_header_rounded_to_16_at_least_32() {
        assert_eq!(allocation(0), 0);
        assert_eq!(allocation(1), 32);
        assert_eq!(allocation(24), 32);
        assert_eq!(allocation(25), 48);
        assert_eq!(allocation(40), 48);
        assert_eq!(allocation(41), 64);
        assert_eq!(allocation(1_424), 1_440);
    }

    #[test]
    fn containers_take_their_blocks() {
        assert_eq!(vec::<u64>(0), 0);
        assert_eq!(vec::<(u64, [u64; 3])>(1), 48);
        assert_eq!(vec::<(u64, [u64; 3])>(2), 80);
        assert_eq!(arc::<[u64; 6]>(), 64 + 16);
    }

    /// std's nodes as glibc allocates them, read from a counting allocator: a leaf of 11 keys
    /// of 16 bytes and values of 112, 1,424 bytes, 1,440 allocated; one of `u64` keys and
    /// values of two, 280 and 288; its internal node 384.
    #[test]
    fn nodes_are_sized_as_std_lays_them_out() {
        assert_eq!(leaf::<u128, Value112>(), 1_424);
        assert_eq!(leaf::<u64, (u64, u64)>(), 280);
        assert_eq!(allocation(internal::<u64, (u64, u64)>()), 384);
        assert_eq!(leaf::<(u64, u128), ()>(), 368);
        assert_eq!(btree_entry::<u128, Value112>(), 1_440 / 7);
        assert_eq!(btree_entry::<(u64, u128), ()>(), 384 / 7);
    }

    /// Against what a counting allocator found std's B-tree take, keys inserted rising.
    #[test]
    fn a_tree_made_in_order_takes_what_std_allocates() {
        let measured = [
            (1, 288),
            (11, 288),
            (12, 960),
            (18, 960),
            (19, 1_248),
            (30, 1_536),
            (60, 2_688),
            (78, 3_552),
            (84, 3_840),
            (90, 4_896),
            (100, 5_184),
            (200, 9_984),
            (500, 24_672),
            (1_000, 50_400),
        ];
        assert_eq!(btree_ascending::<u64, (u64, u64)>(0), 0);
        for (len, bytes) in measured {
            assert_eq!(
                btree_ascending::<u64, (u64, u64)>(len),
                bytes,
                "{len} entries"
            );
        }
    }

    /// Against tables of 8 and 16 bytes an entry as hashbrown lays them out: 4 buckets, then
    /// 8, 16 and so on, each with 16 bytes of control besides on x86.
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn a_hash_table_is_its_buckets_and_control_bytes() {
        assert_eq!(hash_table::<u64>(0), 0);
        assert_eq!(hash_table::<u64>(3), allocation(4 * 8 + 4 + 16));
        assert_eq!(hash_table::<u64>(7), allocation(8 * 8 + 8 + 16));
        assert_eq!(hash_table::<u64>(14), allocation(16 * 8 + 16 + 16));
        assert_eq!(hash_table::<[u64; 2]>(28), allocation(32 * 16 + 32 + 16));
        assert_eq!(
            hash_table::<[u64; 9]>(917_504),
            allocation(1_048_576 * 73 + 16)
        );
    }
}
