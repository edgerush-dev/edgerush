use super::*;
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Six to a chunk: not a capacity a growing `Vec` reaches on its own (4, 8, ...), so a chunk
/// made without its room shows.
const SMALL: usize = 6;

/// A key, and a number unique to the item, so that no two items are equal and both heaps
/// take the same one.
type Item = (u8, u32);

#[derive(Debug, Clone)]
enum Op {
    Push(u8),
    Pop,
    /// Keeps the items whose number is not `r` modulo `m`.
    Retain(u32, u32),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => any::<u8>().prop_map(Op::Push),
        4 => Just(Op::Pop),
        1 => (2..5u32, 0..5u32).prop_map(|(m, r)| Op::Retain(m, r % m)),
    ]
}

/// Every chunk has room for `CHUNK` and has never grown; all are full but the last holding
/// any items; past that, at most one empty.
fn assert_chunks<T, const CHUNK: usize>(heap: &MinHeap<T, CHUNK>) {
    let holding = heap.len.div_ceil(CHUNK);
    assert!(heap.chunks.len() <= holding + 1, "at most one spare chunk");
    for (i, chunk) in heap.chunks.iter().enumerate() {
        assert_eq!(
            chunk.capacity(),
            CHUNK,
            "chunk {i} made with its room, never grown"
        );
        let expected = heap.len.saturating_sub(i * CHUNK).min(CHUNK);
        assert_eq!(chunk.len(), expected, "chunk {i} filled in order");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2_048)
    ))]

    /// The chunked heap takes items in the order `BinaryHeap` does, over any pushes, pops
    /// and retains, its chunks as they should be throughout.
    #[test]
    fn takes_items_as_a_binary_heap_does(ops in prop::collection::vec(op(), 0..300)) {
        let mut heap = MinHeap::<Item, SMALL>::new();
        let mut reference = BinaryHeap::<Reverse<Item>>::new();
        for (number, op) in (0u32..).zip(ops) {
            match op {
                Op::Push(key) => {
                    heap.push((key, number));
                    reference.push(Reverse((key, number)));
                }
                Op::Pop => {
                    prop_assert_eq!(heap.pop(), reference.pop().map(|Reverse(item)| item));
                }
                Op::Retain(m, r) => {
                    heap.retain(|&(_, n)| n % m != r);
                    reference.retain(|&Reverse((_, n))| n % m != r);
                }
            }
            prop_assert_eq!(heap.len(), reference.len());
            prop_assert_eq!(heap.peek(), reference.peek().map(|Reverse(item)| item));
            assert_chunks(&heap);
        }
        while let Some(Reverse(item)) = reference.pop() {
            prop_assert_eq!(heap.pop(), Some(item));
        }
        prop_assert!(heap.is_empty());
        prop_assert_eq!(heap.pop(), None);
    }
}

#[test]
fn a_heap_at_a_chunks_edge_makes_and_frees_none() {
    let mut heap = MinHeap::<u32, SMALL>::new();
    for i in 0..2 * SMALL as u32 {
        heap.push(i);
    }
    let mut chunks = Vec::new();
    for round in 0..10 {
        heap.push(100 + round);
        assert!(heap.pop().is_some());
        assert!(heap.pop().is_some());
        heap.push(200 + round);
        let now: Vec<*const u32> = heap.chunks.iter().map(|c| c.as_ptr()).collect();
        assert_eq!(now.len(), 3, "the full two and one spare");
        if round == 0 {
            chunks = now;
        } else {
            assert_eq!(now, chunks, "the same chunks, none made again");
        }
    }
    while heap.pop().is_some() {}
    assert_eq!(heap.chunks.len(), 1, "one spare left when empty");
}

#[test]
fn a_chunk_holds_sixty_four_kib_of_eviction_times() {
    let mut heap = MinHeap::<[u64; 2]>::new();
    heap.push([1, 0]);
    assert_eq!(heap.chunks[0].capacity() * size_of::<[u64; 2]>(), 64 * 1024);
}
