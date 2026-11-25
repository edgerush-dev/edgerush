use super::*;
use proptest::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::rc::Rc;

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
fn assert_chunks<T, const CHUNK: usize>(items: &Chunks<T, CHUNK>) {
    let holding = items.len.div_ceil(CHUNK);
    assert!(items.chunks.len() <= holding + 1, "at most one spare chunk");
    for (i, chunk) in items.chunks.iter().enumerate() {
        assert_eq!(
            chunk.capacity(),
            CHUNK,
            "chunk {i} made with its room, never grown"
        );
        let expected = items.len.saturating_sub(i * CHUNK).min(CHUNK);
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
            assert_chunks(&heap.items);
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
        let now: Vec<*const u32> = heap.items.chunks.iter().map(|c| c.as_ptr()).collect();
        assert_eq!(now.len(), 3, "the full two and one spare");
        if round == 0 {
            chunks = now;
        } else {
            assert_eq!(now, chunks, "the same chunks, none made again");
        }
    }
    while heap.pop().is_some() {}
    assert_eq!(heap.items.chunks.len(), 1, "one spare left when empty");
}

#[test]
fn a_chunk_holds_sixty_four_kib_of_eviction_times() {
    let mut heap = MinHeap::<[u64; 2]>::new();
    heap.push([1, 0]);
    assert_eq!(
        heap.items.chunks[0].capacity() * size_of::<[u64; 2]>(),
        64 * 1024
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2_048)
    ))]

    /// The chunked list holds what a `Vec` pushed and retained alike would, in any order, its
    /// chunks as they should be throughout, and drops the items it does not keep as it
    /// retains.
    #[test]
    fn a_list_holds_what_a_vec_would(ops in prop::collection::vec(op(), 0..300)) {
        let mut list = List::<Rc<u32>, SMALL>::new();
        let mut reference = Vec::<u32>::new();
        let mut made = Vec::new();
        for (number, op) in (0u32..).zip(ops) {
            let retained = matches!(op, Op::Retain(..));
            match op {
                Op::Push(_) | Op::Pop => {
                    let item = Rc::new(number);
                    made.push(Rc::downgrade(&item));
                    list.push(item);
                    reference.push(number);
                }
                Op::Retain(m, r) => {
                    list.retain(|n| **n % m != r);
                    reference.retain(|n| n % m != r);
                }
            }
            prop_assert_eq!(list.len(), reference.len());
            prop_assert_eq!(list.is_empty(), reference.is_empty());
            assert_chunks(&list.items);
            let mut held: Vec<u32> = (0..list.len()).map(|i| **list.items.at(i)).collect();
            held.sort_unstable();
            prop_assert_eq!(&held, &reference, "the same items");
            let alive = made.iter().filter(|item| item.strong_count() > 0).count();
            prop_assert_eq!(alive, reference.len(), "those not kept dropped");
            if retained && list.is_empty() {
                prop_assert_eq!(list.bytes(), 0, "emptied, it keeps no chunk");
            }
        }
    }
}

/// A list counts its chunks as glibc allocates them, each with its room however few items
/// it holds, and the list of them; empty, it takes nothing.
#[test]
fn a_list_counts_its_chunks_with_their_room() {
    let mut list = List::<u64, SMALL>::new();
    assert_eq!(list.bytes(), 0);
    list.push(1);
    let chunk = allocation(SMALL * size_of::<u64>());
    let outer = |list: &List<u64, SMALL>| vec::<Vec<u64>>(list.items.chunks.capacity());
    assert_eq!(list.bytes(), chunk + outer(&list));
    for n in 2..=SMALL as u64 + 1 {
        list.push(n);
    }
    assert_eq!(list.items.chunks.len(), 2);
    assert_eq!(list.bytes(), 2 * chunk + outer(&list));
}

#[test]
fn a_list_chunk_holds_sixty_four_kib_of_handles() {
    let mut list = List::<Rc<u32>>::new();
    list.push(Rc::new(1));
    assert_eq!(
        list.items.chunks[0].capacity() * size_of::<Rc<u32>>(),
        64 * 1024
    );
}
