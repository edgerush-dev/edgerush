//! The bytes a worker lends to an exchange and takes back when it is done with them.
//!
//! An exchange needs somewhere to put what it reads and what it is about to write. Making
//! that somewhere when the exchange starts and dropping it when the exchange ends is the
//! obvious arrangement and the expensive one: the memory is new every request, so the
//! first write to it is a cache miss and the allocator does the work twice. What nginx and
//! haproxy both do instead is keep the memory and lend it out — nginx from a free list of
//! buffers it has already made, haproxy from a pool it returns each buffer to the moment
//! that buffer is empty.
//!
//! So this holds blocks, hands them out on demand and takes them back. Two things follow
//! from that and are the point of the whole module:
//!
//! - **A block is lent, not made.** The same few blocks go round, so they stay in cache
//!   rather than wandering through the heap, and the memory a worker uses is bounded by
//!   how many exchanges are in flight rather than by how many connections it holds.
//! - **A returned block keeps its bytes.** They are nobody's bytes — [`Block::data`] shows
//!   only what has been put in since, and the cursors say where that is — so a block that
//!   comes back is not cleared, and one that goes out again is not cleared either. The
//!   zeroing happens once, when a block is first made, and never again.
//!
//! **A block is taken out, not borrowed**, in the sense [`super::pool`] means it: what
//! [`Blocks::take`] hands over is gone from the store until somebody gives it back, and a
//! block that is dropped instead is memory freed rather than an accounting error. Nothing
//! here returns a block by itself, because a block that returned itself when it went out
//! of scope would have to reach the store from wherever it was dropped, and the hot path
//! is not the place to find out whether that reach is allowed.
//!
//! Nothing here does I/O or reads a clock ([13 §7](../../../docs/13-http1-upstream.md)).

use super::H1Limits;

/// What a block holds when it is first lent: the read bound of
/// [13 §7](../../../docs/13-http1-upstream.md).
pub const SMALL: usize = 16 * 1024;

/// How big the blocks are and how many are kept.
///
/// `small` is what an exchange is given to begin with, and what nearly every exchange
/// finishes with: a head and a small answer fit in one. `large` is for the exchange that
/// does not fit — see [`Blocks::grow`] — and is made from the limits by [`Sizes::within`]
/// so that nothing the codec would accept can find itself with nowhere to be assembled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sizes {
    /// What a block holds when it is first asked for.
    pub small: usize,
    /// What a block holds after it has been grown once.
    pub large: usize,
    /// How many free blocks of each size a worker keeps. Beyond this a returned block is
    /// dropped: a burst should not leave its memory parked here for ever.
    pub parked: usize,
}

impl Sizes {
    /// Blocks that start at `small` and, grown, have room for anything `limits` lets the
    /// codec assemble whole.
    ///
    /// What has to be in one place at once is a head, a trailer section or a chunk line,
    /// each held to its own bound; everything else is handed on as it arrives. A grown
    /// block holds the biggest of the three and one more read besides, since the bytes
    /// that complete one arrive together with whatever came after it.
    pub fn within(limits: &H1Limits, small: usize) -> Self {
        let whole = limits.head.max(limits.trailers).max(limits.chunk_line);
        Self {
            small,
            large: whole + small,
            parked: 64,
        }
    }
}

impl Default for Sizes {
    fn default() -> Self {
        // Every exchange takes one of these, so it is the number the working set is made
        // of.
        Self::within(&H1Limits::default(), SMALL)
    }
}

/// Bytes on loan, and how far into them the reading and the writing have got.
///
/// The bytes are held at their full length throughout, which is what lets the same block
/// be used again without being cleared: length is not what says how much is in it.
/// [`Block::data`] does.
#[derive(Debug)]
pub struct Block {
    bytes: Vec<u8>,
    /// How much of `bytes` holds anything.
    filled: usize,
    /// How much of what it holds has been dealt with. Never past `filled`.
    taken: usize,
}

impl Block {
    /// What is in it and has not been dealt with yet.
    pub fn data(&self) -> &[u8] {
        &self.bytes[self.taken..self.filled]
    }

    /// How much [`Block::data`] would give.
    pub fn len(&self) -> usize {
        self.filled - self.taken
    }

    /// Whether it holds nothing that has not been dealt with — the moment a block is worth
    /// giving back.
    pub fn is_empty(&self) -> bool {
        self.filled == self.taken
    }

    /// The whole of it, whether or not anything is in it.
    pub fn capacity(&self) -> usize {
        self.bytes.len()
    }

    /// Says that the first `count` of [`Block::data`] have been dealt with.
    ///
    /// Nothing is moved: the cursor goes forward and the bytes stay where they are, which
    /// is the difference between this and taking them off the front. A block that has been
    /// dealt with entirely starts again from nothing, so the common exchange — fill it,
    /// use all of it — never moves a byte at all.
    ///
    /// # Panics
    ///
    /// In a debug build, if `count` is more than [`Block::len`]: dealing with bytes that
    /// were never put in is a mistake in the caller, and one worth hearing about where the
    /// tests and the fuzzers will hear it. A release build has no say in what reaches it,
    /// so it holds the cursor inside the block instead of failing a request over it.
    pub fn consume(&mut self, count: usize) {
        debug_assert!(count <= self.len(), "consumed {count} of {}", self.len());
        self.taken = self.taken.saturating_add(count).min(self.filled);
        if self.taken == self.filled {
            self.taken = 0;
            self.filled = 0;
        }
    }

    /// Where the next bytes read may go, which is whatever is left after what is in it.
    ///
    /// Empty when the block is full. A caller that is given nothing here has a block that
    /// needs [`Blocks::grow`], not a smaller read.
    pub fn room(&mut self) -> &mut [u8] {
        // Bytes already dealt with are in the way of nothing until the block is full, and
        // then they are in the way of everything: move what is left down over them, which
        // is a copy of what remains rather than of the block.
        if self.filled == self.bytes.len() && self.taken > 0 {
            self.bytes.copy_within(self.taken..self.filled, 0);
            self.filled -= self.taken;
            self.taken = 0;
        }
        &mut self.bytes[self.filled..]
    }

    /// Says that `count` bytes were put into the front of [`Block::room`].
    ///
    /// # Panics
    ///
    /// In a debug build, if `count` is more than the room there was, for the reason
    /// [`Block::consume`] gives.
    pub fn arrived(&mut self, count: usize) {
        let room = self.bytes.len() - self.filled;
        debug_assert!(count <= room, "arrived {count} where {room} would fit");
        self.filled = self.filled.saturating_add(count).min(self.bytes.len());
    }
}

/// The blocks one worker has, lent out and taken back.
///
/// One per worker and it never leaves it, for the reason [`super::pool`] gives: what is
/// shared between threads is what two requests can be half way through at once.
#[derive(Debug)]
pub struct Blocks {
    /// Free blocks, by the size they were made at.
    small: Vec<Vec<u8>>,
    large: Vec<Vec<u8>>,
    /// Free buffers for what is waiting to be written, empty and with their room made.
    staging: Vec<Vec<u8>>,
    sizes: Sizes,
}

impl Blocks {
    /// A worker's blocks, holding nothing until something is asked of them.
    pub fn new(sizes: Sizes) -> Self {
        Self {
            small: Vec::new(),
            large: Vec::new(),
            staging: Vec::new(),
            sizes,
        }
    }

    /// The sizes it was made with.
    pub fn sizes(&self) -> Sizes {
        self.sizes
    }

    /// How many free blocks and buffers are being kept, which is the memory parked here.
    pub fn parked(&self) -> usize {
        self.small.len() + self.large.len() + self.staging.len()
    }

    /// A block holding nothing, of the size an exchange starts with.
    ///
    /// One that has been used before if there is one, and a new one otherwise. Either way
    /// it holds nothing as far as its reader is concerned.
    pub fn take(&mut self) -> Block {
        Self::lend(&mut self.small, self.sizes.small)
    }

    /// The same block with room for a head that would not fit in it.
    ///
    /// The bytes that were in it come across; the block it was goes back. Growing a block
    /// that is already large gives it back unchanged: there is one step, because `large`
    /// is bigger than the most a head may be and a head is the only thing that has to be
    /// assembled whole.
    pub fn grow(&mut self, block: Block) -> Block {
        if block.capacity() >= self.sizes.large {
            return block;
        }
        let mut grown = Self::lend(&mut self.large, self.sizes.large);
        let data = block.data();
        grown.bytes[..data.len()].copy_from_slice(data);
        grown.filled = data.len();
        self.give(block);
        grown
    }

    /// Takes a block back, to be lent again.
    ///
    /// Dropped rather than kept when there are already enough of its size: memory parked
    /// here is memory a worker is holding for work it is not doing.
    pub fn give(&mut self, block: Block) {
        let free = if block.capacity() >= self.sizes.large {
            &mut self.large
        } else {
            &mut self.small
        };
        if free.len() < self.sizes.parked {
            free.push(block.bytes);
        }
    }

    /// A buffer to stage what is to be written in: empty, with room for `room` bytes.
    ///
    /// Given out empty rather than at its full length, which is the difference between
    /// this and a block. What is staged is appended, so nothing beyond what was put there
    /// can be reached, and appending needs no bytes to have been set first. What it saves
    /// is the growing: a buffer built up from nothing reaches its size by doubling, and
    /// every doubling is another allocation and another copy of what was already in it.
    pub fn take_staging(&mut self, room: usize) -> Vec<u8> {
        match self.staging.pop() {
            Some(buffer) if buffer.capacity() >= room => buffer,
            _ => Vec::with_capacity(room),
        }
    }

    /// Takes a staging buffer back, to be lent again, emptied; or drops it, when there are
    /// already enough.
    pub fn give_staging(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.staging.len() < self.sizes.parked {
            self.staging.push(buffer);
        }
    }

    /// Drops free blocks down to `keep` of each size.
    ///
    /// For the once-a-second maintenance a worker already does: a burst that has passed
    /// should not go on costing what it cost at its peak.
    pub fn trim(&mut self, keep: usize) {
        self.small.truncate(keep);
        self.large.truncate(keep);
        self.staging.truncate(keep);
    }

    /// A free block if there is one, and a new one if there is not.
    ///
    /// The one place a block's bytes are ever set to anything: made once, at full length,
    /// and from then on reused as they are.
    fn lend(free: &mut Vec<Vec<u8>>, size: usize) -> Block {
        let bytes = free.pop().unwrap_or_else(|| vec![0; size]);
        Block {
            bytes,
            filled: 0,
            taken: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn sizes() -> Sizes {
        Sizes {
            small: 16,
            large: 64,
            parked: 2,
        }
    }

    /// Puts bytes in the way an exchange does: into the room, then saying how many.
    fn put(block: &mut Block, bytes: &[u8]) -> usize {
        let room = block.room();
        let take = room.len().min(bytes.len());
        room[..take].copy_from_slice(&bytes[..take]);
        block.arrived(take);
        take
    }

    #[test]
    fn a_block_starts_empty_and_at_its_size() {
        let mut blocks = Blocks::new(sizes());
        let block = blocks.take();
        assert!(block.is_empty());
        assert_eq!(block.len(), 0);
        assert_eq!(block.data(), b"");
        assert_eq!(block.capacity(), 16);
    }

    #[test]
    fn what_was_put_in_is_what_comes_out() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        assert_eq!(put(&mut block, b"hello"), 5);
        assert_eq!(block.data(), b"hello");
        block.consume(2);
        assert_eq!(block.data(), b"llo");
        assert_eq!(block.len(), 3);
    }

    #[test]
    fn a_block_used_again_shows_nothing_of_what_it_held() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"secrets");
        blocks.give(block);

        let again = blocks.take();
        assert!(again.is_empty(), "a lent block carried something over");
        assert_eq!(again.data(), b"");
        // The same memory came back rather than being made again: that is the point of
        // giving it back at all.
        assert_eq!(blocks.parked(), 0);
    }

    #[test]
    fn a_block_lent_again_is_not_cleared() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"was here before!");
        block.consume(16);
        blocks.give(block);

        let mut again = blocks.take();
        // Clearing a block that is about to be written over is work nobody asked for, and
        // not doing it is why a block is lent rather than made. What was in it is still
        // there, out of reach of `data` and harmless. This is the module's one claim that
        // nothing else would notice the loss of.
        assert_eq!(
            again.room(),
            &b"was here before!"[..],
            "the block was cleared on its way out"
        );
    }

    #[test]
    fn room_is_what_is_left_and_runs_out() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        assert_eq!(block.room().len(), 16);
        put(&mut block, &[b'x'; 16]);
        assert_eq!(block.room().len(), 0, "a full block offered room");
        assert_eq!(block.len(), 16);
    }

    #[test]
    fn using_everything_costs_no_move() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"abcd");
        block.consume(4);
        // Dealt with entirely, so it starts again rather than holding a cursor at the end:
        // the whole block is room, and nothing was copied to make it so.
        assert!(block.is_empty());
        assert_eq!(block.room().len(), 16);
    }

    #[test]
    fn a_full_block_makes_room_by_moving_what_is_left() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, &[b'a'; 16]);
        block.consume(10);
        // Full, and ten of it dealt with. The six that are left move down and the ten they
        // were behind become room.
        assert_eq!(block.room().len(), 10);
        assert_eq!(block.data(), &[b'a'; 6]);
    }

    #[test]
    fn a_part_full_block_moves_nothing() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"abcdefgh");
        block.consume(4);
        // Room remains, so the four dealt with are in nobody's way and stay where they are.
        assert_eq!(block.room().len(), 8);
        assert_eq!(block.data(), b"efgh");
    }

    #[test]
    fn growing_carries_what_was_in_it_across() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, &[b'h'; 16]);
        block.consume(4);

        let grown = blocks.grow(block);
        assert_eq!(grown.capacity(), 64);
        assert_eq!(
            grown.data(),
            &[b'h'; 12],
            "growing lost what was being assembled"
        );
        // The small one it was goes back to be lent again.
        assert_eq!(blocks.parked(), 1);
    }

    #[test]
    fn growing_a_grown_block_leaves_it_alone() {
        let mut blocks = Blocks::new(sizes());
        let block = blocks.take();
        let grown = blocks.grow(block);
        let again = blocks.grow(grown);
        assert_eq!(again.capacity(), 64);
        assert_eq!(blocks.parked(), 1, "the large block was not kept as it was");
    }

    #[test]
    fn a_grown_block_goes_back_to_its_own_size() {
        let mut blocks = Blocks::new(sizes());
        let block = blocks.take();
        let grown = blocks.grow(block);
        blocks.give(grown);
        // One small, from the growing, and one large.
        assert_eq!(blocks.parked(), 2);
        assert_eq!(
            blocks.take().capacity(),
            16,
            "a large block was lent as a small one"
        );
    }

    #[test]
    fn a_grown_block_has_room_for_anything_assembled_whole() {
        let limits = H1Limits::default();
        let sizes = Sizes::within(&limits, 4096);
        // The largest of each, and the read that completed it, which brought bytes of
        // what came after along with it.
        for (what, whole) in [
            ("head", limits.head),
            ("trailers", limits.trailers),
            ("chunk line", limits.chunk_line),
        ] {
            assert!(
                sizes.large >= whole + sizes.small,
                "a {what} of {whole} bytes and a read behind it do not fit in {}",
                sizes.large
            );
        }
    }

    #[test]
    fn a_staging_buffer_is_lent_empty_with_its_room_made() {
        let mut blocks = Blocks::new(sizes());
        let buffer = blocks.take_staging(32);
        assert!(buffer.is_empty());
        assert!(
            buffer.capacity() >= 32,
            "lent with {} of room",
            buffer.capacity()
        );
    }

    #[test]
    fn a_staging_buffer_comes_back_empty_and_is_the_same_memory() {
        let mut blocks = Blocks::new(sizes());
        let mut buffer = blocks.take_staging(32);
        buffer.extend_from_slice(b"GET / HTTP/1.1\r\n");
        let memory = buffer.as_ptr();
        blocks.give_staging(buffer);

        let again = blocks.take_staging(32);
        assert!(again.is_empty(), "a staging buffer carried something over");
        // The same allocation and not a new one of the same size: making it again is
        // exactly the work lending it is there to save.
        assert_eq!(again.as_ptr(), memory, "the buffer was made again");
    }

    #[test]
    fn a_staging_buffer_too_small_for_the_room_is_not_lent() {
        let mut blocks = Blocks::new(sizes());
        let small = blocks.take_staging(8);
        blocks.give_staging(small);
        let bigger = blocks.take_staging(64);
        assert!(
            bigger.capacity() >= 64,
            "lent with {} of room",
            bigger.capacity()
        );
    }

    #[test]
    fn only_so_many_staging_buffers_are_kept() {
        let mut blocks = Blocks::new(sizes());
        let held: Vec<Vec<u8>> = (0..5).map(|_| blocks.take_staging(32)).collect();
        for buffer in held {
            blocks.give_staging(buffer);
        }
        assert_eq!(blocks.parked(), 2, "a burst left its memory parked");
        blocks.trim(1);
        assert_eq!(blocks.parked(), 1, "trimming left staging buffers behind");
    }

    #[test]
    fn only_so_many_are_kept() {
        let mut blocks = Blocks::new(sizes());
        let held: Vec<Block> = (0..5).map(|_| blocks.take()).collect();
        for block in held {
            blocks.give(block);
        }
        assert_eq!(blocks.parked(), 2, "a burst left its memory parked");
    }

    #[test]
    fn trimming_drops_to_what_is_asked() {
        let mut blocks = Blocks::new(sizes());
        let held: Vec<Block> = (0..2).map(|_| blocks.take()).collect();
        for block in held {
            blocks.give(block);
        }
        assert_eq!(blocks.parked(), 2);
        blocks.trim(1);
        assert_eq!(blocks.parked(), 1);
        blocks.trim(0);
        assert_eq!(blocks.parked(), 0);
    }

    #[test]
    #[should_panic(expected = "consumed")]
    fn dealing_with_more_than_is_there_is_a_bug() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"abc");
        block.consume(4);
    }

    #[test]
    #[should_panic(expected = "arrived")]
    fn arriving_past_the_end_is_a_bug() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        block.arrived(17);
    }

    /// What an exchange does to a block, as a sequence to be replayed against a queue that
    /// obviously behaves: bytes put in come out in order, once each, and nothing else does.
    #[derive(Debug, Clone)]
    enum Doing {
        Put(usize),
        Consume(usize),
        Recycle,
    }

    fn doings() -> impl Strategy<Value = Vec<Doing>> {
        prop::collection::vec(
            prop_oneof![
                (1usize..24).prop_map(Doing::Put),
                (0usize..24).prop_map(Doing::Consume),
                Just(Doing::Recycle),
            ],
            0..60,
        )
    }

    proptest! {
        /// A block loses no byte, invents none and reorders none, however it is filled,
        /// consumed and lent again.
        #[test]
        fn a_block_is_a_queue_of_bytes(doings in doings()) {
            let mut blocks = Blocks::new(sizes());
            let mut block = blocks.take();
            // What the block should be holding, and what to put in next: every byte put in
            // is different from the last, so an off-by-one would show as a wrong byte and
            // not merely a wrong count.
            let mut expected: Vec<u8> = Vec::new();
            let mut next: u8 = 0;

            for doing in doings {
                match doing {
                    Doing::Put(count) => {
                        let bytes: Vec<u8> = (0..count)
                            .map(|_| {
                                next = next.wrapping_add(1);
                                next
                            })
                            .collect();
                        let put = put(&mut block, &bytes);
                        // Only what fitted went in, and the rest was never offered.
                        expected.extend_from_slice(&bytes[..put]);
                        next = next.wrapping_sub((count - put) as u8);
                    }
                    Doing::Consume(count) => {
                        let count = count.min(block.len());
                        block.consume(count);
                        expected.drain(..count);
                    }
                    Doing::Recycle => {
                        // Only an empty block is worth giving back, which is when an
                        // exchange gives one back.
                        if block.is_empty() {
                            blocks.give(block);
                            block = blocks.take();
                            expected.clear();
                        }
                    }
                }
                prop_assert_eq!(block.data(), &expected[..]);
                prop_assert_eq!(block.len(), expected.len());
                prop_assert_eq!(block.is_empty(), expected.is_empty());
            }
        }

        /// However many go out and come back, only so many are kept.
        #[test]
        fn what_is_parked_stays_bounded(rounds in 0usize..40) {
            let mut blocks = Blocks::new(sizes());
            for _ in 0..rounds {
                let held: Vec<Block> = (0..3).map(|_| blocks.take()).collect();
                for block in held {
                    blocks.give(block);
                }
                prop_assert!(blocks.parked() <= 2 * sizes().parked);
            }
        }
    }
}
