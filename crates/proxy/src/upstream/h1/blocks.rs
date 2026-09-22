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
//! So this holds blocks, hands them out on demand and takes them back. Three things follow
//! from that and are the point of the whole module:
//!
//! - **A block is lent, not made.** The same few blocks go round, so they stay in cache
//!   rather than wandering through the heap, and the memory a worker uses is bounded by
//!   how many exchanges are in flight rather than by how many connections it holds.
//! - **A returned block keeps its bytes.** They are nobody's bytes — [`Block::data`] shows
//!   only what has been put in since, and the cursors say where that is — so a block that
//!   comes back is not cleared, and one that goes out again is not cleared either. The
//!   zeroing happens when a block is first made, and again only when memory is taken back
//!   from frames that have gone.
//! - **Frames are cut from a block, not copied out of it.** An answer's data reaches the
//!   engine as a piece of the block's own memory, which the block gives up and takes back
//!   only once the engine has let go of it ([`Block::take_frame`], [`Blocks::refill`]).
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
use bytes::{Buf, Bytes, BytesMut};
use std::ops::Range;

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
    /// How many of each kind a quiet worker keeps: what [`Blocks::sweep`] trims down to,
    /// and all of the grown blocks that are ever parked. A grown block is several times
    /// the size of the others and needed only by the odd large head, so a burst of those
    /// is not kept at all — nginx keeps four of its large header buffers for the same
    /// reason.
    pub kept: usize,
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
            kept: 4,
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
///
/// **Frames are cut from it, not copied out of it** ([`Block::take_frame`]). A frame is a
/// piece of the block's own memory, and from then on the engine's until it lets go: the
/// block keeps only what lies after it. So a block that has had frames cut from it can come
/// to the end of its memory without being full, and [`Blocks::refill`] takes the memory
/// back once every frame cut from it is gone — never before, which is what keeps a frame
/// the engine still holds from being written over.
#[derive(Debug)]
pub struct Block {
    /// What is left of the block's memory, initialised throughout. Frames cut from the
    /// front of it no longer belong to it.
    bytes: BytesMut,
    /// How much of `bytes` holds anything.
    filled: usize,
    /// How much of what it holds has been dealt with. Never past `filled`.
    taken: usize,
    /// What it was made at, whatever is left of it now.
    size: usize,
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

    /// What it was made at, whether or not anything is in it and however much of it has
    /// gone into frames.
    pub fn capacity(&self) -> usize {
        self.size
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

    /// Cuts `range` of [`Block::data`] out as a frame of its own, sharing the block's
    /// memory rather than copying it, and says everything up to `through` has been dealt
    /// with. What came before the frame goes with it; what comes after it stays.
    ///
    /// # Panics
    ///
    /// In a debug build, if `range` does not lie within the first `through` bytes of
    /// [`Block::data`], for the reason [`Block::consume`] gives. A release build holds
    /// both inside what is there instead.
    pub fn take_frame(&mut self, range: Range<usize>, through: usize) -> Bytes {
        debug_assert!(
            range.start <= range.end && range.end <= through && through <= self.len(),
            "a frame of {range:?} through {through} of {}",
            self.len()
        );
        let start = self.taken.saturating_add(range.start).min(self.filled);
        let count = range.len().min(self.filled - start);
        // The frame has to be at the front to be cut off, so what lies before it goes
        // first: bytes already dealt with, and whatever framing preceded it.
        self.bytes.advance(start);
        self.filled -= start;
        self.taken = 0;
        let frame = self.bytes.split_to(count).freeze();
        self.filled -= count;
        self.consume(through.saturating_sub(range.end).min(self.filled));
        frame
    }

    /// Where the next bytes read may go, which is whatever is left after what is in it.
    ///
    /// Empty when the block has no memory left after what is in it. A caller that is given
    /// nothing here needs [`Blocks::refill`], not a smaller read.
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

    /// Takes the block's memory back, at its full size, with what it holds moved to the
    /// front — if nothing else holds any of it. Says whether it could; where a frame cut
    /// from it is still held, the memory is not the block's to take, and it keeps only
    /// what it holds, with no room after it.
    fn reclaim(&mut self) -> bool {
        let held = self.len();
        self.bytes.advance(self.taken);
        self.bytes.truncate(held);
        self.filled = held;
        self.taken = 0;
        if !self.bytes.try_reclaim(self.size - held) {
            return false;
        }
        // The one place memory taken back is set to anything. What was there is bytes of
        // frames that have gone, and a read is given initialised room to put its bytes in.
        self.bytes.resize(self.size, 0);
        true
    }
}

/// The blocks one worker has, lent out and taken back.
///
/// One per worker and it never leaves it, for the reason [`super::pool`] gives: what is
/// shared between threads is what two requests can be half way through at once.
#[derive(Debug)]
pub struct Blocks {
    /// Free blocks, by the size they were made at. Some may still have frames cut from
    /// them in the engine's hands, and are lent only once those have gone.
    small: Vec<BytesMut>,
    large: Vec<BytesMut>,
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

    /// The same block with room to read into, what it held kept at the front.
    ///
    /// A block that has come to the end of its memory because frames were cut from it
    /// takes the memory back if they have all gone; if one is still held, what it holds
    /// moves into a free block and it is given back, to be taken back into use once they
    /// have. A block that is simply full of one thing — a head that does not fit — is
    /// grown ([`Blocks::grow`]).
    pub fn refill(&mut self, mut block: Block) -> Block {
        if !block.room().is_empty() {
            return block;
        }
        if block.bytes.len() < block.size {
            if block.reclaim() {
                return block;
            }
            let free = if block.size >= self.sizes.large {
                &mut self.large
            } else {
                &mut self.small
            };
            let mut fresh = Self::lend(free, block.size);
            let held = block.len();
            fresh.room()[..held].copy_from_slice(block.data());
            fresh.arrived(held);
            block.consume(held);
            self.give(block);
            block = fresh;
            if !block.room().is_empty() {
                return block;
            }
        }
        self.grow(block)
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
        let (free, most) = if block.capacity() >= self.sizes.large {
            (&mut self.large, self.sizes.kept)
        } else {
            (&mut self.small, self.sizes.parked)
        };
        if free.len() < most {
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

    /// Trims down to what a quiet worker keeps, for the worker's once-a-second sweep.
    ///
    /// Under load the blocks trimmed are made again within the second, which is a few
    /// allocations a second rather than one a request; once the load has gone, they are
    /// not.
    pub fn sweep(&mut self) {
        self.trim(self.sizes.kept);
    }

    /// A free block that is whole, or can be made whole, if there is one, and a new one
    /// if there is not.
    ///
    /// A free block whose frames the engine still holds is passed over and left where it
    /// is, to be taken back once they have gone. A new block is set to zeros once, at its
    /// full length; one lent again is not set to anything.
    fn lend(free: &mut Vec<BytesMut>, size: usize) -> Block {
        let found = free.iter_mut().rposition(|bytes| {
            if bytes.len() == size {
                return true;
            }
            // Holds nothing any reader will look at: it came back empty.
            bytes.clear();
            if bytes.try_reclaim(size) {
                bytes.resize(size, 0);
                return true;
            }
            false
        });
        let bytes = match found {
            Some(at) => free.swap_remove(at),
            None => BytesMut::zeroed(size),
        };
        Block {
            bytes,
            filled: 0,
            taken: 0,
            size,
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
            kept: 1,
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

    /// Grown blocks are five times the size of the others and seldom needed, so fewer of
    /// them are parked: one given back beyond `kept` is freed.
    #[test]
    fn only_so_many_grown_blocks_are_kept() {
        let mut blocks = Blocks::new(sizes());
        let grown: Vec<Block> = (0..3)
            .map(|_| {
                let block = blocks.take();
                blocks.grow(block)
            })
            .collect();
        for block in grown {
            blocks.give(block);
        }
        // The one small block that every growing handed back and the next took again, and
        // one grown one. Held to `parked` alone, two grown ones would be here.
        assert_eq!(
            blocks.parked(),
            2,
            "grown blocks were parked past what is kept"
        );
    }

    /// A sweep lets go of what a burst left parked, down to what a quiet worker keeps of
    /// each kind.
    #[test]
    fn a_sweep_keeps_what_a_quiet_worker_keeps() {
        let mut blocks = Blocks::new(sizes());
        let small: Vec<Block> = (0..2).map(|_| blocks.take()).collect();
        let staging: Vec<Vec<u8>> = (0..2).map(|_| blocks.take_staging(8)).collect();
        let block = blocks.take();
        let grown = blocks.grow(block);
        for block in small {
            blocks.give(block);
        }
        for buffer in staging {
            blocks.give_staging(buffer);
        }
        blocks.give(grown);
        assert_eq!(blocks.parked(), 5);
        blocks.sweep();
        assert_eq!(blocks.parked(), 3, "a sweep left a burst's memory parked");
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

    /// A frame is a piece of the block's own memory, not a copy of it: the copy, and the
    /// allocation behind it, is what a large answer spent its time on.
    #[test]
    fn a_frame_is_cut_from_the_block_rather_than_copied() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"5\r\nhello\r\n");
        let at = block.data()[3..].as_ptr();
        let frame = block.take_frame(3..8, 10);
        assert_eq!(&frame[..], b"hello");
        assert_eq!(frame.as_ptr(), at, "the frame was copied");
        assert!(block.is_empty());
    }

    #[test]
    fn what_follows_a_frame_stays_in_the_block() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"5\r\nhello\r\n3\r\nab");
        let frame = block.take_frame(3..8, 10);
        assert_eq!(&frame[..], b"hello");
        assert_eq!(block.data(), b"3\r\nab");
    }

    /// **The one thing a shared block must never do.** A frame the engine still holds is
    /// memory the block no longer owns, and nothing read after it may land there.
    #[test]
    fn a_frame_still_held_is_never_written_over() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, &[b'a'; 16]);
        let held = block.take_frame(0..16, 16);
        // Nothing of its own is left, and what it had is held: refilling it cannot take
        // that memory back.
        let mut block = blocks.refill(block);
        assert!(!block.room().is_empty(), "refilled with no room");
        assert_eq!(put(&mut block, &[b'b'; 16]), 16);
        assert_eq!(&held[..], &[b'a'; 16], "a held frame was written over");
        assert_eq!(block.data(), &[b'b'; 16]);
    }

    /// Once the frames cut from it are gone, the block's memory is its own again, and a
    /// refill takes it back rather than making more; what had not been dealt with comes
    /// along to the front.
    #[test]
    fn a_block_whose_frames_are_gone_is_its_own_again() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, &[b'a'; 16]);
        let base = block.data().as_ptr();
        drop(block.take_frame(0..10, 10));
        let mut block = blocks.refill(block);
        assert_eq!(block.data(), &[b'a'; 6], "unread bytes were lost");
        assert_eq!(block.data().as_ptr(), base, "the memory was not taken back");
        assert_eq!(block.room().len(), 10);
        assert_eq!(blocks.parked(), 0);
    }

    /// Where a frame still holds the memory, the refill is a free block instead, and
    /// what had not been dealt with moves into it.
    #[test]
    fn a_block_whose_frames_are_held_is_refilled_elsewhere_with_what_it_had() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, b"0123456789abcdef");
        let held = block.take_frame(0..10, 10);
        let mut block = blocks.refill(block);
        assert_eq!(block.data(), b"abcdef");
        assert_eq!(block.room().len(), 10);
        assert_eq!(&held[..], b"0123456789");
    }

    /// A block given back while a frame still holds part of it is taken back into use
    /// once the frame has gone, rather than being made again.
    #[test]
    fn a_block_given_back_while_a_frame_holds_it_is_lent_again_once_it_has_gone() {
        let mut blocks = Blocks::new(sizes());
        let mut block = blocks.take();
        put(&mut block, &[b'a'; 16]);
        let base = block.data().as_ptr();
        let frame = block.take_frame(0..16, 16);
        blocks.give(block);
        drop(frame);
        let mut again = blocks.take();
        assert!(again.is_empty());
        assert_eq!(again.room().len(), 16);
        assert_eq!(
            again.room().as_ptr(),
            base.cast_mut(),
            "the block was made again"
        );
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
        /// Cut a frame of up to this many bytes off the front, and hold on to it or not.
        Frame(usize, bool),
        /// Make room, as a read that found none does.
        Refill,
        /// Let go of the oldest frame still held, as the engine does once it is written.
        Release,
    }

    fn doings() -> impl Strategy<Value = Vec<Doing>> {
        prop::collection::vec(
            prop_oneof![
                (1usize..24).prop_map(Doing::Put),
                (0usize..24).prop_map(Doing::Consume),
                Just(Doing::Recycle),
                (0usize..24, any::<bool>()).prop_map(|(most, keep)| Doing::Frame(most, keep)),
                Just(Doing::Refill),
                Just(Doing::Release),
            ],
            0..60,
        )
    }

    proptest! {
        /// A block loses no byte, invents none and reorders none, however it is filled,
        /// consumed, cut into frames, refilled and lent again — and no frame still held
        /// is ever written over, whatever is read after it.
        #[test]
        fn a_block_is_a_queue_of_bytes(doings in doings()) {
            let mut blocks = Blocks::new(sizes());
            let mut block = blocks.take();
            // Frames still held, with what each was when it was cut.
            let mut held: std::collections::VecDeque<(Bytes, Vec<u8>)> = Default::default();
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
                    Doing::Frame(most, keep) => {
                        let count = most.min(block.len());
                        let frame = block.take_frame(0..count, count);
                        let was: Vec<u8> = expected.drain(..count).collect();
                        prop_assert_eq!(&frame[..], &was[..]);
                        if keep {
                            held.push_back((frame, was));
                        }
                    }
                    Doing::Refill => {
                        if block.room().is_empty() {
                            block = blocks.refill(block);
                        }
                    }
                    Doing::Release => {
                        held.pop_front();
                    }
                }
                for (frame, was) in &held {
                    prop_assert_eq!(&frame[..], &was[..], "a held frame was written over");
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
