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
//! **Every block's memory is paid for** against the worker's [`Storage`] before it is made,
//! and stays paid for as long as it lives: lent, parked, or let go of while a frame cut
//! from it is still held, which is when the memory is still there though no block is
//! ([14 §8](../../../docs/14-downstream-server.md)). A block the worker cannot pay for is
//! not made.
//!
//! Nothing here does I/O or reads a clock ([13 §7](../../../docs/13-http1-upstream.md)).

use super::H1Limits;
use crate::storage::{Charge, Exhausted, Storage};
use bytes::{Buf, Bytes, BytesMut};
use http::{HeaderName, HeaderValue};
use std::ops::Range;
use std::rc::Rc;

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
    /// The smallest frame cut from a block rather than copied out of it
    /// ([`Block::take_frame`]). Cutting saves a large answer an allocation and a copy a
    /// piece, and the allocator's handing that memory back to the kernel; copying keeps a
    /// small answer in the hot front of its block, where the next one reads too.
    pub cut: usize,
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
            cut: 4 * 1024,
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

/// A block's memory and what it is charged, which go everywhere together.
///
/// Let go of, it gives its charge back if the memory goes with it, and leaves the charge
/// with the worker's account if a frame cut from it is still held
/// ([`Charge::outlive`]).
#[derive(Debug)]
struct Memory {
    bytes: BytesMut,
    charge: Charge,
}

impl Drop for Memory {
    fn drop(&mut self) {
        self.charge.outlive(std::mem::take(&mut self.bytes));
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
    /// front of it no longer belong to it; its charge is for all of it.
    memory: Memory,
    /// How much of `bytes` holds anything.
    filled: usize,
    /// How much of what it holds has been dealt with. Never past `filled`.
    taken: usize,
    /// What it was made at, whatever is left of it now.
    size: usize,
    /// The smallest frame cut from it rather than copied ([`Sizes::cut`]).
    cut: usize,
}

impl Block {
    /// What is in it and has not been dealt with yet.
    pub fn data(&self) -> &[u8] {
        &self.memory.bytes[self.taken..self.filled]
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
    /// A frame smaller than [`Sizes::cut`] is copied instead, and the block keeps its
    /// memory.
    ///
    /// # Panics
    ///
    /// In a debug build, if `range` does not lie within the first `through` bytes of
    /// [`Block::data`], for the reason [`Block::consume`] gives. A release build holds
    /// both inside what is there instead.
    pub fn take_frame(&mut self, range: Range<usize>, through: usize) -> Bytes {
        let (start, count) = self.placed(&range, through);
        if count < self.cut {
            let frame = Bytes::copy_from_slice(&self.memory.bytes[start..start + count]);
            self.consume(through.min(self.len()));
            return frame;
        }
        self.cut_at(start, count, range.end, through)
    }

    /// The same, cutting however small the frame is: for bytes that are to stay paid for
    /// through the block they were read into for as long as they live, as a copy could
    /// not be ([14 §8](../../../docs/14-downstream-server.md)). The block's memory is held
    /// until the frame goes.
    ///
    /// # Panics
    ///
    /// As [`Block::take_frame`].
    pub fn cut_frame(&mut self, range: Range<usize>, through: usize) -> Bytes {
        let (start, count) = self.placed(&range, through);
        self.cut_at(start, count, range.end, through)
    }

    /// Where `range` of [`Block::data`] starts in the block's memory, and how long it is,
    /// held inside what is there.
    fn placed(&self, range: &Range<usize>, through: usize) -> (usize, usize) {
        debug_assert!(
            range.start <= range.end && range.end <= through && through <= self.len(),
            "a frame of {range:?} through {through} of {}",
            self.len()
        );
        let start = self.taken.saturating_add(range.start).min(self.filled);
        (start, range.len().min(self.filled - start))
    }

    /// Cuts `count` bytes at `start` off as a frame, and deals with everything up to
    /// `through`, whose frame ended at `end`.
    fn cut_at(&mut self, start: usize, count: usize, end: usize, through: usize) -> Bytes {
        // The frame has to be at the front to be cut off, so what lies before it goes
        // first: bytes already dealt with, and whatever framing preceded it.
        self.memory.bytes.advance(start);
        self.filled -= start;
        self.taken = 0;
        let frame = self.memory.bytes.split_to(count).freeze();
        self.filled -= count;
        self.consume(through.saturating_sub(end).min(self.filled));
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
        if self.filled == self.memory.bytes.len() && self.taken > 0 {
            self.memory.bytes.copy_within(self.taken..self.filled, 0);
            self.filled -= self.taken;
            self.taken = 0;
        }
        &mut self.memory.bytes[self.filled..]
    }

    /// Says that `count` bytes were put into the front of [`Block::room`].
    ///
    /// # Panics
    ///
    /// In a debug build, if `count` is more than the room there was, for the reason
    /// [`Block::consume`] gives.
    pub fn arrived(&mut self, count: usize) {
        let room = self.memory.bytes.len() - self.filled;
        debug_assert!(count <= room, "arrived {count} where {room} would fit");
        self.filled = self
            .filled
            .saturating_add(count)
            .min(self.memory.bytes.len());
    }

    /// Takes the block's memory back, at its full size, with what it holds moved to the
    /// front — if nothing else holds any of it. Says whether it could; where a frame cut
    /// from it is still held, the memory is not the block's to take, and it keeps only
    /// what it holds, with no room after it.
    fn reclaim(&mut self) -> bool {
        let held = self.len();
        self.memory.bytes.advance(self.taken);
        self.memory.bytes.truncate(held);
        self.filled = held;
        self.taken = 0;
        if !self.memory.bytes.try_reclaim(self.size - held) {
            return false;
        }
        // The one place memory taken back is set to anything. What was there is bytes of
        // frames that have gone, and a read is given initialised room to put its bytes in.
        self.memory.bytes.resize(self.size, 0);
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
    small: Vec<Memory>,
    large: Vec<Memory>,
    /// Free buffers for what is waiting to be written, empty and with their room made, each
    /// with the charge for its capacity.
    staging: Vec<(Vec<u8>, Charge)>,
    /// Free lists for the fields a request's head adds, empty and with their room made: a
    /// head read by our own server adds some to nearly every request, and would otherwise
    /// make one for each. Small (a few fields each), so not paid for against `storage`.
    edits: Vec<Vec<(HeaderName, HeaderValue)>>,
    sizes: Sizes,
    /// What every block made here is paid for against.
    storage: Rc<Storage>,
}

impl Blocks {
    /// A worker's blocks, holding nothing until something is asked of them, and paying for
    /// what they make against `storage`.
    pub fn new(sizes: Sizes, storage: Rc<Storage>) -> Self {
        Self {
            small: Vec::new(),
            large: Vec::new(),
            staging: Vec::new(),
            edits: Vec::new(),
            sizes,
            storage,
        }
    }

    /// The sizes it was made with.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn sizes(&self) -> Sizes {
        self.sizes
    }

    /// How many free blocks and buffers are being kept, which is the memory parked here.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn parked(&self) -> usize {
        self.small.len() + self.large.len() + self.staging.len()
    }

    /// A block holding nothing, of the size an exchange starts with.
    ///
    /// One that has been used before if there is one, and a new one otherwise. Either way
    /// it holds nothing as far as its reader is concerned.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if a new one is needed and the worker cannot pay for it.
    pub fn take(&mut self) -> Result<Block, Exhausted> {
        Self::lend(
            &self.storage,
            &mut self.small,
            self.sizes.small,
            self.sizes.small / 2,
            self.sizes.cut,
        )
    }

    /// An empty grown block, for a read larger than a small block holds: a long request
    /// body's, which are read up to 64 KiB at a time ([14 §8](../../../../docs/14-downstream-server.md)).
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if the worker cannot pay for it.
    pub fn take_grown(&mut self) -> Result<Block, Exhausted> {
        let (large, cut) = (self.sizes.large, self.sizes.cut);
        Self::lend(&self.storage, &mut self.large, large, large, cut)
    }

    /// The same block with room to read into, what it held kept at the front.
    ///
    /// A block that has come to the end of its memory because frames were cut from it
    /// takes the memory back if they have all gone; if one is still held, what it holds
    /// moves into a free block and it is given back, to be taken back into use once they
    /// have. A block that is simply full of one thing — a head that does not fit — is
    /// grown ([`Blocks::grow`]).
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if a new block is needed and the worker cannot pay for it. The block
    /// and what it held are let go of.
    pub fn refill(&mut self, mut block: Block) -> Result<Block, Exhausted> {
        if !block.room().is_empty() {
            return Ok(block);
        }
        if block.memory.bytes.len() < block.size {
            if block.reclaim() {
                return Ok(block);
            }
            let free = if block.size >= self.sizes.large {
                &mut self.large
            } else {
                &mut self.small
            };
            let held = block.len();
            // Room for what it holds, and half a block more to read into.
            let least = held.saturating_add(block.size / 2).min(block.size);
            let mut fresh = Self::lend(&self.storage, free, block.size, least, block.cut)?;
            fresh.room()[..held].copy_from_slice(block.data());
            fresh.arrived(held);
            block.consume(held);
            self.give(block);
            block = fresh;
            if !block.room().is_empty() {
                return Ok(block);
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
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if a new grown block is needed and the worker cannot pay for it while
    /// still paying for this one, as it has to until what is in it has moved across. The
    /// block and what it held are let go of.
    pub fn grow(&mut self, block: Block) -> Result<Block, Exhausted> {
        if block.capacity() >= self.sizes.large {
            return Ok(block);
        }
        // Whole: its size is what guarantees a head fits in it.
        let (large, cut) = (self.sizes.large, self.sizes.cut);
        let mut grown = Self::lend(&self.storage, &mut self.large, large, large, cut)?;
        let data = block.data();
        grown.memory.bytes[..data.len()].copy_from_slice(data);
        grown.filled = data.len();
        self.give(block);
        Ok(grown)
    }

    /// Takes a block back, to be lent again.
    ///
    /// Dropped rather than kept when there are already enough of its size: memory parked
    /// here is memory a worker is holding for work it is not doing. A block dropped while
    /// a frame cut from it is still held stays charged until the frame has gone.
    pub fn give(&mut self, block: Block) {
        let (free, most) = if block.capacity() >= self.sizes.large {
            (&mut self.large, self.sizes.kept)
        } else {
            (&mut self.small, self.sizes.parked)
        };
        if free.len() < most {
            free.push(block.memory);
        }
    }

    /// A buffer to stage what is to be written in: empty, with room for `room` bytes.
    ///
    /// Given out empty rather than at its full length, which is the difference between
    /// this and a block. What is staged is appended, so nothing beyond what was put there
    /// can be reached, and appending needs no bytes to have been set first. What it saves
    /// is the growing: a buffer built up from nothing reaches its size by doubling, and
    /// every doubling is another allocation and another copy of what was already in it.
    ///
    /// Lent with the charge for its capacity, which goes wherever it goes: whoever grows it
    /// pays for the growth first ([14 §8](../../../docs/14-downstream-server.md)).
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if a new one is needed and the worker cannot pay for it.
    pub fn take_staging(&mut self, room: usize) -> Result<(Vec<u8>, Charge), Exhausted> {
        match self.staging.pop() {
            Some((buffer, charge)) if buffer.capacity() >= room => Ok((buffer, charge)),
            _ => {
                let charge = self.storage.reserve(room)?;
                Ok((Vec::with_capacity(room), charge))
            }
        }
    }

    /// Takes a staging buffer back with its charge, to be lent again, emptied; or drops
    /// both, when there are already enough.
    pub fn give_staging(&mut self, mut buffer: Vec<u8>, charge: Charge) {
        buffer.clear();
        if self.staging.len() < self.sizes.parked {
            self.staging.push((buffer, charge));
        }
    }

    /// A list for the fields a head adds, empty: one given back before, or a new one, which
    /// holds nothing until something is added.
    pub fn take_edits(&mut self) -> Vec<(HeaderName, HeaderValue)> {
        self.edits.pop().unwrap_or_default()
    }

    /// Takes a list of added fields back, to be lent again, emptied; or drops it, when there
    /// are already enough or it never had any room.
    pub fn give_edits(&mut self, mut edits: Vec<(HeaderName, HeaderValue)>) {
        edits.clear();
        if edits.capacity() > 0 && self.edits.len() < self.sizes.parked {
            self.edits.push(edits);
        }
    }

    /// What everything lent here is paid for against.
    pub fn storage(&self) -> &Rc<Storage> {
        &self.storage
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

    /// Trims down to what a quiet worker keeps, for the worker's once-a-second sweep, and
    /// releases the charges of blocks let go of whose last frame has gone since.
    ///
    /// Under load the blocks trimmed are made again within the second, which is a few
    /// allocations a second rather than one a request; once the load has gone, they are
    /// not.
    pub fn sweep(&mut self) {
        self.trim(self.sizes.kept);
        self.storage.sweep();
    }

    /// A free block with at least `least` of its memory left, if there is one, and a new
    /// one paid for against `storage` if there is not.
    ///
    /// One with that much left is lent as it is, frames cut from it or not: what is left is
    /// its own, and taking the rest back would mean setting all of it to zeros, for every
    /// small answer. One with less is taken back whole if its frames have gone, and passed
    /// over otherwise, left where it is to be taken back once they have. A new block is set
    /// to zeros once, at its full length; one lent as it is, is not set to anything.
    fn lend(
        storage: &Rc<Storage>,
        free: &mut Vec<Memory>,
        size: usize,
        least: usize,
        cut: usize,
    ) -> Result<Block, Exhausted> {
        let found = free.iter_mut().rposition(|memory| {
            let bytes = &mut memory.bytes;
            if bytes.len() >= least {
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
        let memory = match found {
            Some(at) => free.swap_remove(at),
            // Paid for before it is made.
            None => {
                let charge = storage.reserve(size)?;
                Memory {
                    bytes: BytesMut::zeroed(size),
                    charge,
                }
            }
        };
        Ok(Block {
            memory,
            filled: 0,
            taken: 0,
            size,
            cut,
        })
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
            cut: 4,
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let block = blocks.take().unwrap();
        assert!(block.is_empty());
        assert_eq!(block.len(), 0);
        assert_eq!(block.data(), b"");
        assert_eq!(block.capacity(), 16);
    }

    #[test]
    fn what_was_put_in_is_what_comes_out() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        assert_eq!(put(&mut block, b"hello"), 5);
        assert_eq!(block.data(), b"hello");
        block.consume(2);
        assert_eq!(block.data(), b"llo");
        assert_eq!(block.len(), 3);
    }

    #[test]
    fn a_block_used_again_shows_nothing_of_what_it_held() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"secrets");
        blocks.give(block);

        let again = blocks.take().unwrap();
        assert!(again.is_empty(), "a lent block carried something over");
        assert_eq!(again.data(), b"");
        // The same memory came back rather than being made again: that is the point of
        // giving it back at all.
        assert_eq!(blocks.parked(), 0);
    }

    #[test]
    fn a_block_lent_again_is_not_cleared() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"was here before!");
        block.consume(16);
        blocks.give(block);

        let mut again = blocks.take().unwrap();
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        assert_eq!(block.room().len(), 16);
        put(&mut block, &[b'x'; 16]);
        assert_eq!(block.room().len(), 0, "a full block offered room");
        assert_eq!(block.len(), 16);
    }

    #[test]
    fn using_everything_costs_no_move() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"abcd");
        block.consume(4);
        // Dealt with entirely, so it starts again rather than holding a cursor at the end:
        // the whole block is room, and nothing was copied to make it so.
        assert!(block.is_empty());
        assert_eq!(block.room().len(), 16);
    }

    #[test]
    fn a_full_block_makes_room_by_moving_what_is_left() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, &[b'a'; 16]);
        block.consume(10);
        // Full, and ten of it dealt with. The six that are left move down and the ten they
        // were behind become room.
        assert_eq!(block.room().len(), 10);
        assert_eq!(block.data(), &[b'a'; 6]);
    }

    #[test]
    fn a_part_full_block_moves_nothing() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"abcdefgh");
        block.consume(4);
        // Room remains, so the four dealt with are in nobody's way and stay where they are.
        assert_eq!(block.room().len(), 8);
        assert_eq!(block.data(), b"efgh");
    }

    #[test]
    fn growing_carries_what_was_in_it_across() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, &[b'h'; 16]);
        block.consume(4);

        let grown = blocks.grow(block).unwrap();
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let block = blocks.take().unwrap();
        let grown = blocks.grow(block).unwrap();
        let again = blocks.grow(grown).unwrap();
        assert_eq!(again.capacity(), 64);
        assert_eq!(blocks.parked(), 1, "the large block was not kept as it was");
    }

    #[test]
    fn a_grown_block_goes_back_to_its_own_size() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let block = blocks.take().unwrap();
        let grown = blocks.grow(block).unwrap();
        blocks.give(grown);
        // One small, from the growing, and one large.
        assert_eq!(blocks.parked(), 2);
        assert_eq!(
            blocks.take().unwrap().capacity(),
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let (buffer, _charge) = blocks.take_staging(32).unwrap();
        assert!(buffer.is_empty());
        assert!(
            buffer.capacity() >= 32,
            "lent with {} of room",
            buffer.capacity()
        );
    }

    #[test]
    fn a_staging_buffer_comes_back_empty_and_is_the_same_memory() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let (mut buffer, charge) = blocks.take_staging(32).unwrap();
        buffer.extend_from_slice(b"GET / HTTP/1.1\r\n");
        let memory = buffer.as_ptr();
        blocks.give_staging(buffer, charge);

        let (again, _charge) = blocks.take_staging(32).unwrap();
        assert!(again.is_empty(), "a staging buffer carried something over");
        // The same allocation and not a new one of the same size: making it again is
        // exactly the work lending it is there to save.
        assert_eq!(again.as_ptr(), memory, "the buffer was made again");
    }

    #[test]
    fn a_staging_buffer_too_small_for_the_room_is_not_lent() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let (small, charge) = blocks.take_staging(8).unwrap();
        blocks.give_staging(small, charge);
        let (bigger, _charge) = blocks.take_staging(64).unwrap();
        assert!(
            bigger.capacity() >= 64,
            "lent with {} of room",
            bigger.capacity()
        );
    }

    #[test]
    fn only_so_many_staging_buffers_are_kept() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let held: Vec<(Vec<u8>, Charge)> =
            (0..5).map(|_| blocks.take_staging(32).unwrap()).collect();
        for (buffer, charge) in held {
            blocks.give_staging(buffer, charge);
        }
        assert_eq!(blocks.parked(), 2, "a burst left its memory parked");
        blocks.trim(1);
        assert_eq!(blocks.parked(), 1, "trimming left staging buffers behind");
    }

    #[test]
    fn only_so_many_are_kept() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let held: Vec<Block> = (0..5).map(|_| blocks.take().unwrap()).collect();
        for block in held {
            blocks.give(block);
        }
        assert_eq!(blocks.parked(), 2, "a burst left its memory parked");
    }

    /// Grown blocks are five times the size of the others and seldom needed, so fewer of
    /// them are parked: one given back beyond `kept` is freed.
    #[test]
    fn only_so_many_grown_blocks_are_kept() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let grown: Vec<Block> = (0..3)
            .map(|_| {
                let block = blocks.take().unwrap();
                blocks.grow(block).unwrap()
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let small: Vec<Block> = (0..2).map(|_| blocks.take().unwrap()).collect();
        let staging: Vec<(Vec<u8>, Charge)> =
            (0..2).map(|_| blocks.take_staging(8).unwrap()).collect();
        let block = blocks.take().unwrap();
        let grown = blocks.grow(block).unwrap();
        for block in small {
            blocks.give(block);
        }
        for (buffer, charge) in staging {
            blocks.give_staging(buffer, charge);
        }
        blocks.give(grown);
        assert_eq!(blocks.parked(), 5);
        blocks.sweep();
        assert_eq!(blocks.parked(), 3, "a sweep left a burst's memory parked");
    }

    #[test]
    fn trimming_drops_to_what_is_asked() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let held: Vec<Block> = (0..2).map(|_| blocks.take().unwrap()).collect();
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"5\r\nhello\r\n");
        let at = block.data()[3..].as_ptr();
        let frame = block.take_frame(3..8, 10);
        assert_eq!(&frame[..], b"hello");
        assert_eq!(frame.as_ptr(), at, "the frame was copied");
        assert!(block.is_empty());
    }

    #[test]
    fn what_follows_a_frame_stays_in_the_block() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"5\r\nhello\r\n3\r\nab");
        let frame = block.take_frame(3..8, 10);
        assert_eq!(&frame[..], b"hello");
        assert_eq!(block.data(), b"3\r\nab");
    }

    /// **The one thing a shared block must never do.** A frame the engine still holds is
    /// memory the block no longer owns, and nothing read after it may land there.
    #[test]
    fn a_frame_still_held_is_never_written_over() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, &[b'a'; 16]);
        let held = block.take_frame(0..16, 16);
        // Nothing of its own is left, and what it had is held: refilling it cannot take
        // that memory back.
        let mut block = blocks.refill(block).unwrap();
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
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, &[b'a'; 16]);
        let base = block.data().as_ptr();
        drop(block.take_frame(0..10, 10));
        let mut block = blocks.refill(block).unwrap();
        assert_eq!(block.data(), &[b'a'; 6], "unread bytes were lost");
        assert_eq!(block.data().as_ptr(), base, "the memory was not taken back");
        assert_eq!(block.room().len(), 10);
        assert_eq!(blocks.parked(), 0);
    }

    /// Where a frame still holds the memory, the refill is a free block instead, and
    /// what had not been dealt with moves into it.
    #[test]
    fn a_block_whose_frames_are_held_is_refilled_elsewhere_with_what_it_had() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"0123456789abcdef");
        let held = block.take_frame(0..10, 10);
        let mut block = blocks.refill(block).unwrap();
        assert_eq!(block.data(), b"abcdef");
        assert_eq!(block.room().len(), 10);
        assert_eq!(&held[..], b"0123456789");
    }

    /// A block given back while a frame still holds part of it is taken back into use
    /// once the frame has gone, rather than being made again.
    #[test]
    fn a_block_given_back_while_a_frame_holds_it_is_lent_again_once_it_has_gone() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, &[b'a'; 16]);
        let base = block.data().as_ptr();
        let frame = block.take_frame(0..16, 16);
        blocks.give(block);
        drop(frame);
        let mut again = blocks.take().unwrap();
        assert!(again.is_empty());
        assert_eq!(again.room().len(), 16);
        assert_eq!(
            again.room().as_ptr(),
            base.cast_mut(),
            "the block was made again"
        );
    }

    /// A block given back with a frame cut from it, and plenty of its memory left, is lent
    /// again as it is: taking the memory back means setting it to zeros, and doing that
    /// for every small answer cost a request more than the copy it replaced.
    #[test]
    fn a_block_with_room_left_is_lent_again_without_being_cleared() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"was here before!");
        drop(block.take_frame(0..4, 4));
        block.consume(block.len());
        blocks.give(block);
        let mut again = blocks.take().unwrap();
        assert_eq!(
            again.room(),
            &b"here before!"[..],
            "the block was taken back and cleared"
        );
    }

    /// A frame smaller than `cut` is copied instead, and the block keeps its memory: a
    /// small answer then reads into the same few hot bytes at the front of its block as
    /// the one before it did, rather than into memory further along that is cold, and
    /// a small copy costs less than that. Measured, cutting every small frame cost the
    /// proxy 5% at saturation.
    #[test]
    fn a_small_frame_is_copied_and_the_block_keeps_its_memory() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        let base = block.room().as_ptr();
        put(&mut block, b"ab");
        let frame = block.take_frame(0..2, 2);
        assert_eq!(&frame[..], b"ab");
        assert_ne!(frame.as_ptr(), base, "a small frame was cut");
        assert!(block.is_empty());
        assert_eq!(block.room().len(), 16, "the block gave up memory");
        assert_eq!(block.room().as_ptr(), base.cast_mut());
    }

    #[test]
    #[should_panic(expected = "consumed")]
    fn dealing_with_more_than_is_there_is_a_bug() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
        put(&mut block, b"abc");
        block.consume(4);
    }

    #[test]
    #[should_panic(expected = "arrived")]
    fn arriving_past_the_end_is_a_bug() {
        let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
        let mut block = blocks.take().unwrap();
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
            let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
            let mut block = blocks.take().unwrap();
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
                            block = blocks.take().unwrap();
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
                            block = blocks.refill(block).unwrap();
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
            let mut blocks = Blocks::new(sizes(), Storage::new(crate::storage::LIMIT));
            for _ in 0..rounds {
                let held: Vec<Block> = (0..3).map(|_| blocks.take().unwrap()).collect();
                for block in held {
                    blocks.give(block);
                }
                prop_assert!(blocks.parked() <= 2 * sizes().parked);
            }
        }
    }

    /// Blocks paying against an account of `limit` bytes, and the account.
    fn charged(limit: usize) -> (Blocks, Rc<Storage>) {
        let storage = Storage::new(limit);
        (Blocks::new(sizes(), Rc::clone(&storage)), storage)
    }

    /// A block is paid for when it is made, and stays paid for while it is lent and while it
    /// is parked; lent again, it is not paid for twice.
    #[test]
    fn a_block_is_paid_for_once_lent_or_parked() {
        let small = sizes().small;
        let (mut blocks, storage) = charged(1024);
        let block = blocks.take().unwrap();
        assert_eq!(storage.used(), small);
        blocks.give(block);
        assert_eq!(storage.used(), small, "parked");
        let again = blocks.take().unwrap();
        let other = blocks.take().unwrap();
        assert_eq!(storage.used(), 2 * small);
        drop((again, other));
        assert_eq!(storage.used(), 0);
    }

    /// A block the worker cannot pay for is not made, and nothing is charged.
    #[test]
    fn a_block_the_worker_cannot_pay_for_is_not_made() {
        let (mut blocks, storage) = charged(sizes().small - 1);
        assert!(blocks.take().is_err());
        assert_eq!(storage.used(), 0);
    }

    /// Growing pays for the grown block while the small one is still held, because what it
    /// holds has yet to move across.
    #[test]
    fn growing_is_paid_for_while_the_small_block_is_still_held() {
        let Sizes { small, large, .. } = sizes();
        let (mut blocks, storage) = charged(small + large - 1);
        let block = blocks.take().unwrap();
        assert!(blocks.grow(block).is_err());
        assert_eq!(storage.used(), 0, "a refused growth lets go of the block");

        let (mut blocks, storage) = charged(small + large);
        let block = blocks.take().unwrap();
        let grown = blocks.grow(block).unwrap();
        assert_eq!(grown.capacity(), large);
        assert_eq!(storage.used(), small + large, "the small one parked");
    }

    /// Given back to a pool that already keeps enough, a block is dropped and its memory
    /// goes with its charge.
    #[test]
    fn a_block_given_back_to_a_full_pool_is_no_longer_paid_for() {
        let Sizes { small, parked, .. } = sizes();
        let (mut blocks, storage) = charged(1024);
        let lent: Vec<Block> = (0..parked + 1).map(|_| blocks.take().unwrap()).collect();
        for block in lent {
            blocks.give(block);
        }
        assert_eq!(storage.used(), parked * small);
    }

    /// A block let go of while a frame cut from it is still held has not gone: its memory
    /// stays paid for, through sweeps, until the frame has gone too.
    #[test]
    fn a_block_let_go_of_with_a_frame_out_stays_paid_for_until_the_frame_goes() {
        let small = sizes().small;
        let (mut blocks, storage) = charged(1024);
        let mut block = blocks.take().unwrap();
        put(&mut block, &[7; 12]);
        let frame = block.take_frame(0..8, 8);
        drop(block);
        blocks.sweep();
        assert_eq!(storage.used(), small, "released while its frame was held");
        drop(frame);
        blocks.sweep();
        assert_eq!(storage.used(), 0);
    }

    /// The same for a parked block the pool lets go of, by a trim or by a sweep.
    #[test]
    fn a_parked_block_trimmed_with_a_frame_out_stays_paid_for_until_the_frame_goes() {
        let small = sizes().small;
        let (mut blocks, storage) = charged(1024);
        let mut block = blocks.take().unwrap();
        put(&mut block, &[7; 12]);
        let frame = block.take_frame(0..8, 8);
        block.consume(block.len());
        blocks.give(block);
        blocks.trim(0);
        assert_eq!(blocks.parked(), 0);
        blocks.sweep();
        assert_eq!(storage.used(), small, "released while its frame was held");
        drop(frame);
        blocks.sweep();
        assert_eq!(storage.used(), 0);
    }

    /// What a test does to a worker's blocks.
    #[derive(Debug, Clone)]
    enum Using {
        Take,
        /// Fills the block at this position, counted around those lent, with this much.
        Fill(usize, usize),
        /// Cuts a frame of up to this much from the block at this position.
        Frame(usize, usize),
        Grow(usize),
        Refill(usize),
        Give(usize),
        Drop(usize),
        DropFrame(usize),
        Trim(usize),
        Sweep,
    }

    fn using() -> impl Strategy<Value = Using> {
        prop_oneof![
            Just(Using::Take),
            (any::<usize>(), 1usize..80).prop_map(|(at, n)| Using::Fill(at, n)),
            (any::<usize>(), 1usize..40).prop_map(|(at, n)| Using::Frame(at, n)),
            any::<usize>().prop_map(Using::Grow),
            any::<usize>().prop_map(Using::Refill),
            any::<usize>().prop_map(Using::Give),
            any::<usize>().prop_map(Using::Drop),
            any::<usize>().prop_map(Using::DropFrame),
            (0usize..3).prop_map(Using::Trim),
            Just(Using::Sweep),
        ]
    }

    proptest! {
        /// Whatever is lent, filled, cut, grown, given back, dropped and swept, in whatever
        /// order: nothing passes the limit, a refusal leaves nothing charged that is not
        /// held, and once everything is let go of and swept the account is empty — no charge
        /// lost, none released twice.
        #[test]
        fn the_blocks_account_for_everything_they_make(
            limit in 0usize..=512,
            steps in proptest::collection::vec(using(), 0..80),
        ) {
            let (mut blocks, storage) = charged(limit);
            let mut lent: Vec<Block> = Vec::new();
            let mut frames: Vec<Bytes> = Vec::new();
            for step in steps {
                let pick = |at: usize, len: usize| (len > 0).then(|| at % len);
                match step {
                    Using::Take => {
                        if let Ok(block) = blocks.take() {
                            lent.push(block);
                        }
                    }
                    Using::Fill(at, n) => {
                        if let Some(at) = pick(at, lent.len()) {
                            put(&mut lent[at], &vec![1; n]);
                        }
                    }
                    Using::Frame(at, n) => {
                        if let Some(at) = pick(at, lent.len()) {
                            let count = n.min(lent[at].len());
                            frames.push(lent[at].take_frame(0..count, count));
                        }
                    }
                    Using::Grow(at) => {
                        if let Some(at) = pick(at, lent.len()) {
                            let block = lent.swap_remove(at);
                            if let Ok(grown) = blocks.grow(block) {
                                lent.push(grown);
                            }
                        }
                    }
                    Using::Refill(at) => {
                        if let Some(at) = pick(at, lent.len()) {
                            let block = lent.swap_remove(at);
                            if let Ok(block) = blocks.refill(block) {
                                lent.push(block);
                            }
                        }
                    }
                    Using::Give(at) => {
                        if let Some(at) = pick(at, lent.len()) {
                            let mut block = lent.swap_remove(at);
                            block.consume(block.len());
                            blocks.give(block);
                        }
                    }
                    Using::Drop(at) => {
                        if let Some(at) = pick(at, lent.len()) {
                            drop(lent.swap_remove(at));
                        }
                    }
                    Using::DropFrame(at) => {
                        if let Some(at) = pick(at, frames.len()) {
                            drop(frames.swap_remove(at));
                        }
                    }
                    Using::Trim(keep) => blocks.trim(keep),
                    Using::Sweep => blocks.sweep(),
                }
                prop_assert!(storage.used() <= limit, "{} of {limit}", storage.used());
                let held: usize = lent.iter().map(Block::capacity).sum();
                prop_assert!(storage.used() >= held, "{} for {held} lent", storage.used());
            }
            drop(lent);
            drop(frames);
            drop(blocks);
            storage.sweep();
            prop_assert_eq!(storage.used(), 0);
            prop_assert_eq!(storage.outlived(), 0);
        }
    }
}
