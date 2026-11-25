//! What a thread off the request path keeps one of for every key, or for every key let go
//! of, held in chunks of a fixed size (06 §4): a binary min-heap and a list.
//!
//! A `Vec` or a `BinaryHeap` holds its items in one block that doubles as it fills. Grown
//! inside glibc's arena, a block is copied under the arena's lock, and a large one freed trims
//! the arena under the same lock: a worker freeing anything the thread made waits meanwhile,
//! some 6 ms for a block of 16 MB. A chunk here is made once with room for `CHUNK` items and
//! never grows; one is freed only when the items have shrunk past it and another chunk, so
//! that items rising and falling at a boundary make and free none.
//!
//! Every index taken below is under the length, so within the chunks.

#[cfg(test)]
mod tests;

use edgerush_memory::{allocation, vec};

/// Items of 16 bytes, an eviction time's size, to a 64 KiB chunk.
const HEAP_CHUNK: usize = 4_096;

/// Items of 8 bytes, a handle's size, to a 64 KiB chunk.
const LIST_CHUNK: usize = 8_192;

/// Items in chunks of `CHUNK`: all full but the last that holds any, then at most one empty.
#[derive(Debug)]
struct Chunks<T, const CHUNK: usize> {
    chunks: Vec<Vec<T>>,
    len: usize,
}

impl<T, const CHUNK: usize> Chunks<T, CHUNK> {
    const fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }

    fn push(&mut self, item: T) {
        let chunk = self.len / CHUNK;
        if chunk == self.chunks.len() {
            self.chunks.push(Vec::with_capacity(CHUNK));
        }
        self.chunks[chunk].push(item);
        self.len += 1;
    }

    /// Takes the last item.
    fn pop(&mut self) -> Option<T> {
        let last = self.len.checked_sub(1)?;
        let item = self.chunks[last / CHUNK].pop();
        self.len = last;
        self.shed();
        item
    }

    /// Keeps only the items `keep` says to, moved up in their order into the places of those
    /// dropped, in one pass over them all.
    fn keep(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut kept = 0;
        for i in 0..self.len {
            if keep(self.at(i)) {
                self.swap(kept, i);
                kept += 1;
            }
        }
        while self.len > kept {
            self.len -= 1;
            self.chunks[self.len / CHUNK].pop();
        }
        self.shed();
    }

    /// Frees the chunks past the last holding any items and one spare.
    fn shed(&mut self) {
        self.chunks.truncate(self.len.div_ceil(CHUNK) + 1);
    }

    fn at(&self, i: usize) -> &T {
        &self.chunks[i / CHUNK][i % CHUNK]
    }

    fn swap(&mut self, a: usize, b: usize) {
        let ((low, i), (high, j)) = if a <= b {
            ((a / CHUNK, a % CHUNK), (b / CHUNK, b % CHUNK))
        } else {
            ((b / CHUNK, b % CHUNK), (a / CHUNK, a % CHUNK))
        };
        if low == high {
            self.chunks[low].swap(i, j);
        } else {
            let (before, from) = self.chunks.split_at_mut(high);
            std::mem::swap(&mut before[low][i], &mut from[0][j]);
        }
    }
}

/// A min-heap: [`Self::pop`] takes the least item, any one of those equal.
pub(crate) struct MinHeap<T, const CHUNK: usize = HEAP_CHUNK> {
    items: Chunks<T, CHUNK>,
}

impl<T: Ord, const CHUNK: usize> MinHeap<T, CHUNK> {
    pub(crate) const fn new() -> Self {
        Self {
            items: Chunks::new(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.items.len
    }

    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.items.len == 0
    }

    pub(crate) fn push(&mut self, item: T) {
        self.items.push(item);
        self.sift_up(self.items.len - 1);
    }

    /// The least item.
    pub(crate) fn peek(&self) -> Option<&T> {
        (self.items.len > 0).then(|| self.items.at(0))
    }

    /// Takes the least item.
    pub(crate) fn pop(&mut self) -> Option<T> {
        let last = self.items.len.checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        self.sift_down(0);
        item
    }

    /// Keeps only the items `keep` says to, in one pass over them all.
    pub(crate) fn retain(&mut self, keep: impl FnMut(&T) -> bool) {
        self.items.keep(keep);
        for i in (0..self.items.len / 2).rev() {
            self.sift_down(i);
        }
    }

    fn less(&self, a: usize, b: usize) -> bool {
        self.items.at(a) < self.items.at(b)
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if !self.less(i, parent) {
                break;
            }
            self.items.swap(i, parent);
            i = parent;
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let left = 2 * i + 1;
            if left >= self.items.len {
                break;
            }
            let right = left + 1;
            let least = if right < self.items.len && self.less(right, left) {
                right
            } else {
                left
            };
            if !self.less(least, i) {
                break;
            }
            self.items.swap(i, least);
            i = least;
        }
    }
}

/// A list, its items in no order that matters: [`Self::retain`] moves those it keeps up into
/// the places of those it drops.
#[derive(Debug)]
pub(crate) struct List<T, const CHUNK: usize = LIST_CHUNK> {
    items: Chunks<T, CHUNK>,
}

impl<T, const CHUNK: usize> List<T, CHUNK> {
    pub(crate) const fn new() -> Self {
        Self {
            items: Chunks::new(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.items.len
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.items.len == 0
    }

    pub(crate) fn push(&mut self, item: T) {
        self.items.push(item);
    }

    /// Keeps only the items `keep` says to, in one pass over them all; those it does not keep
    /// are dropped then. Emptied, it keeps no chunk: what it holds is counted against a cap,
    /// and a spare chunk would count there with nothing in it.
    pub(crate) fn retain(&mut self, keep: impl FnMut(&T) -> bool) {
        self.items.keep(keep);
        if self.items.len == 0 {
            self.items.chunks = Vec::new();
        }
    }

    /// What it takes in memory, as glibc allocates it: its chunks, each with its room, and
    /// the list of them.
    pub(crate) fn bytes(&self) -> u64 {
        let chunk = allocation(CHUNK.saturating_mul(size_of::<T>()));
        let chunks = u64::try_from(self.items.chunks.len()).unwrap_or(u64::MAX);
        chunks
            .saturating_mul(chunk)
            .saturating_add(vec::<Vec<T>>(self.items.chunks.capacity()))
    }
}
