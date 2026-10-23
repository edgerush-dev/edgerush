//! A binary min-heap held in chunks of a fixed size, for what the participant's thread keeps
//! one of for every key (06 §4).
//!
//! A `BinaryHeap` holds its items in one block that doubles as it fills. Grown inside
//! glibc's arena, a block is copied under the arena's lock, and a large one freed trims the
//! arena under the same lock: a worker freeing anything the thread made waits meanwhile,
//! some 6 ms for a block of 16 MB. A chunk here is made once with room
//! for `CHUNK` items and never grows; one is freed only when the heap has shrunk past it
//! and another chunk, so that one heap rising and falling at a boundary makes and frees none.
//!
//! Every index taken below is under the heap's length, so within its chunks.

#[cfg(test)]
mod tests;

/// Items of 16 bytes, an eviction time's size, to a 64 KiB chunk.
const DEFAULT_CHUNK: usize = 4_096;

/// A min-heap: [`Self::pop`] takes the least item, any one of those equal.
pub(super) struct MinHeap<T, const CHUNK: usize = DEFAULT_CHUNK> {
    /// All full but the last that holds any items, then at most one empty.
    chunks: Vec<Vec<T>>,
    len: usize,
}

impl<T: Ord, const CHUNK: usize> MinHeap<T, CHUNK> {
    pub(super) const fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }

    #[cfg(test)]
    pub(super) const fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(super) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn push(&mut self, item: T) {
        let chunk = self.len / CHUNK;
        if chunk == self.chunks.len() {
            self.chunks.push(Vec::with_capacity(CHUNK));
        }
        self.chunks[chunk].push(item);
        self.len += 1;
        self.sift_up(self.len - 1);
    }

    /// The least item.
    pub(super) fn peek(&self) -> Option<&T> {
        (self.len > 0).then(|| self.at(0))
    }

    /// Takes the least item.
    pub(super) fn pop(&mut self) -> Option<T> {
        let last = self.len.checked_sub(1)?;
        self.swap(0, last);
        let item = self.chunks[last / CHUNK].pop();
        self.len = last;
        self.chunks.truncate(self.len.div_ceil(CHUNK) + 1);
        self.sift_down(0);
        item
    }

    /// Keeps only the items `keep` says to, in one pass over them all.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
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
        self.chunks.truncate(self.len.div_ceil(CHUNK) + 1);
        for i in (0..self.len / 2).rev() {
            self.sift_down(i);
        }
    }

    fn at(&self, i: usize) -> &T {
        &self.chunks[i / CHUNK][i % CHUNK]
    }

    fn less(&self, a: usize, b: usize) -> bool {
        self.at(a) < self.at(b)
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

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if !self.less(i, parent) {
                break;
            }
            self.swap(i, parent);
            i = parent;
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let left = 2 * i + 1;
            if left >= self.len {
                break;
            }
            let right = left + 1;
            let least = if right < self.len && self.less(right, left) {
                right
            } else {
                left
            };
            if !self.less(least, i) {
                break;
            }
            self.swap(i, least);
            i = least;
        }
    }
}
