//! Where eviction past a cap stops, found in passes over the keys rather than from a list of
//! them all sorted (06 §8): the first finds the span of the keys' places in the order keys go
//! in ([`Places`]); the second counts each key's bytes in a fixed table of ranges, equal steps
//! over that span ([`Count`]); the third evicts the keys before the range the bytes needed end
//! in, and of that range enough to make up the rest ([`Stop`]). What it keeps beside the keys
//! is the table alone, 8 KB however many keys there are.
//!
//! With many keys the first two passes read a sample, the keys of one shard in [`STRIDE`],
//! each counted as that many: keys go to shards by their hash, so a sample is like the
//! whole, and the passes that find where to stop cost a sixteenth of the one that evicts
//! (#602). Measured on 1.6 million keys, each pass over all of them took a third of a second.
//!
//! A key's place is a number, a time in nanoseconds. Keys go in order between ranges; within
//! the range the bytes end in, evenly over the pass, so that the keys taken from it are spread
//! over every shard and part of the tables they are in. Taken in the order the pass meets
//! them, they would come from the first shards alone, emptying those, whose tables never
//! shrink, while the rest grow: keys under one limit made close together fall in one range
//! when ranges are not fitted to the span, and measured so, a process's tables grew from 0.39
//! to 0.54 of its cap (#602).

#[cfg(test)]
mod tests;

use crate::HANDLES_PRUNED;

/// Ranges in all.
const RANGES: usize = 1_024;

/// With [`SAMPLED_FROM`] keys or more, the first two passes read one shard in this many.
pub(crate) const STRIDE: usize = 16;

/// Keys from which the first two passes read a sample: 4,096 keys or more sampled, four to a
/// range, fewer than this read whole.
pub(crate) const SAMPLED_FROM: usize = 65_536;

/// The shards the first two passes read, one in how many, with `keys` in all.
pub(crate) const fn stride(keys: usize) -> usize {
    if keys >= SAMPLED_FROM { STRIDE } else { 1 }
}

/// Which keys go first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum First {
    /// The least first: local mode's time until a key is full.
    Least,
    /// The greatest first: the participant's time since a key was last asked for.
    Greatest,
}

impl First {
    /// The `nth` range keys go from, counting from 0.
    const fn nth(self, nth: usize) -> usize {
        match self {
            Self::Least => nth,
            Self::Greatest => RANGES - 1 - nth,
        }
    }
}

/// The first pass: the least and the most of the keys' places.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Places {
    least: u64,
    most: u64,
    any: bool,
}

impl Places {
    /// A key at `place`.
    pub(crate) fn add(&mut self, place: u64) {
        if self.any {
            self.least = self.least.min(place);
            self.most = self.most.max(place);
        } else {
            (self.least, self.most, self.any) = (place, place, true);
        }
    }
}

/// The ranges over a span: equal steps from its least, the last taking the rest.
#[derive(Clone, Copy, Debug)]
struct Ranges {
    least: u64,
    step: u64,
}

impl Ranges {
    const fn over(span: Places) -> Self {
        Self {
            least: span.least,
            // So that the most falls in the last range or before it.
            step: (span.most - span.least) / RANGES as u64 + 1,
        }
    }

    /// The range of a key at `place`: one before the span in the first, one past it in the
    /// last, as a key's place may move between passes.
    const fn of(self, place: u64) -> usize {
        let at = place.saturating_sub(self.least) / self.step;
        if at < RANGES as u64 {
            // Below `RANGES`, so it fits.
            at as usize
        } else {
            RANGES - 1
        }
    }
}

/// The second pass: the bytes of the keys in each range.
pub(crate) struct Count {
    first: First,
    ranges: Ranges,
    /// What each key counted stands for: one in how many read.
    stride: u64,
    bytes: [u64; RANGES],
}

/// The third pass: whether each key goes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stop {
    first: First,
    ranges: Ranges,
    /// Every key goes: fewer were counted than needed.
    all: bool,
    /// The range the bytes needed end in.
    range: usize,
    /// The bytes to go from that range, of those it holds as counted.
    wanted: u64,
    share: u64,
    /// The bytes met in that range so far, and taken from it.
    seen: u64,
    taken: u64,
    /// The bytes still to go from the ranges before it, as counted.
    before: u64,
}

impl Count {
    /// None counted yet, over `span`, the keys `first` to go first, each read for `stride`
    /// keys: one shard read in that many.
    pub(crate) const fn new(first: First, span: Places, stride: usize) -> Self {
        Self {
            first,
            ranges: Ranges::over(span),
            stride: stride as u64,
            bytes: [0; RANGES],
        }
    }

    /// A key at `place`, of `bytes`.
    pub(crate) fn add(&mut self, place: u64, bytes: u64) {
        let at = self.ranges.of(place);
        self.bytes[at] = self.bytes[at].saturating_add(bytes.saturating_mul(self.stride));
    }

    /// Where to stop to free `needed` bytes: every key before the range they end in goes, and
    /// of it keys until the rest is made up. With fewer counted than needed, every key goes.
    pub(crate) fn stop(&self, needed: u64) -> Stop {
        let mut stop = Stop {
            first: self.first,
            ranges: self.ranges,
            all: true,
            range: 0,
            wanted: 0,
            share: 0,
            seen: 0,
            taken: 0,
            before: u64::MAX,
        };
        let mut left = needed;
        for nth in 0..RANGES {
            let at = self.first.nth(nth);
            if left <= self.bytes[at] {
                stop.all = false;
                stop.range = at;
                stop.wanted = left;
                stop.share = self.bytes[at];
                stop.before = needed - left;
                return stop;
            }
            left -= self.bytes[at];
        }
        stop
    }
}

impl Stop {
    /// Whether a key at `place`, of `bytes`, goes.
    pub(crate) fn takes(&mut self, place: u64, bytes: u64) -> bool {
        if self.all {
            return true;
        }
        let at = self.ranges.of(place);
        let before = match self.first {
            First::Least => at < self.range,
            First::Greatest => at > self.range,
        };
        if before {
            self.before = self.before.saturating_sub(bytes);
            return true;
        }
        if at != self.range || self.taken >= self.wanted {
            return false;
        }
        // Kept up with the share of the range met so far: never behind it, so that by the
        // range's last key what was wanted has gone, and so never more than a key past it.
        self.seen = self.seen.saturating_add(bytes);
        let pace = u128::from(self.wanted) * u128::from(self.seen);
        if u128::from(self.taken) * u128::from(self.share.max(1)) >= pace {
            return false;
        }
        self.taken = self.taken.saturating_add(bytes);
        true
    }

    /// Whether every key that goes has gone, as the passes before counted them: the rest of
    /// this one takes none, unless a key has moved to an earlier range since.
    pub(crate) const fn done(&self) -> bool {
        !self.all && self.taken >= self.wanted && self.before == 0
    }
}

/// The bytes of keys made lately, which eviction leaves room for: what workers may go on
/// holding of keys let go of is at most what was made over [`HANDLES_PRUNED`]. At each look
/// it is told the bytes made all told, and gives the more of those made over the last whole
/// such span and those made since, so that a burst counts at once and a quiet spell after
/// one only once its span has passed.
#[derive(Debug, Default)]
pub(crate) struct Lately {
    /// When the span now counting began, and the bytes made all told then; none before the
    /// first look.
    start: Option<(u64, u64)>,
    /// The bytes made over the last whole span.
    last: u64,
}

impl Lately {
    /// At `now`, with `made` bytes made all told: the bytes made lately.
    pub(crate) fn at(&mut self, now: u64, made: u64) -> u64 {
        let Some((start, then)) = self.start else {
            self.start = Some((now, made));
            return 0;
        };
        let since = made.saturating_sub(then);
        let span = now.saturating_sub(start);
        if span < HANDLES_PRUNED {
            return self.last.max(since);
        }
        // Looks further apart than a span: what was made scaled to one span.
        let scaled = u128::from(since) * u128::from(HANDLES_PRUNED) / u128::from(span);
        self.last = u64::try_from(scaled).unwrap_or(u64::MAX);
        self.start = Some((now, made));
        self.last
    }
}
