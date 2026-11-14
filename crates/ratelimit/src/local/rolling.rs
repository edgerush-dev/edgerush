//! A rolling window in local mode (06 §3, #204): at most `limit` requests in any span of its
//! window, each admission counted in a slot of `slots` a window, its time rounded down to the
//! slot's start — room comes back up to a slot early, never late, as the synchronous store
//! counts one.
//!
//! A key's 128-bit word holds the admissions counted in the window in its low half, below
//! the closed bit every key has, and in its high half `through`, the first slot not yet taken
//! off them. Beside it, a ring of one count a slot, slot *x* at *x* mod `slots`, tagged with
//! its lap (*x* / `slots`) so that a slot's count is never read as another's, and a hint: the
//! oldest slot a refusal found holding a count. All of it is the key's own: a rolling key's
//! terms are its own, not its limit's, so that no other key grows by it.
//!
//! A decision takes off the slots that left the window first, one compare-and-swap moving
//! `through` on, and whoever moves it empties their entries; then, with room, counts the
//! request in its slot and charges the total, a compare-and-swap that checks the room again.
//! The slot first, the total second: a worker held up between the two for a whole window
//! has its count taken off a total that never had it, and the key admits one too many — never
//! one too few, which the other order would leave.

use super::sync::{AtomicU32, AtomicU64, AtomicU128, Ordering};
use super::{CLOSED, Look, Rolling, Seen};
use crate::word::{local, local_parts};

#[cfg(test)]
mod tests;

/// The most slots a window has: a key holds a count for each.
pub const MOST_SLOTS: u64 = 1_000;
/// Limits below this count in entries of 4 bytes, whose 20-bit counts a slot never fills:
/// a slot counts no more than the limit and what racing workers admit over it.
const NARROW_BELOW: u64 = 1 << 19;

/// Whether a ring under `limit` counts in entries of 4 bytes, rather than 8.
const fn narrow(limit: u64) -> bool {
    limit < NARROW_BELOW
}

/// A rolling window's state beside its key's word.
#[derive(Debug)]
pub(super) struct Ring {
    limit: u64,
    /// A slot's length, in nanoseconds.
    len: u64,
    slots: u64,
    /// 1 + the oldest slot a refusal found holding a count, or 0: no slot older holds one
    /// while it is in the window, an admission counting in its own, the latest, slot.
    hint: AtomicU64,
    cells: Cells,
}

impl Ring {
    pub(super) fn new(rolling: Rolling) -> Self {
        let slots = rolling.slots.get().clamp(1, MOST_SLOTS);
        let limit = rolling.limit.get();
        Self {
            limit,
            len: (rolling.window.get() / slots).max(1),
            slots,
            hint: AtomicU64::new(0),
            cells: if narrow(limit) {
                Cells::Narrow((0..slots).map(|_| AtomicU32::new(0)).collect())
            } else {
                Cells::Wide((0..slots).map(|_| AtomicU64::new(0)).collect())
            },
        }
    }

    /// The bytes a key under `rolling` keeps beside the others': its own terms and ring.
    pub(super) fn bytes(rolling: Rolling) -> u64 {
        let slots = rolling.slots.get().clamp(1, MOST_SLOTS);
        let entry = if narrow(rolling.limit.get()) { 4 } else { 8 };
        // An `Arc`'s two counts beside the terms.
        (2 * size_of::<usize>() + size_of::<super::Terms>() + size_of::<Self>()) as u64
            + slots * entry
    }

    /// The window as its slots cover it.
    pub(super) const fn window(&self) -> u64 {
        self.len * self.slots
    }

    /// A new key's word at `now`: nothing counted, nothing to take off.
    pub(super) fn word_at(&self, now: u64) -> u128 {
        local(0, self.oldest(self.slot(now)))
    }

    fn slot(&self, now: u64) -> u64 {
        now / self.len
    }

    /// The oldest slot the window holds when `slot` is the latest.
    fn oldest(&self, slot: u64) -> u64 {
        slot.saturating_sub(self.slots - 1)
    }

    /// The total with the slots from `through` to below `oldest` taken off: none of them
    /// left once the window has moved on by all its slots.
    fn taken_off(&self, total: u64, through: u64, oldest: u64) -> u64 {
        if oldest - through >= self.slots {
            return 0;
        }
        (through..oldest).fold(total, |total, x| {
            total - self.cells.counted(x, self.slots).min(total)
        })
    }

    /// Empties the entries of the slots from `through` to below `oldest`, which a total has
    /// just been taken off: each only while it holds what it held, so that a count made since,
    /// in a later lap that took the entry over, stays. Slots more than a ring before `oldest`
    /// left their entries, at most a ring's, from `through` on.
    fn empty(&self, through: u64, oldest: u64) {
        let end = oldest.min(through + self.slots);
        for x in through..end {
            self.cells.clear(x, self.slots);
        }
    }

    /// Takes off what left the window by `now`'s slot and reads the total, closed or not:
    /// the word as it then stands, and the total in it.
    fn moved_on(&self, state: &AtomicU128, slot: u64) -> Option<(u128, u64)> {
        let oldest = self.oldest(slot);
        loop {
            let seen = state.load(Ordering::Acquire);
            let (low, through) = local_parts(seen);
            if low & CLOSED != 0 {
                return None;
            }
            if through >= oldest {
                return Some((seen, low));
            }
            let total = self.taken_off(low, through, oldest);
            let moved = local(total, oldest);
            if state
                .compare_exchange(seen, moved, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.empty(through, oldest);
                return Some((moved, total));
            }
        }
    }

    /// Nanoseconds from `now` until a request fits, `total` counted in the window by `slot`:
    /// until enough of the oldest slots holding counts have left it.
    fn wait(&self, total: u64, slot: u64, now: u64) -> u64 {
        let oldest = self.oldest(slot);
        let need = (total + 1).saturating_sub(self.limit).max(1);
        let hinted = self.hint.load(Ordering::Relaxed);
        let start = if hinted > oldest && hinted - 1 <= slot {
            hinted - 1
        } else {
            oldest
        };
        let mut counted = 0;
        let mut first = true;
        for x in start..=slot {
            let count = self.cells.counted(x, self.slots);
            if count == 0 {
                continue;
            }
            if first {
                first = false;
                if hinted != x + 1 {
                    self.hint.store(x + 1, Ordering::Relaxed);
                }
            }
            counted += count;
            if counted >= need {
                return (x + self.slots) * self.len - now;
            }
        }
        // Counts not yet in their slots, of requests between their two writes: a slot's wait.
        self.len - now % self.len
    }

    /// Whether a request at `now` fits, nothing charged.
    pub(super) fn look(&self, state: &AtomicU128, now: u64) -> Look {
        let slot = self.slot(now);
        let oldest = self.oldest(slot);
        let seen = state.load(Ordering::Acquire);
        let (low, through) = local_parts(seen);
        if low & CLOSED != 0 {
            return Look::Closed;
        }
        let total = if through < oldest {
            self.taken_off(low, through, oldest)
        } else {
            low
        };
        if total >= self.limit {
            return Look::Full {
                wait: self.wait(total, slot, now),
            };
        }
        Look::Fits {
            seen: Seen::of(seen),
            from: total,
        }
    }

    /// One request at `now`, admitted if it fits.
    pub(super) fn take(&self, state: &AtomicU128, now: u64) -> Look {
        self.charged(state, now, true)
    }

    /// One request at `now` charged, whether it fits or not: false if the key was closed.
    pub(super) fn charge(&self, state: &AtomicU128, now: u64) -> bool {
        !matches!(self.charged(state, now, false), Look::Closed)
    }

    fn charged(&self, state: &AtomicU128, now: u64, checked: bool) -> Look {
        let slot = self.slot(now);
        let Some((mut seen, total)) = self.moved_on(state, slot) else {
            return Look::Closed;
        };
        let uncharged = |seen: u128, low: u64| Look::Fits {
            seen: Seen::of(seen),
            from: low,
        };
        let (_, through) = local_parts(seen);
        if through > slot {
            // Its slot has left the window, a clock a window behind: admitted, uncharged.
            return uncharged(seen, total);
        }
        if checked && total >= self.limit {
            return Look::Full {
                wait: self.wait(total, slot, now),
            };
        }
        if !self.cells.add(slot, self.slots) {
            // Its entry holds a later lap's count: its slot has left the window since.
            return uncharged(seen, total);
        }
        loop {
            let (low, through) = local_parts(seen);
            if low & CLOSED != 0 {
                self.cells.sub(slot, self.slots);
                return Look::Closed;
            }
            if through > slot {
                // Its slot left the window while it waited: admitted, uncharged.
                self.cells.sub(slot, self.slots);
                return uncharged(seen, low);
            }
            if checked && low >= self.limit {
                self.cells.sub(slot, self.slots);
                return Look::Full {
                    wait: self.wait(low, slot, now),
                };
            }
            match state.compare_exchange(
                seen,
                local(low + 1, through),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Look::Fits {
                        seen: Seen::of(seen),
                        from: low,
                    };
                }
                Err(now_seen) => seen = now_seen,
            }
        }
    }

    /// Nanoseconds from `now` until it counts none: until the newest slot holding a count
    /// leaves the window.
    pub(super) fn full_in(&self, state: &AtomicU128, now: u64) -> u64 {
        let slot = self.slot(now);
        let oldest = self.oldest(slot);
        let (_, through) = local_parts(state.load(Ordering::Acquire));
        let newest = slot.min(through + self.slots - 1);
        (oldest.max(through)..=newest)
            .rev()
            .find(|x| self.cells.counted(*x, self.slots) > 0)
            .map_or(0, |x| (x + self.slots) * self.len - now)
    }

    /// At `now`, the total its word holds once what left the window is taken off, and what
    /// its slots in the window count: the same, but for requests between their two writes.
    #[cfg(test)]
    pub(super) fn standing(&self, state: &AtomicU128, now: u64) -> (u64, u64) {
        let slot = self.slot(now);
        let oldest = self.oldest(slot);
        let (low, through) = local_parts(state.load(Ordering::Acquire));
        let low = low & !CLOSED;
        let total = if through < oldest {
            self.taken_off(low, through, oldest)
        } else {
            low
        };
        let counted = (oldest.max(through)..=slot)
            .map(|x| self.cells.counted(x, self.slots))
            .sum();
        (total, counted)
    }

    /// Closes it if, at `now`, it counts none and nothing used it for `quiet`: true if it did.
    pub(super) fn close_if_forgettable(&self, state: &AtomicU128, now: u64, quiet: u64) -> bool {
        let seen = state.load(Ordering::Acquire);
        let (low, through) = local_parts(seen);
        if low & CLOSED != 0 {
            return true;
        }
        // The latest slot a decision took off to is a ring after `through`; its end is when
        // the key was last surely used.
        let used = (through + self.slots) * self.len;
        if now.saturating_sub(used) < quiet {
            return false;
        }
        let oldest = self.oldest(self.slot(now));
        let counting = if through < oldest {
            self.taken_off(low, through, oldest)
        } else {
            low
        };
        if counting > 0 {
            return false;
        }
        // Lost to a request just made, the key stays: it is in use.
        state
            .compare_exchange(
                seen,
                seen | u128::from(CLOSED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

/// A ring's entries: slot *x*'s count at *x* mod the slots, tagged with its lap.
#[derive(Debug)]
enum Cells {
    /// 12-bit laps, 20-bit counts.
    Narrow(Box<[AtomicU32]>),
    /// 24-bit laps, 40-bit counts.
    Wide(Box<[AtomicU64]>),
}

/// One entry, as a number of `BITS` bits: its lap above its count.
trait Entry {
    const BITS: u32;
    const LAP_BITS: u32;
    fn get(&self) -> u64;
    fn swap_if(&self, seen: u64, next: u64) -> Result<(), u64>;

    fn count_bits() -> u32 {
        Self::BITS - Self::LAP_BITS
    }

    /// Slot `x`'s lap, as the entry tags it.
    fn lap(x: u64, slots: u64) -> u64 {
        (x / slots) & ((1 << Self::LAP_BITS) - 1)
    }

    /// What slot `x` counted, if the entry holds its lap.
    fn counted(&self, x: u64, slots: u64) -> u64 {
        let entry = self.get();
        if entry >> Self::count_bits() == Self::lap(x, slots) {
            entry & ((1 << Self::count_bits()) - 1)
        } else {
            0
        }
    }

    /// One more for slot `x`, the entry taken over if it holds an earlier lap: false, and
    /// nothing written, if it holds a later one, whose count is in the window and `x`'s not —
    /// a clock a window behind, which taken over, the total would hold a count no slot does.
    fn add(&self, x: u64, slots: u64) -> bool {
        let lap = Self::lap(x, slots);
        let half = 1 << (Self::LAP_BITS - 1);
        let mut seen = self.get();
        loop {
            let held = seen >> Self::count_bits();
            let next = if held == lap {
                seen + 1
            } else if seen != 0 && (held.wrapping_sub(lap) & ((1 << Self::LAP_BITS) - 1)) < half {
                return false;
            } else {
                lap << Self::count_bits() | 1
            };
            match self.swap_if(seen, next) {
                Ok(()) => return true,
                Err(now) => seen = now,
            }
        }
    }

    /// One fewer for slot `x`, if the entry still holds its lap and a count.
    fn sub(&self, x: u64, slots: u64) {
        let lap = Self::lap(x, slots);
        let mut seen = self.get();
        while seen >> Self::count_bits() == lap && seen & ((1 << Self::count_bits()) - 1) > 0 {
            match self.swap_if(seen, seen - 1) {
                Ok(()) => return,
                Err(now) => seen = now,
            }
        }
    }

    /// Emptied, if it holds slot `x`'s count and nothing has changed it since it was read.
    fn clear(&self, x: u64, slots: u64) {
        let seen = self.get();
        if seen != 0 && seen >> Self::count_bits() == Self::lap(x, slots) {
            // Lost to a request counted since, the entry keeps it.
            let _ = self.swap_if(seen, 0);
        }
    }
}

impl Entry for AtomicU32 {
    const BITS: u32 = 32;
    const LAP_BITS: u32 = 12;

    fn get(&self) -> u64 {
        u64::from(self.load(Ordering::Acquire))
    }

    fn swap_if(&self, seen: u64, next: u64) -> Result<(), u64> {
        // Both are entries of this width: below 2^32.
        let (seen, next) = (seen as u32, next as u32);
        self.compare_exchange(seen, next, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(u64::from)
    }
}

impl Entry for AtomicU64 {
    const BITS: u32 = 64;
    const LAP_BITS: u32 = 24;

    fn get(&self) -> u64 {
        self.load(Ordering::Acquire)
    }

    fn swap_if(&self, seen: u64, next: u64) -> Result<(), u64> {
        self.compare_exchange(seen, next, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
    }
}

impl Cells {
    fn counted(&self, x: u64, slots: u64) -> u64 {
        // The index is below the slots, the entries' length.
        let at = (x % slots) as usize;
        match self {
            Self::Narrow(cells) => cells[at].counted(x, slots),
            Self::Wide(cells) => cells[at].counted(x, slots),
        }
    }

    fn add(&self, x: u64, slots: u64) -> bool {
        let at = (x % slots) as usize;
        match self {
            Self::Narrow(cells) => cells[at].add(x, slots),
            Self::Wide(cells) => cells[at].add(x, slots),
        }
    }

    fn sub(&self, x: u64, slots: u64) {
        let at = (x % slots) as usize;
        match self {
            Self::Narrow(cells) => cells[at].sub(x, slots),
            Self::Wide(cells) => cells[at].sub(x, slots),
        }
    }

    fn clear(&self, x: u64, slots: u64) {
        let at = (x % slots) as usize;
        match self {
            Self::Narrow(cells) => cells[at].clear(x, slots),
            Self::Wide(cells) => cells[at].clear(x, slots),
        }
    }
}
