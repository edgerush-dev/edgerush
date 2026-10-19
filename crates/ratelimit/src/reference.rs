//! The admission models of docs/06 §3 as slow, obvious code: the specification that what
//! is built for the request path will be compared with.
//!
//! Each model limits one key. A request costs one, and a refused request is charged
//! nothing (06 §2). Times are nanoseconds from any fixed origin and must not go backwards:
//! a time earlier than the last one seen is taken as the last one, as a monotonic clock
//! read on several workers can make it. A rate or a window is `limit` requests per
//! `window` nanoseconds; every model starts with nothing counted against the key.
//!
//! Every model plugs in behind [`Model`]. Nothing here minds its cost: the rolling window
//! keeps every admission it can still see, states are cloned to be looked at, and
//! arithmetic is done in `u128` so that it is exact.

use std::collections::VecDeque;
use std::num::NonZeroU64;

/// One key's admission state under one model: the interface every model plugs in behind.
///
/// Admitting and charging are apart so that several rules can be asked before any is
/// charged, all or nothing (06 §2). A charge can be released, for a backend attempt that
/// was reserved and never reached the backend. What remains and when to retry are what a
/// response's `RateLimit` and `Retry-After` fields are made from, and the state says for
/// itself when the key can be forgotten without anyone noticing.
pub trait Model {
    /// How many requests arriving at `now` would be admitted one after another, if no
    /// time passed. Changes nothing.
    fn remaining(&self, now: u64) -> u64;

    /// Charges a request arriving at `now`, whether or not there was room for it. The
    /// receipt is what [`release`](Model::release) takes back.
    fn charge(&mut self, now: u64) -> Charge;

    /// Takes a charge back. Exact for the windows aligned to the clock and the rolling
    /// windows: the key is as if the charge had never been made. The others say how they
    /// fall short.
    fn release(&mut self, charge: Charge);

    /// When a request refused at `now` would first be admitted, if nothing else arrived;
    /// `None` if one would be admitted at `now`.
    fn retry_at(&self, now: u64) -> Option<u64>;

    /// From when on the key behaves as a new one would, if nothing else arrived: from
    /// then, its state can be dropped and nobody can tell.
    fn forgettable_at(&self) -> u64;

    /// Whether a request arriving at `now` would be admitted. Changes nothing.
    fn admits(&self, now: u64) -> bool {
        self.remaining(now) > 0
    }

    /// Decides a request arriving at `now`: `true` admits it and charges it, `false`
    /// refuses it and charges nothing.
    fn decide(&mut self, now: u64) -> bool {
        let admitted = self.admits(now);
        if admitted {
            self.charge(now);
        }
        admitted
    }
}

/// What a charge was, for [`Model::release`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Charge {
    /// When it was made, as the model took it.
    at: u64,
}

/// Decides a request arriving at `now` under every rule that applies to it: charged to
/// all of them if all admit it, to none if any refuses (06 §2).
pub fn decide_all(rules: &mut [&mut dyn Model], now: u64) -> bool {
    let admitted = rules.iter().all(|rule| rule.admits(now));
    if admitted {
        for rule in rules.iter_mut() {
            rule.charge(now);
        }
    }
    admitted
}

/// Where a fixed window's boundaries fall.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alignment {
    /// On multiples of the window from the time origin, the same for every key: one
    /// second's window runs from one whole second to the next.
    Clock,
    /// From the request that finds no window open: a key's window starts with its first
    /// request after the last one ended, as gubernator's "token bucket" does. Releasing
    /// the charge that opened a window leaves the window open.
    FirstRequest,
}

/// At most `limit` requests in each window; the count starts again at zero when the
/// window ends. A whole window's worth may come at once, and two windows' worth across
/// a boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedWindow {
    limit: u64,
    window: u64,
    alignment: Alignment,
    /// The start of the window being counted, if one has been opened.
    start: Option<u64>,
    count: u64,
    last: u64,
}

impl FixedWindow {
    /// A fixed window of `limit` requests per `window` nanoseconds.
    #[must_use]
    pub fn new(limit: NonZeroU64, window: NonZeroU64, alignment: Alignment) -> Self {
        Self {
            limit: limit.get(),
            window: window.get(),
            alignment,
            start: None,
            count: 0,
            last: 0,
        }
    }

    /// The state as it stands at `now`: the window `now` falls in, with what it counted.
    fn at(&self, now: u64) -> Self {
        let now = now.max(self.last);
        let start = match (self.alignment, self.start) {
            (Alignment::Clock, _) => now - now % self.window,
            (Alignment::FirstRequest, Some(start)) if now - start < self.window => start,
            (Alignment::FirstRequest, _) => now,
        };
        let count = if self.start == Some(start) {
            self.count
        } else {
            0
        };
        Self {
            start: Some(start),
            count,
            last: now,
            ..self.clone()
        }
    }
}

impl Model for FixedWindow {
    fn remaining(&self, now: u64) -> u64 {
        let state = self.at(now);
        state.limit.saturating_sub(state.count)
    }

    fn charge(&mut self, now: u64) -> Charge {
        *self = self.at(now);
        self.count += 1;
        Charge { at: self.last }
    }

    fn release(&mut self, charge: Charge) {
        if let Some(start) = self.start
            && (start..start + self.window).contains(&charge.at)
        {
            self.count = self.count.saturating_sub(1);
        }
    }

    fn retry_at(&self, now: u64) -> Option<u64> {
        let state = self.at(now);
        let start = state.start?;
        (state.count >= state.limit).then_some(start + state.window)
    }

    fn forgettable_at(&self) -> u64 {
        match (self.alignment, self.start) {
            (Alignment::Clock, Some(start)) if self.count > 0 => start + self.window,
            // A window opened by a request stays the key's until it ends, counting nothing.
            (Alignment::FirstRequest, Some(start)) => start + self.window,
            _ => 0,
        }
    }
}

/// At most `limit` requests in any span of `window` nanoseconds: a request is admitted
/// only if fewer than `limit` were admitted in the `window` before it, an admission
/// exactly `window` earlier no longer counting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollingWindow {
    limit: u64,
    window: u64,
    /// Admissions still inside the window, oldest first.
    admitted: VecDeque<u64>,
    last: u64,
}

impl RollingWindow {
    /// A rolling window of `limit` requests per `window` nanoseconds.
    #[must_use]
    pub fn new(limit: NonZeroU64, window: NonZeroU64) -> Self {
        Self {
            limit: limit.get(),
            window: window.get(),
            admitted: VecDeque::new(),
            last: 0,
        }
    }

    /// The state as it stands at `now`: only the admissions still inside the window.
    fn at(&self, now: u64) -> Self {
        let now = now.max(self.last);
        let mut state = Self {
            last: now,
            ..self.clone()
        };
        while state
            .admitted
            .front()
            .is_some_and(|&at| now - at >= state.window)
        {
            state.admitted.pop_front();
        }
        state
    }
}

impl Model for RollingWindow {
    fn remaining(&self, now: u64) -> u64 {
        let state = self.at(now);
        state.limit.saturating_sub(state.admitted.len() as u64)
    }

    fn charge(&mut self, now: u64) -> Charge {
        *self = self.at(now);
        self.admitted.push_back(self.last);
        Charge { at: self.last }
    }

    fn release(&mut self, charge: Charge) {
        if let Some(index) = self.admitted.iter().position(|&at| at == charge.at) {
            self.admitted.remove(index);
        }
    }

    fn retry_at(&self, now: u64) -> Option<u64> {
        let state = self.at(now);
        let inside = state.admitted.len() as u64;
        // The admission whose leaving brings the count below the limit.
        let leaving = usize::try_from(inside.checked_sub(state.limit)?).ok()?;
        state.admitted.get(leaving).map(|at| at + state.window)
    }

    fn forgettable_at(&self) -> u64 {
        self.admitted.back().map_or(0, |at| at + self.window)
    }
}

/// Cloudflare's approximation of a rolling window from two fixed windows on the clock:
/// the count of the window before is taken as spread evenly over it, and the part of it
/// still inside the last `window` is added to the current window's count. A request is
/// admitted if that estimate, the request included, is at most `limit`. Cheap, but it can
/// refuse a client an exact rolling window admits, and admit one it refuses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApproximateRollingWindow {
    limit: u64,
    window: u64,
    /// The index of the current window (its start over `window`), once one has been seen.
    index: Option<u64>,
    previous: u64,
    current: u64,
    last: u64,
}

impl ApproximateRollingWindow {
    /// An approximate rolling window of `limit` requests per `window` nanoseconds.
    #[must_use]
    pub fn new(limit: NonZeroU64, window: NonZeroU64) -> Self {
        Self {
            limit: limit.get(),
            window: window.get(),
            index: None,
            previous: 0,
            current: 0,
            last: 0,
        }
    }

    /// The state as it stands at `now`: the counts of the window `now` falls in and the
    /// one before it.
    fn at(&self, now: u64) -> Self {
        let now = now.max(self.last);
        let index = now / self.window;
        let (previous, current) = match self.index {
            Some(seen) if seen == index => (self.previous, self.current),
            Some(seen) if seen + 1 == index => (self.current, 0),
            _ => (0, 0),
        };
        Self {
            index: Some(index),
            previous,
            current,
            last: now,
            ..self.clone()
        }
    }

    /// How many more would be admitted `elapsed` into a window with `previous` and
    /// `current` counted: the most `k` with
    /// `previous × (window − elapsed) + (current + k) × window ≤ limit × window`.
    fn room(&self, previous: u64, current: u64, elapsed: u64) -> u64 {
        let window = u128::from(self.window);
        let carried = u128::from(previous) * (window - u128::from(elapsed));
        let allowed = (u128::from(self.limit) * window).saturating_sub(carried) / window;
        u64::try_from(allowed.saturating_sub(u128::from(current))).unwrap_or(u64::MAX)
    }

    /// The first point at or after `from` into a window with `previous` and `current`
    /// counted where one more would be admitted, if there is one before the window ends.
    fn first_room(&self, previous: u64, current: u64, from: u64) -> Option<u64> {
        if current >= self.limit {
            return None;
        }
        // previous × (window − elapsed) ≤ (limit − current − 1) × window.
        let window = u128::from(self.window);
        let spare = match previous {
            0 => window,
            previous => u128::from(self.limit - current - 1) * window / u128::from(previous),
        };
        let earliest = u64::try_from(window.saturating_sub(spare)).unwrap_or(u64::MAX);
        let elapsed = earliest.max(from);
        (elapsed < self.window).then_some(elapsed)
    }
}

impl Model for ApproximateRollingWindow {
    fn remaining(&self, now: u64) -> u64 {
        let state = self.at(now);
        self.room(state.previous, state.current, state.last % self.window)
    }

    fn charge(&mut self, now: u64) -> Charge {
        *self = self.at(now);
        self.current += 1;
        Charge { at: self.last }
    }

    fn release(&mut self, charge: Charge) {
        let index = charge.at / self.window;
        match self.index {
            Some(seen) if seen == index => self.current = self.current.saturating_sub(1),
            Some(seen) if seen == index + 1 => self.previous = self.previous.saturating_sub(1),
            _ => {}
        }
    }

    fn retry_at(&self, now: u64) -> Option<u64> {
        let state = self.at(now);
        let elapsed = state.last % self.window;
        if self.room(state.previous, state.current, elapsed) > 0 {
            return None;
        }
        let start = state.last - elapsed;
        // In this window, as the share of the one before fades; else in the next, where
        // this one's count is the share that fades; else after that, with nothing left.
        if let Some(at) = self.first_room(state.previous, state.current, elapsed) {
            return Some(start + at);
        }
        if let Some(at) = self.first_room(state.current, 0, 0) {
            return Some(start + self.window + at);
        }
        Some(start + 2 * self.window)
    }

    fn forgettable_at(&self) -> u64 {
        match self.index {
            Some(index) if self.current > 0 => (index + 2) * self.window,
            Some(index) if self.previous > 0 => (index + 1) * self.window,
            _ => 0,
        }
    }
}

/// A sustained rate of `limit` requests per `window` nanoseconds with a bucket of
/// `capacity` requests, kept as GCRA keeps it: the theoretical arrival time of the next
/// request. Each admission moves it on by one request's interval, `window / limit`; a
/// request is admitted unless that time is more than `capacity − 1` intervals ahead of
/// it. A new key's bucket is full. In any span of `T` nanoseconds at most
/// `limit × T / window + capacity` requests are admitted.
///
/// How a rule's `burst` maps to `capacity` is open (06 §3): the whole bucket, one request
/// more (NGINX), or a window's worth more.
///
/// Releasing a charge gives back one request's interval. If the bucket filled up while the
/// charge was out, some of it had come back already, and the release gives back up to one
/// request more than if the charge had never been made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenBucket {
    /// Times are kept multiplied by `limit`, so that one interval is exactly `window`.
    limit: u128,
    window: u128,
    /// How far ahead of now the arrival time may be, in scaled time: `(capacity − 1) × window`.
    tolerance: u128,
    /// The theoretical arrival time, scaled; `None` until the first charge.
    arrival: Option<u128>,
    last: u64,
}

impl TokenBucket {
    /// A token bucket refilling `limit` requests per `window` nanoseconds and holding at
    /// most `capacity`.
    #[must_use]
    pub fn new(limit: NonZeroU64, window: NonZeroU64, capacity: NonZeroU64) -> Self {
        Self {
            limit: u128::from(limit.get()),
            window: u128::from(window.get()),
            tolerance: u128::from(capacity.get() - 1) * u128::from(window.get()),
            arrival: None,
            last: 0,
        }
    }

    /// `now` as the model takes it, and scaled.
    fn scaled(&self, now: u64) -> (u64, u128) {
        let now = now.max(self.last);
        (now, u128::from(now) * self.limit)
    }

    /// How far ahead of `now` (scaled) the arrival time is.
    fn ahead(&self, now: u128) -> u128 {
        self.arrival.unwrap_or(now).max(now) - now
    }

    /// How far the bucket is from full at `now`, in time multiplied by `limit` (one
    /// request's interval is exactly `window`): zero when full, more than the bucket holds
    /// when it owes. The bucket's state relative to the moment, so that a copy can be set
    /// from it on another machine's clock with [`set_behind`](Self::set_behind).
    #[must_use]
    pub fn behind(&self, now: u64) -> u128 {
        let (_, now) = self.scaled(now);
        self.ahead(now)
    }

    /// Sets the bucket to be `behind` from full at `now`, as [`behind`](Self::behind) says
    /// it; earlier times are taken as `now` from then on.
    pub fn set_behind(&mut self, now: u64, behind: u128) {
        let (at, scaled) = self.scaled(now);
        self.last = at;
        self.arrival = (behind > 0).then(|| scaled + behind);
    }

    /// What the bucket holds besides its terms, in less room: for holding many buckets
    /// whose terms are kept elsewhere, each made again with [`from_state`](Self::from_state)
    /// to be used.
    #[must_use]
    pub fn state(&self) -> BucketState {
        BucketState {
            arrival: self.arrival.unwrap_or(0),
            last: self.last,
        }
    }

    /// The bucket refilling `limit` requests per `window` nanoseconds and holding at most
    /// `capacity`, as [`state`](Self::state) left it.
    #[must_use]
    pub fn from_state(
        limit: NonZeroU64,
        window: NonZeroU64,
        capacity: NonZeroU64,
        state: BucketState,
    ) -> Self {
        Self {
            arrival: (state.arrival > 0).then_some(state.arrival),
            last: state.last,
            ..Self::new(limit, window, capacity)
        }
    }
}

/// A [`TokenBucket`]'s state without its terms. An arrival time of zero stands for none:
/// a bucket arriving at zero is behind every time, as one with none is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketState {
    arrival: u128,
    last: u64,
}

impl Model for TokenBucket {
    /// Room for one more: the arrival time no further ahead than the bucket allows. The
    /// same as `remaining(now) > 0`, without its division; it is on the request path.
    fn admits(&self, now: u64) -> bool {
        let (_, now) = self.scaled(now);
        self.ahead(now) <= self.tolerance
    }

    fn remaining(&self, now: u64) -> u64 {
        let (_, now) = self.scaled(now);
        let ahead = self.ahead(now);
        if ahead > self.tolerance {
            return 0;
        }
        u64::try_from((self.tolerance - ahead) / self.window + 1).unwrap_or(u64::MAX)
    }

    fn charge(&mut self, now: u64) -> Charge {
        let (at, scaled) = self.scaled(now);
        self.last = at;
        self.arrival = Some(scaled + self.ahead(scaled) + self.window);
        Charge { at }
    }

    fn release(&mut self, _: Charge) {
        self.arrival = self
            .arrival
            .map(|arrival| arrival.saturating_sub(self.window));
    }

    fn retry_at(&self, now: u64) -> Option<u64> {
        let (now, scaled) = self.scaled(now);
        let ahead = self.ahead(scaled);
        if ahead <= self.tolerance {
            return None;
        }
        // The first time `t` with `arrival − t × limit ≤ tolerance`.
        let wait = (ahead - self.tolerance).div_ceil(self.limit);
        Some(now.saturating_add(u64::try_from(wait).unwrap_or(u64::MAX)))
    }

    fn forgettable_at(&self) -> u64 {
        // Full again once the arrival time is no longer ahead.
        self.arrival.map_or(0, |arrival| {
            u64::try_from(arrival.div_ceil(self.limit)).unwrap_or(u64::MAX)
        })
    }
}

/// The same contract as [`TokenBucket`], written the other way round: tokens drip in at
/// `limit` per `window` up to `capacity`, and a request takes one. Kept only so that the
/// two can be checked against each other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountingBucket {
    limit: u128,
    /// Tokens are kept multiplied by `window`, so that one token is exactly `window`.
    window: u128,
    capacity: u128,
    /// Tokens as of `last`.
    tokens: u128,
    /// What charges made without room took beyond the tokens there were: paid back by
    /// the refill before any token comes back. Only ever owed with no tokens left.
    owed: u128,
    last: u64,
}

impl CountingBucket {
    /// A bucket refilling `limit` tokens per `window` nanoseconds and holding at most
    /// `capacity`, full at the start.
    #[must_use]
    pub fn new(limit: NonZeroU64, window: NonZeroU64, capacity: NonZeroU64) -> Self {
        let window = u128::from(window.get());
        let capacity = u128::from(capacity.get()) * window;
        Self {
            limit: u128::from(limit.get()),
            window,
            capacity,
            tokens: capacity,
            owed: 0,
            last: 0,
        }
    }

    /// The state as it stands at `now`: refilled, what was owed paid back first.
    fn at(&self, now: u64) -> Self {
        let now = now.max(self.last);
        let refill = u128::from(now - self.last) * self.limit;
        let repaid = refill.min(self.owed);
        Self {
            tokens: (self.tokens + refill - repaid).min(self.capacity),
            owed: self.owed - repaid,
            last: now,
            ..self.clone()
        }
    }
}

impl Model for CountingBucket {
    fn remaining(&self, now: u64) -> u64 {
        let state = self.at(now);
        u64::try_from(state.tokens / self.window).unwrap_or(u64::MAX)
    }

    fn charge(&mut self, now: u64) -> Charge {
        *self = self.at(now);
        let taken = self.tokens.min(self.window);
        self.tokens -= taken;
        self.owed += self.window - taken;
        Charge { at: self.last }
    }

    fn release(&mut self, _: Charge) {
        let repaid = self.owed.min(self.window);
        self.owed -= repaid;
        self.tokens = (self.tokens + self.window - repaid).min(self.capacity);
    }

    fn retry_at(&self, now: u64) -> Option<u64> {
        let state = self.at(now);
        if state.tokens >= self.window {
            return None;
        }
        let short = state.owed + self.window - state.tokens;
        let wait = u64::try_from(short.div_ceil(self.limit)).unwrap_or(u64::MAX);
        Some(state.last.saturating_add(wait))
    }

    fn forgettable_at(&self) -> u64 {
        let missing = self.capacity - self.tokens + self.owed;
        if missing == 0 {
            return 0;
        }
        let wait = u64::try_from(missing.div_ceil(self.limit)).unwrap_or(u64::MAX);
        self.last.saturating_add(wait)
    }
}

#[cfg(test)]
mod tests;
