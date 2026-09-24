//! The two kinds of number that are kept: one that only grows, and one that goes up and
//! down.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// A number that only grows. Adding never waits: within a [`Sharded`](crate::Sharded) group
/// a thread has the counter to itself, and the ordering is relaxed because nothing is
/// decided on the value — it is only ever added up and shown.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// One more.
    pub fn inc(&self) {
        self.add(1);
    }

    /// `amount` more. Wraps around after 2⁶⁴, as Prometheus expects of a counter.
    pub fn add(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Relaxed);
    }

    /// What has been counted here so far.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A number that goes up and down: how many of something there are right now.
///
/// What goes up on one thread may come down on another — a connection that is accepted
/// here and ends there — so a single shard's value means nothing and may be negative; the
/// sum over the shards is the number.
#[derive(Debug, Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    /// One more.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// One fewer.
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    /// Sets this shard's part to `value`: for a gauge whose shard has one writer, which
    /// samples what it holds rather than counting it up and down.
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }

    /// This shard's part of the number.
    #[must_use]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counter_adds_up() {
        let counter = Counter::default();
        assert_eq!(counter.get(), 0);
        counter.inc();
        counter.add(41);
        assert_eq!(counter.get(), 42);
    }

    #[test]
    fn a_counter_wraps_around_rather_than_panic() {
        let counter = Counter::default();
        counter.add(u64::MAX);
        counter.add(2);
        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn a_gauge_goes_up_and_down_and_below_zero() {
        let gauge = Gauge::default();
        gauge.inc();
        gauge.inc();
        gauge.dec();
        assert_eq!(gauge.get(), 1);
        gauge.dec();
        gauge.dec();
        assert_eq!(gauge.get(), -1);
    }

    #[test]
    fn a_gauge_can_be_set_to_what_its_one_writer_found() {
        let gauge = Gauge::default();
        gauge.set(3_145_728);
        assert_eq!(gauge.get(), 3_145_728);
        gauge.set(0);
        assert_eq!(gauge.get(), 0);
    }
}
