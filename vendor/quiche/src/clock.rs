// EdgeRush: this module is not in quiche as published.

//! The time a connection reads for itself, which quiche's own tests may hold
//! still and move on by hand.
//!
//! A connection reads the clock inside `recv()`, `send()`, `timeout()`,
//! `on_timeout()` and `migrate()`, so a test of what it does at a given moment
//! raced the real clock: a thread held up on a busy machine between two calls
//! saw a timer fire that the test meant to look at before it was due. Outside
//! quiche's own tests this is `Instant::now()` and nothing more.

/// The time now.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn now() -> std::time::Instant {
    std::time::Instant::now()
}

#[cfg(test)]
pub(crate) use held::now;

#[cfg(test)]
pub(crate) mod held {
    use std::cell::Cell;
    use std::time::Duration;
    use std::time::Instant;

    thread_local! {
        static HELD: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    /// The time this thread's clock is held at, or the real time when it is
    /// not held.
    pub(crate) fn now() -> Instant {
        HELD.with(Cell::get).unwrap_or_else(Instant::now)
    }

    /// Holds this thread's clock at the real time now until the returned
    /// guard is dropped. Every time read before is no later than it.
    pub(crate) fn hold() -> Held {
        HELD.with(|held| held.set(Some(Instant::now())));
        Held(())
    }

    /// This thread's clock held still; dropping it lets the clock run again.
    pub(crate) struct Held(());

    impl Held {
        /// Moves the held clock on by `by`.
        pub(crate) fn advance(&self, by: Duration) {
            HELD.with(|held| held.set(held.get().map(|at| at + by)));
        }
    }

    impl Drop for Held {
        fn drop(&mut self) {
            HELD.with(|held| held.set(None));
        }
    }

    #[test]
    fn a_held_clock_moves_only_when_moved() {
        let held = hold();
        let at = now();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(now(), at);

        held.advance(Duration::from_millis(5));
        assert_eq!(now(), at + Duration::from_millis(5));

        drop(held);
        assert!(now() > at + Duration::from_millis(1));
    }

    #[test]
    fn a_clock_is_held_on_its_own_thread_only() {
        let _held = hold();
        let at = now();

        std::thread::sleep(Duration::from_millis(2));
        let elsewhere = std::thread::spawn(now).join().unwrap();
        assert!(elsewhere > at);
    }
}
