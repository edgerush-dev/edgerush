//! Which of a client connection's deadlines runs, and when it falls due.
//!
//! Told what happened and when, and asked which single deadline is next: the connection
//! driver arms one timer from the answer rather than a timer for every clock, and re-arms
//! it only when the answer changes. It reads no clock, so every rule below can be put to a
//! table of times ([14 §8](../../../../docs/14-downstream-server.md)).
//!
//! Two kinds of clock. An **absolute** one is set once and never moves, however busy the
//! peer is: a head trickled a byte at a time cannot buy itself more time. An **inactivity**
//! one runs only while its own work is what is being waited for, and only progress in that
//! work sets it again: a client stopped by backpressure is not charged for it, and a poll
//! that found nothing is not progress.

use std::time::{Duration, Instant};

/// How long each clock runs. Test-overridable; the defaults are 14 §8's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// From accept to the first complete request head, whatever the time is spent on.
    pub first_request: Duration,
    /// From the moment the next head can be read — its first byte, or bytes already
    /// buffered once the previous request is done — to that head being complete.
    pub next_head: Duration,
    /// Between requests, with nothing of the next one yet.
    pub keep_alive: Duration,
    /// A request body or a response write waited on without progress.
    pub idle: Duration,
    /// A lingering close with nothing arriving.
    pub linger_quiet: Duration,
    /// A lingering close at the most, however much keeps arriving.
    pub linger_most: Duration,
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            first_request: Duration::from_secs(10),
            next_head: Duration::from_secs(10),
            keep_alive: Duration::from_secs(30),
            idle: Duration::from_secs(30),
            linger_quiet: Duration::from_secs(5),
            linger_most: Duration::from_secs(30),
        }
    }
}

impl Bounds {
    /// The least of them: no deadline is ever set to fall due sooner than this after
    /// whatever set it.
    pub fn shortest(&self) -> Duration {
        [
            self.first_request,
            self.next_head,
            self.keep_alive,
            self.idle,
            self.linger_quiet,
            self.linger_most,
        ]
        .into_iter()
        .min()
        .unwrap_or_default()
    }
}

/// Which deadline it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clock {
    /// The first request head did not arrive in time.
    FirstRequest,
    /// A later request head did not arrive in time once it could be read.
    NextHead,
    /// Nothing more was asked in time.
    KeepAlive,
    /// The request body stopped while there was room for it.
    BodyIdle,
    /// The client stopped taking the answer.
    WriteIdle,
    /// A lingering close heard nothing for long enough.
    LingerQuiet,
    /// A lingering close ran as long as one may.
    LingerMost,
}

/// Where the connection is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// From accept to the first complete head, detection included.
    First { by: Instant },
    /// A request is being served. Each clock is set only while its work is waited on.
    Serving {
        body: Option<Instant>,
        write: Option<Instant>,
    },
    /// Between requests, holding nothing of the next.
    KeepAlive { by: Instant },
    /// The next head can be read and must be finished by then.
    NextHead { by: Instant },
    /// Done with the connection, its end being said: shutting its sending half, which a
    /// client that takes nothing more holds up, so it is a write's clock that runs.
    Shutting { by: Instant },
    /// Done with the connection: reading and discarding what the client still sends.
    Lingering { quiet: Instant, most: Instant },
    /// Nothing is running.
    Closed,
}

/// The deadlines of one client connection.
#[derive(Debug, Clone)]
pub struct Deadlines {
    bounds: Bounds,
    phase: Phase,
}

impl Deadlines {
    /// A connection accepted at `now`.
    pub fn accepted(now: Instant, bounds: Bounds) -> Self {
        Self {
            bounds,
            phase: Phase::First {
                by: now + bounds.first_request,
            },
        }
    }

    /// A request head is complete. The head's clock stops; nothing runs until some work
    /// of the request's is waited on.
    pub fn head_read(&mut self) {
        if matches!(
            self.phase,
            Phase::First { .. } | Phase::NextHead { .. } | Phase::KeepAlive { .. }
        ) {
            self.phase = Phase::Serving {
                body: None,
                write: None,
            };
        }
    }

    /// Bytes arrived from the client. Between requests they start the next head's clock;
    /// while lingering they keep the quiet clock from running out; anywhere else they
    /// change nothing — an absolute clock is not moved by progress.
    pub fn bytes_arrived(&mut self, now: Instant) {
        match self.phase {
            Phase::KeepAlive { .. } => {
                self.phase = Phase::NextHead {
                    by: now + self.bounds.next_head,
                };
            }
            Phase::Lingering { most, .. } => {
                self.phase = Phase::Lingering {
                    quiet: now + self.bounds.linger_quiet,
                    most,
                };
            }
            Phase::First { .. }
            | Phase::Serving { .. }
            | Phase::NextHead { .. }
            | Phase::Shutting { .. }
            | Phase::Closed => {}
        }
    }

    /// Whether the request body is what is being waited for: unfinished, allowed to be
    /// read, and with room to put it. Saying so again while it already is changes nothing.
    pub fn body_waited_on(&mut self, now: Instant, waited_on: bool) {
        let idle = self.bounds.idle;
        if let Phase::Serving { body, .. } = &mut self.phase {
            match (waited_on, *body) {
                (true, None) => *body = Some(now + idle),
                (false, Some(_)) => *body = None,
                _ => {}
            }
        }
    }

    /// The request body moved.
    pub fn body_moved(&mut self, now: Instant) {
        let idle = self.bounds.idle;
        if let Phase::Serving {
            body: Some(due), ..
        } = &mut self.phase
        {
            *due = now + idle;
        }
    }

    /// Whether the answer is waiting on the client: bytes queued that the socket will not
    /// take. Saying so again while it already is changes nothing.
    pub fn write_waited_on(&mut self, now: Instant, waited_on: bool) {
        let idle = self.bounds.idle;
        if let Phase::Serving { write, .. } = &mut self.phase {
            match (waited_on, *write) {
                (true, None) => *write = Some(now + idle),
                (false, Some(_)) => *write = None,
                _ => {}
            }
        }
    }

    /// The socket took bytes of the answer.
    pub fn write_moved(&mut self, now: Instant) {
        let idle = self.bounds.idle;
        if let Phase::Serving {
            write: Some(due), ..
        } = &mut self.phase
        {
            *due = now + idle;
        }
    }

    /// The request is done — its framing complete, its answer written, its exchange let
    /// go of — and the connection carries on. With bytes of the next request already
    /// buffered, that head's clock starts now; with none, the connection is idle.
    pub fn answered(&mut self, now: Instant, read_ahead: bool) {
        if matches!(self.phase, Phase::Serving { .. }) {
            self.phase = if read_ahead {
                Phase::NextHead {
                    by: now + self.bounds.next_head,
                }
            } else {
                Phase::KeepAlive {
                    by: now + self.bounds.keep_alive,
                }
            };
        }
    }

    /// The server is done with the connection and shuts its sending half, which must go
    /// within a write's idle bound.
    pub fn shutting(&mut self, now: Instant) {
        if !matches!(self.phase, Phase::Lingering { .. } | Phase::Closed) {
            self.phase = Phase::Shutting {
                by: now + self.bounds.idle,
            };
        }
    }

    /// The server is done with the connection and lingers, reading and discarding what
    /// the client still sends (13 §5).
    pub fn lingering(&mut self, now: Instant) {
        if !matches!(self.phase, Phase::Lingering { .. } | Phase::Closed) {
            self.phase = Phase::Lingering {
                quiet: now + self.bounds.linger_quiet,
                most: now + self.bounds.linger_most,
            };
        }
    }

    /// The connection is gone.
    pub fn closed(&mut self) {
        self.phase = Phase::Closed;
    }

    /// Whether the connection is idle between requests, holding nothing of the next: the
    /// state in which it may hold no request storage at all (§3).
    pub fn is_idle(&self) -> bool {
        matches!(self.phase, Phase::KeepAlive { .. })
    }

    /// The deadline that falls due first, and when.
    pub fn next(&self) -> Option<(Clock, Instant)> {
        match self.phase {
            Phase::First { by } => Some((Clock::FirstRequest, by)),
            Phase::NextHead { by } => Some((Clock::NextHead, by)),
            Phase::KeepAlive { by } => Some((Clock::KeepAlive, by)),
            Phase::Shutting { by } => Some((Clock::WriteIdle, by)),
            Phase::Serving { body, write } => {
                let body = body.map(|due| (Clock::BodyIdle, due));
                let write = write.map(|due| (Clock::WriteIdle, due));
                match (body, write) {
                    (Some(body), Some(write)) => Some(if write.1 < body.1 { write } else { body }),
                    (one, other) => one.or(other),
                }
            }
            Phase::Lingering { quiet, most } => Some(if most <= quiet {
                (Clock::LingerMost, most)
            } else {
                (Clock::LingerQuiet, quiet)
            }),
            Phase::Closed => None,
        }
    }

    /// The deadline that has run out by `now`, if one has.
    pub fn expired(&self, now: Instant) -> Option<Clock> {
        self.next()
            .and_then(|(clock, due)| (due <= now).then_some(clock))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    /// A connection accepted at a fixed moment, and that moment.
    fn accepted() -> (Deadlines, Instant) {
        let start = Instant::now();
        (Deadlines::accepted(start, Bounds::default()), start)
    }

    #[test]
    fn the_first_head_has_ten_seconds_from_accept_whatever_arrives() {
        let (mut connection, start) = accepted();
        assert_eq!(
            connection.next(),
            Some((Clock::FirstRequest, start + secs(10)))
        );
        // A byte a second does not move an absolute clock.
        for second in 1..10 {
            connection.bytes_arrived(start + secs(second));
        }
        assert_eq!(connection.expired(start + secs(9)), None);
        assert_eq!(
            connection.expired(start + secs(10)),
            Some(Clock::FirstRequest)
        );
    }

    #[test]
    fn nothing_runs_while_a_request_is_served_until_something_is_waited_on() {
        let (mut connection, start) = accepted();
        connection.head_read();
        assert_eq!(connection.next(), None);
        assert!(!connection.is_idle());
        connection.body_waited_on(start + secs(1), true);
        assert_eq!(connection.next(), Some((Clock::BodyIdle, start + secs(31))));
    }

    /// Asking again is not progress; moving is. Backpressure stops the clock rather than
    /// charging the client for it.
    #[test]
    fn a_body_clock_runs_only_while_the_body_is_waited_on() {
        let (mut connection, start) = accepted();
        connection.head_read();
        connection.body_waited_on(start, true);
        connection.body_waited_on(start + secs(10), true);
        assert_eq!(connection.next(), Some((Clock::BodyIdle, start + secs(30))));
        connection.body_moved(start + secs(20));
        assert_eq!(connection.next(), Some((Clock::BodyIdle, start + secs(50))));
        connection.body_waited_on(start + secs(25), false);
        assert_eq!(
            connection.next(),
            None,
            "backpressure is not the client's delay"
        );
        assert_eq!(connection.expired(start + secs(100)), None);
        connection.body_waited_on(start + secs(60), true);
        assert_eq!(connection.next(), Some((Clock::BodyIdle, start + secs(90))));
    }

    #[test]
    fn body_and_write_clocks_run_apart_and_the_first_due_is_next() {
        let (mut connection, start) = accepted();
        connection.head_read();
        connection.body_waited_on(start, true);
        connection.write_waited_on(start + secs(5), true);
        assert_eq!(connection.next(), Some((Clock::BodyIdle, start + secs(30))));
        connection.body_moved(start + secs(10));
        assert_eq!(
            connection.next(),
            Some((Clock::WriteIdle, start + secs(35)))
        );
        connection.write_moved(start + secs(20));
        assert_eq!(connection.next(), Some((Clock::BodyIdle, start + secs(40))));
        // Progress in one does not vouch for the other.
        connection.write_moved(start + secs(39));
        assert_eq!(connection.expired(start + secs(40)), Some(Clock::BodyIdle));
    }

    #[test]
    fn an_idle_connection_waits_thirty_seconds_then_its_next_head_ten_from_its_first_byte() {
        let (mut connection, start) = accepted();
        connection.head_read();
        connection.answered(start + secs(1), false);
        assert!(connection.is_idle());
        assert_eq!(
            connection.next(),
            Some((Clock::KeepAlive, start + secs(31)))
        );
        connection.bytes_arrived(start + secs(20));
        assert!(!connection.is_idle());
        assert_eq!(connection.next(), Some((Clock::NextHead, start + secs(30))));
        // And from there it is absolute.
        connection.bytes_arrived(start + secs(29));
        assert_eq!(connection.expired(start + secs(30)), Some(Clock::NextHead));
    }

    /// 14 §8's case: request B arrives in part while A's answer streams for thirty
    /// seconds. B's head clock starts when A is done, not when B's bytes came, and
    /// trickling B cannot extend it.
    #[test]
    fn a_pipelined_head_gets_its_ten_seconds_when_the_one_before_it_is_done() {
        let (mut connection, start) = accepted();
        connection.head_read();
        // B's first bytes, buffered as read-ahead while A is served: nothing starts.
        connection.bytes_arrived(start + secs(2));
        for second in 3..31 {
            connection.write_waited_on(start + secs(second), true);
            connection.write_moved(start + secs(second));
        }
        assert_eq!(connection.expired(start + secs(31)), None);
        connection.answered(start + secs(31), true);
        assert!(!connection.is_idle(), "bytes of the next request are held");
        assert_eq!(connection.next(), Some((Clock::NextHead, start + secs(41))));
        for second in 32..41 {
            connection.bytes_arrived(start + secs(second));
        }
        assert_eq!(connection.expired(start + secs(40)), None);
        assert_eq!(connection.expired(start + secs(41)), Some(Clock::NextHead));
    }

    #[test]
    fn a_lingering_close_ends_when_quiet_or_at_its_most() {
        let (mut connection, start) = accepted();
        connection.lingering(start);
        assert_eq!(
            connection.next(),
            Some((Clock::LingerQuiet, start + secs(5)))
        );
        connection.bytes_arrived(start + secs(4));
        assert_eq!(
            connection.next(),
            Some((Clock::LingerQuiet, start + secs(9)))
        );
        // A client that keeps sending is let go at the most all the same.
        for second in (8..30).step_by(4) {
            connection.bytes_arrived(start + secs(second));
        }
        connection.bytes_arrived(start + secs(29));
        assert_eq!(
            connection.expired(start + secs(30)),
            Some(Clock::LingerMost)
        );
        // Lingering again does not start it over.
        connection.lingering(start + secs(29));
        assert_eq!(
            connection.next().map(|(_, due)| due),
            Some(start + secs(30))
        );
    }

    /// Shutting the sending half waits on the client as a write does, from whatever the
    /// connection was doing, and for no longer: what the client sends meanwhile does not
    /// put it off.
    #[test]
    fn a_shutting_connection_waits_a_writes_idle_bound() {
        let (mut connection, start) = accepted();
        connection.head_read();
        connection.body_waited_on(start + secs(1), true);
        connection.shutting(start + secs(2));
        assert_eq!(
            connection.next(),
            Some((Clock::WriteIdle, start + secs(32)))
        );
        connection.bytes_arrived(start + secs(20));
        assert_eq!(connection.expired(start + secs(31)), None);
        assert_eq!(connection.expired(start + secs(32)), Some(Clock::WriteIdle));
        // Nor does it start a lingering or a closed connection's clocks again.
        connection.lingering(start + secs(33));
        connection.shutting(start + secs(34));
        assert_eq!(
            connection.next().map(|(clock, _)| clock),
            Some(Clock::LingerQuiet)
        );
        connection.closed();
        connection.shutting(start + secs(35));
        assert_eq!(connection.next(), None);
    }

    #[test]
    fn events_that_do_not_apply_change_nothing() {
        let (mut connection, start) = accepted();
        let first = connection.next();
        connection.answered(start + secs(1), false);
        connection.body_waited_on(start + secs(1), true);
        connection.write_waited_on(start + secs(1), true);
        connection.body_moved(start + secs(1));
        connection.write_moved(start + secs(1));
        assert_eq!(connection.next(), first);
        connection.closed();
        assert_eq!(connection.next(), None);
        connection.bytes_arrived(start + secs(2));
        connection.lingering(start + secs(2));
        connection.head_read();
        assert_eq!(connection.next(), None, "a closed connection has no clocks");
    }

    #[test]
    fn the_shortest_bound_is_the_least_of_them() {
        assert_eq!(Bounds::default().shortest(), secs(5));
        let bounds = Bounds {
            idle: secs(2),
            ..Bounds::default()
        };
        assert_eq!(bounds.shortest(), secs(2));
    }

    proptest::proptest! {
        /// Whatever happens and whenever, the next deadline comes sooner only by being set
        /// at least the shortest bound after what set it. The connection's timer relies on
        /// this: set that far ahead, no deadline set after it falls due before it.
        #[test]
        fn a_deadline_comes_sooner_only_by_the_shortest_bound_from_now(
            seconds in proptest::collection::vec(1_u64..40, 6),
            events in proptest::collection::vec((0_u8..11, 0_u64..20_000, proptest::bool::ANY), 0..40),
        ) {
            let [first_request, next_head, keep_alive, idle, linger_quiet, linger_most] =
                [0, 1, 2, 3, 4, 5].map(|at| secs(seconds[at]));
            let bounds = Bounds { first_request, next_head, keep_alive, idle, linger_quiet, linger_most };
            let start = Instant::now();
            let mut connection = Deadlines::accepted(start, bounds);
            let mut now = start;
            for (event, later, flag) in events {
                now += Duration::from_millis(later);
                let before = connection.next().map(|(_, due)| due);
                match event {
                    0 => connection.head_read(),
                    1 => connection.bytes_arrived(now),
                    2 => connection.body_waited_on(now, flag),
                    3 => connection.body_moved(now),
                    4 => connection.write_waited_on(now, flag),
                    5 => connection.write_moved(now),
                    6 => connection.answered(now, flag),
                    7 => connection.lingering(now),
                    8 => connection.closed(),
                    9 => connection.shutting(now),
                    _ => {}
                }
                if let Some((clock, due)) = connection.next()
                    && before.is_none_or(|before| due < before)
                {
                    proptest::prop_assert!(
                        due >= now + bounds.shortest(),
                        "{clock:?} came sooner, to {:?} after an event at {:?}",
                        due - start,
                        now - start
                    );
                }
            }
        }
    }
}
