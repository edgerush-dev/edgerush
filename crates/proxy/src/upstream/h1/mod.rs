//! HTTP/1.1 to an upstream.
//!
//! The protocol engine ([`codec`]) is by itself: it is given bytes and told of events, and
//! says what it made of them. It opens no socket, reads no clock and spawns nothing, which
//! is what lets every one of its answers be checked against a table and every split of its
//! input be tried. The code that does hold a socket ([`exchange`]) keeps that separation,
//! and is where the clock lives: the engine is told what happened, never when.

pub mod codec;
pub mod exchange;
pub mod pool;
// The scripted peer the differential harness is driven from, and the bounded run it
// happens under: for this crate's own tests and for the fuzz targets, which are a crate
// of their own ([13 §8](../../../../docs/13-http1-upstream.md)).
// The specification as code, for the harness to compare both clients against.
#[cfg(any(test, feature = "fuzzing"))]
pub mod reference;
#[cfg(any(test, feature = "fuzzing"))]
pub mod script;

use std::time::Duration;

/// What a worker will not go beyond, whatever an upstream sends. Conservative numbers for
/// development, not settings anybody configures and not the pool's policy, which waits for
/// the benchmark to be run again with TLS ([13 §7](../../../docs/13-http1-upstream.md)).
///
/// A gain that came of loosening these is not a gain, so a benchmark that changes them
/// says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct H1Limits {
    /// The most a response head may come to, status line and fields together.
    pub head: usize,
    /// The most fields a head may carry. No more than [`codec::MOST_FIELDS`] is read,
    /// whatever this says.
    pub fields: usize,
    /// The most a chunk's size line may come to, its extensions included.
    pub chunk_line: usize,
    /// The most the fields after the last chunk may come to.
    pub trailers: usize,
    /// The most fields there may be among them.
    pub trailer_fields: usize,
    /// How many interim answers one exchange will wait through.
    pub interim_heads: usize,
    /// What those interim answers may come to together.
    pub interim_bytes: usize,
    /// How long an exchange has to reach a final head, counted from when it begins.
    ///
    /// Absolute on purpose: an upload arriving a byte at a time must not be able to hold
    /// an exchange open for as long as it keeps trickling, and an upstream that keeps
    /// sending interim answers does not buy itself more time by doing so. A very long
    /// upload needs a profile of its own, which this stage does not have.
    pub final_head: Duration,
    /// How long nothing at all may happen before an exchange is given up on.
    ///
    /// About progress, not about time passing: a round of an exchange waits only when
    /// neither direction can move, so a round that does not finish is nothing moving.
    /// Where a peer is waiting on something legitimate — a client that has not asked for
    /// the next frame yet — no round is outstanding and nothing is counted against it.
    pub idle: Duration,
    /// How long a request that said `Expect: 100-continue` holds its body back, waiting
    /// to be asked for it.
    ///
    /// Short, because it is a wait for nothing useful: an upstream that does not answer
    /// the expectation is one that means to read the body anyway, and the client is
    /// waiting on both of them meanwhile.
    pub continue_wait: Duration,
    /// How many idle connections a worker keeps to any one destination.
    pub idle_per_destination: usize,
    /// How many it keeps in all. Workers do not share, so a pod's total is this times
    /// the number of them.
    pub idle_total: usize,
    /// How long a connection may sit unused before it is dropped rather than trusted.
    pub idle_timeout: Duration,
    /// How long a connection may be reused at all, counted from when it was opened, and
    /// whatever it has been doing meanwhile. An old connection has had more chances to
    /// have quietly stopped working.
    pub max_age: Duration,
    /// How often a worker looks over what it is keeping.
    pub sweep: Duration,
    /// How long opening a connection to an upstream may take before it is given up on.
    pub connect: Duration,
    /// How many exchanges a worker will have in hand at once, counted from before a
    /// connection is looked for until the answer's body has been let go of.
    ///
    /// Without it a worker holds as many sockets, buffers and timers as happen to
    /// arrive. The excess is answered rather than queued: a queue would only move the
    /// same unboundedness somewhere quieter
    /// ([13 §7](../../../../docs/13-http1-upstream.md)).
    pub exchanges: usize,
}

impl Default for H1Limits {
    fn default() -> Self {
        Self {
            head: 64 * 1024,
            fields: codec::MOST_FIELDS,
            chunk_line: 4 * 1024,
            trailers: 16 * 1024,
            trailer_fields: 64,
            interim_heads: 16,
            interim_bytes: 128 * 1024,
            final_head: Duration::from_secs(60),
            idle: Duration::from_secs(30),
            continue_wait: Duration::from_secs(1),
            idle_per_destination: 8,
            idle_total: 256,
            idle_timeout: Duration::from_secs(30),
            max_age: Duration::from_secs(5 * 60),
            sweep: Duration::from_secs(1),
            connect: Duration::from_secs(5),
            exchanges: 1024,
        }
    }
}
