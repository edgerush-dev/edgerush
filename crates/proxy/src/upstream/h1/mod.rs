//! HTTP/1.1 to an upstream.
//!
//! The protocol engine ([`codec`]) is by itself: it is given bytes and told of events, and
//! says what it made of them. It opens no socket, reads no clock and spawns nothing, which
//! is what lets every one of its answers be checked against a table and every split of its
//! input be tried. The code that does hold a socket comes later, and keeps that separation.

pub(crate) mod codec;

/// What a worker will not go beyond, whatever an upstream sends. Conservative numbers for
/// development, not settings anybody configures and not the pool's policy, which waits for
/// the benchmark to be run again with TLS ([13 §7](../../../docs/13-http1-upstream.md)).
///
/// A gain that came of loosening these is not a gain, so a benchmark that changes them
/// says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct H1Limits {
    /// The most a response head may come to, status line and fields together.
    pub(crate) head: usize,
    /// The most fields a head may carry. No more than [`codec::MOST_FIELDS`] is read,
    /// whatever this says.
    pub(crate) fields: usize,
}

impl Default for H1Limits {
    fn default() -> Self {
        Self {
            head: 64 * 1024,
            fields: codec::MOST_FIELDS,
        }
    }
}
