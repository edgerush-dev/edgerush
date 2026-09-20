//! HTTP/1.1 to an upstream.
//!
//! The protocol engine ([`codec`]) is by itself: it is given bytes and told of events, and
//! says what it made of them. It opens no socket, reads no clock and spawns nothing, which
//! is what lets every one of its answers be checked against a table and every split of its
//! input be tried. The code that does hold a socket comes later, and keeps that separation.

pub mod codec;
pub mod exchange;

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
        }
    }
}
