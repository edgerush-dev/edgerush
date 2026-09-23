//! HTTP/1.0 and HTTP/1.1 from a client.
//!
//! The protocol engine ([`codec`]) is by itself: it is given bytes and says what it made of
//! them. It opens no socket, reads no clock and spawns nothing, so every one of its answers
//! can be checked against a table and every split of its input tried.

pub mod codec;
pub(crate) mod connection;
pub mod continuing;
pub mod date;
pub mod deadlines;
pub mod outbound;
// What a connection's life must have been, given what its client sent: the harness's
// second oracle.
#[cfg(any(test, feature = "fuzzing"))]
pub mod lifecycle;
// The specification as code, for the harness to judge the reader and the driver against.
#[cfg(any(test, feature = "fuzzing"))]
pub mod reference;
pub mod writer;
// The coordinator, writer and commitment tracker composed against a scripted socket.
#[cfg(test)]
mod races;
// Where the codec and hyper's server answer the same bytes differently.
#[cfg(test)]
mod differences;
