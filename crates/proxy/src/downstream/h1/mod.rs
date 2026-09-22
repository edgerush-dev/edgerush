//! HTTP/1.0 and HTTP/1.1 from a client.
//!
//! The protocol engine ([`codec`]) is by itself: it is given bytes and says what it made of
//! them. It opens no socket, reads no clock and spawns nothing, so every one of its answers
//! can be checked against a table and every split of its input tried.

pub mod codec;
pub mod continuing;
pub mod date;
pub mod outbound;
pub mod writer;
// Where the codec and hyper's server answer the same bytes differently.
#[cfg(test)]
mod differences;
