//! EdgeRush's way to an upstream: its own HTTP/1 client, its pool, and what decides which
//! connections a request may share ([13](../../../docs/13-http1-upstream.md)). What it does,
//! measured on raw sockets, is `crates/proxy/tests/wire.rs`.

// What is here is `pub` so that the fuzz targets, which are a crate of their own, can name
// it. The module is public only when they are being built, so in an ordinary build none of
// this is API — which is why the lint that wants a `pub` item to be reachable is wrong
// here and nowhere else.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod auth;
// Public when the benchmarks are built, for a worker's state brought up to a new config.
#[cfg(feature = "fuzzing")]
pub mod balancing;
#[cfg(not(feature = "fuzzing"))]
pub(crate) mod balancing;
pub mod destination;
pub mod dial;
pub mod h1;
pub(crate) mod secure;
// Crate-private even when the fuzz targets are built: nothing of it is theirs.
pub(crate) mod h2;
