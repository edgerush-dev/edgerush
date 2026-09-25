//! EdgeRush's way to an upstream: its own HTTP/1 client, its pool, and what decides which
//! connections a request may share ([13](../../../docs/13-http1-upstream.md)). What it does,
//! measured on raw sockets, is `crates/proxy/tests/wire.rs`.

// What is here is `pub` so that the fuzz targets, which are a crate of their own, can name
// it. The module is public only when they are being built, so in an ordinary build none of
// this is API — which is why the lint that wants a `pub` item to be reachable is wrong
// here and nowhere else.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod auth;
pub mod destination;
pub mod h1;
// Not yet used by the data plane: the multiplexed client that sends by it is 15 step 6.
#[cfg_attr(
    not(any(test, feature = "fuzzing")),
    expect(
        dead_code,
        reason = "sent by the HTTP/2 client of 15 step 6, still to come"
    )
)]
pub mod h2;
