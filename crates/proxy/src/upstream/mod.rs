//! EdgeRush's way to an upstream: its own HTTP/1 client, its pool, and what decides which
//! connections a request may share ([13](../../../docs/13-http1-upstream.md)). What it does,
//! measured on raw sockets, is `crates/proxy/tests/wire.rs`.

// Some items here are reached only by the tests and the fuzz targets. An expectation and
// not an allowance, so that once each is kept to the builds that use it the compiler says
// this has served its purpose.
#![cfg_attr(
    not(feature = "fuzzing"),
    expect(
        dead_code,
        reason = "some items are reached only by tests and the fuzz targets"
    )
)]
// What is here is `pub` so that the fuzz targets, which are a crate of their own, can name
// it. The module is public only when they are being built, so in an ordinary build none of
// this is API — which is why the lint that wants a `pub` item to be reachable is wrong
// here and nowhere else.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub(crate) mod auth;
pub mod destination;
pub mod h1;
