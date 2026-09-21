//! EdgeRush's own way to an upstream, beside the engine's.
//!
//! What is here is a candidate: hyper's client still carries every request, and this is
//! adopted only if it keeps to what that client does and a worthwhile gain survives being
//! measured on the finished thing ([13](../../../docs/13-http1-upstream.md)). What the
//! current path does, measured on raw sockets, is `crates/proxy/tests/wire.rs`.

// Nothing outside reaches into this yet: the engine's client still carries every request,
// and what is here answers only to its own tests until the code that drives a socket
// arrives (13 §8, step 3). An expectation and not an allowance, so that the day a caller
// appears the compiler says this line has served its purpose.
#![cfg_attr(
    not(feature = "fuzzing"),
    expect(
        dead_code,
        reason = "reached only by its own tests until the exchange that drives a socket exists"
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
