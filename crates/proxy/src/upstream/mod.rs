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
#![expect(
    dead_code,
    reason = "reached only by its own tests until the exchange that drives a socket exists"
)]

pub(crate) mod h1;
