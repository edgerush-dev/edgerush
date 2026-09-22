//! EdgeRush's own way of serving a client, beside the engine's.
//!
//! What is here is a candidate: hyper's server still takes every connection, and this is
//! adopted only if it passes the gates of [14 §9](../../../docs/14-downstream-server.md).
//! So far it is the pure half of step 2: bytes in, an answer out, and no socket.

// Nothing outside reaches into this yet: hyper's server serves every connection, and
// what is here answers only to its own tests until the connection driver arrives (14 §9,
// step 3). An expectation and not an allowance, so that the day a caller appears the
// compiler says this line has served its purpose.
#![cfg_attr(
    not(any(test, feature = "fuzzing")),
    expect(
        dead_code,
        reason = "reached only by its own tests until the connection driver exists"
    )
)]
// What is here is `pub` so that the fuzz targets, which are a crate of their own, can name
// it. The module is public only when they are being built, so in an ordinary build none of
// this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod h1;
