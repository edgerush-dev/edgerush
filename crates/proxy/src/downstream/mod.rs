//! EdgeRush's own way of serving a client.
//!
//! Every connection goes through the detector: HTTP/1 to EdgeRush's own server
//! ([14](../../../docs/14-downstream-server.md)), HTTP/2 to its own server over h2
//! ([15](../../../docs/15-http2-and-grpc.md)). The HTTP/1 pieces are pure, bytes in
//! and an answer out, with no socket, except the connection driver that holds them.

// The HTTP/1 pieces answer only to their own tests until the connection driver arrives
// (14 §9, step 3). An expectation and not an allowance, so that the day a caller appears
// the compiler says this line has served its purpose.
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

pub mod detect;
pub mod h1;
pub(crate) mod h2;
pub mod h3;
