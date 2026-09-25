//! What a worker reads of a QUIC datagram before quiche is handed it, and the connection IDs
//! it issues ([16 §3](../../../docs/16-http3.md)).
//!
//! Pure: no socket, clock or randomness. The keys and the nonces' starting points are
//! passed in; the driver that owns the socket brings them.

// Reached only by its own tests and the fuzz targets until the HTTP/3 driver exists (16,
// step 2). An expectation and not an allowance, so that the day a caller appears the
// compiler says this line has served its purpose.
#![cfg_attr(
    not(any(test, feature = "fuzzing")),
    expect(
        dead_code,
        reason = "reached only by its own tests until the HTTP/3 driver exists"
    )
)]
// What is here is `pub` so that the fuzz targets and benchmarks, which are crates of their
// own, can name it. The module is public only when they are being built, so in an ordinary
// build none of this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod header;
pub mod id;
pub mod token;
