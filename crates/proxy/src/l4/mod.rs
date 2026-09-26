//! Connections a listener does not terminate: TCP proxied by the listener alone, TLS by the
//! name its ClientHello asks for ([17](../../../docs/17-tcp-and-tls-passthrough.md)).
//!
//! Pure so far: what a TLS passthrough listener reads of a connection before it knows where
//! the connection goes.

// Reached only by its own tests and the fuzz targets until the tunnel exists (17, step 3).
// An expectation and not an allowance, so that the day a caller appears the compiler says
// this line has served its purpose.
#![cfg_attr(
    not(any(test, feature = "fuzzing")),
    expect(
        dead_code,
        reason = "reached only by its own tests until the tunnel exists"
    )
)]
// What is here is `pub` so that the fuzz targets, a crate of their own, can name it. The
// module is public only when they are being built, so in an ordinary build none of this is
// API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod hello;
