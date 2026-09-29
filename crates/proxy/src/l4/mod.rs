//! Connections a listener does not terminate: TCP proxied by the listener alone, TLS by the
//! name its ClientHello asks for ([17](../../../docs/17-tcp-and-tls-passthrough.md)).
//!
//! [`hello`] is pure: what a TLS passthrough listener reads of a connection before it knows
//! where the connection goes. [`crate::tunnel`] carries the connection once it does.

// What is here is `pub` so that the fuzz targets, a crate of their own, can name it. The
// module is public only when they are being built, so in an ordinary build none of this is
// API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod hello;
