//! EdgeRush's rate-limiting service, a prototype of the candidate in the rate-limiting
//! working note: service pods running the protocol of `edgerush_ratelimit::service` over
//! gRPC, and the client the data planes use to reach them.
//!
//! - [`wire`]: the messages as protobuf, and their conversions.
//! - [`grpc`]: the gRPC service and its client.
//! - [`pod`]: a service pod running, on real time.
//! - [`scrape`]: its metrics, at `/metrics`.

pub mod grpc;
pub mod pod;
pub mod scrape;
pub mod wire;
