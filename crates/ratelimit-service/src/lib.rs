//! EdgeRush's rate-limiting service, a prototype of the candidate in the rate-limiting
//! working note: service pods running the protocol of `edgerush_ratelimit::service` over
//! gRPC, and the client the data planes use to reach them.
//!
//! - [`wire`]: the messages as protobuf, and their conversions.
//! - [`cluster`]: the cluster store's messages as protobuf (`edgerush_cluster`), which the
//!   service is moving onto.
//! - [`grpc`]: the gRPC service and its client.
//! - [`pod`]: a service pod running, on real time.
//! - [`scrape`]: its metrics, at `/metrics`.
//! - [`secure`]: mutual TLS, and who may call what.

pub mod cluster;
pub mod grpc;
pub mod pod;
pub mod scrape;
pub mod secure;
pub mod wire;
