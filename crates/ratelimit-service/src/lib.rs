//! EdgeRush's rate-limiting service: pods of the cluster store (`edgerush_cluster`), either
//! mode's, over gRPC, the data planes' streams to them, and the binary that runs one; and the
//! protocol's own service pod by a view of which pods serve (`edgerush_ratelimit::service`),
//! with its client, which the binary no longer runs.
//!
//! - [`cluster`]: the cluster store between pods and data planes, its messages as protobuf.
//! - [`wire`]: the protocol's messages as protobuf, and their conversions.
//! - [`grpc`]: the protocol's gRPC service and its client.
//! - [`pod`]: the protocol's service pod running, on real time.
//! - [`scrape`]: the metrics of either, at `/metrics`.
//! - [`secure`]: mutual TLS, and who may call what.

pub mod cluster;
pub mod grpc;
pub mod pod;
pub mod scrape;
pub mod secure;
pub mod wire;
