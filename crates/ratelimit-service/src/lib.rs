//! EdgeRush's rate-limiting service: pods of the cluster store (`edgerush_cluster`), either
//! mode's, over gRPC, the data planes' streams to them, and the binary that runs one.
//!
//! - [`cluster`]: the cluster store between pods and data planes, its messages as protobuf.
//! - [`service_file`]: what a pod runs as, from its config file.
//! - [`wire`]: an asynchronous entry's contract and report as protobuf, and their conversions.
//! - [`grpc`]: what the store's calls share: the codec, the longest message, a pod's client.
//! - [`scrape`]: a pod's metrics at `/metrics`, and its readiness at `/ready`.
//! - [`secure`]: mutual TLS, and who may call what.

pub mod cluster;
pub mod grpc;
pub mod scrape;
pub mod secure;
pub mod service_file;
pub mod wire;
