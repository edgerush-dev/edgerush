//! The cluster store (`edgerush_cluster`) between service pods, which the service is moving
//! onto in place of its own membership and copies (`membership-design.md`).
//!
//! - [`wire`]: the store's messages as protobuf, a mode's value with them, and the pieces a
//!   message too long for one is sent in.
//! - [`pod`]: a pod of the store on sockets, and its gRPC server.
//! - [`link`]: what a pod sends to one peer, and how it dials it.
//! - [`settings`]: what a pod runs the store with, from its file, and its member id.

pub mod link;
pub mod pod;
pub mod settings;
pub mod wire;
