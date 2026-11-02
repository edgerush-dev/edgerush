//! The cluster store (`edgerush_cluster`) between service pods, which the service is moving
//! onto in place of its own membership and copies (`membership-design.md`).
//!
//! - [`wire`]: the store's messages as protobuf, a mode's value with them, and the pieces a
//!   message too long for one is sent in.

pub mod wire;
