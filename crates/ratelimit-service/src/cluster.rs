//! The cluster store (`edgerush_cluster`) between service pods, which both modes run on in
//! place of the protocol's own membership and copies (`membership-design.md`).
//!
//! - [`wire`]: the store's messages as protobuf, a mode's value with them, and the pieces a
//!   message too long for one is sent in.
//! - [`pod`]: a pod of the store on sockets, and its gRPC server.
//! - [`link`]: what a pod sends to one peer, and how it dials it.
//! - [`mode`]: what a pod on sockets needs of the mode it runs, for both modes.
//! - [`settings`]: what a pod runs the store with, from its file, and its member id.
//! - [`forming`]: forming the cluster before Kubernetes, as the bench's example program does.
//! - [`plane`]: the data planes' side: their keys, the table they learn, whom a key is asked
//!   of, and what goes up and comes down their stream.

pub mod forming;
pub mod link;
pub mod mode;
pub mod plane;
pub mod pod;
pub mod settings;
pub mod wire;
