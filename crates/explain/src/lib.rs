//! `edgerush explain` and `edgerush test` without their command lines
//! ([22](../../../docs/22-explain-and-test.md)): a request as they state it ([`asked`]), and
//! where the data plane would send it and why ([`Snapshot::explain`]). Nothing here reads a
//! file or writes to a stream; the binary does that.

pub mod asked;
mod explained;

pub use asked::{Asked, Invalid};
pub use explained::{Explained, STAND_IN_ID, Snapshot, Unexplained, protocol_name};
