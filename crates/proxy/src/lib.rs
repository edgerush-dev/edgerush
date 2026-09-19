//! The EdgeRush data plane.
//!
//! At its centre is the request core ([`decide`]): from the head of a request to where it
//! goes, on plain `http` types, whatever protocol the request came in by. Around it — and
//! nowhere else in EdgeRush — is the code that touches the HTTP engine.
//!
//! So far: the request core.

pub mod host;
mod request;

pub use host::HostError;
pub use request::{Forward, Rejection, decide};
