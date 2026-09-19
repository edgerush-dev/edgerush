//! The EdgeRush data plane.
//!
//! At its centre is the request core ([`decide`]): from the head of a request to where it
//! goes, on plain `http` types, whatever protocol the request came in by. Around it — and
//! nowhere else in EdgeRush — is the code that touches the HTTP engine.
//!
//! So far: the request core, and [`Proxy`], which serves listeners and forwards to
//! upstreams over HTTP/1.1.

mod hop_by_hop;
pub mod host;
mod random;
mod request;
mod serve;

pub use hop_by_hop::ConnectionError;
pub use host::HostError;
pub use request::{Forward, Rejection, decide};
pub use serve::{Proxy, ProxyError};
