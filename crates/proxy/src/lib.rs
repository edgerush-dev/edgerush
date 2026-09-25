//! The EdgeRush data plane.
//!
//! At its centre is the request core ([`decide`]): from the head of a request to where it
//! goes, on plain `http` types, whatever protocol the request came in by. Around it — and
//! nowhere else in EdgeRush — is the code that touches the HTTP engine.
//!
//! So far: the request core; [`Proxy`], one for the process, which holds the config every
//! worker serves — taking a new one while they run, without dropping a request — and the
//! counters they all add to, which it serves to a scraper; and [`Worker`], one for each
//! thread that serves, which forwards to upstreams over HTTP/1.1 on connections of its
//! own and never leaves the thread it was made on.

mod cookies;
mod drain;
// Private, save when the fuzz targets are being built, as `upstream` is below.
#[cfg(feature = "fuzzing")]
pub mod downstream;
#[cfg(not(feature = "fuzzing"))]
mod downstream;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod fields;
#[cfg(not(feature = "fuzzing"))]
mod fields;
mod h1;
// A scripted HTTP/2 peer, for tests here and for the probes in `tests/h2_library.rs`.
#[cfg(test)]
mod h2_peer;
pub mod head;
#[cfg(feature = "fuzzing")]
pub mod hop_by_hop;
#[cfg(not(feature = "fuzzing"))]
mod hop_by_hop;
pub mod host;
mod interim;
mod linger;
mod metrics;
mod random;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod raw;
#[cfg(not(feature = "fuzzing"))]
mod raw;
mod request;
mod request_body;
mod scrape;
mod serve;
// Private, save when the fuzz targets are being built, for the blocks `upstream` lends.
#[cfg(feature = "fuzzing")]
pub mod storage;
#[cfg(not(feature = "fuzzing"))]
mod storage;
mod tls;
// Private, save when the fuzz targets are being built: they are a crate of their own and
// cannot otherwise reach what they drive.
#[cfg(feature = "fuzzing")]
pub mod upstream;
#[cfg(not(feature = "fuzzing"))]
mod upstream;

pub use hop_by_hop::ConnectionError;
pub use host::HostError;
pub use request::{Forward, Rejection, decide};
pub use serve::{Proxy, ProxyError, Worker};
pub use tls::TlsError;
// What a worker will not go beyond. There is no configuration for these; what there
// is, is one value per worker, which whoever makes the workers hands them.
pub use upstream::h1::H1Limits;
