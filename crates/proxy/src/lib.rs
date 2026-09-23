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
// Private, save when the fuzz targets are being built, as `upstream` is below.
#[cfg(feature = "fuzzing")]
pub mod downstream;
#[cfg(not(feature = "fuzzing"))]
mod downstream;
// Private, save when the fuzz targets are being built. Nothing reads a head through it yet:
// step 4 proves it reads as the header map does before the parser builds it (14 §6).
#[cfg(feature = "fuzzing")]
pub mod fields;
#[cfg(not(feature = "fuzzing"))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired into the parser by the next change")
)]
mod fields;
mod h1;
mod hop_by_hop;
pub mod host;
mod interim;
mod linger;
mod metrics;
mod random;
mod request;
mod request_body;
mod scrape;
mod serve;
// Private, save when the fuzz targets are being built, for the blocks `upstream` lends.
#[cfg(feature = "fuzzing")]
pub mod storage;
#[cfg(not(feature = "fuzzing"))]
mod storage;
// Private, save when the fuzz targets are being built: they are a crate of their own and
// cannot otherwise reach what they drive.
#[cfg(feature = "fuzzing")]
pub mod upstream;
#[cfg(not(feature = "fuzzing"))]
mod upstream;

pub use hop_by_hop::ConnectionError;
pub use host::HostError;
pub use request::{Forward, Rejection, decide};
pub use serve::{Downstream, Proxy, ProxyError, Upstream, Worker};
// What a worker will not go beyond. There is no configuration for these; what there
// is, is one value per worker, which whoever makes the workers hands them.
pub use upstream::h1::H1Limits;
