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

// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod balance;
#[cfg(not(feature = "fuzzing"))]
mod balance;
pub mod connections;
mod cookies;
mod drain;
mod forwarding;
mod gathered;
mod grpc;
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
mod h2_stream;
mod health;
// A scripted HTTP/2 peer, for tests here and for the probes in `tests/h2_library.rs`.
#[cfg(test)]
mod h2_peer;
// Two quiche connections joined in memory and a scripted HTTP/3 peer, for the probes in
// `tests/h3_library.rs` and the HTTP/3 tests to come.
#[cfg(test)]
mod h3_peer;
pub mod head;
#[cfg(feature = "fuzzing")]
pub mod hop_by_hop;
#[cfg(not(feature = "fuzzing"))]
mod hop_by_hop;
pub mod host;
mod interim;
// What keeps a request's body for later, driven from frames given: for the benchmarks.
#[cfg(feature = "fuzzing")]
pub mod kept;
// Whether a failed test waited on the code or on a machine that stood it still.
#[cfg(test)]
mod stall;
// Private, save when the fuzz targets are being built.
#[cfg(feature = "fuzzing")]
pub mod l4;
#[cfg(not(feature = "fuzzing"))]
mod l4;
mod linger;
// Private, save when the fuzz targets and benchmarks are being built, as `raw` is.
#[cfg(feature = "fuzzing")]
pub mod map_head;
#[cfg(not(feature = "fuzzing"))]
mod map_head;
mod metrics;
mod mirror;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod places;
#[cfg(not(feature = "fuzzing"))]
mod places;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod proxy_protocol;
#[cfg(not(feature = "fuzzing"))]
mod proxy_protocol;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod quic;
#[cfg(not(feature = "fuzzing"))]
mod quic;
mod random;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod received;
#[cfg(not(feature = "fuzzing"))]
mod received;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod raw;
#[cfg(not(feature = "fuzzing"))]
mod raw;
mod request;
mod request_body;
mod retry;
mod routed;
mod runs;
mod scrape;
mod serve;
pub mod share;
// Private, save when the fuzz targets and benchmarks are being built.
#[cfg(feature = "fuzzing")]
pub mod slots;
#[cfg(not(feature = "fuzzing"))]
mod slots;
// Private, save when the fuzz targets are being built, for the blocks `upstream` lends.
#[cfg(feature = "fuzzing")]
pub mod storage;
#[cfg(not(feature = "fuzzing"))]
mod storage;
// Private, save when the fuzz targets are being built, for the timers an exchange keeps
// its deadlines in.
#[cfg(feature = "fuzzing")]
pub mod timers;
#[cfg(not(feature = "fuzzing"))]
mod timers;
mod tls;
mod tunnel;
// Private, save when the fuzz targets are being built: they are a crate of their own and
// cannot otherwise reach what they drive.
#[cfg(feature = "fuzzing")]
pub mod upstream;
#[cfg(not(feature = "fuzzing"))]
mod upstream;
// Private, save when the fuzz targets are being built, for its frame reader.
#[cfg(feature = "fuzzing")]
pub mod websocket;
#[cfg(not(feature = "fuzzing"))]
mod websocket;

pub use forwarding::Client;
pub use hop_by_hop::ConnectionError;
pub use host::HostError;
pub use metrics::AcceptPause;
pub use request::{Copied, Decision, Forward, Mirroring, Opening, Redirected, Rejection, decide};
pub use serve::{Forwarding, Proxy, ProxyError, Worker};
pub use tls::TlsError;
// What a worker will not go beyond. There is no configuration for these; what there
// is, is one value per worker, which whoever makes the workers hands them.
pub use upstream::h1::H1Limits;
