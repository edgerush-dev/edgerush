//! EdgeRush metrics: what is counted on the request path, and how it is read.
//!
//! A pure crate: no I/O, no clock — durations are measured by whoever has one and passed
//! in — and no dependencies.
//!
//! Counting must cost a request next to nothing and must never make one worker wait for
//! another. So every group of counters exists once per shard ([`Sharded`]), every thread
//! counts in a shard of its own, and the shards are only added up when somebody asks
//! ([`Sharded::sum`]), which is rare and may be slow. What is asked for is written in the
//! Prometheus text format ([`Exposition`]).

mod exposition;
mod histogram;
mod sharded;
mod value;

pub use exposition::{Exposition, Kind};
pub use histogram::Histogram;
pub use sharded::Sharded;
pub use value::{Counter, Gauge};
