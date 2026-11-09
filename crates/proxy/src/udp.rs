//! What a worker does with a `udp` listener's datagrams
//! ([24](../../../docs/24-udp-proxying.md)).

// Public only for the benchmark of the session table.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod sessions;
