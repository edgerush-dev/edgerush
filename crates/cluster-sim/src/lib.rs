//! A deterministic, seeded simulation of the cluster store's pods (`membership-design.md` §5).
//!
//! One [`Sim`] holds the pods, each a mode's pod over its store with a clock of its own — the
//! synchronous mode's ([`World`]) or the asynchronous mode's ([`Asynchronous`]) — a network
//! whose every link can be delayed, made lossy or cut, one way or both, and clients standing
//! for data planes, each sending requests at a steady rate and following the table as pods
//! tell it. Everything happens in a queue of
//! events ordered by time; the only randomness is a seeded generator, so a run is the same
//! every time for its seed, and a failure replays exactly.
//!
//! While it runs it checks, after every event, what must never happen — two leaders in one
//! term, two tables of one number — and measures what is allowed within bounds: how long two
//! pods both serve one shard, how many requests a client is refused, and how many admissions
//! the store loses track of.

pub mod client;
pub mod oracle;
pub mod rng;
pub mod scenario;
pub mod world;

pub use client::Outcome;
pub use rng::Rng;
pub use world::{Asynchronous, Link, Mode, Plane, Sim, Stats, Synchronous, World};
