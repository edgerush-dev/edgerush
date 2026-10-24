//! EdgeRush rate limiting.
//!
//! A pure crate: it decides whether a request is admitted and does nothing else — no
//! sockets, clocks, randomness or async runtime. The time of every decision is passed in,
//! in nanoseconds from any fixed origin, which keeps it deterministic and easy to test.
//!
//! It holds [`reference`](mod@reference): the admission models of docs/06 §3, each written
//! as plainly as it can be, so that what each one promises can be read, tested and compared
//! before any of them is built for the request path; and [`service`](mod@service): the
//! protocol of the rate-limiting service, a candidate, as two state machines that the
//! simulator and the prototype both run; and [`local`](mod@local): local mode's limiter, each
//! process on what it sees alone.

pub mod local;
pub mod reference;
pub mod service;
