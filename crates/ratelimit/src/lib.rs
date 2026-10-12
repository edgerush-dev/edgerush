//! EdgeRush rate limiting.
//!
//! A pure crate: it decides whether a request is admitted and does nothing else — no
//! sockets, clocks, randomness or async runtime. The time of every decision is passed in,
//! in nanoseconds from any fixed origin, which keeps it deterministic and easy to test.
//!
//! For now it holds only [`reference`](mod@reference): the admission models of docs/06 §3,
//! each written as plainly as it can be, so that what each one promises can be read,
//! tested and compared before any of them is built for the request path.

pub mod reference;
