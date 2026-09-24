//! EdgeRush's own HTTP/2 serving, over the locked `h2` used directly
//! ([15](../../../../docs/15-http2-and-grpc.md)): a request's body as h2 received it, and
//! an answer sent back within the capacity h2 grants. The connection driver that joins
//! them to the request core is 15 step 2; until it exists these answer only to their own
//! tests, as `downstream`'s expectation of dead code says.

pub(crate) mod body;
pub(crate) mod connection;
pub(crate) mod writer;

#[cfg(test)]
mod testing;
#[cfg(test)]
mod through;
