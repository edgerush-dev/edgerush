//! EdgeRush's own HTTP/2 server, over the locked `h2` used directly
//! ([15](../../../../docs/15-http2-and-grpc.md)): the connection driver, a request's body
//! as h2 received it, and an answer sent back within the capacity h2 grants.

pub(crate) mod body;
pub(crate) mod connection;
pub(crate) mod writer;

#[cfg(test)]
mod testing;
#[cfg(test)]
mod through;
