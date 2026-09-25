//! EdgeRush's HTTP/2 client: requests to an upstream that speaks HTTP/2, many at once on
//! each connection, over `h2` used directly ([15](../../../../docs/15-http2-and-grpc.md)).
//! So far, the head a request is sent with and the pool that decides which connection
//! its stream goes on.

pub mod head;
pub mod pool;
