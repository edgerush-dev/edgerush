//! gRPC's semantics at the gateway ([15 §6](../../../docs/15-http2-and-grpc.md)): payloads
//! stay opaque, but a gRPC call's deadline, and the status the gateway answers one with,
//! are gRPC's own. Kept apart from the HTTP/2 adapters, which know nothing of gRPC.

pub(crate) mod answer;
pub(crate) mod call;
pub(crate) mod status;
pub(crate) mod timeout;
