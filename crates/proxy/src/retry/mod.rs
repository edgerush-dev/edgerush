//! Sending a request again when its answer says to ([03 §6](../../../docs/03-data-plane.md)):
//! the budget that bounds it, and the body that lets it be sent twice.

pub(crate) mod budget;
pub(crate) mod replay;
