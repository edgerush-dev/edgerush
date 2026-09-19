//! EdgeRush's built-in filters: what is done to a request on its way to an upstream and to
//! the response on its way back.
//!
//! A pure crate: filters work on plain `http` types and do no I/O. They are built once per
//! config snapshot, already validated, and applying one never fails.
//!
//! So far: header modification ([`HeaderModifier`]).

mod header_modifier;

pub use header_modifier::{HeaderModifier, HeaderModifierError};
