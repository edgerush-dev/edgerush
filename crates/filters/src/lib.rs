//! EdgeRush's built-in filters: what is done to a request on its way to an upstream and to
//! the response on its way back.
//!
//! A pure crate: filters work on plain `http` types and do no I/O. They are built once per
//! config snapshot, already validated, and applying one never fails.
//!
//! So far: header modification ([`HeaderModifier`]), changes to a path ([`PathModifier`])
//! and redirects ([`Redirect`]).

mod header_modifier;
mod path_modifier;
mod redirect;

pub use header_modifier::{Edit, HeaderModifier, HeaderModifierError, MOST_PER_LIST, RESERVED};
pub use path_modifier::{MOST_BYTES, PathModifier, PathModifierError};
pub use redirect::{Query, Redirect, RedirectError, Requested, Scheme};
