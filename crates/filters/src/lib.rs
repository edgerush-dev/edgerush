//! EdgeRush's built-in filters: what is done to a request on its way to an upstream and to
//! the response on its way back.
//!
//! A pure crate: filters work on plain `http` types and do no I/O. They are built once per
//! config snapshot, already validated, and applying one never fails.
//!
//! So far: header modification ([`HeaderModifier`]), changes to a path ([`PathModifier`]),
//! redirects ([`Redirect`]), rewrites ([`UrlRewrite`]), what an upstream is told of the
//! client ([`forwarding`]), and a request's ID ([`request_id`]).

pub mod forwarding;
mod header_modifier;
mod path_modifier;
mod redirect;
pub mod request_id;
mod rewrite;

pub use header_modifier::{Edit, HeaderModifier, HeaderModifierError, MOST_PER_LIST, RESERVED};
pub use path_modifier::{MOST_BYTES, PathModifier, PathModifierError};
pub use redirect::{
    Query, Redirect, RedirectError, Requested, STATUSES as REDIRECT_STATUSES, Scheme,
};
pub use rewrite::{RewriteError, UrlRewrite};
