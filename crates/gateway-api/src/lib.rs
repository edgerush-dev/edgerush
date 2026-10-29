//! The Gateway API kinds EdgeRush follows ([04 §1](../../../docs/04-control-plane.md)), as
//! Rust types written by `edgerush-crdgen` from the CRDs of a release's standard channel,
//! kept in `crds/` ([02 §7](../../../docs/02-architecture.md)). Each module is one kind's,
//! at the version EdgeRush reads, `v1`.
//!
//! An enumerated string is a `String` and a field the types lack is passed over, so that an
//! object of a newer release or of the experimental channel is read as well, for
//! translation to judge what it does not know ([05 §6](../../../docs/05-operator.md)).
//!
//! Types alone: no I/O. The modules are written, never edited: `crds/` is fetched with
//! `fetch.sh` and the modules written again with
//! `EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-gateway-api --test generated`, which
//! otherwise fails when they are not what the CRDs make.

// The CRDs' own prose continues list items without indenting them.
#[allow(clippy::doc_lazy_continuation)]
pub mod gateway;
#[allow(clippy::doc_lazy_continuation)]
pub mod gateway_class;
#[allow(clippy::doc_lazy_continuation)]
pub mod http_route;
#[allow(clippy::doc_lazy_continuation)]
pub mod reference_grant;
