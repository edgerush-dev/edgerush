//! EdgeRush's own kinds, `ControlPlane` and `DataPlane`, `gateway.edgerush.dev/v1alpha1`
//! ([05 §1](../../../docs/05-operator.md)), and what the install ships beside them.
//!
//! `install/` holds what is applied before the operator starts: the two CRDs; the four
//! ClusterRoles, the only rights the operator ever grants, by `bind` (05 §2); and the
//! admission policy that refuses `externalIPs` on a Service a control plane writes. The
//! CRDs are written by hand, and the types here are written from them by `edgerush-crdgen`,
//! as Gateway API's are ([02 §7](../../../docs/02-architecture.md)); the DataPlane's
//! `spec.gateway` holds the Gateway's own fields, copied from Gateway API's CRD. The
//! modules are written, never edited, with
//! `EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test generated`, which otherwise
//! fails when they, or the copy, are not what the CRDs make.
//!
//! A resource that cannot be served is refused as it is applied, by its CRD's schema and
//! CEL rules (05 §1): the types read what the API server admitted.
//!
//! [`objects`] is what the operator makes for each resource: a pure function, no I/O.

// The CRDs' own prose continues list items without indenting them.
#[allow(clippy::doc_lazy_continuation)]
pub mod control_plane;
#[allow(clippy::doc_lazy_continuation)]
pub mod data_plane;
pub mod objects;
mod workers;
