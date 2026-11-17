//! The DataPlane the operator generates for a Gateway written alone, of a class whose
//! `PlatformDefaults` entry asks for one: Gateway-first
//! ([05 §3, §4](../../../docs/05-operator.md)). A pure function of the Gateway, the entry
//! and the class's ControlPlane; no I/O.
//!
//! It lives in the Gateway's namespace, is named `<gateway>-<class>` ([`data_plane_name`]),
//! names its ControlPlane by namespace and its Gateway by `gatewayRef`, and is owned by the
//! Gateway, so it goes with it. Its spec is the entry's fields and nothing else. It carries
//! GEP-1762's labels and the Gateway's `infrastructure` labels and annotations, which
//! [`for_data_plane`](crate::objects::for_data_plane) puts on everything made for it.

use std::collections::BTreeMap;

use edgerush_gateway_api::gateway::Gateway;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

use crate::control_plane::ControlPlane;
use crate::data_plane::{DataPlane, DataPlaneSpec};
use crate::objects::{MANAGED_BY, MANAGED_BY_LABEL, Made};
use crate::platform_defaults::PlatformDefaultsGatewayClassesDataPlane;

#[cfg(test)]
mod tests;

/// The longest name a DataPlane may have: its Service, `edgerush-proxy-<name>`, is then a
/// DNS label (05 §1).
pub const LONGEST: usize = 48;

/// How much of `<gateway>-<class>` a shortened name keeps: what fits beside `-` and the 8
/// hex digits of the hash.
const KEPT: usize = LONGEST - 9;

/// GEP-1762's label of the Gateway's name, on everything made for it: an annotation of the
/// same key where the name is longer than a label's value may be.
pub const GATEWAY_NAME: &str = "gateway.networking.k8s.io/gateway-name";

/// GEP-1762's label of the Gateway's class, beside [`GATEWAY_NAME`].
pub const GATEWAY_CLASS_NAME: &str = "gateway.networking.k8s.io/gateway-class-name";

/// The longest value a label may have, as Kubernetes has it.
const LABEL_VALUE: usize = 63;

/// Why no DataPlane could be made: each a Gateway, ControlPlane or entry that should not
/// have reached here.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The Gateway or ControlPlane lacks what the API server gives every object it stores.
    #[error("the {kind} has no {what}")]
    Missing {
        /// Its kind.
        kind: &'static str,
        /// Its name, namespace or UID.
        what: &'static str,
    },
    /// The entry does not read as a DataPlane's fields, which its schema is a copy of.
    #[error("the PlatformDefaults entry is not a DataPlane's fields: {0}")]
    Entry(String),
}

/// The name of the DataPlane generated for Gateway `gateway` of class `class`:
/// `<gateway>-<class>`, GEP-1762's pattern, as NGINX Gateway Fabric and Istio name theirs.
/// A Gateway's name is a DNS subdomain of up to 253 characters, a DataPlane's a DNS label of
/// at most [`LONGEST`]: where `<gateway>-<class>` is not one, its first 39 characters, dots
/// made `-`, then `-` and 8 hex digits of a hash of the whole, as NGINX Gateway Fabric
/// shortens. The hash is FNV-1a, the same in every build: a name that changed with an
/// upgrade would be another DataPlane, the old deleted and its address lost.
#[must_use]
pub fn data_plane_name(gateway: &str, class: &str) -> String {
    let whole = format!("{gateway}-{class}");
    if whole.len() <= LONGEST && !whole.contains('.') {
        return whole;
    }
    let kept: String = whole
        .chars()
        .take(KEPT)
        .map(|c| if c == '.' { '-' } else { c })
        .collect();
    format!("{kept}-{:08x}", fnv1a(whole.as_bytes()))
}

/// FNV-1a, 32 bits: 8 hex digits, the same on every machine and in every build.
fn fnv1a(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0x811c_9dc5, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
    })
}

/// The DataPlane generated for `gateway`, from `entry`, its class's, for `control_plane`,
/// the ControlPlane whose class it is.
///
/// # Errors
///
/// [`Error::Missing`] for a Gateway without a name, namespace or UID, or a ControlPlane
/// without a name or namespace; [`Error::Entry`] for an entry that is not a DataPlane's
/// fields.
pub fn data_plane(
    gateway: &Gateway,
    entry: &PlatformDefaultsGatewayClassesDataPlane,
    control_plane: &ControlPlane,
) -> Result<DataPlane, Error> {
    let missing = |kind, what| Error::Missing { kind, what };
    let metadata = &gateway.metadata;
    let name = metadata
        .name
        .as_deref()
        .ok_or_else(|| missing("Gateway", "name"))?;
    let namespace = metadata
        .namespace
        .as_deref()
        .ok_or_else(|| missing("Gateway", "namespace"))?;
    let uid = metadata
        .uid
        .as_deref()
        .ok_or_else(|| missing("Gateway", "UID"))?;
    let of = control_plane
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| missing("ControlPlane", "name"))?;
    let of_namespace = control_plane
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| missing("ControlPlane", "namespace"))?;
    let class = &gateway.spec.gateway_class_name;

    // The entry's fields are a DataPlane's, of the same schema (05 §4); the rest is the
    // class's and the Gateway's.
    let mut fields = serde_json::to_value(entry).map_err(|e| Error::Entry(e.to_string()))?;
    if let Some(fields) = fields.as_object_mut() {
        fields.insert(
            "controlPlaneRef".to_owned(),
            serde_json::json!({ "name": of, "namespace": of_namespace }),
        );
        fields.insert("gatewayRef".to_owned(), serde_json::json!({ "name": name }));
    }
    let spec: DataPlaneSpec =
        serde_json::from_value(fields).map_err(|e| Error::Entry(e.to_string()))?;

    // The Gateway's own first, then GEP-1762's and EdgeRush's, which win.
    let infrastructure = gateway.spec.infrastructure.as_ref();
    let mut labels = infrastructure
        .and_then(|infrastructure| infrastructure.labels.clone())
        .unwrap_or_default();
    let mut annotations = infrastructure
        .and_then(|infrastructure| infrastructure.annotations.clone())
        .unwrap_or_default();
    labels.insert(GATEWAY_CLASS_NAME.to_owned(), class.clone());
    if name.len() <= LABEL_VALUE {
        labels.insert(GATEWAY_NAME.to_owned(), name.to_owned());
    } else {
        // No label can hold it: as NGINX Gateway Fabric writes it (05 §3).
        labels.remove(GATEWAY_NAME);
        annotations.insert(GATEWAY_NAME.to_owned(), name.to_owned());
    }
    labels.insert(MANAGED_BY_LABEL.to_owned(), MANAGED_BY.to_owned());

    Ok(DataPlane {
        metadata: ObjectMeta {
            name: Some(data_plane_name(name, class)),
            namespace: Some(namespace.to_owned()),
            labels: Some(labels),
            annotations: (!annotations.is_empty()).then_some(annotations),
            owner_references: Some(vec![OwnerReference {
                api_version: "gateway.networking.k8s.io/v1".to_owned(),
                kind: "Gateway".to_owned(),
                name: name.to_owned(),
                uid: uid.to_owned(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        spec,
        status: None,
    })
}

/// `data_plane`, a generated one, as the operator applies it: with its `apiVersion` and
/// `kind`, and no status.
#[must_use]
pub fn applied(data_plane: &DataPlane) -> Made<&DataPlaneSpec> {
    Made {
        api_version: "gateway.edgerush.dev/v1alpha1".to_owned(),
        kind: "DataPlane".to_owned(),
        metadata: data_plane.metadata.clone(),
        spec: &data_plane.spec,
    }
}

/// What everything made for `data_plane` carries beside EdgeRush's own: for a generated
/// one, its labels and annotations, GEP-1762's and its Gateway's `infrastructure` among them
/// ([`data_plane`]); for any other, nothing, as a DataPlane written by hand keeps its own.
#[must_use]
pub(crate) fn carried(
    data_plane: &DataPlane,
) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
    if data_plane.spec.gateway_ref.is_none() {
        return (BTreeMap::new(), BTreeMap::new());
    }
    let metadata = &data_plane.metadata;
    (
        metadata.labels.clone().unwrap_or_default(),
        metadata.annotations.clone().unwrap_or_default(),
    )
}
