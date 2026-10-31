//! `ReferenceGrant`, `gateway.networking.k8s.io/v1`, as Gateway API v1.7.0 defines it in its standard
//! channel. Written by `edgerush-crdgen` from `crds/gateway.networking.k8s.io_referencegrants.yaml`: not to be edited (the
//! crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::{Deserialize, Serialize};

/// ReferenceGrant identifies kinds of resources in other namespaces that are
/// trusted to reference the specified kinds of resources in the same namespace
/// as the policy.
///
/// Each ReferenceGrant can be used to represent a unique trust relationship.
/// Additional Reference Grants can be used to add to the set of trusted
/// sources of inbound references for the namespace they are defined within.
///
/// All cross-namespace references in Gateway API (with the exception of cross-namespace
/// Gateway-route attachment) require a ReferenceGrant.
///
/// ReferenceGrant is a form of runtime verification allowing users to assert
/// which cross-namespace object references are permitted. Implementations that
/// support ReferenceGrant MUST NOT permit cross-namespace references which have
/// no grant, and MUST respond to the removal of a grant by revoking the access
/// that the grant allowed.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReferenceGrant {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec defines the desired state of ReferenceGrant.
    #[serde(default)]
    pub spec: ReferenceGrantSpec,
}

/// Spec defines the desired state of ReferenceGrant.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ReferenceGrantSpec {
    /// From describes the trusted namespaces and kinds that can reference the
    /// resources described in "To". Each entry in this list MUST be considered
    /// to be an additional place that references can be valid from, or to put
    /// this another way, entries MUST be combined using OR.
    ///
    /// Support: Core
    #[serde(default)]
    pub from: Vec<ReferenceGrantFrom>,
    /// To describes the resources that may be referenced by the resources
    /// described in "From". Each entry in this list MUST be considered to be an
    /// additional place that references can be valid to, or to put this another
    /// way, entries MUST be combined using OR.
    ///
    /// Support: Core
    #[serde(default)]
    pub to: Vec<ReferenceGrantTo>,
}

/// ReferenceGrantFrom describes trusted namespaces and kinds.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ReferenceGrantFrom {
    /// Group is the group of the referent.
    /// When empty, the Kubernetes core API group is inferred.
    ///
    /// Support: Core
    #[serde(default)]
    pub group: String,
    /// Kind is the kind of the referent. Although implementations may support
    /// additional resources, the following types are part of the "Core"
    /// support level for this field.
    ///
    /// When used to permit a SecretObjectReference:
    ///
    /// * Gateway
    ///
    /// When used to permit a BackendObjectReference:
    ///
    /// * GRPCRoute
    /// * HTTPRoute
    /// * TCPRoute
    /// * TLSRoute
    /// * UDPRoute
    #[serde(default)]
    pub kind: String,
    /// Namespace is the namespace of the referent.
    ///
    /// Support: Core
    #[serde(default)]
    pub namespace: String,
}

/// ReferenceGrantTo describes what Kinds are allowed as targets of the
/// references.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ReferenceGrantTo {
    /// Group is the group of the referent.
    /// When empty, the Kubernetes core API group is inferred.
    ///
    /// Support: Core
    #[serde(default)]
    pub group: String,
    /// Kind is the kind of the referent. Although implementations may support
    /// additional resources, the following types are part of the "Core"
    /// support level for this field:
    ///
    /// * Secret when used to permit a SecretObjectReference
    /// * Service when used to permit a BackendObjectReference
    #[serde(default)]
    pub kind: String,
    /// Name is the name of the referent. When unspecified, this policy
    /// refers to all resources of the specified Group and Kind in the local
    /// namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl k8s_openapi::Resource for ReferenceGrant {
    const API_VERSION: &'static str = "gateway.networking.k8s.io/v1";
    const GROUP: &'static str = "gateway.networking.k8s.io";
    const KIND: &'static str = "ReferenceGrant";
    const VERSION: &'static str = "v1";
    const URL_PATH_SEGMENT: &'static str = "referencegrants";
    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::ListableResource for ReferenceGrant {
    const LIST_KIND: &'static str = "ReferenceGrantList";
}

impl k8s_openapi::Metadata for ReferenceGrant {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
