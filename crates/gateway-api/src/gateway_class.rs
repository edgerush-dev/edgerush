//! `GatewayClass`, `gateway.networking.k8s.io/v1`, as Gateway API v1.7.0 defines it in its standard
//! channel. Written by `edgerush-crdgen` from `crds/gateway.networking.k8s.io_gatewayclasses.yaml`: not to be edited (the
//! crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta};
use serde::{Deserialize, Serialize};

/// GatewayClass describes a class of Gateways available to the user for creating
/// Gateway resources.
///
/// It is recommended that this resource be used as a template for Gateways. This
/// means that a Gateway is based on the state of the GatewayClass at the time it
/// was created and changes to the GatewayClass or associated parameters are not
/// propagated down to existing Gateways. This recommendation is intended to
/// limit the blast radius of changes to GatewayClass or associated parameters.
/// If implementations choose to propagate GatewayClass changes to existing
/// Gateways, that MUST be clearly documented by the implementation.
///
/// Whenever one or more Gateways are using a GatewayClass, implementations SHOULD
/// add the `gateway-exists-finalizer.gateway.networking.k8s.io` finalizer on the
/// associated GatewayClass. This ensures that a GatewayClass associated with a
/// Gateway is not deleted while in use.
///
/// GatewayClass is a Cluster level resource.
///
/// A GatewayClass name SHOULD be compliant with RFC 1035, consisting of a maximum of 63 lower case alphanumeric
/// characters or hyphens ('-'), and MUST start and end with an alphanumeric character.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct GatewayClass {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec defines the desired state of GatewayClass.
    #[serde(default)]
    pub spec: GatewayClassSpec,
    /// Status defines the current state of GatewayClass.
    ///
    /// Implementations MUST populate status on all GatewayClass resources which
    /// specify their controller name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<GatewayClassStatus>,
}

/// Spec defines the desired state of GatewayClass.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct GatewayClassSpec {
    /// ControllerName is the name of the controller that is managing Gateways of
    /// this class. The value of this field MUST be a domain prefixed path.
    ///
    /// Example: "example.net/gateway-controller".
    ///
    /// This field is not mutable and cannot be empty.
    ///
    /// Support: Core
    #[serde(rename = "controllerName")]
    #[serde(default)]
    pub controller_name: String,
    /// Description helps describe a GatewayClass with more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// ParametersRef is a reference to a resource that contains the configuration
    /// parameters corresponding to the GatewayClass. This is optional if the
    /// controller does not require any additional configuration.
    ///
    /// ParametersRef can reference a standard Kubernetes resource, i.e. ConfigMap,
    /// or an implementation-specific custom resource. The resource can be
    /// cluster-scoped or namespace-scoped.
    ///
    /// If the referent cannot be found, refers to an unsupported kind, or when
    /// the data within that resource is malformed, the GatewayClass SHOULD be
    /// rejected with the "Accepted" status condition set to "False" and an
    /// "InvalidParameters" reason.
    ///
    /// A Gateway for this GatewayClass may provide its own `parametersRef`. When both are specified,
    /// the merging behavior is implementation specific.
    /// It is generally recommended that GatewayClass provides defaults that can be overridden by a Gateway.
    ///
    /// Support: Implementation-specific
    #[serde(rename = "parametersRef")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters_ref: Option<GatewayClassParametersRef>,
}

/// ParametersRef is a reference to a resource that contains the configuration
/// parameters corresponding to the GatewayClass. This is optional if the
/// controller does not require any additional configuration.
///
/// ParametersRef can reference a standard Kubernetes resource, i.e. ConfigMap,
/// or an implementation-specific custom resource. The resource can be
/// cluster-scoped or namespace-scoped.
///
/// If the referent cannot be found, refers to an unsupported kind, or when
/// the data within that resource is malformed, the GatewayClass SHOULD be
/// rejected with the "Accepted" status condition set to "False" and an
/// "InvalidParameters" reason.
///
/// A Gateway for this GatewayClass may provide its own `parametersRef`. When both are specified,
/// the merging behavior is implementation specific.
/// It is generally recommended that GatewayClass provides defaults that can be overridden by a Gateway.
///
/// Support: Implementation-specific
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct GatewayClassParametersRef {
    /// Group is the group of the referent.
    #[serde(default)]
    pub group: String,
    /// Kind is kind of the referent.
    #[serde(default)]
    pub kind: String,
    /// Name is the name of the referent.
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of the referent.
    /// This field is required when referring to a Namespace-scoped resource and
    /// MUST be unset when referring to a Cluster-scoped resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Status defines the current state of GatewayClass.
///
/// Implementations MUST populate status on all GatewayClass resources which
/// specify their controller name.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct GatewayClassStatus {
    /// Conditions is the current status from the controller for
    /// this GatewayClass.
    ///
    /// Controllers should prefer to publish conditions using values
    /// of GatewayClassConditionType for the type of each Condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
    /// SupportedFeatures is the set of features the GatewayClass support.
    /// It MUST be sorted in ascending alphabetical order by the Name key.
    #[serde(rename = "supportedFeatures")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_features: Option<Vec<GatewayClassStatusSupportedFeatures>>,
}

/// `GatewayClassStatusSupportedFeatures`, which the schema does not describe.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct GatewayClassStatusSupportedFeatures {
    /// FeatureName is used to describe distinct features that are covered by
    /// conformance tests.
    #[serde(default)]
    pub name: String,
}

impl k8s_openapi::Resource for GatewayClass {
    const API_VERSION: &'static str = "gateway.networking.k8s.io/v1";
    const GROUP: &'static str = "gateway.networking.k8s.io";
    const KIND: &'static str = "GatewayClass";
    const VERSION: &'static str = "v1";
    const URL_PATH_SEGMENT: &'static str = "gatewayclasses";
    type Scope = k8s_openapi::ClusterResourceScope;
}

impl k8s_openapi::ListableResource for GatewayClass {
    const LIST_KIND: &'static str = "GatewayClassList";
}

impl k8s_openapi::Metadata for GatewayClass {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
