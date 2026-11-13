//! `ControlPlane`, `gateway.edgerush.dev/v1alpha1`. Written by `edgerush-crdgen` from
//! `install/gateway.edgerush.dev_controlplanes.yaml`: not to be edited (the crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ControlPlane is an EdgeRush control plane: a controller that follows a watch scope
/// and translates what it finds there for the DataPlanes of its own namespace that name
/// it. The operator deploys it, gives it the rights of its scope, and makes the
/// GatewayClass named as it is.
///
/// Its name is a DNS label, unique in the cluster: the oldest ControlPlane of a name
/// holds it, and a later one of the same name in another namespace is not accepted.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ControlPlane {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec is what the control plane is to be.
    #[serde(default = "default_control_plane_spec")]
    pub spec: ControlPlaneSpec,
    /// Status is what the operator and the control plane report of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ControlPlaneStatus>,
}

/// Spec is what the control plane is to be.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneSpec {
    /// Authority is the certificate authority the control plane signs its workloads'
    /// certificates with, provided by the platform. Left out, the control plane makes
    /// one of its own on first start, in its own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<ControlPlaneAuthority>,
    /// Replicas is how many controllers run. One leads, elected by a Lease; the
    /// others stand by to take over.
    #[serde(default = "default_control_plane_spec_replicas")]
    pub replicas: i32,
    /// Watch is where the control plane follows Ingresses, routes and what they name.
    #[serde(default = "default_control_plane_spec_watch")]
    pub watch: ControlPlaneWatch,
    /// Webhook is the control plane's admission webhook, which judges an Ingress as it is written.
    #[serde(default = "default_control_plane_spec_webhook")]
    pub webhook: ControlPlaneWebhook,
}

/// Authority is the certificate authority the control plane signs its workloads'
/// certificates with, provided by the platform. Left out, the control plane makes
/// one of its own on first start, in its own namespace.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneAuthority {
    /// SecretRef names a Secret in the ControlPlane's namespace holding the
    /// authority's key and certificate, signed by the platform's own root, with
    /// the chain up to that root. The control plane signs with it and never
    /// writes it: its rotation is the platform's.
    #[serde(rename = "secretRef")]
    #[serde(default)]
    pub secret_ref: ControlPlaneAuthoritySecretRef,
}

/// SecretRef names a Secret in the ControlPlane's namespace holding the
/// authority's key and certificate, signed by the platform's own root, with
/// the chain up to that root. The control plane signs with it and never
/// writes it: its rotation is the platform's.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneAuthoritySecretRef {
    /// Name is the Secret's name.
    #[serde(default)]
    pub name: String,
}

/// Watch is where the control plane follows Ingresses, routes and what they name.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneWatch {
    /// Namespaces is which namespaces the control plane watches. Beyond its own,
    /// the platform's allowance bounds them: a ControlPlane asking for more is not
    /// accepted.
    #[serde(default = "default_control_plane_watch_namespaces")]
    pub namespaces: ControlPlaneWatchNamespaces,
}

/// Namespaces is which namespaces the control plane watches. Beyond its own,
/// the platform's allowance bounds them: a ControlPlane asking for more is not
/// accepted.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneWatchNamespaces {
    /// From is where the namespaces watched are:
    ///
    /// * Same: the ControlPlane's own namespace alone.
    /// * Selector: those whose labels the selector matches.
    /// * All: every namespace in the cluster.
    ///
    /// The schema's values: `All`, `Selector`, `Same`. Another is read as it is.
    #[serde(default = "default_control_plane_watch_namespaces_from")]
    pub from: String,
    /// Selector is the namespaces watched where From is "Selector", and
    /// must be left out otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<ControlPlaneWatchNamespacesSelector>,
}

/// Selector is the namespaces watched where From is "Selector", and
/// must be left out otherwise.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneWatchNamespacesSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<ControlPlaneWatchNamespacesSelectorMatchExpressions>>,
    /// matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels
    /// map is equivalent to an element of matchExpressions, whose key field is "key", the
    /// operator is "In", and the values array contains only "value". The requirements are ANDed.
    #[serde(rename = "matchLabels")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<BTreeMap<String, String>>,
}

/// A label selector requirement is a selector that contains values, a key, and an operator that
/// relates the key and values.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneWatchNamespacesSelectorMatchExpressions {
    /// key is the label key that the selector applies to.
    #[serde(default)]
    pub key: String,
    /// operator represents a key's relationship to a set of values.
    /// Valid operators are In, NotIn, Exists and DoesNotExist.
    #[serde(default)]
    pub operator: String,
    /// values is an array of string values. If the operator is In or NotIn,
    /// the values array must be non-empty. If the operator is Exists or DoesNotExist,
    /// the values array must be empty. This array is replaced during a strategic
    /// merge patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// Webhook is the control plane's admission webhook, which judges an Ingress as it is written.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneWebhook {
    /// Enabled registers the webhook. Off, an Ingress is judged only in its
    /// status: for a platform that keeps admission webhooks to its own, or a team
    /// that would rather read status alone.
    #[serde(default = "default_control_plane_webhook_enabled")]
    pub enabled: bool,
}

/// Status is what the operator and the control plane report of it.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ControlPlaneStatus {
    /// Conditions describe the ControlPlane's state. Known condition types are:
    ///
    /// * "Accepted": whether the operator accepted it, by the checks only the
    ///   cluster can make; what the control plane acts on.
    /// * "Programmed": whether what it asks for is made, and its workload has a
    ///   replica available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_spec() -> ControlPlaneSpec {
    ControlPlaneSpec { replicas: default_control_plane_spec_replicas(), watch: default_control_plane_spec_watch(), webhook: default_control_plane_spec_webhook(), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_spec_replicas() -> i32 {
    1
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_spec_watch() -> ControlPlaneWatch {
    ControlPlaneWatch { namespaces: default_control_plane_watch_namespaces() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_spec_webhook() -> ControlPlaneWebhook {
    ControlPlaneWebhook { enabled: default_control_plane_webhook_enabled() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_watch_namespaces() -> ControlPlaneWatchNamespaces {
    ControlPlaneWatchNamespaces { from: String::from("Same"), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_watch_namespaces_from() -> String {
    String::from("Same")
}

/// The schema's default.
#[rustfmt::skip]
fn default_control_plane_webhook_enabled() -> bool {
    true
}

impl k8s_openapi::Resource for ControlPlane {
    const API_VERSION: &'static str = "gateway.edgerush.dev/v1alpha1";
    const GROUP: &'static str = "gateway.edgerush.dev";
    const KIND: &'static str = "ControlPlane";
    const VERSION: &'static str = "v1alpha1";
    const URL_PATH_SEGMENT: &'static str = "controlplanes";
    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::ListableResource for ControlPlane {
    const LIST_KIND: &'static str = "ControlPlaneList";
}

impl k8s_openapi::Metadata for ControlPlane {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
