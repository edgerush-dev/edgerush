//! `PlatformDefaults`, `gateway.edgerush.dev/v1alpha1`. Written by `edgerush-crdgen` from
//! `install/gateway.edgerush.dev_platformdefaults.yaml`: not to be edited (the crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// PlatformDefaults is what the platform allows the cluster's ControlPlanes and
/// DataPlanes. There is one, named "default"; without it, each ControlPlane may
/// watch its own namespace alone.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PlatformDefaults {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec is what the platform allows.
    #[serde(default = "default_platform_defaults_spec")]
    pub spec: PlatformDefaultsSpec,
}

/// Spec is what the platform allows.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsSpec {
    /// WatchScopes is which namespaces ControlPlanes may watch beyond their own,
    /// each an allowance for the ControlPlanes it picks. A ControlPlane may use any
    /// allowance that picks it; one asking for more than every one of them allows
    /// is not accepted, and nothing is made for it. An allowance narrowed takes back
    /// what it granted.
    #[serde(rename = "watchScopes")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_scopes: Option<Vec<PlatformDefaultsWatchScopes>>,
}

/// WatchScope is one allowance.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopes {
    /// ControlPlanes is which ControlPlanes the allowance is for.
    #[serde(rename = "controlPlanes")]
    #[serde(default)]
    pub control_planes: PlatformDefaultsWatchScopesControlPlanes,
    /// Namespaces is which namespaces they may watch.
    #[serde(default)]
    pub namespaces: PlatformDefaultsWatchScopesNamespaces,
}

/// ControlPlanes is which ControlPlanes the allowance is for.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesControlPlanes {
    /// NamespaceSelector picks the ControlPlanes of the namespaces whose
    /// labels it matches. Every namespace carries its name in the label
    /// kubernetes.io/metadata.name, so a list of namespaces is a
    /// selector of that label. It must have at least one requirement.
    #[serde(rename = "namespaceSelector")]
    #[serde(default)]
    pub namespace_selector: PlatformDefaultsWatchScopesControlPlanesNamespaceSelector,
}

/// NamespaceSelector picks the ControlPlanes of the namespaces whose
/// labels it matches. Every namespace carries its name in the label
/// kubernetes.io/metadata.name, so a list of namespaces is a
/// selector of that label. It must have at least one requirement.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesControlPlanesNamespaceSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions:
        Option<Vec<PlatformDefaultsWatchScopesControlPlanesNamespaceSelectorMatchExpressions>>,
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
pub struct PlatformDefaultsWatchScopesControlPlanesNamespaceSelectorMatchExpressions {
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

/// Namespaces is which namespaces they may watch.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesNamespaces {
    /// From is where the namespaces they may watch are:
    ///
    /// * All: every namespace in the cluster, so any watch scope.
    /// * Selector: those whose labels the selector matches, so a
    ///   Selector scope whose selector carries every requirement of
    ///   this one's, and so selects no namespace beyond it.
    ///
    /// The schema's values: `All`, `Selector`. Another is read as it is.
    #[serde(default)]
    pub from: String,
    /// Selector is the namespaces they may watch where From is "Selector",
    /// and must be left out otherwise. It must have at least one requirement:
    /// every namespace is From "All".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<PlatformDefaultsWatchScopesNamespacesSelector>,
}

/// Selector is the namespaces they may watch where From is "Selector",
/// and must be left out otherwise. It must have at least one requirement:
/// every namespace is From "All".
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesNamespacesSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions:
        Option<Vec<PlatformDefaultsWatchScopesNamespacesSelectorMatchExpressions>>,
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
pub struct PlatformDefaultsWatchScopesNamespacesSelectorMatchExpressions {
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

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_spec() -> PlatformDefaultsSpec {
    PlatformDefaultsSpec { ..Default::default() }
}

impl k8s_openapi::Resource for PlatformDefaults {
    const API_VERSION: &'static str = "gateway.edgerush.dev/v1alpha1";
    const GROUP: &'static str = "gateway.edgerush.dev";
    const KIND: &'static str = "PlatformDefaults";
    const VERSION: &'static str = "v1alpha1";
    const URL_PATH_SEGMENT: &'static str = "platformdefaults";
    type Scope = k8s_openapi::ClusterResourceScope;
}

impl k8s_openapi::ListableResource for PlatformDefaults {
    const LIST_KIND: &'static str = "PlatformDefaultsList";
}

impl k8s_openapi::Metadata for PlatformDefaults {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
