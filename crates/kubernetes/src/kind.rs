//! The kinds EdgeRush lists and watches, and what it asks of each: the data plane's
//! EndpointSlices; the control plane's Ingresses, IngressClasses, Services and Secrets, the
//! Gateway API's kinds and their CRDs ([04 §1](../../../docs/04-control-plane.md)), and the
//! Lease its replicas elect a leader by ([02 §3](../../../docs/02-architecture.md)).

use crate::ApiError;
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::{ListableResource, Metadata};
use serde::de::DeserializeOwned;

/// A kind EdgeRush lists and watches.
pub trait Kind:
    ListableResource + Metadata<Ty = ObjectMeta> + DeserializeOwned + Clone + PartialEq + Send + Sync
{
    /// Whether its objects are a namespace's, rather than the cluster's.
    const NAMESPACED: bool;

    /// The most objects a list's page asks for: few enough that a page of the largest
    /// objects of the kind there can be is no larger than
    /// [`MOST_PAGE_BYTES`](crate::MOST_PAGE_BYTES).
    const PAGE: u32 = 500;

    /// The objects the API server is asked to send alone, as a field selector, if not all.
    const FIELDS: Option<&'static str> = None;

    /// The object as it is kept: without what EdgeRush never reads, so that a list of many
    /// holds no more than it must. Of every kind, `managedFields` goes, the record of who
    /// set each field.
    fn trim(&mut self) {
        self.metadata_mut().managed_fields = None;
    }
}

impl Kind for EndpointSlice {
    const NAMESPACED: bool = true;
}

impl Kind for Lease {
    const NAMESPACED: bool = true;
}

impl Kind for Ingress {
    const NAMESPACED: bool = true;
}

impl Kind for IngressClass {
    const NAMESPACED: bool = false;
}

impl Kind for Service {
    const NAMESPACED: bool = true;
}

/// A Secret's keys a certificate is read from: those alone are kept.
const CERTIFICATE_KEYS: [&str; 2] = ["tls.crt", "tls.key"];

impl Kind for Secret {
    const NAMESPACED: bool = true;

    /// Secrets are the kind whose objects are large: etcd keeps one of up to 1.5 MiB, which
    /// as JSON, its data in base64, is some 2 MiB at most. Twelve such fit a page with room
    /// to spare.
    const PAGE: u32 = 12;

    /// Helm keeps each release of each chart as a Secret, large and many, and every service
    /// account may have a token's; neither holds a certificate, so neither is sent.
    const FIELDS: Option<&'static str> =
        Some("type!=helm.sh/release.v1,type!=kubernetes.io/service-account-token");

    /// Only a certificate and its key are kept, and of the metadata only what names the
    /// Secret: its annotations are dropped with the rest, kubectl's record of what it
    /// applied among them, which holds the whole Secret again.
    fn trim(&mut self) {
        let metadata = self.metadata_mut();
        metadata.managed_fields = None;
        metadata.annotations = None;
        metadata.labels = None;
        self.string_data = None;
        if let Some(data) = self.data.as_mut() {
            data.retain(|key, _| CERTIFICATE_KEYS.contains(&key.as_str()));
        }
    }
}

impl Kind for GatewayClass {
    const NAMESPACED: bool = false;
}

impl Kind for Gateway {
    const NAMESPACED: bool = true;
}

impl Kind for HTTPRoute {
    const NAMESPACED: bool = true;
}

impl Kind for ReferenceGrant {
    const NAMESPACED: bool = true;
}

/// The annotations of a CRD that are kept: the Gateway API's release and channel.
const CRD_ANNOTATIONS: [&str; 2] = [
    "gateway.networking.k8s.io/bundle-version",
    "gateway.networking.k8s.io/channel",
];

impl Kind for CustomResourceDefinition {
    const NAMESPACED: bool = false;

    /// A CRD is read for the versions it serves, and for the Gateway API's release, so that
    /// is all that is kept: its schemas, which run to hundreds of kilobytes, go.
    fn trim(&mut self) {
        let metadata = self.metadata_mut();
        metadata.managed_fields = None;
        metadata.labels = None;
        if let Some(annotations) = metadata.annotations.as_mut() {
            annotations.retain(|key, _| CRD_ANNOTATIONS.contains(&key.as_str()));
        }
        self.spec.conversion = None;
        for version in &mut self.spec.versions {
            version.schema = None;
            version.additional_printer_columns = None;
            version.selectable_fields = None;
        }
    }
}

/// What is listed and watched of a kind: every object in the cluster, a namespace's, or one
/// object by its name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    /// Every object in the cluster.
    Cluster,
    /// A namespace's objects.
    Namespace(String),
    /// The object of this name, of a kind whose objects are the cluster's: what is asked
    /// for where only some objects of a kind may be read (RBAC's `resourceNames`), such as
    /// the CRDs of the Gateway API kinds followed (04 §9). Listed as the cluster's with a
    /// field selector on its name, which a list of no such object answers empty.
    Named(String),
}

impl Scope {
    /// The objects of namespace `name`.
    ///
    /// # Errors
    ///
    /// [`ApiError::Namespace`] for a name no namespace can have, which would not make a
    /// path.
    pub fn namespace(name: &str) -> Result<Self, ApiError> {
        if !is_namespace(name) {
            return Err(ApiError::Namespace(name.to_owned()));
        }
        Ok(Self::Namespace(name.to_owned()))
    }

    /// The object named `name`.
    ///
    /// # Errors
    ///
    /// [`ApiError::Name`] for a name no object can have.
    pub fn named(name: &str) -> Result<Self, ApiError> {
        if !is_name(name) {
            return Err(ApiError::Name(name.to_owned()));
        }
        Ok(Self::Named(name.to_owned()))
    }
}

/// Whether `name` can be an object's: a DNS subdomain, as Kubernetes asks of most.
fn is_name(name: &str) -> bool {
    (1..=253).contains(&name.len())
        && name.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
}

/// Whether `name` can be a namespace's: a DNS label.
fn is_namespace(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

/// The path of `K`'s objects in `scope`.
pub(crate) fn collection<K: Kind>(scope: &Scope) -> Result<String, ApiError> {
    let group = if K::GROUP.is_empty() {
        format!("/api/{}", K::VERSION)
    } else {
        format!("/apis/{}/{}", K::GROUP, K::VERSION)
    };
    match scope {
        Scope::Cluster => Ok(format!("{group}/{}", K::URL_PATH_SEGMENT)),
        Scope::Named(name) => {
            if K::NAMESPACED {
                return Err(ApiError::Namespaced(K::KIND));
            }
            if !is_name(name) {
                return Err(ApiError::Name(name.clone()));
            }
            Ok(format!("{group}/{}", K::URL_PATH_SEGMENT))
        }
        Scope::Namespace(namespace) => {
            if !K::NAMESPACED {
                return Err(ApiError::NotNamespaced(K::KIND));
            }
            if !is_namespace(namespace) {
                return Err(ApiError::Namespace(namespace.clone()));
            }
            Ok(format!(
                "{group}/namespaces/{namespace}/{}",
                K::URL_PATH_SEGMENT
            ))
        }
    }
}
