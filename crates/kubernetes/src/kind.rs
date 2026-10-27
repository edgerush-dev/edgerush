//! The kinds EdgeRush lists and watches, and what it asks of each: the data plane's
//! EndpointSlices; the control plane's Ingresses, IngressClasses, Services and Secrets
//! ([04 §1](../../../docs/04-control-plane.md)).

use crate::ApiError;
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
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

/// What is listed and watched of a kind: every object in the cluster, or a namespace's.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    /// Every object in the cluster.
    Cluster,
    /// A namespace's objects.
    Namespace(String),
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
