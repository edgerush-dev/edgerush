//! The objects the operator makes for a `ControlPlane` or a `DataPlane`
//! ([05 §2](../../../docs/05-operator.md)): a pure function of the resource, its
//! ControlPlane for a DataPlane, the namespaces a selector of its watch scope selects, and
//! the install. No I/O: the operator reads the cluster and applies what this returns.
//!
//! Every object is labelled with the plane it is for, so that what has no owner reference
//! (a cluster-scoped object, a RoleBinding in another namespace) is found by label and
//! deleted with it, and so that an object of the same name that the operator did not make
//! is told apart and never taken over. What sits in the plane's own namespace has an owner
//! reference to it.
//!
//! The data plane's Service is [`ProxyService`], apart, as the control plane writes it, of
//! the ports its config binds (05 §2); the certificates the workloads mount are the control
//! plane's too, and the data plane's probes come with the slice that builds them; the
//! controller's readiness probe asks its `/ready`. A ControlPlane's
//! webhook is registered only once its status holds its authority's bundle, which the
//! control plane publishes there (05 §2): never with an empty one, filled in later. A
//! ControlPlane that declares a rate-limiting service has its objects made too
//! ([`ratelimit`]), and none while it does not.

use std::collections::{BTreeMap, BTreeSet};

use edgerush_gateway_api::gateway::GatewaySpec;
use edgerush_gateway_api::gateway_class::GatewayClassSpec;
use k8s_openapi::ByteString;
use k8s_openapi::Resource;
use k8s_openapi::api::admissionregistration::v1::{
    RuleWithOperations, ServiceReference, ValidatingWebhook, ValidatingWebhookConfiguration,
    WebhookClientConfig,
};
use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use k8s_openapi::api::autoscaling::v2::{
    CrossVersionObjectReference, HorizontalPodAutoscaler, HorizontalPodAutoscalerSpec,
};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction,
    ObjectFieldSelector, PodSecurityContext, PodSpec, PodTemplateSpec, Probe, ResourceRequirements,
    SeccompProfile, SecretVolumeSource, SecurityContext, Service, ServiceAccount, ServicePort,
    ServiceSpec, Sysctl, TopologySpreadConstraint, Volume, VolumeMount,
};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass, IngressClassSpec};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::api::rbac::v1::{ClusterRoleBinding, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    LabelSelector, LabelSelectorRequirement, ObjectMeta, OwnerReference,
};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::Serialize;

use crate::control_plane::{ControlPlane, ControlPlaneWatchNamespacesSelector};
use crate::data_plane::{DataPlane, DataPlaneRateLimiting};
use crate::gateway_first::carried;
use crate::rate_limit_policy::RateLimitPolicy;
use crate::workers::workers;

pub mod ratelimit;
#[cfg(test)]
mod tests;

/// The control plane's reads and status writes in a watched namespace (05 §2).
pub const CONTROL_PLANE_ROLE: &str = "edgerush-control-plane";
/// The control plane's reads of cluster-scoped kinds.
pub const CONTROL_PLANE_CLUSTER_ROLE: &str = "edgerush-control-plane-cluster";
/// What the control plane reads and writes in its own namespace.
pub const CONTROL_PLANE_OWN_ROLE: &str = "edgerush-control-plane-own";
/// What the control plane writes for a DataPlane generated in another namespace, there
/// ([05 §3](../../../docs/05-operator.md)): its Service and identity Secret.
pub const CONTROL_PLANE_GENERATED_ROLE: &str = "edgerush-control-plane-generated";
/// The data plane's one right, list and watch on EndpointSlices.
pub const DATA_PLANE_ROLE: &str = "edgerush-data-plane";

/// The five ClusterRoles the install ships, the only ones the operator binds.
pub const ROLES: [&str; 5] = [
    CONTROL_PLANE_ROLE,
    CONTROL_PLANE_CLUSTER_ROLE,
    CONTROL_PLANE_OWN_ROLE,
    CONTROL_PLANE_GENERATED_ROLE,
    DATA_PLANE_ROLE,
];

/// The controller an IngressClass names to be served by EdgeRush.
pub const INGRESS_CONTROLLER: &str = "edgerush.dev/ingress-controller";
/// The controller a GatewayClass names to be served by EdgeRush.
pub const GATEWAY_CONTROLLER: &str = "edgerush.dev/gateway-controller";

/// The label naming the ControlPlane an object is for: its name, unique in the cluster.
pub const CONTROL_PLANE_LABEL: &str = "gateway.edgerush.dev/control-plane";
/// The label naming the DataPlane an object is for, beside its ControlPlane's.
pub const DATA_PLANE_LABEL: &str = "gateway.edgerush.dev/data-plane";

/// The annotation on a data plane's pods holding its rate limiting's mode and the key cap
/// it states, where it keeps keys. A proxy sets memory aside for its keys' cap as it starts
/// and never raises the cap while it runs (06 §8): a change of either changes the pods'
/// template, and so rolls them. A change of the memory limit or the CPU, which a cap left
/// out is worked out from, rolls them already.
pub const RATE_LIMITING_ANNOTATION: &str = "gateway.edgerush.dev/rate-limiting";
/// The label naming the namespace of the resource an object is for: names are unique only
/// with it, so that a resource of the same names in another namespace never selects, by
/// label, what was made for this one (05 §2).
pub const NAMESPACE_LABEL: &str = "gateway.edgerush.dev/namespace";
/// The label naming the namespace of the ControlPlane whose control plane wrote an object,
/// beside [`CONTROL_PLANE_LABEL`], on the data plane's Service and identity Secret: outside
/// its own namespace, the shipped admission policy lets a control plane write only what
/// carries both, of its own (05 §2). [`NAMESPACE_LABEL`] there is the DataPlane's.
pub const CONTROL_PLANE_NAMESPACE_LABEL: &str = "gateway.edgerush.dev/control-plane-namespace";
/// The label naming which of a plane's workloads an object is for: `controller`, `proxy`, or
/// the rate-limiting service's ([`ratelimit::RATE_LIMITING_COMPONENT`]).
pub const COMPONENT_LABEL: &str = "app.kubernetes.io/component";
/// The label saying the operator made an object.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
/// Its value.
pub const MANAGED_BY: &str = "edgerush-operator";

/// The port the control plane serves its data planes' streams on.
const STREAM_PORT: i32 = 9443;
/// The port its pods answer their readiness probe on, `/ready`, apart from anything else.
const HEALTH_PORT: i32 = 8081;
/// The port its replicas serve each other their snapshots on, apart from the data planes'
/// stream (04 §2).
const PEERS_PORT: i32 = 9444;
/// The port its pods serve the admission webhook on, and the Service's for it.
const WEBHOOK_PORT: i32 = 8443;
const WEBHOOK_SERVICE_PORT: i32 = 443;
/// The path its pods answer reviews on.
const WEBHOOK_PATH: &str = "/validate";
/// How long the API server waits for an answer before it admits the Ingress as for an
/// outage: its own default, and the webhook's own bound on a request's head and again on
/// its body (04 §3).
const WEBHOOK_TIMEOUT: i32 = 10;
/// The label every namespace carries of its own name, which a webhook selects one by.
const NAMESPACE_NAME_LABEL: &str = "kubernetes.io/metadata.name";

/// `nobody`, which the image runs as: stated by number, as `runAsNonRoot` can check only a
/// number.
const NOBODY: i64 = 65534;

/// What every object takes from the install rather than from a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Install {
    /// The image the planes run, the operator's own: one image, holding both binaries,
    /// `edgerush` and the rate-limiting service's (05 §7).
    pub image: String,
    /// The cluster's DNS domain, which a Service's name ends in: `cluster.local` unless
    /// the cluster was set up otherwise (05 §3).
    pub cluster_domain: String,
}

/// An object the operator makes, written as the API server takes it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Object {
    /// A plane's ServiceAccount.
    ServiceAccount(ServiceAccount),
    /// A binding of one of the five roles in a namespace.
    RoleBinding(RoleBinding),
    /// A binding of one of the five roles over the cluster.
    ClusterRoleBinding(ClusterRoleBinding),
    /// A plane's workload.
    Deployment(Box<Deployment>),
    /// A Service: the control plane's, which its data planes dial, or one of its
    /// rate-limiting service's two.
    Service(Box<Service>),
    /// The rate-limiting service's config file.
    ConfigMap(ConfigMap),
    /// The data plane's autoscaler, where its replicas' `max` is above `min`.
    HorizontalPodAutoscaler(Box<HorizontalPodAutoscaler>),
    /// The data plane's disruption budget.
    PodDisruptionBudget(PodDisruptionBudget),
    /// The control plane's GatewayClass.
    GatewayClass(Box<Made<GatewayClassSpec>>),
    /// The data plane's IngressClass.
    IngressClass(IngressClass),
    /// The data plane's Gateway.
    Gateway(Box<Made<GatewaySpec>>),
    /// The control plane's webhook registration.
    ValidatingWebhookConfiguration(Box<ValidatingWebhookConfiguration>),
}

/// An object of a kind whose type is written from its CRD, and so serialized without its
/// `apiVersion` and `kind`, with them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Made<S> {
    /// Its API version.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Its kind.
    pub kind: String,
    /// Its metadata.
    pub metadata: ObjectMeta,
    /// Its spec.
    pub spec: S,
}

impl Object {
    /// Its kind.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ServiceAccount(_) => "ServiceAccount",
            Self::RoleBinding(_) => "RoleBinding",
            Self::ClusterRoleBinding(_) => "ClusterRoleBinding",
            Self::Deployment(_) => "Deployment",
            Self::Service(_) => "Service",
            Self::ConfigMap(_) => "ConfigMap",
            Self::HorizontalPodAutoscaler(_) => "HorizontalPodAutoscaler",
            Self::PodDisruptionBudget(_) => "PodDisruptionBudget",
            Self::GatewayClass(_) => "GatewayClass",
            Self::IngressClass(_) => "IngressClass",
            Self::Gateway(_) => "Gateway",
            Self::ValidatingWebhookConfiguration(_) => "ValidatingWebhookConfiguration",
        }
    }

    /// Its metadata: its name, its namespace for a namespace's object, and its labels.
    #[must_use]
    pub fn metadata(&self) -> &ObjectMeta {
        match self {
            Self::ServiceAccount(object) => &object.metadata,
            Self::RoleBinding(object) => &object.metadata,
            Self::ClusterRoleBinding(object) => &object.metadata,
            Self::Deployment(object) => &object.metadata,
            Self::Service(object) => &object.metadata,
            Self::ConfigMap(object) => &object.metadata,
            Self::HorizontalPodAutoscaler(object) => &object.metadata,
            Self::PodDisruptionBudget(object) => &object.metadata,
            Self::GatewayClass(object) => &object.metadata,
            Self::IngressClass(object) => &object.metadata,
            Self::Gateway(object) => &object.metadata,
            Self::ValidatingWebhookConfiguration(object) => &object.metadata,
        }
    }

    /// Its metadata, to be added to.
    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        match self {
            Self::ServiceAccount(object) => &mut object.metadata,
            Self::RoleBinding(object) => &mut object.metadata,
            Self::ClusterRoleBinding(object) => &mut object.metadata,
            Self::Deployment(object) => &mut object.metadata,
            Self::Service(object) => &mut object.metadata,
            Self::ConfigMap(object) => &mut object.metadata,
            Self::HorizontalPodAutoscaler(object) => &mut object.metadata,
            Self::PodDisruptionBudget(object) => &mut object.metadata,
            Self::GatewayClass(object) => &mut object.metadata,
            Self::IngressClass(object) => &mut object.metadata,
            Self::Gateway(object) => &mut object.metadata,
            Self::ValidatingWebhookConfiguration(object) => &mut object.metadata,
        }
    }
}

/// A data plane's [`RATE_LIMITING_ANNOTATION`]: its mode, and the cap it states, where it
/// keeps keys; none where it keeps none.
fn rate_limiting_annotation(
    limiting: Option<&DataPlaneRateLimiting>,
) -> Option<BTreeMap<String, String>> {
    let limiting = limiting.filter(|limiting| limiting.mode != "Disabled")?;
    let value = match limiting.max_key_bytes {
        Some(bytes) => format!("{}, maxKeyBytes {bytes}", limiting.mode),
        None => limiting.mode.clone(),
    };
    Some(BTreeMap::from([(
        RATE_LIMITING_ANNOTATION.to_owned(),
        value,
    )]))
}

/// `labels` and `annotations` added to `metadata`, under what it already has, which wins.
fn carry(
    metadata: &mut ObjectMeta,
    labels: &BTreeMap<String, String>,
    annotations: &BTreeMap<String, String>,
) {
    for (into, from) in [
        (&mut metadata.labels, labels),
        (&mut metadata.annotations, annotations),
    ] {
        if from.is_empty() {
            continue;
        }
        let into = into.get_or_insert_with(BTreeMap::new);
        for (key, value) in from {
            into.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
}

/// Why the objects could not be made: each a resource that should not have reached here.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The resource lacks what the API server gives every object it stores.
    #[error("the {kind} has no {what}")]
    Missing {
        /// The resource's kind.
        kind: &'static str,
        /// Its name, namespace or UID.
        what: &'static str,
    },
    /// The DataPlane is not the ControlPlane's.
    #[error("the DataPlane {data_plane} is not one of the ControlPlane {control_plane}'s")]
    NotItsControlPlane {
        /// The DataPlane, as `<namespace>/<name>`.
        data_plane: String,
        /// The ControlPlane, as `<namespace>/<name>`.
        control_plane: String,
    },
    /// A CPU quantity of the DataPlane's resources that no number of workers is read from.
    #[error("the CPU quantity {0} is not one a number of workers can be read from")]
    Cpu(String),
    /// The DataPlane's `gateway` does not read as a Gateway's spec.
    #[error("the DataPlane's gateway is not a Gateway's: {0}")]
    Gateway(String),
    /// A requirement of the ControlPlane's selector of namespaces that the API server would
    /// not read, which no controller could follow.
    #[error("the ControlPlane's selector of namespaces: {0}")]
    Selector(String),
    /// The DataPlane's `ipFamily` is none its schema holds.
    #[error("the DataPlane's ipFamily {0} is not IPv4, IPv6 or Dual")]
    IpFamily(String),
    /// A memory limit of the rate-limiting service's resources that no cap of its keys can
    /// be read from.
    #[error("the memory quantity {0} is not one a cap of keys can be read from")]
    Memory(String),
    /// The rate-limiting service's config file could not be written.
    #[error("the rate-limiting service's config file cannot be written: {0}")]
    ServiceFile(String),
}

/// A plane's name, namespace and UID, read from its metadata.
struct Identity<'a> {
    name: &'a str,
    namespace: &'a str,
    uid: &'a str,
}

impl<'a> Identity<'a> {
    fn of(metadata: &'a ObjectMeta, kind: &'static str) -> Result<Self, Error> {
        let missing = |what| Error::Missing { kind, what };
        Ok(Self {
            name: metadata.name.as_deref().ok_or_else(|| missing("name"))?,
            namespace: metadata
                .namespace
                .as_deref()
                .ok_or_else(|| missing("namespace"))?,
            uid: metadata.uid.as_deref().ok_or_else(|| missing("UID"))?,
        })
    }

    fn owner(&self, kind: &str) -> OwnerReference {
        OwnerReference {
            api_version: "gateway.edgerush.dev/v1alpha1".to_owned(),
            kind: kind.to_owned(),
            name: self.name.to_owned(),
            uid: self.uid.to_owned(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }
}

/// The name of a control plane's ServiceAccount, Deployment and Service: what the shipped
/// admission policy matches its writes by (05 §2).
#[must_use]
pub fn controller_name(control_plane: &str) -> String {
    format!("edgerush-controller-{control_plane}")
}

/// The name of a data plane's ServiceAccount and Deployment, and of its Service, which the
/// control plane makes ([05 §2](../../../docs/05-operator.md)).
#[must_use]
pub fn proxy_name(data_plane: &str) -> String {
    format!("edgerush-proxy-{data_plane}")
}

/// The Secret a ControlPlane's own authority is kept in, where it makes one (05 §2).
#[must_use]
pub fn authority_secret(control_plane: &str) -> String {
    format!("{}-authority", controller_name(control_plane))
}

/// The Secret a DataPlane's certificate is kept in, which the control plane writes and
/// its pods mount (05 §2).
#[must_use]
pub fn identity_secret(data_plane: &str) -> String {
    format!("{}-identity", proxy_name(data_plane))
}

/// The SPIFFE ID a workload of ControlPlane `control_plane`'s is known by: its
/// ServiceAccount's, in its ControlPlane's own trust domain, `<controlplane>.<cluster
/// domain>` ([12](../../../docs/12-open-questions.md) item 22).
#[must_use]
pub fn spiffe_id(
    control_plane: &str,
    cluster_domain: &str,
    namespace: &str,
    service_account: &str,
) -> String {
    format!("spiffe://{control_plane}.{cluster_domain}/ns/{namespace}/sa/{service_account}")
}

/// The SPIFFE ID DataPlane `data_plane` of ControlPlane `control_plane`'s is known by, its
/// pods sharing it: its ServiceAccount's, in its own namespace `namespace`, its
/// ControlPlane's or, for one generated, its Gateway's. Its certificate carries it, and the
/// config stream serves it its own config by it
/// ([05 §3](../../../docs/05-operator.md)).
#[must_use]
pub fn data_plane_id(
    control_plane: &str,
    cluster_domain: &str,
    namespace: &str,
    data_plane: &str,
) -> String {
    spiffe_id(
        control_plane,
        cluster_domain,
        namespace,
        &proxy_name(data_plane),
    )
}

/// Where a data plane's pods mount their certificate.
const IDENTITY_MOUNT: &str = "/etc/edgerush/identity";

/// The name of a data plane's bindings, in namespaces other DataPlanes' may share, and of
/// its ClusterRoleBinding: `edgerush-proxy-<controlplane>.<dataplane>` for one of its
/// ControlPlane's namespace, and for one generated elsewhere (05 §3), whose name is unique
/// only in its own, `edgerush-proxy-<controlplane>.<namespace>.<dataplane>`. None of the
/// three holds a dot, so no two are one name.
fn proxy_binding(
    control_plane: &str,
    control_plane_namespace: &str,
    namespace: &str,
    data_plane: &str,
) -> String {
    if namespace == control_plane_namespace {
        format!("edgerush-proxy-{control_plane}.{data_plane}")
    } else {
        format!("edgerush-proxy-{control_plane}.{namespace}.{data_plane}")
    }
}

/// A data plane's IngressClass: unique, as ControlPlane names are and hold no dot (05 §3).
#[must_use]
pub fn ingress_class(control_plane: &str, data_plane: &str) -> String {
    format!("{control_plane}.{data_plane}")
}

/// The namespaces a ControlPlane watches, from its spec and, where `from` is `Selector`, the
/// namespaces its selector selects: `None` for all of them.
fn watched(
    control_plane: &ControlPlane,
    own: &str,
    selected: &BTreeSet<String>,
) -> Option<BTreeSet<String>> {
    match control_plane.spec.watch.namespaces.from.as_str() {
        "All" => None,
        "Selector" => Some(selected.clone()),
        // `Same`, and anything a newer CRD may allow that this does not know: the narrowest.
        _ => Some(BTreeSet::from([own.to_owned()])),
    }
}

/// The selector a ControlPlane's controller follows its namespaces by, written as the API
/// server reads one (`env in (canary,prod),!legacy,owner,team=payments`): each requirement
/// in the order of its key, as Kubernetes writes them, `matchLabels` as `=`. No requirement
/// is every namespace.
fn namespace_selector(selector: &ControlPlaneWatchNamespacesSelector) -> Result<String, Error> {
    let mut each = Vec::new();
    for (key, value) in selector.match_labels.iter().flatten() {
        each.push((key.as_str(), format!("{key}={value}")));
    }
    for expression in selector.match_expressions.iter().flatten() {
        let key = expression.key.as_str();
        let values: BTreeSet<&str> = expression
            .values
            .iter()
            .flatten()
            .map(String::as_str)
            .collect();
        let values = values.into_iter().collect::<Vec<_>>().join(",");
        let operator = expression.operator.as_str();
        let problem = |what: &str| Error::Selector(format!("{operator} of key {key} {what}"));
        let written = match operator {
            "In" | "NotIn" if values.is_empty() => return Err(problem("has no values")),
            "Exists" | "DoesNotExist" if !values.is_empty() => return Err(problem("has values")),
            "In" => format!("{key} in ({values})"),
            "NotIn" => format!("{key} notin ({values})"),
            "Exists" => key.to_owned(),
            "DoesNotExist" => format!("!{key}"),
            _ => return Err(problem("is not an operator Kubernetes has")),
        };
        each.push((key, written));
    }
    each.sort();
    let written: Vec<String> = each.into_iter().map(|(_, written)| written).collect();
    Ok(written.join(","))
}

/// What every object made for ControlPlane `control_plane` of `namespace` is labelled
/// with, and what it is found by where no owner reference reaches it (05 §2): none of its
/// DataPlanes' carries them all, nor any other resource's.
#[must_use]
pub fn control_plane_labels(namespace: &str, control_plane: &str) -> BTreeMap<String, String> {
    labels("controller", namespace, control_plane, None)
}

/// What every object made for DataPlane `data_plane` of `namespace`, of ControlPlane
/// `control_plane`, is labelled with, and found by.
#[must_use]
pub fn data_plane_labels(
    namespace: &str,
    control_plane: &str,
    data_plane: &str,
) -> BTreeMap<String, String> {
    labels("proxy", namespace, control_plane, Some(data_plane))
}

/// What every object for the control plane `cp`, and for its data plane `dp`, both of
/// `namespace`, is labelled with; its pods' selector is these.
fn labels(
    component: &str,
    namespace: &str,
    cp: &str,
    dp: Option<&str>,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "edgerush".to_owned()),
        (COMPONENT_LABEL.to_owned(), component.to_owned()),
        (MANAGED_BY_LABEL.to_owned(), MANAGED_BY.to_owned()),
        (CONTROL_PLANE_LABEL.to_owned(), cp.to_owned()),
        (NAMESPACE_LABEL.to_owned(), namespace.to_owned()),
    ]);
    if let Some(dp) = dp {
        labels.insert(DATA_PLANE_LABEL.to_owned(), dp.to_owned());
    }
    labels
}

fn metadata(
    name: String,
    namespace: Option<&str>,
    labels: &BTreeMap<String, String>,
    owner: Option<&OwnerReference>,
) -> ObjectMeta {
    ObjectMeta {
        name: Some(name),
        namespace: namespace.map(str::to_owned),
        labels: Some(labels.clone()),
        owner_references: owner.map(|owner| vec![owner.clone()]),
        ..ObjectMeta::default()
    }
}

fn role_ref(role: &str) -> RoleRef {
    RoleRef {
        api_group: Some("rbac.authorization.k8s.io".to_owned()),
        kind: "ClusterRole".to_owned(),
        name: role.to_owned(),
    }
}

fn service_account_subject(name: &str, namespace: &str) -> Vec<Subject> {
    vec![Subject {
        kind: "ServiceAccount".to_owned(),
        name: name.to_owned(),
        namespace: Some(namespace.to_owned()),
        ..Subject::default()
    }]
}

/// `role` bound to `account`, over the cluster where `namespaces` is `None`, otherwise in
/// each of them, those in `own` owned by `owner`.
fn bindings(
    name: &str,
    role: &str,
    account: &str,
    own: &str,
    namespaces: Option<&BTreeSet<String>>,
    labels: &BTreeMap<String, String>,
    owner: &OwnerReference,
) -> Vec<Object> {
    let subjects = service_account_subject(account, own);
    let Some(namespaces) = namespaces else {
        return vec![Object::ClusterRoleBinding(ClusterRoleBinding {
            metadata: metadata(name.to_owned(), None, labels, None),
            role_ref: role_ref(role),
            subjects: Some(subjects),
        })];
    };
    namespaces
        .iter()
        .map(|namespace| {
            let owner = (namespace == own).then_some(owner);
            Object::RoleBinding(RoleBinding {
                metadata: metadata(name.to_owned(), Some(namespace), labels, owner),
                role_ref: role_ref(role),
                subjects: Some(subjects.clone()),
            })
        })
        .collect()
}

/// A container that runs as nobody, on a read-only root filesystem, with no capabilities.
fn locked_down() -> SecurityContext {
    SecurityContext {
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(true),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".to_owned()]),
            ..Capabilities::default()
        }),
        ..SecurityContext::default()
    }
}

/// A pod run as nobody, under the runtime's default seccomp profile.
fn pod_security(sysctls: Option<Vec<Sysctl>>) -> PodSecurityContext {
    PodSecurityContext {
        run_as_non_root: Some(true),
        run_as_user: Some(NOBODY),
        run_as_group: Some(NOBODY),
        seccomp_profile: Some(SeccompProfile {
            type_: "RuntimeDefault".to_owned(),
            ..SeccompProfile::default()
        }),
        sysctls,
        ..PodSecurityContext::default()
    }
}

fn from_field(name: &str, path: &str) -> EnvVar {
    EnvVar {
        name: name.to_owned(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: path.to_owned(),
                ..ObjectFieldSelector::default()
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    }
}

fn port(name: &str, number: i32) -> ContainerPort {
    ContainerPort {
        name: Some(name.to_owned()),
        container_port: number,
        ..ContainerPort::default()
    }
}

/// Every object the operator makes for `control_plane`, whose selector of namespaces, if it
/// has one, selects `selected`.
///
/// # Errors
///
/// [`Error::Missing`], for a ControlPlane the API server did not give a name, namespace or
/// UID; [`Error::Selector`], for a selector of namespaces the API server would not read;
/// [`Error::ServiceFile`], for a rate-limiting service whose file cannot be written.
pub fn for_control_plane(
    control_plane: &ControlPlane,
    selected: &BTreeSet<String>,
    install: &Install,
) -> Result<Vec<Object>, Error> {
    let cp = Identity::of(&control_plane.metadata, "ControlPlane")?;
    let owner = cp.owner("ControlPlane");
    let labels = control_plane_labels(cp.namespace, cp.name);
    let name = controller_name(cp.name);
    let watched = watched(control_plane, cp.namespace, selected);
    let webhook = control_plane.spec.webhook.enabled;

    let mut objects = vec![Object::ServiceAccount(ServiceAccount {
        metadata: metadata(name.clone(), Some(cp.namespace), &labels, Some(&owner)),
        ..ServiceAccount::default()
    })];
    objects.extend(bindings(
        &name,
        CONTROL_PLANE_ROLE,
        &name,
        cp.namespace,
        watched.as_ref(),
        &labels,
        &owner,
    ));
    objects.extend(bindings(
        &format!("{name}-cluster"),
        CONTROL_PLANE_CLUSTER_ROLE,
        &name,
        cp.namespace,
        None,
        &labels,
        &owner,
    ));
    objects.extend(bindings(
        &format!("{name}-own"),
        CONTROL_PLANE_OWN_ROLE,
        &name,
        cp.namespace,
        Some(&BTreeSet::from([cp.namespace.to_owned()])),
        &labels,
        &owner,
    ));

    let mut args = vec![
        "controller".to_owned(),
        // Its DataPlane, in its own namespace, which it reads itself (05 §3).
        "--control-plane".to_owned(),
        cp.name.to_owned(),
        "--cluster-domain".to_owned(),
        install.cluster_domain.clone(),
        "--listen".to_owned(),
        format!("0.0.0.0:{STREAM_PORT}"),
        "--leader-lease".to_owned(),
        format!("{}/{name}", cp.namespace),
        // Its readiness, which its probe asks (04 §2).
        "--health".to_owned(),
        format!("0.0.0.0:{HEALTH_PORT}"),
        // What its replicas share of what translation cannot read back (04 §2).
        "--peers".to_owned(),
        format!("0.0.0.0:{PEERS_PORT}"),
    ];
    // The authority it signs with: its own, or the platform's it is given, never written
    // (05 §2).
    match &control_plane.spec.authority {
        Some(provided) => args.extend([
            "--authority".to_owned(),
            provided.secret_ref.name.clone(),
            "--authority-provided".to_owned(),
        ]),
        None => args.extend(["--authority".to_owned(), authority_secret(cp.name)]),
    }
    // A selector's namespaces the controller follows itself, so that one labelled in or
    // out rolls no controller (04 §1).
    let namespaces = &control_plane.spec.watch.namespaces;
    if namespaces.from == "Selector" {
        let selector = namespaces.selector.clone().unwrap_or_default();
        args.extend([
            "--namespace-selector".to_owned(),
            namespace_selector(&selector)?,
        ]);
    } else {
        for namespace in watched.iter().flatten() {
            args.extend(["--namespace".to_owned(), namespace.clone()]);
        }
    }
    let mut ports = vec![
        port("stream", STREAM_PORT),
        port("health", HEALTH_PORT),
        port("peers", PEERS_PORT),
    ];
    if webhook {
        args.extend([
            "--webhook-listen".to_owned(),
            format!("0.0.0.0:{WEBHOOK_PORT}"),
        ]);
        ports.push(port("webhook", WEBHOOK_PORT));
    }
    let container = Container {
        name: "edgerush".to_owned(),
        image: Some(install.image.clone()),
        args: Some(args),
        // Its Pod's UID, which owns the report of what its data planes run (04 §6).
        env: Some(vec![from_field("EDGERUSH_POD_UID", "metadata.uid")]),
        ports: Some(ports),
        // Out of its Service until it may serve (04 §2).
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/ready".to_owned()),
                port: IntOrString::String("health".to_owned()),
                ..HTTPGetAction::default()
            }),
            ..Probe::default()
        }),
        security_context: Some(locked_down()),
        ..Container::default()
    };
    objects.push(Object::Deployment(Box::new(Deployment {
        metadata: metadata(name.clone(), Some(cp.namespace), &labels, Some(&owner)),
        spec: DeploymentSpec {
            replicas: Some(control_plane.spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            },
            // A new pod is ready before an old one goes, so a single replica's upgrade
            // keeps what it serves (04 §2).
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_owned()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_surge: Some(IntOrString::Int(1)),
                    max_unavailable: Some(IntOrString::Int(0)),
                }),
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(name.clone()),
                    security_context: Some(pod_security(None)),
                    containers: vec![container],
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        },
        ..Deployment::default()
    })));

    let mut service_ports = vec![ServicePort {
        name: Some("stream".to_owned()),
        port: STREAM_PORT,
        target_port: Some(IntOrString::String("stream".to_owned())),
        ..ServicePort::default()
    }];
    if webhook {
        service_ports.push(ServicePort {
            name: Some("webhook".to_owned()),
            port: WEBHOOK_SERVICE_PORT,
            target_port: Some(IntOrString::String("webhook".to_owned())),
            ..ServicePort::default()
        });
    }
    objects.push(Object::Service(Box::new(Service {
        metadata: metadata(name.clone(), Some(cp.namespace), &labels, Some(&owner)),
        spec: Some(ServiceSpec {
            selector: Some(labels.clone()),
            ports: Some(service_ports),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    })));

    if let Some(service) = control_plane
        .spec
        .rate_limiting
        .as_ref()
        .and_then(|rate_limiting| rate_limiting.service.as_ref())
    {
        objects.extend(ratelimit::objects(&cp, &owner, service, install)?);
    }

    if webhook && let Some(bundle) = authority_bundle(control_plane) {
        objects.push(Object::ValidatingWebhookConfiguration(Box::new(
            registration(control_plane, cp.namespace, cp.name, &name, bundle, &labels),
        )));
    }

    objects.push(Object::GatewayClass(Box::new(Made {
        api_version: "gateway.networking.k8s.io/v1".to_owned(),
        kind: "GatewayClass".to_owned(),
        metadata: metadata(cp.name.to_owned(), None, &labels, None),
        spec: GatewayClassSpec {
            controller_name: GATEWAY_CONTROLLER.to_owned(),
            description: Some(format!(
                "Served by EdgeRush's ControlPlane {}/{}.",
                cp.namespace, cp.name
            )),
            ..GatewayClassSpec::default()
        },
    })));
    Ok(objects)
}

/// The bundle of its authority's certificates that `control_plane`'s status publishes, if
/// it holds one: what its webhook is registered with (05 §2). A bundle with no certificate
/// in it is none, as the API server would trust nothing by it.
#[must_use]
pub fn authority_bundle(control_plane: &ControlPlane) -> Option<&str> {
    control_plane
        .status
        .as_ref()?
        .authority
        .as_ref()?
        .bundle
        .as_deref()
        .filter(|bundle| bundle.contains("-----BEGIN CERTIFICATE-----"))
}

/// The name of ControlPlane `control_plane`'s webhook registration, the cluster's: its
/// controller's, unique as ControlPlane names are.
#[must_use]
pub fn registration_name(control_plane: &str) -> String {
    controller_name(control_plane)
}

/// `control_plane`'s webhook registration, trusting `bundle`, its Service named `service`:
/// Ingresses and RateLimitPolicies made or changed in its watch scope sent there for
/// review, and admitted as for an outage where no answer comes (04 §3). Each kind is a
/// webhook of its own, so that a refusal names what judged it.
fn registration(
    control_plane: &ControlPlane,
    namespace: &str,
    control_plane_name: &str,
    service: &str,
    bundle: &str,
    labels: &BTreeMap<String, String>,
) -> ValidatingWebhookConfiguration {
    let hook = |group: &str, version: &str, resource: &str| ValidatingWebhook {
        name: format!("{control_plane_name}.{resource}.gateway.edgerush.dev"),
        admission_review_versions: vec!["v1".to_owned()],
        client_config: WebhookClientConfig {
            ca_bundle: Some(ByteString(bundle.as_bytes().to_vec())),
            service: Some(ServiceReference {
                namespace: namespace.to_owned(),
                name: service.to_owned(),
                path: Some(WEBHOOK_PATH.to_owned()),
                port: Some(WEBHOOK_SERVICE_PORT),
            }),
            url: None,
        },
        failure_policy: Some("Ignore".to_owned()),
        namespace_selector: scope_selector(control_plane, namespace),
        // The resource alone, not its status: what a controller writes there is not judged.
        rules: Some(vec![RuleWithOperations {
            api_groups: Some(vec![group.to_owned()]),
            api_versions: Some(vec![version.to_owned()]),
            operations: Some(vec!["CREATE".to_owned(), "UPDATE".to_owned()]),
            resources: Some(vec![resource.to_owned()]),
            scope: Some("Namespaced".to_owned()),
        }]),
        side_effects: "None".to_owned(),
        timeout_seconds: Some(WEBHOOK_TIMEOUT),
        ..ValidatingWebhook::default()
    };
    ValidatingWebhookConfiguration {
        metadata: metadata(registration_name(control_plane_name), None, labels, None),
        webhooks: Some(vec![
            hook(Ingress::GROUP, Ingress::VERSION, Ingress::URL_PATH_SEGMENT),
            hook(
                RateLimitPolicy::GROUP,
                RateLimitPolicy::VERSION,
                RateLimitPolicy::URL_PATH_SEGMENT,
            ),
        ]),
    }
}

/// The namespaces of `control_plane`'s watch scope as a webhook selects them, so that it is
/// asked of nothing beyond it: its own namespace, `own`, by name; a selector's, by that
/// selector; every namespace, by none. A scope not known is taken as the narrowest, as
/// [`watched`] takes it.
fn scope_selector(control_plane: &ControlPlane, own: &str) -> Option<LabelSelector> {
    let namespaces = &control_plane.spec.watch.namespaces;
    match namespaces.from.as_str() {
        "All" => None,
        "Selector" => {
            let selector = namespaces.selector.clone().unwrap_or_default();
            Some(LabelSelector {
                match_labels: selector.match_labels,
                match_expressions: selector.match_expressions.map(|expressions| {
                    expressions
                        .into_iter()
                        .map(|expression| LabelSelectorRequirement {
                            key: expression.key,
                            operator: expression.operator,
                            values: expression.values,
                        })
                        .collect()
                }),
            })
        }
        _ => Some(LabelSelector {
            match_labels: Some(BTreeMap::from([(
                NAMESPACE_NAME_LABEL.to_owned(),
                own.to_owned(),
            )])),
            ..LabelSelector::default()
        }),
    }
}

/// Every object the operator makes for `data_plane`, of `control_plane`, whose selector of
/// namespaces, if it has one, selects `selected`.
///
/// # Errors
///
/// As [`Error`] says: each a resource the API server or the operator should not have let
/// through.
pub fn for_data_plane(
    data_plane: &DataPlane,
    control_plane: &ControlPlane,
    selected: &BTreeSet<String>,
    install: &Install,
) -> Result<Vec<Object>, Error> {
    let dp = Identity::of(&data_plane.metadata, "DataPlane")?;
    let cp = Identity::of(&control_plane.metadata, "ControlPlane")?;
    let of = &data_plane.spec.control_plane_ref;
    if of.namespace.as_deref().unwrap_or(dp.namespace) != cp.namespace || of.name != cp.name {
        return Err(Error::NotItsControlPlane {
            data_plane: format!("{}/{}", dp.namespace, dp.name),
            control_plane: format!("{}/{}", cp.namespace, cp.name),
        });
    }
    let owner = dp.owner("DataPlane");
    let labels = data_plane_labels(dp.namespace, cp.name, dp.name);
    let name = proxy_name(dp.name);
    let spec = &data_plane.spec;

    let mut objects = vec![Object::ServiceAccount(ServiceAccount {
        metadata: metadata(name.clone(), Some(dp.namespace), &labels, Some(&owner)),
        ..ServiceAccount::default()
    })];
    // Over the watch scope, and in its own namespace whatever the scope, where its own
    // Service's slices say its zone (05 §2).
    let scope = watched(control_plane, cp.namespace, selected).map(|mut namespaces| {
        namespaces.insert(dp.namespace.to_owned());
        namespaces
    });
    objects.extend(bindings(
        &proxy_binding(cp.name, cp.namespace, dp.namespace, dp.name),
        DATA_PLANE_ROLE,
        &name,
        dp.namespace,
        scope.as_ref(),
        &labels,
        &owner,
    ));
    // Generated in a namespace not its ControlPlane's (05 §3): what the control plane
    // writes for it there, its Service and identity Secret, bound there for as long as it
    // lasts, and gone with it. Its name holds a dot, as no binding of the ControlPlane's own
    // does, and the DataPlane's name, unique in its namespace.
    if dp.namespace != cp.namespace {
        let controller = controller_name(cp.name);
        objects.push(Object::RoleBinding(RoleBinding {
            metadata: metadata(
                format!("{controller}-generated.{}", dp.name),
                Some(dp.namespace),
                &labels,
                Some(&owner),
            ),
            role_ref: role_ref(CONTROL_PLANE_GENERATED_ROLE),
            subjects: Some(service_account_subject(&controller, cp.namespace)),
        }));
    }

    let resources = spec.resources.as_ref();
    let workers = workers(
        resources.and_then(|r| r.requests.as_ref()),
        resources.and_then(|r| r.limits.as_ref()),
    )
    .map_err(Error::Cpu)?;
    let autoscaled = spec.replicas.max.is_some_and(|max| max > spec.replicas.min);
    let container = Container {
        name: "edgerush".to_owned(),
        image: Some(install.image.clone()),
        args: Some(vec![
            "proxy".to_owned(),
            "--control-plane".to_owned(),
            format!(
                "{}.{}.svc:{STREAM_PORT}",
                controller_name(cp.name),
                cp.namespace
            ),
            "--workers".to_owned(),
            workers.to_string(),
            // Its certificate on the stream, which the control plane writes (05 §2).
            "--certificate".to_owned(),
            format!("{IDENTITY_MOUNT}/tls.crt"),
            "--key".to_owned(),
            format!("{IDENTITY_MOUNT}/tls.key"),
            "--authorities".to_owned(),
            format!("{IDENTITY_MOUNT}/ca.crt"),
        ]),
        // The pod's own addresses, which it never sends a request to (03 §6).
        env: Some(vec![from_field("POD_IPS", "status.podIPs")]),
        volume_mounts: Some(vec![VolumeMount {
            name: "identity".to_owned(),
            mount_path: IDENTITY_MOUNT.to_owned(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]),
        resources: resources.map(|r| requirements(r.limits.as_ref(), r.requests.as_ref())),
        security_context: Some(locked_down()),
        ..Container::default()
    };
    objects.push(Object::Deployment(Box::new(Deployment {
        metadata: metadata(name.clone(), Some(dp.namespace), &labels, Some(&owner)),
        spec: DeploymentSpec {
            // Never stated while an autoscaler runs the fleet (05 §2).
            replicas: (!autoscaled).then_some(spec.replicas.min),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            },
            // A new pod serves before an old one drains (05 §5).
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_owned()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_surge: Some(IntOrString::Int(1)),
                    max_unavailable: Some(IntOrString::Int(0)),
                }),
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    annotations: rate_limiting_annotation(spec.rate_limiting.as_ref()),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(name.clone()),
                    // Run as nobody, it binds the ports its listeners state, 80 among them
                    // (05 §2).
                    security_context: Some(pod_security(Some(vec![Sysctl {
                        name: "net.ipv4.ip_unprivileged_port_start".to_owned(),
                        value: "0".to_owned(),
                    }]))),
                    // Spread evenly over the zones: zone preference keeps a request among
                    // its pod's zone's backends (05 §5).
                    topology_spread_constraints: Some(vec![TopologySpreadConstraint {
                        max_skew: 1,
                        topology_key: "topology.kubernetes.io/zone".to_owned(),
                        when_unsatisfiable: "ScheduleAnyway".to_owned(),
                        label_selector: Some(LabelSelector {
                            match_labels: Some(labels.clone()),
                            ..LabelSelector::default()
                        }),
                        ..TopologySpreadConstraint::default()
                    }]),
                    containers: vec![container],
                    // Its certificate, which the control plane writes: a pod waits for it,
                    // as it can serve nothing without the stream (05 §2).
                    volumes: Some(vec![Volume {
                        name: "identity".to_owned(),
                        secret: Some(SecretVolumeSource {
                            secret_name: Some(identity_secret(dp.name)),
                            ..SecretVolumeSource::default()
                        }),
                        ..Volume::default()
                    }]),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        },
        ..Deployment::default()
    })));

    if let Some(max) = spec.replicas.max.filter(|_| autoscaled) {
        objects.push(Object::HorizontalPodAutoscaler(Box::new(
            HorizontalPodAutoscaler {
                metadata: metadata(name.clone(), Some(dp.namespace), &labels, Some(&owner)),
                // No metrics stated: Kubernetes' default, 80% of the CPU requested.
                spec: HorizontalPodAutoscalerSpec {
                    scale_target_ref: CrossVersionObjectReference {
                        api_version: Some("apps/v1".to_owned()),
                        kind: "Deployment".to_owned(),
                        name: name.clone(),
                    },
                    min_replicas: Some(spec.replicas.min),
                    max_replicas: max,
                    ..HorizontalPodAutoscalerSpec::default()
                },
                ..HorizontalPodAutoscaler::default()
            },
        )));
    }
    // One pod at a time taken for a node's drain, never more, never none.
    objects.push(Object::PodDisruptionBudget(PodDisruptionBudget {
        metadata: metadata(name, Some(dp.namespace), &labels, Some(&owner)),
        spec: Some(PodDisruptionBudgetSpec {
            max_unavailable: Some(IntOrString::Int(1)),
            selector: Some(LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            }),
            ..PodDisruptionBudgetSpec::default()
        }),
        ..PodDisruptionBudget::default()
    }));

    if spec.ingress.is_some() {
        objects.push(Object::IngressClass(IngressClass {
            metadata: metadata(ingress_class(cp.name, dp.name), None, &labels, None),
            spec: Some(IngressClassSpec {
                controller: Some(INGRESS_CONTROLLER.to_owned()),
                ..IngressClassSpec::default()
            }),
        }));
    }
    if let Some(gateway) = &spec.gateway {
        // The DataPlane's `gateway` is the Gateway's own fields, of the same schema (05 §1).
        let mut fields =
            serde_json::to_value(gateway).map_err(|e| Error::Gateway(e.to_string()))?;
        if let Some(fields) = fields.as_object_mut() {
            fields.insert("gatewayClassName".to_owned(), cp.name.into());
        }
        let spec: GatewaySpec =
            serde_json::from_value(fields).map_err(|e| Error::Gateway(e.to_string()))?;
        objects.push(Object::Gateway(Box::new(Made {
            api_version: "gateway.networking.k8s.io/v1".to_owned(),
            kind: "Gateway".to_owned(),
            metadata: metadata(
                dp.name.to_owned(),
                Some(dp.namespace),
                &labels,
                Some(&owner),
            ),
            spec,
        })));
    }
    // A generated DataPlane's labels and annotations, on everything made for it and its
    // pods (05 §3); never on a selector, which a Deployment's cannot change.
    let (carried_labels, carried_annotations) = carried(data_plane);
    for object in &mut objects {
        carry(object.metadata_mut(), &carried_labels, &carried_annotations);
        if let Object::Deployment(deployment) = object
            && let Some(template) = deployment.spec.template.metadata.as_mut()
        {
            carry(template, &carried_labels, &carried_annotations);
        }
    }
    Ok(objects)
}

/// What a port of the data plane's Service is served over.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Transports {
    /// TCP: any listener but a `udp` one.
    pub tcp: bool,
    /// UDP: a `udp` listener, or one serving HTTP/3 ([24](../../../docs/24-udp-proxying.md)).
    pub udp: bool,
}

/// What the labels of [`ProxyService`]'s Service say made it: the control plane, not
/// the operator, which makes everything else of a DataPlane's.
pub const SERVICE_MANAGED_BY: &str = "edgerush-controller";

/// The data plane's Service, its address, as its `DataPlane` shapes it: all of it but the
/// ports, which are what the config handed on binds
/// ([05 §2](../../../docs/05-operator.md)). The control plane writes it, not the operator,
/// so that a port follows its listener with the operator down. Named as the data plane's
/// Deployment is, owned by the DataPlane, its pods those of that Deployment.
#[derive(Debug, Clone, PartialEq)]
pub struct ProxyService {
    name: String,
    namespace: String,
    owner: OwnerReference,
    /// The DataPlane's own, then EdgeRush's, which win.
    labels: BTreeMap<String, String>,
    annotations: Option<BTreeMap<String, String>>,
    /// The data plane's pods: the labels of its Deployment's.
    selector: BTreeMap<String, String>,
    type_: String,
    load_balancer_class: Option<String>,
    load_balancer_source_ranges: Option<Vec<String>>,
    ip_family_policy: &'static str,
    ip_families: Vec<String>,
    /// Ingress's two ports, listed while the DataPlane has `ingress`, whatever is bound.
    ingress: Option<(i32, i32)>,
}

impl ProxyService {
    /// The Service `data_plane` shapes.
    ///
    /// # Errors
    ///
    /// [`Error::Missing`] for a DataPlane without a name, namespace or UID;
    /// [`Error::IpFamily`] for an `ipFamily` its schema does not hold.
    pub fn of(data_plane: &DataPlane) -> Result<Self, Error> {
        let dp = Identity::of(&data_plane.metadata, "DataPlane")?;
        let spec = &data_plane.spec;
        let of = &spec.control_plane_ref;
        let selector = data_plane_labels(dp.namespace, &of.name, dp.name);
        // A generated DataPlane's, then the DataPlane's for its Service, then EdgeRush's.
        let (mut labels, mut carried_annotations) = carried(data_plane);
        labels.extend(spec.service.labels.clone().unwrap_or_default());
        labels.extend(selector.clone());
        labels.insert(MANAGED_BY_LABEL.to_owned(), SERVICE_MANAGED_BY.to_owned());
        // Its ControlPlane's namespace, which the admission policy checks its writer by
        // (05 §2); never on the selector.
        labels.insert(
            CONTROL_PLANE_NAMESPACE_LABEL.to_owned(),
            of.namespace.as_deref().unwrap_or(dp.namespace).to_owned(),
        );
        let annotations = if carried_annotations.is_empty() {
            spec.service.annotations.clone()
        } else {
            carried_annotations.extend(spec.service.annotations.clone().unwrap_or_default());
            Some(carried_annotations)
        };
        // Each a policy the API server keeps the families to, never stated apart (05 §2).
        let (ip_family_policy, ip_families) = match spec.ip_family.as_str() {
            "IPv4" => ("SingleStack", vec!["IPv4"]),
            "IPv6" => ("SingleStack", vec!["IPv6"]),
            "Dual" => ("RequireDualStack", vec!["IPv4", "IPv6"]),
            other => return Err(Error::IpFamily(other.to_owned())),
        };
        Ok(Self {
            name: proxy_name(dp.name),
            namespace: dp.namespace.to_owned(),
            owner: dp.owner("DataPlane"),
            labels,
            annotations,
            selector,
            type_: spec.service.r#type.clone(),
            load_balancer_class: spec.service.load_balancer_class.clone(),
            load_balancer_source_ranges: spec.service.load_balancer_source_ranges.clone(),
            ip_family_policy,
            ip_families: ip_families.into_iter().map(str::to_owned).collect(),
            ingress: spec
                .ingress
                .as_ref()
                .map(|ingress| (ingress.http_port, ingress.https_port)),
        })
    }

    /// Its name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its namespace, the DataPlane's.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The UID of the DataPlane that owns it: a Service of its name owned otherwise is not
    /// this one, and is left alone.
    #[must_use]
    pub fn owner_uid(&self) -> &str {
        &self.owner.uid
    }

    /// The Service, listing the ports `bound` and Ingress's two, each over what it is served
    /// on, a port of both named apart (`port-<n>`, `port-<n>-udp`); none where there is no
    /// port at all, as Kubernetes allows no Service without one.
    #[must_use]
    pub fn service(&self, bound: &BTreeMap<u16, Transports>) -> Option<Service> {
        let mut ports: BTreeMap<i32, Transports> = bound
            .iter()
            .map(|(port, transports)| (i32::from(*port), *transports))
            .collect();
        if let Some((http, https)) = self.ingress {
            for port in [http, https] {
                ports.entry(port).or_default().tcp = true;
            }
        }
        let ports: Vec<ServicePort> = ports
            .into_iter()
            .flat_map(|(port, transports)| {
                let tcp = transports.tcp.then(|| ServicePort {
                    name: Some(format!("port-{port}")),
                    port,
                    protocol: Some("TCP".to_owned()),
                    // The data plane binds the port a client uses (05 §2).
                    target_port: Some(IntOrString::Int(port)),
                    ..ServicePort::default()
                });
                let udp = transports.udp.then(|| ServicePort {
                    name: Some(format!("port-{port}-udp")),
                    port,
                    protocol: Some("UDP".to_owned()),
                    target_port: Some(IntOrString::Int(port)),
                    ..ServicePort::default()
                });
                tcp.into_iter().chain(udp)
            })
            .collect();
        if ports.is_empty() {
            return None;
        }
        // Kubernetes takes a traffic policy only where traffic comes from outside.
        let external = matches!(self.type_.as_str(), "LoadBalancer" | "NodePort");
        let mut metadata = metadata(
            self.name.clone(),
            Some(&self.namespace),
            &self.labels,
            Some(&self.owner),
        );
        metadata.annotations.clone_from(&self.annotations);
        Some(Service {
            metadata,
            spec: Some(ServiceSpec {
                type_: Some(self.type_.clone()),
                selector: Some(self.selector.clone()),
                ports: Some(ports),
                ip_family_policy: Some(self.ip_family_policy.to_owned()),
                ip_families: Some(self.ip_families.clone()),
                // The client's address kept, which rate-limit keys need (05 §2).
                external_traffic_policy: external.then(|| "Local".to_owned()),
                session_affinity: Some("None".to_owned()),
                load_balancer_class: self.load_balancer_class.clone(),
                load_balancer_source_ranges: self.load_balancer_source_ranges.clone(),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        })
    }
}

/// A resource's `limits` and `requests`, as its CRD has them, as a container's.
fn requirements(
    limits: Option<&BTreeMap<String, Quantity>>,
    requests: Option<&BTreeMap<String, Quantity>>,
) -> ResourceRequirements {
    ResourceRequirements {
        limits: limits.cloned(),
        requests: requests.cloned(),
        ..ResourceRequirements::default()
    }
}
