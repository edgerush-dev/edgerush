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
//! The data plane's Service is not here, as the control plane writes it (05 §2); nor are
//! the certificates the workloads mount, nor the webhook's registration, which come with
//! the slices that build them, as do the probes.

use std::collections::{BTreeMap, BTreeSet};

use edgerush_gateway_api::gateway::GatewaySpec;
use edgerush_gateway_api::gateway_class::GatewayClassSpec;
use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use k8s_openapi::api::autoscaling::v2::{
    CrossVersionObjectReference, HorizontalPodAutoscaler, HorizontalPodAutoscalerSpec,
};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector,
    PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements, SeccompProfile,
    SecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec, Sysctl,
    TopologySpreadConstraint,
};
use k8s_openapi::api::networking::v1::{IngressClass, IngressClassSpec};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::api::rbac::v1::{ClusterRoleBinding, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::Serialize;

use crate::control_plane::ControlPlane;
use crate::data_plane::{DataPlane, DataPlaneResources};
use crate::workers::workers;

#[cfg(test)]
mod tests;

/// The control plane's reads and status writes in a watched namespace (05 §2).
pub const CONTROL_PLANE_ROLE: &str = "edgerush-control-plane";
/// The control plane's reads of cluster-scoped kinds.
pub const CONTROL_PLANE_CLUSTER_ROLE: &str = "edgerush-control-plane-cluster";
/// What the control plane reads and writes in its own namespace.
pub const CONTROL_PLANE_OWN_ROLE: &str = "edgerush-control-plane-own";
/// The data plane's one right, list and watch on EndpointSlices.
pub const DATA_PLANE_ROLE: &str = "edgerush-data-plane";

/// The four ClusterRoles the install ships, the only ones the operator binds.
pub const ROLES: [&str; 4] = [
    CONTROL_PLANE_ROLE,
    CONTROL_PLANE_CLUSTER_ROLE,
    CONTROL_PLANE_OWN_ROLE,
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
/// The label saying the operator made an object.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
/// Its value.
pub const MANAGED_BY: &str = "edgerush-operator";

/// The port the control plane serves its data planes' streams on.
const STREAM_PORT: i32 = 9443;
/// The port its pods serve the admission webhook on, and the Service's for it.
const WEBHOOK_PORT: i32 = 8443;
const WEBHOOK_SERVICE_PORT: i32 = 443;

/// `nobody`, which the image runs as: stated by number, as `runAsNonRoot` can check only a
/// number.
const NOBODY: i64 = 65534;

/// What every object takes from the install rather than from a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Install {
    /// The image the planes run, the operator's own: one binary, one image (05 §7).
    pub image: String,
}

/// An object the operator makes, written as the API server takes it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Object {
    /// A plane's ServiceAccount.
    ServiceAccount(ServiceAccount),
    /// A binding of one of the four roles in a namespace.
    RoleBinding(RoleBinding),
    /// A binding of one of the four roles over the cluster.
    ClusterRoleBinding(ClusterRoleBinding),
    /// A plane's workload.
    Deployment(Box<Deployment>),
    /// The control plane's Service, which its data planes dial.
    Service(Box<Service>),
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

/// The name of a data plane's ServiceAccount and workload.
fn proxy_name(data_plane: &str) -> String {
    format!("edgerush-proxy-{data_plane}")
}

/// The name of a data plane's bindings, in namespaces other DataPlanes' may share.
fn proxy_binding(control_plane: &str, data_plane: &str) -> String {
    format!("edgerush-proxy-{control_plane}.{data_plane}")
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

/// What every object for the control plane `cp`, and for its data plane `dp`, is labelled
/// with; its pods' selector is these.
fn labels(component: &str, cp: &str, dp: Option<&str>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "edgerush".to_owned()),
        (
            "app.kubernetes.io/component".to_owned(),
            component.to_owned(),
        ),
        (MANAGED_BY_LABEL.to_owned(), MANAGED_BY.to_owned()),
        (CONTROL_PLANE_LABEL.to_owned(), cp.to_owned()),
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
/// UID.
pub fn for_control_plane(
    control_plane: &ControlPlane,
    selected: &BTreeSet<String>,
    install: &Install,
) -> Result<Vec<Object>, Error> {
    let cp = Identity::of(&control_plane.metadata, "ControlPlane")?;
    let owner = cp.owner("ControlPlane");
    let labels = labels("controller", cp.name, None);
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
        "--listen".to_owned(),
        format!("0.0.0.0:{STREAM_PORT}"),
        "--leader-lease".to_owned(),
        format!("{}/{name}", cp.namespace),
    ];
    for namespace in watched.iter().flatten() {
        args.extend(["--namespace".to_owned(), namespace.clone()]);
    }
    let mut ports = vec![port("stream", STREAM_PORT)];
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
        security_context: Some(locked_down()),
        ..Container::default()
    };
    objects.push(Object::Deployment(Box::new(Deployment {
        metadata: metadata(name.clone(), Some(cp.namespace), &labels, Some(&owner)),
        spec: Some(DeploymentSpec {
            replicas: Some(control_plane.spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            },
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
        }),
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
        metadata: metadata(name, Some(cp.namespace), &labels, Some(&owner)),
        spec: Some(ServiceSpec {
            selector: Some(labels.clone()),
            ports: Some(service_ports),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    })));

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
    if dp.namespace != cp.namespace || data_plane.spec.control_plane_ref.name != cp.name {
        return Err(Error::NotItsControlPlane {
            data_plane: format!("{}/{}", dp.namespace, dp.name),
            control_plane: format!("{}/{}", cp.namespace, cp.name),
        });
    }
    let owner = dp.owner("DataPlane");
    let labels = labels("proxy", cp.name, Some(dp.name));
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
        &proxy_binding(cp.name, dp.name),
        DATA_PLANE_ROLE,
        &name,
        dp.namespace,
        scope.as_ref(),
        &labels,
        &owner,
    ));

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
        ]),
        // The pod's own addresses, which it never sends a request to (03 §6).
        env: Some(vec![from_field("POD_IPS", "status.podIPs")]),
        resources: resources.map(requirements),
        security_context: Some(locked_down()),
        ..Container::default()
    };
    objects.push(Object::Deployment(Box::new(Deployment {
        metadata: metadata(name.clone(), Some(dp.namespace), &labels, Some(&owner)),
        spec: Some(DeploymentSpec {
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
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        }),
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
    Ok(objects)
}

/// The DataPlane's resources as a container's.
fn requirements(resources: &DataPlaneResources) -> ResourceRequirements {
    let quantities = |q: &Option<BTreeMap<String, IntOrString>>| {
        q.as_ref().map(|q| {
            q.iter()
                .map(|(name, amount)| {
                    let amount = match amount {
                        IntOrString::Int(n) => n.to_string(),
                        IntOrString::String(s) => s.clone(),
                    };
                    (name.clone(), Quantity(amount))
                })
                .collect()
        })
    };
    ResourceRequirements {
        limits: quantities(&resources.limits),
        requests: quantities(&resources.requests),
        ..ResourceRequirements::default()
    }
}
