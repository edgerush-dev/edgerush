//! One step of the leader: what the follows have seen, judged, made and reported
//! ([05 §1-2](../../../docs/05-operator.md)).
//!
//! For each resource accepted, its finalizer first, so that nothing is made that it could
//! not delete; then its objects, each read before it is applied, an object of its name
//! that is not the operator's left alone; then its conditions, where they say something new.
//! A resource is applied again when it, its ControlPlane for a DataPlane, or one of the
//! objects made for it has changed since it was last applied whole, so that a hand edit
//! is undone and an object deleted by hand made again; and at every step while anything of
//! it is not applied. One being deleted has what no owner reference reaches deleted by its
//! labels, then its finalizer taken away.
//!
//! Nothing is kept from step to step but what saves a write: a new leader applies
//! everything once, and converges from what the cluster holds.

use crate::judge::{Refused, judge};
use crate::status::{self, Made};
use crate::{FINALIZER, MANAGER};
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_kubernetes::{ApiError, ApiServer, Kind, Scope};
use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::objects::{
    self, Install, Object, control_plane_labels, controller_name, data_plane_labels, proxy_name,
};
use k8s_openapi::Resource;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Service, ServiceAccount};
use k8s_openapi::api::networking::v1::IngressClass;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{ClusterRoleBinding, RoleBinding};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta, Time};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// An object the operator made, as the follow of its kind last saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Child {
    /// Its kind.
    pub kind: &'static str,
    /// Its namespace; empty for a kind whose objects are the cluster's.
    pub namespace: String,
    /// Its name.
    pub name: String,
    /// Its `resourceVersion`: any change to it is a new one.
    pub version: String,
    /// Its labels, which say which resource it was made for.
    pub labels: BTreeMap<String, String>,
    /// For a Deployment, its replicas available.
    pub available: Option<i32>,
}

impl Child {
    /// `object`, `K`'s, as a child.
    #[must_use]
    pub fn of<K: Kind>(object: &K) -> Self {
        let metadata = object.metadata();
        Self {
            kind: K::KIND,
            namespace: metadata.namespace.clone().unwrap_or_default(),
            name: metadata.name.clone().unwrap_or_default(),
            version: metadata.resource_version.clone().unwrap_or_default(),
            labels: metadata.labels.clone().unwrap_or_default(),
            available: None,
        }
    }

    /// A Deployment as a child, with its replicas available.
    #[must_use]
    pub fn of_deployment(deployment: &Deployment) -> Self {
        Self {
            available: Some(
                deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.available_replicas)
                    .unwrap_or(0),
            ),
            ..Self::of(deployment)
        }
    }
}

/// What the follows have seen.
#[derive(Debug, Clone, Default)]
pub struct Seen {
    /// Every ControlPlane in the cluster.
    pub control_planes: Vec<ControlPlane>,
    /// Every DataPlane in the cluster.
    pub data_planes: Vec<DataPlane>,
    /// Every object the operator made, of every kind it makes.
    pub children: Vec<Child>,
}

/// The kinds of resource the operator makes objects for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaneKind {
    ControlPlane,
    DataPlane,
}

impl PlaneKind {
    fn name(self) -> &'static str {
        match self {
            Self::ControlPlane => "ControlPlane",
            Self::DataPlane => "DataPlane",
        }
    }
}

/// A resource, as this step sees it.
struct Plane<'a> {
    kind: PlaneKind,
    metadata: &'a ObjectMeta,
    conditions: &'a [Condition],
    /// What its objects carry, and are found by.
    labels: BTreeMap<String, String>,
    /// Its Deployment's name.
    workload: String,
    accepted: Result<(), Refused>,
    /// What to make for it, or why nothing is.
    objects: Result<Vec<Object>, Made>,
    /// For a DataPlane, its ControlPlane's generation, which its objects follow too.
    depends: Option<i64>,
}

/// What a resource was last applied whole as: when none of it has changed, applying it
/// again would change nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Applied {
    generation: Option<i64>,
    depends: Option<i64>,
    /// Each of its objects, by kind, namespace and name, at its version.
    children: Vec<(&'static str, String, String, String)>,
}

impl Applied {
    fn of(plane: &Plane<'_>, seen: &Seen) -> Self {
        let mut children: Vec<_> = seen
            .children
            .iter()
            .filter(|child| carries(&child.labels, &plane.labels))
            .map(|child| {
                (
                    child.kind,
                    child.namespace.clone(),
                    child.name.clone(),
                    child.version.clone(),
                )
            })
            .collect();
        children.sort();
        Self {
            generation: plane.metadata.generation,
            depends: plane.depends,
            children,
        }
    }
}

/// The leader's state from step to step: what each resource, by UID, was last applied
/// whole as.
#[derive(Debug)]
pub struct Operator {
    install: Install,
    applied: HashMap<String, Applied>,
}

impl Operator {
    /// An operator making objects as `install` says.
    #[must_use]
    pub fn new(install: Install) -> Self {
        Self {
            install,
            applied: HashMap::new(),
        }
    }

    /// Forgets what was applied: what a replica does on losing the lead, so that it
    /// applies everything again once it leads again.
    pub fn forget(&mut self) {
        self.applied.clear();
    }

    /// One step through `api`, of what was `seen`, at `now`: what it does and what fails
    /// said to `say`. Whether all of it was done: if not, it is to be stepped again after
    /// a wait, however little has changed.
    pub async fn step(
        &mut self,
        api: &ApiServer,
        seen: &Seen,
        now: &Time,
        say: &dyn Fn(&str),
    ) -> bool {
        let judged = judge(&seen.control_planes, &seen.data_planes);
        let selected = BTreeSet::new();
        let mut planes = Vec::new();
        for (control_plane, verdict) in &judged.control_planes {
            let name = control_plane.metadata.name.as_deref().unwrap_or_default();
            let namespace = control_plane
                .metadata
                .namespace
                .as_deref()
                .unwrap_or_default();
            let objects = match verdict {
                Ok(()) => objects::for_control_plane(control_plane, &selected, &self.install)
                    .map_err(|error| Made::Refused(error.to_string())),
                Err(_) => Err(Made::Pending(String::new())),
            };
            planes.push(Plane {
                kind: PlaneKind::ControlPlane,
                metadata: &control_plane.metadata,
                conditions: conditions(control_plane.status.as_ref().map(|s| &s.conditions)),
                labels: control_plane_labels(namespace, name),
                workload: controller_name(name),
                accepted: verdict.clone(),
                objects,
                depends: None,
            });
        }
        for (data_plane, verdict) in &judged.data_planes {
            let name = data_plane.metadata.name.as_deref().unwrap_or_default();
            let namespace = data_plane.metadata.namespace.as_deref().unwrap_or_default();
            let of = &data_plane.spec.control_plane_ref.name;
            let objects = match verdict {
                Ok(control_plane) => {
                    let accepted = judged.control_planes.iter().any(|(judged, verdict)| {
                        std::ptr::eq(*judged, *control_plane) && verdict.is_ok()
                    });
                    if accepted {
                        objects::for_data_plane(data_plane, control_plane, &selected, &self.install)
                            .map_err(|error| Made::Refused(error.to_string()))
                    } else {
                        Err(Made::Pending(format!(
                            "ControlPlane {of} is not accepted: nothing of this generation is \
                             made until it is."
                        )))
                    }
                }
                Err(_) => Err(Made::Pending(String::new())),
            };
            planes.push(Plane {
                kind: PlaneKind::DataPlane,
                metadata: &data_plane.metadata,
                conditions: conditions(data_plane.status.as_ref().map(|s| &s.conditions)),
                labels: data_plane_labels(namespace, of, name),
                workload: proxy_name(name),
                accepted: verdict.as_ref().map(|_| ()).map_err(Clone::clone),
                objects,
                depends: verdict
                    .as_ref()
                    .ok()
                    .and_then(|control_plane| control_plane.metadata.generation),
            });
        }
        let present: HashSet<String> = planes
            .iter()
            .filter_map(|plane| plane.metadata.uid.clone())
            .collect();
        self.applied.retain(|uid, _| present.contains(uid));
        let mut done = true;
        for plane in planes {
            done &= self.plane(api, plane, seen, now, say).await;
        }
        done
    }

    /// One resource's step.
    async fn plane(
        &mut self,
        api: &ApiServer,
        plane: Plane<'_>,
        seen: &Seen,
        now: &Time,
        say: &dyn Fn(&str),
    ) -> bool {
        let metadata = plane.metadata;
        let (Some(name), Some(namespace), Some(uid)) = (
            metadata.name.as_deref(),
            metadata.namespace.as_deref(),
            metadata.uid.as_deref(),
        ) else {
            return true;
        };
        let what = format!("{} {namespace}/{name}", plane.kind.name());
        let finalized = metadata
            .finalizers
            .iter()
            .flatten()
            .any(|finalizer| finalizer == FINALIZER);
        if metadata.deletion_timestamp.is_some() {
            self.applied.remove(uid);
            if !finalized {
                return true;
            }
            if let Err(error) = clean(api, &plane, namespace, seen).await {
                say(&format!(
                    "{what}: what was made for it cannot be deleted: {error}"
                ));
                return false;
            }
            return match unfinalize(api, plane.kind, metadata, namespace, name).await {
                Ok(()) => {
                    say(&format!("{what}: deleted, with what was made for it"));
                    true
                }
                Err(error) if error.is_not_found() => true,
                Err(error) => {
                    say(&format!(
                        "{what}: its finalizer cannot be taken away: {error}"
                    ));
                    false
                }
            };
        }
        let mut done = true;
        let made = match &plane.objects {
            Ok(objects) => {
                // Nothing made that could not be deleted with it.
                if !finalized && let Err(error) = finalize(api, plane.kind, namespace, name).await {
                    say(&format!("{what}: its finalizer cannot be added: {error}"));
                    return false;
                }
                let applied = Applied::of(&plane, seen);
                if self.applied.get(uid) == Some(&applied) {
                    Made::Applied
                } else {
                    let made = apply_all(api, objects, &plane.labels, uid).await;
                    match &made {
                        Made::Applied => {
                            self.applied.insert(uid.to_owned(), applied);
                        }
                        Made::NotOurs(names) => {
                            self.applied.remove(uid);
                            done = false;
                            say(&format!(
                                "{what}: not the operator's, so left alone: {}",
                                names.join(", ")
                            ));
                        }
                        Made::Refused(why) | Made::Pending(why) => {
                            self.applied.remove(uid);
                            done = false;
                            say(&format!("{what}: {why}"));
                        }
                    }
                    made
                }
            }
            Err(made) => made.clone(),
        };
        let available = seen.children.iter().any(|child| {
            child.kind == Deployment::KIND
                && child.namespace == namespace
                && child.name == plane.workload
                && carries(&child.labels, &plane.labels)
                && child.available.unwrap_or(0) > 0
        });
        let wanted = status::conditions(
            metadata.generation,
            plane.accepted.as_ref().map(|_| ()),
            &made,
            &plane.workload,
            available,
            plane.conditions,
            now,
        );
        if status::differs(plane.conditions, &wanted) {
            let status = serde_json::json!({ "conditions": wanted });
            let written = match plane.kind {
                PlaneKind::ControlPlane => {
                    api.apply_status::<ControlPlane>(MANAGER, namespace, name, &status)
                        .await
                }
                PlaneKind::DataPlane => {
                    api.apply_status::<DataPlane>(MANAGER, namespace, name, &status)
                        .await
                }
            };
            match written {
                Ok(()) => say(&format!("{what}: {}", summary(&wanted))),
                // Gone meanwhile: nothing to say of it.
                Err(error) if error.is_not_found() => {}
                Err(error) => {
                    say(&format!("{what}: its status cannot be written: {error}"));
                    done = false;
                }
            }
        }
        done
    }
}

/// A resource's conditions, as its status holds them.
fn conditions(held: Option<&Option<Vec<Condition>>>) -> &[Condition] {
    held.and_then(Option::as_deref).unwrap_or_default()
}

/// Conditions as said on stderr: `Accepted True Accepted, Programmed False Pending`.
fn summary(conditions: &[Condition]) -> String {
    let each: Vec<String> = conditions
        .iter()
        .map(|condition| {
            format!(
                "{} {} {}: {}",
                condition.type_, condition.status, condition.reason, condition.message
            )
        })
        .collect();
    each.join("; ")
}

/// Whether `labels` carries each of `wanted`.
fn carries(labels: &BTreeMap<String, String>, wanted: &BTreeMap<String, String>) -> bool {
    wanted
        .iter()
        .all(|(key, value)| labels.get(key) == Some(value))
}

/// Whether an object is the operator's, made for the resource of UID `uid` that labels
/// what it makes with `labels`: it carries them, or that resource owns it.
fn ours(metadata: &ObjectMeta, labels: &BTreeMap<String, String>, uid: &str) -> bool {
    metadata
        .labels
        .as_ref()
        .is_some_and(|held| carries(held, labels))
        || metadata
            .owner_references
            .iter()
            .flatten()
            .any(|owner| owner.uid == uid)
}

/// An object as said: `Deployment edge/edgerush-proxy-web`, `IngressClass edge.web`.
fn described(object: &Object) -> String {
    let metadata = object.metadata();
    let name = metadata.name.as_deref().unwrap_or_default();
    match metadata.namespace.as_deref() {
        Some(namespace) => format!("{} {namespace}/{name}", object.kind()),
        None => format!("{} {name}", object.kind()),
    }
}

/// Every one of `objects` applied, those that are not the operator's left alone.
async fn apply_all(
    api: &ApiServer,
    objects: &[Object],
    labels: &BTreeMap<String, String>,
    uid: &str,
) -> Made {
    let mut not_ours = Vec::new();
    for object in objects {
        let applied = match object {
            Object::ServiceAccount(_) => apply::<ServiceAccount>(api, object, labels, uid).await,
            Object::RoleBinding(_) => apply::<RoleBinding>(api, object, labels, uid).await,
            Object::ClusterRoleBinding(_) => {
                apply::<ClusterRoleBinding>(api, object, labels, uid).await
            }
            Object::Deployment(_) => apply::<Deployment>(api, object, labels, uid).await,
            Object::Service(_) => apply::<Service>(api, object, labels, uid).await,
            Object::HorizontalPodAutoscaler(_) => {
                apply::<HorizontalPodAutoscaler>(api, object, labels, uid).await
            }
            Object::PodDisruptionBudget(_) => {
                apply::<PodDisruptionBudget>(api, object, labels, uid).await
            }
            Object::GatewayClass(_) => apply::<GatewayClass>(api, object, labels, uid).await,
            Object::IngressClass(_) => apply::<IngressClass>(api, object, labels, uid).await,
            Object::Gateway(_) => apply::<Gateway>(api, object, labels, uid).await,
        };
        match applied {
            Ok(true) => {}
            Ok(false) => not_ours.push(described(object)),
            Err(error) => return Made::Refused(format!("{}: {error}", described(object))),
        }
    }
    if not_ours.is_empty() {
        Made::Applied
    } else {
        Made::NotOurs(not_ours)
    }
}

/// `object`, `K`'s, applied, unless one of its name is there that is not the operator's:
/// whether it was. An object made between the read and the apply is the one race that
/// takes one over.
async fn apply<K: Kind>(
    api: &ApiServer,
    object: &Object,
    labels: &BTreeMap<String, String>,
    uid: &str,
) -> Result<bool, ApiError> {
    let metadata = object.metadata();
    let namespace = metadata.namespace.as_deref().unwrap_or_default();
    let name = metadata.name.as_deref().unwrap_or_default();
    if let Some(there) = api.object::<K>(namespace, name).await?
        && !ours(there.metadata(), labels, uid)
    {
        return Ok(false);
    }
    api.apply::<K>(MANAGER, namespace, name, object).await?;
    Ok(true)
}

/// The operator's finalizer added to a resource, by an apply of it alone, under its own
/// field manager.
async fn finalize(
    api: &ApiServer,
    kind: PlaneKind,
    namespace: &str,
    name: &str,
) -> Result<(), ApiError> {
    let object = serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": kind.name(),
        "metadata": { "name": name, "namespace": namespace, "finalizers": [FINALIZER] },
    });
    match kind {
        PlaneKind::ControlPlane => api
            .apply::<ControlPlane>(MANAGER, namespace, name, &object)
            .await
            .map(drop),
        PlaneKind::DataPlane => api
            .apply::<DataPlane>(MANAGER, namespace, name, &object)
            .await
            .map(drop),
    }
}

/// The operator's finalizer taken away from a resource whose metadata is `metadata`: its
/// finalizers written whole without it, as of the version read, whoever put it there, so
/// that a change meanwhile is a conflict and read again rather than undone.
async fn unfinalize(
    api: &ApiServer,
    kind: PlaneKind,
    metadata: &ObjectMeta,
    namespace: &str,
    name: &str,
) -> Result<(), ApiError> {
    let others: Vec<&String> = metadata
        .finalizers
        .iter()
        .flatten()
        .filter(|finalizer| *finalizer != FINALIZER)
        .collect();
    let patch = serde_json::json!({
        "metadata": { "resourceVersion": metadata.resource_version, "finalizers": others },
    });
    match kind {
        PlaneKind::ControlPlane => api.patch::<ControlPlane>(namespace, name, &patch).await,
        PlaneKind::DataPlane => api.patch::<DataPlane>(namespace, name, &patch).await,
    }
}

/// What no owner reference reaches deleted by its labels: the resource's cluster-scoped
/// objects, and its RoleBindings in namespaces other than its own.
async fn clean(
    api: &ApiServer,
    plane: &Plane<'_>,
    namespace: &str,
    seen: &Seen,
) -> Result<(), ApiError> {
    api.delete_all::<ClusterRoleBinding>(&Scope::Cluster, &plane.labels)
        .await?;
    match plane.kind {
        PlaneKind::ControlPlane => {
            api.delete_all::<GatewayClass>(&Scope::Cluster, &plane.labels)
                .await?;
        }
        PlaneKind::DataPlane => {
            api.delete_all::<IngressClass>(&Scope::Cluster, &plane.labels)
                .await?;
        }
    }
    let elsewhere: BTreeSet<&str> = seen
        .children
        .iter()
        .filter(|child| {
            child.kind == RoleBinding::KIND
                && child.namespace != namespace
                && carries(&child.labels, &plane.labels)
        })
        .map(|child| child.namespace.as_str())
        .collect();
    for other in elsewhere {
        api.delete_all::<RoleBinding>(&Scope::namespace(other)?, &plane.labels)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
