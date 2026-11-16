//! One step of the leader: what the follows have seen, judged, made and reported
//! ([05 §1-2](../../../docs/05-operator.md)).
//!
//! For each resource accepted, its finalizer first, so that nothing is made that it could
//! not delete; then its objects, each read before it is applied, an object of its name
//! that is not the operator's left alone, and a Deployment's replicas handed over before
//! they are left out for an autoscaler; then its conditions, where they say something new.
//! A resource is applied again when it, its ControlPlane for a DataPlane, or one of the
//! objects made for it has changed since it was last applied whole, so that a hand edit
//! is undone and an object deleted by hand made again; and at every step while anything of
//! it is not applied. Its GatewayClass or Gateway is left out while the cluster does not
//! serve the kind, and made once it does. A ControlPlane's webhook registration is made
//! once its status publishes its authority's bundle, and until then its status says it
//! waits for it (05 §2). Its bindings it no longer asks for are deleted: those of
//! namespaces its ControlPlane's selector no longer selects, and, for one whose
//! ControlPlane is not accepted, every right beyond its own namespace, so that an allowance
//! narrowed takes back what it granted (05 §4); and so is a webhook registration no longer
//! asked for, its webhook turned off or its ControlPlane not accepted, a DataPlane's
//! IngressClass or Gateway once its `ingress` or `gateway` is taken away, and its
//! HorizontalPodAutoscaler once it no longer autoscales. One being deleted
//! has what no owner reference reaches deleted by its labels, then its finalizer taken
//! away.
//!
//! Nothing is kept from step to step but what saves a write: a new leader applies
//! everything once, and converges from what the cluster holds.

use crate::allowance::{self, Labels};
use crate::judge::{Refused, judge};
use crate::status::{self, Made};
use crate::{FINALIZER, MANAGER};
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_kubernetes::{ApiError, ApiServer, Kind, Scope};
use edgerush_planes::control_plane::{ControlPlane, ControlPlaneWatchNamespaces};
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::objects::{
    self, Install, Object, authority_bundle, control_plane_labels, controller_name,
    data_plane_labels, proxy_name, registration_name,
};
use edgerush_planes::platform_defaults::PlatformDefaults;
use k8s_openapi::Resource;
use k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration;
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
#[derive(Debug, Clone)]
pub struct Seen {
    /// Every ControlPlane in the cluster.
    pub control_planes: Vec<ControlPlane>,
    /// Every DataPlane in the cluster.
    pub data_planes: Vec<DataPlane>,
    /// The `PlatformDefaults`, if there is one.
    pub platform_defaults: Option<PlatformDefaults>,
    /// Every Namespace's labels, by its name.
    pub namespaces: BTreeMap<String, Labels>,
    /// Every object the operator made, of every kind it makes.
    pub children: Vec<Child>,
    /// Which of the Gateway API's kinds it makes the cluster serves.
    pub gateway_api: GatewayApi,
}

/// Which of the Gateway API's kinds the operator makes the cluster serves, its CRD there
/// and serving `v1`: a cluster without them is a valid install, of Ingress alone
/// ([05 §6](../../../docs/05-operator.md)). An object of a kind not served is not made, and
/// is made once it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayApi {
    /// GatewayClasses, which ControlPlanes make.
    pub gateway_classes: bool,
    /// Gateways, which DataPlanes with a `gateway` make.
    pub gateways: bool,
}

impl GatewayApi {
    /// Whether `object` is of a kind served, or of none of the Gateway API's.
    fn serves(self, object: &Object) -> bool {
        match object {
            Object::GatewayClass(_) => self.gateway_classes,
            Object::Gateway(_) => self.gateways,
            _ => true,
        }
    }
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
    /// The namespaces its ControlPlane's selector selects, which its bindings follow too.
    selected: BTreeSet<String>,
    /// The objects it may keep of those deleted once no longer asked for, each
    /// `(kind, namespace, name)`: any other of them made for it is deleted. `None` for one
    /// whose rights are not judged here.
    keep: Option<BTreeSet<Withdrawn>>,
    /// For a ControlPlane whose webhook is on, the bundle its status publishes, which its
    /// registration trusts, if there is one.
    bundle: Option<String>,
    /// For a ControlPlane whose webhook is on, whether it waits for that bundle.
    waits_for_bundle: bool,
}

/// An object deleted once it is no longer asked for, as `(kind, namespace, name)`: a
/// binding, a webhook registration, an IngressClass, a Gateway or a HorizontalPodAutoscaler,
/// the namespace empty for one of the cluster's.
type Withdrawn = (&'static str, String, String);

/// Whether `kind` is of the objects deleted once no longer asked for: those a resource
/// asks for or not as its spec and its allowance say. The rest it always has while it is.
fn withdrawable(kind: &str) -> bool {
    kind == RoleBinding::KIND
        || kind == ClusterRoleBinding::KIND
        || kind == ValidatingWebhookConfiguration::KIND
        || kind == IngressClass::KIND
        || kind == Gateway::KIND
        || kind == HorizontalPodAutoscaler::KIND
}

/// Those of `objects` deleted once no longer asked for.
fn withdrawable_of(objects: &[Object]) -> BTreeSet<Withdrawn> {
    objects
        .iter()
        .filter(|object| withdrawable(object.kind()))
        .map(|object| {
            let metadata = object.metadata();
            (
                object.kind(),
                metadata.namespace.clone().unwrap_or_default(),
                metadata.name.clone().unwrap_or_default(),
            )
        })
        .collect()
}

/// `control_plane` as it would be watching its own namespace alone, its webhook off: what
/// it keeps while it is not accepted, the rights beyond its namespace taken back, and its
/// webhook no longer asked of the scope it was registered for.
fn own_namespace_only(control_plane: &ControlPlane) -> ControlPlane {
    let mut own = control_plane.clone();
    own.spec.watch.namespaces = ControlPlaneWatchNamespaces {
        from: "Same".to_owned(),
        selector: None,
    };
    own.spec.webhook.enabled = false;
    own
}

/// What a resource was last applied whole as: when none of it has changed, applying it
/// again would change nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Applied {
    generation: Option<i64>,
    depends: Option<i64>,
    /// The namespaces its ControlPlane's selector selects.
    selected: BTreeSet<String>,
    /// Which kinds were served, so that an object left out is made once its kind is.
    gateway_api: GatewayApi,
    /// For a ControlPlane, the bundle its webhook was registered with: a status change,
    /// which changes no generation.
    bundle: Option<String>,
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
            selected: plane.selected.clone(),
            gateway_api: seen.gateway_api,
            bundle: plane.bundle.clone(),
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
        let judged = judge(
            &seen.control_planes,
            &seen.data_planes,
            seen.platform_defaults.as_ref(),
            &seen.namespaces,
        );
        let mut planes = Vec::new();
        for (control_plane, verdict) in &judged.control_planes {
            let name = control_plane.metadata.name.as_deref().unwrap_or_default();
            let namespace = control_plane
                .metadata
                .namespace
                .as_deref()
                .unwrap_or_default();
            let selected = selected(control_plane, verdict.is_ok(), seen);
            let hooked = control_plane.spec.webhook.enabled;
            let bundle = authority_bundle(control_plane)
                .filter(|_| hooked)
                .map(str::to_owned);
            let (objects, keep) = match verdict {
                Ok(()) => {
                    let objects =
                        objects::for_control_plane(control_plane, &selected, &self.install);
                    let keep = objects.as_deref().ok().map(withdrawable_of);
                    (
                        objects.map_err(|error| Made::Refused(error.to_string())),
                        keep,
                    )
                }
                Err(_) => {
                    let own = own_namespace_only(control_plane);
                    let keep = objects::for_control_plane(&own, &selected, &self.install)
                        .ok()
                        .map(|objects| withdrawable_of(&objects));
                    (Err(Made::Pending(String::new())), keep)
                }
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
                selected,
                keep,
                waits_for_bundle: hooked && bundle.is_none(),
                bundle,
            });
        }
        for (data_plane, verdict) in &judged.data_planes {
            let name = data_plane.metadata.name.as_deref().unwrap_or_default();
            let namespace = data_plane.metadata.namespace.as_deref().unwrap_or_default();
            let of = &data_plane.spec.control_plane_ref.name;
            let (objects, keep, selected) = match verdict {
                Ok(control_plane) => {
                    let accepted = judged.control_planes.iter().any(|(judged, verdict)| {
                        std::ptr::eq(*judged, *control_plane) && verdict.is_ok()
                    });
                    let selected = selected(control_plane, accepted, seen);
                    if accepted {
                        let objects = objects::for_data_plane(
                            data_plane,
                            control_plane,
                            &selected,
                            &self.install,
                        );
                        let keep = objects.as_deref().ok().map(withdrawable_of);
                        let objects = objects.map_err(|error| Made::Refused(error.to_string()));
                        (objects, keep, selected)
                    } else {
                        let own = own_namespace_only(control_plane);
                        let keep =
                            objects::for_data_plane(data_plane, &own, &selected, &self.install)
                                .ok()
                                .map(|objects| withdrawable_of(&objects));
                        let pending = Made::Pending(format!(
                            "ControlPlane {of} is not accepted: nothing of this generation is \
                             made until it is."
                        ));
                        (Err(pending), keep, selected)
                    }
                }
                Err(_) => (Err(Made::Pending(String::new())), None, BTreeSet::new()),
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
                selected,
                keep,
                bundle: None,
                waits_for_bundle: false,
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
                // An object of a kind the cluster does not serve is made once it does.
                let (objects, left_out): (Vec<&Object>, Vec<&Object>) = objects
                    .iter()
                    .partition(|object| seen.gateway_api.serves(object));
                let applied = Applied::of(&plane, seen);
                let made = if self.applied.get(uid) == Some(&applied) {
                    Made::Applied
                } else {
                    let made = apply_all(api, &objects, &plane.labels, uid).await;
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
                };
                // Without its Gateway, a DataPlane is not programmed, though the rest is
                // made: not a step left undone, as the CRD's follow says when it is there.
                // A ControlPlane is programmed without its GatewayClass, as Ingress alone
                // has none; not without its webhook's registration, which waits for its
                // status's bundle, as its follow says when it comes.
                match left_out
                    .iter()
                    .find(|object| matches!(object, Object::Gateway(_)))
                {
                    Some(gateway) if made == Made::Applied => Made::Pending(format!(
                        "{} is not made until the cluster serves the Gateway API's Gateways, \
                         their CRD at v1; the rest is made.",
                        described(gateway)
                    )),
                    None if made == Made::Applied && plane.waits_for_bundle => {
                        Made::Pending(format!(
                            "ValidatingWebhookConfiguration {} is not made until the control \
                             plane publishes its authority's bundle in this status; the rest \
                             is made.",
                            registration_name(name)
                        ))
                    }
                    _ => made,
                }
            }
            Err(made) => made.clone(),
        };
        if let Some(keep) = &plane.keep {
            done &= withdraw(api, &plane, keep, seen, &what, say).await;
        }
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

/// The namespaces `control_plane`'s selector selects while it is `accepted`, by their
/// labels as `seen`: none while it is not, so that nothing is bound for a scope not allowed.
fn selected(control_plane: &ControlPlane, accepted: bool, seen: &Seen) -> BTreeSet<String> {
    if accepted {
        allowance::selected(&control_plane.spec.watch.namespaces, &seen.namespaces)
    } else {
        BTreeSet::new()
    }
}

/// The objects made for `plane`, said as `what`, of those deleted once no longer asked for,
/// that are not among those it may `keep`, deleted: whether they all were. Each is one the
/// operator made, by its labels, so nothing else of the name is touched.
async fn withdraw(
    api: &ApiServer,
    plane: &Plane<'_>,
    keep: &BTreeSet<Withdrawn>,
    seen: &Seen,
    what: &str,
    say: &dyn Fn(&str),
) -> bool {
    let mut done = true;
    let kept =
        |child: &Child| keep.contains(&(child.kind, child.namespace.clone(), child.name.clone()));
    for child in seen.children.iter().filter(|child| {
        withdrawable(child.kind) && carries(&child.labels, &plane.labels) && !kept(child)
    }) {
        let (namespace, name) = (child.namespace.as_str(), child.name.as_str());
        let (deleted, why) = match child.kind {
            RoleBinding::KIND => (
                api.delete::<RoleBinding>(namespace, name).await,
                "no longer granted",
            ),
            ClusterRoleBinding::KIND => (
                api.delete::<ClusterRoleBinding>("", name).await,
                "no longer granted",
            ),
            IngressClass::KIND => (
                api.delete::<IngressClass>("", name).await,
                "no longer asked for",
            ),
            Gateway::KIND => (
                api.delete::<Gateway>(namespace, name).await,
                "no longer asked for",
            ),
            HorizontalPodAutoscaler::KIND => (
                api.delete::<HorizontalPodAutoscaler>(namespace, name).await,
                "no longer asked for",
            ),
            // A ValidatingWebhookConfiguration, the last of `withdrawable`'s.
            _ => (
                api.delete::<ValidatingWebhookConfiguration>("", name).await,
                "no longer asked for",
            ),
        };
        let deleted = deleted.map(drop);
        let object = if child.namespace.is_empty() {
            format!("{} {}", child.kind, child.name)
        } else {
            format!("{} {}/{}", child.kind, child.namespace, child.name)
        };
        match deleted {
            Ok(()) => say(&format!("{what}: {object} deleted, {why}")),
            Err(error) => {
                say(&format!("{what}: {object} cannot be deleted: {error}"));
                done = false;
            }
        }
    }
    done
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
    objects: &[&Object],
    labels: &BTreeMap<String, String>,
    uid: &str,
) -> Made {
    let mut not_ours = Vec::new();
    for &object in objects {
        let applied = match object {
            Object::ServiceAccount(_) => apply::<ServiceAccount>(api, object, labels, uid).await,
            Object::RoleBinding(_) => apply::<RoleBinding>(api, object, labels, uid).await,
            Object::ClusterRoleBinding(_) => {
                apply::<ClusterRoleBinding>(api, object, labels, uid).await
            }
            Object::Deployment(deployment) => {
                apply_deployment(api, deployment, object, labels, uid).await
            }
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
            Object::ValidatingWebhookConfiguration(_) => {
                apply::<ValidatingWebhookConfiguration>(api, object, labels, uid).await
            }
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

/// The field manager that holds a Deployment's replicas from the operator's last apply that
/// stated them to its autoscaler's first write (05 §2).
const HANDOVER: &str = "edgerush-operator-handover";

/// `object`, `deployment`, applied as [`apply`] applies any, but that where it leaves out
/// the replicas the operator's last apply of it stated, for an autoscaler to run, it first
/// hands them over to a field manager of their own, at the count they are at. Left out by
/// the only manager that holds them, they would be removed, and the API server would run 1
/// until the autoscaler's first write; held by another, they stay, until that write takes
/// them from it, as Kubernetes' server-side apply has it (05 §2).
async fn apply_deployment(
    api: &ApiServer,
    deployment: &Deployment,
    object: &Object,
    labels: &BTreeMap<String, String>,
    uid: &str,
) -> Result<bool, ApiError> {
    let namespace = deployment.metadata.namespace.as_deref().unwrap_or_default();
    let name = deployment.metadata.name.as_deref().unwrap_or_default();
    if let Some(there) = api.object::<Deployment>(namespace, name).await? {
        if !ours(&there.metadata, labels, uid) {
            return Ok(false);
        }
        let leaves_out = deployment
            .spec
            .as_ref()
            .is_none_or(|spec| spec.replicas.is_none());
        let at = there.spec.as_ref().and_then(|spec| spec.replicas);
        if leaves_out
            && holds_replicas(&there)
            && let Some(replicas) = at
        {
            let handover = serde_json::json!({
                "apiVersion": Deployment::API_VERSION,
                "kind": Deployment::KIND,
                "metadata": { "name": name, "namespace": namespace },
                "spec": { "replicas": replicas },
            });
            api.apply::<Deployment>(HANDOVER, namespace, name, &handover)
                .await?;
        }
    }
    api.apply::<Deployment>(MANAGER, namespace, name, object)
        .await?;
    Ok(true)
}

/// Whether the operator's own apply holds `deployment`'s replicas.
fn holds_replicas(deployment: &Deployment) -> bool {
    deployment
        .metadata
        .managed_fields
        .iter()
        .flatten()
        .any(|entry| {
            entry.manager.as_deref() == Some(MANAGER)
                && entry.operation.as_deref() == Some("Apply")
                && entry.subresource.as_deref().unwrap_or_default().is_empty()
                && entry
                    .fields_v1
                    .as_ref()
                    .and_then(|fields| fields.0.get("f:spec"))
                    .and_then(|spec| spec.get("f:replicas"))
                    .is_some()
        })
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
/// objects, its webhook registration among them, and its RoleBindings in namespaces other
/// than its own. GatewayClasses only
/// where the cluster serves them: without their CRD there are none, and a delete of them
/// would fail.
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
            api.delete_all::<ValidatingWebhookConfiguration>(&Scope::Cluster, &plane.labels)
                .await?;
            if seen.gateway_api.gateway_classes {
                api.delete_all::<GatewayClass>(&Scope::Cluster, &plane.labels)
                    .await?;
            }
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
