//! Gateway API status ([04 §6](../../../docs/04-control-plane.md)): what translation says
//! the data plane's GatewayClass, Gateway and HTTPRoutes are to say, merged with what each
//! says now as the spec has a controller write it. Each condition of the generation it
//! says of, its `lastTransitionTime` kept while its status holds; a condition whose
//! `observedGeneration` is newer left as it is; condition types not this controller's,
//! and other controllers' parents of a route, kept whole; this controller's own parents of
//! a route that are no longer wanted taken away. Pure: the time is passed in.

use crate::acks::Pods;
use crate::gateway_api::{GatewayKind, Served, supported};
use edgerush_gateway_api::gateway::{
    GatewayStatus, GatewayStatusAddresses, GatewayStatusListeners,
    GatewayStatusListenersSupportedKinds,
};
use edgerush_gateway_api::gateway_class::GatewayClassStatus;
use edgerush_gateway_api::http_route::{
    HTTPRouteStatus, HTTPRouteStatusParents, HTTPRouteStatusParentsParentRef,
};
use edgerush_translate::{self as translate, GATEWAY_CONTROLLER, Object};
use k8s_openapi::api::networking::v1::IngressLoadBalancerIngress;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use std::net::IpAddr;

/// The API group of the Gateway API's kinds.
const GROUP: &str = "gateway.networking.k8s.io";

/// The most parents a route's status holds, as its CRD has it.
const MOST_PARENTS: usize = 32;

/// The condition types this controller writes on a GatewayClass.
const CLASS_TYPES: [&str; 2] = ["Accepted", "SupportedVersion"];
/// On a Gateway.
const GATEWAY_TYPES: [&str; 2] = ["Accepted", "Programmed"];
/// On a Gateway's listener.
const LISTENER_TYPES: [&str; 4] = ["Accepted", "Conflicted", "ResolvedRefs", "Programmed"];
/// On a route's parent.
const PARENT_TYPES: [&str; 3] = ["Accepted", "ResolvedRefs", "PartiallyInvalid"];

/// A condition to write.
struct Wanted {
    kind: &'static str,
    status: &'static str,
    reason: &'static str,
    message: String,
}

impl Wanted {
    fn new(
        kind: &'static str,
        status: &'static str,
        reason: &'static str,
        message: String,
    ) -> Self {
        Self {
            kind,
            status,
            reason,
            message,
        }
    }
}

impl From<&translate::Condition> for Wanted {
    fn from(condition: &translate::Condition) -> Self {
        Self::new(
            condition.kind,
            if condition.status { "True" } else { "False" },
            condition.reason,
            condition.message.clone(),
        )
    }
}

/// `wanted`, of `generation`, merged with `live`, conditions of the `ours` types being this
/// controller's: each of `live` not ours kept where it is, each ours not wanted taken away,
/// each wanted in the place of its type's or after the rest, and kept as it is where it
/// says of a newer generation. A condition's `lastTransitionTime` is `now` only where its
/// status changes.
fn merged(
    live: &[Condition],
    wanted: Vec<Wanted>,
    ours: &[&str],
    generation: i64,
    now: Timestamp,
) -> Vec<Condition> {
    let made = |wanted: Wanted| {
        let old = live.iter().find(|c| c.type_ == wanted.kind);
        if let Some(old) = old
            && old
                .observed_generation
                .is_some_and(|theirs| theirs > generation)
        {
            return old.clone();
        }
        let since = old
            .filter(|old| old.status == wanted.status)
            .map_or(Time(now), |old| old.last_transition_time.clone());
        Condition {
            last_transition_time: since,
            message: wanted.message,
            observed_generation: Some(generation),
            reason: wanted.reason.to_owned(),
            status: wanted.status.to_owned(),
            type_: wanted.kind.to_owned(),
        }
    };
    let mut wanted: Vec<Option<Wanted>> = wanted.into_iter().map(Some).collect();
    let mut conditions = Vec::new();
    for old in live {
        if !ours.contains(&old.type_.as_str()) {
            conditions.push(old.clone());
        } else if let Some(slot) = wanted
            .iter_mut()
            .find(|w| w.as_ref().is_some_and(|w| w.kind == old.type_))
            && let Some(wanted) = slot.take()
        {
            conditions.push(made(wanted));
        }
    }
    conditions.extend(wanted.into_iter().flatten().map(made));
    conditions
}

/// What the data plane's GatewayClass's status is to say, of `generation`, the Gateway API
/// kinds being as `served` says, merged with `live`. Accepted, as translation reads the
/// class's Gateway only where it names this controller.
pub(crate) fn class(
    live: Option<&GatewayClassStatus>,
    generation: i64,
    served: &[(GatewayKind, Served)],
    now: Timestamp,
) -> GatewayClassStatus {
    let version = match supported(served) {
        Ok(()) => Wanted::new(
            "SupportedVersion",
            "True",
            "SupportedVersion",
            String::new(),
        ),
        Err(why) => Wanted::new("SupportedVersion", "False", "UnsupportedVersion", why),
    };
    let wanted = vec![
        Wanted::new("Accepted", "True", "Accepted", String::new()),
        version,
    ];
    let live_conditions = live
        .and_then(|status| status.conditions.as_deref())
        .unwrap_or_default();
    GatewayClassStatus {
        conditions: Some(merged(
            live_conditions,
            wanted,
            &CLASS_TYPES,
            generation,
            now,
        )),
        // None until conformance runs say which (04 §6).
        supported_features: None,
    }
}

/// Whether the data plane runs a generation of its Gateway, as its pods say they do: what
/// the Gateway's `Programmed` says, and each listener's that is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Programmed {
    status: &'static str,
    reason: &'static str,
    message: String,
}

impl Programmed {
    /// What it says of `generation`, the Gateway's in the config handed on, its pods
    /// running what `pods` says and `ready` those ready (none known: those `pods` says):
    /// true once every ready pod runs it; false where one refused the latest config; and
    /// until then unknown, or false once `overdue`, the bound it is waited on past.
    pub(crate) fn of(
        generation: i64,
        pods: &Pods,
        ready: Option<&[IpAddr]>,
        overdue: bool,
    ) -> Self {
        let ready: Vec<IpAddr> = match ready {
            Some(ready) => ready.to_vec(),
            None => pods.keys().copied().collect(),
        };
        if let Some((pod, why)) = ready.iter().find_map(|pod| {
            pods.get(pod)
                .and_then(|said| said.refused.as_ref())
                .map(|why| (pod, why))
        }) {
            return Self {
                status: "False",
                reason: "Invalid",
                message: format!("data plane {pod} refused the config: {why}"),
            };
        }
        let running = ready
            .iter()
            .filter(|pod| {
                pods.get(pod)
                    .is_some_and(|said| said.runs == Some(generation))
            })
            .count();
        let message = format!(
            "{running} of {} data plane pods run generation {generation}",
            ready.len()
        );
        let status = match (running == ready.len() && running > 0, overdue) {
            (true, _) => "True",
            (false, true) => "False",
            (false, false) => "Unknown",
        };
        Self {
            status,
            reason: if status == "True" {
                "Programmed"
            } else {
                "Pending"
            },
            message,
        }
    }

    fn wanted(&self) -> Wanted {
        Wanted::new("Programmed", self.status, self.reason, self.message.clone())
    }
}

/// What the data plane's Gateway's status is to say, as `wanted` has it, its addresses
/// `addresses`, merged with `live`: its `Programmed` and that of each listener served as
/// `programmed` says.
pub(crate) fn gateway(
    live: Option<&GatewayStatus>,
    wanted: &translate::GatewayStatus,
    addresses: &[GatewayStatusAddresses],
    programmed: &Programmed,
    now: Timestamp,
) -> GatewayStatus {
    let generation = wanted.generation;
    let mut conditions: Vec<Wanted> = wanted.conditions.iter().map(Wanted::from).collect();
    conditions.push(if wanted.programmable {
        programmed.wanted()
    } else {
        Wanted::new(
            "Programmed",
            "False",
            "Invalid",
            "no listener is served".to_owned(),
        )
    });
    let live_listeners = live
        .and_then(|status| status.listeners.as_deref())
        .unwrap_or_default();
    let listeners = wanted
        .listeners
        .iter()
        .map(|listener| {
            let mut conditions: Vec<Wanted> =
                listener.conditions.iter().map(Wanted::from).collect();
            conditions.push(match &listener.programmable {
                Ok(()) => programmed.wanted(),
                Err(why) => Wanted::new("Programmed", "False", "Invalid", why.clone()),
            });
            let old = live_listeners
                .iter()
                .find(|old| old.name == listener.name)
                .map(|old| old.conditions.as_slice())
                .unwrap_or_default();
            GatewayStatusListeners {
                attached_routes: listener.attached_routes,
                conditions: merged(old, conditions, &LISTENER_TYPES, generation, now),
                name: listener.name.clone(),
                supported_kinds: Some(
                    listener
                        .supported_kinds
                        .iter()
                        .map(|kind| GatewayStatusListenersSupportedKinds {
                            group: Some(GROUP.to_owned()),
                            kind: (*kind).to_owned(),
                        })
                        .collect(),
                ),
            }
        })
        .collect();
    let live_conditions = live
        .and_then(|status| status.conditions.as_deref())
        .unwrap_or_default();
    GatewayStatus {
        addresses: (!addresses.is_empty()).then(|| addresses.to_vec()),
        // Not this controller's to say.
        attached_listener_sets: live.and_then(|status| status.attached_listener_sets),
        conditions: Some(merged(
            live_conditions,
            conditions,
            &GATEWAY_TYPES,
            generation,
            now,
        )),
        listeners: Some(listeners),
    }
}

/// The data plane's addresses, as an Ingress's status says them, as a Gateway's says them:
/// each an IP address, or else a host name.
pub(crate) fn addresses(addresses: &[IngressLoadBalancerIngress]) -> Vec<GatewayStatusAddresses> {
    addresses
        .iter()
        .filter_map(|address| match (&address.ip, &address.hostname) {
            (Some(ip), _) => Some(GatewayStatusAddresses {
                r#type: Some("IPAddress".to_owned()),
                value: ip.clone(),
            }),
            (None, Some(hostname)) => Some(GatewayStatusAddresses {
                r#type: Some("Hostname".to_owned()),
                value: hostname.clone(),
            }),
            (None, None) => None,
        })
        .collect()
}

/// What the status of an HTTPRoute of `namespace` is to say, as `wanted` has it of its
/// parents that are `gateway`, the data plane's, merged with `live`: none wanted where it
/// no longer names that Gateway. Other controllers' parents, and this controller's of other
/// Gateways (other data planes'), are kept whole; this controller's of `gateway` are those
/// wanted, in their order, after them, as many as the status can hold.
pub(crate) fn route(
    live: Option<&HTTPRouteStatus>,
    namespace: &str,
    gateway: &Object,
    wanted: Option<&translate::RouteStatus>,
    now: Timestamp,
) -> HTTPRouteStatus {
    let live_parents = live
        .map(|status| status.parents.as_slice())
        .unwrap_or_default();
    let ours = |parent: &HTTPRouteStatusParents| {
        let to = &parent.parent_ref;
        parent.controller_name == GATEWAY_CONTROLLER
            && to.group.as_deref().unwrap_or(GROUP) == GROUP
            && to.kind.as_deref().unwrap_or("Gateway") == "Gateway"
            && to.namespace.as_deref().unwrap_or(namespace) == gateway.namespace
            && to.name == gateway.name
    };
    let mut parents: Vec<HTTPRouteStatusParents> = live_parents
        .iter()
        .filter(|parent| !ours(parent))
        .cloned()
        .collect();
    if let Some(wanted) = wanted {
        for parent in &wanted.parents {
            if parents.len() >= MOST_PARENTS {
                break;
            }
            let parent_ref = HTTPRouteStatusParentsParentRef {
                group: Some(GROUP.to_owned()),
                kind: Some("Gateway".to_owned()),
                name: parent.name.clone(),
                namespace: parent.namespace.clone(),
                port: parent.port,
                section_name: parent.section_name.clone(),
            };
            let old = live_parents
                .iter()
                .find(|old| ours(old) && same(&old.parent_ref, &parent_ref, namespace))
                .map(|old| old.conditions.as_slice())
                .unwrap_or_default();
            parents.push(HTTPRouteStatusParents {
                conditions: merged(
                    old,
                    parent.conditions.iter().map(Wanted::from).collect(),
                    &PARENT_TYPES,
                    wanted.generation,
                    now,
                ),
                controller_name: GATEWAY_CONTROLLER.to_owned(),
                parent_ref,
            });
        }
    }
    HTTPRouteStatus { parents }
}

/// Whether two `parentRef`s of a route of `namespace` name one parent, written alike: their
/// group, kind and namespace as defaulted.
fn same(
    a: &HTTPRouteStatusParentsParentRef,
    b: &HTTPRouteStatusParentsParentRef,
    namespace: &str,
) -> bool {
    a.group.as_deref().unwrap_or(GROUP) == b.group.as_deref().unwrap_or(GROUP)
        && a.kind.as_deref().unwrap_or("Gateway") == b.kind.as_deref().unwrap_or("Gateway")
        && a.namespace.as_deref().unwrap_or(namespace)
            == b.namespace.as_deref().unwrap_or(namespace)
        && a.name == b.name
        && a.section_name == b.section_name
        && a.port == b.port
}

#[cfg(test)]
mod tests;
