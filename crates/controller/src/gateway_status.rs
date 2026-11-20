//! Gateway API status ([04 §6](../../../docs/04-control-plane.md)): what translation says
//! the data plane's GatewayClass, Gateway and routes are to say, merged with what each
//! says now as the spec has a controller write it. Each condition of the generation it
//! says of, its `lastTransitionTime` kept while its status holds; a condition whose
//! `observedGeneration` is newer left as it is; condition types not this controller's,
//! and other controllers' parents of a route, kept whole; this controller's own parents of
//! a route that are no longer wanted taken away. A BackendTLSPolicy's ancestor of this
//! controller's is written where its status holds it, every other ancestor kept whole and in
//! place. Pure: the time is passed in.

use crate::acks::{Pod, Pods};
use edgerush_gateway_api::backend_tls_policy::{
    BackendTLSPolicyStatus, BackendTLSPolicyStatusAncestors,
    BackendTLSPolicyStatusAncestorsAncestorRef,
};
use edgerush_gateway_api::gateway::{
    GatewayStatus, GatewayStatusAddresses, GatewayStatusListeners,
    GatewayStatusListenersSupportedKinds,
};
use edgerush_gateway_api::gateway_class::{
    GatewayClassStatus, GatewayClassStatusSupportedFeatures,
};
use edgerush_gateway_api::grpc_route::{
    GRPCRouteStatus, GRPCRouteStatusParents, GRPCRouteStatusParentsParentRef,
};
use edgerush_gateway_api::http_route::{
    HTTPRouteStatus, HTTPRouteStatusParents, HTTPRouteStatusParentsParentRef,
};
use edgerush_gateway_api::listener_set::{
    ListenerSetStatus, ListenerSetStatusListeners, ListenerSetStatusListenersSupportedKinds,
};
use edgerush_gateway_api::tcp_route::{
    TCPRouteStatus, TCPRouteStatusParents, TCPRouteStatusParentsParentRef,
};
use edgerush_gateway_api::tls_route::{
    TLSRouteStatus, TLSRouteStatusParents, TLSRouteStatusParentsParentRef,
};
use edgerush_gateway_api::udp_route::{
    UDPRouteStatus, UDPRouteStatusParents, UDPRouteStatusParentsParentRef,
};
use edgerush_kubernetes::gateway_api::{GatewayKind, Served, supported};
use edgerush_planes::rate_limit_policy::{
    RateLimitPolicyStatus, RateLimitPolicyStatusAncestors,
    RateLimitPolicyStatusAncestorsAncestorRef,
};
use edgerush_translate::{self as translate, GATEWAY_CONTROLLER, Object};
use k8s_openapi::api::networking::v1::IngressLoadBalancerIngress;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

/// The API group of the Gateway API's kinds.
const GROUP: &str = "gateway.networking.k8s.io";

/// The API group of Ingress.
const INGRESS_GROUP: &str = "networking.k8s.io";

/// The most parents a route's status holds, as its CRD has it.
const MOST_PARENTS: usize = 32;

/// The most ancestors a policy's status holds, as its CRD has it.
const MOST_ANCESTORS: usize = 16;

/// The Gateway API features EdgeRush claims, by the conformance suite's names: what every
/// GatewayClass of this controller's says in `status.supportedFeatures`, and what the
/// conformance suite reads from it to know what to test (04 §6, §7). Only what passes its
/// profile's conformance run is listed. Sorted, as the spec requires the status to be, and
/// no longer than its CRD allows.
pub(crate) const SUPPORTED_FEATURES: &[&str] = &[
    // GATEWAY-HTTP's core (Gateway, HTTPRoute, ReferenceGrant), GATEWAY-GRPC's (Gateway,
    // GRPCRoute, ReferenceGrant), GATEWAY-TLS's (Gateway, TLSRoute, ReferenceGrant),
    // GATEWAY-TCP's (Gateway, TCPRoute, ReferenceGrant) and GATEWAY-UDP's (Gateway, UDPRoute,
    // ReferenceGrant), and the extended features that pass them.
    "GRPCRoute",
    "GRPCRouteNamedRouteRule",
    "GRPCRouteRequestHeaderModifier",
    "Gateway",
    "GatewayAddressEmpty",
    "GatewayBackendClientCertificate",
    "GatewayHTTPListenerIsolation",
    "GatewayHTTPSListenerDetectMisdirectedRequests",
    "GatewayInfrastructure",
    "GatewayPort8080",
    "HTTPRoute",
    "HTTPRoute303RedirectStatusCode",
    "HTTPRoute307RedirectStatusCode",
    "HTTPRoute308RedirectStatusCode",
    "HTTPRouteBackendProtocolH2C",
    "HTTPRouteBackendProtocolWebSocket",
    "HTTPRouteBackendTimeout",
    "HTTPRouteDestinationPortMatching",
    "HTTPRouteHostRewrite",
    "HTTPRouteMethodMatching",
    "HTTPRouteNamedRouteRule",
    "HTTPRouteParentRefPort",
    "HTTPRoutePathRedirect",
    "HTTPRoutePathRewrite",
    "HTTPRoutePortRedirect",
    "HTTPRouteQueryParamMatching",
    "HTTPRouteRequestMirror",
    "HTTPRouteRequestMultipleMirrors",
    "HTTPRouteRequestPercentageMirror",
    "HTTPRouteRequestTimeout",
    "HTTPRouteResponseHeaderModification",
    "HTTPRouteRetryCodes",
    "HTTPRouteRetryConnectionError",
    "HTTPRouteSchemeRedirect",
    "ReferenceGrant",
    "TCPRoute",
    "TLSRoute",
    "UDPRoute",
];

/// The most features a GatewayClass's status holds, as its CRD has it.
const MOST_FEATURES: usize = 64;

// A longer table would have every class's status refused by the API server.
const _: () = assert!(SUPPORTED_FEATURES.len() <= MOST_FEATURES);

/// The condition types this controller writes on a GatewayClass.
const CLASS_TYPES: [&str; 2] = ["Accepted", "SupportedVersion"];
/// On a Gateway.
const GATEWAY_TYPES: [&str; 3] = ["Accepted", "ResolvedRefs", "Programmed"];
/// On a Gateway's listener.
const LISTENER_TYPES: [&str; 4] = ["Accepted", "Conflicted", "ResolvedRefs", "Programmed"];
/// On a route's parent.
const PARENT_TYPES: [&str; 3] = ["Accepted", "ResolvedRefs", "PartiallyInvalid"];
/// On a ListenerSet.
const SET_TYPES: [&str; 2] = ["Accepted", "Programmed"];
/// On a BackendTLSPolicy's ancestor.
const ANCESTOR_TYPES: [&str; 2] = ["Accepted", "ResolvedRefs"];
/// On a RateLimitPolicy's ancestor.
pub(crate) const LIMIT_ANCESTOR_TYPES: [&str; 1] = ["Accepted"];

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
/// kinds being as `served` says, merged with `live`, claiming the features in `table`
/// ([`SUPPORTED_FEATURES`]) and only those. Accepted, as translation reads the class's
/// Gateway only where it names this controller.
pub(crate) fn class(
    live: Option<&GatewayClassStatus>,
    generation: i64,
    served: &[(GatewayKind, Served)],
    table: &[&str],
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
        supported_features: features(table),
    }
}

/// What a GatewayClass's status says of the features in `table`: each in its order, and the
/// field left out while there are none.
fn features(table: &[&str]) -> Option<Vec<GatewayClassStatusSupportedFeatures>> {
    (!table.is_empty()).then(|| {
        table
            .iter()
            .map(|name| GatewayClassStatusSupportedFeatures {
                name: (*name).to_owned(),
            })
            .collect()
    })
}

/// Whether the data plane runs a generation of its Gateway, as its pods say they do: what
/// the Gateway's `Programmed` says, and each listener's that is served, but where a ready
/// pod running it does not serve the listener's port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Programmed {
    status: &'static str,
    reason: &'static str,
    message: String,
    /// The data plane's listeners a ready pod running the generation does not serve, by
    /// name: the first such pod, and why it says.
    unserved: BTreeMap<String, (IpAddr, String)>,
}

impl Programmed {
    /// What it says of `generation`, the Gateway's in the config handed on, its pods
    /// running what `pods` says and `ready` those ready (none known: those `pods` says):
    /// true once every ready pod runs it; false where one refused the latest config; and
    /// until then unknown, or false once `overdue`, the bound it is waited on past. And the
    /// listeners of that generation a ready pod running it does not serve: a pod on an
    /// older one says what it serves of its own config, not of this.
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
                unserved: BTreeMap::new(),
            };
        }
        let on_it: Vec<(&IpAddr, &Pod)> = ready
            .iter()
            .filter_map(|pod| Some((pod, pods.get(pod)?)))
            .filter(|(_, said)| said.runs == Some(generation))
            .collect();
        let running = on_it.len();
        let mut unserved = BTreeMap::new();
        for (pod, said) in &on_it {
            for (listener, why) in &said.unserved {
                unserved
                    .entry(listener.clone())
                    .or_insert_with(|| (**pod, why.clone()));
            }
        }
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
            unserved,
        }
    }

    fn wanted(&self) -> Wanted {
        Wanted::new("Programmed", self.status, self.reason, self.message.clone())
    }

    /// What a Gateway listener served on the data plane's listeners `served_on` says: not
    /// programmed, still pending, where a ready pod running the generation does not serve
    /// one, saying that pod and why; else what the Gateway says (04 §6).
    fn of_listener(&self, served_on: &[String]) -> Wanted {
        match served_on.iter().find_map(|name| self.unserved.get(name)) {
            Some((pod, why)) => Wanted::new(
                "Programmed",
                "False",
                "Pending",
                format!("data plane {pod}: {why}"),
            ),
            None => self.wanted(),
        }
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
            let old = live_listeners
                .iter()
                .find(|old| old.name == listener.name)
                .map(|old| old.conditions.as_slice())
                .unwrap_or_default();
            GatewayStatusListeners {
                attached_routes: listener.attached_routes,
                conditions: listener_conditions(old, listener, programmed, generation, now),
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
        attached_listener_sets: Some(wanted.attached_listener_sets),
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

/// A listener's conditions, as `wanted` has them and `programmed` its `Programmed`, merged
/// with `old`, its live ones: a Gateway's and a ListenerSet's alike.
fn listener_conditions(
    old: &[Condition],
    wanted: &translate::ListenerStatus,
    programmed: &Programmed,
    generation: i64,
    now: Timestamp,
) -> Vec<Condition> {
    let mut conditions: Vec<Wanted> = wanted.conditions.iter().map(Wanted::from).collect();
    conditions.push(match &wanted.programmable {
        Ok(served_on) => programmed.of_listener(served_on),
        Err(why) => Wanted::new("Programmed", "False", "Invalid", why.clone()),
    });
    merged(old, conditions, &LISTENER_TYPES, generation, now)
}

/// What a ListenerSet's status is to say, as `wanted` has it, merged with `live`: of its own
/// listeners alone, its `Programmed` the Gateway's config's where any of them is served
/// (GEP-1713), each listener's as a Gateway's is.
pub(crate) fn listener_set(
    live: Option<&ListenerSetStatus>,
    wanted: &translate::ListenerSetStatus,
    programmed: &Programmed,
    now: Timestamp,
) -> ListenerSetStatus {
    let generation = wanted.generation;
    let mut conditions: Vec<Wanted> = wanted.conditions.iter().map(Wanted::from).collect();
    conditions.push(match &wanted.programmable {
        Ok(()) => programmed.wanted(),
        Err(condition) => Wanted::from(condition),
    });
    let live_listeners = live
        .and_then(|status| status.listeners.as_deref())
        .unwrap_or_default();
    let listeners = wanted
        .listeners
        .iter()
        .map(|listener| {
            let old = live_listeners
                .iter()
                .find(|old| old.name == listener.name)
                .map(|old| old.conditions.as_slice())
                .unwrap_or_default();
            ListenerSetStatusListeners {
                attached_routes: listener.attached_routes,
                conditions: listener_conditions(old, listener, programmed, generation, now),
                name: listener.name.clone(),
                supported_kinds: Some(
                    listener
                        .supported_kinds
                        .iter()
                        .map(|kind| ListenerSetStatusListenersSupportedKinds {
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
    ListenerSetStatus {
        conditions: Some(merged(
            live_conditions,
            conditions,
            &SET_TYPES,
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

/// A route's status of another kind as an HTTPRoute's: the spec has one type for every
/// kind, `RouteStatus`, of which the generated types are copies, so that each is merged as
/// the other is and written as the same JSON. Taken apart whole, so that a field one gains
/// and the other not stops this building.
macro_rules! as_http {
    ($(#[$doc:meta])* $name:ident, $status:ident, $parents:ident, $parent_ref:ident) => {
        $(#[$doc])*
        pub(crate) fn $name(status: &$status) -> HTTPRouteStatus {
            let $status { parents } = status;
            HTTPRouteStatus {
                parents: parents
                    .iter()
                    .map(|parent| {
                        let $parents {
                            conditions,
                            controller_name,
                            parent_ref,
                        } = parent;
                        let $parent_ref {
                            group,
                            kind,
                            name,
                            namespace,
                            port,
                            section_name,
                        } = parent_ref;
                        HTTPRouteStatusParents {
                            conditions: conditions.clone(),
                            controller_name: controller_name.clone(),
                            parent_ref: HTTPRouteStatusParentsParentRef {
                                group: group.clone(),
                                kind: kind.clone(),
                                name: name.clone(),
                                namespace: namespace.clone(),
                                port: *port,
                                section_name: section_name.clone(),
                            },
                        }
                    })
                    .collect(),
            }
        }
    };
}

as_http!(
    /// A GRPCRoute's status as an HTTPRoute's.
    grpc_as_http,
    GRPCRouteStatus,
    GRPCRouteStatusParents,
    GRPCRouteStatusParentsParentRef
);
as_http!(
    /// A TLSRoute's status as an HTTPRoute's.
    tls_as_http,
    TLSRouteStatus,
    TLSRouteStatusParents,
    TLSRouteStatusParentsParentRef
);
as_http!(
    /// A TCPRoute's status as an HTTPRoute's.
    tcp_as_http,
    TCPRouteStatus,
    TCPRouteStatusParents,
    TCPRouteStatusParentsParentRef
);
as_http!(
    /// A UDPRoute's status as an HTTPRoute's.
    udp_as_http,
    UDPRouteStatus,
    UDPRouteStatusParents,
    UDPRouteStatusParentsParentRef
);

/// The Gateways a control plane can tell are there or gone: every one of the namespaces it
/// follows Gateways in, `namespaces`, that is there, `there`. Of any other namespace it can
/// tell nothing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Gateways<'a> {
    pub(crate) namespaces: &'a [String],
    pub(crate) there: &'a [Object],
}

impl Gateways<'_> {
    /// Whether Gateway `namespace`/`name` is of a namespace it follows Gateways in, and is
    /// not there.
    fn gone(&self, namespace: &str, name: &str) -> bool {
        self.namespaces.iter().any(|followed| followed == namespace)
            && !self
                .there
                .iter()
                .any(|gateway| gateway.namespace == namespace && gateway.name == name)
    }

    /// Whether an entry of `controller` naming `group`, `kind`, `namespace` and `name`
    /// (defaulted as a reference of `of` is) is of this controller's, and of a Gateway gone:
    /// one no data plane serves, whoever wrote it, as every control plane writes under one
    /// controller name (04 §6).
    fn stale(
        &self,
        controller: &str,
        (group, kind, namespace, name): (Option<&str>, Option<&str>, Option<&str>, &str),
        of: &str,
    ) -> bool {
        controller == GATEWAY_CONTROLLER
            && group.unwrap_or(GROUP) == GROUP
            && kind.unwrap_or("Gateway") == "Gateway"
            && self.gone(namespace.unwrap_or(of), name)
    }
}

/// What the status of a route of `namespace`, of any kind, is to say, as `wanted` has it of its
/// parents that are `parents_of_ours`, the Gateways of the data planes served and the
/// ListenerSets naming them, merged with `live`: none wanted where it no longer names one.
/// Other controllers' parents, and this controller's of other Gateways (other control
/// planes'), are kept whole where they are, but for this controller's of a Gateway that
/// `gateways` says is gone, which are taken away. This controller's of `parents_of_ours`
/// are those wanted: each written in the place of the first the status holds for its
/// parent, any others for it taken away, or after the rest, in their order, as many as the
/// status can hold. They stay where they are, so that another writer doing likewise never
/// finds them moved.
pub(crate) fn route(
    live: Option<&HTTPRouteStatus>,
    namespace: &str,
    parents_of_ours: &[Object],
    wanted: Option<&translate::RouteStatus>,
    gateways: Gateways<'_>,
    now: Timestamp,
) -> HTTPRouteStatus {
    let live_parents = live
        .map(|status| status.parents.as_slice())
        .unwrap_or_default();
    let ours = |parent: &HTTPRouteStatusParents| {
        let to = &parent.parent_ref;
        parent.controller_name == GATEWAY_CONTROLLER
            && to.group.as_deref().unwrap_or(GROUP) == GROUP
            && parents_of_ours.iter().any(|ours| {
                to.kind.as_deref().unwrap_or("Gateway") == ours.kind
                    && to.namespace.as_deref().unwrap_or(namespace) == ours.namespace
                    && to.name == ours.name
            })
    };
    let (generation, wanted) = wanted.map_or((0, &[][..]), |wanted| {
        (wanted.generation, wanted.parents.as_slice())
    });
    let made = |old: &[Condition],
                parent_ref: HTTPRouteStatusParentsParentRef,
                parent: &translate::ParentStatus| HTTPRouteStatusParents {
        conditions: merged(
            old,
            parent.conditions.iter().map(Wanted::from).collect(),
            &PARENT_TYPES,
            generation,
            now,
        ),
        controller_name: GATEWAY_CONTROLLER.to_owned(),
        parent_ref,
    };
    let mut wanted: Vec<Option<(HTTPRouteStatusParentsParentRef, &translate::ParentStatus)>> =
        wanted
            .iter()
            .map(|parent| {
                let parent_ref = HTTPRouteStatusParentsParentRef {
                    group: Some(GROUP.to_owned()),
                    kind: Some(parent.kind.to_owned()),
                    name: parent.name.clone(),
                    namespace: parent.namespace.clone(),
                    port: parent.port,
                    section_name: parent.section_name.clone(),
                };
                Some((parent_ref, parent))
            })
            .collect();
    let stale = |parent: &HTTPRouteStatusParents| {
        let to = &parent.parent_ref;
        gateways.stale(
            &parent.controller_name,
            (
                to.group.as_deref(),
                to.kind.as_deref(),
                to.namespace.as_deref(),
                &to.name,
            ),
            namespace,
        )
    };
    let mut parents = Vec::new();
    for old in live_parents {
        if !ours(old) {
            if !stale(old) {
                parents.push(old.clone());
            }
        } else if let Some(slot) = wanted.iter_mut().find(|slot| {
            slot.as_ref()
                .is_some_and(|(parent_ref, _)| same(&old.parent_ref, parent_ref, namespace))
        }) && let Some((parent_ref, parent)) = slot.take()
        {
            parents.push(made(&old.conditions, parent_ref, parent));
        }
    }
    for (parent_ref, parent) in wanted.into_iter().flatten() {
        if parents.len() >= MOST_PARENTS {
            break;
        }
        parents.push(made(&[], parent_ref, parent));
    }
    HTTPRouteStatus { parents }
}

/// What a policy's status is to say of one ancestor: its conditions, of the policy's
/// generation `generation`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OfAncestor<'a> {
    pub(crate) generation: i64,
    pub(crate) conditions: &'a [translate::Condition],
}

impl<'a> From<&'a translate::PolicyStatus> for OfAncestor<'a> {
    fn from(status: &'a translate::PolicyStatus) -> Self {
        Self {
            generation: status.generation,
            conditions: &status.conditions,
        }
    }
}

/// The API group of an ancestor of `kind`: an Ingress's, or the Gateway API's.
fn group_of(kind: &str) -> &'static str {
    if kind == "Ingress" {
        INGRESS_GROUP
    } else {
        GROUP
    }
}

/// What the status of a policy of `namespace`, a BackendTLSPolicy or a RateLimitPolicy, is
/// to say, as `wanted` has it of each ancestor a data plane served may write of, its
/// conditions of the types `ours`: a Gateway, or an Ingress; merged with `live`: nothing of
/// an ancestor where it does not reach the policy. Every ancestor that is not this
/// controller's of one of those, other controllers' and this controller's of other control
/// planes' Gateways, is kept whole where it is, but for this controller's of a Gateway that
/// `gateways` says is gone, or of an Ingress of `namespace` not among `ingresses`, where
/// they are known, which is taken away. This controller's of each ancestor is written in
/// the place of the first the status holds for it, any others taken away, or after the
/// rest where the status holds none and has room for it; it stays where it is, so that
/// another writer doing likewise never finds it moved.
pub(crate) fn policy_status(
    live: Option<&BackendTLSPolicyStatus>,
    namespace: &str,
    wanted: &[(&Object, Option<OfAncestor<'_>>)],
    ours: &[&str],
    (gateways, ingresses): (Gateways<'_>, Option<&BTreeSet<Object>>),
    now: Timestamp,
) -> BackendTLSPolicyStatus {
    let live_ancestors = live
        .map(|status| status.ancestors.as_slice())
        .unwrap_or_default();
    // Which ancestor of `wanted` an ancestor is this controller's of, if one.
    let of = |ancestor: &BackendTLSPolicyStatusAncestors| {
        let to = &ancestor.ancestor_ref;
        let kind = to.kind.as_deref().unwrap_or("Gateway");
        (ancestor.controller_name == GATEWAY_CONTROLLER
            && to.group.as_deref().unwrap_or(GROUP) == group_of(kind))
        .then(|| {
            wanted.iter().position(|(object, _)| {
                object.kind == kind
                    && to.namespace.as_deref().unwrap_or(namespace) == object.namespace
                    && to.name == object.name
            })
        })
        .flatten()
    };
    let stale = |ancestor: &BackendTLSPolicyStatusAncestors| {
        let to = &ancestor.ancestor_ref;
        let ingress_gone = ingresses.is_some_and(|there| {
            ancestor.controller_name == GATEWAY_CONTROLLER
                && to.group.as_deref() == Some(INGRESS_GROUP)
                && to.kind.as_deref() == Some("Ingress")
                && to.namespace.as_deref().unwrap_or(namespace) == namespace
                && !there.contains(&Object {
                    kind: "Ingress",
                    namespace: namespace.to_owned(),
                    name: to.name.clone(),
                })
        });
        ingress_gone
            || gateways.stale(
                &ancestor.controller_name,
                (
                    to.group.as_deref(),
                    to.kind.as_deref(),
                    to.namespace.as_deref(),
                    &to.name,
                ),
                namespace,
            )
    };
    let made = |old: &[Condition], object: &Object, wanted: OfAncestor<'_>| {
        BackendTLSPolicyStatusAncestors {
            ancestor_ref: BackendTLSPolicyStatusAncestorsAncestorRef {
                group: Some(group_of(object.kind).to_owned()),
                kind: Some(object.kind.to_owned()),
                name: object.name.clone(),
                namespace: Some(object.namespace.clone()),
                port: None,
                section_name: None,
            },
            conditions: merged(
                old,
                wanted.conditions.iter().map(Wanted::from).collect(),
                ours,
                wanted.generation,
                now,
            ),
            controller_name: GATEWAY_CONTROLLER.to_owned(),
        }
    };
    let mut left: Vec<Option<OfAncestor<'_>>> = wanted.iter().map(|(_, wanted)| *wanted).collect();
    let mut ancestors = Vec::new();
    for ancestor in live_ancestors {
        match of(ancestor) {
            Some(at) => {
                if let Some(status) = left[at].take() {
                    ancestors.push(made(&ancestor.conditions, wanted[at].0, status));
                }
            }
            None if stale(ancestor) => {}
            None => ancestors.push(ancestor.clone()),
        }
    }
    for (at, status) in left.into_iter().enumerate() {
        if let Some(status) = status
            && ancestors.len() < MOST_ANCESTORS
        {
            ancestors.push(made(&[], wanted[at].0, status));
        }
    }
    BackendTLSPolicyStatus { ancestors }
}

/// What the status of a BackendTLSPolicy of `namespace` is to say, as `wanted` has it of
/// each Gateway of a data plane served, merged with `live`, as [`policy_status`] merges it:
/// nothing of a Gateway where no route of it reaches the policy.
pub(crate) fn policy(
    live: Option<&BackendTLSPolicyStatus>,
    namespace: &str,
    wanted: &[(&Object, Option<&translate::PolicyStatus>)],
    gateways: Gateways<'_>,
    now: Timestamp,
) -> BackendTLSPolicyStatus {
    let wanted: Vec<(&Object, Option<OfAncestor<'_>>)> = wanted
        .iter()
        .map(|(gateway, status)| (*gateway, status.map(OfAncestor::from)))
        .collect();
    policy_status(
        live,
        namespace,
        &wanted,
        &ANCESTOR_TYPES,
        (gateways, None),
        now,
    )
}

/// A RateLimitPolicy's status as a BackendTLSPolicy's: the spec has one type for every
/// policy, `PolicyStatus`, of which the generated types are copies, so that each is merged
/// as the other is and written as the same JSON. Taken apart whole, so that a field one
/// gains and the other not stops this building.
pub(crate) fn limits_as_tls(status: &RateLimitPolicyStatus) -> BackendTLSPolicyStatus {
    let RateLimitPolicyStatus { ancestors } = status;
    BackendTLSPolicyStatus {
        ancestors: ancestors
            .iter()
            .map(|ancestor| {
                let RateLimitPolicyStatusAncestors {
                    ancestor_ref,
                    conditions,
                    controller_name,
                } = ancestor;
                let RateLimitPolicyStatusAncestorsAncestorRef {
                    group,
                    kind,
                    name,
                    namespace,
                    port,
                    section_name,
                } = ancestor_ref;
                BackendTLSPolicyStatusAncestors {
                    ancestor_ref: BackendTLSPolicyStatusAncestorsAncestorRef {
                        group: group.clone(),
                        kind: kind.clone(),
                        name: name.clone(),
                        namespace: namespace.clone(),
                        port: *port,
                        section_name: section_name.clone(),
                    },
                    conditions: conditions.clone(),
                    controller_name: controller_name.clone(),
                }
            })
            .collect(),
    }
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
