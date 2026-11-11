//! UDPRoutes attached to the data plane's Gateway's UDP listeners
//! ([04 §2](../../../docs/04-control-plane.md)), attached and ranked as TCPRoutes are
//! (`tcp_route`): each accepted UDP listener is a `udp` listener of the data plane's
//! ([24](../../../docs/24-udp-proxying.md)), which has one UDP route: the oldest UDPRoute
//! attached to it, its sessions sent to the backends in proportion to their weights, an
//! invalid backend's share of datagrams dropped, as the spec has it; or, until one is, a
//! route that drops every datagram, so that the port is bound as every accepted listener's
//! is.

use crate::backend_tls::Policies;
use crate::gateway::GatewayOutcome;
use crate::http_route::{Reference, RouteOutcome, attachment_name, backends};
use crate::{Cluster, Plane, RefProblem, namespace};
use edgerush_config::{Backend, UdpRoute, Upstream};
use edgerush_gateway_api::udp_route::UDPRoute;
use std::collections::BTreeMap;

/// What a UDPRoute comes to: a UDP route for each listener it is attached to and the
/// upstreams they lead to, the backends whose share of datagrams is dropped, each with why;
/// or why it is not accepted at all.
#[derive(Debug, Default)]
pub(crate) struct Translated {
    pub(crate) routes: Vec<UdpRoute>,
    pub(crate) upstreams: BTreeMap<String, Upstream>,
    pub(crate) invalid: Vec<RefProblem>,
    pub(crate) refused: Option<String>,
}

/// `route`, attached as `outcome` says, translated for the listeners of `gateway` that
/// serve each attachment: none of it where it is not of the shape the `v1` API gives a
/// UDPRoute, exactly one rule, as one written through `v1alpha2` may not be.
pub(crate) fn translated(
    route: &UDPRoute,
    outcome: &RouteOutcome,
    gateway: &GatewayOutcome,
    plane: &Plane,
    cluster: &Cluster,
    policies: &Policies<'_>,
) -> Translated {
    let mut translated = Translated::default();
    let rules = &route.spec.rules;
    if rules.len() != 1 {
        translated.refused = Some(format!(
            "it has {} rules, where the v1 API has a UDPRoute of exactly one",
            rules.len()
        ));
        return translated;
    }
    let route_namespace = namespace(&route.metadata);
    let mut upstreams = BTreeMap::new();
    let mut invalid = Vec::new();
    let written: Vec<Backend> = backends(
        rules[0].backend_refs.iter().map(|written| {
            let reference = Reference {
                group: &written.group,
                kind: &written.kind,
                name: &written.name,
                namespace: written.namespace.as_deref(),
                port: written.port,
            };
            (written.weight, reference)
        }),
        ("UDPRoute", route_namespace),
        plane,
        cluster,
        policies,
        (&mut upstreams, &mut invalid),
    );
    for parent in &outcome.parents {
        let Ok(attachments) = &parent.attached else {
            continue;
        };
        for attachment in attachments {
            let Some(listener) = gateway.listeners.iter().find(|listener| {
                listener.set == attachment.set && listener.name == attachment.listener
            }) else {
                continue;
            };
            let Ok(served) = &listener.served else {
                continue;
            };
            let name = attachment_name("udproute", outcome, attachment);
            if translated.routes.iter().any(|known| known.name == name) {
                continue;
            }
            translated.routes.push(UdpRoute {
                name,
                listeners: served.clone(),
                backends: written.clone(),
            });
        }
    }
    translated.upstreams = upstreams;
    translated.invalid = invalid;
    translated
}

/// The UDP route each accepted UDP listener of `gateway` that no UDPRoute serves is claimed
/// by, its datagrams dropped: `gateway/<namespace>/<name>/<listener>` (or `listenerset/…`),
/// its one backend invalid. `served` holds the data plane's listeners a UDPRoute serves.
pub(crate) fn claims(gateway: &GatewayOutcome, served: &[String]) -> Vec<UdpRoute> {
    gateway
        .listeners
        .iter()
        .filter_map(|listener| {
            let on = listener.served.as_ref().ok()?;
            // A UDP listener, and no other on its port number, is served on the `udp` one.
            let udp = crate::listeners::udp_name(listener.port);
            if !on.contains(&udp) || served.contains(&udp) {
                return None;
            }
            let stater = listener.set.as_ref().unwrap_or(&gateway.gateway);
            let kind = if listener.set.is_some() {
                "listenerset"
            } else {
                "gateway"
            };
            Some(UdpRoute {
                name: format!(
                    "{kind}/{}/{}/{}",
                    stater.namespace, stater.name, listener.name
                ),
                listeners: on.clone(),
                backends: vec![Backend {
                    upstream: None,
                    invalid: Some(format!("no UDPRoute serves listener {}", listener.name)),
                    weight: 1,
                }],
            })
        })
        .collect()
}
