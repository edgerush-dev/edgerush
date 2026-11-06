//! TCPRoutes attached to the data plane's Gateway's TCP listeners
//! ([04 §2](../../../docs/04-control-plane.md)), attached as HTTPRoutes are (`http_route`):
//! each accepted TCP listener is a `tcp` listener of the data plane's (17), which has one
//! TCP route: the oldest TCPRoute attached to it, its connections sent to the backends in
//! proportion to their weights, an invalid backend's share closed, as the spec has it; or,
//! until one is, a route that closes every connection, so that the port is bound as every
//! accepted listener's is (Lucas, Q-c2).

use crate::backend_tls::Policies;
use crate::gateway::GatewayOutcome;
use crate::http_route::{Reference, RouteOutcome, attachment_name, backends};
use crate::{Cluster, Plane, RefProblem, namespace};
use edgerush_config::{Backend, Protocol, TcpRoute, Upstream};
use edgerush_gateway_api::tcp_route::TCPRoute;
use std::collections::BTreeMap;

/// What a TCPRoute comes to: a TCP route for each listener it is attached to and the
/// upstreams they lead to, the backends whose share of connections is closed, each with
/// why; or why it is not accepted at all.
#[derive(Debug, Default)]
pub(crate) struct Translated {
    pub(crate) routes: Vec<TcpRoute>,
    pub(crate) upstreams: BTreeMap<String, Upstream>,
    pub(crate) invalid: Vec<RefProblem>,
    pub(crate) refused: Option<String>,
}

/// `route`, attached as `outcome` says, translated for the listeners of `gateway` that
/// serve each attachment: none of it where it is not of the shape the `v1` API gives a
/// TCPRoute, exactly one rule, as one written through an older version may not be.
pub(crate) fn translated(
    route: &TCPRoute,
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
            "it has {} rules, where the v1 API has a TCPRoute of exactly one",
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
        ("TCPRoute", route_namespace),
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
            let name = attachment_name("tcproute", outcome, attachment);
            if translated.routes.iter().any(|known| known.name == name) {
                continue;
            }
            translated.routes.push(TcpRoute {
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

/// The TCP route each accepted TCP listener of `gateway` that no TCPRoute serves is claimed
/// by, its connections closed: `gateway/<namespace>/<name>/<listener>` (or
/// `listenerset/…`), its one backend invalid. `served` holds the data plane's listeners a
/// TCPRoute serves.
pub(crate) fn claims(gateway: &GatewayOutcome, served: &[String]) -> Vec<TcpRoute> {
    gateway
        .listeners
        .iter()
        .filter(|listener| gateway.ports.get(&listener.port) == Some(&Protocol::Tcp))
        .filter_map(|listener| {
            let on = listener.served.as_ref().ok()?;
            if on.iter().all(|name| served.contains(name)) {
                return None;
            }
            let stater = listener.set.as_ref().unwrap_or(&gateway.gateway);
            let kind = if listener.set.is_some() {
                "listenerset"
            } else {
                "gateway"
            };
            Some(TcpRoute {
                name: format!(
                    "{kind}/{}/{}/{}",
                    stater.namespace, stater.name, listener.name
                ),
                listeners: on.clone(),
                backends: vec![Backend {
                    upstream: None,
                    invalid: Some(format!("no TCPRoute serves listener {}", listener.name)),
                    weight: 1,
                }],
            })
        })
        .collect()
}
