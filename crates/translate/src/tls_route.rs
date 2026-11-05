//! TLSRoutes attached to the data plane's Gateway's TLS listeners
//! ([04 §2](../../../docs/04-control-plane.md)), attached as HTTPRoutes are (`http_route`):
//! each attachment a passthrough route of the data plane's (17), its hostnames the route's
//! own and its listener's met, its connections sent to the backends in proportion to their
//! weights, an invalid backend's share closed, as the spec has it (Lucas).

use crate::gateway::GatewayOutcome;
use crate::http_route::{Reference, RouteOutcome, attachment_name, backends, claim};
use crate::{Cluster, Plane, RefProblem, namespace};
use edgerush_config::{Backend, TlsRoute, Upstream};
use edgerush_gateway_api::tls_route::TLSRoute;
use std::collections::BTreeMap;

/// What a TLSRoute comes to: a passthrough route for each listener it is attached to and the
/// upstreams they lead to, the backends whose share of connections is closed, each with
/// why; or why it is not accepted at all.
#[derive(Debug, Default)]
pub(crate) struct Translated {
    pub(crate) routes: Vec<TlsRoute>,
    pub(crate) upstreams: BTreeMap<String, Upstream>,
    pub(crate) invalid: Vec<RefProblem>,
    pub(crate) refused: Option<String>,
}

/// `route`, attached as `outcome` says, translated for the listeners of `gateway` that
/// serve each attachment: none of it where it is not of the shape the `v1` API gives a
/// TLSRoute, one rule and at least one hostname, as one written through an older version
/// may be.
pub(crate) fn translated(
    route: &TLSRoute,
    outcome: &RouteOutcome,
    gateway: &GatewayOutcome,
    plane: &Plane,
    cluster: &Cluster,
) -> Translated {
    let mut translated = Translated::default();
    let rules = &route.spec.rules;
    if rules.len() != 1 {
        translated.refused = Some(format!(
            "it has {} rules, where the v1 API has a TLSRoute of exactly one",
            rules.len()
        ));
        return translated;
    }
    if route.spec.hostnames.is_empty() {
        translated.refused =
            Some("it has no hostname, where the v1 API has a TLSRoute of at least one".to_owned());
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
        ("TLSRoute", route_namespace),
        plane,
        cluster,
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
            let (Ok(served), false) = (&listener.served, attachment.hostnames.is_empty()) else {
                continue;
            };
            let name = attachment_name("tlsroute", outcome, attachment);
            if translated.routes.iter().any(|known| known.name == name) {
                continue;
            }
            translated.routes.push(TlsRoute {
                name,
                listeners: served.clone(),
                hostnames: attachment.hostnames.iter().map(|h| claim(h)).collect(),
                backends: written.clone(),
            });
        }
    }
    translated.upstreams = upstreams;
    translated.invalid = invalid;
    translated
}

/// The passthrough route each accepted TLS listener's hostname is claimed by, after every
/// TLSRoute's, so that a name no route takes there is closed rather than served by a less
/// specific listener's routes (listener isolation, 04 §2): `gateway/<namespace>/<name>/
/// <listener>`, its one backend invalid.
pub(crate) fn claims(gateway: &GatewayOutcome) -> Vec<TlsRoute> {
    gateway
        .listeners
        .iter()
        .filter(|listener| listener.passes_through)
        .filter_map(|listener| {
            let served = listener.served.as_ref().ok()?;
            let hostname = listener.hostname.as_ref()?;
            let stater = listener.set.as_ref().unwrap_or(&gateway.gateway);
            let kind = if listener.set.is_some() {
                "listenerset"
            } else {
                "gateway"
            };
            Some(TlsRoute {
                name: format!(
                    "{kind}/{}/{}/{}",
                    stater.namespace, stater.name, listener.name
                ),
                listeners: served.clone(),
                hostnames: vec![claim(hostname)],
                backends: vec![Backend {
                    upstream: None,
                    invalid: Some(format!(
                        "no TLSRoute takes {hostname} on listener {}",
                        listener.name
                    )),
                    weight: 1,
                }],
            })
        })
        .collect()
}
