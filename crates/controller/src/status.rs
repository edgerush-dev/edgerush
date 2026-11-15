//! Ingress status ([04 §6](../../../docs/04-control-plane.md)): the data plane's addresses in
//! `status.loadBalancer.ingress` of each Ingress it serves, and none in that of each of its
//! class it does not, the addresses the data plane's Service gives
//! ([`service::addresses`](crate::service::addresses)); and none in that of each Ingress of a
//! class of the ControlPlane's that no DataPlane may still serve (`released`). Pure: what
//! each Ingress's status should say is a function of what was translated, what each says now
//! and the addresses.

use edgerush_translate::{Object, class_of};
use k8s_openapi::api::networking::v1::{Ingress, IngressLoadBalancerIngress};
use std::collections::BTreeSet;

/// An address as a status holds it: an IP address, or else a host name.
#[must_use]
pub fn address(stated: &str) -> IngressLoadBalancerIngress {
    if stated.parse::<std::net::IpAddr>().is_ok() {
        IngressLoadBalancerIngress {
            ip: Some(stated.to_owned()),
            ..Default::default()
        }
    } else {
        IngressLoadBalancerIngress {
            hostname: Some(stated.to_owned()),
            ..Default::default()
        }
    }
}

/// An Ingress whose status says other than it should, and what it should say.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Change {
    pub(crate) ingress: Object,
    pub(crate) addresses: Vec<IngressLoadBalancerIngress>,
}

/// Each Ingress of `claimed` whose status, as `ingresses` hold it now, says other than it
/// should: `addresses` if it is `served`, none if not. In `claimed`'s order.
pub(crate) fn changes(
    claimed: &[Object],
    served: &[Object],
    ingresses: &[Ingress],
    addresses: &[IngressLoadBalancerIngress],
) -> Vec<Change> {
    claimed
        .iter()
        .filter_map(|object| {
            let ingress = ingresses.iter().find(|ingress| {
                ingress.metadata.namespace.as_deref() == Some(object.namespace.as_str())
                    && ingress.metadata.name.as_deref() == Some(object.name.as_str())
            })?;
            let now = sorted(
                ingress
                    .status
                    .as_ref()
                    .and_then(|status| status.load_balancer.as_ref())
                    .and_then(|balancer| balancer.ingress.clone())
                    .unwrap_or_default(),
            );
            let wanted = if served.contains(object) {
                addresses.to_vec()
            } else {
                Vec::new()
            };
            // Compared in an order of their own, written in the order given.
            (now != sorted(wanted.clone())).then(|| Change {
                ingress: object.clone(),
                addresses: wanted,
            })
        })
        .collect()
}

/// Each Ingress of `ingresses` that holds an address and is of a class of
/// `control_plane`'s, `<control_plane>.<dataplane>` (05 §3), that `kept` does not hold: to
/// say none (04 §6). In `ingresses`' order. A class of the ControlPlane's is split only one
/// way, as neither name holds a dot.
pub(crate) fn released(
    control_plane: &str,
    kept: &BTreeSet<String>,
    ingresses: &[Ingress],
) -> Vec<Change> {
    let ours = |class: &str| {
        class.split_once('.').is_some_and(|(owner, data_plane)| {
            owner == control_plane && !data_plane.is_empty() && !data_plane.contains('.')
        })
    };
    ingresses
        .iter()
        .filter(|ingress| {
            class_of(ingress).is_some_and(|class| ours(class) && !kept.contains(class))
        })
        .filter(|ingress| {
            ingress
                .status
                .as_ref()
                .and_then(|status| status.load_balancer.as_ref())
                .and_then(|balancer| balancer.ingress.as_ref())
                .is_some_and(|held| !held.is_empty())
        })
        .map(|ingress| Change {
            ingress: Object {
                kind: "Ingress",
                namespace: ingress.metadata.namespace.clone().unwrap_or_default(),
                name: ingress.metadata.name.clone().unwrap_or_default(),
            },
            addresses: Vec::new(),
        })
        .collect()
}

/// The merge patch that has a status say `addresses`: none takes the list away.
pub(crate) fn patch(
    addresses: &[IngressLoadBalancerIngress],
) -> Result<serde_json::Value, serde_json::Error> {
    let ingress = if addresses.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::to_value(addresses)?
    };
    Ok(serde_json::json!({ "status": { "loadBalancer": { "ingress": ingress } } }))
}

/// In an order of their own, so that two lists of the same addresses compare equal.
fn sorted(mut addresses: Vec<IngressLoadBalancerIngress>) -> Vec<IngressLoadBalancerIngress> {
    addresses.sort_by(|a, b| (&a.ip, &a.hostname).cmp(&(&b.ip, &b.hostname)));
    addresses
}
