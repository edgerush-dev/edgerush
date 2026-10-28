//! Ingress status ([04 §6](../../../docs/04-control-plane.md)): the data plane's addresses in
//! `status.loadBalancer.ingress` of each Ingress it serves, and none in that of each of its
//! class it does not. Pure: what each Ingress's status should say is a function of what was
//! translated, what each says now and the addresses.

use edgerush_translate::Object;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::{Ingress, IngressLoadBalancerIngress};

/// Where the data plane's addresses come from.
#[derive(Debug, Clone, PartialEq)]
pub enum Addresses {
    /// These, stated: for a cluster with no load balancer.
    Stated(Vec<IngressLoadBalancerIngress>),
    /// The data plane's Service, `name` in `namespace`, followed: its load balancer's
    /// addresses, else its external IPs.
    Service {
        /// Its namespace.
        namespace: String,
        /// Its name.
        name: String,
    },
}

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

/// The addresses `service` gives: its load balancer's, else its external IPs; none for a
/// Service that is not there, or has neither yet.
pub(crate) fn of_service(service: Option<&Service>) -> Vec<IngressLoadBalancerIngress> {
    let Some(service) = service else {
        return Vec::new();
    };
    let balanced: Vec<IngressLoadBalancerIngress> = service
        .status
        .as_ref()
        .and_then(|status| status.load_balancer.as_ref())
        .and_then(|balancer| balancer.ingress.as_ref())
        .into_iter()
        .flatten()
        .map(|ingress| IngressLoadBalancerIngress {
            ip: ingress.ip.clone(),
            hostname: ingress.hostname.clone(),
            ports: None,
        })
        .collect();
    if !balanced.is_empty() {
        return balanced;
    }
    service
        .spec
        .as_ref()
        .and_then(|spec| spec.external_ips.as_ref())
        .into_iter()
        .flatten()
        .map(|ip| address(ip))
        .collect()
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
