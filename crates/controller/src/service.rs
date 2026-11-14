//! The data plane's Service, which the control plane writes
//! ([05 §2](../../../docs/05-operator.md)): its ports are those the config handed on binds,
//! and Ingress's two; the rest is its DataPlane's, as
//! [`ProxyService`](edgerush_planes::objects::ProxyService) shapes it. Pure: what is
//! bound, whether the Service there says other than what is wanted, whether it is ours, and
//! the addresses it gives.

use edgerush_config::{Config, Protocol};
use edgerush_planes::objects::Transports;
use k8s_openapi::api::core::v1::{Service, ServicePort};
use std::collections::{BTreeMap, BTreeSet};

/// The ports `config` binds, each over what it is served on: UDP for a `udp` listener or
/// one serving HTTP/3 beside it, TCP for every other. A port of two listeners, as `Dual`
/// binds, is one.
#[must_use]
pub fn bound(config: &Config) -> BTreeMap<u16, Transports> {
    let mut ports: BTreeMap<u16, Transports> = BTreeMap::new();
    for listener in config.listeners.values() {
        let transports = ports.entry(listener.address.port()).or_default();
        if listener.protocol == Protocol::Udp {
            transports.udp = true;
        } else {
            transports.tcp = true;
            if listener.http3.is_some() {
                transports.udp = true;
            }
        }
    }
    ports
}

/// Whether `live` is the Service of the DataPlane of UID `owner`: one of its name it does
/// not own is someone else's, and left alone.
#[must_use]
pub fn ours(live: &Service, owner: &str) -> bool {
    live.metadata
        .owner_references
        .iter()
        .flatten()
        .any(|reference| reference.uid == owner)
}

/// Whether `live` says other than `wanted` in what `wanted` states: so that a Service as it
/// is wanted is not written again, and one edited by hand is put back. What `wanted` no
/// longer states, its apply takes away; that is looked for by comparing it with what was
/// last applied, not here.
#[must_use]
pub fn differs(live: &Service, wanted: &Service) -> bool {
    let holds = |live: Option<&BTreeMap<String, String>>,
                 wanted: Option<&BTreeMap<String, String>>| {
        wanted
            .into_iter()
            .flatten()
            .all(|(key, value)| live.and_then(|live| live.get(key)) == Some(value))
    };
    let owner = wanted
        .metadata
        .owner_references
        .iter()
        .flatten()
        .all(|reference| ours(live, &reference.uid));
    let metadata = holds(
        live.metadata.labels.as_ref(),
        wanted.metadata.labels.as_ref(),
    ) && holds(
        live.metadata.annotations.as_ref(),
        wanted.metadata.annotations.as_ref(),
    ) && owner;
    let (Some(live), Some(wanted)) = (&live.spec, &wanted.spec) else {
        return true;
    };
    // What `wanted` leaves out is the API server's, and not looked at.
    fn stated<T: PartialEq>(live: Option<&T>, wanted: Option<&T>) -> bool {
        wanted.is_none() || live == wanted
    }
    let ports = |spec: &k8s_openapi::api::core::v1::ServiceSpec| -> BTreeSet<String> {
        spec.ports.iter().flatten().map(port).collect()
    };
    let ranges = |ranges: Option<&Vec<String>>| -> Option<BTreeSet<String>> {
        ranges.map(|ranges| ranges.iter().cloned().collect())
    };
    let spec = stated(live.type_.as_ref(), wanted.type_.as_ref())
        && stated(live.selector.as_ref(), wanted.selector.as_ref())
        && ports(live) == ports(wanted)
        && stated(
            live.ip_family_policy.as_ref(),
            wanted.ip_family_policy.as_ref(),
        )
        && stated(live.ip_families.as_ref(), wanted.ip_families.as_ref())
        && stated(
            live.external_traffic_policy.as_ref(),
            wanted.external_traffic_policy.as_ref(),
        )
        && stated(
            live.session_affinity.as_ref(),
            wanted.session_affinity.as_ref(),
        )
        && stated(
            live.load_balancer_class.as_ref(),
            wanted.load_balancer_class.as_ref(),
        )
        && stated(
            ranges(live.load_balancer_source_ranges.as_ref()).as_ref(),
            ranges(wanted.load_balancer_source_ranges.as_ref()).as_ref(),
        );
    !(metadata && spec)
}

/// A port as it is compared: what of it the control plane states.
fn port(port: &ServicePort) -> String {
    format!(
        "{} {} {} {:?}",
        port.name.as_deref().unwrap_or_default(),
        port.port,
        port.protocol.as_deref().unwrap_or("TCP"),
        port.target_port
    )
}

/// The ports `service` lists, as said: `80/TCP`, `443/UDP`.
#[must_use]
pub fn listed(service: &Service) -> Vec<String> {
    service
        .spec
        .iter()
        .flat_map(|spec| spec.ports.iter().flatten())
        .map(|port| {
            format!(
                "{}/{}",
                port.port,
                port.protocol.as_deref().unwrap_or("TCP")
            )
        })
        .collect()
}

/// The addresses the data plane's Service gives, for status: a LoadBalancer's, from its
/// load balancer, none while that is pending; any other's, its cluster IPs, as Envoy
/// Gateway and Istio give for a ClusterIP Service. None for a Service not there.
#[must_use]
pub fn addresses(service: Option<&Service>) -> Vec<String> {
    let Some(service) = service else {
        return Vec::new();
    };
    let spec = service.spec.as_ref();
    if spec.and_then(|spec| spec.type_.as_deref()) == Some("LoadBalancer") {
        return service
            .status
            .as_ref()
            .and_then(|status| status.load_balancer.as_ref())
            .and_then(|balancer| balancer.ingress.as_ref())
            .into_iter()
            .flatten()
            .filter_map(|ingress| ingress.ip.clone().or_else(|| ingress.hostname.clone()))
            .collect();
    }
    spec.and_then(|spec| spec.cluster_ips.as_ref())
        .into_iter()
        .flatten()
        // A headless Service's, which is no address.
        .filter(|ip| *ip != "None")
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
