//! A Kubernetes Service's endpoints, from its EndpointSlices
//! ([03 §6](../../../docs/03-data-plane.md)).
//!
//! A pure crate: the slices are passed in, as the data plane's watch has them, and
//! [`Endpoints::of`] says which addresses take requests. No I/O, no clocks.
//!
//! An endpoint's conditions are read as the API asks: `ready` unknown is ready, `serving`
//! unknown is `ready`'s value, `terminating` unknown is not terminating. A ready endpoint
//! takes requests. One that is terminating but still serving takes them only while the
//! Service has no ready endpoint, as kube-proxy has it, so that a Service of one pod being
//! replaced is not unreachable while the old pod finishes and the new one is not ready yet.
//! Any other takes none.
//!
//! An endpoint is its first address, which the API says the others are interchangeable
//! with, so that a pod is one endpoint however many addresses it lists. The port is the
//! slice's own for the Service's port, matched by its name, or, for the Service's only
//! port left unnamed, the one port without a name; TCP only. IPv4 and IPv6 slices both
//! count; FQDN slices do not, as endpoints are pod addresses. An address listed more than
//! once, as while slices are being replaced, is one endpoint, the readiest of its copies.
//!
//! Zone hints are followed as kube-proxy follows them: [`Endpoints::for_zone`] gives the
//! endpoints in use hinted for a zone only when every one in use carries a zone hint and at
//! least one names it. An address listed more than once is hinted only if every copy is,
//! for the zones of them all, so that a slice being replaced turns narrowing off rather
//! than half on. [`zone_of`] finds the data plane's own zone, that of the endpoint at one of
//! its own addresses in the slices of a Service that selects its pods.

use edgerush_config::{KubernetesService, ServicePort};
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice};
use std::collections::BTreeSet;
use std::collections::btree_map::{BTreeMap, Entry};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The label an EndpointSlice names its Service with.
pub const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// A Service's endpoints, for one of its ports: those ready, and those terminating that
/// still serve, and the zones each is hinted for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Endpoints {
    ready: BTreeSet<SocketAddr>,
    terminating: BTreeSet<SocketAddr>,
    /// The zones each is hinted for, those of every copy of it; none for one a copy of
    /// which carries no zone hint.
    zones: BTreeMap<SocketAddr, Option<BTreeSet<String>>>,
}

/// What an endpoint is fit for, readiest last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Fit {
    /// Terminating, and still serving.
    Terminating,
    /// Ready.
    Ready,
}

impl Endpoints {
    /// The endpoints of `service`'s port in those of `slices` that are its: in its
    /// namespace and labelled with its name. Others are passed over, as is whatever the
    /// data plane cannot use.
    #[must_use]
    pub fn of(service: &KubernetesService, slices: &[EndpointSlice]) -> Self {
        let mut fit: BTreeMap<SocketAddr, Fit> = BTreeMap::new();
        let mut zones: BTreeMap<SocketAddr, Option<BTreeSet<String>>> = BTreeMap::new();
        for slice in slices
            .iter()
            .filter(|slice| is_of(&service.namespace, &service.service, slice))
        {
            let Some(port) = slice
                .ports
                .iter()
                .flatten()
                .find(|port| is_port(&service.port, port))
                .and_then(|port| u16::try_from(port.port?).ok())
                .filter(|port| *port != 0)
            else {
                continue;
            };
            for endpoint in slice.endpoints.iter().flatten() {
                let Some(ip) = endpoint
                    .addresses
                    .first()
                    .and_then(|address| ip_of(&slice.address_type, address))
                else {
                    continue;
                };
                let Some(found) = fit_of(endpoint.conditions.as_ref()) else {
                    continue;
                };
                let address = SocketAddr::new(ip, port);
                let hinted = hinted_zones(endpoint);
                match zones.entry(address) {
                    Entry::Vacant(free) => {
                        free.insert(hinted);
                    }
                    Entry::Occupied(mut listed) => {
                        let both = listed.get_mut().take().zip(hinted).map(|(mut a, b)| {
                            a.extend(b);
                            a
                        });
                        listed.insert(both);
                    }
                }
                match fit.entry(address) {
                    Entry::Vacant(free) => {
                        free.insert(found);
                    }
                    Entry::Occupied(mut listed) => {
                        let readiest = found.max(*listed.get());
                        listed.insert(readiest);
                    }
                }
            }
        }
        let mut endpoints = Self {
            zones,
            ..Self::default()
        };
        for (address, fit) in fit {
            match fit {
                Fit::Ready => endpoints.ready.insert(address),
                Fit::Terminating => endpoints.terminating.insert(address),
            };
        }
        endpoints
    }

    /// Those ready.
    #[must_use]
    pub fn ready(&self) -> &BTreeSet<SocketAddr> {
        &self.ready
    }

    /// Those terminating that still serve.
    #[must_use]
    pub fn terminating(&self) -> &BTreeSet<SocketAddr> {
        &self.terminating
    }

    /// Those that take requests: the ready, or, while there are none, the terminating that
    /// still serve.
    #[must_use]
    pub fn in_use(&self) -> &BTreeSet<SocketAddr> {
        if self.ready.is_empty() {
            &self.terminating
        } else {
            &self.ready
        }
    }

    /// Of those in use, the ones hinted for `zone`, to be preferred (03 §6): only when every
    /// one in use carries a zone hint and at least one names `zone`, as kube-proxy has it,
    /// since hints are written after the endpoints and a Service partly hinted is one
    /// changing. Otherwise none, and all in use are used alike.
    #[must_use]
    pub fn for_zone(&self, zone: &str) -> Option<BTreeSet<SocketAddr>> {
        let mut preferred = BTreeSet::new();
        for address in self.in_use() {
            let zones = self.zones.get(address)?.as_ref()?;
            if zones.contains(zone) {
                preferred.insert(*address);
            }
        }
        (!preferred.is_empty()).then_some(preferred)
    }
}

/// The zone of the endpoint at one of `own`, the data plane's own addresses, in the slices
/// of Service `service` of `namespace`, one that selects the data plane's pods: the zone the
/// EndpointSlice controller writes from the pod's node. Any port, and an endpoint whether
/// ready or not, as one not ready yet is still the pod. None if no such endpoint is listed
/// with a zone.
#[must_use]
pub fn zone_of<'a>(
    namespace: &str,
    service: &str,
    slices: &'a [EndpointSlice],
    own: &[IpAddr],
) -> Option<&'a str> {
    slices
        .iter()
        .filter(|slice| is_of(namespace, service, slice))
        .flat_map(|slice| {
            slice
                .endpoints
                .iter()
                .flatten()
                .map(move |endpoint| (slice, endpoint))
        })
        .find_map(|(slice, endpoint)| {
            let at_own = endpoint.addresses.iter().any(|address| {
                ip_of(&slice.address_type, address).is_some_and(|ip| own.contains(&ip))
            });
            endpoint.zone.as_deref().filter(|_| at_own)
        })
}

/// The zones an endpoint is hinted for; none if it carries no zone hint.
fn hinted_zones(endpoint: &Endpoint) -> Option<BTreeSet<String>> {
    let zones: BTreeSet<String> = endpoint
        .hints
        .as_ref()?
        .for_zones
        .iter()
        .flatten()
        .map(|zone| zone.name.clone())
        .collect();
    (!zones.is_empty()).then_some(zones)
}

/// Whether a slice is Service `service`'s of `namespace`: in it, and labelled with its name.
fn is_of(namespace: &str, service: &str, slice: &EndpointSlice) -> bool {
    let metadata = &slice.metadata;
    metadata.namespace.as_deref() == Some(namespace)
        && metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(SERVICE_NAME_LABEL))
            .is_some_and(|name| name == service)
}

/// Whether a slice's port is the Service's: the one of its name, or, for the Service's only
/// port, the one without a name; over TCP, which a port that does not say is.
fn is_port(wanted: &ServicePort, port: &EndpointPort) -> bool {
    let name = port.name.as_deref().unwrap_or_default();
    let named = match wanted {
        ServicePort::Named(wanted) => name == wanted,
        ServicePort::Only(_) => name.is_empty(),
    };
    named && port.protocol.as_deref().unwrap_or("TCP") == "TCP"
}

/// An address of a slice of this type, if it is an IP address of the slice's family.
fn ip_of(address_type: &str, address: &str) -> Option<IpAddr> {
    match address_type {
        "IPv4" => address.parse::<Ipv4Addr>().ok().map(IpAddr::V4),
        "IPv6" => address.parse::<Ipv6Addr>().ok().map(IpAddr::V6),
        _ => None,
    }
}

/// What an endpoint of these conditions is fit for, read as the API asks; none if it takes
/// no requests at all.
fn fit_of(conditions: Option<&EndpointConditions>) -> Option<Fit> {
    let ready = conditions.and_then(|c| c.ready).unwrap_or(true);
    let serving = conditions.and_then(|c| c.serving).unwrap_or(ready);
    let terminating = conditions.and_then(|c| c.terminating).unwrap_or(false);
    if ready {
        Some(Fit::Ready)
    } else if serving && terminating {
        Some(Fit::Terminating)
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
