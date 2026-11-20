//! Backend protection's side of a config ([25](../../../docs/25-backend-protection.md)):
//! each protected upstream's bound, and the count of each of its endpoints, every worker's
//! requests together.
//!
//! **Counts outlive the config.** A reload swaps the config, not the requests in flight, so
//! the thread that builds a config finds each pod's count again from the config before
//! ([`Pods`]): a pod in both has one count, and a new config never starts it at zero beside
//! the old one's requests (25 §3).
//!
//! **A pod is whose it is.** A Kubernetes Service's pods are counted by address alone,
//! every upstream of the Service's ports sharing a pod's count; any other endpoint by its
//! address and port, its upstream's own (25 §3).

use edgerush_config::{Compiled, PodsOf};
use edgerush_protection::{PodCount, Pods};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

/// What a pod's count is filed under.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodKey {
    of: PodsOf,
    at: PodAddress,
}

/// Where a pod is, as far as its count is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PodAddress {
    /// A Service's pod, whichever of its ports a request is for.
    Pod(IpAddr),
    /// Any other endpoint.
    Endpoint(SocketAddr),
}

impl PodKey {
    fn of(pods_of: &PodsOf, address: SocketAddr) -> Self {
        let at = match pods_of {
            PodsOf::Service { .. } => PodAddress::Pod(address.ip()),
            PodsOf::Upstream(_) => PodAddress::Endpoint(address),
        };
        Self {
            of: pods_of.clone(),
            at,
        }
    }
}

/// A protected upstream in one config: its bound, and by position of the endpoint each
/// one's count.
#[derive(Debug)]
pub struct Protected {
    /// Requests in flight to one endpoint, every worker's together.
    pub cap: NonZeroU32,
    /// By position of the endpoint, as the config's endpoints are.
    pub counts: Box<[Arc<PodCount>]>,
}

/// By position of the upstream of `config`, whose endpoints are `endpoints`, its
/// protection if it has one; and every pod's count, found again from what the config
/// before had (`previous`).
#[must_use]
pub fn protected(
    config: &Compiled,
    endpoints: &[Vec<SocketAddr>],
    previous: Option<&Pods<PodKey>>,
) -> (Pods<PodKey>, Vec<Option<Arc<Protected>>>) {
    let mut pods = Pods::after(previous);
    let protected = config
        .upstreams()
        .iter()
        .enumerate()
        .map(|(position, upstream)| {
            let protection = upstream.protection.as_ref()?;
            let counts = endpoints
                .get(position)
                .into_iter()
                .flatten()
                .map(|&address| pods.count(PodKey::of(&protection.pods_of, address)))
                .collect();
            Some(Arc::new(Protected {
                cap: protection.max_in_flight,
                counts,
            }))
        })
        .collect();
    (pods, protected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Config, compile};

    fn config(upstreams: &str) -> Compiled {
        let yaml = format!("listeners: {{}}\nroutes: []\nupstreams:\n{upstreams}");
        compile(&serde_saphyr::from_str::<Config>(&yaml).unwrap()).unwrap()
    }

    fn address(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    const CAP: NonZeroU32 = NonZeroU32::new(1).unwrap();

    #[test]
    fn an_unprotected_upstream_has_no_counts() {
        let config =
            config("  plain: { load_balancer: p2c, endpoints: { listed: [\"127.0.0.1:81\"] } }\n");
        let (pods, protected) = protected(&config, &[vec![address("127.0.0.1:81")]], None);
        assert!(protected[0].is_none());
        assert!(pods.is_empty());
    }

    #[test]
    fn a_services_ports_share_a_pods_count_and_listed_endpoints_are_their_own() {
        let config = config(concat!(
            "  http: { load_balancer: p2c, endpoints: { kubernetes: \"search.shop.svc.cluster.local:http\" }, protection: { max_in_flight: 2 } }\n",
            "  grpc: { load_balancer: p2c, endpoints: { kubernetes: \"search.shop.svc.cluster.local:grpc\" }, protection: { max_in_flight: 2 } }\n",
            "  listed: { load_balancer: p2c, endpoints: { listed: [\"10.0.0.1:80\", \"10.0.0.1:81\"] }, protection: { max_in_flight: 2 } }\n",
        ));
        // Upstreams are by name: `grpc`, `http`, `listed`.
        let endpoints = [
            vec![address("10.0.0.1:9000")],
            vec![address("10.0.0.1:8080")],
            vec![address("10.0.0.1:80"), address("10.0.0.1:81")],
        ];
        let (pods, protected) = protected(&config, &endpoints, None);
        let counts = |at: usize| &protected[at].as_ref().unwrap().counts;
        assert!(
            Arc::ptr_eq(&counts(0)[0], &counts(1)[0]),
            "one pod, two ports"
        );
        assert!(
            !Arc::ptr_eq(&counts(2)[0], &counts(2)[1]),
            "two listed endpoints"
        );
        assert!(
            !Arc::ptr_eq(&counts(0)[0], &counts(2)[0]),
            "not the Service's"
        );
        assert_eq!(pods.len(), 3);
    }

    #[test]
    fn a_pod_kept_by_the_next_config_keeps_its_count_whatever_its_cap() {
        let first = config(
            "  up: { load_balancer: p2c, endpoints: { listed: [\"127.0.0.1:81\"] }, protection: { max_in_flight: 1 } }\n",
        );
        let second = config(
            "  up: { load_balancer: p2c, endpoints: { listed: [\"127.0.0.1:81\", \"127.0.0.1:82\"] }, protection: { max_in_flight: 5 } }\n",
        );
        let (pods, before) = protected(&first, &[vec![address("127.0.0.1:81")]], None);
        let held = PodCount::take(&before[0].as_ref().unwrap().counts[0], CAP).unwrap();
        let (_, after) = protected(
            &second,
            &[vec![address("127.0.0.1:81"), address("127.0.0.1:82")]],
            Some(&pods),
        );
        let after = after[0].as_ref().unwrap();
        assert_eq!(after.cap.get(), 5);
        assert_eq!(after.counts[0].in_flight(), 1, "its request still counts");
        assert_eq!(after.counts[1].in_flight(), 0);
        drop(held);
        assert_eq!(after.counts[0].in_flight(), 0);
    }
}
