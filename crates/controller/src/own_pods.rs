//! The gateway's own pods, which no data plane may send to
//! ([03 §6](../../../docs/03-data-plane.md)): the control plane's replicas and the pods of
//! every data plane it serves, read from the EndpointSlices of their Services in the control plane's own
//! namespace, every endpoint, ready or not. The addresses are found without I/O by
//! [`addresses`]; [`follow_own_pods`] is the follow that hands them on.

use edgerush_kubernetes::{ApiServer, Listed, Retry, Scope, follow};
use edgerush_planes::objects::controller_name;
use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::Planes;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// The label the EndpointSlice controller names a slice's Service by.
const SERVICE_NAME: &str = "kubernetes.io/service-name";

/// The addresses of every endpoint of `slices` whose Service is one of `services`, ready,
/// not ready or terminating alike, sorted and each once. An address that is not one is
/// left out: the API server checks them, so none is expected.
#[must_use]
pub fn addresses(slices: &[EndpointSlice], services: &BTreeSet<String>) -> Arc<[IpAddr]> {
    slices
        .iter()
        .filter(|slice| {
            slice
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(SERVICE_NAME))
                .is_some_and(|service| services.contains(service))
        })
        .flat_map(|slice| slice.endpoints.iter().flatten())
        .flat_map(|endpoint| &endpoint.addresses)
        .filter_map(|address| address.parse().ok())
        .collect::<BTreeSet<IpAddr>>()
        .into_iter()
        .collect()
}

/// The Services whose pods are the gateway's own: the control plane's, of the name the
/// operator gives it for `control_plane`, and that of every data plane `planes` has with
/// one, each refused by every other as by itself (03 §6).
#[must_use]
pub fn services(control_plane: &str, planes: &Planes) -> BTreeSet<String> {
    let mut services = BTreeSet::from([controller_name(control_plane)]);
    for service in planes
        .values()
        .filter_map(|settings| settings.service.as_ref())
    {
        services.insert(service.name().to_owned());
    }
    services
}

/// Follows the EndpointSlices of `namespace`, the control plane's own, on `api`, and
/// publishes on `out` the addresses of [`services`]' endpoints, for `control_plane` and the
/// data planes `planes` says, each time they change, until no one holds a receiver of
/// `out`. Nothing is published until a list has been read whole; while the follow fails,
/// the last addresses are kept. Each failure is said to `say`.
pub async fn follow_own_pods(
    api: ApiServer,
    namespace: Scope,
    control_plane: String,
    mut planes: watch::Receiver<Planes>,
    out: watch::Sender<Arc<[IpAddr]>>,
    say: impl Fn(&str) + 'static,
) {
    let (listing, mut listed) = watch::channel(Arc::new(Listed::<EndpointSlice>::default()));
    let mut tasks = JoinSet::new();
    tasks.spawn_local(async move {
        follow(&api, &namespace, Retry::default(), listing, |failure| {
            say(&format!(
                "the gateway's own pods: endpointslices: {failure}"
            ));
        })
        .await;
    });
    let mut wanted = services(&control_plane, &planes.borrow_and_update());
    loop {
        tokio::select! {
            changed = listed.changed() => if changed.is_err() { return },
            changed = planes.changed() => {
                if changed.is_err() {
                    return;
                }
                wanted = services(&control_plane, &planes.borrow_and_update());
            }
            () = out.closed() => return,
        }
        let now = Arc::clone(&listed.borrow_and_update());
        if !now.listed {
            continue;
        }
        let found = addresses(&now.objects, &wanted);
        out.send_if_modified(|sent| {
            let changed = *sent != found;
            *sent = Arc::clone(&found);
            changed
        });
    }
}

#[cfg(test)]
mod tests;
