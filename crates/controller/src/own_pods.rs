//! The gateway's own pods, which no data plane may send to
//! ([03 §6](../../../docs/03-data-plane.md)): the control plane's replicas and the pods of
//! every data plane it serves, read from the EndpointSlices of their Services, in the
//! control plane's own namespace and in that of each DataPlane generated elsewhere
//! ([05 §3](../../../docs/05-operator.md)), every endpoint, ready or not. The addresses are
//! found without I/O by [`addresses`]; [`follow_own_pods`] is the follow that hands them
//! on. The same slices say the control plane's [`replicas`], which a follower finds the
//! leader among and the replica taking the lease asks for their snapshots
//! ([04 §2](../../../docs/04-control-plane.md)).

use edgerush_kubernetes::{ApiServer, Scope};
use edgerush_planes::objects::controller_name;
use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::Planes;
use crate::across::{Across, follow_across};
use crate::data_plane::in_part;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::watch;

/// The label the EndpointSlice controller names a slice's Service by.
const SERVICE_NAME: &str = "kubernetes.io/service-name";

/// A Service, by its namespace and name.
pub type Named = (String, String);

/// The addresses of every endpoint of `slices` whose Service, of its namespace, is one of
/// `services`, ready, not ready or terminating alike, sorted and each once. An address that
/// is not one is left out: the API server checks them, so none is expected.
#[must_use]
pub fn addresses(slices: &[EndpointSlice], services: &BTreeSet<Named>) -> Arc<[IpAddr]> {
    slices
        .iter()
        .filter(|slice| of(slice, services))
        .flat_map(|slice| slice.endpoints.iter().flatten())
        .flat_map(|endpoint| &endpoint.addresses)
        .filter_map(|address| address.parse().ok())
        .collect::<BTreeSet<IpAddr>>()
        .into_iter()
        .collect()
}

/// One of the control plane's replicas, as its own Service's EndpointSlices say it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Replica {
    /// Its pod's name, which the Lease names its holder by.
    pub pod: String,
    /// Its pod's address.
    pub address: IpAddr,
    /// Whether it is ready: where its endpoint says nothing of it, taken as ready, as the
    /// API says an endpoint of no `ready` is to be.
    pub ready: bool,
}

/// The control plane's replicas: each endpoint of `slices` of its own Service, of the name
/// the operator gives it for `control_plane`, in its own namespace `namespace`, that names
/// the pod it is of, by that pod's name, with its first address, sorted and each once.
#[must_use]
pub fn replicas(slices: &[EndpointSlice], namespace: &str, control_plane: &str) -> Vec<Replica> {
    let own = BTreeSet::from([(namespace.to_owned(), controller_name(control_plane))]);
    slices
        .iter()
        .filter(|slice| of(slice, &own))
        .flat_map(|slice| slice.endpoints.iter().flatten())
        .filter_map(|endpoint| {
            let pod = endpoint
                .target_ref
                .as_ref()
                .filter(|target| target.kind.as_deref() == Some("Pod"))?
                .name
                .clone()?;
            let address = endpoint.addresses.first()?.parse().ok()?;
            let ready = endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.ready)
                .unwrap_or(true);
            Some(Replica {
                pod,
                address,
                ready,
            })
        })
        .collect::<BTreeSet<Replica>>()
        .into_iter()
        .collect()
}

/// Whether `slice` is of one of `services`, of its namespace.
fn of(slice: &EndpointSlice, services: &BTreeSet<Named>) -> bool {
    let namespace = slice.metadata.namespace.clone().unwrap_or_default();
    slice
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(SERVICE_NAME))
        .is_some_and(|service| services.contains(&(namespace, service.clone())))
}

/// The Services whose pods are the gateway's own: the control plane's, of the name the
/// operator gives it for `control_plane`, in its own namespace `namespace`, and that of
/// every data plane `planes` has with one, of its DataPlane's namespace, each refused by
/// every other as by itself (03 §6).
#[must_use]
pub fn services(namespace: &str, control_plane: &str, planes: &Planes) -> BTreeSet<Named> {
    let mut services = BTreeSet::from([(namespace.to_owned(), controller_name(control_plane))]);
    for service in planes
        .values()
        .filter_map(|settings| settings.service.as_ref())
    {
        services.insert((service.namespace().to_owned(), service.name().to_owned()));
    }
    services
}

/// The namespaces whose EndpointSlices are followed for `services`: each of theirs once.
fn parts(services: &BTreeSet<Named>) -> BTreeSet<Scope> {
    services
        .iter()
        .map(|(namespace, _)| Scope::Namespace(namespace.clone()))
        .collect()
}

/// Follows on `api` the EndpointSlices of `namespace`, the control plane's own, and of the
/// namespace of each data plane's Service `planes` says, and publishes on `out` the
/// addresses of [`services`]' endpoints, for `control_plane` and those data planes, each
/// time they change, until no one holds a receiver of `out`. Nothing is published until the
/// namespaces first followed have been listed whole; one added after is taken in once it
/// is; while a follow fails, the last addresses of its namespace are kept. With `replicas`,
/// the control plane's [`replicas`] are published on it likewise. Each failure is said to
/// `say`. For a `LocalSet`.
pub async fn follow_own_pods(
    api: ApiServer,
    namespace: String,
    control_plane: String,
    mut planes: watch::Receiver<Planes>,
    out: watch::Sender<Arc<[IpAddr]>>,
    replicas_out: Option<watch::Sender<Arc<[Replica]>>>,
    say: impl Fn(&str) + 'static,
) {
    let mut wanted = services(&namespace, &control_plane, &planes.borrow_and_update());
    let (parting, held) = watch::channel(Some(parts(&wanted)));
    let (listing, mut listed) = watch::channel(Arc::new(Across::<EndpointSlice>::default()));
    let _following = tokio::task::spawn_local(follow_across(
        Rc::new(api),
        held,
        listing,
        move |part, failure| {
            say(&format!(
                "the gateway's own pods: endpointslices {}: {failure}",
                in_part(part)
            ));
        },
    ));
    loop {
        tokio::select! {
            changed = listed.changed() => if changed.is_err() { return },
            changed = planes.changed() => {
                if changed.is_err() {
                    return;
                }
                wanted = services(&namespace, &control_plane, &planes.borrow_and_update());
                parting.send_if_modified(|held| {
                    let now = Some(parts(&wanted));
                    let changed = *held != now;
                    *held = now;
                    changed
                });
            }
            () = out.closed() => return,
        }
        let now = Arc::clone(&listed.borrow_and_update());
        if !now.listed {
            continue;
        }
        let objects = now.objects();
        let found = addresses(&objects, &wanted);
        out.send_if_modified(|sent| {
            let changed = *sent != found;
            *sent = Arc::clone(&found);
            changed
        });
        if let Some(replicas_out) = &replicas_out {
            let found: Arc<[Replica]> = replicas(&objects, &namespace, &control_plane).into();
            replicas_out.send_if_modified(|sent| {
                let changed = *sent != found;
                *sent = Arc::clone(&found);
                changed
            });
        }
    }
}

#[cfg(test)]
mod tests;
