//! EdgeRush's control plane ([04](../../../docs/04-control-plane.md)): the objects a data
//! plane's config is made of, followed in the scope the control plane watches, translated
//! for the data plane once every kind has been listed whole, and each config that compiles
//! and differs from the last handed on. Where the data plane is a Gateway, the Gateway
//! API's kinds are followed too, each while its CRD serves the version read
//! ([`gateway_api`](edgerush_kubernetes::gateway_api)).
//!
//! The changes of a moment make one translation: it waits until no change has come for
//! [`Debounce::quiet`], and never more than [`Debounce::most`] after the first it waits on
//! (04 §2). Each translation is whole, of everything followed, and pure
//! ([`edgerush_translate`]); the follows and the waiting are all that is not. It writes the
//! status of the data plane's GatewayClass, its Gateway and each route of that Gateway
//! (HTTPRoute, GRPCRoute or TLSRoute) as the config handed on serves them, merged with what each says now as the spec has it;
//! where it is asked to, each Ingress's likewise (04 §6), [`status`]. The admission
//! webhook's reviews are read, the Ingress each asks about judged alone, and answered by
//! [`admission`] (04 §3).

use acks::{Pods, REPORT_LABEL};
use edgerush_config::ServiceName;
use edgerush_config::{ConfigError, compile};
use edgerush_gateway_api::backend_tls_policy::BackendTLSPolicy;
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::grpc_route::GRPCRoute;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::listener_set::ListenerSet;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
use edgerush_gateway_api::tcp_route::TCPRoute;
use edgerush_gateway_api::tls_route::TLSRoute;
use edgerush_gateway_api::udp_route::UDPRoute;
use edgerush_kubernetes::{
    ApiError, ApiServer, Failure, Kind, Listed, Retry, Scope, follow, lease,
};
use edgerush_translate::{
    Cluster, GatewayName, GatewayStatus, Object, Plane, PolicyStatus, RouteStatus, Translation,
    translate,
};
use gateway_status::Programmed;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::IngressLoadBalancerIngress;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::Instant;

pub mod acks;
pub mod admission;
pub mod data_plane;
pub mod expiry;
mod gateway_status;
pub mod issuing;
pub mod status;

use edgerush_kubernetes::gateway_api::{GatewayKind, Served};
use status::Addresses;

/// How long the data plane's pods are waited on to run a generation of its Gateway before
/// its `Programmed` says false (04 §6).
pub const PROGRAMMED_WITHIN: Duration = Duration::from_secs(30);

/// What the data-plane pods run, as the replicas that serve them say it (04 §6).
#[derive(Debug, Clone)]
pub struct Acks {
    /// What those this replica serves run.
    pub own: watch::Receiver<Pods>,
    /// Where every replica reports what its pods run, where there are several: their
    /// namespace, the Lease they share, which labels each report, and this replica's own
    /// report, which `own` says sooner.
    pub reports: Option<Reports>,
}

/// Where the replicas' reports are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reports {
    /// Their namespace.
    pub namespace: String,
    /// The Lease the replicas share.
    pub lease: String,
    /// This replica's own report's name.
    pub own: String,
}

/// How long changes are gathered before they are translated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Debounce {
    /// How long no change must have come.
    pub quiet: Duration,
    /// The most a change is waited on, however many come after it.
    pub most: Duration,
}

impl Default for Debounce {
    /// Contour's: 100 ms, and 500 ms at most.
    fn default() -> Self {
        Self {
            quiet: Duration::from_millis(100),
            most: Duration::from_millis(500),
        }
    }
}

/// What the control plane says as it goes, for its caller to log or act on.
#[derive(Debug)]
pub enum Said<'a> {
    /// These have not been listed whole, and nothing is translated until they are: said at
    /// the start, and again each time they are fewer, until none are left.
    Waiting(&'a [String]),
    /// What the cluster has of each Gateway API kind, where the data plane is a Gateway:
    /// said once each kind's CRD has been listed, and again whenever it changes. A kind not
    /// served is taken as having no objects.
    GatewayApi(&'a [(GatewayKind, Served)]),
    /// A namespace came to match the watch scope's selector, joined it, or stopped matching
    /// and was dropped with its objects (04 §1).
    Namespace {
        /// The namespace.
        namespace: &'a str,
        /// What became of it.
        now: Membership,
    },
    /// Asking for what is followed failed, and is asked again after a wait.
    Failed {
        /// What is followed: `secrets in shop`, `ingressclasses in the cluster`.
        followed: &'a str,
        /// What failed.
        failure: &'a Failure,
    },
    /// A translation that compiles and differs from the last: the data plane's config.
    Translated(&'a Translation),
    /// A translation the data plane would refuse, which is not handed on: the last one
    /// stays. Each object is compiled alone as it is translated, and left out if it is
    /// refused (04 §2), so only the data plane's own settings come to this.
    Refused {
        /// The translation.
        translation: &'a Translation,
        /// Why the data plane would refuse it.
        problems: &'a [ConfigError],
    },
    /// The status of a Gateway API object written, as the config handed on serves it; or
    /// why it could not be, to be written again at the next change.
    GatewayApiStatus {
        /// The object.
        object: &'a Object,
        /// Whether it says it now.
        written: Result<(), &'a ApiError>,
    },
    /// An Ingress's status written to say these addresses, none taking them away; or why it
    /// could not be, to be written again at the next change.
    Status {
        /// The Ingress.
        ingress: &'a Object,
        /// What it is to say.
        addresses: &'a [IngressLoadBalancerIngress],
        /// Whether it says it now.
        written: Result<(), &'a ApiError>,
    },
}

/// Where the objects of a data plane's config are followed (04 §1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchScope {
    /// These namespaces, or the cluster, for good.
    Parts(Vec<Scope>),
    /// The namespaces this label selector of Namespaces matches, written as the API server
    /// reads one, followed as they come to match and stop. One that comes to match after
    /// the first list of them is taken in whole, once every kind of it has been listed, the
    /// rest translated without it meanwhile; those of the first list are waited on as every
    /// kind is at the start.
    Selector(String),
}

/// What became of a namespace of a [`WatchScope::Selector`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    /// It came to match, and is followed, but not yet translated.
    Joining,
    /// Every kind of it has been listed, and it is translated with the rest.
    Joined,
    /// It no longer matches, and is followed no longer.
    Left,
}

/// Follows, through `api`, the kinds `plane`'s config is made of in `scope`,
/// IngressClasses in the cluster, and translates them for it each time they
/// change, once every one has been listed whole; says how it goes to `said`. Where `plane`
/// names a Gateway, the Gateway API's kinds are followed too, GatewayClasses in the
/// cluster and Gateways in that Gateway's namespace alone, each from when its CRD is listed
/// and serves the version read until it no longer does, and Namespaces in the cluster. With
/// `status`, it writes each Ingress's status to say the addresses it gives as the config
/// handed on serves the Ingress, where it says otherwise. It writes that of the data
/// plane's GatewayClass, its Gateway (those addresses among it, or none without `status`)
/// and each HTTPRoute, GRPCRoute and TLSRoute that names or named that Gateway likewise,
/// always. With `leading`,
/// only while it says this replica leads, every status that says otherwise written as it
/// comes to (02 §3).
/// The data plane's settings are what `planes` holds, as it changes: none, and nothing is
/// followed or translated until it holds some. A change that leaves what is followed as it
/// was, such as an Ingress's redirect, is translated again as any change is; one that changes
/// it, the data plane's Gateway or its own Service, has the follows begun again for it
/// ([05 §3](../../../docs/05-operator.md)).
/// Runs for as long as it is polled, its follows ended with it. For a thread and runtime of
/// its own, in a `LocalSet`.
#[allow(clippy::too_many_arguments)]
pub async fn control(
    api: ApiServer,
    scope: &WatchScope,
    mut planes: watch::Receiver<Option<Plane>>,
    debounce: Debounce,
    status: Option<Addresses>,
    leading: Option<watch::Receiver<bool>>,
    acks: Acks,
    said: impl Fn(Said<'_>) + 'static,
) {
    let api = Rc::new(api);
    let said: Rc<dyn Fn(Said<'_>)> = Rc::new(said);
    loop {
        let plane = planes.borrow_and_update().clone();
        match plane {
            Some(plane) => {
                serve(
                    &api,
                    scope,
                    plane,
                    &mut planes,
                    debounce,
                    status.as_ref(),
                    leading.as_ref(),
                    &acks,
                    &said,
                )
                .await;
            }
            // Settings that will never come are nothing to serve.
            None => {
                if planes.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

/// What a data plane's settings shape of what is followed: its Gateway, and its own Service.
fn shape(plane: &Plane) -> (Option<GatewayName>, Option<ServiceName>) {
    (plane.gateway.clone(), own_service(plane))
}

/// [`control`] for `plane`, until `planes` holds none, or settings of another [`shape`].
#[allow(clippy::too_many_arguments)]
async fn serve(
    api: &Rc<ApiServer>,
    scope: &WatchScope,
    mut plane: Plane,
    planes: &mut watch::Receiver<Option<Plane>>,
    debounce: Debounce,
    status: Option<&Addresses>,
    leading: Option<&watch::Receiver<bool>>,
    acks: &Acks,
    said: &Rc<dyn Fn(Said<'_>)>,
) {
    let changed = Rc::new(Notify::new());
    let mut tasks = JoinSet::new();
    // The settings changing is a change.
    {
        let (mut told, changed) = (planes.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                changed.notify_one();
            }
        });
    }
    let mut start = Start {
        tasks: &mut tasks,
        api,
        changed: &changed,
        said,
        joining: BTreeSet::new(),
    };
    // A selector's namespaces are followed once its Namespaces are listed.
    let (parts, selected) = match scope {
        WatchScope::Parts(parts) => (parts.clone(), None),
        WatchScope::Selector(selector) => (
            Vec::new(),
            start
                .follows(&[Scope::selected(selector)])
                .into_iter()
                .next(),
        ),
    };
    // A Gateway's certificateRefs of its own namespace need no grant, and are read there
    // whatever the scope (04 §1).
    let gateway_namespace = plane
        .gateway
        .as_ref()
        .map(|gateway| Scope::Namespace(gateway.namespace.clone()));
    let mut followed = Followed {
        ingress_classes: start.follows(&[Scope::Cluster]),
        ingresses: start.follows(&parts),
        services: start.follows(&parts),
        secrets: start.follows(&with_gateway_namespace(&parts, gateway_namespace.as_ref())),
        gateway_namespace,
        status_service: match status {
            Some(Addresses::Service { namespace, .. }) => {
                start.follows(&[Scope::Namespace(namespace.clone())])
            }
            _ => Vec::new(),
        },
        gateway_api: plane
            .gateway
            .as_ref()
            .map(|gateway| GatewayApi::start(&mut start, gateway)),
        watched: match scope {
            WatchScope::Parts(parts) => watched(parts),
            WatchScope::Selector(_) => Some(Vec::new()),
        },
        selected,
        parts,
        scoped: false,
        joining: BTreeSet::new(),
        reports: match &acks.reports {
            Some(reports) => start.follows(&[Scope::Namespace(reports.namespace.clone())]),
            None => Vec::new(),
        },
        ready: match own_service(&plane) {
            Some(service) => start.follows(&[Scope::Namespace(service.namespace)]),
            None => Vec::new(),
        },
    };
    // What this replica's pods run changing is a change.
    {
        let (mut told, changed) = (acks.own.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                changed.notify_one();
            }
        });
    }
    // The generation of the Gateway handed on, and when it was first.
    let mut handed_since: Option<(i64, Instant)> = None;
    // Coming to lead is a change: every status is looked at, as the last leader may have
    // left some stale.
    if let Some(leading) = leading {
        let (mut told, changed) = (leading.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                changed.notify_one();
            }
        });
    }
    let mut waiting = Waiting::default();
    let mut translator = Translator::default();
    // What the config handed on serves: the Ingresses claimed, and those served.
    let mut handed: Option<(Vec<Object>, Vec<Object>)> = None;
    // What the config handed on says the Gateway API's objects' status is to say.
    let mut handed_api: Option<Handed> = None;
    loop {
        // New settings: translated as any change is, unless what is followed is theirs no
        // longer. A sender gone leaves them as they were.
        if planes.has_changed().unwrap_or(false) {
            match planes.borrow_and_update().clone() {
                Some(next) if shape(&next) == shape(&plane) => plane = next,
                _ => return,
            }
        }
        // The namespaces of a selector, followed as they come to match and stop.
        if let Some(matching) = followed.matching() {
            let mut start = Start {
                tasks: &mut tasks,
                api,
                changed: &changed,
                said,
                joining: BTreeSet::new(),
            };
            for (namespace, now) in followed.rescope(&matching, &mut start) {
                said(Said::Namespace {
                    namespace: &namespace,
                    now,
                });
            }
        }
        // The follows of the Gateway API kinds, started and stopped as their CRDs say.
        if let Some(gateway_api) = &mut followed.gateway_api
            && let Some(served) = gateway_api.served()
            && gateway_api.last.as_ref() != Some(&served)
        {
            let mut start = Start {
                tasks: &mut tasks,
                api,
                changed: &changed,
                said,
                joining: followed.joining.clone(),
            };
            gateway_api.follow(&served, &mut start, &followed.parts);
            said(Said::GatewayApi(&served));
            gateway_api.last = Some(served);
        }
        // A namespace that came to match joins once every kind of it is listed.
        for namespace in followed.join() {
            said(Said::Namespace {
                namespace: &namespace,
                now: Membership::Joined,
            });
        }
        // Follows stopped are aborted: what is left of them is let go of.
        while tasks.try_join_next().is_some() {}
        match translator.step(&plane, followed.unlisted(), || followed.cluster()) {
            Step::Nothing => {}
            Step::Waiting(unlisted) => said(Said::Waiting(unlisted)),
            Step::Translated(translation) => {
                handed = Some((translation.claimed.clone(), translation.served.clone()));
                handed_api = Handed::of(translation);
                if let Some(handed) = &handed_api {
                    let generation = handed.status.generation;
                    if handed_since.is_none_or(|(since, _)| since != generation) {
                        handed_since = Some((generation, Instant::now()));
                    }
                }
                said(Said::Translated(translation));
            }
            Step::Refused {
                translation,
                problems,
            } => said(Said::Refused {
                translation,
                problems: &problems,
            }),
        }
        // Every round, so that a status changed by another, or written in vain, is written
        // again; one at a time, which is all the pace there is. A status written is a change
        // to its object, and the round it brings finds nothing more to write. The data
        // plane's addresses, where it is told where to find them, once they are listed.
        let addresses = match status {
            _ if !followed.status_listed() => None,
            None => Some(Vec::new()),
            Some(Addresses::Stated(addresses)) => Some(addresses.clone()),
            Some(Addresses::Service { name, .. }) => {
                let services = followed.status_services();
                let named = services
                    .iter()
                    .find(|service| service.metadata.name.as_deref() == Some(name.as_str()));
                Some(status::of_service(named))
            }
        };
        let leads = leading.is_none_or(|leads| *leads.borrow());
        // The Gateway API's, always, as the spec has every controller write them (04 §6).
        if let (Some(gateway_api), Some(handed), Some(addresses), true) =
            (&followed.gateway_api, &handed_api, &addresses, leads)
        {
            let addresses = gateway_status::addresses(addresses);
            let pods = followed.pods(acks);
            let ready = own_service(&plane).map(|service| followed.ready(&service));
            let overdue =
                handed_since.is_some_and(|(_, since)| since.elapsed() >= PROGRAMMED_WITHIN);
            let programmed =
                Programmed::of(handed.status.generation, &pods, ready.as_deref(), overdue);
            gateway_api
                .write_statuses(api, handed, &addresses, &programmed, &**said)
                .await;
        }
        // An Ingress's only where the control plane is told where the addresses are.
        if let (Some(_), Some((claimed, served)), Some(addresses), true) =
            (status, &handed, &addresses, leads)
        {
            let ingresses = followed.ingresses();
            for change in status::changes(claimed, served, &ingresses, addresses) {
                let written = match status::patch(&change.addresses) {
                    Ok(patch) => {
                        let object = &change.ingress;
                        api.patch_status::<Ingress>(&object.namespace, &object.name, &patch)
                            .await
                    }
                    Err(error) => Err(ApiError::Unreadable(error.to_string())),
                };
                said(Said::Status {
                    ingress: &change.ingress,
                    addresses: &change.addresses,
                    written: written.as_ref().map(|_| ()),
                });
            }
        }
        // Until a change comes, or the bound its pods are waited on to run the Gateway's
        // generation, and then until its moment is over.
        let bound = handed_since
            .map(|(_, since)| since + PROGRAMMED_WITHIN)
            .filter(|bound| *bound > Instant::now());
        match bound {
            Some(bound) => {
                if tokio::time::timeout_at(bound, changed.notified())
                    .await
                    .is_err()
                {
                    continue;
                }
            }
            None => changed.notified().await,
        }
        waiting.changed(Instant::now());
        while let Some(due) = waiting.due(debounce) {
            if tokio::time::timeout_at(due, changed.notified())
                .await
                .is_ok()
            {
                waiting.changed(Instant::now());
            } else {
                waiting = Waiting::default();
            }
        }
    }
}

/// What starts the follows.
struct Start<'a> {
    tasks: &'a mut JoinSet<()>,
    api: &'a Rc<ApiServer>,
    changed: &'a Rc<Notify>,
    said: &'a Rc<dyn Fn(Said<'_>)>,
    /// The namespaces joining the scope: a follow started in one is held out of
    /// translation, and not waited on, until the namespace joins.
    joining: BTreeSet<String>,
}

impl Start<'_> {
    /// Starts following `K`'s objects in each of `scope`'s parts.
    fn follows<K: Kind + 'static>(&mut self, scope: &[Scope]) -> Vec<Follow<K>> {
        scope
            .iter()
            .map(|part| {
                let name = followed::<K>(part);
                let joining = matches!(part, Scope::Namespace(namespace)
                    if self.joining.contains(namespace));
                let (out, objects) = watch::channel(Arc::new(Listed::default()));
                let (api, said, followed_part, failed) = (
                    Rc::clone(self.api),
                    Rc::clone(self.said),
                    part.clone(),
                    name.clone(),
                );
                let following = self.tasks.spawn_local(async move {
                    follow(&api, &followed_part, Retry::default(), out, |failure| {
                        said(Said::Failed {
                            followed: &failed,
                            failure,
                        });
                    })
                    .await;
                });
                // Each change to the objects is a change to wait out.
                let (mut told, changed) = (objects.clone(), Rc::clone(self.changed));
                let telling = self.tasks.spawn_local(async move {
                    while told.changed().await.is_ok() {
                        changed.notify_one();
                    }
                });
                Follow {
                    name,
                    part: part.clone(),
                    joining,
                    objects,
                    _tasks: Tasks(vec![following, telling]),
                }
            })
            .collect()
    }
}

/// What a follow of `K` in `part` is called: `secrets in shop`.
fn followed<K: Kind>(part: &Scope) -> String {
    match part {
        Scope::Cluster => format!("{} in the cluster", K::URL_PATH_SEGMENT),
        Scope::Namespace(namespace) => format!("{} in {namespace}", K::URL_PATH_SEGMENT),
        Scope::Named(name) => format!("{} named {name}", K::URL_PATH_SEGMENT),
        Scope::Labelled(labels) if labels.is_empty() => {
            format!("{} in the cluster", K::URL_PATH_SEGMENT)
        }
        Scope::Labelled(labels) => format!("{} labelled {labels}", K::URL_PATH_SEGMENT),
    }
}

/// A kind's objects in one part of the scope, as their follow last published them. Let go
/// of, it is stopped.
struct Follow<K> {
    name: String,
    /// The part of the scope it follows.
    part: Scope,
    /// Whether it was started in a namespace joining the scope, which has not joined yet:
    /// its objects are held out of translation, and not waited on.
    joining: bool,
    objects: watch::Receiver<Arc<Listed<K>>>,
    _tasks: Tasks,
}

/// A follow in a part of the scope, of whatever kind.
trait Part {
    /// The part of the scope it follows.
    fn part(&self) -> &Scope;
    /// Whether it is held out until its namespace joins.
    fn joining(&self) -> bool;
    /// Whether it has been listed whole.
    fn listed(&self) -> bool;
    /// Its namespace joins the scope.
    fn join(&mut self);
}

impl<K> Part for Follow<K> {
    fn part(&self) -> &Scope {
        &self.part
    }

    fn joining(&self) -> bool {
        self.joining
    }

    fn listed(&self) -> bool {
        self.objects.borrow().listed
    }

    fn join(&mut self) {
        self.joining = false;
    }
}

/// `follows` made to follow `parts`: each of a part no longer among them stopped, and one
/// started in each part that has none, in the order of their parts.
fn rescope<K: Kind + 'static>(
    follows: &mut Vec<Follow<K>>,
    parts: &[Scope],
    start: &mut Start<'_>,
) {
    follows.retain(|follow| parts.contains(&follow.part));
    let missing: Vec<Scope> = parts
        .iter()
        .filter(|part| !follows.iter().any(|follow| follow.part == **part))
        .cloned()
        .collect();
    follows.extend(start.follows(&missing));
    follows.sort_by(|a, b| a.part.cmp(&b.part));
}

/// `follows`, each as a [`Part`], onto `into`.
fn parts_of<'a, K>(follows: &'a mut [Follow<K>], into: &mut Vec<&'a mut dyn Part>) {
    into.extend(follows.iter_mut().map(|follow| follow as &mut dyn Part));
}

/// A follow's tasks, aborted as it is let go of.
struct Tasks(Vec<AbortHandle>);

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// The follows of the Gateway API's kinds: each kind's CRD by its name, and the kind's
/// objects while the CRD serves the version read.
struct GatewayApi {
    crds: Vec<(GatewayKind, Follow<CustomResourceDefinition>)>,
    /// Every Namespace, for the labels a listener's selector of namespaces matches: a kind
    /// every cluster has, so followed whatever the CRDs say.
    namespaces: Vec<Follow<Namespace>>,
    /// The data plane's Gateway's namespace: the one Gateways are followed in, as no other
    /// Gateway is read (04 §2).
    namespace: Scope,
    /// What was last said of the kinds, and followed by.
    last: Option<Vec<(GatewayKind, Served)>>,
    gateway_classes: Option<Vec<Follow<GatewayClass>>>,
    gateways: Option<Vec<Follow<Gateway>>>,
    listener_sets: Option<Vec<Follow<ListenerSet>>>,
    http_routes: Option<Vec<Follow<HTTPRoute>>>,
    grpc_routes: Option<Vec<Follow<GRPCRoute>>>,
    tls_routes: Option<Vec<Follow<TLSRoute>>>,
    tcp_routes: Option<Vec<Follow<TCPRoute>>>,
    udp_routes: Option<Vec<Follow<UDPRoute>>>,
    reference_grants: Option<Vec<Follow<ReferenceGrant>>>,
    backend_tls_policies: Option<Vec<Follow<BackendTLSPolicy>>>,
    /// The ConfigMaps of the watch scope, followed while BackendTLSPolicies are, for the
    /// CA certificates they name (04 §2).
    config_maps: Option<Vec<Follow<ConfigMap>>>,
}

impl GatewayApi {
    /// Starts following each kind's CRD; the kinds wait on what they say.
    fn start(start: &mut Start<'_>, gateway: &GatewayName) -> Self {
        let crds = GatewayKind::ALL
            .into_iter()
            .flat_map(|kind| {
                start
                    .follows(&[Scope::Named(kind.crd().to_owned())])
                    .into_iter()
                    .map(move |follow| (kind, follow))
            })
            .collect();
        Self {
            crds,
            namespaces: start.follows(&[Scope::Cluster]),
            namespace: Scope::Namespace(gateway.namespace.clone()),
            last: None,
            gateway_classes: None,
            gateways: None,
            listener_sets: None,
            http_routes: None,
            grpc_routes: None,
            tls_routes: None,
            tcp_routes: None,
            udp_routes: None,
            reference_grants: None,
            backend_tls_policies: None,
            config_maps: None,
        }
    }

    /// What the cluster has of each kind, once every CRD has been listed.
    fn served(&self) -> Option<Vec<(GatewayKind, Served)>> {
        self.crds
            .iter()
            .map(|(kind, follow)| {
                let listed = follow.objects.borrow();
                listed.listed.then(|| (*kind, Served::of(&listed.objects)))
            })
            .collect()
    }

    /// Follows each kind `served` says is served, in `scope` (GatewayClasses in the
    /// cluster, Gateways in the data plane's Gateway's namespace; ListenerSets, HTTPRoutes,
    /// GRPCRoutes, TLSRoutes, TCPRoutes, UDPRoutes, ReferenceGrants and BackendTLSPolicies
    /// in `scope`, with the ConfigMaps there while BackendTLSPolicies are), and stops
    /// following each it says is not.
    fn follow(&mut self, served: &[(GatewayKind, Served)], start: &mut Start<'_>, scope: &[Scope]) {
        for (kind, served) in served {
            let on = served.is_served();
            match kind {
                GatewayKind::GatewayClass => {
                    keep(&mut self.gateway_classes, on, || {
                        start.follows(&[Scope::Cluster])
                    });
                }
                GatewayKind::Gateway => {
                    let namespace = std::slice::from_ref(&self.namespace);
                    keep(&mut self.gateways, on, || start.follows(namespace));
                }
                GatewayKind::ListenerSet => {
                    let scope = with_gateway_namespace(scope, Some(&self.namespace));
                    keep(&mut self.listener_sets, on, || start.follows(&scope));
                }
                GatewayKind::HTTPRoute => keep(&mut self.http_routes, on, || start.follows(scope)),
                GatewayKind::GRPCRoute => keep(&mut self.grpc_routes, on, || start.follows(scope)),
                GatewayKind::TLSRoute => keep(&mut self.tls_routes, on, || start.follows(scope)),
                GatewayKind::TCPRoute => keep(&mut self.tcp_routes, on, || start.follows(scope)),
                GatewayKind::UDPRoute => keep(&mut self.udp_routes, on, || start.follows(scope)),
                GatewayKind::ReferenceGrant => {
                    keep(&mut self.reference_grants, on, || start.follows(scope));
                }
                GatewayKind::BackendTLSPolicy => {
                    keep(&mut self.backend_tls_policies, on, || start.follows(scope));
                    keep(&mut self.config_maps, on, || start.follows(scope));
                }
            }
        }
    }

    /// Every follow translation waits on that has not been listed whole: the CRDs', and
    /// those of each kind followed.
    fn unlisted(&self) -> impl Iterator<Item = String> + '_ {
        self.crds
            .iter()
            .map(|(_, follow)| follow)
            .filter(|follow| !follow.objects.borrow().listed)
            .map(|follow| follow.name.clone())
            .chain(unlisted(&self.namespaces))
            .chain(unlisted(
                self.gateway_classes.as_deref().unwrap_or_default(),
            ))
            .chain(unlisted(self.gateways.as_deref().unwrap_or_default()))
            .chain(unlisted(self.listener_sets.as_deref().unwrap_or_default()))
            .chain(unlisted(self.http_routes.as_deref().unwrap_or_default()))
            .chain(unlisted(self.grpc_routes.as_deref().unwrap_or_default()))
            .chain(unlisted(self.tls_routes.as_deref().unwrap_or_default()))
            .chain(unlisted(self.tcp_routes.as_deref().unwrap_or_default()))
            .chain(unlisted(self.udp_routes.as_deref().unwrap_or_default()))
            .chain(unlisted(
                self.reference_grants.as_deref().unwrap_or_default(),
            ))
            .chain(unlisted(
                self.backend_tls_policies.as_deref().unwrap_or_default(),
            ))
            .chain(unlisted(self.config_maps.as_deref().unwrap_or_default()))
    }

    /// The follows of each kind followed in the watch scope made to follow `parts`, and
    /// ListenerSets the Gateway's namespace beside them.
    fn rescope(&mut self, parts: &[Scope], start: &mut Start<'_>) {
        fn each<K: Kind + 'static>(
            slot: &mut Option<Vec<Follow<K>>>,
            parts: &[Scope],
            start: &mut Start<'_>,
        ) {
            if let Some(follows) = slot {
                rescope(follows, parts, start);
            }
        }
        let listener_sets = with_gateway_namespace(parts, Some(&self.namespace));
        each(&mut self.listener_sets, &listener_sets, start);
        each(&mut self.http_routes, parts, start);
        each(&mut self.grpc_routes, parts, start);
        each(&mut self.tls_routes, parts, start);
        each(&mut self.tcp_routes, parts, start);
        each(&mut self.udp_routes, parts, start);
        each(&mut self.reference_grants, parts, start);
        each(&mut self.backend_tls_policies, parts, start);
        each(&mut self.config_maps, parts, start);
    }

    /// The follows of each kind followed in the watch scope, onto `into`.
    fn parts<'a>(&'a mut self, into: &mut Vec<&'a mut dyn Part>) {
        fn each<'a, K>(slot: &'a mut Option<Vec<Follow<K>>>, into: &mut Vec<&'a mut dyn Part>) {
            if let Some(follows) = slot {
                parts_of(follows, into);
            }
        }
        each(&mut self.listener_sets, into);
        each(&mut self.http_routes, into);
        each(&mut self.grpc_routes, into);
        each(&mut self.tls_routes, into);
        each(&mut self.tcp_routes, into);
        each(&mut self.udp_routes, into);
        each(&mut self.reference_grants, into);
        each(&mut self.backend_tls_policies, into);
        each(&mut self.config_maps, into);
    }
}

/// What the config handed on says the status of the Gateway API's objects is to say.
#[derive(Debug)]
struct Handed {
    /// The data plane's Gateway.
    gateway: Object,
    /// Its status.
    status: GatewayStatus,
    /// That of each route, of either kind, that names it.
    routes: Vec<RouteStatus>,
    /// That of each BackendTLSPolicy a route of it reaches.
    policies: Vec<PolicyStatus>,
}

impl Handed {
    /// What `translation` says, where it translated the data plane's Gateway.
    fn of(translation: &Translation) -> Option<Self> {
        let gateway = translation.gateway.as_ref()?;
        Some(Self {
            gateway: gateway.gateway.clone(),
            status: translation.gateway_status.clone()?,
            routes: translation.route_statuses.clone(),
            policies: translation.policy_statuses.clone(),
        })
    }
}

impl GatewayApi {
    /// Writes, one at a time, each status that says other than `handed` has it: that of
    /// the data plane's Gateway's class, of the Gateway, `addresses` among it, of each
    /// ListenerSet naming it, of each route followed, HTTPRoute, GRPCRoute, TLSRoute,
    /// TCPRoute or UDPRoute, whether or not it still names the Gateway or one of those
    /// ListenerSets, and
    /// of each BackendTLSPolicy followed, whether or not a route of the Gateway reaches it.
    async fn write_statuses(
        &self,
        api: &ApiServer,
        handed: &Handed,
        addresses: &[edgerush_gateway_api::gateway::GatewayStatusAddresses],
        programmed: &Programmed,
        said: &dyn Fn(Said<'_>),
    ) {
        let now = lease::wall_clock();
        let gateways = self.gateways.as_deref().map(objects).unwrap_or_default();
        let Some(gateway) = gateways.iter().find(|gateway| {
            gateway.metadata.namespace.as_deref() == Some(handed.gateway.namespace.as_str())
                && gateway.metadata.name.as_deref() == Some(handed.gateway.name.as_str())
        }) else {
            return;
        };
        let classes = self
            .gateway_classes
            .as_deref()
            .map(objects)
            .unwrap_or_default();
        let class = classes.iter().find(|class| {
            class.metadata.name.as_deref() == Some(gateway.spec.gateway_class_name.as_str())
        });
        if let (Some(class), Some(served)) = (class, &self.last) {
            let generation = class.metadata.generation.unwrap_or_default();
            let status = gateway_status::class(class.status.as_ref(), generation, served, now);
            if class.status.as_ref() != Some(&status) {
                let object = Object {
                    kind: "GatewayClass",
                    namespace: String::new(),
                    name: gateway.spec.gateway_class_name.clone(),
                };
                let version = class
                    .metadata
                    .resource_version
                    .as_deref()
                    .unwrap_or_default();
                let result = api
                    .replace_status::<GatewayClass>("", &object.name, version, &status)
                    .await;
                said(Said::GatewayApiStatus {
                    object: &object,
                    written: result.as_ref().map(|_| ()),
                });
            }
        }
        let status = gateway_status::gateway(
            gateway.status.as_ref(),
            &handed.status,
            addresses,
            programmed,
            now,
        );
        if gateway.status.as_ref() != Some(&status) {
            let object = &handed.gateway;
            let version = gateway
                .metadata
                .resource_version
                .as_deref()
                .unwrap_or_default();
            let result = api
                .replace_status::<Gateway>(&object.namespace, &object.name, version, &status)
                .await;
            said(Said::GatewayApiStatus {
                object,
                written: result.as_ref().map(|_| ()),
            });
        }
        let sets = self
            .listener_sets
            .as_deref()
            .map(objects)
            .unwrap_or_default();
        for wanted in &handed.status.listener_sets {
            let object = &wanted.set;
            let Some(set) = sets.iter().find(|set| {
                set.metadata.namespace.as_deref() == Some(object.namespace.as_str())
                    && set.metadata.name.as_deref() == Some(object.name.as_str())
            }) else {
                continue;
            };
            let status = gateway_status::listener_set(set.status.as_ref(), wanted, programmed, now);
            if set.status.as_ref() != Some(&status) {
                let version = set.metadata.resource_version.as_deref().unwrap_or_default();
                let result = api
                    .replace_status::<ListenerSet>(
                        &object.namespace,
                        &object.name,
                        version,
                        &status,
                    )
                    .await;
                said(Said::GatewayApiStatus {
                    object,
                    written: result.as_ref().map(|_| ()),
                });
            }
        }
        // A route's parents of this data plane's: its Gateway, and each ListenerSet naming
        // it, taken or not.
        let parents_of_ours: Vec<Object> = std::iter::once(handed.gateway.clone())
            .chain(
                handed
                    .status
                    .listener_sets
                    .iter()
                    .map(|set| set.set.clone()),
            )
            .collect();
        // A GRPCRoute's, a TLSRoute's, a TCPRoute's and a UDPRoute's status are merged and
        // written as an HTTPRoute's, of one type in the spec (gateway_status::grpc_as_http,
        // tls_as_http, tcp_as_http, udp_as_http).
        let http_routes = self.http_routes.as_deref().map(objects).unwrap_or_default();
        let grpc_routes = self.grpc_routes.as_deref().map(objects).unwrap_or_default();
        let tls_routes = self.tls_routes.as_deref().map(objects).unwrap_or_default();
        let tcp_routes = self.tcp_routes.as_deref().map(objects).unwrap_or_default();
        let udp_routes = self.udp_routes.as_deref().map(objects).unwrap_or_default();
        let routes = http_routes
            .iter()
            .map(|route| ("HTTPRoute", &route.metadata, route.status.clone()))
            .chain(grpc_routes.iter().map(|route| {
                let status = route.status.as_ref().map(gateway_status::grpc_as_http);
                ("GRPCRoute", &route.metadata, status)
            }))
            .chain(tls_routes.iter().map(|route| {
                let status = route.status.as_ref().map(gateway_status::tls_as_http);
                ("TLSRoute", &route.metadata, status)
            }))
            .chain(tcp_routes.iter().map(|route| {
                let status = route.status.as_ref().map(gateway_status::tcp_as_http);
                ("TCPRoute", &route.metadata, status)
            }))
            .chain(udp_routes.iter().map(|route| {
                let status = route.status.as_ref().map(gateway_status::udp_as_http);
                ("UDPRoute", &route.metadata, status)
            }));
        for (kind, metadata, live) in routes {
            let object = Object {
                kind,
                namespace: metadata.namespace.clone().unwrap_or_default(),
                name: metadata.name.clone().unwrap_or_default(),
            };
            let wanted = handed.routes.iter().find(|wanted| wanted.route == object);
            let status = gateway_status::route(
                live.as_ref(),
                &object.namespace,
                &parents_of_ours,
                wanted,
                now,
            );
            if live.unwrap_or_default() != status {
                let version = metadata.resource_version.as_deref().unwrap_or_default();
                let (namespace, name) = (&object.namespace, &object.name);
                let result = match kind {
                    "GRPCRoute" => {
                        api.replace_status::<GRPCRoute>(namespace, name, version, &status)
                            .await
                    }
                    "TLSRoute" => {
                        api.replace_status::<TLSRoute>(namespace, name, version, &status)
                            .await
                    }
                    "TCPRoute" => {
                        api.replace_status::<TCPRoute>(namespace, name, version, &status)
                            .await
                    }
                    "UDPRoute" => {
                        api.replace_status::<UDPRoute>(namespace, name, version, &status)
                            .await
                    }
                    _ => {
                        api.replace_status::<HTTPRoute>(namespace, name, version, &status)
                            .await
                    }
                };
                said(Said::GatewayApiStatus {
                    object: &object,
                    written: result.as_ref().map(|_| ()),
                });
            }
        }
        let policies = self
            .backend_tls_policies
            .as_deref()
            .map(objects)
            .unwrap_or_default();
        for policy in &policies {
            let object = Object {
                kind: "BackendTLSPolicy",
                namespace: policy.metadata.namespace.clone().unwrap_or_default(),
                name: policy.metadata.name.clone().unwrap_or_default(),
            };
            let wanted = handed
                .policies
                .iter()
                .find(|wanted| wanted.policy == object);
            let status = gateway_status::policy(
                policy.status.as_ref(),
                &object.namespace,
                &handed.gateway,
                wanted,
                now,
            );
            if policy.status.clone().unwrap_or_default() != status {
                let version = policy
                    .metadata
                    .resource_version
                    .as_deref()
                    .unwrap_or_default();
                let result = api
                    .replace_status::<BackendTLSPolicy>(
                        &object.namespace,
                        &object.name,
                        version,
                        &status,
                    )
                    .await;
                said(Said::GatewayApiStatus {
                    object: &object,
                    written: result.as_ref().map(|_| ()),
                });
            }
        }
    }
}

/// `slot`'s follows started where `on` and there are none, and stopped where not `on`.
fn keep<K>(slot: &mut Option<Vec<Follow<K>>>, on: bool, start: impl FnOnce() -> Vec<Follow<K>>) {
    if on && slot.is_none() {
        *slot = Some(start());
    } else if !on {
        *slot = None;
    }
}

/// The names of `follows` that have not been listed whole, but for those held out until
/// their namespace joins, which nothing waits on.
fn unlisted<K>(follows: &[Follow<K>]) -> impl Iterator<Item = String> + '_ {
    follows
        .iter()
        .filter(|follow| !follow.joining && !follow.objects.borrow().listed)
        .map(|follow| follow.name.clone())
}

/// Every follow, by kind.
struct Followed {
    ingress_classes: Vec<Follow<IngressClass>>,
    ingresses: Vec<Follow<Ingress>>,
    services: Vec<Follow<Service>>,
    secrets: Vec<Follow<Secret>>,
    /// The data plane's Gateway's namespace, where it names one, whose Secrets are followed
    /// whatever the scope.
    gateway_namespace: Option<Scope>,
    /// The namespace of the data plane's Service, followed for its addresses.
    status_service: Vec<Follow<Service>>,
    /// The Gateway API's kinds, where the data plane is a Gateway.
    gateway_api: Option<GatewayApi>,
    /// The namespaces watched, where not every one is: of a selector, those joined.
    watched: Option<Vec<String>>,
    /// The Namespaces a selector scope matches, where the scope is one.
    selected: Option<Follow<Namespace>>,
    /// The parts of the scope followed: of a selector, every namespace it matches, joined
    /// or joining, once its Namespaces are listed.
    parts: Vec<Scope>,
    /// Whether a selector's first list of Namespaces has been followed.
    scoped: bool,
    /// The namespaces of a selector that came to match after its first list and have not
    /// joined yet.
    joining: BTreeSet<String>,
    /// The namespace of the replicas' reports of what their pods run.
    reports: Vec<Follow<ConfigMap>>,
    /// The namespace of the data plane's own Service, for its ready pods.
    ready: Vec<Follow<EndpointSlice>>,
}

impl Followed {
    /// The follows translation waits on that have not been listed whole, by name.
    fn unlisted(&self) -> Vec<String> {
        unlisted(self.selected.as_slice())
            .chain(unlisted(&self.ingress_classes))
            .chain(unlisted(&self.ingresses))
            .chain(unlisted(&self.services))
            .chain(unlisted(&self.secrets))
            .chain(self.gateway_api.iter().flat_map(GatewayApi::unlisted))
            .collect()
    }

    /// Whether the data plane's Service's namespace has been listed whole: what statuses
    /// wait for, and translation does not, so that a config is never held up by its status.
    fn status_listed(&self) -> bool {
        self.status_service
            .iter()
            .all(|follow| follow.objects.borrow().listed)
    }

    /// What each data-plane pod runs: as this replica's own say, and as every other
    /// replica's report of `acks` says for the rest.
    fn pods(&self, acks: &Acks) -> Pods {
        let mut pods = acks.own.borrow().clone();
        if let Some(reports) = &acks.reports {
            for report in objects(&self.reports) {
                let labels = report.metadata.labels.as_ref();
                let theirs = labels.and_then(|labels| labels.get(REPORT_LABEL))
                    == Some(&reports.lease)
                    && report.metadata.name.as_deref() != Some(reports.own.as_str());
                if let (true, Some(said)) = (theirs, acks::read(&report)) {
                    for (pod, runs) in said {
                        pods.entry(pod).or_insert(runs);
                    }
                }
            }
        }
        pods
    }

    /// The addresses of `service`'s ready endpoints: the data plane's ready pods.
    fn ready(&self, service: &ServiceName) -> Vec<IpAddr> {
        objects(&self.ready)
            .iter()
            .filter(|slice| {
                slice
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kubernetes.io/service-name"))
                    == Some(&service.service)
            })
            .flat_map(|slice| slice.endpoints.iter().flatten())
            .filter(|endpoint| {
                endpoint
                    .conditions
                    .as_ref()
                    .and_then(|conditions| conditions.ready)
                    != Some(false)
            })
            .flat_map(|endpoint| &endpoint.addresses)
            .filter_map(|address| address.parse().ok())
            .collect::<std::collections::BTreeSet<IpAddr>>()
            .into_iter()
            .collect()
    }

    /// The Ingresses followed, as they are now.
    fn ingresses(&self) -> Vec<Ingress> {
        objects(&self.ingresses)
    }

    /// The Services of the data plane's Service's namespace, as they are now.
    fn status_services(&self) -> Vec<Service> {
        objects(&self.status_service)
    }

    /// Every object followed, as translation takes them.
    fn cluster(&self) -> Cluster {
        let gateway_api = self.gateway_api.as_ref();
        // A kind not followed has no objects.
        fn of<K: Clone>(follows: Option<&Vec<Follow<K>>>) -> Vec<K> {
            follows.map(|follows| objects(follows)).unwrap_or_default()
        }
        Cluster {
            ingress_classes: objects(&self.ingress_classes),
            ingresses: objects(&self.ingresses),
            services: objects(&self.services),
            secrets: objects(&self.secrets),
            secrets_assumed: false,
            gateway_classes: of(gateway_api.and_then(|g| g.gateway_classes.as_ref())),
            gateways: of(gateway_api.and_then(|g| g.gateways.as_ref())),
            listener_sets: of(gateway_api.and_then(|g| g.listener_sets.as_ref())),
            http_routes: of(gateway_api.and_then(|g| g.http_routes.as_ref())),
            grpc_routes: of(gateway_api.and_then(|g| g.grpc_routes.as_ref())),
            tls_routes: of(gateway_api.and_then(|g| g.tls_routes.as_ref())),
            tcp_routes: of(gateway_api.and_then(|g| g.tcp_routes.as_ref())),
            udp_routes: of(gateway_api.and_then(|g| g.udp_routes.as_ref())),
            reference_grants: of(gateway_api.and_then(|g| g.reference_grants.as_ref())),
            backend_tls_policies: of(gateway_api.and_then(|g| g.backend_tls_policies.as_ref())),
            config_maps: of(gateway_api.and_then(|g| g.config_maps.as_ref())),
            namespaces: gateway_api
                .map(|g| objects(&g.namespaces))
                .unwrap_or_default(),
            watched: self.watched.clone(),
        }
    }
}

impl Followed {
    /// The namespaces a selector scope matches, once its Namespaces are listed whole.
    fn matching(&self) -> Option<BTreeSet<String>> {
        let listed = self.selected.as_ref()?.objects.borrow();
        listed.listed.then(|| {
            listed
                .objects
                .iter()
                .filter_map(|namespace| namespace.metadata.name.clone())
                .collect()
        })
    }

    /// Follows the namespaces `matching`, where they differ from those followed: each that
    /// no longer matches stopped, its objects with it; each that comes to match started,
    /// joining, but for those of the first list, which are waited on as the start is
    /// (04 §1). What became of each, but those of the first list.
    fn rescope(
        &mut self,
        matching: &BTreeSet<String>,
        start: &mut Start<'_>,
    ) -> Vec<(String, Membership)> {
        let parts: Vec<Scope> = matching
            .iter()
            .map(|namespace| Scope::Namespace(namespace.clone()))
            .collect();
        if self.scoped && parts == self.parts {
            return Vec::new();
        }
        let mut became = Vec::new();
        for part in &self.parts {
            if let Scope::Namespace(namespace) = part
                && !matching.contains(namespace)
            {
                self.joining.remove(namespace);
                became.push((namespace.clone(), Membership::Left));
            }
        }
        if self.scoped {
            for part in &parts {
                if let Scope::Namespace(namespace) = part
                    && !self.parts.contains(part)
                {
                    self.joining.insert(namespace.clone());
                    became.push((namespace.clone(), Membership::Joining));
                }
            }
        }
        start.joining = self.joining.clone();
        rescope(&mut self.ingresses, &parts, start);
        rescope(&mut self.services, &parts, start);
        let secrets = with_gateway_namespace(&parts, self.gateway_namespace.as_ref());
        rescope(&mut self.secrets, &secrets, start);
        if let Some(gateway_api) = &mut self.gateway_api {
            gateway_api.rescope(&parts, start);
        }
        self.parts = parts;
        self.scoped = true;
        self.watched = Some(self.joined());
        became
    }

    /// Each namespace joining whose every follow has been listed joins: its objects are
    /// translated from now on. Those that joined.
    fn join(&mut self) -> Vec<String> {
        if self.joining.is_empty() {
            return Vec::new();
        }
        let mut held = BTreeSet::new();
        for follow in self.namespace_parts() {
            if let Scope::Namespace(namespace) = follow.part()
                && follow.joining()
                && !follow.listed()
            {
                held.insert(namespace.clone());
            }
        }
        let joined: Vec<String> = self.joining.difference(&held).cloned().collect();
        for follow in self.namespace_parts() {
            if let Scope::Namespace(namespace) = follow.part()
                && joined.contains(namespace)
            {
                follow.join();
            }
        }
        self.joining = held;
        if !joined.is_empty() {
            self.watched = Some(self.joined());
        }
        joined
    }

    /// The namespaces of a selector followed that have joined.
    fn joined(&self) -> Vec<String> {
        self.parts
            .iter()
            .filter_map(|part| match part {
                Scope::Namespace(namespace) if !self.joining.contains(namespace) => {
                    Some(namespace.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Every follow of a kind followed in the watch scope.
    fn namespace_parts(&mut self) -> Vec<&mut dyn Part> {
        let mut parts = Vec::new();
        parts_of(&mut self.ingresses, &mut parts);
        parts_of(&mut self.services, &mut parts);
        parts_of(&mut self.secrets, &mut parts);
        if let Some(gateway_api) = &mut self.gateway_api {
            gateway_api.parts(&mut parts);
        }
        parts
    }
}

/// Where ListenerSets and Secrets are followed: the watch scope, as routes are, and the
/// Gateway's own namespace, `gateway`, where the scope does not hold it, as a Gateway that
/// takes ListenerSets `from: Same` takes them there, and its listeners' certificateRefs of
/// its own namespace need no grant (04 §1).
fn with_gateway_namespace(scope: &[Scope], gateway: Option<&Scope>) -> Vec<Scope> {
    let mut followed = scope.to_vec();
    if let Some(gateway) = gateway
        && !scope
            .iter()
            .any(|part| *part == Scope::Cluster || part == gateway)
    {
        followed.push(gateway.clone());
    }
    followed
}

/// The namespaces `scope` watches, where it does not watch the whole cluster: those a
/// reference into another is said to be out of (04 §9).
fn watched(scope: &[Scope]) -> Option<Vec<String>> {
    scope
        .iter()
        .map(|part| match part {
            Scope::Namespace(namespace) => Some(namespace.clone()),
            Scope::Cluster | Scope::Named(_) | Scope::Labelled(_) => None,
        })
        .collect()
}

/// The data plane's own Service, where its settings name it: its ready pods are those
/// counted for `Programmed`.
fn own_service(plane: &Plane) -> Option<ServiceName> {
    let named = plane.data_plane.own_service.as_deref()?;
    ServiceName::parse(named).ok()
}

/// The objects of `follows`, as they last published them, but for those held out until
/// their namespace joins.
fn objects<K: Clone>(follows: &[Follow<K>]) -> Vec<K> {
    follows
        .iter()
        .filter(|follow| !follow.joining)
        .flat_map(|follow| follow.objects.borrow().objects.clone())
        .collect()
}

/// The changes waited on: when the first and the last came.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Waiting {
    since: Option<(Instant, Instant)>,
}

impl Waiting {
    /// A change, at `now`.
    fn changed(&mut self, now: Instant) {
        let first = self.since.map_or(now, |(first, _)| first);
        self.since = Some((first, now));
    }

    /// When the changes waited on are translated, unless another comes first: none while
    /// none is waited on.
    fn due(&self, debounce: Debounce) -> Option<Instant> {
        self.since
            .map(|(first, last)| (last + debounce.quiet).min(first + debounce.most))
    }
}

/// What was last said of the objects followed, so that nothing is said twice.
#[derive(Debug, Default)]
struct Translator {
    /// The follows last said to be waited on.
    unlisted: Option<Vec<String>>,
    /// The last translation said, handed on or refused.
    last: Option<Translation>,
}

/// What there is to say of the objects followed.
#[derive(Debug, PartialEq)]
enum Step<'a> {
    /// Nothing new.
    Nothing,
    /// Not everything is listed, and these are not: fewer than last said.
    Waiting(&'a [String]),
    /// A translation that compiles, and differs from the last.
    Translated(&'a Translation),
    /// A translation that differs from the last, which the data plane would refuse.
    Refused {
        translation: &'a Translation,
        problems: Vec<ConfigError>,
    },
}

impl Translator {
    /// Translates for `plane` the objects `cluster` gives, once `unlisted` is empty.
    fn step(
        &mut self,
        plane: &Plane,
        unlisted: Vec<String>,
        cluster: impl FnOnce() -> Cluster,
    ) -> Step<'_> {
        if !unlisted.is_empty() {
            if self.unlisted.as_ref() == Some(&unlisted) {
                return Step::Nothing;
            }
            return Step::Waiting(self.unlisted.insert(unlisted));
        }
        let translation = translate(plane, &cluster());
        if self.last.as_ref() == Some(&translation) {
            return Step::Nothing;
        }
        let refused = compile(&translation.config).err();
        let translation = self.last.insert(translation);
        match refused {
            None => Step::Translated(translation),
            Some(problems) => Step::Refused {
                translation,
                problems,
            },
        }
    }
}

#[cfg(test)]
mod tests;
