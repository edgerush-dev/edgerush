//! EdgeRush's control plane ([04](../../../docs/04-control-plane.md)): the objects its data
//! planes' configs are made of, followed once for them all in the scope the control plane
//! watches, translated for each data plane on its own once every kind it needs has been
//! listed whole, and each config that compiles and differs from that data plane's last
//! handed on. Where a data plane is a Gateway, the Gateway API's kinds are followed too,
//! each while its CRD serves the version read
//! ([`gateway_api`](edgerush_kubernetes::gateway_api)).
//!
//! The changes of a moment make one translation: it waits until no change has come for
//! [`Debounce::quiet`], and never more than [`Debounce::most`] after the first it waits on
//! (04 §2). Each translation is whole, of everything followed, and pure
//! ([`edgerush_translate`]); the follows and the waiting are all that is not. It writes the
//! status of each data plane's GatewayClass, its Gateway and each route of that Gateway
//! (HTTPRoute, GRPCRoute or TLSRoute) as the config handed on serves them, merged with what each says now as the spec has it;
//! where it writes the data plane's Service, each Ingress's likewise (04 §6), [`status`],
//! the addresses that Service gives in both. That Service it writes as the config handed
//! on binds, [`service`] (05 §2). The admission
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
use edgerush_planes::objects::{
    ProxyService, SERVICE_MANAGED_BY, Transports, control_plane_labels,
};
use edgerush_planes::rate_limit_policy::RateLimitPolicy;
use edgerush_translate::{
    AcceptedPolicy, Cluster, GATEWAY_CONTROLLER, GatewayStatus, LimitPolicyStatus, Object, Plane,
    PolicyStatus, Problem, RouteStatus, Translation, translate_from,
};
use gateway_status::Programmed;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::IngressLoadBalancerIngress;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::Instant;

pub mod acks;
pub mod across;
pub mod admission;
pub mod data_plane;
pub mod expiry;
mod gateway_status;
pub mod issuing;
pub mod own_pods;
pub mod service;
pub mod status;

use edgerush_kubernetes::gateway_api::{GatewayKind, Served};

/// What a control plane serves of one data plane: the settings its config is translated
/// for, and, where its DataPlane states them, the data plane's Service it writes (05 §2),
/// whose addresses its statuses say. The development harness's settings have no Service,
/// and its statuses say no address.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// What translation is given.
    pub plane: Plane,
    /// The data plane's Service, but for its ports.
    pub service: Option<ProxyService>,
    /// Whether its DataPlane is one the operator generated for its Gateway (05 §3): a
    /// Gateway of that one that sets `infrastructure.parametersRef` is translated no
    /// further while it does, and its status left to the operator.
    pub generated: bool,
}

/// The data planes a control plane serves, each by its DataPlane as it is said,
/// `DataPlane <namespace>/<name>`; the development harness's one by no name.
pub type Planes = BTreeMap<String, Settings>;

/// What a ControlPlane's control plane takes Ingresses' addresses away by (04 §6): its
/// IngressClasses are `<controlplane>.<dataplane>` (05 §3), and an Ingress of one that no
/// DataPlane may still serve holds none.
#[derive(Debug, Clone)]
pub struct Classes {
    /// The ControlPlane's name.
    pub control_plane: String,
    /// Its namespace, the pod's own: with its name, what the GatewayClass the operator makes
    /// for it is labelled with, the one class its Gateways are translated of (05 §3).
    pub namespace: String,
    /// The classes its DataPlanes may still serve, as [`data_plane::kept`] has them, beside
    /// those of the data planes served; `None` until the DataPlanes are listed.
    pub kept: watch::Receiver<Option<BTreeSet<String>>>,
}

/// `cluster` as the control plane of `classes`, a ControlPlane's, translates it: its
/// Gateways only of the class the operator made for it, by that class's labels, so that one
/// of its name made by hand is not taken, as the operator refuses it (05 §3). Without
/// `classes`, as for the development harness, as it is.
fn of_control_plane(cluster: Cluster, classes: Option<&Classes>) -> Cluster {
    Cluster {
        gateway_class_labels: classes
            .map(|classes| control_plane_labels(&classes.namespace, &classes.control_plane)),
        ..cluster
    }
}

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
    /// A translation that compiles and differs from the last: the config of the data plane
    /// whose certificate carries `id`, or of any the stream serves where its settings state
    /// none, as the stream offers it to that data plane alone (05 §3).
    Translated {
        /// The data plane, as [`Planes`] names it.
        data_plane: &'a str,
        /// The translation.
        translation: &'a Translation,
        /// The data plane's ID.
        id: Option<&'a str>,
    },
    /// A translation the data plane would refuse, which is not handed on: the last one
    /// stays. Each object is compiled alone as it is translated, and left out if it is
    /// refused (04 §2), so only the data plane's own settings come to this.
    Refused {
        /// The data plane, as [`Planes`] names it.
        data_plane: &'a str,
        /// The translation.
        translation: &'a Translation,
        /// Why the data plane would refuse it.
        problems: &'a [ConfigError],
    },
    /// A generation of a data plane's DataPlane held back whole, the generation before
    /// served until it may be taken on (04 §4). Said once for each reason a generation is
    /// held back.
    HeldBack {
        /// The data plane, as [`Planes`] names it.
        data_plane: &'a str,
        /// Why.
        why: &'a HeldBack,
    },
    /// The status of a Gateway API object written, as the config handed on serves it; or
    /// why it could not be, to be written again at the next change.
    GatewayApiStatus {
        /// The object.
        object: &'a Object,
        /// Whether it says it now.
        written: Result<(), &'a ApiError>,
    },
    /// The data plane's Service, `<namespace>/<name>`, and what became of it at a config
    /// handed on, or at a change to it.
    Service {
        /// The Service.
        service: &'a str,
        /// What became of it.
        now: ServiceNow<'a>,
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

/// What became of the data plane's Service.
#[derive(Debug, Clone, Copy)]
pub enum ServiceNow<'a> {
    /// Written, listing these ports, `80/TCP`, `443/UDP`.
    Applied(&'a [String]),
    /// The API server refused it, and it is written again at the next change; what it was
    /// stays.
    Refused(&'a ApiError),
    /// One of its name is there that its DataPlane does not own: left alone, and no address
    /// taken from it. Said once, until that changes.
    NotOurs,
    /// The config binds no port and the DataPlane serves no Ingress: Kubernetes allows no
    /// Service without a port, so the last stays. Said once, until that changes.
    NoPort,
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

/// Follows, through `api`, the kinds the configs of the data planes `planes` holds are made
/// of in `scope`, IngressClasses in the cluster, and translates them for each data plane on
/// its own each time they change, once every one it needs has been listed whole; says how
/// it goes to `said`. Where a data plane names a Gateway, the Gateway API's kinds are
/// followed too, GatewayClasses in the cluster and Gateways in the namespaces of the
/// Gateways named alone, each from when its CRD is listed and serves the version read until
/// it no longer does, and Namespaces in the cluster. Where a data plane's settings hold its
/// Service, it writes that Service as each config handed on for it binds, follows it, and
/// writes each Ingress's status to say the addresses it gives as that config serves the
/// Ingress, where it says otherwise. It writes that of each data plane's GatewayClass, its
/// Gateway (those addresses among it, or none without that Service) and each route that
/// names or named that Gateway likewise, always. With `leading`, only while it says this
/// replica leads, every status that says otherwise written as it comes to (02 §3).
/// The data planes are what `planes` holds, as it changes: none, and nothing is translated,
/// nor, without `classes`, as the development harness has none, followed, until it holds
/// some. With `classes`, a ControlPlane's, what is followed is followed whether or not it
/// serves any, and each Ingress of a class of its own that none may still serve has its
/// address taken away (04 §6). One added, edited or let go of leaves the others as they
/// were: what is followed for all of them is followed on, and only what that one alone
/// needed started or stopped; an edit is translated and written again as any change is,
/// and one of another ID begins its data plane's translation afresh
/// ([05 §3](../../../docs/05-operator.md)).
/// Runs for as long as it is polled, its follows ended with it. For a thread and runtime of
/// its own, in a `LocalSet`.
#[allow(clippy::too_many_arguments)]
pub async fn control(
    api: ApiServer,
    scope: &WatchScope,
    mut planes: watch::Receiver<Planes>,
    classes: Option<Classes>,
    debounce: Debounce,
    leading: Option<watch::Receiver<bool>>,
    acks: Acks,
    said: impl Fn(Said<'_>) + 'static,
) {
    let api = Rc::new(api);
    let said: Rc<dyn Fn(Said<'_>)> = Rc::new(said);
    loop {
        if planes.borrow_and_update().is_empty() && classes.is_none() {
            // Settings that will never come are nothing to serve.
            if planes.changed().await.is_err() {
                return;
            }
            continue;
        }
        serve(
            &api,
            scope,
            &mut planes,
            classes.as_ref(),
            debounce,
            leading.as_ref(),
            &acks,
            &said,
        )
        .await;
    }
}

/// What the data planes served shape of what is followed for them all: the namespaces of
/// their Gateways, of the Services written, and of their own Services, whose ready pods
/// are counted, each once and in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Shape {
    gateways: Vec<Scope>,
    services: Vec<Scope>,
    own_services: Vec<Scope>,
}

impl Shape {
    fn of(planes: &Planes) -> Self {
        let namespaces = |of: &dyn Fn(&Settings) -> Option<String>| -> Vec<Scope> {
            planes
                .values()
                .filter_map(of)
                .collect::<BTreeSet<String>>()
                .into_iter()
                .map(Scope::Namespace)
                .collect()
        };
        Self {
            gateways: namespaces(&|settings| {
                settings
                    .plane
                    .gateway
                    .as_ref()
                    .map(|gateway| gateway.namespace.clone())
            }),
            services: namespaces(&|settings| {
                settings
                    .service
                    .as_ref()
                    .map(|service| service.namespace().to_owned())
            }),
            own_services: namespaces(&|settings| {
                own_service(&settings.plane).map(|service| service.namespace)
            }),
        }
    }
}

/// What is served of one data plane: its settings, and what was last translated, handed
/// on and written for it, each apart from every other data plane's.
struct Serving {
    settings: Settings,
    /// Its DataPlane as it now is, where that generation adds limits to its `global` and is
    /// not taken on yet: each translation judges whether it may be, and `settings` stay
    /// until it is (04 §4).
    wanted: Option<Settings>,
    /// Why `wanted` was last said to be held back, so that each reason is said once.
    said_held: Option<HeldBack>,
    translator: Translator,
    /// What the config handed on serves: the Ingresses claimed, and those served.
    handed: Option<(Vec<Object>, Vec<Object>)>,
    /// What the config handed on says the Gateway API's objects' status is to say.
    handed_api: Option<Handed>,
    /// What the config handed on says each RateLimitPolicy's status is to say of the
    /// ancestors it reaches here.
    handed_limits: Option<Vec<LimitPolicyStatus>>,
    /// The ports the config handed on binds.
    handed_ports: Option<BTreeMap<u16, Transports>>,
    /// The generation of the Gateway handed on, and when it was first.
    handed_since: Option<(i64, Instant)>,
    /// Its Service as this replica last wrote it while it leads, and what was last said of
    /// it that is said once.
    written: Written,
}

/// Why a generation of a DataPlane is held back whole, the one before served (04 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldBack {
    /// A limit of its `global` has a name a RateLimitPolicy holds in the config last handed
    /// on.
    NameHeld {
        /// The limit's name.
        limit: String,
        /// The policy holding it.
        by: Object,
    },
    /// Its `global` would put a rule over the most limits a rule takes where the generation
    /// before does not: a policy's change refused for it, or the config refused whole.
    TooManyLimits,
}

impl fmt::Display for HeldBack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameHeld { limit, by } => {
                write!(
                    f,
                    "its global limit `{limit}` has the name of {by}'s, which holds it"
                )
            }
            Self::TooManyLimits => write!(
                f,
                "its global limits would put a rule over the {} limits a rule takes",
                edgerush_config::MOST_LIMITS
            ),
        }
    }
}

impl Serving {
    fn new(settings: Settings) -> Self {
        Self {
            settings,
            wanted: None,
            said_held: None,
            translator: Translator::default(),
            handed: None,
            handed_api: None,
            handed_limits: None,
            handed_ports: None,
            handed_since: None,
            written: Written::default(),
        }
    }

    /// Takes on `settings`, its DataPlane as it now is, unless they add a limit to its
    /// `global`: then they are judged as the objects are translated ([`Self::judge`]), and
    /// the settings served stay until they are taken on.
    fn offer(&mut self, settings: &Settings) {
        if !adds_limits(&self.settings.plane, &settings.plane) {
            self.settings.clone_from(settings);
            self.wanted = None;
            self.said_held = None;
        } else if self.wanted.as_ref() != Some(settings) {
            self.wanted = Some(settings.clone());
            self.said_held = None;
        }
    }

    /// Why `wanted` is held back, translated from `cluster` beside the settings served; none
    /// where it may be taken on: its `global` takes no name a policy holds, and puts no rule
    /// over the most limits that the settings served do not, whether a policy's change is
    /// refused for it or the config refused whole. `cluster` is what is followed for the
    /// settings served: a generation that would have more followed, a Gateway's kinds say,
    /// is judged without them, and a rule they bring over once it is taken on is as a
    /// route's change.
    fn judge(&self, wanted: &Settings, cluster: &Cluster) -> Option<HeldBack> {
        if let Some((limit, by)) = self.translator.held_by_policy(&wanted.plane) {
            return Some(HeldBack::NameHeld { limit, by });
        }
        // The changes refused for the most limits, and whether a rule is over all the same.
        let over = |plane: &Plane| -> (BTreeSet<Object>, bool) {
            let translation = translate_from(plane, cluster, &self.translator.accepted);
            let still = translation
                .config
                .routes
                .iter()
                .flat_map(|route| &route.rules)
                .any(|rule| rule.rate_limits.len() > edgerush_config::MOST_LIMITS);
            let refused = translation
                .report
                .into_iter()
                .filter(|finding| matches!(finding.problem, Problem::LimitsTooMany { .. }))
                .map(|finding| finding.object)
                .collect();
            (refused, still)
        };
        let (refused_before, over_before) = over(&self.settings.plane);
        let (refused, over) = over(&wanted.plane);
        (!refused.is_subset(&refused_before) || (over && !over_before))
            .then_some(HeldBack::TooManyLimits)
    }
}

/// Whether `now`'s global limits have a name `before`'s do not: a generation that could put
/// a rule over the most limits, or take a name a policy holds.
fn adds_limits(before: &Plane, now: &Plane) -> bool {
    let names = |plane: &Plane| -> BTreeSet<String> {
        plane
            .rate_limiting
            .iter()
            .flat_map(|limiting| &limiting.global)
            .map(|limit| limit.name.clone())
            .collect()
    };
    !names(now).is_subset(&names(before))
}

/// The settings each data plane is served with: its DataPlane's, or where a generation of
/// it is not taken on yet, the one before.
fn served_planes(serving: &BTreeMap<String, Serving>) -> Planes {
    serving
        .iter()
        .map(|(name, served)| (name.clone(), served.settings.clone()))
        .collect()
}

/// Each data plane served, as `planes` now has them: one of the same ID as before taken
/// on with its new settings, or offered them where they add global limits, what was handed
/// for it kept to be bettered, and one of another ID, or new, begun afresh, so that what is
/// translated for one data plane is never handed to another.
fn served_now(serving: &mut BTreeMap<String, Serving>, planes: &Planes) {
    serving.retain(|name, served| {
        planes
            .get(name)
            .is_some_and(|settings| settings.plane.id == served.settings.plane.id)
    });
    for (name, settings) in planes {
        match serving.get_mut(name) {
            Some(served) => served.offer(settings),
            None => {
                serving.insert(name.clone(), Serving::new(settings.clone()));
            }
        }
    }
}

/// [`control`] for the data planes `planes` holds, until it holds none, or, with `classes`,
/// for good.
#[allow(clippy::too_many_arguments)]
async fn serve(
    api: &Rc<ApiServer>,
    scope: &WatchScope,
    planes: &mut watch::Receiver<Planes>,
    classes: Option<&Classes>,
    debounce: Debounce,
    leading: Option<&watch::Receiver<bool>>,
    acks: &Acks,
    said: &Rc<dyn Fn(Said<'_>)>,
) {
    let changed = Rc::new(Notify::new());
    let mut tasks = JoinSet::new();
    // The settings changing is a change, and so are the classes kept.
    {
        let (mut told, changed) = (planes.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                changed.notify_one();
            }
        });
    }
    if let Some(classes) = classes {
        let (mut told, changed) = (classes.kept.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                changed.notify_one();
            }
        });
    }
    let first = planes.borrow_and_update().clone();
    let shape = Shape::of(&first);
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
    let mut followed = Followed {
        ingress_classes: start.follows(&[Scope::Cluster]),
        ingresses: start.follows(&parts),
        services: start.follows(&parts),
        secrets: start.follows(&with_gateway_namespaces(&parts, &shape.gateways)),
        rate_limit_policies: start.follows(&with_gateway_namespaces(&parts, &shape.gateways)),
        own_services: start.follows(&shape.services),
        gateway_api: (classes.is_some() || !shape.gateways.is_empty())
            .then(|| GatewayApi::start(&mut start, &shape.gateways)),
        own_class: classes.is_some(),
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
        ready: start.follows(&shape.own_services),
        shape,
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
    let mut serving = BTreeMap::new();
    served_now(&mut serving, &first);
    let mut waiting = Waiting::default();
    // The follows last said to be waited on.
    let mut said_unlisted: Option<Vec<String>> = None;
    // A generation of a DataPlane taken on last round, after it was judged.
    let mut taken_on = false;
    loop {
        // New settings, or a generation taken on: what those served alone need followed
        // started, what none needs any more stopped, the rest followed on. A sender gone
        // leaves them as they were.
        let mut reshaped = std::mem::take(&mut taken_on);
        if planes.has_changed().unwrap_or(false) {
            let now = planes.borrow_and_update().clone();
            if now.is_empty() && classes.is_none() {
                return;
            }
            served_now(&mut serving, &now);
            reshaped = true;
        }
        if reshaped {
            let mut start = Start {
                tasks: &mut tasks,
                api,
                changed: &changed,
                said,
                joining: followed.joining.clone(),
            };
            followed.reshape(Shape::of(&served_planes(&serving)), &mut start);
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
        // Each data plane translated on its own, once what it needs is listed whole: the
        // Gateway API's kinds only where it is a Gateway. The objects are taken once for
        // them all.
        let (unlisted, unlisted_api) = followed.unlisted();
        let every = unlisted.iter().chain(&unlisted_api).cloned().collect();
        if let Some(waited) = wait_said(&mut said_unlisted, every) {
            said(Said::Waiting(waited));
        }
        let mut cluster = None;
        for (name, served) in &mut serving {
            let is_gateway = served.settings.plane.gateway.is_some();
            if !unlisted.is_empty() || (is_gateway && !unlisted_api.is_empty()) {
                continue;
            }
            // Its data plane keeps its last config, and the operator says why (05 §3).
            if held(followed.gateway_api.as_ref(), &served.settings) {
                continue;
            }
            let cluster =
                cluster.get_or_insert_with(|| of_control_plane(followed.cluster(), classes));
            // A generation that adds global limits, judged against the objects as they are:
            // taken on, and translated next round once what it needs is followed; or held
            // back, which is said, the settings served translated meanwhile (04 §4).
            if let Some(wanted) = served.wanted.clone() {
                match served.judge(&wanted, cluster) {
                    None => {
                        served.settings = wanted;
                        served.wanted = None;
                        served.said_held = None;
                        taken_on = true;
                        changed.notify_one();
                        continue;
                    }
                    Some(why) => {
                        if served.said_held.as_ref() != Some(&why) {
                            said(Said::HeldBack {
                                data_plane: name,
                                why: &why,
                            });
                            served.said_held = Some(why);
                        }
                    }
                }
            }
            match served.translator.step(&served.settings.plane, cluster) {
                Step::Nothing => {}
                Step::Translated(translation) => {
                    served.handed = Some((translation.claimed.clone(), translation.served.clone()));
                    served.handed_ports = Some(service::bound(&translation.config));
                    served.handed_api = Handed::of(translation);
                    served.handed_limits = Some(translation.limit_policy_statuses.clone());
                    if let Some(handed) = &served.handed_api {
                        let generation = handed.status.generation;
                        if served
                            .handed_since
                            .is_none_or(|(since, _)| since != generation)
                        {
                            served.handed_since = Some((generation, Instant::now()));
                        }
                    }
                    said(Said::Translated {
                        data_plane: name,
                        translation,
                        id: served.settings.plane.id.as_deref(),
                    });
                    // What its policies hold changed: a generation held back is judged
                    // again.
                    if served.wanted.is_some() {
                        changed.notify_one();
                    }
                }
                Step::Refused {
                    translation,
                    problems,
                } => said(Said::Refused {
                    data_plane: name,
                    translation,
                    problems: &problems,
                }),
            }
        }
        // Every round, so that a status changed by another, or written in vain, is written
        // again; one at a time, which is all the pace there is. A status written is a change
        // to its object, and the round it brings finds nothing more to write. Each data
        // plane's addresses, where it writes its Service, once that Service's namespace is
        // listed, from that Service if it is its DataPlane's.
        let leads = leading.is_none_or(|leads| *leads.borrow());
        for served in serving.values_mut() {
            let is_held = held(followed.gateway_api.as_ref(), &served.settings);
            write_statuses(api, &followed, served, acks, leads, is_held, &**said).await;
        }
        // Each Ingress of a class of the ControlPlane's that no data plane serves and none
        // is kept for, once the DataPlanes are listed (04 §6). Taken out of the watch first,
        // as nothing is held of it across the writes.
        let kept = classes.and_then(|classes| classes.kept.borrow().clone());
        if let (Some(classes), Some(mut kept), true) = (classes, kept, leads) {
            kept.extend(
                serving
                    .values()
                    .filter_map(|served| served.settings.plane.ingress_class.clone()),
            );
            let ingresses = followed.ingresses();
            for change in status::released(&classes.control_plane, &kept, &ingresses) {
                write_ingress_status(api, &change, &**said).await;
            }
        }
        // Those the data planes share, once for them all (04 §6).
        if let (Some(gateway_api), true, true) =
            (&followed.gateway_api, followed.own_listed(), leads)
        {
            let handed: Vec<&Handed> = serving
                .values()
                .filter_map(|served| served.handed_api.as_ref())
                .collect();
            let own = classes
                .map(|classes| control_plane_labels(&classes.namespace, &classes.control_plane));
            gateway_api
                .write_shared(api, &handed, own.as_ref(), &**said)
                .await;
        }
        // Each RateLimitPolicy's, once for every data plane, whether or not the Gateway API
        // is followed: an Ingress is an ancestor too (04 §6).
        if leads {
            write_limit_policies(api, &followed, &serving, &**said).await;
        }
        // Until a change comes, or the bound a data plane's pods are waited on to run its
        // Gateway's generation, and then until its moment is over.
        let now = Instant::now();
        let bound = serving
            .values()
            .filter_map(|served| served.handed_since)
            .map(|(_, since)| since + PROGRAMMED_WITHIN)
            .filter(|bound| *bound > now)
            .min();
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

/// Whether the Gateway of a data plane served as `settings` say is held as it was: one of a
/// DataPlane generated for it that sets `infrastructure.parametersRef`, which names no kind
/// EdgeRush reads. While it does, it is translated no further, its data plane keeping its
/// last config, and its status is the operator's (05 §3).
fn held(gateway_api: Option<&GatewayApi>, settings: &Settings) -> bool {
    let (true, Some(gateway_api), Some(name)) =
        (settings.generated, gateway_api, &settings.plane.gateway)
    else {
        return false;
    };
    gateway_api.gateway(name).is_some_and(|gateway| {
        gateway
            .spec
            .infrastructure
            .as_ref()
            .is_some_and(|infrastructure| infrastructure.parameters_ref.is_some())
    })
}

/// Writes, as far as `leads`, each status of `served`'s data plane alone that says other
/// than what was handed on for it: its Service, its Gateway's and the ListenerSets naming
/// it, and its Ingresses'. Those it shares with other data planes are written once for them
/// all ([`GatewayApi::write_shared`]). Its Gateway's and its ListenerSets' are left alone
/// while it is `held`.
async fn write_statuses(
    api: &ApiServer,
    followed: &Followed,
    served: &mut Serving,
    acks: &Acks,
    leads: bool,
    held: bool,
    said: &dyn Fn(Said<'_>),
) {
    let own = served.settings.service.as_ref();
    let live = own.and_then(|own| followed.own_service(own));
    if let (Some(own), Some(ports), true, true) =
        (own, &served.handed_ports, followed.own_listed(), leads)
    {
        served
            .written
            .write(api, own, live.as_ref(), ports, said)
            .await;
    } else if !leads {
        // A leader before it may have written it otherwise: looked at afresh as it comes
        // to lead.
        served.written = Written::default();
    }
    let addresses = match own {
        _ if !followed.own_listed() => None,
        None => Some(Vec::new()),
        Some(own) => {
            let ours = live
                .as_ref()
                .filter(|live| service::ours(live, own.owner_uid()));
            Some(
                service::addresses(ours)
                    .iter()
                    .map(|address| status::address(address))
                    .collect(),
            )
        }
    };
    // The Gateway API's, always, as the spec has every controller write them (04 §6), but
    // for a Gateway held, whose status the operator writes.
    if let (Some(gateway_api), Some(handed), Some(addresses), true, false) = (
        &followed.gateway_api,
        &served.handed_api,
        &addresses,
        leads,
        held,
    ) {
        let addresses = gateway_status::addresses(addresses);
        let pods = followed.pods(acks);
        let ready = own_service(&served.settings.plane).map(|service| followed.ready(&service));
        let overdue = served
            .handed_since
            .is_some_and(|(_, since)| since.elapsed() >= PROGRAMMED_WITHIN);
        let programmed = Programmed::of(handed.status.generation, &pods, ready.as_deref(), overdue);
        gateway_api
            .write_own(api, handed, &addresses, &programmed, said)
            .await;
    }
    // An Ingress's only where the control plane writes the Service its addresses are.
    if let (Some(_), Some((claimed, served)), Some(addresses), true) =
        (own, &served.handed, &addresses, leads)
    {
        let ingresses = followed.ingresses();
        for change in status::changes(claimed, served, &ingresses, addresses) {
            write_ingress_status(api, &change, said).await;
        }
    }
}

/// Writes, one at a time and once for every data plane `serving`, each RateLimitPolicy's
/// status that says other than they have it (04 §6): an ancestor of ours for each data
/// plane's Gateway and each Ingress of its class that a policy reaches, as its translation
/// handed on says, and none for one it no longer reaches. This controller's entries of a
/// Gateway gone, or of an Ingress of the policy's namespace that is not there, are taken
/// away; every other kept whole.
async fn write_limit_policies(
    api: &ApiServer,
    followed: &Followed,
    serving: &BTreeMap<String, Serving>,
    said: &dyn Fn(Said<'_>),
) {
    let handed: Vec<&Serving> = serving
        .values()
        .filter(|served| served.handed_limits.is_some())
        .collect();
    if handed.is_empty() {
        return;
    }
    // The ancestors the data planes write of: each one's Gateway, and each Ingress of its
    // class; and what each policy's status is to say of those it reaches.
    let mut of_ours: BTreeSet<Object> = BTreeSet::new();
    let mut reached: BTreeMap<&Object, Vec<(&Object, gateway_status::OfAncestor<'_>)>> =
        BTreeMap::new();
    for served in &handed {
        of_ours.extend(
            served
                .handed_api
                .as_ref()
                .map(|handed| handed.gateway.clone()),
        );
        if let Some((claimed, _)) = &served.handed {
            of_ours.extend(claimed.iter().cloned());
        }
        for status in served.handed_limits.iter().flatten() {
            let of = gateway_status::OfAncestor {
                generation: status.generation,
                conditions: &status.conditions,
            };
            reached
                .entry(&status.policy)
                .or_default()
                .extend(status.ancestors.iter().map(|ancestor| (ancestor, of)));
        }
    }
    let (followed_gateways, gateways_there) = followed
        .gateway_api
        .as_ref()
        .map(GatewayApi::gateways_known)
        .unwrap_or_default();
    let gateways = gateway_status::Gateways {
        namespaces: &followed_gateways,
        there: &gateways_there,
    };
    let ingresses_there: BTreeSet<Object> = followed
        .ingresses()
        .iter()
        .map(|ingress| Object {
            kind: "Ingress",
            namespace: ingress.metadata.namespace.clone().unwrap_or_default(),
            name: ingress.metadata.name.clone().unwrap_or_default(),
        })
        .collect();
    let now = lease::wall_clock();
    for policy in objects(&followed.rate_limit_policies) {
        let object = Object {
            kind: "RateLimitPolicy",
            namespace: policy.metadata.namespace.clone().unwrap_or_default(),
            name: policy.metadata.name.clone().unwrap_or_default(),
        };
        // Those it reaches, and each other of the data planes' its status holds an entry of
        // ours for, which is to be taken away.
        let mut wanted: Vec<(Object, Option<gateway_status::OfAncestor<'_>>)> = reached
            .get(&object)
            .into_iter()
            .flatten()
            .map(|(ancestor, of)| ((*ancestor).clone(), Some(*of)))
            .collect();
        for ancestor in policy.status.iter().flat_map(|status| &status.ancestors) {
            let to = &ancestor.ancestor_ref;
            let kind = match to.kind.as_deref() {
                None | Some("Gateway") => "Gateway",
                Some("Ingress") => "Ingress",
                Some(_) => continue,
            };
            let named = Object {
                kind,
                namespace: to
                    .namespace
                    .clone()
                    .unwrap_or_else(|| object.namespace.clone()),
                name: to.name.clone(),
            };
            if ancestor.controller_name == GATEWAY_CONTROLLER
                && of_ours.contains(&named)
                && !wanted.iter().any(|(known, _)| *known == named)
            {
                wanted.push((named, None));
            }
        }
        let wanted: Vec<(&Object, Option<gateway_status::OfAncestor<'_>>)> = wanted
            .iter()
            .map(|(ancestor, of)| (ancestor, *of))
            .collect();
        let live = policy.status.as_ref().map(gateway_status::limits_as_tls);
        let status = gateway_status::policy_status(
            live.as_ref(),
            &object.namespace,
            &wanted,
            &gateway_status::LIMIT_ANCESTOR_TYPES,
            (gateways, Some(&ingresses_there)),
            now,
        );
        if live.unwrap_or_default() != status {
            let version = policy
                .metadata
                .resource_version
                .as_deref()
                .unwrap_or_default();
            let result = api
                .replace_status::<RateLimitPolicy>(
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

/// Writes an Ingress's status to say what `change` has it say, and says so.
async fn write_ingress_status(api: &ApiServer, change: &status::Change, said: &dyn Fn(Said<'_>)) {
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
pub(crate) struct Tasks(pub(crate) Vec<AbortHandle>);

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
    /// every cluster has, so followed whatever the CRDs say, while a data plane is a
    /// Gateway.
    namespaces: Vec<Follow<Namespace>>,
    /// The namespaces of the data planes' Gateways: those Gateways are followed in, as no
    /// other Gateway is read (04 §2).
    gateway_namespaces: Vec<Scope>,
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
    fn start(start: &mut Start<'_>, gateways: &[Scope]) -> Self {
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
            namespaces: if gateways.is_empty() {
                Vec::new()
            } else {
                start.follows(&[Scope::Cluster])
            },
            gateway_namespaces: gateways.to_vec(),
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
    /// cluster, Gateways in the namespaces of the data planes' Gateways; ListenerSets, HTTPRoutes,
    /// GRPCRoutes, TLSRoutes, TCPRoutes, UDPRoutes, ReferenceGrants and BackendTLSPolicies
    /// in `scope`, with the ConfigMaps there while BackendTLSPolicies are), and stops
    /// following each it says is not; while no data plane is a Gateway, GatewayClasses
    /// alone.
    fn follow(&mut self, served: &[(GatewayKind, Served)], start: &mut Start<'_>, scope: &[Scope]) {
        let gateways = !self.gateway_namespaces.is_empty();
        for (kind, served) in served {
            let on = served.is_served() && (gateways || *kind == GatewayKind::GatewayClass);
            match kind {
                GatewayKind::GatewayClass => {
                    keep(&mut self.gateway_classes, on, || {
                        start.follows(&[Scope::Cluster])
                    });
                }
                GatewayKind::Gateway => {
                    let namespaces = &self.gateway_namespaces;
                    keep(&mut self.gateways, on, || start.follows(namespaces));
                }
                GatewayKind::ListenerSet => {
                    let scope = with_gateway_namespaces(scope, &self.gateway_namespaces);
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

    /// The follows made to follow the Gateways of `gateways`, the namespaces of the data
    /// planes' Gateways now, Gateways and ListenerSets there, in the watch scope `parts`;
    /// the kinds but GatewayClasses started as the first data plane is a Gateway, and
    /// stopped as the last is no longer.
    fn reshape(&mut self, gateways: &[Scope], parts: &[Scope], start: &mut Start<'_>) {
        if let Some(follows) = &mut self.gateways {
            rescope(follows, gateways, start);
        }
        if let Some(follows) = &mut self.listener_sets {
            rescope(follows, &with_gateway_namespaces(parts, gateways), start);
        }
        let (were, are) = (!self.gateway_namespaces.is_empty(), !gateways.is_empty());
        self.gateway_namespaces = gateways.to_vec();
        if were != are {
            self.namespaces = if are {
                start.follows(&[Scope::Cluster])
            } else {
                Vec::new()
            };
            if let Some(served) = self.last.clone() {
                self.follow(&served, start, parts);
            }
        }
    }

    /// The follows of each kind followed in the watch scope made to follow `parts`, and
    /// ListenerSets the Gateways' namespaces beside them.
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
        let listener_sets = with_gateway_namespaces(parts, &self.gateway_namespaces);
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
    /// The Gateway `name`, as followed, if it is there.
    fn gateway(&self, name: &edgerush_translate::GatewayName) -> Option<Gateway> {
        self.gateways
            .as_deref()
            .map(objects)
            .unwrap_or_default()
            .into_iter()
            .find(|gateway| {
                gateway.metadata.namespace.as_deref() == Some(name.namespace.as_str())
                    && gateway.metadata.name.as_deref() == Some(name.name.as_str())
            })
    }

    /// The Gateway `handed` is of, as followed, if it is there.
    fn gateway_of(&self, handed: &Handed) -> Option<Gateway> {
        self.gateways
            .as_deref()
            .map(objects)
            .unwrap_or_default()
            .into_iter()
            .find(|gateway| {
                gateway.metadata.namespace.as_deref() == Some(handed.gateway.namespace.as_str())
                    && gateway.metadata.name.as_deref() == Some(handed.gateway.name.as_str())
            })
    }

    /// The namespaces it follows Gateways in, and the Gateways there: what tells a Gateway
    /// gone (04 §6).
    fn gateways_known(&self) -> (Vec<String>, Vec<Object>) {
        let there = self
            .gateways
            .as_deref()
            .map(objects)
            .unwrap_or_default()
            .iter()
            .map(|gateway| Object {
                kind: "Gateway",
                namespace: gateway.metadata.namespace.clone().unwrap_or_default(),
                name: gateway.metadata.name.clone().unwrap_or_default(),
            })
            .collect();
        let followed = self
            .gateway_namespaces
            .iter()
            .filter_map(|scope| match scope {
                Scope::Namespace(namespace) => Some(namespace.clone()),
                _ => None,
            })
            .collect();
        (followed, there)
    }

    /// Writes, one at a time, each status of a data plane's own that says other than
    /// `handed` has it: that of its Gateway, `addresses` among it, and of each ListenerSet
    /// naming it. Nothing while the Gateway is not there.
    async fn write_own(
        &self,
        api: &ApiServer,
        handed: &Handed,
        addresses: &[edgerush_gateway_api::gateway::GatewayStatusAddresses],
        programmed: &Programmed,
        said: &dyn Fn(Said<'_>),
    ) {
        let now = lease::wall_clock();
        let Some(gateway) = self.gateway_of(handed) else {
            return;
        };
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
    }

    /// Writes, one at a time and once for every data plane served, each status shared
    /// between them that says other than they have it, from what was handed on for each,
    /// `handed` (04 §6): that of each class of their Gateways; of each route followed,
    /// HTTPRoute, GRPCRoute, TLSRoute, TCPRoute or UDPRoute, its parents of every data
    /// plane's Gateway and the ListenerSets naming it at once, whether or not it still names
    /// one; and of each BackendTLSPolicy followed, an ancestor for each Gateway, whether or
    /// not a route of it reaches the policy. This controller's entries of a Gateway gone are
    /// taken away. Only Gateways that are there are written of. And, of a ControlPlane's
    /// control plane, `own` the labels the operator makes its class with, that class's
    /// whether or not a Gateway of it is served: accepted before any is written, as Gateway
    /// API tooling waits for, and saying nothing of a data plane.
    async fn write_shared(
        &self,
        api: &ApiServer,
        handed: &[&Handed],
        own: Option<&BTreeMap<String, String>>,
        said: &dyn Fn(Said<'_>),
    ) {
        let now = lease::wall_clock();
        let there: Vec<(&Handed, Gateway)> = handed
            .iter()
            .filter_map(|handed| Some((*handed, self.gateway_of(handed)?)))
            .collect();
        // Each class once, whichever Gateways name it, and the ControlPlane's own.
        let classes = self
            .gateway_classes
            .as_deref()
            .map(objects)
            .unwrap_or_default();
        let mut named: BTreeSet<&str> = there
            .iter()
            .map(|(_, gateway)| gateway.spec.gateway_class_name.as_str())
            .collect();
        if let Some(own) = own {
            named.extend(
                classes
                    .iter()
                    .filter(|class| class.spec.controller_name == GATEWAY_CONTROLLER)
                    .filter(|class| {
                        let held = class.metadata.labels.as_ref();
                        own.iter()
                            .all(|(key, value)| held.and_then(|held| held.get(key)) == Some(value))
                    })
                    .filter_map(|class| class.metadata.name.as_deref()),
            );
        }
        for name in named {
            let class = classes
                .iter()
                .find(|class| class.metadata.name.as_deref() == Some(name));
            if let (Some(class), Some(served)) = (class, &self.last) {
                let generation = class.metadata.generation.unwrap_or_default();
                let status = gateway_status::class(
                    class.status.as_ref(),
                    generation,
                    served,
                    gateway_status::SUPPORTED_FEATURES,
                    now,
                );
                if class.status.as_ref() != Some(&status) {
                    let object = Object {
                        kind: "GatewayClass",
                        namespace: String::new(),
                        name: name.to_owned(),
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
        }
        if there.is_empty() {
            return;
        }
        let (followed, gateways_there) = self.gateways_known();
        let gateways = gateway_status::Gateways {
            namespaces: &followed,
            there: &gateways_there,
        };
        // A route's parents of the data planes': their Gateways, and each ListenerSet
        // naming one, taken or not.
        let parents_of_ours: Vec<Object> = there
            .iter()
            .flat_map(|(handed, _)| {
                std::iter::once(handed.gateway.clone()).chain(
                    handed
                        .status
                        .listener_sets
                        .iter()
                        .map(|set| set.set.clone()),
                )
            })
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
            let wanted = wanted_of_every(there.iter().map(|(handed, _)| *handed), &object);
            let status = gateway_status::route(
                live.as_ref(),
                &object.namespace,
                &parents_of_ours,
                wanted.as_ref(),
                gateways,
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
            let wanted: Vec<(&Object, Option<&PolicyStatus>)> = there
                .iter()
                .map(|(handed, _)| {
                    let wanted = handed
                        .policies
                        .iter()
                        .find(|wanted| wanted.policy == object);
                    (&handed.gateway, wanted)
                })
                .collect();
            let status = gateway_status::policy(
                policy.status.as_ref(),
                &object.namespace,
                &wanted,
                gateways,
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

/// What the status of `route` is to say of every data plane's parents, as each of `handed`
/// has it: their parents one after another, of the one generation the route is of. None
/// where no data plane's says anything of it.
fn wanted_of_every<'a>(
    handed: impl Iterator<Item = &'a Handed>,
    route: &Object,
) -> Option<RouteStatus> {
    let mut merged: Option<RouteStatus> = None;
    for wanted in
        handed.filter_map(|handed| handed.routes.iter().find(|wanted| wanted.route == *route))
    {
        match &mut merged {
            Some(merged) => merged.parents.extend(wanted.parents.iter().cloned()),
            None => merged = Some(wanted.clone()),
        }
    }
    merged
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
    /// RateLimitPolicies, where Secrets are: a policy is in its targets' namespace, a
    /// Gateway's or a ListenerSet's as well as a route's or an Ingress's (04 §1).
    rate_limit_policies: Vec<Follow<RateLimitPolicy>>,
    /// What the data planes shape of what is followed: the namespaces of their Gateways,
    /// whose Secrets are followed whatever the scope, of the Services written and of their
    /// own Services.
    shape: Shape,
    /// The namespaces of the data planes' Services, where the control plane writes them,
    /// followed for what they are and the addresses they give.
    own_services: Vec<Follow<Service>>,
    /// The Gateway API's kinds, where a data plane is a Gateway, and the CRDs and
    /// GatewayClasses alone where none is but the control plane has a class of its own.
    gateway_api: Option<GatewayApi>,
    /// Whether the control plane has a class of its own, a ControlPlane's: written whether
    /// or not a data plane is a Gateway (04 §6).
    own_class: bool,
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
    /// The namespaces of the data planes' own Services, for their ready pods.
    ready: Vec<Follow<EndpointSlice>>,
}

impl Followed {
    /// The follows translation waits on that have not been listed whole, by name: those
    /// every data plane's waits on, and those of the Gateway API's kinds, which only a
    /// Gateway's waits on.
    fn unlisted(&self) -> (Vec<String>, Vec<String>) {
        (
            unlisted(self.selected.as_slice())
                .chain(unlisted(&self.ingress_classes))
                .chain(unlisted(&self.ingresses))
                .chain(unlisted(&self.services))
                .chain(unlisted(&self.secrets))
                .chain(unlisted(&self.rate_limit_policies))
                .collect(),
            self.gateway_api
                .iter()
                .flat_map(GatewayApi::unlisted)
                .collect(),
        )
    }

    /// The follows made to follow what `next` shapes: what it alone needs started, what it
    /// no longer needs stopped, and the rest followed on as it was, so that no data plane
    /// added or let go of begins another's follows again (05 §3). The Gateway API's kinds
    /// are followed while any data plane is a Gateway, and its CRDs and GatewayClasses
    /// always where the control plane has a class of its own.
    fn reshape(&mut self, next: Shape, start: &mut Start<'_>) {
        if next == self.shape {
            return;
        }
        let secrets = with_gateway_namespaces(&self.parts, &next.gateways);
        rescope(&mut self.secrets, &secrets, start);
        rescope(&mut self.rate_limit_policies, &secrets, start);
        rescope(&mut self.own_services, &next.services, start);
        rescope(&mut self.ready, &next.own_services, start);
        if next.gateways.is_empty() && !self.own_class {
            self.gateway_api = None;
        } else if let Some(gateway_api) = &mut self.gateway_api {
            gateway_api.reshape(&next.gateways, &self.parts, start);
        } else {
            self.gateway_api = Some(GatewayApi::start(start, &next.gateways));
        }
        self.shape = next;
    }

    /// Whether the data plane's Service's namespace has been listed whole: what statuses
    /// and the Service's writes wait for, and translation does not, so that a config is
    /// never held up by them.
    fn own_listed(&self) -> bool {
        self.own_services
            .iter()
            .all(|follow| follow.objects.borrow().listed)
    }

    /// The data plane's Service `own` names, as it is now, if it is there.
    fn own_service(&self, own: &ProxyService) -> Option<Service> {
        objects(&self.own_services).into_iter().find(|service| {
            service.metadata.namespace.as_deref() == Some(own.namespace())
                && service.metadata.name.as_deref() == Some(own.name())
        })
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
            rate_limit_policies: objects(&self.rate_limit_policies),
            namespaces: gateway_api
                .map(|g| objects(&g.namespaces))
                .unwrap_or_default(),
            watched: self.watched.clone(),
            gateway_class_labels: None,
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
        let secrets = with_gateway_namespaces(&parts, &self.shape.gateways);
        rescope(&mut self.secrets, &secrets, start);
        rescope(&mut self.rate_limit_policies, &secrets, start);
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
        parts_of(&mut self.rate_limit_policies, &mut parts);
        if let Some(gateway_api) = &mut self.gateway_api {
            gateway_api.parts(&mut parts);
        }
        parts
    }
}

/// Where ListenerSets, Secrets and RateLimitPolicies are followed: the watch scope, as routes are, and each
/// Gateway's own namespace of `gateways` the scope does not hold, as a Gateway that takes
/// ListenerSets `from: Same` takes them there, and its listeners' certificateRefs of its
/// own namespace need no grant (04 §1).
fn with_gateway_namespaces(scope: &[Scope], gateways: &[Scope]) -> Vec<Scope> {
    let mut followed = scope.to_vec();
    if scope.contains(&Scope::Cluster) {
        return followed;
    }
    for gateway in gateways {
        if !followed.contains(gateway) {
            followed.push(gateway.clone());
        }
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

/// The data plane's Service as this replica last wrote it, and what was last said of it
/// that is said once.
#[derive(Debug, Default)]
struct Written {
    /// What was last applied, while it leads: none after a refusal, or as it comes to lead.
    applied: Option<Service>,
    /// What was last said once: its being another's, or of no port.
    quiet: Option<Quiet>,
}

/// What is said once of the data plane's Service, until it changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quiet {
    NotOurs,
    NoPort,
}

impl Written {
    /// Writes the Service `own` shapes, of `ports`, where `live` says otherwise than it, or
    /// it differs from what was last written; a Service of its name that is not its
    /// DataPlane's left alone.
    async fn write(
        &mut self,
        api: &ApiServer,
        own: &ProxyService,
        live: Option<&Service>,
        ports: &BTreeMap<u16, Transports>,
        said: &dyn Fn(Said<'_>),
    ) {
        let named = format!("{}/{}", own.namespace(), own.name());
        let wanted = own.service(ports);
        let quiet = match (&wanted, live) {
            (_, Some(live)) if !service::ours(live, own.owner_uid()) => Some(Quiet::NotOurs),
            (None, _) => Some(Quiet::NoPort),
            (Some(_), _) => None,
        };
        if let Some(quiet) = quiet {
            if self.quiet != Some(quiet) {
                said(Said::Service {
                    service: &named,
                    now: match quiet {
                        Quiet::NotOurs => ServiceNow::NotOurs,
                        Quiet::NoPort => ServiceNow::NoPort,
                    },
                });
            }
            self.quiet = Some(quiet);
            return;
        }
        self.quiet = None;
        let Some(wanted) = wanted else {
            return;
        };
        let changed = self.applied.as_ref() != Some(&wanted)
            || live.is_none_or(|live| service::differs(live, &wanted));
        if !changed {
            return;
        }
        match api
            .apply::<Service>(SERVICE_MANAGED_BY, own.namespace(), own.name(), &wanted)
            .await
        {
            Ok(_) => {
                said(Said::Service {
                    service: &named,
                    now: ServiceNow::Applied(&service::listed(&wanted)),
                });
                self.applied = Some(wanted);
            }
            Err(error) => {
                said(Said::Service {
                    service: &named,
                    now: ServiceNow::Refused(&error),
                });
                self.applied = None;
            }
        }
    }
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

/// What there is to say of the follows waited on, `unlisted` now, `said` being what was
/// said of them last: each wait once, and again only as it changes; nothing while none is.
fn wait_said(said: &mut Option<Vec<String>>, unlisted: Vec<String>) -> Option<&[String]> {
    if unlisted.is_empty() {
        *said = None;
        return None;
    }
    if said.as_ref() == Some(&unlisted) {
        return None;
    }
    Some(said.insert(unlisted))
}

/// What was last translated for a data plane, so that nothing is said twice, and what of
/// the RateLimitPolicies its config served applied.
#[derive(Debug, Default)]
struct Translator {
    /// The last translation said, handed on or refused.
    last: Option<Translation>,
    /// The version of each RateLimitPolicy that applied in the last config handed on: each
    /// holds its limits' names, and applies where a later spec is refused (04 §4). Kept in
    /// memory alone, so lost on a restart (docs/12 item 33).
    accepted: Vec<AcceptedPolicy>,
}

/// What there is to say of the objects followed, for a data plane.
#[derive(Debug, PartialEq)]
enum Step<'a> {
    /// Nothing new.
    Nothing,
    /// A translation that compiles, and differs from the last.
    Translated(&'a Translation),
    /// A translation that differs from the last, which the data plane would refuse.
    Refused {
        translation: &'a Translation,
        problems: Vec<ConfigError>,
    },
}

impl Translator {
    /// Translates for `plane` the objects of `cluster`, from what the last config handed on
    /// applied of their RateLimitPolicies; a translation that compiles is handed on, and
    /// what it applies of them is what the next translates from.
    fn step(&mut self, plane: &Plane, cluster: &Cluster) -> Step<'_> {
        let translation = translate_from(plane, cluster, &self.accepted);
        if self.last.as_ref() == Some(&translation) {
            return Step::Nothing;
        }
        let refused = compile(&translation.config).err();
        let translation = self.last.insert(translation);
        match refused {
            None => {
                self.accepted.clone_from(&translation.rate_limit_policies);
                Step::Translated(translation)
            }
            Some(problems) => Step::Refused {
                translation,
                problems,
            },
        }
    }

    /// A limit of `plane`'s `global` of a name a RateLimitPolicy holds in the last config
    /// handed on, and that policy, where there is one: a generation of its DataPlane that
    /// brings it is held back (04 §4).
    fn held_by_policy(&self, plane: &Plane) -> Option<(String, Object)> {
        let global = &plane.rate_limiting.as_ref()?.global;
        global.iter().find_map(|limit| {
            self.accepted
                .iter()
                .find(|policy| {
                    policy
                        .spec
                        .limits
                        .iter()
                        .flatten()
                        .any(|own| own.name == limit.name)
                })
                .map(|policy| {
                    let by = Object {
                        kind: "RateLimitPolicy",
                        namespace: policy.namespace.clone(),
                        name: policy.name.clone(),
                    };
                    (limit.name.clone(), by)
                })
        })
    }
}

#[cfg(test)]
mod tests;
