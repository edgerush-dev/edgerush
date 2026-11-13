//! EdgeRush's operator ([05](../../../docs/05-operator.md)): `ControlPlane` and `DataPlane`
//! resources turned into running infrastructure, and kept so. It never sees traffic
//! configuration, and has no part in a plane's day-to-day running.
//!
//! [`judge`] says which resources are accepted, by the checks only the cluster can make;
//! [`status`] what their conditions say; [`Operator::step`] applies what each accepted one
//! asks for, as `edgerush-planes` makes it, and deletes what no owner reference reaches of
//! one deleted. [`operate`] follows every ControlPlane and DataPlane in the cluster and
//! every object the operator made, elects a leader among the replicas by a Lease, and has
//! the leader step each time any of them changes. GatewayClasses and Gateways it follows,
//! and makes, only while the cluster serves them: Ingress alone needs no Gateway API CRDs
//! ([05 §6](../../../docs/05-operator.md)).

pub mod judge;
pub mod status;
mod step;

pub use step::{Child, GatewayApi, Operator, Seen};

use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_kubernetes::gateway_api::{GatewayKind, Served};
use edgerush_kubernetes::lease::{self, elect};
use edgerush_kubernetes::{ApiServer, Kind, Listed, Retry, Scope, follow};
use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::objects::{Install, MANAGED_BY, MANAGED_BY_LABEL};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Service, ServiceAccount};
use k8s_openapi::api::networking::v1::IngressClass;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{ClusterRoleBinding, RoleBinding};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

/// The field manager the operator applies under, the value of the label it marks what it
/// makes with.
pub const MANAGER: &str = MANAGED_BY;

/// The finalizer it puts on a resource before it makes anything for it, and takes away
/// once what no owner reference reaches is deleted.
pub const FINALIZER: &str = "gateway.edgerush.dev/operator";

/// How long changes are gathered before a step: a resource applied changes several
/// objects at once.
const GATHER: Duration = Duration::from_millis(100);

/// What the operator is told.
#[derive(Debug, Clone)]
pub struct Settings {
    /// What every object takes from the install.
    pub install: Install,
    /// The namespace its Lease is in, its own.
    pub namespace: String,
    /// The Lease its replicas elect a leader by.
    pub lease: String,
    /// This replica, as the Lease names its holder.
    pub identity: String,
    /// How the Lease is held.
    pub timing: lease::Timing,
    /// How long a step that left something undone is waited out before another: from the
    /// first, doubling to the most.
    pub retry: Retry,
}

/// A kind's objects, as their follow last published them.
type Followed<K> = watch::Receiver<Arc<Listed<K>>>;

/// Follows `K`'s objects in `scope` through `api` in a task of `tasks`, each change told to
/// `changed`, each failure said to `say`.
fn followed<K: Kind + 'static>(
    tasks: &mut JoinSet<()>,
    api: &Rc<ApiServer>,
    scope: Scope,
    changed: &Rc<Notify>,
    say: &Rc<dyn Fn(&str)>,
) -> Followed<K> {
    let (out, listed) = watch::channel(Arc::new(Listed::<K>::default()));
    let (client, failing) = (Rc::clone(api), Rc::clone(say));
    tasks.spawn_local(async move {
        follow(&client, &scope, Retry::default(), out, |failure| {
            failing(&format!("{}: {failure}", K::URL_PATH_SEGMENT));
        })
        .await;
    });
    let (mut told, notify) = (listed.clone(), Rc::clone(changed));
    tasks.spawn_local(async move {
        while told.changed().await.is_ok() {
            notify.notify_one();
        }
    });
    listed
}

/// `followed`'s objects added to `into` as children: whether they are listed whole.
fn each<K: Kind>(followed: &Followed<K>, into: &mut Vec<Child>) -> bool {
    let listed = followed.borrow();
    into.extend(listed.objects.iter().map(Child::of));
    listed.listed
}

/// A kind of the Gateway API's the operator makes: its CRD followed by name, and the
/// objects of it the operator made only while the CRD serves the version read.
struct WhileServed<K: Kind> {
    kind: GatewayKind,
    crd: Followed<CustomResourceDefinition>,
    /// What was last said of the kind.
    last: Option<Served>,
    /// The follow of its objects, and its tasks, aborted when dropped.
    objects: Option<(Followed<K>, JoinSet<()>)>,
}

impl<K: Kind + 'static> WhileServed<K> {
    /// Follows `kind`'s CRD in a task of `tasks`, its objects waiting on what it says.
    fn start(
        kind: GatewayKind,
        tasks: &mut JoinSet<()>,
        api: &Rc<ApiServer>,
        changed: &Rc<Notify>,
        say: &Rc<dyn Fn(&str)>,
    ) -> Self {
        let crd = followed(
            tasks,
            api,
            Scope::Named(kind.crd().to_owned()),
            changed,
            say,
        );
        Self {
            kind,
            crd,
            last: None,
            objects: None,
        }
    }

    /// Whether the kind is served, once its CRD is listed: its objects in `scope` followed
    /// from when it is, and no longer from when it is not, each change said to `say`.
    fn keep(
        &mut self,
        api: &Rc<ApiServer>,
        scope: &Scope,
        changed: &Rc<Notify>,
        say: &Rc<dyn Fn(&str)>,
    ) -> Option<bool> {
        let served = {
            let listed = self.crd.borrow();
            if !listed.listed {
                return None;
            }
            Served::of(&listed.objects)
        };
        if self.last.as_ref() != Some(&served) {
            say(&format!("Gateway API's {}: {served}", self.kind.name()));
            if served.is_served() != self.objects.is_some() {
                self.objects = served.is_served().then(|| {
                    let mut tasks = JoinSet::new();
                    let objects = followed(&mut tasks, api, scope.clone(), changed, say);
                    (objects, tasks)
                });
            }
            self.last = Some(served);
        }
        Some(self.objects.is_some())
    }

    /// Its objects added to `into` as children, none while it is not served: whether they
    /// are listed whole.
    fn seen(&self, into: &mut Vec<Child>) -> bool {
        self.objects
            .as_ref()
            .is_none_or(|(objects, _)| each(objects, into))
    }
}

/// Every object of a kind the operator makes, followed.
struct Children {
    service_accounts: Followed<ServiceAccount>,
    role_bindings: Followed<RoleBinding>,
    cluster_role_bindings: Followed<ClusterRoleBinding>,
    deployments: Followed<Deployment>,
    services: Followed<Service>,
    autoscalers: Followed<HorizontalPodAutoscaler>,
    disruption_budgets: Followed<PodDisruptionBudget>,
    ingress_classes: Followed<IngressClass>,
    gateway_classes: WhileServed<GatewayClass>,
    gateways: WhileServed<Gateway>,
}

impl Children {
    /// Each kind's objects, as listed so far: none until every kind is listed whole.
    fn seen(&self) -> Option<Vec<Child>> {
        let mut children = Vec::new();
        let deployments = self.deployments.borrow();
        children.extend(deployments.objects.iter().map(Child::of_deployment));
        let listed = [
            deployments.listed,
            each(&self.service_accounts, &mut children),
            each(&self.role_bindings, &mut children),
            each(&self.cluster_role_bindings, &mut children),
            each(&self.services, &mut children),
            each(&self.autoscalers, &mut children),
            each(&self.disruption_budgets, &mut children),
            each(&self.ingress_classes, &mut children),
            self.gateway_classes.seen(&mut children),
            self.gateways.seen(&mut children),
        ];
        listed.iter().all(|listed| *listed).then_some(children)
    }
}

/// Runs the operator through `api`, and `elector`, a client of its own for the Lease, so
/// that its writes never wait behind the follows', as `settings` say: what it does and what
/// fails said to `say`. Runs for as long as it is polled. For a `LocalSet`.
pub async fn operate(
    api: ApiServer,
    elector: ApiServer,
    settings: Settings,
    say: impl Fn(&str) + 'static,
) {
    let api = Rc::new(api);
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let changed = Rc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let made = BTreeMap::from([(MANAGED_BY_LABEL.to_owned(), MANAGED_BY.to_owned())]);
    let Ok(made) = Scope::labelled(&made) else {
        return;
    };
    let control_planes: Followed<ControlPlane> =
        followed(&mut tasks, &api, Scope::Cluster, &changed, &say);
    let data_planes: Followed<DataPlane> =
        followed(&mut tasks, &api, Scope::Cluster, &changed, &say);
    let mut children = Children {
        service_accounts: followed(&mut tasks, &api, made.clone(), &changed, &say),
        role_bindings: followed(&mut tasks, &api, made.clone(), &changed, &say),
        cluster_role_bindings: followed(&mut tasks, &api, made.clone(), &changed, &say),
        deployments: followed(&mut tasks, &api, made.clone(), &changed, &say),
        services: followed(&mut tasks, &api, made.clone(), &changed, &say),
        autoscalers: followed(&mut tasks, &api, made.clone(), &changed, &say),
        disruption_budgets: followed(&mut tasks, &api, made.clone(), &changed, &say),
        ingress_classes: followed(&mut tasks, &api, made.clone(), &changed, &say),
        gateway_classes: WhileServed::start(
            GatewayKind::GatewayClass,
            &mut tasks,
            &api,
            &changed,
            &say,
        ),
        gateways: WhileServed::start(GatewayKind::Gateway, &mut tasks, &api, &changed, &say),
    };
    let (leading_out, leading) = watch::channel(false);
    {
        let (failing, settings) = (Rc::clone(&say), settings.clone());
        tasks.spawn_local(async move {
            elect(
                &elector,
                &settings.namespace,
                &settings.lease,
                &settings.identity,
                settings.timing,
                &leading_out,
                |error| failing(&format!("lease {}: {error}", settings.lease)),
            )
            .await;
        });
        let (mut told, notify) = (leading.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                notify.notify_one();
            }
        });
    }
    let mut operator = Operator::new(settings.install.clone());
    let mut led = false;
    // How long to wait before stepping again what a step left undone; none while all is
    // done.
    let mut again: Option<Duration> = None;
    loop {
        match again {
            Some(wait) => {
                tokio::select! {
                    () = changed.notified() => {}
                    () = tokio::time::sleep(wait) => {}
                }
            }
            None => changed.notified().await,
        }
        tokio::time::sleep(GATHER).await;
        let leads = *leading.borrow();
        if leads != led {
            say(if leads {
                "leading"
            } else {
                "no longer leading"
            });
            led = leads;
            operator.forget();
            again = None;
        }
        if !leads {
            continue;
        }
        let (control_planes_now, data_planes_now) = (
            Arc::clone(&control_planes.borrow()),
            Arc::clone(&data_planes.borrow()),
        );
        let (Some(gateway_classes), Some(gateways)) = (
            children.gateway_classes.keep(&api, &made, &changed, &say),
            children.gateways.keep(&api, &made, &changed, &say),
        ) else {
            continue;
        };
        let Some(children_now) = children.seen() else {
            continue;
        };
        if !control_planes_now.listed || !data_planes_now.listed {
            continue;
        }
        let seen = Seen {
            control_planes: control_planes_now.objects.clone(),
            data_planes: data_planes_now.objects.clone(),
            children: children_now,
            gateway_api: GatewayApi {
                gateway_classes,
                gateways,
            },
        };
        let now = Time(lease::wall_clock());
        let done = operator.step(&api, &seen, &now, &*say).await;
        again = if done {
            None
        } else {
            Some(again.map_or(settings.retry.first, |wait| {
                (wait * 2).min(settings.retry.most)
            }))
        };
    }
}
