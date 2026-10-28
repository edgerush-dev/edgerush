//! EdgeRush's control plane ([04](../../../docs/04-control-plane.md)): the objects a data
//! plane's config is made of, followed in the scope the control plane watches, translated
//! for the data plane once every kind has been listed whole, and each config that compiles
//! and differs from the last handed on.
//!
//! The changes of a moment make one translation: it waits until no change has come for
//! [`Debounce::quiet`], and never more than [`Debounce::most`] after the first it waits on
//! (04 §2). Each translation is whole, of everything followed, and pure
//! ([`edgerush_translate`]); the follows and the waiting are all that is not. Where it is
//! asked to, it writes each Ingress's status as the config handed on serves it (04 §6),
//! [`status`]. The admission webhook's reviews are read, the Ingress each asks
//! about judged alone, and answered by [`admission`] (04 §3).

use edgerush_config::{ConfigError, compile};
use edgerush_kubernetes::{ApiError, ApiServer, Failure, Kind, Listed, Retry, Scope, follow};
use edgerush_translate::{Cluster, Object, Plane, Translation, translate};
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::networking::v1::IngressLoadBalancerIngress;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

pub mod admission;
pub mod lease;
pub mod status;

use status::Addresses;

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

/// Follows, through `api`, the kinds `plane`'s config is made of in each of `scope`'s
/// parts, IngressClasses in the cluster, and translates them for it each time they
/// change, once every one has been listed whole; says how it goes to `said`. With
/// `status`, it writes each Ingress's status to say the addresses it gives as the config
/// handed on serves the Ingress, where it says otherwise; with `leading` too, only while it
/// says this replica leads, every status that says otherwise written as it comes to (02 §3).
/// Runs for as long as it is polled, its follows ended with it. For a thread and runtime of
/// its own, in a `LocalSet`.
pub async fn control(
    api: ApiServer,
    scope: &[Scope],
    plane: &Plane,
    debounce: Debounce,
    status: Option<Addresses>,
    leading: Option<watch::Receiver<bool>>,
    said: impl Fn(Said<'_>) + 'static,
) {
    let api = Rc::new(api);
    let said: Rc<dyn Fn(Said<'_>)> = Rc::new(said);
    let changed = Rc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let mut start = Start {
        tasks: &mut tasks,
        api: &api,
        changed: &changed,
        said: &said,
    };
    let followed = Followed {
        ingress_classes: start.follows(&[Scope::Cluster]),
        ingresses: start.follows(scope),
        services: start.follows(scope),
        secrets: start.follows(scope),
        status_service: match &status {
            Some(Addresses::Service { namespace, .. }) => {
                start.follows(&[Scope::Namespace(namespace.clone())])
            }
            _ => Vec::new(),
        },
    };
    // Coming to lead is a change: every status is looked at, as the last leader may have
    // left some stale.
    if let Some(leading) = &leading {
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
    loop {
        match translator.step(plane, followed.unlisted(), || followed.cluster()) {
            Step::Nothing => {}
            Step::Waiting(unlisted) => said(Said::Waiting(unlisted)),
            Step::Translated(translation) => {
                handed = Some((translation.claimed.clone(), translation.served.clone()));
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
        // to its Ingress, and the round it brings finds nothing more to write.
        if let (Some(source), Some((claimed, served))) = (&status, &handed)
            && followed.status_listed()
            && leading.as_ref().is_none_or(|leads| *leads.borrow())
        {
            let addresses = match source {
                Addresses::Stated(addresses) => addresses.clone(),
                Addresses::Service { name, .. } => {
                    let services = followed.status_services();
                    let named = services
                        .iter()
                        .find(|service| service.metadata.name.as_deref() == Some(name.as_str()));
                    status::of_service(named)
                }
            };
            let ingresses = followed.ingresses();
            for change in status::changes(claimed, served, &ingresses, &addresses) {
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
        // Until a change comes, and then until its moment is over.
        changed.notified().await;
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
}

impl Start<'_> {
    /// Starts following `K`'s objects in each of `scope`'s parts.
    fn follows<K: Kind + 'static>(&mut self, scope: &[Scope]) -> Vec<Follow<K>> {
        scope
            .iter()
            .map(|part| {
                let name = followed::<K>(part);
                let (out, objects) = watch::channel(Arc::new(Listed::default()));
                let (api, said, part, failed) = (
                    Rc::clone(self.api),
                    Rc::clone(self.said),
                    part.clone(),
                    name.clone(),
                );
                self.tasks.spawn_local(async move {
                    follow(&api, &part, Retry::default(), out, |failure| {
                        said(Said::Failed {
                            followed: &failed,
                            failure,
                        });
                    })
                    .await;
                });
                // Each change to the objects is a change to wait out.
                let (mut told, changed) = (objects.clone(), Rc::clone(self.changed));
                self.tasks.spawn_local(async move {
                    while told.changed().await.is_ok() {
                        changed.notify_one();
                    }
                });
                Follow { name, objects }
            })
            .collect()
    }
}

/// What a follow of `K` in `part` is called: `secrets in shop`.
fn followed<K: Kind>(part: &Scope) -> String {
    match part {
        Scope::Cluster => format!("{} in the cluster", K::URL_PATH_SEGMENT),
        Scope::Namespace(namespace) => format!("{} in {namespace}", K::URL_PATH_SEGMENT),
    }
}

/// A kind's objects in one part of the scope, as their follow last published them.
struct Follow<K> {
    name: String,
    objects: watch::Receiver<Arc<Listed<K>>>,
}

/// Every follow, by kind.
struct Followed {
    ingress_classes: Vec<Follow<IngressClass>>,
    ingresses: Vec<Follow<Ingress>>,
    services: Vec<Follow<Service>>,
    secrets: Vec<Follow<Secret>>,
    /// The namespace of the data plane's Service, followed for its addresses.
    status_service: Vec<Follow<Service>>,
}

impl Followed {
    /// The follows translation waits on that have not been listed whole, by name.
    fn unlisted(&self) -> Vec<String> {
        fn of<K>(follows: &[Follow<K>]) -> impl Iterator<Item = String> {
            follows
                .iter()
                .filter(|follow| !follow.objects.borrow().listed)
                .map(|follow| follow.name.clone())
        }
        of(&self.ingress_classes)
            .chain(of(&self.ingresses))
            .chain(of(&self.services))
            .chain(of(&self.secrets))
            .collect()
    }

    /// Whether the data plane's Service's namespace has been listed whole: what statuses
    /// wait for, and translation does not, so that a config is never held up by its status.
    fn status_listed(&self) -> bool {
        self.status_service
            .iter()
            .all(|follow| follow.objects.borrow().listed)
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
        Cluster {
            ingress_classes: objects(&self.ingress_classes),
            ingresses: objects(&self.ingresses),
            services: objects(&self.services),
            secrets: objects(&self.secrets),
            secrets_assumed: false,
        }
    }
}

/// The objects of `follows`, as they last published them.
fn objects<K: Clone>(follows: &[Follow<K>]) -> Vec<K> {
    follows
        .iter()
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
