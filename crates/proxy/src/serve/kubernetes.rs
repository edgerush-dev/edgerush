//! The namespaces the running config's discovered upstreams are in, followed: a list and a
//! watch of each one's EndpointSlices, their changes gathered and taken in
//! ([03 §6](../../docs/03-data-plane.md)).

use super::Proxy;
use edgerush_kubernetes::{ApiServer, Retry, Slices, follow};
use std::collections::HashMap;
use std::fmt::Display;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// How long a namespace's changes are gathered before they are taken in, so that a rollout
/// that changes its slices many times in a moment makes one snapshot rather than many.
const GATHERED: Duration = Duration::from_millis(100);

/// What is told what fails, with the namespace it failed for.
type Said = Rc<dyn Fn(&str, &dyn Display)>;

impl Proxy {
    /// Follows the namespaces the running config's discovered upstreams are in, through
    /// `api`, for as long as the data plane runs: a namespace a reload comes to name is
    /// followed from then on, and one it no longer names is no longer followed, its slices
    /// forgotten. `said` is told what fails, with the namespace it failed for. For a thread
    /// and runtime of its own, in a `LocalSet`, as [`Proxy::check_health`] is.
    pub async fn follow_kubernetes(
        self: Arc<Self>,
        api: ApiServer,
        said: impl Fn(&str, &dyn Display) + 'static,
    ) {
        let api = Rc::new(api);
        let said: Said = Rc::new(said);
        let mut wanted = self.namespaces.subscribe();
        // Each namespace followed, by the task that takes in its slices: ended, its
        // receiver goes, and with it the follow.
        let mut followed: HashMap<String, JoinHandle<()>> = HashMap::new();
        loop {
            let now = wanted.borrow_and_update().clone();
            let gone: Vec<String> = followed
                .keys()
                .filter(|namespace| !now.contains(*namespace))
                .cloned()
                .collect();
            for namespace in gone {
                if let Some(taking) = followed.remove(&namespace) {
                    taking.abort();
                }
                if let Err(error) = self.rediscover([(namespace.clone(), None)]) {
                    said(&namespace, &error);
                }
            }
            for namespace in now {
                if followed.contains_key(&namespace) {
                    continue;
                }
                let (out, slices) = watch::channel(Arc::new(Slices::default()));
                let (api, failed, name) = (Rc::clone(&api), Rc::clone(&said), namespace.clone());
                tokio::task::spawn_local(async move {
                    follow(&api, &name, Retry::default(), out, |failure| {
                        failed(&name, failure);
                    })
                    .await;
                });
                let taking = tokio::task::spawn_local(take_in(
                    Arc::clone(&self),
                    namespace.clone(),
                    slices,
                    Rc::clone(&said),
                ));
                followed.insert(namespace, taking);
            }
            if wanted.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Takes in `namespace`'s slices each time they change, once a list has been read whole,
/// the changes of a moment gathered into one.
async fn take_in(
    proxy: Arc<Proxy>,
    namespace: String,
    mut slices: watch::Receiver<Arc<Slices>>,
    said: Said,
) {
    while slices.changed().await.is_ok() {
        tokio::time::sleep(GATHERED).await;
        let now = Arc::clone(&slices.borrow_and_update());
        if !now.listed {
            continue;
        }
        if let Err(error) = proxy.rediscover([(namespace.clone(), Some(now.slices.clone().into()))])
        {
            said(&namespace, &error);
        }
    }
}
