//! A kind's objects followed across parts of the cluster that change: the namespaces a
//! control plane reads its DataPlanes in, its own and its watch scope's, a selector's as
//! they come to match and stop ([`watch_parts`]); and those of the DataPlanes it serves,
//! where it keeps their certificates and finds their pods
//! ([05 §3](../../../docs/05-operator.md)). Each part is followed on its own, as the
//! control plane's rights are granted a namespace at a time.

use crate::{Tasks, WatchScope};
use edgerush_kubernetes::{ApiServer, Failure, Kind, Listed, Retry, Scope, follow};
use k8s_openapi::api::core::v1::Namespace;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

/// A kind's objects in each part followed, as last listed.
#[derive(Debug)]
pub struct Across<K> {
    /// Each part followed, and its objects as its follow last published them.
    pub parts: BTreeMap<Scope, Arc<Listed<K>>>,
    /// Whether every part followed from the first has been listed: one added after is not
    /// waited on, and is left out until it is, so that a namespace that cannot be read yet
    /// holds up none of the others.
    pub listed: bool,
}

impl<K> Default for Across<K> {
    fn default() -> Self {
        Self {
            parts: BTreeMap::new(),
            listed: false,
        }
    }
}

impl<K: Clone> Across<K> {
    /// The objects of every part listed, part by part.
    #[must_use]
    pub fn objects(&self) -> Vec<K> {
        self.parts
            .values()
            .filter(|listed| listed.listed)
            .flat_map(|listed| listed.objects.iter().cloned())
            .collect()
    }

    /// The objects of every part listed, once [`Across::listed`] says so.
    #[must_use]
    pub fn objects_if_listed(&self) -> Option<Vec<K>> {
        self.listed.then(|| self.objects())
    }

    /// Whether `part` is followed, and has been listed.
    #[must_use]
    pub fn listed_in(&self, part: &Scope) -> bool {
        self.parts.get(part).is_some_and(|listed| listed.listed)
    }
}

/// What a part's failure is said to.
type Failed = Rc<dyn Fn(&Scope, &Failure)>;

/// A part's follow: what it lists, and its tasks, aborted as it is let go of.
type PartFollow<K> = (watch::Receiver<Arc<Listed<K>>>, Tasks);

/// Follows `K` through `api` in each part `parts` holds, as it changes, none while it holds
/// `None`: one no longer held is let go of, one newly held followed. Hands `out` what is
/// listed each time it changes, and says each failure of a part to `failed`. Runs until no
/// one holds a receiver of `out`; a sender of `parts` gone leaves them as they were. For a
/// `LocalSet`.
pub async fn follow_across<K: Kind + Clone + 'static>(
    api: Rc<ApiServer>,
    mut parts: watch::Receiver<Option<BTreeSet<Scope>>>,
    out: watch::Sender<Arc<Across<K>>>,
    failed: impl Fn(&Scope, &Failure) + 'static,
) {
    let failed: Failed = Rc::new(failed);
    let changed = Rc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let mut follows: BTreeMap<Scope, PartFollow<K>> = BTreeMap::new();
    // The parts first held, which alone are waited on.
    let mut first: Option<BTreeSet<Scope>> = None;
    let mut told = true;
    loop {
        if let Some(now) = parts.borrow_and_update().clone() {
            first.get_or_insert_with(|| now.clone());
            follows.retain(|part, _| now.contains(part));
            for part in now {
                if follows.contains_key(&part) {
                    continue;
                }
                let (listing, listed) = watch::channel(Arc::new(Listed::<K>::default()));
                let (client, failing, followed) =
                    (Rc::clone(&api), Rc::clone(&failed), part.clone());
                let following = tasks.spawn_local(async move {
                    follow(&client, &followed, Retry::default(), listing, |failure| {
                        failing(&followed, failure);
                    })
                    .await;
                });
                let (mut listening, notify) = (listed.clone(), Rc::clone(&changed));
                let telling = tasks.spawn_local(async move {
                    while listening.changed().await.is_ok() {
                        notify.notify_one();
                    }
                });
                follows.insert(part, (listed, Tasks(vec![following, telling])));
            }
        }
        // Follows let go of are aborted: what is left of them is reaped.
        while tasks.try_join_next().is_some() {}
        let now: BTreeMap<Scope, Arc<Listed<K>>> = follows
            .iter()
            .map(|(part, (listed, _))| (part.clone(), Arc::clone(&listed.borrow())))
            .collect();
        let listed = first.as_ref().is_some_and(|first| {
            first
                .iter()
                .all(|part| now.get(part).is_none_or(|listed| listed.listed))
        });
        out.send_replace(Arc::new(Across { parts: now, listed }));
        tokio::select! {
            changed = parts.changed(), if told => if changed.is_err() { told = false },
            () = changed.notified() => {}
            () = out.closed() => return,
        }
    }
}

/// Where a control plane reads `K`s of its own as its scope has them: its own namespace,
/// `own`, and the namespaces of `scope`, the cluster for `All`, a selector's as Namespaces
/// say, followed through `api`, which come to match and stop. Each part once. Handed to
/// `out` as it changes, from when it is known; a failure of the Namespaces' follow said to
/// `failed`. Runs until no one holds a receiver of `out`. For a `LocalSet`.
pub async fn watch_parts(
    api: Rc<ApiServer>,
    scope: WatchScope,
    own: Scope,
    out: watch::Sender<Option<BTreeSet<Scope>>>,
    failed: impl Fn(&Failure) + 'static,
) {
    let with_own = |mut parts: BTreeSet<Scope>| {
        // The cluster holds the namespace already.
        if !parts.contains(&Scope::Cluster) {
            parts.insert(own.clone());
        }
        parts
    };
    let selector = match scope {
        WatchScope::Parts(parts) => {
            out.send_replace(Some(with_own(parts.into_iter().collect())));
            out.closed().await;
            return;
        }
        WatchScope::Selector(selector) => selector,
    };
    let (listing, mut listed) = watch::channel(Arc::new(Listed::<Namespace>::default()));
    let mut tasks = JoinSet::new();
    tasks.spawn_local(async move {
        follow(
            &api,
            &Scope::selected(&selector),
            Retry::default(),
            listing,
            |failure| failed(failure),
        )
        .await;
    });
    loop {
        tokio::select! {
            changed = listed.changed() => if changed.is_err() { return },
            () = out.closed() => return,
        }
        let now = Arc::clone(&listed.borrow_and_update());
        if !now.listed {
            continue;
        }
        let parts = with_own(
            now.objects
                .iter()
                .filter_map(|namespace| namespace.metadata.name.clone())
                .map(Scope::Namespace)
                .collect(),
        );
        out.send_if_modified(|sent| {
            let changed = sent.as_ref() != Some(&parts);
            if changed {
                *sent = Some(parts);
            }
            changed
        });
    }
}

#[cfg(test)]
mod tests;
