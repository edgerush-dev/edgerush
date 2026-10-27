//! A kind's objects followed: a list and a watch kept going for as long as anyone looks at
//! what they give ([03 §6](../../../docs/03-data-plane.md),
//! [04 §1](../../../docs/04-control-plane.md)).
//!
//! What to ask for and what the answers leave is [`Watch`]'s; this asks, and publishes the
//! objects each time they change, and once when the first list is read whole, so that a
//! scope with none is known to have none. A version the API server no longer has is
//! listed afresh at once, and a watch it ended is begun again at once. Any other failure is
//! said, and waited out before asking again: from [`Retry::first`], doubling each time to
//! [`Retry::most`], each wait between half and all of that, so that data planes started
//! together do not ask together; back to the first once an answer is read.

use crate::{ApiError, ApiServer, Kind, Request, Scope, Watch, WatchError};
use std::future::{Future, poll_fn};
use std::hash::{BuildHasher, RandomState};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::watch;

/// A kind's objects in a scope, as last read.
#[derive(Debug, Clone, PartialEq)]
pub struct Listed<K> {
    /// Whether a list has been read whole: until one has, there are none, and what the
    /// scope holds is not known yet.
    pub listed: bool,
    /// The objects, by namespace, then name.
    pub objects: Vec<K>,
}

impl<K> Default for Listed<K> {
    fn default() -> Self {
        Self {
            listed: false,
            objects: Vec::new(),
        }
    }
}

/// How long failures are waited out before asking again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    /// After the first failure.
    pub first: Duration,
    /// The most, however many fail in a row.
    pub most: Duration,
}

impl Default for Retry {
    /// Kubernetes' own clients': 800 ms, to 30 s.
    fn default() -> Self {
        Self {
            first: Duration::from_millis(800),
            most: Duration::from_secs(30),
        }
    }
}

/// What failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Failure {
    /// Asking the API server.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// What it answered.
    #[error(transparent)]
    Watch(#[from] WatchError),
}

/// Follows `K`'s objects in `scope` on `api`, publishing them on `out` each time they
/// change, and saying each failure to `failed`, until no one holds a receiver of `out`.
pub async fn follow<K: Kind>(
    api: &ApiServer,
    scope: &Scope,
    retry: Retry,
    out: watch::Sender<Arc<Listed<K>>>,
    mut failed: impl FnMut(&Failure),
) {
    let mut watch = Watch::<K>::new();
    let mut waits = Waits {
        retry,
        failed: 0,
        random: RandomState::new(),
    };
    let mut published = false;
    let publish = |watch: &Watch<K>| {
        out.send_replace(Arc::new(Listed {
            listed: watch.listed(),
            objects: watch.objects().cloned().collect(),
        }));
    };
    loop {
        let failure: Option<Failure> = match watch.request() {
            Request::List { continue_from } => {
                let Some(answer) =
                    unless_closed(&out, api.list(scope, continue_from.as_deref())).await
                else {
                    return;
                };
                match answer
                    .map_err(Failure::from)
                    .and_then(|page| Ok(watch.page(page)?))
                {
                    Ok(changed) => {
                        if watch.listed() && (changed || !published) {
                            publish(&watch);
                            published = true;
                        }
                        waits.failed = 0;
                        None
                    }
                    Err(failure) => Some(failure),
                }
            }
            Request::Watch { from } => {
                let Some(answer) = unless_closed(&out, api.watch(scope, &from)).await else {
                    return;
                };
                match answer {
                    Err(error) => Some(error.into()),
                    Ok(mut events) => loop {
                        let Some(next) = unless_closed(&out, events.next()).await else {
                            return;
                        };
                        match next {
                            // The API server ended the watch, as it does after the time
                            // asked for: begun again at once from where it was.
                            None => break None,
                            Some(Err(error)) => break Some(error.into()),
                            Some(Ok(event)) => match watch.event(event) {
                                Ok(changed) => {
                                    waits.failed = 0;
                                    if changed {
                                        publish(&watch);
                                    }
                                }
                                Err(error) => break Some(error.into()),
                            },
                        }
                    },
                }
            }
        };
        let Some(failure) = failure else {
            continue;
        };
        failed(&failure);
        let gone = match &failure {
            Failure::Api(error) => error.is_gone(),
            Failure::Watch(error) => matches!(error, WatchError::Gone | WatchError::NoVersion),
        };
        if gone {
            // Listed afresh at once: what was kept stays in use until that list is whole.
            watch.gone();
            continue;
        }
        if unless_closed(&out, tokio::time::sleep(waits.next()))
            .await
            .is_none()
        {
            return;
        }
    }
}

/// How long to wait after a failure.
struct Waits {
    retry: Retry,
    /// Failures in a row.
    failed: u32,
    random: RandomState,
}

impl Waits {
    /// The wait after one more failure: between half and all of the first doubled for each
    /// before it, at most the most.
    fn next(&mut self) -> Duration {
        let doubled = self
            .retry
            .first
            .saturating_mul(1 << self.failed.min(20))
            .min(self.retry.most);
        self.failed = self.failed.saturating_add(1);
        let part = self.random.hash_one(self.failed) % 1000;
        doubled / 2 + doubled / 2 * u32::try_from(part).unwrap_or(0) / 1000
    }
}

/// `work` done, or nothing once no one holds a receiver of `out`.
async fn unless_closed<T, K>(
    out: &watch::Sender<Arc<Listed<K>>>,
    work: impl Future<Output = T>,
) -> Option<T> {
    let mut work = pin!(work);
    let mut closed = pin!(out.closed());
    poll_fn(|cx| {
        if closed.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        work.as_mut().poll(cx).map(Some)
    })
    .await
}

#[cfg(test)]
mod tests;
