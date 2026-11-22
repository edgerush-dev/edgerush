//! Taking a listener's next connection without spinning while none can be taken.
//!
//! A failed accept is most often the process out of open files, and then every try fails
//! at once until one frees: tried again straight away, a listener's loop spins a core for
//! as long as that lasts. So each failure is followed by a pause, as the data plane's own
//! listeners do ([03 §9](../../../docs/03-data-plane.md)). The rarer failure that is the
//! next connection's alone (aborted before it was taken) costs the ones behind it the same
//! pause, which listeners of a control plane's few connections can bear.

use std::io;
use std::time::Duration;

/// How long it waits after a connection could not be taken before it tries again.
pub(crate) const PAUSE: Duration = Duration::from_millis(100);

/// The next connection `accept` takes, each failure followed by a [`PAUSE`] before the
/// next try. Dropped in a pause, it tries no more: a loop that also waits for a connection
/// to end tries again as soon as one does, its file freed.
pub(crate) async fn next<T, F>(mut accept: impl FnMut() -> F) -> T
where
    F: Future<Output = io::Result<T>>,
{
    loop {
        match accept().await {
            Ok(accepted) => return accepted,
            Err(_) => tokio::time::sleep(PAUSE).await,
        }
    }
}

#[cfg(test)]
mod tests;
