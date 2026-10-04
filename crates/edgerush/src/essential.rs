//! The tasks and threads a data plane cannot do without, watched: a worker's sweep and
//! timers, its accepting, its HTTP/3 and the connections handed to it, the worker itself,
//! and the health checker and the scrape. One that ends other than in a drain, or panics,
//! fails the process at once with an error status, so that whatever runs it starts it
//! afresh rather than leave it half working ([03 §2] in the docs): a worker whose sweep is
//! gone accepts on while none of its deadlines comes. That is HAProxy's default, whose
//! master ends every worker and itself when one fails. A connection's or a request's task
//! that panics ends that alone.
//!
//! An end is told by a guard the task or thread carries, as it is dropped: once it has
//! finished, or as it is thrown away unfinished — a task by its runtime once it panicked,
//! a thread as it unwinds.
//!
//! [03 §2]: ../../../docs/03-data-plane.md

use std::fmt;
use std::future::Future;
use std::sync::mpsc::Sender;

/// What the main loop is told.
#[derive(Debug)]
pub(crate) enum Told {
    /// To stop: a signal came.
    Stop,
    /// That something it cannot do without has ended.
    Ended(Ended),
}

/// Something essential that ended, and whether it finished or was thrown away unfinished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ended {
    pub(crate) what: String,
    pub(crate) finished: bool,
}

impl fmt::Display for Ended {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let how = if self.finished { "ended" } else { "panicked" };
        write!(f, "{} {how}", self.what)
    }
}

/// Carried by an essential task or thread: tells the main loop when it is dropped.
#[derive(Debug)]
pub(crate) struct Essential {
    what: String,
    tell: Sender<Told>,
    finished: bool,
}

impl Essential {
    /// A guard for `what`, telling `tell`.
    pub(crate) fn new(what: String, tell: &Sender<Told>) -> Self {
        Self {
            what,
            tell: tell.clone(),
            finished: false,
        }
    }

    /// What it watches has finished: so it is told when this is dropped.
    pub(crate) fn finished(&mut self) {
        self.finished = true;
    }
}

impl Drop for Essential {
    fn drop(&mut self) {
        let ended = Ended {
            what: std::mem::take(&mut self.what),
            finished: self.finished && !std::thread::panicking(),
        };
        // With nobody left to tell, the process is ending anyway.
        let _told = self.tell.send(Told::Ended(ended));
    }
}

/// `future`, watched: told as finished when it finishes, and as panicked when it is thrown
/// away before that.
pub(crate) async fn watched(future: impl Future<Output = ()>, mut essential: Essential) {
    future.await;
    essential.finished();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use tokio::runtime::Builder;
    use tokio::task::LocalSet;

    fn ended(told: &Receiver<Told>) -> Ended {
        match told.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(Told::Ended(ended)) => ended,
            other => panic!("told {other:?}"),
        }
    }

    #[test]
    fn a_task_that_finishes_is_told_as_ended() {
        let (tell, told) = mpsc::channel();
        let runtime = Builder::new_current_thread().build().unwrap();
        runtime.block_on(watched(
            async {},
            Essential::new("a task".to_owned(), &tell),
        ));
        let ended = ended(&told);
        assert_eq!(ended.to_string(), "a task ended");
        assert!(ended.finished);
    }

    /// A worker's tasks run in its `LocalSet`, which goes on after one of them panics: the
    /// guard is what says so.
    #[test]
    fn a_task_that_panics_is_told_as_panicked() {
        let (tell, told) = mpsc::channel();
        let runtime = Builder::new_current_thread().build().unwrap();
        LocalSet::new().block_on(&runtime, async {
            let task = tokio::task::spawn_local(watched(
                async { panic!("a panic the test makes") },
                Essential::new("a task".to_owned(), &tell),
            ));
            assert!(task.await.is_err_and(|error| error.is_panic()));
        });
        let ended = ended(&told);
        assert_eq!(ended.to_string(), "a task panicked");
        assert!(!ended.finished);
    }

    #[test]
    fn a_thread_that_panics_is_told_as_panicked_and_one_that_returns_as_ended() {
        let (tell, told) = mpsc::channel();
        let panicking = tell.clone();
        let joined = thread::spawn(move || {
            let _essential = Essential::new("a thread".to_owned(), &panicking);
            panic!("a panic the test makes");
        })
        .join();
        assert!(joined.is_err());
        assert!(!ended(&told).finished);

        thread::spawn(move || {
            let mut essential = Essential::new("a thread".to_owned(), &tell);
            essential.finished();
        })
        .join()
        .unwrap();
        assert!(ended(&told).finished);
    }
}
