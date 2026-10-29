//! `edgerush proxy --control-plane`: the config stream ([02 §4](../../../docs/02-architecture.md)),
//! followed on a thread and runtime of its own for as long as the process runs, as the
//! health checks are; each config it is given to run handed to the main loop, which says
//! whether it runs, and what happens along the way said ([03 §10](../../../docs/03-data-plane.md)).

use crate::essential::{Essential, Told};
use edgerush_config::Config;
use edgerush_control_service::{
    Backoff, Credentials, Files, Following, Subscription, connector, subscribe,
};
use edgerush_control_stream::Staging;
use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;

/// How often the certificate files are read again.
const REREAD: Duration = Duration::from_secs(1);

/// Where the control plane is, and what the data plane presents to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stream {
    /// `host:port`; its certificate must name the host.
    pub(crate) address: String,
    pub(crate) files: Files,
}

/// What the stream's thread hands the main loop.
pub(crate) enum FromStream {
    /// A config to run: the main loop says on `ran` whether it runs, and why not, which
    /// the stream's thread awaits, its connection going on meanwhile.
    Run {
        config: Box<Config>,
        ran: oneshot::Sender<Result<(), String>>,
    },
    /// A line to say.
    Said(String),
    /// The addresses of the gateway's own pods, to refuse from now on.
    Gateway(Vec<std::net::IpAddr>),
}

/// Why the stream cannot be followed at all.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamError {
    #[error("the config stream's certificates: {0}")]
    Certificates(String),
    #[error("cannot start the config stream's runtime: {0}")]
    Runtime(io::Error),
}

/// Starts following `stream`: what it hands the main loop comes on what is returned, and
/// `tell` is told if its thread ends.
pub(crate) fn start(
    stream: &Stream,
    tell: &Sender<Told>,
) -> Result<Receiver<FromStream>, StreamError> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(StreamError::Runtime)?;
    let unusable = |error: String| StreamError::Certificates(error);
    let credentials: Credentials = runtime
        .block_on(stream.files.read())
        .map_err(|error| unusable(error.to_string()))?;
    connector(&credentials).map_err(|error| unusable(error.to_string()))?;
    let subscription = Subscription {
        address: stream.address.clone(),
        files: stream.files.clone(),
        credentials,
        reread: REREAD,
        backoff: Backoff::default(),
    };
    let (handed, handing) = mpsc::channel();
    let tell = tell.clone();
    thread::Builder::new()
        .name("config stream".to_owned())
        .spawn(move || {
            let mut essential = Essential::new("the config stream".to_owned(), &tell);
            let address = subscription.address.clone();
            let said = handed.clone();
            let gateway = handed.clone();
            let mut staging = Staging::new();
            let followed = runtime.block_on(subscribe(
                subscription,
                &mut staging,
                |config| ran(&handed, config),
                |addresses| {
                    let _gone = gateway.send(FromStream::Gateway(addresses));
                },
                |following| {
                    let _gone = said.send(FromStream::Said(told(&address, &following)));
                },
            ));
            // It only ever ends with the credentials it started with refused, which were
            // checked before; said, and the data plane fails with it.
            let Err(error) = followed;
            let _gone = said.send(FromStream::Said(format!("the config stream: {error}")));
            essential.finished();
        })
        .map_err(StreamError::Runtime)?;
    Ok(handing)
}

/// Hands `config` to the main loop, and awaits whether it runs: awaited rather than waited
/// on, so that the connection, its pings answered, goes on while the main loop starts the
/// data plane, which can wait 15 s for its first EndpointSlices.
async fn ran(handed: &Sender<FromStream>, config: Box<Config>) -> Result<(), String> {
    let stopping = || "the data plane is stopping".to_owned();
    let (ran, running) = oneshot::channel();
    handed
        .send(FromStream::Run { config, ran })
        .map_err(|_| stopping())?;
    running.await.unwrap_or_else(|_| Err(stopping()))
}

/// What is said of `following`, the control plane being at `address`.
fn told(address: &str, following: &Following) -> String {
    match following {
        Following::Ended { ended, wait } => format!(
            "control plane {address}: {ended}; dialled again in {} ms",
            wait.as_millis()
        ),
        Following::Replaced => "the config stream's certificates are replaced".to_owned(),
        Following::Unusable(why) => {
            format!("the config stream's certificates cannot be used, and the last are kept: {why}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_control_service::Ended;

    #[test]
    fn what_happens_along_the_stream_is_said_with_the_control_plane_it_is_of() {
        let ended = Following::Ended {
            ended: Ended::Closed,
            wait: Duration::from_millis(1234),
        };
        assert_eq!(
            told("control:9443", &ended),
            "control plane control:9443: the control plane closed the stream; dialled again in 1234 ms"
        );
        assert_eq!(
            told("control:9443", &Following::Replaced),
            "the config stream's certificates are replaced"
        );
        assert_eq!(
            told("control:9443", &Following::Unusable("why".to_owned())),
            "the config stream's certificates cannot be used, and the last are kept: why"
        );
    }
}
