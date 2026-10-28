//! The data plane's side: the stream it dials, and follows.

use crate::{Prost, Received, SUBSCRIBE};
use boring::ssl::SslConnector;
use edgerush_config::Config;
use edgerush_control_stream::{MAX_MESSAGE, Malformed, Report, Staging, Taken, wire};
use http::Uri;
use http::uri::PathAndQuery;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_boring::SslStream;
use tonic::Request;
use tonic::transport::Endpoint;

/// How long dialling may take, the handshake with it.
const DIAL: Duration = Duration::from_secs(5);

/// How often a quiet connection is pinged, and how long an answer may take before the
/// control plane is taken as gone.
const KEEPALIVE: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a stream followed ended: it is dialled again by the caller, after a wait.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Ended {
    /// The control plane could not be reached, or its certificate was refused, or it
    /// refused the data plane's.
    #[error("the control plane cannot be reached: {0}")]
    Unreachable(String),
    /// The control plane ended the stream, saying why.
    #[error("the control plane ended the stream: {0}")]
    Ended(String),
    /// The control plane closed the stream.
    #[error("the control plane closed the stream")]
    Closed,
    /// The control plane sent what the protocol does not allow.
    #[error("the control plane sent what the protocol does not allow: {0}")]
    Malformed(#[from] Malformed),
}

/// Dials the control plane at `address` (`host:port`), with `tls`, its certificate
/// checked against `name`, and follows the stream until it ends: from what `staging`
/// holds, each config given to run handed to `run`, which runs it, or says why it cannot,
/// and each outcome reported.
pub async fn follow<R>(
    address: &str,
    name: &str,
    tls: &SslConnector,
    staging: &mut Staging,
    mut run: impl FnMut(Box<Config>) -> R,
) -> Ended
where
    R: Future<Output = Result<(), String>>,
{
    let unreachable = |why: String| Ended::Unreachable(why);
    let endpoint = match Endpoint::from_shared(format!("http://{address}")) {
        Ok(endpoint) => endpoint,
        Err(error) => return unreachable(error.to_string()),
    };
    let dialler = Dialler::new(address, name, tls);
    let channel = match endpoint
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect_with_connector(dialler)
        .await
    {
        Ok(channel) => channel,
        Err(error) => return unreachable(error_chain(&error)),
    };
    let mut grpc = tonic::client::Grpc::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    if let Err(error) = grpc.ready().await {
        return unreachable(error_chain(&error));
    }
    let (up, sending) = mpsc::channel(4);
    if up.send(staging.hello()).await.is_err() {
        return Ended::Closed;
    }
    let response = grpc
        .streaming(
            Request::new(Received(sending)),
            PathAndQuery::from_static(SUBSCRIBE),
            Prost::<wire::FromDataPlane, wire::ToDataPlane>::default(),
        )
        .await;
    let mut down = match response {
        Ok(response) => response.into_inner(),
        Err(status) => return unreachable(status.message().to_owned()),
    };
    let say = async |report: Report| up.send((&report).into()).await.is_ok();
    loop {
        let message = match down.message().await {
            Ok(Some(message)) => message,
            Ok(None) => return Ended::Closed,
            Err(status) => return Ended::Ended(status.message().to_owned()),
        };
        let report = match staging.take(message) {
            Err(malformed) => return malformed.into(),
            Ok(Taken::Nothing) => continue,
            Ok(Taken::Say(report)) => Some(report),
            // Awaited, not waited on: the connection, its pings answered, goes on meanwhile.
            Ok(Taken::Run(config)) => match run(config).await {
                Ok(()) => staging.ran(),
                Err(why) => staging.failed(why),
            },
        };
        if let Some(report) = report
            && !say(report).await
        {
            return Ended::Closed;
        }
    }
}

/// An error and what caused it, as one line: tonic's own say little more than "transport
/// error". A cause that says only what the error before it said is left out: a wrapper
/// that shows its cause's message as its own, and then has it for its cause, as the
/// connector's error does, would have it said twice.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut said = error.to_string();
    let mut last = said.clone();
    let mut cause = error.source();
    while let Some(error) = cause {
        let this = error.to_string();
        if this != last {
            said.push_str(": ");
            said.push_str(&this);
        }
        last = this;
        cause = error.source();
    }
    said
}

/// Dials the control plane: TCP, then TLS checked against its name.
#[derive(Clone)]
pub(crate) struct Dialler {
    address: Arc<str>,
    name: Arc<str>,
    tls: SslConnector,
}

impl Dialler {
    /// Dials `address`, checking the certificate against `name`.
    pub(crate) fn new(address: &str, name: &str, tls: &SslConnector) -> Self {
        Self {
            address: Arc::from(address),
            name: Arc::from(name),
            tls: tls.clone(),
        }
    }
}

type Dialled =
    Pin<Box<dyn Future<Output = Result<TokioIo<SslStream<TcpStream>>, std::io::Error>> + Send>>;

impl tower_service::Service<Uri> for Dialler {
    type Response = TokioIo<SslStream<TcpStream>>;
    type Error = std::io::Error;
    type Future = Dialled;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Dialled {
        let dialler = self.clone();
        Box::pin(async move {
            let failed = |why: String| std::io::Error::other(why);
            let dial = async {
                let socket = TcpStream::connect(&*dialler.address).await?;
                socket.set_nodelay(true)?;
                let configured = dialler
                    .tls
                    .configure()
                    .map_err(|error| failed(error.to_string()))?;
                let secured = tokio_boring::connect(configured, &dialler.name, socket)
                    .await
                    .map_err(|error| failed(error.to_string()))?;
                Ok(TokioIo::new(secured))
            };
            tokio::time::timeout(DIAL, dial)
                .await
                .map_err(|_| failed("dialling took too long".to_owned()))?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::error_chain;

    /// An error of its own, with a cause.
    #[derive(Debug)]
    struct Caused(&'static str, Option<Box<Caused>>);

    impl std::fmt::Display for Caused {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Caused {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|cause| cause as _)
        }
    }

    #[test]
    fn each_cause_is_said_once() {
        let failed = Caused("dialling took too long", None);
        // A wrapper that says what its cause says.
        let wrapped = Caused("dialling took too long", Some(Box::new(failed)));
        assert_eq!(
            error_chain(&Caused("transport error", Some(Box::new(wrapped)))),
            "transport error: dialling took too long"
        );
        let different = Caused("refused", Some(Box::new(Caused("no route", None))));
        assert_eq!(error_chain(&different), "refused: no route");
        assert_eq!(error_chain(&Caused("alone", None)), "alone");
    }
}
