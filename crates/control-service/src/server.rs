//! The control plane's side: the server data planes dial.

use crate::{Prost, Received, SERVICE, SUBSCRIBE};
use boring::ssl::SslAcceptor;
use edgerush_control_stream::{Held, MAX_MESSAGE, Offer, OpenError, Report, Session, wire};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_boring::SslStream;
use tonic::server::NamedService;
use tonic::transport::server::Connected;
use tonic::{Request, Response, Status, Streaming};

/// How long a data plane has to finish its handshake.
pub(crate) const HANDSHAKE: Duration = Duration::from_secs(10);

/// How often a quiet connection is pinged, and how long an answer may take before the data
/// plane is taken as gone.
const KEEPALIVE: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many messages wait to be sent down one stream: an offer's resources are sent one
/// after another, each waiting for room.
const WAITING: usize = 16;

/// What the control plane hears of a data plane, for its caller to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Heard {
    /// A connection refused in the handshake: no certificate the authorities vouch for, or
    /// not TLS 1.3, or not in time.
    Refused(String),
    /// A stream opened.
    Opened {
        /// What the data plane holds.
        held: Held,
        /// Its config version.
        version: u32,
        /// The config version it is written, older than its own if this control plane is.
        written: u32,
    },
    /// What the data plane runs, and what it refused.
    Reported(Report),
    /// The stream ended, and why, if not by the data plane closing it.
    Closed(Option<String>),
}

/// Why the server stopped other than as asked.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// gRPC's server failed.
    #[error("serving the config stream: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Serves the config stream on `listener` until `stop`: each data plane that dials it is
/// sent the latest of `offers`, and each later one, and what it says goes to `heard`, with
/// its address. Each connection takes the latest of `tls`, so that certificates replaced
/// serve the next. Streams end as it stops.
///
/// # Errors
///
/// A [`ServeError`] if gRPC's server fails.
pub async fn serve(
    listener: TcpListener,
    tls: watch::Receiver<Arc<SslAcceptor>>,
    offers: watch::Receiver<Option<Arc<Offer>>>,
    heard: impl Fn(SocketAddr, Heard) + Send + Sync + 'static,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), ServeError> {
    let heard: Arc<dyn Fn(SocketAddr, Heard) + Send + Sync> = Arc::new(heard);
    // Nothing is sent on it: each stream ends once it is dropped.
    let (stopping, stopped) = watch::channel(());
    let (secured, incoming) = mpsc::channel(WAITING);
    let accepting = tokio::spawn(accept(listener, tls, secured, heard.clone()));
    let service = Control {
        offers,
        stopped,
        heard,
    };
    let served = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .serve_with_incoming_shutdown(service, Received(incoming), async move {
            stop.await;
            // A stream lasts as long as its data plane keeps it: each is ended, so that
            // the server's own shutdown, which waits for them, can end.
            drop(stopping);
        })
        .await;
    accepting.abort();
    Ok(served?)
}

/// Takes each connection and its handshake, the handshakes apart, so that one slow data
/// plane holds up no other.
async fn accept(
    listener: TcpListener,
    tls: watch::Receiver<Arc<SslAcceptor>>,
    secured: mpsc::Sender<Result<Secured, io::Error>>,
    heard: Arc<dyn Fn(SocketAddr, Heard) + Send + Sync>,
) {
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            // A connection that failed before it was taken: the next is taken as usual.
            Err(_) => continue,
        };
        let _nodelay = socket.set_nodelay(true);
        let acceptor = tls.borrow().clone();
        let secured = secured.clone();
        let heard = heard.clone();
        tokio::spawn(async move {
            let handshake =
                tokio::time::timeout(HANDSHAKE, tokio_boring::accept(&acceptor, socket));
            match handshake.await {
                Ok(Ok(stream)) => {
                    let _stopped = secured.send(Ok(Secured { stream, peer })).await;
                }
                Ok(Err(error)) => heard(peer, Heard::Refused(error.to_string())),
                Err(_) => heard(
                    peer,
                    Heard::Refused("the handshake took too long".to_owned()),
                ),
            }
        });
    }
}

/// A data plane's connection, its handshake done.
struct Secured {
    stream: SslStream<TcpStream>,
    peer: SocketAddr,
}

/// Where a data plane dialled from, as each of its requests carries it.
#[derive(Debug, Clone, Copy)]
struct Peer(SocketAddr);

impl Connected for Secured {
    type ConnectInfo = Peer;

    fn connect_info(&self) -> Peer {
        Peer(self.peer)
    }
}

impl AsyncRead for Secured {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Secured {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// The service, as gRPC serves it.
#[derive(Clone)]
struct Control {
    offers: watch::Receiver<Option<Arc<Offer>>>,
    stopped: watch::Receiver<()>,
    heard: Arc<dyn Fn(SocketAddr, Heard) + Send + Sync>,
}

impl NamedService for Control {
    const NAME: &'static str = SERVICE;
}

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl tower_service::Service<http::Request<tonic::body::Body>> for Control {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let control = self.clone();
        Box::pin(async move {
            Ok(match request.uri().path() {
                SUBSCRIBE => {
                    let mut grpc = tonic::server::Grpc::new(Prost::<
                        wire::ToDataPlane,
                        wire::FromDataPlane,
                    >::default())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE);
                    grpc.streaming(Subscribe(control), request).await
                }
                _ => Status::unimplemented("no such method").into_http(),
            })
        })
    }
}

struct Subscribe(Control);

impl tonic::server::StreamingService<wire::FromDataPlane> for Subscribe {
    type Response = wire::ToDataPlane;
    type ResponseStream = Received<Result<wire::ToDataPlane, Status>>;
    type Future = Boxed<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Streaming<wire::FromDataPlane>>) -> Self::Future {
        let control = self.0.clone();
        Box::pin(async move {
            let peer = request
                .extensions()
                .get::<Peer>()
                .copied()
                .ok_or_else(|| Status::internal("a connection without its address"))?;
            let (sent, sending) = mpsc::channel(WAITING);
            tokio::spawn(async move {
                let heard = control.heard.clone();
                let closed = subscribed(control, peer.0, request.into_inner(), sent).await;
                heard(peer.0, Heard::Closed(closed));
            });
            Ok(Response::new(Received(sending)))
        })
    }
}

/// Serves one data plane's stream, from `peer`, until either end ends it: why, if not by
/// the data plane closing it.
async fn subscribed(
    control: Control,
    peer: SocketAddr,
    mut said: Streaming<wire::FromDataPlane>,
    sent: mpsc::Sender<Result<wire::ToDataPlane, Status>>,
) -> Option<String> {
    let Control {
        mut offers,
        mut stopped,
        heard,
    } = control;
    let first = match said.message().await {
        Ok(Some(first)) => first,
        Ok(None) => return None,
        Err(status) => return Some(status.message().to_owned()),
    };
    let mut session = match Session::open(first) {
        Ok(session) => session,
        Err(error @ (OpenError::DataPlaneTooOld { .. } | OpenError::ControlPlaneTooOld { .. })) => {
            return Some(refuse(&sent, Status::failed_precondition(error.to_string())).await);
        }
        Err(error) => {
            return Some(refuse(&sent, Status::invalid_argument(error.to_string())).await);
        }
    };
    heard(
        peer,
        Heard::Opened {
            held: session.held().clone(),
            version: session.reads(),
            written: session.version().get(),
        },
    );
    loop {
        // The latest offer, as far as the data plane does not hold it already.
        let offer = offers.borrow_and_update().clone();
        if let Some(offer) = offer {
            for message in session.send(&offer) {
                if sent.send(Ok(message)).await.is_err() {
                    return Some("the data plane is gone".to_owned());
                }
            }
        }
        tokio::select! {
            changed = offers.changed() => {
                if changed.is_err() {
                    return Some("the control plane stopped offering".to_owned());
                }
            }
            // Only ever an error, once the server stops: nothing is sent on it.
            _ = stopped.changed() => {
                let stopping = "the control plane is stopping";
                let _gone = sent.send(Err(Status::unavailable(stopping))).await;
                return Some(stopping.to_owned());
            }
            message = said.message() => match message {
                Ok(Some(message)) => match session.heard(message) {
                    Ok(Some(report)) => heard(peer, Heard::Reported(report)),
                    Ok(None) => {}
                    Err(malformed) => {
                        let why = Status::invalid_argument(malformed.to_string());
                        return Some(refuse(&sent, why).await);
                    }
                },
                Ok(None) => return None,
                Err(status) => return Some(status.message().to_owned()),
            },
        }
    }
}

/// Ends a stream the control plane does not serve: says why to the data plane, and returns
/// it.
async fn refuse(sent: &mpsc::Sender<Result<wire::ToDataPlane, Status>>, why: Status) -> String {
    let said = why.message().to_owned();
    let _gone = sent.send(Err(why)).await;
    said
}
