//! The control plane's side: the server data planes dial.

use crate::{Prost, Received, SERVICE, SUBSCRIBE};
use boring::ssl::SslAcceptor;
use edgerush_certificates::spiffe_id;
use edgerush_control_stream::{Held, MAX_MESSAGE, Offer, OpenError, Report, Session, wire};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
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

/// Which data planes the stream serves, and what each is offered: each known by the SPIFFE
/// ID its certificate carries, which alone chooses its config, nothing it says doing so
/// ([05 §3](../../../docs/05-operator.md)).
#[derive(Debug, Clone, Default)]
pub enum Served {
    /// None: every data plane is refused, as no DataPlane is served yet.
    #[default]
    Nobody,
    /// Every data plane the stream's authorities vouch for, each offered this, if anything
    /// yet: a settings file's data plane, which states no ID.
    Anyone(Option<Arc<Offer>>),
    /// The data plane whose certificate carries this ID alone, offered this, if anything yet.
    One {
        /// The ID.
        id: String,
        /// What is offered to it.
        offer: Option<Arc<Offer>>,
    },
}

/// Why a data plane is not served, as it is told.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unserved {
    /// Its certificate carries no SPIFFE ID, or more than one, which no DataPlane's does.
    #[error("the data plane's certificate carries no SPIFFE ID, or more than one")]
    NoId,
    /// Its certificate's ID is no DataPlane's that is served.
    #[error("{0} is no DataPlane this control plane serves")]
    NotServed(String),
}

impl Served {
    /// The data plane whose certificate carries `id`, or any where none, offered nothing yet.
    #[must_use]
    pub fn of(id: Option<&str>) -> Self {
        match id {
            Some(id) => Self::One {
                id: id.to_owned(),
                offer: None,
            },
            None => Self::Anyone(None),
        }
    }

    /// What is offered, to whichever data planes it serves.
    #[must_use]
    pub fn offer(&self) -> Option<&Arc<Offer>> {
        match self {
            Self::Nobody => None,
            Self::Anyone(offer) | Self::One { offer, .. } => offer.as_ref(),
        }
    }

    /// Made to serve the data planes `next` serves: left as it is, what it offers with it,
    /// where it serves them already, and `next` where not, so that nothing offered to one
    /// data plane is offered to another. Whether it changed.
    pub fn serve(&mut self, next: Self) -> bool {
        let same = match (&*self, &next) {
            (Self::Nobody, Self::Nobody) | (Self::Anyone(_), Self::Anyone(_)) => true,
            (Self::One { id, .. }, Self::One { id: next, .. }) => id == next,
            _ => false,
        };
        if !same {
            *self = next;
        }
        !same
    }

    /// `offer` offered to the data planes it serves, in place of what was; nothing where it
    /// serves none.
    pub fn offered(&mut self, offer: Arc<Offer>) {
        match self {
            Self::Nobody => {}
            Self::Anyone(offered) | Self::One { offer: offered, .. } => *offered = Some(offer),
        }
    }

    /// What the data plane whose certificate carries `id` is offered, if anything yet; or
    /// why it is not served.
    ///
    /// # Errors
    ///
    /// [`Unserved`] for a data plane not served.
    pub fn offer_to(&self, id: Option<&str>) -> Result<Option<&Arc<Offer>>, Unserved> {
        match (self, id) {
            (Self::Anyone(offer), _) => Ok(offer.as_ref()),
            (Self::One { id: served, offer }, Some(id)) if served == id => Ok(offer.as_ref()),
            (_, None) => Err(Unserved::NoId),
            (_, Some(id)) => Err(Unserved::NotServed(id.to_owned())),
        }
    }
}

/// Why the server stopped other than as asked.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// gRPC's server failed.
    #[error("serving the config stream: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Serves the config stream on `listener` until `stop`: each data plane that dials it is
/// sent the latest of `gateway`, the addresses of the gateway's own pods, and each later
/// one, then what the latest of `served` offers it, and each later one, and what it says
/// goes to `heard`, with its address. A data plane `served` does not serve is refused, with
/// why, as it dials or once `served` no longer serves it. Each connection takes the latest
/// of `tls`, so that certificates replaced serve the next. Streams end as it stops.
///
/// # Errors
///
/// A [`ServeError`] if gRPC's server fails.
pub async fn serve(
    listener: TcpListener,
    tls: watch::Receiver<Arc<SslAcceptor>>,
    served: watch::Receiver<Served>,
    gateway: watch::Receiver<Arc<[IpAddr]>>,
    heard: impl Fn(SocketAddr, Heard) + Send + Sync + 'static,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), ServeError> {
    let heard: Arc<dyn Fn(SocketAddr, Heard) + Send + Sync> = Arc::new(heard);
    // Nothing is sent on it: each stream ends once it is dropped.
    let (stopping, stopped) = watch::channel(());
    let (secured, incoming) = mpsc::channel(WAITING);
    let accepting = tokio::spawn(accept(listener, tls, secured, heard.clone()));
    let service = Control {
        served,
        gateway,
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
                    // Read once: its certificate is the handshake's, for the connection's life.
                    let id = stream
                        .ssl()
                        .peer_certificate()
                        .as_deref()
                        .and_then(spiffe_id);
                    let peer = Peer { address: peer, id };
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
    peer: Peer,
}

/// Where a data plane dialled from, and the ID its certificate carries, if one, as each of
/// its requests carries them.
#[derive(Debug, Clone)]
struct Peer {
    address: SocketAddr,
    id: Option<String>,
}

impl Connected for Secured {
    type ConnectInfo = Peer;

    fn connect_info(&self) -> Peer {
        self.peer.clone()
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
    served: watch::Receiver<Served>,
    gateway: watch::Receiver<Arc<[IpAddr]>>,
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
                .cloned()
                .ok_or_else(|| Status::internal("a connection without its address"))?;
            let (sent, sending) = mpsc::channel(WAITING);
            tokio::spawn(async move {
                let heard = control.heard.clone();
                let address = peer.address;
                let closed = subscribed(control, peer, request.into_inner(), sent).await;
                heard(address, Heard::Closed(closed));
            });
            Ok(Response::new(Received(sending)))
        })
    }
}

/// Serves one data plane's stream, from `peer`, until either end ends it: why, if not by
/// the data plane closing it.
async fn subscribed(
    control: Control,
    peer: Peer,
    mut said: Streaming<wire::FromDataPlane>,
    sent: mpsc::Sender<Result<wire::ToDataPlane, Status>>,
) -> Option<String> {
    let Control {
        mut served,
        mut gateway,
        mut stopped,
        heard,
    } = control;
    let Peer { address: peer, id } = peer;
    // Nothing is heard of a data plane not served, nor sent to it.
    let unserved = served.borrow().offer_to(id.as_deref()).err();
    if let Some(unserved) = unserved {
        return Some(refuse(&sent, Status::permission_denied(unserved.to_string())).await);
    }
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
    // Whether the gateway's addresses can still change: once whoever says them is gone,
    // the last are all there are, and the stream goes on.
    let mut saying = true;
    loop {
        // Whether it is served still, and what is offered to it.
        let offer = served
            .borrow_and_update()
            .offer_to(id.as_deref())
            .map(Option::<&Arc<Offer>>::cloned);
        let offer = match offer {
            Ok(offer) => offer,
            Err(unserved) => {
                return Some(refuse(&sent, Status::permission_denied(unserved.to_string())).await);
            }
        };
        // The gateway's addresses first, so that none is sent to by a config it runs.
        let addresses = Arc::clone(&gateway.borrow_and_update());
        if let Some(message) = session.gateway(&addresses)
            && sent.send(Ok(message)).await.is_err()
        {
            return Some("the data plane is gone".to_owned());
        }
        // The latest offer, as far as the data plane does not hold it already.
        if let Some(offer) = offer {
            for message in session.send(&offer) {
                if sent.send(Ok(message)).await.is_err() {
                    return Some("the data plane is gone".to_owned());
                }
            }
        }
        tokio::select! {
            changed = served.changed() => {
                if changed.is_err() {
                    return Some("the control plane stopped offering".to_owned());
                }
            }
            changed = gateway.changed(), if saying => {
                saying = changed.is_ok();
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
