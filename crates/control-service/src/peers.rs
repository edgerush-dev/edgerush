//! The controller's replicas among themselves ([04 §2](../../../docs/04-control-plane.md)):
//! `edgerush.control.v1.Peers/Follow`, gRPC over mutual TLS on a listener of its own,
//! beside the stream's, whose acceptor takes the control plane's own identity alone
//! ([`acceptor_of`](crate::acceptor_of)). A replica follows another's snapshots, the
//! leader's: the latest as it asks, and each later one as it is taken.
//!
//! The follower says the config versions it reads as it asks, and each snapshot is written
//! for it at the older of its version and the served replica's, as a config is for a data
//! plane; one outside either's window is refused, saying why. A snapshot is sent in parts,
//! so that no size of one is too big to cross: the message limit bounds a part, not a
//! snapshot. What a snapshot holds is not known here: the caller's own function writes it,
//! and the follower is handed its bytes.

use crate::client::{Dialler, error_chain};
use crate::server::{Peer, WAITING, accept};
use crate::{Prost, Received, ServeError};
use boring::ssl::{SslAcceptor, SslConnector};
use edgerush_control_stream::{CONFIG_VERSION, ConfigVersion, OLDEST_CONFIG_VERSION};
use http::uri::PathAndQuery;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tonic::server::NamedService;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};

/// The service's name, as gRPC calls it.
pub const PEERS: &str = "edgerush.control.v1.Peers";
const FOLLOW: &str = "/edgerush.control.v1.Peers/Follow";

/// The most of a snapshot one part carries: well within gRPC's default limit on a message
/// taken, 4 MiB.
pub(crate) const PART: usize = 1 << 20;

/// How often a quiet connection is pinged, and how long an answer may take before the
/// other end is taken as gone.
const KEEPALIVE: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// What a follower asks with: the config versions it reads.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Follow {
    /// Its config version; 0, from one that does not say, is none written.
    #[prost(uint32, tag = "1")]
    pub(crate) config_version: u32,
    /// The oldest config version it still reads.
    #[prost(uint32, tag = "2")]
    pub(crate) oldest_config_version: u32,
}

/// A part of a snapshot, as it is sent: its bytes, and whether it is the snapshot's last.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Part {
    /// The snapshot's bytes from where the part before ended.
    #[prost(bytes = "vec", tag = "1")]
    pub(crate) bytes: Vec<u8>,
    /// Whether the snapshot ends with this part.
    #[prost(bool, tag = "2")]
    pub(crate) last: bool,
}

/// What a replica hears of the replicas that follow it, for its caller to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Followed {
    /// A connection refused in the handshake: no certificate the authorities vouch for, or
    /// not the control plane's own identity, or not TLS 1.3, or not in time.
    Refused(String),
    /// A follower asked, at its config version, and is written at another, older, if this
    /// replica is.
    Asked {
        /// Its config version.
        version: u32,
        /// The config version it is written.
        written: u32,
    },
    /// A follower is not served: its versions are outside this replica's window.
    Unserved(String),
    /// A follower's stream ended, and why.
    Closed(String),
}

/// A replica's snapshot written for a follower at a config version, or why it cannot be.
type Write<S> = dyn Fn(&S, ConfigVersion) -> Result<Vec<u8>, String> + Send + Sync;

/// Serves the replicas' service on `listener` until `stop`: each replica that dials it and
/// asks is sent the latest of `snapshots`, once there is one, and each later one, each as
/// `write` writes it for the follower's version, and what is heard of each goes to `heard`,
/// with its address. Each connection takes the latest of `tls`, which is to take the
/// control plane's own identity alone, so that certificates replaced serve the next.
/// Streams end as it stops.
///
/// # Errors
///
/// A [`ServeError`] if gRPC's server fails.
pub async fn serve_peers<S: Send + Sync + 'static>(
    listener: TcpListener,
    tls: watch::Receiver<Arc<SslAcceptor>>,
    snapshots: watch::Receiver<Option<Arc<S>>>,
    write: impl Fn(&S, ConfigVersion) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    heard: impl Fn(SocketAddr, Followed) + Send + Sync + 'static,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), ServeError> {
    let heard: Arc<dyn Fn(SocketAddr, Followed) + Send + Sync> = Arc::new(heard);
    // Nothing is sent on it: each stream ends once it is dropped.
    let (stopping, stopped) = watch::channel(());
    let (secured, incoming) = mpsc::channel(WAITING);
    let refused = {
        let heard = heard.clone();
        Arc::new(move |peer, why| heard(peer, Followed::Refused(why)))
    };
    let accepting = tokio::spawn(accept(listener, tls, secured, refused));
    let service = Peers {
        snapshots,
        write: Arc::new(write),
        stopped,
        heard,
    };
    let served = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .serve_with_incoming_shutdown(service, Received(incoming), async move {
            stop.await;
            // A follower's stream lasts as long as it keeps it: each is ended, so that the
            // server's own shutdown, which waits for them, can end.
            drop(stopping);
        })
        .await;
    accepting.abort();
    Ok(served?)
}

/// The service, as gRPC serves it.
struct Peers<S> {
    snapshots: watch::Receiver<Option<Arc<S>>>,
    write: Arc<Write<S>>,
    stopped: watch::Receiver<()>,
    heard: Arc<dyn Fn(SocketAddr, Followed) + Send + Sync>,
}

impl<S> Clone for Peers<S> {
    fn clone(&self) -> Self {
        Self {
            snapshots: self.snapshots.clone(),
            write: self.write.clone(),
            stopped: self.stopped.clone(),
            heard: self.heard.clone(),
        }
    }
}

impl<S> NamedService for Peers<S> {
    const NAME: &'static str = PEERS;
}

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<S: Send + Sync + 'static> tower_service::Service<http::Request<tonic::body::Body>>
    for Peers<S>
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let peers = self.clone();
        Box::pin(async move {
            Ok(match request.uri().path() {
                FOLLOW => {
                    let mut grpc = tonic::server::Grpc::new(Prost::<Part, Follow>::default());
                    grpc.server_streaming(Following(peers), request).await
                }
                _ => Status::unimplemented("no such method").into_http(),
            })
        })
    }
}

struct Following<S>(Peers<S>);

impl<S: Send + Sync + 'static> tonic::server::ServerStreamingService<Follow> for Following<S> {
    type Response = Part;
    type ResponseStream = Received<Result<Part, Status>>;
    type Future = Boxed<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Follow>) -> Self::Future {
        let peers = self.0.clone();
        Box::pin(async move {
            let address = request
                .extensions()
                .get::<Peer>()
                .map(|peer| peer.address)
                .ok_or_else(|| Status::internal("a connection without its address"))?;
            let follow = request.into_inner();
            let heard = peers.heard.clone();
            let (sent, sending) = mpsc::channel(WAITING);
            // Refused down the stream, as the config stream refuses: a call that fails is
            // the replica unreachable, as one whose certificate TLS 1.3 refuses only once
            // the call is made.
            let version = match written_for(&follow) {
                Ok(version) => version,
                Err(why) => {
                    heard(address, Followed::Unserved(why.clone()));
                    let _gone = sent.send(Err(Status::failed_precondition(why))).await;
                    return Ok(Response::new(Received(sending)));
                }
            };
            heard(
                address,
                Followed::Asked {
                    version: follow.config_version,
                    written: version.get(),
                },
            );
            tokio::spawn(async move {
                let closed = followed(peers, version, sent).await;
                heard(address, Followed::Closed(closed));
            });
            Ok(Response::new(Received(sending)))
        })
    }
}

/// The config version a follower that asked with `follow` is written: the older of its and
/// this replica's, if both still have it.
fn written_for(follow: &Follow) -> Result<ConfigVersion, String> {
    let written = follow.config_version.min(CONFIG_VERSION);
    if written < follow.oldest_config_version {
        return Err(format!(
            "the follower reads config version {} and after, and this replica writes \
             {CONFIG_VERSION} at most",
            follow.oldest_config_version
        ));
    }
    ConfigVersion::served(written).ok_or_else(|| {
        format!(
            "config version {} is no longer written: this replica writes \
             {OLDEST_CONFIG_VERSION} to {CONFIG_VERSION}",
            follow.config_version
        )
    })
}

/// Serves one follower, written at `version`, until either end ends it: why.
async fn followed<S>(
    peers: Peers<S>,
    version: ConfigVersion,
    sent: mpsc::Sender<Result<Part, Status>>,
) -> String {
    let Peers {
        mut snapshots,
        write,
        mut stopped,
        ..
    } = peers;
    let gone = || "the follower is gone".to_owned();
    loop {
        let snapshot = snapshots.borrow_and_update().clone();
        if let Some(snapshot) = snapshot {
            let written = match write(&snapshot, version) {
                Ok(written) => written,
                Err(why) => {
                    let why = format!("the snapshot cannot be written: {why}");
                    let _gone = sent.send(Err(Status::internal(why.clone()))).await;
                    return why;
                }
            };
            for part in parts(written) {
                if sent.send(Ok(part)).await.is_err() {
                    return gone();
                }
            }
        }
        tokio::select! {
            changed = snapshots.changed() => {
                if changed.is_err() {
                    let why = "the replica stopped taking snapshots";
                    let _gone = sent.send(Err(Status::unavailable(why))).await;
                    return why.to_owned();
                }
            }
            // Only ever an error, once the server stops: nothing is sent on it.
            _ = stopped.changed() => {
                let why = "the replica is stopping";
                let _gone = sent.send(Err(Status::unavailable(why))).await;
                return why.to_owned();
            }
            () = sent.closed() => return gone(),
        }
    }
}

/// `written` in parts of [`PART`] at most, the last marked: one, empty, for nothing.
fn parts(written: Vec<u8>) -> Vec<Part> {
    if written.len() <= PART {
        return vec![Part {
            bytes: written,
            last: true,
        }];
    }
    let count = written.len().div_ceil(PART);
    written
        .chunks(PART)
        .enumerate()
        .map(|(index, bytes)| Part {
            bytes: bytes.to_vec(),
            last: index + 1 == count,
        })
        .collect()
}

/// Why a replica stopped following another: it is dialled again by the caller, after a
/// wait.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unfollowed {
    /// The replica could not be reached, or its certificate was refused, or it refused
    /// this one's.
    #[error("the replica cannot be reached: {0}")]
    Unreachable(String),
    /// The replica refused to be followed, or ended the stream, saying why.
    #[error("the replica ended the stream: {0}")]
    Ended(String),
    /// The replica closed the stream.
    #[error("the replica closed the stream")]
    Closed,
}

/// Dials the replica at `address` (`host:port`), with `tls`, which is to take the control
/// plane's own identity alone ([`connector_to`](crate::connector_to)), its certificate
/// checked against `name` too, and follows its snapshots until the stream ends: each one
/// handed whole to `taken`, as the replica wrote it for this one's config version.
pub async fn follow_peer(
    address: &str,
    name: &str,
    tls: &SslConnector,
    taken: impl FnMut(Vec<u8>),
) -> Unfollowed {
    let follow = Follow {
        config_version: CONFIG_VERSION,
        oldest_config_version: OLDEST_CONFIG_VERSION,
    };
    follow_with(address, name, tls, follow, taken).await
}

/// As [`follow_peer`], asking with `follow`.
pub(crate) async fn follow_with(
    address: &str,
    name: &str,
    tls: &SslConnector,
    follow: Follow,
    mut taken: impl FnMut(Vec<u8>),
) -> Unfollowed {
    let unreachable = |why: String| Unfollowed::Unreachable(why);
    let endpoint = match Endpoint::from_shared(format!("http://{address}")) {
        Ok(endpoint) => endpoint,
        Err(error) => return unreachable(error.to_string()),
    };
    let channel = match endpoint
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect_with_connector(Dialler::new(address, name, tls))
        .await
    {
        Ok(channel) => channel,
        Err(error) => return unreachable(error_chain(&error)),
    };
    let mut grpc = tonic::client::Grpc::new(channel);
    if let Err(error) = grpc.ready().await {
        return unreachable(error_chain(&error));
    }
    let response = grpc
        .server_streaming(
            Request::new(follow),
            PathAndQuery::from_static(FOLLOW),
            Prost::<Follow, Part>::default(),
        )
        .await;
    let mut down = match response {
        Ok(response) => response.into_inner(),
        Err(status) => return unreachable(status.message().to_owned()),
    };
    let mut snapshot = Vec::new();
    loop {
        match down.message().await {
            Ok(Some(part)) => {
                snapshot.extend_from_slice(&part.bytes);
                if part.last {
                    taken(std::mem::take(&mut snapshot));
                }
            }
            // A snapshot cut short is none: what was taken before it stands.
            Ok(None) => return Unfollowed::Closed,
            Err(status) => return Unfollowed::Ended(status.message().to_owned()),
        }
    }
}

#[cfg(test)]
mod tests;
