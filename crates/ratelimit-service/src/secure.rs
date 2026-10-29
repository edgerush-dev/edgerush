//! Mutual TLS on the service (06 §4): who may call a pod, told apart by the SPIFFE ID in
//! the caller's certificate, the authorities trusted establishing only that it is one of
//! the cluster's workloads (12 item 22). A caller with the pod's own ID, which the
//! service's pods share, is a pod, and may only send heartbeats and copies; one with the
//! ID of the data plane it serves may only exchange, or in the synchronous mode decide;
//! any other is refused. Each end dials the other with the control stream's TLS
//! (`edgerush_control_service`): 1.3 alone, the pod dialled checked against the name
//! dialled and for the pods' ID.
//!
//! TLS is the operator's choice: a pod served plain takes any call from anyone, as before.

use boring::ssl::{SslAcceptor, SslConnector, SslRef};
use edgerush_certificates::{Identity, spiffe_id};
use edgerush_config::Certificate;
use edgerush_control_service::{Credentials, FilesError, Reread, TlsError, acceptor, connector_to};
use http::Uri;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_boring::SslStream;
use tonic::Status;
use tonic::server::NamedService;
use tonic::transport::server::Connected;

/// Who called, as a connection served with TLS tells each of its requests. A connection
/// served plain tells none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    /// The data plane served: its certificate carries that data plane's ID.
    DataPlane,
    /// A service pod: its certificate carries the pods' ID.
    Pod,
}

/// Refuses `request` unless its caller is `allowed`, or it says no caller, as a request on
/// a connection served plain does.
pub(crate) fn refused<B>(request: &http::Request<B>, allowed: Caller) -> Option<Status> {
    match request.extensions().get::<Caller>() {
        Some(caller) if *caller != allowed => Some(Status::permission_denied(match allowed {
            Caller::DataPlane => "only a data plane may call this",
            Caller::Pod => "only a service pod may call this",
        })),
        _ => None,
    }
}

/// What a pod presents, whom it trusts, and which data plane it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodCredentials {
    /// Its certificate, and the key: shown to data planes and to the other pods alike. The
    /// SPIFFE ID it carries is the pods', which they share.
    pub certificate: Certificate,
    /// The certificates, in PEM, of the authorities trusted to vouch for every caller and
    /// every pod dialled.
    pub authorities: String,
    /// The SPIFFE ID of the data plane it serves: its DataPlane's, which that DataPlane's
    /// pods share.
    pub data_plane: String,
}

/// A pod's TLS, made from its credentials: how it is dialled, how it tells its callers
/// apart, and how it dials the other pods.
pub struct PodTls {
    acceptor: SslAcceptor,
    /// The pods' ID, its own.
    pods: String,
    data_plane: String,
    connector: SslConnector,
}

impl PodTls {
    /// The TLS of `credentials`.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for credentials that cannot be used, among them a certificate without
    /// one SPIFFE ID, and a data plane's ID that is not one or is the pods' own.
    pub fn new(credentials: &PodCredentials) -> Result<Self, TlsError> {
        let leaf = Identity::read(&credentials.certificate)?.leaf;
        let pods = spiffe_id(&leaf).ok_or_else(|| {
            TlsError::Identity("the pod's certificate carries no one SPIFFE ID".to_owned())
        })?;
        let data_plane = &credentials.data_plane;
        if !data_plane.starts_with("spiffe://") {
            return Err(TlsError::Identity(format!(
                "the data plane's ID `{data_plane}` is not a SPIFFE ID"
            )));
        }
        // Then a data plane would be served as a pod.
        if *data_plane == pods {
            return Err(TlsError::Identity(
                "the data plane's ID is the pods' own".to_owned(),
            ));
        }
        let own = Credentials {
            certificate: credentials.certificate.clone(),
            authorities: credentials.authorities.clone(),
        };
        Ok(Self {
            acceptor: acceptor(&own)?,
            connector: connector_to(&own, &pods)?,
            pods,
            data_plane: data_plane.clone(),
        })
    }

    /// How it dials the other pods.
    #[must_use]
    pub fn connector(&self) -> &SslConnector {
        &self.connector
    }

    /// Who is on the other end of `ssl`, its handshake done: a pod or the data plane, by the
    /// SPIFFE ID its certificate carries; else why it is refused.
    fn caller(&self, ssl: &SslRef) -> Result<Caller, String> {
        // The acceptor has refused a caller without a certificate.
        let id = ssl.peer_certificate().as_deref().and_then(spiffe_id);
        match id {
            Some(id) if id == self.pods => Ok(Caller::Pod),
            Some(id) if id == self.data_plane => Ok(Caller::DataPlane),
            Some(id) => Err(format!("{id} is neither a pod nor the data plane served")),
            None => Err("the caller's certificate carries no one SPIFFE ID".to_owned()),
        }
    }
}

/// A data plane's TLS to the pods: `certificate` shown, carrying the data plane's ID, and
/// each pod checked against `authorities`, each in PEM, and for the pods' ID, `pods`.
///
/// # Errors
///
/// A [`TlsError`] for credentials that cannot be used.
pub fn data_plane(
    certificate: &Certificate,
    authorities: &[String],
    pods: &str,
) -> Result<SslConnector, TlsError> {
    let credentials = Credentials {
        certificate: certificate.clone(),
        authorities: authorities.join("\n"),
    };
    connector_to(&credentials, pods)
}

/// Where a pod's credentials are, each a PEM file, and the data plane it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodFiles {
    /// The certificate it presents, then the intermediates.
    pub certificate: PathBuf,
    /// Its private key.
    pub key: PathBuf,
    /// The authorities it trusts.
    pub authorities: PathBuf,
    /// The SPIFFE ID of the data plane it serves.
    pub data_plane: String,
}

impl PodFiles {
    /// What they hold now.
    ///
    /// # Errors
    ///
    /// A [`FilesError`] for a file that cannot be read. Whether what they hold can be used
    /// is for [`PodTls::new`] to say.
    pub async fn read(&self) -> Result<PodCredentials, FilesError> {
        Ok(PodCredentials {
            certificate: Certificate {
                chain: read(&self.certificate).await?,
                key: read(&self.key).await?,
            },
            authorities: read(&self.authorities).await?,
            data_plane: self.data_plane.clone(),
        })
    }
}

impl Reread for PodFiles {
    type Held = PodCredentials;

    fn held(&self) -> impl Future<Output = Result<PodCredentials, FilesError>> + Send {
        self.read()
    }
}

async fn read(path: &Path) -> Result<String, FilesError> {
    tokio::fs::read_to_string(path)
        .await
        .map_err(|error| FilesError {
            path: path.display().to_string(),
            error: error.to_string(),
        })
}

/// How long a caller has to finish its handshake.
const HANDSHAKE: Duration = Duration::from_secs(10);

/// How many connections, their handshakes done, wait for the server to take them.
const WAITING: usize = 16;

/// Serves `service` on `listener` with TLS until `stop`, each connection with the latest of
/// `tls`, so that certificates replaced serve the next, and each of its requests told who
/// called. A connection refused in its handshake goes to `refused`, with why.
///
/// # Errors
///
/// When gRPC's server fails.
pub async fn serve<S>(
    listener: TcpListener,
    tls: watch::Receiver<Arc<PodTls>>,
    service: S,
    refused: impl Fn(SocketAddr, String) + Send + Sync + 'static,
    stop: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error>
where
    S: tower_service::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let (secured, incoming) = mpsc::channel(WAITING);
    let accepting = tokio::spawn(accept(listener, tls, secured, Arc::new(refused)));
    let served = tonic::transport::Server::builder()
        .serve_with_incoming_shutdown(service, Incoming(incoming), stop)
        .await;
    accepting.abort();
    served
}

/// Takes each connection and its handshake, the handshakes apart, so that one slow caller
/// holds up no other.
async fn accept(
    listener: TcpListener,
    tls: watch::Receiver<Arc<PodTls>>,
    secured: mpsc::Sender<Result<Secured, io::Error>>,
    refused: Arc<dyn Fn(SocketAddr, String) + Send + Sync>,
) {
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            // A connection that failed before it was taken: the next is taken as usual.
            Err(_) => continue,
        };
        let _nodelay = socket.set_nodelay(true);
        let tls = tls.borrow().clone();
        let secured = secured.clone();
        let refused = refused.clone();
        tokio::spawn(async move {
            let handshake =
                tokio::time::timeout(HANDSHAKE, tokio_boring::accept(&tls.acceptor, socket));
            let stream = match handshake.await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return refused(peer, error.to_string()),
                Err(_) => return refused(peer, "the handshake took too long".to_owned()),
            };
            match tls.caller(stream.ssl()) {
                Ok(caller) => {
                    let _stopped = secured.send(Ok(Secured { stream, caller })).await;
                }
                Err(why) => refused(peer, why),
            }
        });
    }
}

/// The connections taken, as the stream tonic serves from.
struct Incoming(mpsc::Receiver<Result<Secured, io::Error>>);

impl futures_core::Stream for Incoming {
    type Item = Result<Secured, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// A caller's connection, its handshake done.
struct Secured {
    stream: SslStream<TcpStream>,
    caller: Caller,
}

impl Connected for Secured {
    type ConnectInfo = Caller;

    fn connect_info(&self) -> Caller {
        self.caller
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

/// Dials a pod: TCP, then TLS with the latest of its connector, the pod's certificate
/// checked against the host it is dialled by.
#[derive(Clone)]
pub(crate) struct Dialler {
    address: Arc<str>,
    name: Arc<str>,
    tls: watch::Receiver<SslConnector>,
    within: Duration,
}

impl Dialler {
    /// Dials `address` (`host:port`), its host being `name`, within `within`.
    pub(crate) fn new(
        address: &str,
        name: &str,
        tls: watch::Receiver<SslConnector>,
        within: Duration,
    ) -> Self {
        Self {
            address: Arc::from(address),
            name: Arc::from(name),
            tls,
            within,
        }
    }
}

type Dialled =
    Pin<Box<dyn Future<Output = Result<TokioIo<SslStream<TcpStream>>, io::Error>> + Send>>;

impl tower_service::Service<Uri> for Dialler {
    type Response = TokioIo<SslStream<TcpStream>>;
    type Error = io::Error;
    type Future = Dialled;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Dialled {
        let address = self.address.clone();
        let name = self.name.clone();
        let tls = self.tls.borrow().clone();
        let within = self.within;
        Box::pin(async move {
            let failed = |why: String| io::Error::other(why);
            let dial = async {
                let socket = TcpStream::connect(&*address).await?;
                socket.set_nodelay(true)?;
                let configured = tls.configure().map_err(|error| failed(error.to_string()))?;
                let secured = tokio_boring::connect(configured, &name, socket)
                    .await
                    .map_err(|error| failed(error.to_string()))?;
                Ok(TokioIo::new(secured))
            };
            tokio::time::timeout(within, dial)
                .await
                .map_err(|_| failed("dialling took too long".to_owned()))?
        })
    }
}

/// The host of `uri`, as a certificate names it: an IPv6 address without its brackets.
pub(crate) fn host(uri: &Uri) -> Option<&str> {
    uri.host()
        .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
}
