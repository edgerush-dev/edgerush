//! Mutual TLS on the service (06 §4): who may call a pod, told apart by the authority that
//! signed the caller's certificate. A data plane, its certificate signed by the data
//! planes' authorities, may only exchange, or in the synchronous mode decide; a pod, its
//! signed by the pods', may only send heartbeats and copies. Each end dials the other with
//! the control stream's TLS (`edgerush_control_service`): 1.3 alone, the pod dialled
//! checked against the name dialled and the pods' authorities alone.
//!
//! TLS is the operator's choice: a pod served plain takes any call from anyone, as before.

use boring::error::ErrorStack;
use boring::ssl::{SslAcceptor, SslConnector, SslRef};
use boring::stack::Stack;
use boring::x509::store::{X509Store, X509StoreBuilder};
use boring::x509::{X509, X509StoreContext};
use edgerush_config::Certificate;
use edgerush_control_service::{Credentials, FilesError, Reread, TlsError, acceptor, connector};
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
    /// A data plane: its certificate signed by the data planes' authorities.
    DataPlane,
    /// A service pod: its certificate signed by the pods'.
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

/// What a pod presents, and whom it trusts to vouch for each kind of caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodCredentials {
    /// Its certificate, and the key: shown to data planes and to the other pods alike.
    pub certificate: Certificate,
    /// The certificates, in PEM, of the pods' authorities.
    pub pods: String,
    /// The certificates, in PEM, of the data planes' authorities: none of them one of the
    /// pods', or a data plane could pass for a pod.
    pub data_planes: String,
}

/// A pod's TLS, made from its credentials: how it is dialled, how it tells its callers
/// apart, and how it dials the other pods.
pub struct PodTls {
    acceptor: SslAcceptor,
    pods: X509Store,
    connector: SslConnector,
}

impl PodTls {
    /// The TLS of `credentials`.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for credentials that cannot be used, among them an authority that is
    /// both the pods' and the data planes'.
    pub fn new(credentials: &PodCredentials) -> Result<Self, TlsError> {
        let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
        let read = |pem: &str| {
            X509::stack_from_pem(pem.as_bytes())
                .map_err(|error| TlsError::Authorities(error.to_string()))
        };
        let pods = read(&credentials.pods)?;
        let data_planes = read(&credentials.data_planes)?;
        for authority in &data_planes {
            for pod in &pods {
                if authority.to_der().map_err(setup)? == pod.to_der().map_err(setup)? {
                    return Err(TlsError::Authorities(
                        "an authority is both the pods' and the data planes'".to_owned(),
                    ));
                }
            }
        }
        let own = |authorities: String| Credentials {
            certificate: credentials.certificate.clone(),
            authorities,
        };
        let either = format!("{}\n{}", credentials.pods, credentials.data_planes);
        let mut trusted = X509StoreBuilder::new().map_err(setup)?;
        for pod in pods {
            trusted.add_cert(pod).map_err(setup)?;
        }
        Ok(Self {
            acceptor: acceptor(&own(either))?,
            pods: trusted.build(),
            connector: connector(&own(credentials.pods.clone()))?,
        })
    }

    /// How it dials the other pods.
    #[must_use]
    pub fn connector(&self) -> &SslConnector {
        &self.connector
    }

    /// Who is on the other end of `ssl`, its handshake done: a pod if the pods' authorities
    /// alone vouch for its certificate, else a data plane, as the handshake found one of
    /// the two did.
    fn caller(&self, ssl: &SslRef) -> Result<Caller, ErrorStack> {
        let Some(leaf) = ssl.peer_certificate() else {
            // The acceptor refuses a caller without one; this is not reached.
            return Ok(Caller::DataPlane);
        };
        // Served, the chain leaves the leaf out.
        let none = Stack::new()?;
        let chain = ssl.peer_cert_chain().unwrap_or(&none);
        let mut context = X509StoreContext::new()?;
        let pod = context.init(&self.pods, &leaf, chain, |context| context.verify_cert())?;
        Ok(if pod { Caller::Pod } else { Caller::DataPlane })
    }
}

/// Where a pod's credentials are: each a PEM file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodFiles {
    /// The certificate it presents, then the intermediates.
    pub certificate: PathBuf,
    /// Its private key.
    pub key: PathBuf,
    /// The pods' authorities.
    pub pods: PathBuf,
    /// The data planes' authorities.
    pub data_planes: PathBuf,
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
            pods: read(&self.pods).await?,
            data_planes: read(&self.data_planes).await?,
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
                Err(error) => refused(peer, error.to_string()),
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
