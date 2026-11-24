//! Mutual TLS on the service (06 §4): who may call a pod, told apart by the SPIFFE ID in
//! the caller's certificate, the authorities trusted establishing only that it is one of
//! the cluster's workloads (12 item 22). A caller with the pod's own ID, which the
//! service's pods share, is a pod, and may only send heartbeats and copies; one with a data
//! plane's ID of the pod's trust domain, any of its ControlPlane's DataPlanes, may only
//! exchange, or in the synchronous mode decide; one with its controller's may only ask what
//! the pod has applied and name those to form; any other is refused. Each end dials the
//! other with the control stream's TLS (`edgerush_control_service`): 1.3 alone, the pod
//! dialled checked for the pods' ID, and against the name dialled where it is dialled by
//! name: a pod dialled at its own address, its IP, is checked for the ID alone, as the pods
//! share one certificate, made before any pod has an address.
//!
//! TLS is the operator's choice: a pod served plain takes any call from anyone, as before.
//!
//! Served either way, a connection that cannot be taken, out of files most likely, is
//! tried again only after a pause (`edgerush_control_service::accept`), not straight away:
//! tonic's own listener retries at once, and would spin a core until a file frees.

use boring::ssl::{SslAcceptor, SslConnector, SslRef};
use edgerush_certificates::{Identity, spiffe_id};
use edgerush_config::Certificate;
use edgerush_control_service::{
    Credentials, FilesError, Reread, TlsError, accept, acceptor, connector_to,
};
use http::Uri;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_boring::SslStream;
use tonic::Status;
use tonic::server::NamedService;
use tonic::transport::server::Connected;

/// Who called, as a connection served with TLS tells each of its requests. A connection
/// served plain tells none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    /// A data plane of the pod's ControlPlane: its certificate carries a data plane's ID of
    /// the pod's trust domain.
    DataPlane,
    /// A service pod: its certificate carries the pods' ID.
    Pod,
    /// The pod's ControlPlane's controller, which forms the store: its certificate carries
    /// the controller's ID of the pod's trust domain.
    Controller,
}

/// What a data plane's ServiceAccount, and so its SPIFFE ID's last part, starts with: the
/// operator names it `edgerush-proxy-<dataplane>` (05 §2).
pub const DATA_PLANE_ACCOUNT: &str = "edgerush-proxy-";

/// What a controller's ServiceAccount starts with: `edgerush-controller-<controlplane>`.
pub const CONTROLLER_ACCOUNT: &str = "edgerush-controller-";

/// The path of SPIFFE ID `id` past trust domain `domain`, if it is of that domain.
fn within<'a>(id: &'a str, domain: &str) -> Option<&'a str> {
    id.strip_prefix("spiffe://")?
        .strip_prefix(domain)?
        .strip_prefix('/')
}

/// Whose SPIFFE ID `id` is, of trust domain `domain`, by the ServiceAccount it names
/// (`spiffe://<domain>/ns/<namespace>/sa/<account>`, 12 item 22): a data plane's or the
/// controller's, in whatever namespace; none for any other.
#[must_use]
pub fn role(id: &str, domain: &str) -> Option<Caller> {
    let mut parts = within(id, domain)?.split('/');
    let (Some("ns"), Some(namespace), Some("sa"), Some(account), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    let named = |prefix: &str| {
        account
            .strip_prefix(prefix)
            .is_some_and(|name| !name.is_empty())
    };
    if namespace.is_empty() {
        None
    } else if named(DATA_PLANE_ACCOUNT) {
        Some(Caller::DataPlane)
    } else if named(CONTROLLER_ACCOUNT) {
        Some(Caller::Controller)
    } else {
        None
    }
}

/// Refuses `request` unless its caller is `allowed`, or it says no caller, as a request on
/// a connection served plain does.
pub(crate) fn refused<B>(request: &http::Request<B>, allowed: Caller) -> Option<Status> {
    match request.extensions().get::<Caller>() {
        Some(caller) if *caller != allowed => Some(Status::permission_denied(match allowed {
            Caller::DataPlane => "only a data plane may call this",
            Caller::Pod => "only a service pod may call this",
            Caller::Controller => "only the controller may call this",
        })),
        _ => None,
    }
}

/// What a pod presents, whom it trusts, and the trust domain its callers are of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodCredentials {
    /// Its certificate, and the key: shown to data planes and to the other pods alike. The
    /// SPIFFE ID it carries is the pods', which they share.
    pub certificate: Certificate,
    /// The certificates, in PEM, of the authorities trusted to vouch for every caller and
    /// every pod dialled.
    pub authorities: String,
    /// The trust domain of its ControlPlane, whose data planes and controller call it, and
    /// of which its own ID is.
    pub trust_domain: String,
}

/// A pod's TLS, made from its credentials: how it is dialled, how it tells its callers
/// apart, and how it dials the other pods.
pub struct PodTls {
    acceptor: SslAcceptor,
    /// The pods' ID, its own.
    pods: String,
    trust_domain: String,
    connector: SslConnector,
}

impl PodTls {
    /// The TLS of `credentials`.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for credentials that cannot be used, among them a certificate without
    /// one SPIFFE ID, a trust domain that is none, and an ID of the pods' that is not of it
    /// or is a data plane's or the controller's.
    pub fn new(credentials: &PodCredentials) -> Result<Self, TlsError> {
        let leaf = Identity::read(&credentials.certificate)?.leaf;
        let pods = spiffe_id(&leaf).ok_or_else(|| {
            TlsError::Identity("the pod's certificate carries no one SPIFFE ID".to_owned())
        })?;
        let domain = &credentials.trust_domain;
        if domain.is_empty() || domain.contains('/') {
            return Err(TlsError::Identity(format!(
                "the trust domain `{domain}` is not one"
            )));
        }
        if within(&pods, domain).is_none() {
            return Err(TlsError::Identity(format!(
                "the pods' ID `{pods}` is not of the trust domain `{domain}`"
            )));
        }
        // Then a data plane, or the controller, would be served as a pod.
        if role(&pods, domain).is_some() {
            return Err(TlsError::Identity(format!(
                "the pods' ID `{pods}` is a data plane's or the controller's"
            )));
        }
        let own = Credentials {
            certificate: credentials.certificate.clone(),
            authorities: credentials.authorities.clone(),
        };
        Ok(Self {
            acceptor: acceptor(&own)?,
            connector: connector_to(&own, &pods)?,
            pods,
            trust_domain: domain.clone(),
        })
    }

    /// How it dials the other pods.
    #[must_use]
    pub fn connector(&self) -> &SslConnector {
        &self.connector
    }

    /// Who is on the other end of `ssl`, its handshake done: a pod, a data plane or the
    /// controller, by the SPIFFE ID its certificate carries; else why it is refused.
    fn caller(&self, ssl: &SslRef) -> Result<Caller, String> {
        // The acceptor has refused a caller without a certificate.
        let id = ssl.peer_certificate().as_deref().and_then(spiffe_id);
        match id {
            Some(id) if id == self.pods => Ok(Caller::Pod),
            Some(id) => role(&id, &self.trust_domain).ok_or_else(|| {
                format!(
                    "{id} is neither a pod, a data plane nor the controller of `{}`",
                    self.trust_domain
                )
            }),
            None => Err("the caller's certificate carries no one SPIFFE ID".to_owned()),
        }
    }
}

/// A data plane's TLS to the pods, or the controller's: `certificate` shown, carrying its
/// ID, and each pod checked against `authorities`, each in PEM, and for the pods' ID, `pods`.
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

/// The controller's TLS to the pods, as it forms the store: as a data plane's, `certificate`
/// carrying the controller's ID.
///
/// # Errors
///
/// A [`TlsError`] for credentials that cannot be used.
pub fn controller(
    certificate: &Certificate,
    authorities: &[String],
    pods: &str,
) -> Result<SslConnector, TlsError> {
    data_plane(certificate, authorities, pods)
}

/// Where a pod's credentials are, each a PEM file, and the trust domain its callers are of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodFiles {
    /// The certificate it presents, then the intermediates.
    pub certificate: PathBuf,
    /// Its private key.
    pub key: PathBuf,
    /// The authorities it trusts.
    pub authorities: PathBuf,
    /// The trust domain of its ControlPlane.
    pub trust_domain: String,
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
            trust_domain: self.trust_domain.clone(),
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

/// How many connections taken, with TLS their handshakes done, wait for the server to take
/// them.
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
        let (socket, peer) = accept::next(|| listener.accept()).await;
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

/// Serves `service` on `listener` plain until `stop`: any call from anyone, its requests
/// told no caller.
///
/// # Errors
///
/// When gRPC's server fails.
pub async fn serve_plain<S>(
    listener: TcpListener,
    service: S,
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
    let (taken, incoming) = mpsc::channel(WAITING);
    let accepting = tokio::spawn(async move {
        loop {
            let (socket, _peer) = accept::next(|| listener.accept()).await;
            let _nodelay = socket.set_nodelay(true);
            if taken.send(Ok(socket)).await.is_err() {
                return;
            }
        }
    });
    let served = tonic::transport::Server::builder()
        .serve_with_incoming_shutdown(service, Incoming(incoming), stop)
        .await;
    accepting.abort();
    served
}

/// The connections taken, as the stream tonic serves from.
struct Incoming<T>(mpsc::Receiver<Result<T, io::Error>>);

impl<T> futures_core::Stream for Incoming<T> {
    type Item = Result<T, io::Error>;

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
/// checked for the pods' ID and, where it is dialled by name, against the name: one dialled
/// at an IP, its own address, is checked for the ID alone, as every pod shares one
/// certificate, made before any has an address (12 item 22).
#[derive(Clone)]
pub(crate) struct Dialler {
    address: Arc<str>,
    name: Arc<str>,
    tls: watch::Receiver<SslConnector>,
    within: Duration,
    from: Option<IpAddr>,
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
            from: None,
        }
    }

    /// Dialling from `from`, the system's choice if `None`.
    pub(crate) const fn from(mut self, from: Option<IpAddr>) -> Self {
        self.from = from;
        self
    }
}

/// A connection to `address` (`host:port`), from `from` if given.
async fn connect(address: &str, from: Option<IpAddr>) -> io::Result<TcpStream> {
    let Some(from) = from else {
        return TcpStream::connect(address).await;
    };
    let to = tokio::net::lookup_host(address)
        .await?
        .find(|to| to.is_ipv4() == from.is_ipv4())
        .ok_or_else(|| io::Error::other("no address of the dialling one's family"))?;
    let socket = if from.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.bind(SocketAddr::new(from, 0))?;
    socket.connect(to).await
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
        let from = self.from;
        Box::pin(async move {
            let failed = |why: String| io::Error::other(why);
            let dial = async {
                let socket = connect(&address, from).await?;
                socket.set_nodelay(true)?;
                let mut configured = tls.configure().map_err(|error| failed(error.to_string()))?;
                if name.parse::<IpAddr>().is_ok() {
                    configured.set_verify_hostname(false);
                }
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
