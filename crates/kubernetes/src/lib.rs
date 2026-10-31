//! EdgeRush's client of the Kubernetes API: it lists and watches the kinds EdgeRush reads,
//! and does nothing else. The data plane follows the EndpointSlices of the namespaces its
//! upstreams name ([02 §4](../../../docs/02-architecture.md),
//! [03 §6](../../../docs/03-data-plane.md)); the control plane, Ingresses, IngressClasses,
//! Services and Secrets ([04 §1](../../../docs/04-control-plane.md)). [`Kind`] says what is
//! asked of each, [`Watch`] what to ask next, and [`follow`] keeps asking.
//!
//! In a cluster it is [`ApiServer::in_cluster`]: the API server the pod's environment names,
//! as Kubernetes' own clients find it, its certificate checked against the address it is
//! reached by and trusted only if the service account's authority signed it; and the
//! service account's token, read again a minute after it was last read, as the kubelet
//! replaces it before it expires. A token that cannot be read again keeps the last one.
//!
//! Every list and watch goes over one HTTP/2 connection, opened when first needed and again
//! once it has failed, as Kubernetes' own clients share theirs. Every answer is bounded: a
//! list's page in size and time, a watch's event in size, a watch in how long it may go
//! without a byte.

use boring::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use bytes::{Bytes, BytesMut};
use h2::RecvStream;
use h2::client::SendRequest;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use http::{Method, StatusCode};
use k8s_openapi::List;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Status, WatchEvent};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};

/// Where a pod's service account is mounted: its token and its cluster's authority.
pub const SERVICE_ACCOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// The most a list's page may be.
pub const MOST_PAGE_BYTES: usize = 32 << 20;

/// The most one event of a watch may be: etcd keeps an object of at most 1.5 MiB, some
/// 2 MiB as JSON.
pub const MOST_EVENT_BYTES: usize = 4 << 20;

/// The most of an error's body that is read for its message.
const MOST_ERROR_BYTES: usize = 64 << 10;

/// How long the API server is asked to keep a watch open before it ends it, after which it
/// is begun again from where it was.
const WATCH_SECONDS: u32 = 300;

/// How long things may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// Connecting, the handshakes included; an answer's head; and a list's page whole.
    pub answer: Duration,
    /// The most a watch may go without a byte, bookmarks included, which the API server
    /// sends about once a minute, before it is taken to have failed.
    pub quiet: Duration,
    /// How long a token read is used before it is read again.
    pub token_reread: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            answer: Duration::from_secs(30),
            quiet: Duration::from_secs(360),
            token_reread: Duration::from_secs(60),
        }
    }
}

/// Why the API server could not be asked, or what it answered instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    /// The environment names no API server.
    #[error(
        "the environment names no API server: KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT"
    )]
    NoServer,
    /// `KUBERNETES_SERVICE_PORT` is not a port.
    #[error("KUBERNETES_SERVICE_PORT `{0}` is not a port from 1 to 65535")]
    Port(String),
    /// The authorities trusted to sign the API server's certificate cannot be read.
    #[error("the API server's authorities: {0}")]
    Authorities(String),
    /// TLS could not be set up.
    #[error("TLS: {0}")]
    Tls(String),
    /// The service account's token cannot be read, and none was read before.
    #[error("the service account's token: {0}")]
    Token(String),
    /// A namespace that is not a namespace's name, which would not make a path.
    #[error("`{0}` is not a namespace's name")]
    Namespace(String),
    /// A namespace's objects asked for of a kind whose objects are the cluster's.
    #[error("a {0} is the cluster's, not a namespace's")]
    NotNamespaced(&'static str),
    /// A name that no object can have, which would not make a field selector.
    #[error("`{0}` is not an object's name")]
    Name(String),
    /// An object asked for by its name alone of a kind whose objects are a namespace's.
    #[error("a {0} is a namespace's, and has no name of its own in the cluster")]
    Namespaced(&'static str),
    /// The API server could not be connected to, or its certificate is not trusted.
    #[error("connecting to the API server: {0}")]
    Connect(String),
    /// HTTP/2 failed on the connection or the stream.
    #[error("HTTP/2: {0}")]
    Http(String),
    /// The API server took longer than it may.
    #[error("the API server took too long")]
    TimedOut,
    /// The API server answered with a status other than 200: 410 for a version, or a
    /// list's token, that it no longer has.
    #[error("the API server answered {code}: {message}")]
    Answered {
        /// The status.
        code: u16,
        /// What it said, if anything.
        message: String,
    },
    /// An answer larger than it may be.
    #[error("{0} larger than {1} bytes")]
    TooLarge(&'static str, usize),
    /// An answer that is not what was asked for.
    #[error("an answer that cannot be read: {0}")]
    Unreadable(String),
}

impl ApiError {
    /// Whether the API server no longer has what was asked from (410 Gone), and the objects
    /// must be listed afresh.
    #[must_use]
    pub fn is_gone(&self) -> bool {
        matches!(self, Self::Answered { code: 410, .. })
    }

    /// Whether this is the API server's 409: an object written as of a version it has
    /// moved past, or created where there is one already.
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Answered { code: 409, .. })
    }
}

/// The API server, and what it is asked with.
pub struct ApiServer {
    /// Its host, an address or a name, which its certificate must carry.
    host: String,
    /// Its port.
    port: u16,
    /// `host:port`, an IPv6 address in brackets.
    authority: String,
    connector: SslConnector,
    token: Token,
    timing: Timing,
    /// The connection, while it lasts.
    connection: Mutex<Option<SendRequest<Bytes>>>,
}

impl std::fmt::Debug for ApiServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiServer")
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

/// The service account's token, and when it was read.
struct Token {
    path: PathBuf,
    held: Mutex<Option<(String, Instant)>>,
}

impl ApiServer {
    /// The API server of the cluster the pod is in, as its environment and service account
    /// name it.
    ///
    /// # Errors
    ///
    /// An [`ApiError`] for an environment that names no API server, or authorities that
    /// cannot be read.
    pub fn in_cluster() -> Result<Self, ApiError> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST").ok();
        let port = std::env::var("KUBERNETES_SERVICE_PORT").ok();
        let (host, port) = server(host.as_deref(), port.as_deref())?;
        let account = Path::new(SERVICE_ACCOUNT);
        let authorities = std::fs::read_to_string(account.join("ca.crt"))
            .map_err(|error| ApiError::Authorities(error.to_string()))?;
        Self::new(
            &host,
            port,
            &authorities,
            account.join("token"),
            Timing::default(),
        )
    }

    /// The API server at `host` and `port`, its certificate signed by one of `authorities`
    /// (PEM) for `host`, asked with the token in the file at `token`.
    ///
    /// # Errors
    ///
    /// An [`ApiError`] for authorities that cannot be read, or TLS that cannot be set up.
    pub fn new(
        host: &str,
        port: u16,
        authorities: &str,
        token: PathBuf,
        timing: Timing,
    ) -> Result<Self, ApiError> {
        let setup = |error: boring::error::ErrorStack| ApiError::Tls(error.to_string());
        let mut builder = SslConnector::builder(SslMethod::tls()).map_err(setup)?;
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(setup)?;
        builder.set_verify(SslVerifyMode::PEER);
        let read = X509::stack_from_pem(authorities.as_bytes())
            .map_err(|error| ApiError::Authorities(error.to_string()))?;
        if read.is_empty() {
            return Err(ApiError::Authorities("no certificate in them".to_owned()));
        }
        let mut trusted = X509StoreBuilder::new().map_err(setup)?;
        for authority in read {
            trusted.add_cert(authority).map_err(setup)?;
        }
        // These alone: the machine's own store is not consulted.
        builder
            .set_verify_cert_store(trusted.build())
            .map_err(setup)?;
        builder.set_alpn_protos(b"\x02h2").map_err(setup)?;
        let authority = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        Ok(Self {
            host: host.to_owned(),
            port,
            authority,
            connector: builder.build(),
            token: Token {
                path: token,
                held: Mutex::new(None),
            },
            timing,
            connection: Mutex::new(None),
        })
    }

    /// A page of `K`'s objects in `scope`: the first, or the one `continue_from` continues
    /// to, each object trimmed as [`Kind::trim`] keeps it.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]: [`ApiError::is_gone`] when the token is too old.
    pub async fn list<K: Kind>(
        &self,
        scope: &Scope,
        continue_from: Option<&str>,
    ) -> Result<List<K>, ApiError> {
        let mut path = format!("{}?limit={}", kind::collection::<K>(scope)?, K::PAGE);
        fields_into::<K>(scope, &mut path);
        if let Some(token) = continue_from {
            path.push_str("&continue=");
            encode_into(&mut path, token);
        }
        let deadline = Instant::now() + self.timing.answer;
        let body = self.get(&path, deadline).await?;
        let page = read_whole(body, MOST_PAGE_BYTES, "a list's page", deadline).await?;
        let mut page: List<K> = serde_json::from_slice(&page)
            .map_err(|error| ApiError::Unreadable(error.to_string()))?;
        page.items.iter_mut().for_each(K::trim);
        Ok(page)
    }

    /// A watch of `K`'s objects in `scope` from version `from`, with bookmarks.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]: [`ApiError::is_gone`] when the version is too old.
    pub async fn watch<K: Kind>(&self, scope: &Scope, from: &str) -> Result<Events<K>, ApiError> {
        let mut path = format!(
            "{}?watch=true&allowWatchBookmarks=true&timeoutSeconds={WATCH_SECONDS}",
            kind::collection::<K>(scope)?
        );
        fields_into::<K>(scope, &mut path);
        path.push_str("&resourceVersion=");
        encode_into(&mut path, from);
        let body = self.get(&path, Instant::now() + self.timing.answer).await?;
        Ok(Events {
            body,
            pending: BytesMut::new(),
            quiet: self.timing.quiet,
            kind: PhantomData,
        })
    }

    /// Merges `patch`, JSON, into the status of `K`'s object `name` in `namespace`, as
    /// `kubectl patch --subresource status --type merge` does (RFC 7386): a field it states
    /// takes the place of the one there, a list whole, and one it leaves out is left as it
    /// is. Nothing but the status is changed, whatever the patch says of the rest.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]: what the API server answered if not 200, such as 403 without the
    /// permission to update the status, or 404 for an object gone.
    pub async fn patch_status<K: Kind>(
        &self,
        namespace: &str,
        name: &str,
        patch: &serde_json::Value,
    ) -> Result<(), ApiError> {
        let mut path = object_path::<K>(namespace, name)?;
        path.push_str("/status");
        let body = serde_json::to_vec(patch).map_err(|error| ApiError::Http(error.to_string()))?;
        let deadline = Instant::now() + self.timing.answer;
        let merge = ("application/merge-patch+json", Bytes::from(body));
        // What it answers, the object as it now is, is not needed.
        let _object = self
            .send(Method::PATCH, &path, Some(merge), deadline)
            .await?;
        Ok(())
    }

    /// `K`'s object `name` in `namespace`, as it is now; none if there is none.
    ///
    /// # Errors
    ///
    /// An [`ApiError`] for anything the API server answers but the object or 404.
    pub async fn object<K: Kind>(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<K>, ApiError> {
        let path = object_path::<K>(namespace, name)?;
        let deadline = Instant::now() + self.timing.answer;
        match self.get(&path, deadline).await {
            Ok(body) => self.read_object(body, deadline).await.map(Some),
            Err(ApiError::Answered { code: 404, .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Creates `object`, `K`'s, in `namespace`: the object as the API server has made it.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]: [`ApiError::is_conflict`] when there is one of its name already.
    pub async fn create<K: Kind + serde::Serialize>(
        &self,
        namespace: &str,
        object: &K,
    ) -> Result<K, ApiError> {
        let path = kind::collection::<K>(&Scope::namespace(namespace)?)?;
        self.write(Method::POST, &path, object).await
    }

    /// Replaces `K`'s object `name` in `namespace` with `object`, as of the version its
    /// metadata says it was read at: the object as the API server now has it.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]: [`ApiError::is_conflict`] when it has changed since that version.
    pub async fn replace<K: Kind + serde::Serialize>(
        &self,
        namespace: &str,
        name: &str,
        object: &K,
    ) -> Result<K, ApiError> {
        let path = object_path::<K>(namespace, name)?;
        self.write(Method::PUT, &path, object).await
    }

    /// Replaces the status of `K`'s object `name` in `namespace` (`""` for a kind whose
    /// objects are the cluster's) with `status`, as of `version`, the `resourceVersion` it
    /// was read at: the object is sent with its metadata and status alone, as the API server
    /// takes nothing else of it there.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]: [`ApiError::is_conflict`] when it has changed since that version.
    pub async fn replace_status<K: Kind>(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
        status: &impl serde::Serialize,
    ) -> Result<(), ApiError> {
        let mut path = object_path::<K>(namespace, name)?;
        path.push_str("/status");
        let api_version = if K::GROUP.is_empty() {
            K::VERSION.to_owned()
        } else {
            format!("{}/{}", K::GROUP, K::VERSION)
        };
        let mut metadata = serde_json::json!({ "name": name, "resourceVersion": version });
        if !namespace.is_empty() {
            metadata["namespace"] = serde_json::Value::from(namespace);
        }
        let object = serde_json::json!({
            "apiVersion": api_version,
            "kind": K::KIND,
            "metadata": metadata,
            "status": status,
        });
        let body =
            serde_json::to_vec(&object).map_err(|error| ApiError::Http(error.to_string()))?;
        let deadline = Instant::now() + self.timing.answer;
        // What it answers, the object as it now is, is not needed.
        let _object = self
            .send(
                Method::PUT,
                &path,
                Some(("application/json", Bytes::from(body))),
                deadline,
            )
            .await?;
        Ok(())
    }

    /// Sends `object` with `method` to `path`, and reads back what the API server made of
    /// it.
    async fn write<K: Kind + serde::Serialize>(
        &self,
        method: Method,
        path: &str,
        object: &K,
    ) -> Result<K, ApiError> {
        let body = serde_json::to_vec(object).map_err(|error| ApiError::Http(error.to_string()))?;
        let deadline = Instant::now() + self.timing.answer;
        // The API server answers a create with 201: a success as 200 is.
        let answered = self
            .send(
                method,
                path,
                Some(("application/json", Bytes::from(body))),
                deadline,
            )
            .await?;
        self.read_object(answered, deadline).await
    }

    /// The object `body` holds, trimmed as [`Kind::trim`] keeps it.
    async fn read_object<K: Kind>(
        &self,
        body: RecvStream,
        deadline: Instant,
    ) -> Result<K, ApiError> {
        let read = read_whole(body, MOST_PAGE_BYTES, "an object", deadline).await?;
        let mut object: K = serde_json::from_slice(&read)
            .map_err(|error| ApiError::Unreadable(error.to_string()))?;
        object.trim();
        Ok(object)
    }

    /// The body of what `path` answers with 200, its head by `deadline`; or what it
    /// answered instead.
    async fn get(&self, path: &str, deadline: Instant) -> Result<RecvStream, ApiError> {
        self.send(Method::GET, path, None, deadline).await
    }

    /// The body of what `path` answers `method` with, with `body` of its content type if
    /// any, with 200, its head by `deadline`; or what it answered instead.
    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<(&'static str, Bytes)>,
        deadline: Instant,
    ) -> Result<RecvStream, ApiError> {
        let token = self.token.read(self.timing.token_reread).await?;
        let mut request = http::Request::builder()
            .method(method)
            .uri(format!("https://{}{path}", self.authority))
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(ACCEPT, "application/json")
            .header(USER_AGENT, "edgerush");
        if let Some((content_type, _)) = &body {
            request = request.header(CONTENT_TYPE, *content_type);
        }
        let request = request
            .body(())
            .map_err(|error| ApiError::Http(error.to_string()))?;
        let answered = async {
            let mut send = self.sender().await?;
            let (answer, mut stream) = send
                .send_request(request, body.is_none())
                .map_err(|error| ApiError::Http(error.to_string()))?;
            if let Some((_, body)) = body {
                stream
                    .send_data(body, true)
                    .map_err(|error| ApiError::Http(error.to_string()))?;
            }
            answer
                .await
                .map_err(|error| ApiError::Http(error.to_string()))
        };
        let answer = tokio::time::timeout_at(deadline, answered)
            .await
            .map_err(|_| ApiError::TimedOut)??;
        let (head, body) = answer.into_parts();
        if head.status == StatusCode::OK || head.status == StatusCode::CREATED {
            return Ok(body);
        }
        let said = read_whole(body, MOST_ERROR_BYTES, "an error", deadline)
            .await
            .unwrap_or_default();
        let message = serde_json::from_slice::<Status>(&said)
            .ok()
            .and_then(|status| status.message)
            .unwrap_or_default();
        Err(ApiError::Answered {
            code: head.status.as_u16(),
            message,
        })
    }

    /// The connection's sender, ready for a stream: the one held, or a new connection's if
    /// it has failed or there is none.
    async fn sender(&self) -> Result<SendRequest<Bytes>, ApiError> {
        let mut held = self.connection.lock().await;
        if let Some(send) = held.clone()
            && let Ok(ready) = send.ready().await
        {
            return Ok(ready);
        }
        *held = None;
        let send = self.connect().await?;
        *held = Some(send.clone());
        Ok(send)
    }

    /// A new connection, driven by a task of its own until it ends.
    async fn connect(&self) -> Result<SendRequest<Bytes>, ApiError> {
        let connect = |error: String| ApiError::Connect(error);
        let socket = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|error| connect(error.to_string()))?;
        let configured = self
            .connector
            .configure()
            .map_err(|error| ApiError::Tls(error.to_string()))?;
        // The certificate is checked against the host as the environment names it, an
        // address as an address.
        let secured = tokio_boring::connect(configured, &self.host, socket)
            .await
            .map_err(|error| connect(format!("{error:?}")))?;
        if secured.ssl().selected_alpn_protocol() != Some(&b"h2"[..]) {
            return Err(connect("the API server did not agree on HTTP/2".to_owned()));
        }
        let (send, connection) = h2::client::Builder::new()
            .enable_push(false)
            .max_concurrent_streams(0)
            .initial_max_send_streams(1)
            .initial_window_size(1 << 20)
            .initial_connection_window_size(4 << 20)
            .max_header_list_size(64 << 10)
            .header_table_size(4096)
            .max_frame_size(16_384)
            .max_send_buffer_size(64 << 10)
            .max_concurrent_reset_streams(50)
            .reset_stream_duration(Duration::from_secs(1))
            .handshake::<_, Bytes>(secured)
            .await
            .map_err(|error| ApiError::Http(error.to_string()))?;
        tokio::spawn(async move {
            // It ends when the API server closes it, or when every sender is gone; how is
            // seen by the next request, which opens another.
            let _ended = connection.await;
        });
        send.ready()
            .await
            .map_err(|error| ApiError::Http(error.to_string()))
    }
}

/// The path of `K`'s object `name` in `namespace`.
fn object_path<K: Kind>(namespace: &str, name: &str) -> Result<String, ApiError> {
    // A kind whose objects are the cluster's is named in no namespace.
    let scope = match (K::NAMESPACED, namespace) {
        (false, "") => Scope::Cluster,
        (false, _) => return Err(ApiError::NotNamespaced(K::KIND)),
        (true, namespace) => Scope::namespace(namespace)?,
    };
    let mut path = kind::collection::<K>(&scope)?;
    path.push('/');
    encode_into(&mut path, name);
    Ok(path)
}

/// A watch's events, as the API server sends them.
#[derive(Debug)]
pub struct Events<K> {
    body: RecvStream,
    /// What has come of an event not yet whole.
    pending: BytesMut,
    quiet: Duration,
    kind: PhantomData<fn() -> K>,
}

impl<K: Kind> Events<K> {
    /// The next event, its object trimmed as [`Kind::trim`] keeps it, or none when the API
    /// server has ended the watch.
    ///
    /// # Errors
    ///
    /// An [`ApiError`] for an event too large or unreadable, a watch quiet too long, or one
    /// that failed; the watch is begun again then.
    pub async fn next(&mut self) -> Option<Result<WatchEvent<K>, ApiError>> {
        loop {
            // One event a line.
            while let Some(end) = self.pending.iter().position(|b| *b == b'\n') {
                let line = self.pending.split_to(end + 1);
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Some(
                    serde_json::from_slice::<WatchEvent<K>>(&line)
                        .map(|mut event| {
                            if let WatchEvent::Added(object)
                            | WatchEvent::Modified(object)
                            | WatchEvent::Deleted(object) = &mut event
                            {
                                object.trim();
                            }
                            event
                        })
                        .map_err(|error| ApiError::Unreadable(error.to_string())),
                );
            }
            if self.pending.len() > MOST_EVENT_BYTES {
                return Some(Err(ApiError::TooLarge("a watch's event", MOST_EVENT_BYTES)));
            }
            match timeout(self.quiet, self.body.data()).await {
                Err(_) => return Some(Err(ApiError::TimedOut)),
                Ok(None) => return None,
                Ok(Some(Err(error))) => return Some(Err(ApiError::Http(error.to_string()))),
                Ok(Some(Ok(chunk))) => {
                    let _released = self.body.flow_control().release_capacity(chunk.len());
                    self.pending.extend_from_slice(&chunk);
                }
            }
        }
    }
}

impl Token {
    /// The token: as last read, or read again if that was `reread` ago or longer, the last
    /// kept if it cannot be.
    async fn read(&self, reread: Duration) -> Result<String, ApiError> {
        let mut held = self.held.lock().await;
        if let Some((token, at)) = held.as_ref()
            && at.elapsed() < reread
        {
            return Ok(token.clone());
        }
        let read = std::fs::read_to_string(&self.path)
            .map_err(|error| error.to_string())
            .and_then(|token| {
                let token = token.trim().to_owned();
                if token.is_empty() {
                    Err("it is empty".to_owned())
                } else {
                    Ok(token)
                }
            });
        match (read, held.take()) {
            (Ok(token), _) => {
                *held = Some((token.clone(), Instant::now()));
                Ok(token)
            }
            // Kept, and tried again next time.
            (Err(_), Some((token, at))) => {
                *held = Some((token.clone(), at));
                Ok(token)
            }
            (Err(why), None) => Err(ApiError::Token(why)),
        }
    }
}

/// The API server's host and port, from what the environment says of them.
fn server(host: Option<&str>, port: Option<&str>) -> Result<(String, u16), ApiError> {
    let (Some(host), Some(port)) = (host.filter(|h| !h.is_empty()), port) else {
        return Err(ApiError::NoServer);
    };
    let number = port
        .parse::<u16>()
        .ok()
        .filter(|number| *number != 0)
        .ok_or_else(|| ApiError::Port(port.to_owned()))?;
    Ok((host.to_owned(), number))
}

/// The field selectors of `K` and of `scope`, if either has one, written into a query: an
/// object named alone is selected by its name.
fn fields_into<K: Kind>(scope: &Scope, query: &mut String) {
    let named = match scope {
        Scope::Named(name) => Some(format!("metadata.name={name}")),
        _ => None,
    };
    let fields: Vec<&str> = K::FIELDS.into_iter().chain(named.as_deref()).collect();
    if !fields.is_empty() {
        query.push_str("&fieldSelector=");
        encode_into(query, &fields.join(","));
    }
}

/// `value` written into a query, every byte but RFC 3986's unreserved percent-encoded:
/// versions and list tokens are opaque.
fn encode_into(query: &mut String, value: &str) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            query.push(char::from(byte));
        } else {
            query.push_str(&format!("%{byte:02X}"));
        }
    }
}

/// A body whole, if it is no larger than `most` and comes by `deadline`.
async fn read_whole(
    mut body: RecvStream,
    most: usize,
    what: &'static str,
    deadline: Instant,
) -> Result<Bytes, ApiError> {
    let mut whole = BytesMut::new();
    let read = async {
        while let Some(chunk) = body.data().await {
            let chunk = chunk.map_err(|error| ApiError::Http(error.to_string()))?;
            let _released = body.flow_control().release_capacity(chunk.len());
            if whole.len() + chunk.len() > most {
                return Err(ApiError::TooLarge(what, most));
            }
            whole.extend_from_slice(&chunk);
        }
        Ok(())
    };
    tokio::time::timeout_at(deadline, read)
        .await
        .map_err(|_| ApiError::TimedOut)??;
    Ok(whole.freeze())
}

mod follow;
mod kind;
mod watch;

pub use follow::{Failure, Listed, Retry, follow};
pub use kind::{Kind, Scope};
pub use watch::{Request, Watch, WatchError};

#[cfg(any(test, feature = "testing"))]
pub mod testing;
#[cfg(test)]
mod tests;
