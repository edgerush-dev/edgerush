//! A fake API server, for tests here and in the crates that follow namespaces with this
//! one: HTTP/2 over TLS, answering as each test scripts it.

#![allow(
    clippy::unwrap_used,
    reason = "test set-up: a fake that cannot be made is a broken test"
)]

use crate::{ApiServer, Timing};
use boring::pkey::PKey;
use boring::ssl::{AlpnError, SslAcceptor, SslMethod, select_next_proto};
use boring::x509::X509;
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

/// What the fake API server answers a request with: a status, its body in these chunks,
/// and a pause before each.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The status.
    pub status: u16,
    /// The body, a chunk at a time.
    pub chunks: Vec<Bytes>,
    /// The pause before each chunk.
    pub pause: Duration,
}

/// `status`, with `body` at once.
#[must_use]
pub fn answer(status: u16, body: &str) -> Answer {
    Answer {
        status,
        chunks: vec![Bytes::copy_from_slice(body.as_bytes())],
        pause: Duration::ZERO,
    }
}

/// A request as the fake API server saw it.
#[derive(Debug, Clone)]
pub struct Seen {
    /// Its method.
    pub method: String,
    /// Its path and query.
    pub path: String,
    /// Its `Authorization`, if it had one.
    pub authorization: Option<String>,
    /// Its `Content-Type`, if it had one.
    pub content_type: Option<String>,
    /// Its body, as text.
    pub body: String,
}

/// A fake API server over HTTP/2 and TLS.
#[derive(Debug)]
pub struct Fake {
    /// Where it listens.
    pub address: SocketAddr,
    /// Its certificate, which a client is given as its authority.
    pub authority: String,
    /// The requests it has seen, in order.
    pub seen: Arc<std::sync::Mutex<Vec<Seen>>>,
    /// The connections it has accepted.
    pub connections: Arc<AtomicUsize>,
}

impl Fake {
    /// A fake whose certificate is made for `names`, answering each request as `answers`
    /// says for its path and query. On the runtime it is started on, which must be a
    /// multi-threaded one or be driven while the client waits.
    pub async fn start(
        names: &[&str],
        answers: impl Fn(&str) -> Answer + Send + Sync + 'static,
    ) -> Self {
        Self::start_by_method(names, move |_method, path| answers(path)).await
    }

    /// As [`Fake::start`], each request answered as `answers` says for its method and its
    /// path and query.
    pub async fn start_by_method(
        names: &[&str],
        answers: impl Fn(&str, &str) -> Answer + Send + Sync + 'static,
    ) -> Self {
        let certificate = edgerush_certificates::testing::certificate(names);
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
        acceptor
            .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
            .unwrap();
        acceptor
            .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
            .unwrap();
        acceptor.set_alpn_select_callback(|_, offered| {
            select_next_proto(b"\x02h2", offered).ok_or(AlpnError::NOACK)
        });
        let acceptor = Arc::new(acceptor.build());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let answers = Arc::new(answers);
        let (recorded, counted) = (Arc::clone(&seen), Arc::clone(&connections));
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                counted.fetch_add(1, Ordering::SeqCst);
                let (acceptor, answers, recorded) = (
                    Arc::clone(&acceptor),
                    Arc::clone(&answers),
                    Arc::clone(&recorded),
                );
                tokio::spawn(async move {
                    // A client that refuses the certificate ends the handshake: nothing more.
                    let Ok(secured) = tokio_boring::accept(&acceptor, socket).await else {
                        return;
                    };
                    let mut connection = h2::server::handshake(secured).await.unwrap();
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        let path = request.uri().path_and_query().unwrap().to_string();
                        let header = |name| {
                            request
                                .headers()
                                .get(name)
                                .map(|value: &http::HeaderValue| value.to_str().unwrap().to_owned())
                        };
                        let mut seen = Seen {
                            method: request.method().to_string(),
                            path: path.clone(),
                            authorization: header(AUTHORIZATION),
                            content_type: header(CONTENT_TYPE),
                            body: String::new(),
                        };
                        // A read has no body, and is recorded at once, in the order the
                        // requests came; a write once its body is read, before it is
                        // answered, so that a client answered finds it recorded.
                        let mut body = request.into_body();
                        let read = body.is_end_stream();
                        if read {
                            recorded.lock().unwrap().push(seen.clone());
                        }
                        let answer = answers(&seen.method, &path);
                        let recorded = Arc::clone(&recorded);
                        tokio::spawn(async move {
                            if !read {
                                let mut written = Vec::new();
                                while let Some(Ok(chunk)) = body.data().await {
                                    let _ = body.flow_control().release_capacity(chunk.len());
                                    written.extend_from_slice(&chunk);
                                }
                                seen.body = String::from_utf8(written).unwrap();
                                recorded.lock().unwrap().push(seen);
                            }
                            let head = http::Response::builder()
                                .status(answer.status)
                                .body(())
                                .unwrap();
                            let mut stream = respond.send_response(head, false).unwrap();
                            for chunk in answer.chunks {
                                tokio::time::sleep(answer.pause).await;
                                if stream.send_data(chunk, false).is_err() {
                                    return;
                                }
                            }
                            let _ = stream.send_data(Bytes::new(), true);
                        });
                    }
                });
            }
        });
        Self {
            address,
            authority: certificate.chain,
            seen,
            connections,
        }
    }

    /// The requests it has seen so far.
    #[must_use]
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// A client of it, at `127.0.0.1`, trusting its certificate, with the token in `token`.
    #[must_use]
    pub fn client(&self, token: &Path, timing: Timing) -> ApiServer {
        ApiServer::new(
            "127.0.0.1",
            self.address.port(),
            &self.authority,
            token.to_owned(),
            timing,
        )
        .unwrap()
    }
}

/// A token file of the test's own, `name` in a directory of these tests', holding `token`.
pub fn token(name: &str, token: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("edgerush-kubernetes-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, token).unwrap();
    path
}

/// A slice of Service `web` in `shop`, at version 4, of one endpoint.
pub const SLICE: &str = r#"{"metadata":{"name":"web-1","namespace":"shop","resourceVersion":"4"},"addressType":"IPv4","endpoints":[{"addresses":["10.0.0.1"]}]}"#;

/// A list's page of [`SLICE`] as of `version`, continuing to `next` if it is not the last.
#[must_use]
pub fn list(version: &str, next: Option<&str>) -> String {
    let next = next
        .map(|next| format!(r#","continue":"{next}""#))
        .unwrap_or_default();
    format!(
        r#"{{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{{"resourceVersion":"{version}"{next}}},"items":[{SLICE}]}}"#
    )
}
