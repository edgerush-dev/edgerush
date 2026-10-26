//! A fake API server, for the tests: HTTP/2 over TLS, answering as each test scripts it.

use super::*;
use boring::pkey::PKey;
use boring::ssl::{AlpnError, SslAcceptor, select_next_proto};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;

/// What the fake API server answers a request with: a status, its body in these chunks,
/// and a pause before each.
pub(crate) struct Answer {
    pub(crate) status: u16,
    pub(crate) chunks: Vec<Bytes>,
    pub(crate) pause: Duration,
}

pub(crate) fn answer(status: u16, body: &str) -> Answer {
    Answer {
        status,
        chunks: vec![Bytes::copy_from_slice(body.as_bytes())],
        pause: Duration::ZERO,
    }
}

/// A request as the fake API server saw it.
#[derive(Debug, Clone)]
pub(crate) struct Seen {
    pub(crate) path: String,
    pub(crate) authorization: Option<String>,
}

/// A fake API server over HTTP/2 and TLS, its certificate made for `names`.
pub(crate) struct Fake {
    pub(crate) address: SocketAddr,
    /// Its certificate, which the client is given as its authority.
    pub(crate) authority: String,
    pub(crate) seen: Arc<std::sync::Mutex<Vec<Seen>>>,
    pub(crate) connections: Arc<AtomicUsize>,
}

impl Fake {
    pub(crate) async fn start(
        names: &[&str],
        answers: impl Fn(&str) -> Answer + Send + Sync + 'static,
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
                        recorded.lock().unwrap().push(Seen {
                            path: path.clone(),
                            authorization: request
                                .headers()
                                .get(AUTHORIZATION)
                                .map(|value| value.to_str().unwrap().to_owned()),
                        });
                        let answer = answers(&path);
                        tokio::spawn(async move {
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

    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// A client of it, at `127.0.0.1`, trusting its certificate, with the token in `token`.
    pub(crate) fn client(&self, token: &Path, timing: Timing) -> ApiServer {
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

/// A token file of the test's own, holding `token`.
pub(crate) fn token(name: &str, token: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("edgerush-kubernetes-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, token).unwrap();
    path
}

pub(crate) const SLICE: &str = r#"{"metadata":{"name":"web-1","namespace":"shop","resourceVersion":"4"},"addressType":"IPv4","endpoints":[{"addresses":["10.0.0.1"]}]}"#;

pub(crate) fn list(version: &str, next: Option<&str>) -> String {
    let next = next
        .map(|next| format!(r#","continue":"{next}""#))
        .unwrap_or_default();
    format!(
        r#"{{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{{"resourceVersion":"{version}"{next}}},"items":[{SLICE}]}}"#
    )
}
