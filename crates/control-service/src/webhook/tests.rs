use super::*;
use crate::{CertificateFiles, webhook_acceptor};
use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use edgerush_certificates::testing::Authority;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A webhook serving on a port of its own, its answers each the length of the review it
/// was sent, and how many it was asked to answer.
struct Webhook {
    address: String,
    tls: watch::Sender<Arc<SslAcceptor>>,
    asked: Arc<AtomicUsize>,
    stop: Option<oneshot::Sender<()>>,
    served: JoinHandle<()>,
}

impl Webhook {
    async fn start(authority: &Authority, bounds: Bounds) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let acceptor = webhook_acceptor(&authority.issue(&["webhook.test"])).unwrap();
        let (tls, accepting) = watch::channel(Arc::new(acceptor));
        let asked = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&asked);
        let (stop, stopped) = oneshot::channel();
        let served = tokio::spawn(serve(
            listener,
            accepting,
            bounds,
            move |review| {
                counted.fetch_add(1, Ordering::SeqCst);
                Answer {
                    status: StatusCode::OK,
                    body: format!("{{\"length\":{}}}", review.len()).into_bytes(),
                }
            },
            async move {
                let _ = stopped.await;
            },
        ));
        Self {
            address,
            tls,
            asked,
            stop: Some(stop),
            served,
        }
    }

    fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.served)
            .await
            .expect("still serving after it was stopped")
            .unwrap();
    }
}

/// The API server's TLS as the test makes it: no certificate of its own, `authority`
/// trusted, offering `alpn`.
fn client(authority: &Authority, alpn: &[u8]) -> SslConnector {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    let mut trusted = X509StoreBuilder::new().unwrap();
    for certificate in X509::stack_from_pem(authority.pem().as_bytes()).unwrap() {
        trusted.add_cert(certificate).unwrap();
    }
    builder.set_verify_cert_store(trusted.build()).unwrap();
    builder.set_verify(SslVerifyMode::PEER);
    if !alpn.is_empty() {
        builder.set_alpn_protos(alpn).unwrap();
    }
    builder.build()
}

const HTTP1: &[u8] = b"\x08http/1.1";
const H2: &[u8] = b"\x02h2";

async fn secured(
    address: &str,
    tls: &SslConnector,
    name: &str,
) -> Result<tokio_boring::SslStream<TcpStream>, tokio_boring::HandshakeError<TcpStream>> {
    let socket = TcpStream::connect(address).await.unwrap();
    tokio_boring::connect(tls.configure().unwrap(), name, socket).await
}

/// What the webhook sends back to `request`, written as it is over HTTP/1.1, until it
/// closes the connection.
async fn http1(address: &str, tls: &SslConnector, request: &[u8]) -> String {
    let mut stream = secured(address, tls, "webhook.test").await.unwrap();
    stream.write_all(request).await.unwrap();
    let mut read = Vec::new();
    let reading = async {
        let mut buffer = [0; 4096];
        // A close without TLS's own goodbye ends it as well as one with.
        while let Ok(count @ 1..) = stream.read(&mut buffer).await {
            read.extend_from_slice(&buffer[..count]);
        }
    };
    tokio::time::timeout(Duration::from_secs(5), reading)
        .await
        .expect("the connection was not closed");
    String::from_utf8_lossy(&read).into_owned()
}

fn post(path: &str, body: &str) -> Vec<u8> {
    format!(
        "POST {path} HTTP/1.1\r\nhost: webhook.test\r\ncontent-type: application/json\r\n\
         content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// The status, content type and body of `body` posted over HTTP/2.
async fn h2(
    address: &str,
    tls: &SslConnector,
    method: Method,
    path: &str,
    body: &[u8],
) -> (StatusCode, String, String) {
    let stream = secured(address, tls, "webhook.test").await.unwrap();
    assert_eq!(stream.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
    let (mut client, connection) = ::h2::client::handshake(stream).await.unwrap();
    let connected = tokio::spawn(connection);
    let request = Request::builder()
        .method(method)
        .uri(format!("https://webhook.test{path}"))
        .body(())
        .unwrap();
    let (response, mut sending) = client.send_request(request, false).unwrap();
    sending
        .send_data(Bytes::copy_from_slice(body), true)
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .expect("no answer")
        .unwrap();
    let status = response.status();
    let kind = response.headers()[CONTENT_TYPE]
        .to_str()
        .unwrap()
        .to_owned();
    let mut received = response.into_body();
    let mut read = Vec::new();
    while let Some(data) = received.data().await {
        let data = data.unwrap();
        received
            .flow_control()
            .release_capacity(data.len())
            .unwrap();
        read.extend_from_slice(&data);
    }
    connected.abort();
    (status, kind, String::from_utf8(read).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_review_posted_is_answered_as_the_caller_says_over_http1_or_http2() {
    let authority = Authority::new("webhook");
    let webhook = Webhook::start(&authority, Bounds::default()).await;
    let said = http1(
        &webhook.address,
        &client(&authority, HTTP1),
        &post(PATH, "{\"a\":1}"),
    )
    .await;
    assert!(said.starts_with("HTTP/1.1 200 OK\r\n"), "{said}");
    assert!(
        said.contains("content-type: application/json\r\n"),
        "{said}"
    );
    assert!(said.ends_with("\r\n\r\n{\"length\":7}"), "{said}");
    // No ALPN offered at all, or only what the webhook does not speak: HTTP/1.1, as with
    // it offered.
    for offered in [&b""[..], b"\x08spdy/3.1"] {
        let said = http1(
            &webhook.address,
            &client(&authority, offered),
            &post(PATH, "{}"),
        )
        .await;
        assert!(said.ends_with("{\"length\":2}"), "{said}");
    }
    let (status, kind, body) = h2(
        &webhook.address,
        &client(&authority, H2),
        Method::POST,
        PATH,
        b"{\"b\":22}",
    )
    .await;
    assert_eq!(
        (status, kind.as_str(), body.as_str()),
        (StatusCode::OK, "application/json", "{\"length\":8}")
    );
    assert_eq!(webhook.asked(), 4);
    webhook.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn its_certificate_files_are_read_again_as_they_change() {
    let dir = std::env::temp_dir().join("edgerush-control-service-webhook-files");
    std::fs::create_dir_all(&dir).unwrap();
    let files = CertificateFiles {
        certificate: dir.join("tls.crt"),
        key: dir.join("tls.key"),
    };
    // Each file put in place whole, as Kubernetes swaps a mounted Secret's.
    let put = |path: &std::path::Path, contents: &str| {
        let beside = path.with_extension("partial");
        std::fs::write(&beside, contents).unwrap();
        std::fs::rename(&beside, path).unwrap();
    };
    put(&files.certificate, "first");
    put(&files.key, "key");
    let first = files.read().await.unwrap();
    assert_eq!((first.chain.as_str(), first.key.as_str()), ("first", "key"));
    let (seen, mut changes) = tokio::sync::mpsc::unbounded_channel();
    let rereading = tokio::spawn(crate::reread(
        files.clone(),
        first,
        Duration::from_millis(20),
        move |changed| {
            let _ = seen.send(changed.map(|now| now.chain.clone()).map_err(Clone::clone));
        },
    ));
    put(&files.certificate, "second");
    let changed = tokio::time::timeout(Duration::from_secs(5), changes.recv())
        .await
        .expect("no change handed on")
        .unwrap();
    assert_eq!(changed, Ok("second".to_owned()));
    rereading.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_answer_other_than_200_goes_as_plain_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let authority = Authority::new("webhook");
    let acceptor = webhook_acceptor(&authority.issue(&["webhook.test"])).unwrap();
    let (_tls, accepting) = watch::channel(Arc::new(acceptor));
    let served = tokio::spawn(serve(
        listener,
        accepting,
        Bounds::default(),
        |_| Answer {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"not ready".to_vec(),
        },
        std::future::pending(),
    ));
    let said = http1(&address, &client(&authority, HTTP1), &post(PATH, "{}")).await;
    assert!(
        said.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
        "{said}"
    );
    assert!(
        said.contains("content-type: text/plain; charset=utf-8\r\n"),
        "{said}"
    );
    assert!(said.ends_with("\r\n\r\nnot ready"), "{said}");
    served.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn another_path_or_method_is_no_review_and_never_handed_on() {
    let authority = Authority::new("webhook");
    let webhook = Webhook::start(&authority, Bounds::default()).await;
    let tls = client(&authority, HTTP1);
    let said = http1(&webhook.address, &tls, &post("/validate/more", "{}")).await;
    assert!(said.starts_with("HTTP/1.1 404 Not Found\r\n"), "{said}");
    let said = http1(&webhook.address, &tls, &post("/", "{}")).await;
    assert!(said.starts_with("HTTP/1.1 404 Not Found\r\n"), "{said}");
    let get = b"GET /validate HTTP/1.1\r\nhost: webhook.test\r\nconnection: close\r\n\r\n";
    let said = http1(&webhook.address, &tls, get).await;
    assert!(
        said.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
        "{said}"
    );
    let (status, ..) = h2(
        &webhook.address,
        &client(&authority, H2),
        Method::PUT,
        PATH,
        b"{}",
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(webhook.asked(), 0);
    webhook.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_over_the_bound_is_refused_and_never_handed_on() {
    let authority = Authority::new("webhook");
    let bounds = Bounds {
        most: 64,
        ..Bounds::default()
    };
    let webhook = Webhook::start(&authority, bounds).await;
    let at_most = "x".repeat(64);
    let said = http1(
        &webhook.address,
        &client(&authority, HTTP1),
        &post(PATH, &at_most),
    )
    .await;
    assert!(said.ends_with("{\"length\":64}"), "{said}");
    let over = "x".repeat(65);
    let said = http1(
        &webhook.address,
        &client(&authority, HTTP1),
        &post(PATH, &over),
    )
    .await;
    assert!(
        said.starts_with("HTTP/1.1 413 Payload Too Large\r\n"),
        "{said}"
    );
    // Without a length said first: chunked, the bound found as it is read.
    let chunked = format!(
        "POST {PATH} HTTP/1.1\r\nhost: webhook.test\r\ntransfer-encoding: chunked\r\n\
         connection: close\r\n\r\n20\r\n{0}\r\n20\r\n{0}\r\n20\r\n{0}\r\n0\r\n\r\n",
        "y".repeat(32)
    );
    let said = http1(
        &webhook.address,
        &client(&authority, HTTP1),
        chunked.as_bytes(),
    )
    .await;
    assert!(
        said.starts_with("HTTP/1.1 413 Payload Too Large\r\n"),
        "{said}"
    );
    let (status, ..) = h2(
        &webhook.address,
        &client(&authority, H2),
        Method::POST,
        PATH,
        over.as_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(webhook.asked(), 1);
    webhook.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_head_or_body_that_does_not_come_in_time_is_given_up() {
    let authority = Authority::new("webhook");
    let bounds = Bounds {
        deadline: Duration::from_millis(300),
        ..Bounds::default()
    };
    let webhook = Webhook::start(&authority, bounds).await;
    let tls = client(&authority, HTTP1);
    // A body said to be ten bytes, of which one comes.
    let started = tokio::time::Instant::now();
    let said = http1(
        &webhook.address,
        &tls,
        format!("POST {PATH} HTTP/1.1\r\nhost: webhook.test\r\ncontent-length: 10\r\n\r\nx")
            .as_bytes(),
    )
    .await;
    assert!(
        said.starts_with("HTTP/1.1 408 Request Timeout\r\n"),
        "{said}"
    );
    assert!(started.elapsed() >= Duration::from_millis(300));
    // A head that never ends: the connection is closed.
    let said = http1(
        &webhook.address,
        &tls,
        b"POST /validate HTTP/1.1\r\nhost: webh",
    )
    .await;
    assert!(!said.contains(" 200 "), "{said}");
    assert_eq!(webhook.asked(), 0);
    webhook.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn its_client_needs_no_certificate_and_one_replaced_serves_the_next_connection() {
    let authority = Authority::new("webhook");
    let webhook = Webhook::start(&authority, Bounds::default()).await;
    let tls = client(&authority, HTTP1);
    // The test's client presents none: served all the same.
    let said = http1(&webhook.address, &tls, &post(PATH, "{}")).await;
    assert!(said.starts_with("HTTP/1.1 200 OK\r\n"), "{said}");
    webhook.tls.send_replace(Arc::new(
        webhook_acceptor(&authority.issue(&["renamed.test"])).unwrap(),
    ));
    assert!(
        secured(&webhook.address, &tls, "webhook.test")
            .await
            .is_err()
    );
    assert!(
        secured(&webhook.address, &tls, "renamed.test")
            .await
            .is_ok()
    );
    webhook.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_ends_the_connections_still_open() {
    let authority = Authority::new("webhook");
    let webhook = Webhook::start(&authority, Bounds::default()).await;
    // An HTTP/2 connection, kept open, as the API server keeps its own.
    let stream = secured(&webhook.address, &client(&authority, H2), "webhook.test")
        .await
        .unwrap();
    let (_client, connection) = ::h2::client::handshake(stream).await.unwrap();
    let connected = tokio::spawn(connection);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!connected.is_finished());
    webhook.stop().await;
    tokio::time::timeout(Duration::from_secs(5), connected)
        .await
        .expect("the connection was left open")
        .unwrap()
        .unwrap_or_default();
}
