//! Clients of a worker under test, over HTTP/1, HTTP/2 and TLS.

use super::*;

/// Reads from `stream` until the proxy closes it, and says how long that took. Bounded
/// well past every deadline under test, so that a deadline missing fails rather than
/// hangs.
pub(super) async fn closed_after(stream: &mut TcpStream) -> Duration {
    use tokio::io::AsyncReadExt;
    let started = tokio::time::Instant::now();
    let mut rest = Vec::new();
    let _ended = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest))
        .await
        .expect("the connection was never closed");
    started.elapsed()
}

/// Reads one answer of the counting upstream's (`ok`, and its head) off `stream`.
pub(super) async fn answered(stream: &mut TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut seen = Vec::new();
    let mut byte = [0; 1];
    while !seen.ends_with(b"\r\n\r\nok") {
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .expect("no answer")
            .unwrap();
        assert_ne!(
            read,
            0,
            "closed before answering: {}",
            String::from_utf8_lossy(&seen)
        );
        seen.push(byte[0]);
    }
    String::from_utf8(seen).unwrap()
}

pub(super) const ASKED: &[u8] = b"GET / HTTP/1.1\r\nhost: example.test\r\n\r\n";

/// An HTTP/2 client of the worker at `front`, past its handshake, over a real socket.
pub(super) async fn h2_client(front: SocketAddr) -> crate::h2_peer::Peer<TcpStream> {
    use crate::h2_peer::{self, Peer, flag, kind};
    let stream = TcpStream::connect(front).await.unwrap();
    let mut peer = Peer::open_as_client(stream, &[]).await;
    peer.until(|f| f.kind == kind::SETTINGS && !f.has(flag::ACK))
        .await;
    peer.send(&h2_peer::settings_ack()).await;
    peer
}

/// A GET of `path` on stream `id`, ending the stream.
pub(super) async fn h2_get(peer: &mut crate::h2_peer::Peer<TcpStream>, id: u32, path: &str) {
    use crate::h2_peer::{self};
    let block = h2_peer::block(&[
        (":method", "GET"),
        (":scheme", "http"),
        (":authority", "example.test"),
        (":path", path),
    ]);
    peer.send(&h2_peer::headers(id, block, true)).await;
}

/// An HTTP/2 client of h2's own, over a real socket to `front`, built as `builder` says.
pub(super) async fn h2_library_client(
    front: SocketAddr,
    builder: &::h2::client::Builder,
) -> ::h2::client::SendRequest<Bytes> {
    let stream = TcpStream::connect(front).await.unwrap();
    let (send, connection) = builder.handshake(stream).await.unwrap();
    let _driving = tokio::task::spawn_local(async move {
        let _ended = connection.await;
    });
    send
}

/// A TLS client of `front`, asking for `name` and offering `protocols` (ALPN, in the
/// wire form), set up beyond that by `configure`. It takes whatever certificate it is
/// given: which one that is, is for the test to look at.
pub(super) async fn tls_client(
    front: SocketAddr,
    name: &str,
    protocols: Option<&[u8]>,
    configure: impl FnOnce(&mut boring::ssl::SslConnectorBuilder),
) -> Result<tokio_boring::SslStream<TcpStream>, String> {
    use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    if let Some(protocols) = protocols {
        builder.set_alpn_protos(protocols).unwrap();
    }
    configure(&mut builder);
    let config = builder.build().configure().unwrap().verify_hostname(false);
    let stream = TcpStream::connect(front).await.unwrap();
    within(tokio_boring::connect(config, name, stream))
        .await
        .map_err(|error| format!("{error:?}"))
}

/// Sends one HTTP/1.1 request on `stream` and reads the answer to its end.
pub(super) async fn h1_over<S>(mut stream: S) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    let _ended = within(stream.read_to_end(&mut answer)).await;
    String::from_utf8_lossy(&answer).into_owned()
}

/// What a TLS client asking for `name` (or none) through `front` is told once its
/// handshake is done, or `None` if there was no handshake.
pub(super) async fn told_over_tls(front: SocketAddr, name: Option<&str>) -> Option<String> {
    use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
    use tokio::io::AsyncReadExt;
    let stream = TcpStream::connect(front).await.unwrap();
    let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
    connector.set_verify(SslVerifyMode::NONE);
    let mut config = connector.build().configure().unwrap();
    config.set_use_server_name_indication(name.is_some());
    config.set_verify_hostname(false);
    let mut secured = bounded(tokio_boring::connect(
        config,
        name.unwrap_or("none.test"),
        stream,
    ))
    .await
    .ok()?;
    let mut told = String::new();
    let _ended = bounded(secured.read_to_string(&mut told)).await;
    Some(told)
}

/// Whether `client`'s tunnel is still open after `waited`: nothing read, and no end.
pub(super) async fn still_open(client: &mut TcpStream, waited: Duration) -> bool {
    use tokio::io::AsyncReadExt;
    let mut byte = [0; 1];
    tokio::time::timeout(waited, client.read(&mut byte))
        .await
        .is_err()
}

/// What a request over `stream` is answered, or nothing if the stream fails: under
/// TLS 1.3 a client's certificate is judged after the client has finished its side of
/// the handshake, so a refusal can arrive as the first thing read.
pub(super) async fn h1_over_or_nothing<S>(mut stream: S) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let request = b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n";
    if stream.write_all(request).await.is_err() {
        return String::new();
    }
    let mut answer = Vec::new();
    let _ended = within(stream.read_to_end(&mut answer)).await;
    String::from_utf8_lossy(&answer).into_owned()
}

/// Sends one HTTP/1.1 request of `request` bytes, and reads the answer to its end.
pub(super) async fn h1_answer(front: SocketAddr, request: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(front).await.unwrap();
    stream.write_all(request).await.unwrap();
    let mut answer = Vec::new();
    let _ended = within(stream.read_to_end(&mut answer)).await;
    String::from_utf8_lossy(&answer).into_owned()
}

pub(super) const CLOSING_GET: &[u8] =
    b"GET /a/b?c=d HTTP/1.1\r\nhost: shop.example.com\r\nconnection: close\r\naccept: */*\r\n\r\n";

/// A gRPC call, as a gRPC client makes one: POST, `application/grpc`, `te: trailers`,
/// and `grpc-timeout` if `timeout` says one.
pub(super) fn grpc_call(path: &str, timeout: Option<&str>) -> Request<()> {
    let mut call = Request::post(format!("http://a.test{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if let Some(timeout) = timeout {
        call = call.header("grpc-timeout", timeout);
    }
    call.body(()).unwrap()
}

/// What a gRPC call came back with: the HTTP status, and the gRPC status and whether
/// it came in the head (a trailers-only answer) or in trailers.
pub(super) async fn grpc_outcome(
    answer: ::h2::client::ResponseFuture,
) -> (StatusCode, String, bool) {
    let answer = within(answer).await.unwrap();
    let status = answer.status();
    if let Some(code) = answer.headers().get("grpc-status") {
        assert!(
            answer.body().is_end_stream(),
            "a status in the head, and more after it"
        );
        return (status, code.to_str().unwrap().to_owned(), true);
    }
    let mut body = answer.into_body();
    while let Some(chunk) = within(body.data()).await {
        let chunk = chunk.expect("the stream was reset, not ended with a status");
        let _ = body.flow_control().release_capacity(chunk.len());
    }
    let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx)))
        .await
        .expect("the stream was reset, not ended with a status")
        .expect("no status at all");
    (
        status,
        trailers["grpc-status"].to_str().unwrap().to_owned(),
        false,
    )
}

/// What one HTTP/1.1 request to `address` is answered with.
pub(super) async fn status_over_http1(address: SocketAddr) -> StatusCode {
    status_of(address, "/").await
}

pub(super) async fn status_of(address: SocketAddr, path: &str) -> StatusCode {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let _detached = tokio::task::spawn_local(async move {
        let _closed = connection.await;
    });
    // Routed like any other request, so it needs a host to be routed by.
    let request = Request::builder()
        .uri(path)
        .header("host", "example.test")
        .body(Empty::<Bytes>::new())
        .unwrap();
    sender.send_request(request).await.unwrap().status()
}

/// The status one HTTP/2 request to `address` is answered with.
pub(super) async fn status_over_http2(address: SocketAddr) -> StatusCode {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(OnThisWorker, TokioIo::new(stream))
            .await
            .unwrap();
    let _detached = tokio::task::spawn_local(async move {
        let _closed = connection.await;
    });
    let mut request = Request::new(Empty::<Bytes>::new());
    *request.version_mut() = Version::HTTP_2;
    *request.uri_mut() = format!("http://{address}/").parse().unwrap();
    sender.send_request(request).await.unwrap().status()
}
