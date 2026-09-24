//! Both ends of an HTTP/2 connection in memory, for the tests of this module: h2's own
//! server on one side, and either h2's client or the scripted peer on the other. Everything
//! runs on the test's thread, as a worker does, since what is served here is not `Send`.

use bytes::Bytes;
use std::future::Future;
use std::time::Duration;
use tokio::io::DuplexStream;

/// An idle bound no test here runs into, for the ones that are not about it.
pub(super) const LONG: Duration = Duration::from_secs(600);

/// A test that waits for what never comes should fail, not hang.
pub(super) async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out")
}

/// Runs `test` on this thread, where the server's pieces may be spawned.
pub(super) fn locally<F: Future>(test: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(tokio::task::LocalSet::new().run_until(test))
}

/// The same on a stopped clock, which moves only when every task is waiting.
pub(super) fn locally_paused<F: Future>(test: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("a runtime");
    runtime.block_on(tokio::task::LocalSet::new().run_until(test))
}

pub(super) fn wire() -> (DuplexStream, DuplexStream) {
    tokio::io::duplex(1 << 20)
}

/// An h2 client, driven on this thread, and the server's connection, handshaken.
pub(super) async fn pair(
    builder: &::h2::server::Builder,
) -> (
    ::h2::client::SendRequest<Bytes>,
    ::h2::server::Connection<DuplexStream, super::writer::Outgoing>,
) {
    let (near, far) = wire();
    let (client, server) = tokio::join!(
        ::h2::client::handshake(far),
        builder.handshake::<_, super::writer::Outgoing>(near)
    );
    let (send, connection) = client.expect("the client's handshake");
    tokio::task::spawn_local(connection);
    (send, server.expect("the server's handshake"))
}

/// A stream the server accepted: the request and what answers it.
pub(super) type Accepted = (
    http::Request<::h2::RecvStream>,
    ::h2::server::SendResponse<super::writer::Outgoing>,
);

/// Drives the server's connection in the background, handing over each stream it accepts.
pub(super) fn serving(
    mut connection: ::h2::server::Connection<DuplexStream, super::writer::Outgoing>,
) -> tokio::sync::mpsc::UnboundedReceiver<Accepted> {
    let (tx, accepted) = tokio::sync::mpsc::unbounded_channel();
    tokio::task::spawn_local(async move {
        while let Some(Ok(stream)) = connection.accept().await {
            let _ = tx.send(stream);
        }
    });
    accepted
}

/// A request with a body to come, over HTTP/2.
pub(super) fn post() -> http::Request<()> {
    http::Request::post("http://example.com/")
        .version(http::Version::HTTP_2)
        .body(())
        .expect("a request")
}
