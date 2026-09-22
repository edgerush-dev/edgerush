//! hyper's HTTP/1 client a connection at a time, over the worker's own pool: the client
//! EdgeRush's own is compared with ([14 §2](../../../docs/14-downstream-server.md)).
//!
//! What the pool holds is a whole hyper connection: the sender a request goes in by, and
//! the driver that moves the connection's bytes, which runs as a task of the worker for as
//! long as the connection lives and is stopped when it goes. A socket is never taken out
//! of one and handshaken again.
//!
//! Unlike hyper-util's pooled client, this one takes a request body that cannot leave the
//! worker, which a body read by EdgeRush's own server is: its driver is spawned into the
//! worker's `LocalSet`, never onto a thread pool.
//!
//! A connection goes back only once it has shown it is done. Its answer was read to the
//! end, and hyper says it will take another request — which hyper says only when the
//! request went out whole as well, and nothing was left over. The pool's own policy — how
//! many, how long, which destination — is the one EdgeRush's client keeps to.

use super::h1::H1Limits;
use super::h1::pool::Lease;
use bytes::Bytes;
use http::{Request, Response};
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// One of hyper's HTTP/1 connections to an upstream: how to send on it, and the task that
/// keeps it moving. Dropping it stops that task and closes the connection.
#[derive(Debug)]
pub struct HyperConnection<B> {
    sender: SendRequest<B>,
    driver: JoinHandle<()>,
}

impl<B> Drop for HyperConnection<B> {
    fn drop(&mut self) {
        // hyper stops the driver itself once the sender has gone and nothing is under way,
        // but not while a request is still going out: an answer that ended before its
        // request did leaves the driver uploading for as long as the client goes on
        // sending, or holding the connection open for as long as it stalls.
        self.driver.abort();
    }
}

impl<B> HyperConnection<B>
where
    B: Body + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Handshakes on `socket` and sets the connection's driver running in the worker's
    /// `LocalSet`, which it has to be inside.
    ///
    /// # Errors
    ///
    /// hyper's, if the handshake fails.
    pub async fn open(socket: TcpStream) -> Result<Self, hyper::Error> {
        let (sender, connection) = http1::handshake(TokioIo::new(socket)).await?;
        let driver = tokio::task::spawn_local(async move {
            // A connection that fails ends here, and its sender says so to whoever holds
            // it next. There is nobody else to tell.
            let _closed = connection.await;
        });
        Ok(Self { sender, driver })
    }

    /// Sends a request. Only ever on a connection that [`HyperConnection::is_ready`] says
    /// will take one.
    pub fn send(
        &mut self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Incoming>, hyper::Error>> + use<B> {
        self.sender.send_request(request)
    }
}

impl<B> HyperConnection<B> {
    /// Whether hyper will take another request on this connection now. hyper says so only
    /// once the last exchange is over in both directions and the connection is still open.
    pub fn is_ready(&self) -> bool {
        self.sender.is_ready()
    }

    /// Whether the connection has closed, and will never be ready again.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// The driver, for a test to watch it stop.
    #[cfg(test)]
    fn driver(&self) -> tokio::task::AbortHandle {
        self.driver.abort_handle()
    }
}

/// An answer's body as hyper's client reads it, and, where it came by a pooled connection,
/// the way that connection goes back.
///
/// The connection travels with the body because the answer is still arriving on it. It
/// goes back the moment the body is known to be over, which for a body of known length is
/// its last frame rather than a later poll that may never come, and only if hyper then says
/// it will take another request. A body let go of before its end takes its connection with
/// it, and that closes it.
#[derive(Debug)]
pub struct HyperBody<B> {
    incoming: Incoming,
    returning: Option<Returning<B>>,
    /// The whole answer has been handed on.
    ended: bool,
}

/// A connection in use by an answer's body, and where it goes when the answer is over.
#[derive(Debug)]
struct Returning<B> {
    connection: HyperConnection<B>,
    lease: Lease<HyperConnection<B>>,
    limits: H1Limits,
}

impl<B> HyperBody<B> {
    /// A body on a connection no pool of ours holds: hyper-util's pooled client keeps its
    /// own.
    pub fn unpooled(incoming: Incoming) -> Self {
        Self {
            incoming,
            returning: None,
            ended: false,
        }
    }

    /// A body on `connection`, which goes back through `lease` if it earns it.
    pub fn returning(
        incoming: Incoming,
        connection: HyperConnection<B>,
        lease: Lease<HyperConnection<B>>,
        limits: H1Limits,
    ) -> Self {
        let mut body = Self {
            incoming,
            returning: Some(Returning {
                connection,
                lease,
                limits,
            }),
            ended: false,
        };
        // Nothing need ever poll a body with nothing in it, so its connection would
        // otherwise wait until the body object was dropped.
        if body.incoming.is_end_stream() {
            body.ended = true;
            body.give_back();
        }
        body
    }

    /// Puts the connection back if hyper will take another request on it, and closes it
    /// otherwise.
    fn give_back(&mut self) {
        if let Some(Returning {
            connection,
            lease,
            limits,
        }) = self.returning.take()
            && connection.is_ready()
        {
            lease.keep(connection, &limits);
        }
    }
}

impl<B> Body for HyperBody<B> {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.incoming).poll_frame(context);
        match &polled {
            Poll::Ready(None) => this.ended = true,
            // Trailers are the last frame there is.
            Poll::Ready(Some(Ok(frame)))
                if frame.is_trailers() || this.incoming.is_end_stream() =>
            {
                this.ended = true;
            }
            // An error is not the end, so the connection goes with the body.
            _ => {}
        }
        if this.ended {
            this.give_back();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.incoming.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.incoming.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::destination::{Destinations, Keys, ReuseIdentity};
    use crate::upstream::h1::pool::Pool;
    use edgerush_config::{Config, compile};
    use http_body_util::BodyExt;
    use std::cell::{Cell, RefCell};
    use std::convert::Infallible;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    /// A request body that cannot leave the worker, as one read by EdgeRush's own server
    /// cannot: it holds an `Rc`, whose count also says when the body has been let go of.
    /// Its frames are handed to it one at a time, while the request is under way.
    struct LocalBody {
        frames: mpsc::UnboundedReceiver<Bytes>,
        _here: Rc<()>,
    }

    impl Body for LocalBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.get_mut()
                .frames
                .poll_recv(context)
                .map(|frame| frame.map(|data| Ok(Frame::data(data))))
        }
    }

    /// A body, what feeds it, and the count that says whether it is still held.
    fn local_body() -> (LocalBody, mpsc::UnboundedSender<Bytes>, Rc<()>) {
        let (feeding, frames) = mpsc::unbounded_channel();
        let here = Rc::new(());
        let body = LocalBody {
            frames,
            _here: Rc::clone(&here),
        };
        (body, feeding, here)
    }

    /// An upstream that says only what it is told to, and records everything it is sent
    /// and whether its connection has closed. One connection at a time.
    struct Peer {
        address: std::net::SocketAddr,
        accepted: Rc<Cell<usize>>,
        seen: Rc<RefCell<Vec<u8>>>,
        closed: Rc<Cell<bool>>,
        say: mpsc::UnboundedSender<&'static [u8]>,
    }

    async fn peer() -> Peer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (say, mut told) = mpsc::unbounded_channel::<&'static [u8]>();
        let accepted = Rc::new(Cell::new(0));
        let seen = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(Cell::new(false));
        let (counting, seeing, closing) =
            (Rc::clone(&accepted), Rc::clone(&seen), Rc::clone(&closed));
        let _peer = tokio::task::spawn_local(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                counting.set(counting.get() + 1);
                closing.set(false);
                let mut read = [0; 1024];
                loop {
                    tokio::select! {
                        got = stream.read(&mut read) => match got {
                            Ok(0) | Err(_) => {
                                closing.set(true);
                                break;
                            }
                            Ok(n) => seeing.borrow_mut().extend_from_slice(&read[..n]),
                        },
                        said = told.recv() => {
                            let Some(said) = said else { return };
                            stream.write_all(said).await.unwrap();
                        }
                    }
                }
            }
        });
        Peer {
            address,
            accepted,
            seen,
            closed,
            say,
        }
    }

    impl Peer {
        fn has_seen(&self, bytes: &[u8]) -> bool {
            self.seen
                .borrow()
                .windows(bytes.len())
                .any(|at| at == bytes)
        }
    }

    /// Waits for something the test is about to depend on, and fails rather than hangs.
    async fn until(mut settled: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !settled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waited for something that never happened");
    }

    fn request(body: LocalBody) -> Request<LocalBody> {
        Request::builder()
            .method("POST")
            .uri("/upload")
            .header("host", "upstream.test")
            .body(body)
            .unwrap()
    }

    /// A destination to file connections under, and the destinations that keep it live.
    fn destination(peer: &Peer) -> (Destinations, Arc<ReuseIdentity>) {
        let yaml = format!(
            "listeners: {{}}\nroutes: []\nupstreams:\n  up: {{ endpoints: [\"{}\"] }}\n",
            peer.address
        );
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        let held = Destinations::reconcile(
            &compile(&config).unwrap(),
            &Destinations::default(),
            &Keys::default(),
        );
        let identity = Arc::clone(held.at(0, 0).unwrap());
        (held, identity)
    }

    async fn opened(peer: &Peer) -> HyperConnection<LocalBody> {
        HyperConnection::open(TcpStream::connect(peer.address).await.unwrap())
            .await
            .unwrap()
    }

    fn in_local_set(test: impl Future<Output = ()>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, test);
    }

    /// A body that cannot leave the worker goes out a frame at a time as it is fed, and the
    /// answer comes back a frame at a time as it arrives: neither is gathered up first.
    #[test]
    fn a_worker_local_body_streams_out_and_its_answer_streams_back() {
        in_local_set(async {
            let peer = peer().await;
            let mut connection = opened(&peer).await;
            let (body, feeding, _here) = local_body();
            let answer = connection.send(request(body));
            let answering = tokio::task::spawn_local(answer);

            feeding.send(Bytes::from_static(b"first")).unwrap();
            until(|| peer.has_seen(b"first")).await;
            assert!(!peer.has_seen(b"second"));
            feeding.send(Bytes::from_static(b"second")).unwrap();
            until(|| peer.has_seen(b"second")).await;
            drop(feeding);
            until(|| peer.has_seen(b"0\r\n\r\n")).await;

            peer.say
                .send(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\none\r\n")
                .unwrap();
            let mut body = answering.await.unwrap().unwrap().into_body();
            let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(
                first, "one",
                "the answer was gathered up before it was handed on"
            );
            peer.say.send(b"3\r\ntwo\r\n0\r\n\r\n").unwrap();
            let rest = body.collect().await.unwrap().to_bytes();
            assert_eq!(rest, "two");
        });
    }

    /// How an answer's body is finished with.
    #[derive(Debug, Clone, Copy)]
    enum Finished {
        /// A chunked body read to its end, the poll that says so included: nothing but
        /// that poll says a chunked body is over.
        Collected,
        /// Its one frame read, and then dropped without asking again: a client told how
        /// long a body is has no reason to.
        LastFrameOnly,
        /// Its data and then its trailers read, and dropped: trailers are the last frame.
        Trailers,
        /// A body with nothing in it, never polled, and held on to.
        NeverPolled,
    }

    /// Requests one after another on one connection, their answers finished with every way
    /// a body can be: each answer over puts the connection back, and the next request is
    /// sent on it. The upstream accepted only once.
    #[test]
    fn a_connection_whose_exchange_finished_carries_the_next() {
        in_local_set(async {
            let peer = peer().await;
            let (_held, identity) = destination(&peer);
            let limits = H1Limits::default();
            let pool = Rc::new(RefCell::new(Pool::default()));

            let mut connection = opened(&peer).await;
            for finished in [
                Finished::Collected,
                Finished::LastFrameOnly,
                Finished::Trailers,
                Finished::NeverPolled,
            ] {
                let (body, feeding, here) = local_body();
                peer.seen.borrow_mut().clear();
                let answer = connection.send(request(body));
                feeding.send(Bytes::from_static(b"up")).unwrap();
                drop(feeding);
                // Answered only once the request is all there, so that the exchange is
                // over when its answer is.
                let answer = tokio::task::spawn_local(answer);
                until(|| peer.has_seen(b"0\r\n\r\n")).await;
                peer.say
                    .send(match finished {
                        Finished::Collected => {
                            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n"
                        }
                        Finished::LastFrameOnly => {
                            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"
                        }
                        Finished::Trailers => {
                            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nok\r\n0\r\nx-sum: 1\r\n\r\n"
                        }
                        Finished::NeverPolled => b"HTTP/1.1 204 No Content\r\n\r\n",
                    })
                    .unwrap();
                let (_head, incoming) = answer.await.unwrap().unwrap().into_parts();
                let lease = Lease::in_use(Arc::clone(&identity), Instant::now(), &pool);
                let mut body = HyperBody::returning(incoming, connection, lease, limits);
                let _held_on_to = match finished {
                    Finished::Collected => {
                        assert_eq!(body.collect().await.unwrap().to_bytes(), "ok");
                        None
                    }
                    Finished::LastFrameOnly => {
                        let only = body.frame().await.unwrap().unwrap();
                        assert_eq!(only.into_data().unwrap(), "ok");
                        drop(body);
                        None
                    }
                    Finished::Trailers => {
                        let data = body.frame().await.unwrap().unwrap();
                        assert_eq!(data.into_data().unwrap(), "ok");
                        let trailers = body.frame().await.unwrap().unwrap();
                        assert!(trailers.into_trailers().unwrap().contains_key("x-sum"));
                        drop(body);
                        None
                    }
                    Finished::NeverPolled => Some(body),
                };
                assert_eq!(
                    Rc::strong_count(&here),
                    1,
                    "{finished:?}: the request body is held"
                );
                assert_eq!(pool.borrow().idle(), 1, "{finished:?}: it did not go back");

                let (taken, _) = pool.borrow_mut().take(&identity, &limits).unwrap();
                assert!(taken.is_ready(), "{finished:?}: back before it was ready");
                connection = taken;
            }
            assert_eq!(peer.accepted.get(), 1);
        });
    }

    /// An answer that ends while its request is still going out is over, but the exchange
    /// is not: the connection is closed rather than put back out of step.
    #[test]
    fn an_answer_that_ends_before_its_request_does_not_put_the_connection_back() {
        in_local_set(async {
            let peer = peer().await;
            let (_held, identity) = destination(&peer);
            let pool = Rc::new(RefCell::new(Pool::default()));

            let mut connection = opened(&peer).await;
            let (body, feeding, _here) = local_body();
            let answer = connection.send(request(body));
            feeding.send(Bytes::from_static(b"part")).unwrap();
            until(|| peer.has_seen(b"part")).await;
            peer.say
                .send(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .unwrap();
            let (_head, incoming) = answer.await.unwrap().into_parts();
            let lease = Lease::in_use(Arc::clone(&identity), Instant::now(), &pool);
            let body = HyperBody::returning(incoming, connection, lease, H1Limits::default());
            assert_eq!(body.collect().await.unwrap().to_bytes(), "ok");
            assert_eq!(
                pool.borrow().idle(),
                0,
                "put back with its request unfinished"
            );
            until(|| peer.closed.get()).await;
            drop(feeding);
        });
    }

    /// An answer let go of part way closes its connection, and so does a request let go of
    /// before its answer: in neither case does the connection go back, the driver stops,
    /// the upstream sees the close, and the request body is let go of.
    #[test]
    fn an_exchange_let_go_part_way_closes_its_connection_and_lets_go_of_its_body() {
        in_local_set(async {
            let peer = peer().await;
            let (_held, identity) = destination(&peer);
            let pool = Rc::new(RefCell::new(Pool::default()));

            // An answer arrives while the upload is still going, and is let go of after
            // its first frame.
            let mut connection = opened(&peer).await;
            let driver = connection.driver();
            let (body, feeding, here) = local_body();
            let answer = connection.send(request(body));
            feeding.send(Bytes::from_static(b"part")).unwrap();
            until(|| peer.has_seen(b"part")).await;
            peer.say
                .send(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\none\r\n")
                .unwrap();
            let (_head, incoming) = answer.await.unwrap().into_parts();
            let lease = Lease::in_use(Arc::clone(&identity), Instant::now(), &pool);
            let mut body = HyperBody::returning(incoming, connection, lease, H1Limits::default());
            let _first = body.frame().await.unwrap().unwrap();
            drop(body);
            until(|| peer.closed.get()).await;
            until(|| driver.is_finished()).await;
            assert_eq!(Rc::strong_count(&here), 1, "the request body is still held");
            assert_eq!(pool.borrow().idle(), 0);
            drop(feeding);

            // A request let go of before anything came back.
            let mut connection = opened(&peer).await;
            let driver = connection.driver();
            let (body, feeding, here) = local_body();
            let answer = connection.send(request(body));
            feeding.send(Bytes::from_static(b"more")).unwrap();
            let waiting = tokio::task::spawn_local(answer);
            until(|| peer.has_seen(b"more")).await;
            waiting.abort();
            drop(connection);
            until(|| peer.closed.get()).await;
            until(|| driver.is_finished()).await;
            assert_eq!(Rc::strong_count(&here), 1, "the request body is still held");
        });
    }

    /// A connection the pool lets go of — refused, swept or simply dropped — stops its
    /// driver and closes, and so does one whose upstream closed while it sat idle, which
    /// hyper then says will never be ready.
    #[test]
    fn a_connection_let_go_of_stops_its_driver() {
        in_local_set(async {
            let peer = peer().await;
            let connection = opened(&peer).await;
            let driver = connection.driver();
            until(|| peer.accepted.get() == 1).await;
            drop(connection);
            until(|| driver.is_finished()).await;
            until(|| peer.closed.get()).await;

            // The same seen from the other end: the upstream goes, and the idle
            // connection says it will not be ready again.
            let connection = opened(&peer).await;
            until(|| peer.accepted.get() == 2).await;
            assert!(connection.is_ready());
            drop(peer.say);
            until(|| connection.is_closed()).await;
            assert!(!connection.is_ready());
        });
    }
}
