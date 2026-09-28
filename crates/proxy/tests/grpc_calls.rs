//! gRPC through the data plane, as a real gRPC client and server see it
//! ([15 §6](../../../docs/15-http2-and-grpc.md)).
//!
//! tonic on both sides, its client calling through the proxy to its server over HTTP/2
//! without TLS: all four call shapes, a status of the upstream's own, and a deadline the
//! gateway enforces. Messages are opaque bytes to all three, as they are to the gateway.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use bytes::{Buf, BufMut, Bytes};
use edgerush_config::{Config, compile};
use edgerush_proxy::{H1Limits, Proxy, Worker};
use http::uri::PathAndQuery;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status, Streaming};

/// Messages as they come: bytes, encoded and decoded as themselves.
#[derive(Debug, Clone, Copy, Default)]
struct Raw;

impl Codec for Raw {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Raw;
    type Decoder = Raw;
    fn encoder(&mut self) -> Raw {
        Raw
    }
    fn decoder(&mut self) -> Raw {
        Raw
    }
}

impl Encoder for Raw {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for Raw {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let length = src.remaining();
        Ok(Some(src.copy_to_bytes(length)))
    }
}

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Messages = Pin<Box<dyn tokio_stream::Stream<Item = Result<Bytes, Status>> + Send>>;

/// The upstream: one service, `test.Echo`, with a method of every shape.
#[derive(Debug, Clone, Copy)]
struct Echo;

/// Unary: the message back. `Fail` answers `NOT_FOUND`; `Slow` takes a second.
struct Unary;
impl tonic::server::UnaryService<Bytes> for Unary {
    type Response = Bytes;
    type Future = Boxed<Result<Response<Bytes>, Status>>;
    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
        Box::pin(async move { Ok(Response::new(request.into_inner())) })
    }
}

struct Fail;
impl tonic::server::UnaryService<Bytes> for Fail {
    type Response = Bytes;
    type Future = Boxed<Result<Response<Bytes>, Status>>;
    fn call(&mut self, _request: Request<Bytes>) -> Self::Future {
        Box::pin(async {
            Err(Status::with_details(
                Code::NotFound,
                "nobody by that name",
                Bytes::from_static(b"details"),
            ))
        })
    }
}

struct Slow;
impl tonic::server::UnaryService<Bytes> for Slow {
    type Response = Bytes;
    type Future = Boxed<Result<Response<Bytes>, Status>>;
    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(Response::new(request.into_inner()))
        })
    }
}

/// Client streaming: every message, joined.
struct Gather;
impl tonic::server::ClientStreamingService<Bytes> for Gather {
    type Response = Bytes;
    type Future = Boxed<Result<Response<Bytes>, Status>>;
    fn call(&mut self, request: Request<Streaming<Bytes>>) -> Self::Future {
        Box::pin(async move {
            let mut messages = request.into_inner();
            let mut joined = Vec::new();
            while let Some(message) = messages.next().await {
                joined.extend_from_slice(&message?);
            }
            Ok(Response::new(Bytes::from(joined)))
        })
    }
}

/// Server streaming: the message, a byte at a time.
struct Scatter;
impl tonic::server::ServerStreamingService<Bytes> for Scatter {
    type Response = Bytes;
    type ResponseStream = Messages;
    type Future = Boxed<Result<Response<Messages>, Status>>;
    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
        Box::pin(async move {
            let bytes: Vec<Result<Bytes, Status>> = request
                .into_inner()
                .iter()
                .map(|byte| Ok(Bytes::copy_from_slice(&[*byte])))
                .collect();
            let messages: Messages = Box::pin(tokio_stream::iter(bytes));
            Ok(Response::new(messages))
        })
    }
}

/// Bidirectional: each message back as it comes.
struct Mirror;
impl tonic::server::StreamingService<Bytes> for Mirror {
    type Response = Bytes;
    type ResponseStream = Messages;
    type Future = Boxed<Result<Response<Messages>, Status>>;
    fn call(&mut self, request: Request<Streaming<Bytes>>) -> Self::Future {
        Box::pin(async move {
            let messages: Messages = Box::pin(request.into_inner());
            Ok(Response::new(messages))
        })
    }
}

impl tower_service::Service<http::Request<hyper::body::Incoming>> for Echo {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<hyper::body::Incoming>) -> Self::Future {
        let request = request.map(tonic::body::Body::new);
        let path = request.uri().path().to_owned();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(Raw);
            Ok(match path.as_str() {
                "/test.Echo/Unary" => grpc.unary(Unary, request).await,
                "/test.Echo/Fail" => grpc.unary(Fail, request).await,
                "/test.Echo/Slow" => grpc.unary(Slow, request).await,
                "/test.Echo/Gather" => grpc.client_streaming(Gather, request).await,
                "/test.Echo/Scatter" => grpc.server_streaming(Scatter, request).await,
                "/test.Echo/Mirror" => grpc.streaming(Mirror, request).await,
                _ => Status::unimplemented("no such method").into_http(),
            })
        })
    }
}

/// Serves `Echo` over HTTP/2 without TLS, and says where.
async fn upstream() -> SocketAddr {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            tokio::spawn(async move {
                let _served = Builder::new(TokioExecutor::new())
                    .http2_only()
                    .serve_connection(TokioIo::new(stream), TowerToHyperService::new(Echo))
                    .await;
            });
        }
    });
    address
}

/// A data plane of one worker whose every request goes to `upstream`, spoken to in
/// HTTP/2, and where it listens.
async fn proxy_to(upstream: SocketAddr) -> SocketAddr {
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }} }}
routes:
  - name: echo
    listeners: [web]
    hostnames: [{{ name: "*", falls_through: true }}]
    rules:
      - matches: [{{ grpc: {{ service: test.Echo }} }}]
        forward: {{ backends: [{{ upstream: echo, weight: 1 }}] }}
upstreams:
  echo: {{ endpoints: ["{upstream}"], protocol: http2 }}
"#
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let worker = Worker::with_limits(proxy, H1Limits::default());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
    let _sweeping = tokio::task::spawn_local(worker.maintain());
    front
}

/// A tonic client of `front`.
async fn client(front: SocketAddr) -> tonic::client::Grpc<Channel> {
    let channel = Channel::from_shared(format!("http://{front}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    tonic::client::Grpc::new(channel)
}

fn method(path: &'static str) -> PathAndQuery {
    PathAndQuery::from_static(path)
}

/// Bounded, so that a call nobody answers fails the test rather than hangs it.
async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("did not finish")
}

#[tokio::test]
async fn all_four_call_shapes_go_through() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let front = proxy_to(upstream().await).await;
            let mut grpc = client(front).await;

            within(grpc.ready()).await.unwrap();
            let unary = within(grpc.unary(
                Request::new(Bytes::from_static(b"hello")),
                method("/test.Echo/Unary"),
                Raw,
            ))
            .await
            .unwrap();
            assert_eq!(unary.into_inner(), "hello");

            within(grpc.ready()).await.unwrap();
            let parts = tokio_stream::iter([
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]);
            let gathered = within(grpc.client_streaming(
                Request::new(parts),
                method("/test.Echo/Gather"),
                Raw,
            ))
            .await
            .unwrap();
            assert_eq!(gathered.into_inner(), "abc");

            within(grpc.ready()).await.unwrap();
            let scattered = within(grpc.server_streaming(
                Request::new(Bytes::from_static(b"xyz")),
                method("/test.Echo/Scatter"),
                Raw,
            ))
            .await
            .unwrap();
            let mut scattered = scattered.into_inner();
            let mut pieces = Vec::new();
            while let Some(piece) = within(scattered.next()).await {
                pieces.push(piece.unwrap());
            }
            assert_eq!(pieces, ["x", "y", "z"]);

            within(grpc.ready()).await.unwrap();
            let (sending, sent) = tokio::sync::mpsc::channel::<Bytes>(4);
            let outgoing = tokio_stream::wrappers::ReceiverStream::new(sent);
            let mirrored =
                within(grpc.streaming(Request::new(outgoing), method("/test.Echo/Mirror"), Raw));
            sending.send(Bytes::from_static(b"one")).await.unwrap();
            let mut mirrored = mirrored.await.unwrap().into_inner();
            assert_eq!(within(mirrored.next()).await.unwrap().unwrap(), "one");
            // Each goes back before the next is sent: the stream is carried both ways at
            // once, not gathered and handed over.
            sending.send(Bytes::from_static(b"two")).await.unwrap();
            assert_eq!(within(mirrored.next()).await.unwrap().unwrap(), "two");
            drop(sending);
            assert!(within(mirrored.next()).await.is_none());
        })
        .await;
}

#[tokio::test]
async fn the_upstreams_status_and_the_gateways_deadline_reach_the_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let front = proxy_to(upstream().await).await;
            let mut grpc = client(front).await;

            within(grpc.ready()).await.unwrap();
            let failed =
                within(grpc.unary(Request::new(Bytes::new()), method("/test.Echo/Fail"), Raw))
                    .await
                    .unwrap_err();
            assert_eq!(failed.code(), Code::NotFound);
            assert_eq!(failed.message(), "nobody by that name");
            // Its details, as binary metadata, as they came.
            assert_eq!(failed.details(), b"details");

            // The client's deadline goes up as `grpc-timeout` and ends the call long before
            // the upstream's second. tonic's client keeps the same deadline itself, from
            // before the call was sent, so it is usually first to give up and reports it
            // as CANCELLED; the gateway's own DEADLINE_EXCEEDED is pinned in its unit
            // tests (`a_grpc_calls_deadline_bounds_it_…`).
            within(grpc.ready()).await.unwrap();
            let mut call = Request::new(Bytes::new());
            call.set_timeout(Duration::from_millis(200));
            let asked = tokio::time::Instant::now();
            let exceeded = within(grpc.unary(call, method("/test.Echo/Slow"), Raw))
                .await
                .unwrap_err();
            assert!(
                matches!(exceeded.code(), Code::DeadlineExceeded | Code::Cancelled),
                "{exceeded:?}"
            );
            assert!(
                asked.elapsed() < Duration::from_millis(800),
                "{:?}",
                asked.elapsed()
            );

            // A method no route serves is one the gateway does not implement.
            within(grpc.ready()).await.unwrap();
            let unrouted =
                within(grpc.unary(Request::new(Bytes::new()), method("/other.Service/Do"), Raw))
                    .await
                    .unwrap_err();
            assert_eq!(unrouted.code(), Code::Unimplemented);
        })
        .await;
}
