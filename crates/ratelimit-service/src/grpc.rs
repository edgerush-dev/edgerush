//! The service over gRPC: `edgerush.ratelimit.v1.RateLimit`, written by hand rather than
//! generated, as its two methods are small.
//!
//! - `Exchange`: a data plane's batch, answered by the service pod's answers.
//! - `Replicate`: an owner's copies, for every other pod, or keys handed over.
//! - `Heartbeat`: a pod's heartbeat, for every other.

use crate::pod::Pod;
use crate::secure::{self, Caller, Dialler, refused};
use crate::wire;
use boring::ssl::SslConnector;
use bytes::Buf;
use edgerush_ratelimit::service::{self, membership};
use http::uri::PathAndQuery;
use std::convert::Infallible;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::watch;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::NamedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

/// The service's name, as gRPC calls it.
pub const SERVICE: &str = "edgerush.ratelimit.v1.RateLimit";
const EXCHANGE: &str = "/edgerush.ratelimit.v1.RateLimit/Exchange";
const REPLICATE: &str = "/edgerush.ratelimit.v1.RateLimit/Replicate";
pub(crate) const HEARTBEAT: &str = "/edgerush.ratelimit.v1.RateLimit/Heartbeat";

/// How long a call may take before it is given up: the protocol sends again what matters.
const CALL_TIMEOUT: Duration = Duration::from_secs(1);

/// How often a client pings its connection, idle or not, and how long it waits for the
/// answer before letting the connection go (#127): a call that times out leaves its
/// connection in place, so one that is open but never answers — a wedged peer whose
/// kernel still acknowledges, a middlebox — would otherwise take every later call with
/// it, for as long as it stays open.
const KEEP_ALIVE: Duration = Duration::from_secs(1);

/// The longest message either side sends or takes, set rather than left to tonic's
/// defaults: 4 MiB. What the protocol sends is cut to a quarter of it (06 §4), so that
/// a piece made by an estimate of its size, or an answer to it, always fits; a message
/// longer than this is refused before it is sent rather than after.
pub const MAX_MESSAGE: usize = 4 << 20;

/// Encodes `E` and decodes `D` as protobuf.
pub(crate) struct Prost<E, D>(PhantomData<fn(E) -> D>);

impl<E, D> Default for Prost<E, D> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<E, D> Codec for Prost<E, D>
where
    E: prost::Message + Send + 'static,
    D: prost::Message + Default + Send + 'static,
{
    type Encode = E;
    type Decode = D;
    type Encoder = Prost<E, D>;
    type Decoder = Prost<E, D>;

    fn encoder(&mut self) -> Self::Encoder {
        Self::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        Self::default()
    }
}

impl<E: prost::Message, D> Encoder for Prost<E, D> {
    type Item = E;
    type Error = Status;

    fn encode(&mut self, item: E, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.reserve(item.encoded_len());
        item.encode(dst)
            .map_err(|error| Status::internal(error.to_string()))
    }
}

impl<E, D: prost::Message + Default> Decoder for Prost<E, D> {
    type Item = D;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<D>, Status> {
        let length = src.remaining();
        D::decode(src.copy_to_bytes(length))
            .map(Some)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }
}

pub(crate) type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The service as a gRPC server, in front of a service pod.
#[derive(Clone)]
pub struct Server {
    pod: Pod,
}

impl Server {
    /// Serves `pod`.
    #[must_use]
    pub fn new(pod: Pod) -> Self {
        Self { pod }
    }
}

impl NamedService for Server {
    const NAME: &'static str = SERVICE;
}

struct Exchange(Pod);

impl tonic::server::UnaryService<wire::Batch> for Exchange {
    type Response = wire::Answers;
    type Future = Boxed<Result<Response<wire::Answers>, Status>>;

    fn call(&mut self, request: Request<wire::Batch>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let batch = service::Batch::try_from(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let answers = pod
                .batch(batch)
                .await
                .ok_or_else(|| Status::unavailable("the service pod is stopping"))?;
            Ok(Response::new(answers.into()))
        })
    }
}

struct Replicate(Pod);

impl tonic::server::UnaryService<wire::Replicas> for Replicate {
    type Response = wire::Empty;
    type Future = Boxed<Result<Response<wire::Empty>, Status>>;

    fn call(&mut self, request: Request<wire::Replicas>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let replicas = service::Replicas::try_from(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            pod.replicas(replicas).await;
            Ok(Response::new(wire::Empty {}))
        })
    }
}

struct Beat(Pod);

impl tonic::server::UnaryService<wire::Heartbeat> for Beat {
    type Response = wire::Empty;
    type Future = Boxed<Result<Response<wire::Empty>, Status>>;

    fn call(&mut self, request: Request<wire::Heartbeat>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let beat = membership::Heartbeat::try_from(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            pod.heartbeat(beat).await;
            Ok(Response::new(wire::Empty {}))
        })
    }
}

/// A server's side of a call, held to [`MAX_MESSAGE`] both ways.
pub(crate) fn limited<C: Codec>(codec: C) -> tonic::server::Grpc<C> {
    tonic::server::Grpc::new(codec)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE)
}

impl tower_service::Service<http::Request<tonic::body::Body>> for Server {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let pod = self.pod.clone();
        Box::pin(async move {
            let allowed = match request.uri().path() {
                EXCHANGE => Caller::DataPlane,
                _ => Caller::Pod,
            };
            if let Some(status) = refused(&request, allowed) {
                return Ok(status.into_http());
            }
            Ok(match request.uri().path() {
                EXCHANGE => {
                    let mut grpc = limited(Prost::<wire::Answers, wire::Batch>::default());
                    grpc.unary(Exchange(pod), request).await
                }
                HEARTBEAT => {
                    let mut grpc = limited(Prost::<wire::Empty, wire::Heartbeat>::default());
                    grpc.unary(Beat(pod), request).await
                }
                REPLICATE => {
                    let mut grpc = limited(Prost::<wire::Empty, wire::Replicas>::default());
                    grpc.unary(Replicate(pod), request).await
                }
                _ => Status::unimplemented("no such method").into_http(),
            })
        })
    }
}

/// Why a call to a service pod failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The address is not one to connect to.
    #[error("an address that cannot be connected to: {0}")]
    Address(#[from] tonic::transport::Error),
    /// The call failed.
    #[error("the call failed: {0}")]
    Status(#[from] Status),
    /// The answer cannot be used.
    #[error("an answer that cannot be used: {0}")]
    Invalid(#[from] wire::Invalid),
    /// The address names no host to check a certificate against.
    #[error("an address with no host")]
    NoHost,
    /// The message is longer than [`MAX_MESSAGE`]: not sent.
    #[error("a message of {0} bytes, longer than the {MAX_MESSAGE} a call takes")]
    TooLong(usize),
}

/// Where a client connects, and how its calls are timed and its connection watched.
fn endpoint(address: &str) -> Result<Endpoint, Error> {
    Ok(Endpoint::from_shared(format!("http://{address}"))?
        .timeout(CALL_TIMEOUT)
        .http2_keep_alive_interval(KEEP_ALIVE)
        .keep_alive_timeout(KEEP_ALIVE)
        .keep_alive_while_idle(true))
}

/// A client of one service pod. Cheap to clone: clones share the connection, made when
/// first needed and again when lost.
#[derive(Clone, Debug)]
pub struct Client {
    channel: Channel,
}

impl Client {
    /// A client of the service pod at `address` (`host:port`), spoken to plain.
    ///
    /// # Errors
    ///
    /// When `address` cannot be made into a URI.
    pub fn new(address: &str) -> Result<Self, Error> {
        let channel = endpoint(address)?
            .connect_timeout(CALL_TIMEOUT)
            .tcp_nodelay(true)
            .connect_lazy();
        Ok(Self { channel })
    }

    /// A client of the service pod at `address` (`host:port`), spoken to with the latest
    /// of `tls` as each connection is made: its certificate checked against the host.
    ///
    /// # Errors
    ///
    /// When `address` cannot be made into a URI, or names no host.
    pub fn secured(address: &str, tls: watch::Receiver<SslConnector>) -> Result<Self, Error> {
        let endpoint = endpoint(address)?;
        let name = secure::host(endpoint.uri()).ok_or(Error::NoHost)?;
        let dialler = Dialler::new(address, name, tls, CALL_TIMEOUT);
        let channel = endpoint.connect_with_connector_lazy(dialler);
        Ok(Self { channel })
    }

    pub(crate) async fn call<E, D>(&self, message: E, path: &'static str) -> Result<D, Error>
    where
        E: prost::Message + Send + 'static,
        D: prost::Message + Default + Send + 'static,
    {
        // Said here, rather than left to tonic, whose refusal on the way out resets the
        // stream and says only that.
        let length = message.encoded_len();
        if length > MAX_MESSAGE {
            return Err(Error::TooLong(length));
        }
        let mut grpc = tonic::client::Grpc::new(self.channel.clone())
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        grpc.ready()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let response = grpc
            .unary(
                Request::new(message),
                PathAndQuery::from_static(path),
                Prost::<E, D>::default(),
            )
            .await?;
        Ok(response.into_inner())
    }

    /// Sends a data plane's batch; the service pod's answers come back.
    ///
    /// # Errors
    ///
    /// When the call fails or its answer cannot be used.
    pub async fn exchange(&self, batch: service::Batch) -> Result<service::Answers, Error> {
        let answers: wire::Answers = self.call(wire::Batch::from(batch), EXCHANGE).await?;
        Ok(answers.into())
    }

    /// Sends a pod's heartbeat.
    ///
    /// # Errors
    ///
    /// When the call fails.
    pub async fn heartbeat(&self, beat: membership::Heartbeat) -> Result<(), Error> {
        let _: wire::Empty = self.call(wire::Heartbeat::from(beat), HEARTBEAT).await?;
        Ok(())
    }

    /// Sends copies to another pod, or keys handed over to their new owner.
    ///
    /// # Errors
    ///
    /// When the call fails.
    pub async fn replicate(&self, replicas: service::Replicas) -> Result<(), Error> {
        let _: wire::Empty = self.call(wire::Replicas::from(replicas), REPLICATE).await?;
        Ok(())
    }
}
