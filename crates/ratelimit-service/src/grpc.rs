//! What the store's gRPC calls share (`edgerush.cluster.v1.Cluster`, [`crate::cluster`]),
//! written by hand rather than generated: protobuf as the codec, the longest message either
//! side takes, how a call is timed and its connection watched, and a client of one pod for
//! its unary calls.

use crate::secure::{self, Dialler};
use boring::ssl::SslConnector;
use bytes::Buf;
use http::uri::PathAndQuery;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::watch;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

/// How long a call may take before it is given up: the protocol sends again what matters.
pub(crate) const CALL_TIMEOUT: Duration = Duration::from_secs(1);

/// How often a client pings its connection, idle or not, and how long it waits for the
/// answer before letting the connection go (#127): a call that times out leaves its
/// connection in place, so one that is open but never answers — a wedged peer whose
/// kernel still acknowledges, a middlebox — would otherwise take every later call with
/// it, for as long as it stays open.
pub(crate) const KEEP_ALIVE: Duration = Duration::from_secs(1);

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

/// A server's side of a call, held to [`MAX_MESSAGE`] both ways.
pub(crate) fn limited<C: Codec>(codec: C) -> tonic::server::Grpc<C> {
    tonic::server::Grpc::new(codec)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::task::{Context, Poll};
    use tokio::net::TcpListener;
    use tonic::Response;
    use tonic::server::NamedService;
    use tonic::transport::server::TcpIncoming;

    /// A message of any length.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Padded {
        #[prost(bytes = "vec", tag = "1")]
        body: Vec<u8>,
    }

    const ECHO: &str = "/edgerush.test.v1.Echo/Echo";

    /// A server answering each call with what it was sent, held to [`MAX_MESSAGE`] as the
    /// store's are ([`limited`]).
    #[derive(Clone)]
    struct Echo;

    impl NamedService for Echo {
        const NAME: &'static str = "edgerush.test.v1.Echo";
    }

    impl tonic::server::UnaryService<Padded> for Echo {
        type Response = Padded;
        type Future = Boxed<Result<Response<Padded>, Status>>;

        fn call(&mut self, request: Request<Padded>) -> Self::Future {
            Box::pin(async move { Ok(Response::new(request.into_inner())) })
        }
    }

    impl tower_service::Service<http::Request<tonic::body::Body>> for Echo {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = Boxed<Result<Self::Response, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
            Box::pin(async move {
                let mut grpc = limited(Prost::<Padded, Padded>::default());
                Ok(grpc.unary(Self, request).await)
            })
        }
    }

    /// Messages of up to 4 MiB go through, both ways; a longer one is refused by the client
    /// that would send it, before it is sent, not by the server after it has been.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn messages_are_held_to_4_mib_by_whoever_sends_them() {
        assert_eq!(MAX_MESSAGE, 4 << 20);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(Echo, TcpIncoming::from(listener))
                .await
                .unwrap();
        });
        let client = Client::new(&address).unwrap();
        let padded = |bytes: usize| Padded {
            body: vec![b'k'; bytes],
        };
        let answer: Padded = client.call(padded(MAX_MESSAGE - 1024), ECHO).await.unwrap();
        assert_eq!(answer.body.len(), MAX_MESSAGE - 1024);
        let refused = client.call::<_, Padded>(padded(MAX_MESSAGE), ECHO).await;
        assert!(
            matches!(refused, Err(Error::TooLong(length)) if length > MAX_MESSAGE),
            "{refused:?}"
        );
    }
}
