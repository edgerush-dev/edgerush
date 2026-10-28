//! The config stream over gRPC and mutual TLS ([02 §4](../../../docs/02-architecture.md)):
//! `edgerush.control.v1.Control/Subscribe`, the control plane's server and the data
//! plane's client, around the protocol of [`edgerush_control_stream`].
//!
//! A data plane dials the control plane and opens one stream: up go what it holds, then a
//! report after each config; down come the resources of each offer it lacks. TLS is 1.3
//! alone, and each end checks the other's certificate against the stream's own
//! authorities; the data plane checks the control plane's name too, as it dials it. Any
//! data plane the authorities vouch for is served: none is told apart by its name yet. Each
//! end's certificate files are read again as they change ([`reread`]).
//!
//! Beside it, the control plane serves the admission webhook's listener, [`webhook`].

use bytes::Buf;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

mod client;
mod files;
mod server;
mod subscribe;
mod tls;
pub mod webhook;

pub use boring::ssl::{SslAcceptor, SslConnector};
pub use client::{Ended, follow};
pub use files::{CertificateFiles, Files, FilesError, Reread, reread};
pub use server::{Heard, ServeError, serve};
pub use subscribe::{Backoff, Following, Subscription, subscribe};
pub use tls::{Credentials, TlsError, acceptor, connector, webhook_acceptor};

/// The service's name, as gRPC calls it.
pub const SERVICE: &str = "edgerush.control.v1.Control";
const SUBSCRIBE: &str = "/edgerush.control.v1.Control/Subscribe";

/// Encodes `E` and decodes `D` as protobuf.
struct Prost<E, D>(PhantomData<fn(E) -> D>);

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

/// What a channel receives, as the stream tonic sends from.
struct Received<T>(mpsc::Receiver<T>);

impl<T> futures_core::Stream for Received<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.0.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests;
