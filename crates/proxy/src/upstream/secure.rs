//! TLS to an upstream's endpoints ([03 §3](../../../../docs/03-data-plane.md)): whom they
//! must be, whom to trust to say so, and what to speak once connected.
//!
//! Made once for each upstream when a config arrives and kept by a later config with the
//! same TLS, as a listener's is. Only the authorities the config names are trusted — not
//! the machine's — and the endpoint's certificate must carry the configured server name.
//! An HTTP/2 upstream must agree on `h2` in the handshake: one that does not is a failed
//! connection, never one spoken to in HTTP/1.1 instead (15 §5).

use crate::gathered::Gathered;
use crate::tls::TlsError;
use boring::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use edgerush_config::{UpstreamProtocol, UpstreamTls};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_boring::SslStream;

/// The key exchanges offered, as a listener offers them.
const GROUPS: &str = "X25519MLKEM768:X25519:P-256:P-384";

/// What connections to an upstream's endpoints are secured with.
pub(crate) struct Secure {
    connector: SslConnector,
    /// What it was made from, which a later config's is compared with.
    source: UpstreamTls,
    protocol: UpstreamProtocol,
}

impl std::fmt::Debug for Secure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Secure")
            .field("server_name", &self.source.server_name)
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

impl Secure {
    /// What `source` secures connections spoken to in `protocol` with.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for an authority that cannot be read as a certificate.
    pub(crate) fn new(source: &UpstreamTls, protocol: UpstreamProtocol) -> Result<Self, TlsError> {
        let setup = |error: boring::error::ErrorStack| TlsError::Setup(error.to_string());
        let mut builder = SslConnector::builder(SslMethod::tls()).map_err(setup)?;
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(setup)?;
        builder.set_curves_list(GROUPS).map_err(setup)?;
        builder.set_verify(SslVerifyMode::PEER);
        let mut trusted = X509StoreBuilder::new().map_err(setup)?;
        for (index, authority) in source.authorities.iter().enumerate() {
            let certificates =
                X509::stack_from_pem(authority.as_bytes()).map_err(|error| TlsError::Chain {
                    index,
                    reason: error.to_string(),
                })?;
            if certificates.is_empty() {
                return Err(TlsError::Empty { index });
            }
            for certificate in certificates {
                trusted.add_cert(certificate).map_err(setup)?;
            }
        }
        // Only these: the machine's own store, which the builder loaded, is not consulted.
        builder
            .set_verify_cert_store(trusted.build())
            .map_err(setup)?;
        if let Some(certificate) = &source.client_certificate {
            let identity = crate::tls::Identity::read(certificate, 0)?;
            builder.set_certificate(&identity.leaf).map_err(setup)?;
            for intermediate in identity.intermediates {
                builder.add_extra_chain_cert(intermediate).map_err(setup)?;
            }
            builder.set_private_key(&identity.key).map_err(setup)?;
        }
        let offered: &[u8] = match protocol {
            UpstreamProtocol::Http1 => b"\x08http/1.1",
            UpstreamProtocol::Http2 => b"\x02h2",
        };
        builder.set_alpn_protos(offered).map_err(setup)?;
        Ok(Self {
            connector: builder.build(),
            source: source.clone(),
            protocol,
        })
    }

    /// What it was made from.
    pub(crate) fn source(&self) -> &UpstreamTls {
        &self.source
    }

    /// Whether this was made from `source` for `protocol`, and can serve a config that
    /// has them.
    pub(crate) fn is_for(&self, source: &UpstreamTls, protocol: UpstreamProtocol) -> bool {
        self.source == *source && self.protocol == protocol
    }

    /// Secures `socket`: the handshake, the endpoint's certificate checked against the
    /// trusted authorities and the server name, and for HTTP/2 the protocol agreed on.
    ///
    /// # Errors
    ///
    /// An I/O error for a handshake that fails, a certificate that is not trusted or not
    /// the server's, or an HTTP/2 upstream that did not agree on `h2`.
    pub(crate) async fn connect(&self, socket: TcpStream) -> io::Result<SslStream<TcpStream>> {
        let refused = |why: String| io::Error::new(io::ErrorKind::ConnectionRefused, why);
        let configured = self
            .connector
            .configure()
            .map_err(|error| refused(error.to_string()))?;
        let secured = tokio_boring::connect(configured, &self.source.server_name, socket)
            .await
            .map_err(|error| refused(format!("{error:?}")))?;
        if self.protocol == UpstreamProtocol::Http2
            && secured.ssl().selected_alpn_protocol() != Some(&b"h2"[..])
        {
            return Err(refused("the upstream did not agree on h2".to_owned()));
        }
        Ok(secured)
    }
}

/// A connection to an upstream, as the HTTP/1 client and its pool hold it: plain, or
/// secured.
#[derive(Debug)]
pub(crate) enum Socket {
    /// Plain TCP.
    Plain(TcpStream),
    /// TLS over TCP, a request's pieces gathered into records ([`Gathered`]).
    Secured(Gathered<SslStream<TcpStream>>),
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_read(cx, buf),
            Self::Secured(socket) => Pin::new(socket).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_write(cx, buf),
            Self::Secured(socket) => Pin::new(socket).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_write_vectored(cx, bufs),
            Self::Secured(socket) => Pin::new(socket).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(socket) => socket.is_write_vectored(),
            Self::Secured(socket) => socket.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_flush(cx),
            Self::Secured(socket) => Pin::new(socket).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_shutdown(cx),
            Self::Secured(socket) => Pin::new(socket).poll_shutdown(cx),
        }
    }
}
