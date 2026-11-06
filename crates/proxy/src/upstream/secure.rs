//! TLS to an upstream's endpoints ([03 §3](../../../../docs/03-data-plane.md)): whom they
//! must be, whom to trust to say so, and what to speak once connected.
//!
//! Made once for each upstream when a config arrives and kept by a later config with the
//! same TLS, as a listener's is. Only the authorities the config names are trusted — not
//! the machine's — and the endpoint's certificate must carry the configured server name,
//! or, where the config names others to verify, one of those among its subject alternative
//! names, the server name then asked for alone.
//! An HTTP/2 upstream must agree on `h2` in the handshake: one that does not is a failed
//! connection, never one spoken to in HTTP/1.1 instead (15 §5).

use crate::gathered::Gathered;
use crate::tls::TlsError;
use crate::upstream::dial::Counted;
use crate::upstream::h1::pool::Close;
use boring::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use boring::x509::X509Ref;
use boring::x509::store::X509StoreBuilder;
use edgerush_config::{CompiledUpstreamTls, UpstreamProtocol, VerifiedName};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_boring::SslStream;

/// The key exchanges offered, as a listener offers them.
const GROUPS: &str = "X25519MLKEM768:X25519:P-256:P-384";

/// What connections to an upstream's endpoints are secured with.
pub(crate) struct Secure {
    connector: SslConnector,
    /// What it was made from, which a later snapshot's is compared with.
    source: CompiledUpstreamTls,
    protocol: UpstreamProtocol,
    /// `source`'s verified names, shared with each handshake's check of them.
    verified_names: Arc<[VerifiedName]>,
}

/// Whether `certificate` carries one of `names` among its subject alternative names.
fn carries(certificate: &X509Ref, names: &[VerifiedName]) -> bool {
    let Some(alternatives) = certificate.subject_alt_names() else {
        return false;
    };
    names.iter().any(|name| {
        alternatives.iter().any(|alternative| match name {
            VerifiedName::Dns(reference) => alternative
                .dnsname()
                .is_some_and(|presented| dns_matches(presented, reference)),
            VerifiedName::Uri(reference) => alternative.uri() == Some(reference.as_str()),
        })
    })
}

/// Whether a certificate's DNS name `presented` is `reference`, letters' case aside: a
/// wildcard of a whole leftmost label over two or more labels matches one label in its
/// place (RFC 6125 §6.4.3); so a `reference` that is itself a wildcard, its `*` a label
/// as any, matches only the same wildcard.
fn dns_matches(presented: &str, reference: &str) -> bool {
    match presented.strip_prefix("*.") {
        Some(base) => {
            base.contains('.')
                && reference.split_once('.').is_some_and(|(label, rest)| {
                    !label.is_empty() && rest.eq_ignore_ascii_case(base)
                })
        }
        None => presented.eq_ignore_ascii_case(reference),
    }
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
    /// A [`TlsError`] for an authority that cannot be read as a certificate, or a client
    /// certificate that cannot be used.
    pub(crate) fn new(
        source: &CompiledUpstreamTls,
        protocol: UpstreamProtocol,
    ) -> Result<Self, TlsError> {
        let setup = |error: boring::error::ErrorStack| TlsError::Setup(error.to_string());
        let mut builder = SslConnector::builder(SslMethod::tls()).map_err(setup)?;
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(setup)?;
        builder.set_curves_list(GROUPS).map_err(setup)?;
        builder.set_verify(SslVerifyMode::PEER);
        let mut trusted = X509StoreBuilder::new().map_err(setup)?;
        for authority in crate::tls::authorities(&source.authorities)? {
            trusted.add_cert(authority).map_err(setup)?;
        }
        // Only these: the machine's own store, which the builder loaded, is not consulted.
        builder
            .set_verify_cert_store(trusted.build())
            .map_err(setup)?;
        if let Some(certificate) = &source.client_certificate {
            let identity = crate::tls::identity(certificate)?;
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
            verified_names: source.verified_names.clone().into(),
        })
    }

    /// What it was made from.
    pub(crate) fn source(&self) -> &CompiledUpstreamTls {
        &self.source
    }

    /// Whether this was made from `source` for `protocol`, and can serve a config that
    /// has them.
    pub(crate) fn is_for(&self, source: &CompiledUpstreamTls, protocol: UpstreamProtocol) -> bool {
        self.source == *source && self.protocol == protocol
    }

    /// Secures `socket`: the handshake, the endpoint's certificate checked against the
    /// trusted authorities and the server name, and for HTTP/2 the protocol agreed on.
    ///
    /// # Errors
    ///
    /// An I/O error for a handshake that fails, a certificate that is not trusted or not
    /// the server's, or an HTTP/2 upstream that did not agree on `h2`.
    pub(crate) async fn connect<S>(&self, socket: S) -> io::Result<SslStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin + std::fmt::Debug,
    {
        let refused = |why: String| io::Error::new(io::ErrorKind::ConnectionRefused, why);
        let mut configured = self
            .connector
            .configure()
            .map_err(|error| refused(error.to_string()))?;
        // Names checked in place of the server name, which is then asked for alone: the
        // endpoint's own certificate, once its chain is trusted, must carry one of them.
        if !self.verified_names.is_empty() {
            configured.set_verify_hostname(false);
            let names = Arc::clone(&self.verified_names);
            configured.set_verify_callback(SslVerifyMode::PEER, move |trusted, context| {
                trusted
                    && (context.error_depth() != 0
                        || context
                            .current_cert()
                            .is_some_and(|leaf| carries(leaf, &names)))
            });
        }
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
    Plain(Counted),
    /// TLS over TCP, a request's pieces gathered into records ([`Gathered`]).
    Secured(Gathered<SslStream<Counted>>),
}

impl Close for Socket {
    /// Over TLS, the closure alert, if the socket takes it at once; over TCP the close says
    /// it. Nothing waits: a waker that wakes nobody, as the question is only whether it can
    /// go now.
    fn close(self) {
        if let Self::Secured(mut socket) = self {
            let mut context = Context::from_waker(Waker::noop());
            let _told = Pin::new(&mut socket).poll_shutdown(&mut context);
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{VerifiedName, carries, dns_matches};
    use boring::x509::X509;

    /// A certificate's DNS name is the reference, case aside, or a wildcard of one whole
    /// leftmost label over at least two labels standing for exactly one; a reference that
    /// is a wildcard is matched by that wildcard alone.
    #[test]
    fn a_dns_name_matches_as_rfc_6125_has_it() {
        for (presented, reference) in [
            ("backend.test", "backend.test"),
            ("Backend.TEST", "backend.test"),
            ("*.zone.test", "a.zone.test"),
            ("*.Zone.test", "A.zone.TEST"),
            ("*.zone.test", "*.zone.test"),
        ] {
            assert!(
                dns_matches(presented, reference),
                "{presented} for {reference}"
            );
        }
        for (presented, reference) in [
            ("backend.test", "other.test"),
            ("backend.test", "a.backend.test"),
            ("*.zone.test", "zone.test"),
            ("*.zone.test", "a.b.zone.test"),
            ("*.zone.test", ".zone.test"),
            // Over a single label, a wildcard stands for nothing.
            ("*.test", "a.test"),
            // A partial wildcard is not one.
            ("a*.zone.test", "ab.zone.test"),
            ("a.zone.test", "*.zone.test"),
            ("*.other.test", "*.zone.test"),
        ] {
            assert!(
                !dns_matches(presented, reference),
                "{presented} for {reference}"
            );
        }
    }

    /// A certificate carries a name among its subject alternative names: a DNS name as
    /// `dns_matches` has it, a URI exactly; its subject's common name never counts.
    #[test]
    fn a_certificate_carries_its_alternative_names_alone() {
        let leaf = |names: &[&str]| {
            X509::from_pem(crate::tls::testing::certificate(names).chain.as_bytes()).unwrap()
        };
        let dns = |name: &str| VerifiedName::Dns(name.to_owned());
        let uri = |name: &str| VerifiedName::Uri(name.to_owned());
        let shown = leaf(&["web.test", "*.zone.test", "spiffe://cluster/ns/shop/sa/web"]);
        assert!(carries(&shown, &[dns("web.test")]));
        assert!(carries(&shown, &[dns("a.zone.test")]));
        assert!(carries(&shown, &[uri("spiffe://cluster/ns/shop/sa/web")]));
        assert!(carries(
            &shown,
            &[dns("other.test"), uri("spiffe://cluster/ns/shop/sa/web")]
        ));
        assert!(!carries(&shown, &[dns("other.test")]));
        assert!(!carries(
            &shown,
            &[uri("spiffe://cluster/ns/shop/sa/other")]
        ));
        assert!(!carries(&shown, &[uri("SPIFFE://cluster/ns/shop/sa/web")]));
        // A DNS name is not a URI, nor a URI a DNS name.
        assert!(!carries(&shown, &[uri("web.test")]));
        assert!(!carries(&shown, &[dns("spiffe://cluster/ns/shop/sa/web")]));
        assert!(!carries(&shown, &[]));
        // A certificate of no alternative names, its subject `none`, carries nothing.
        assert!(!carries(&leaf(&[]), &[dns("none")]));
    }
}
