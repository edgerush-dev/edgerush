//! The stream's TLS: 1.3 alone, each end checked against the stream's own authorities. And
//! the admission webhook's: 1.3 alone too, its client not asked for a certificate.

use boring::error::ErrorStack;
use boring::ssl::{
    AlpnError, SslAcceptor, SslConnector, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion,
    select_next_proto,
};
use boring::x509::store::X509StoreBuilder;
use boring::x509::{X509, X509VerifyError};
use edgerush_certificates::{CertificateError, Identity, spiffe_id};
use edgerush_config::Certificate;

/// What one end presents, and whom it trusts to vouch for the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// Its certificate, and the key.
    pub certificate: Certificate,
    /// The certificates, in PEM, of the authorities trusted: the stream's own.
    pub authorities: String,
}

/// Why credentials cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TlsError {
    /// The certificate or its key.
    #[error("the certificate: {0}")]
    Certificate(#[from] CertificateError),
    /// The authorities are not PEM certificates, or none.
    #[error("the authorities: {0}")]
    Authorities(String),
    /// BoringSSL refused the set-up.
    #[error("setting up TLS: {0}")]
    Setup(String),
    /// A workload's identity: a certificate without a SPIFFE ID where one is needed, or an
    /// ID that is not one.
    #[error("the identity: {0}")]
    Identity(String),
}

/// HTTP/2, as gRPC is spoken, in ALPN's form.
const H2: &[u8] = b"\x02h2";

/// HTTP/2, then HTTP/1.1, in ALPN's form: what the webhook speaks, as its client offers.
const H2_OR_H1: &[u8] = b"\x02h2\x08http/1.1";

/// The control plane's TLS: it refuses a data plane without a certificate its authorities
/// vouch for.
///
/// # Errors
///
/// A [`TlsError`] for credentials that cannot be used.
pub fn acceptor(credentials: &Credentials) -> Result<SslAcceptor, TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).map_err(setup)?;
    common(&mut builder, credentials)?;
    let authorities = authorities(&credentials.authorities)?;
    for authority in &authorities {
        builder.add_client_ca(authority).map_err(setup)?;
    }
    builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    builder.set_alpn_select_callback(|_ssl, offered| {
        select_next_proto(H2, offered).ok_or(AlpnError::ALERT_FATAL)
    });
    Ok(builder.build())
}

/// The data plane's TLS: it refuses a control plane without a certificate its authorities
/// vouch for, or for another name than the one it dials.
///
/// # Errors
///
/// A [`TlsError`] for credentials that cannot be used.
pub fn connector(credentials: &Credentials) -> Result<SslConnector, TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut builder = SslConnector::builder(SslMethod::tls()).map_err(setup)?;
    common(&mut builder, credentials)?;
    builder.set_verify(SslVerifyMode::PEER);
    builder.set_alpn_protos(H2).map_err(setup)?;
    Ok(builder.build())
}

/// A connector as [`connector`]'s, that also refuses a peer whose certificate does not
/// carry the SPIFFE ID `peer`: the authorities establish trust, the ID says which workload
/// answered (12 item 22).
///
/// # Errors
///
/// A [`TlsError`] for credentials that cannot be used.
pub fn connector_to(credentials: &Credentials, peer: &str) -> Result<SslConnector, TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut builder = SslConnector::builder(SslMethod::tls()).map_err(setup)?;
    common(&mut builder, credentials)?;
    let peer = peer.to_owned();
    builder.set_verify_callback(SslVerifyMode::PEER, move |trusted, context| {
        // Depth 0 is the peer's own certificate, reached once the chain above it is checked.
        if !trusted || context.error_depth() > 0 {
            return trusted;
        }
        let named = context
            .current_cert()
            .and_then(spiffe_id)
            .is_some_and(|id| id == peer);
        if !named {
            context.set_error(Err(X509VerifyError::APPLICATION_VERIFICATION));
        }
        named
    });
    builder.set_alpn_protos(H2).map_err(setup)?;
    Ok(builder.build())
}

/// The admission webhook's TLS: it asks its client for no certificate, as the API server
/// presents none unless it is set up to (04 §3), and speaks HTTP/2 or HTTP/1.1, as it is
/// offered.
///
/// # Errors
///
/// A [`TlsError`] for a certificate that cannot be used.
pub fn webhook_acceptor(certificate: &Certificate) -> Result<SslAcceptor, TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).map_err(setup)?;
    presenting(&mut builder, certificate)?;
    builder.set_verify(SslVerifyMode::NONE);
    // A client that offers neither is spoken HTTP/1.1 to, as one that offers nothing is.
    builder.set_alpn_select_callback(|_ssl, offered| {
        select_next_proto(H2_OR_H1, offered).ok_or(AlpnError::NOACK)
    });
    Ok(builder.build())
}

/// What both ends are set up with: TLS 1.3 alone, their own certificate, and the stream's
/// authorities, alone: the machine's own store is not consulted.
fn common(builder: &mut SslContextBuilder, credentials: &Credentials) -> Result<(), TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    presenting(builder, &credentials.certificate)?;
    let mut trusted = X509StoreBuilder::new().map_err(setup)?;
    for authority in authorities(&credentials.authorities)? {
        trusted.add_cert(authority).map_err(setup)?;
    }
    builder
        .set_verify_cert_store(trusted.build())
        .map_err(setup)?;
    Ok(())
}

/// TLS 1.3 alone, presenting `certificate`.
fn presenting(builder: &mut SslContextBuilder, certificate: &Certificate) -> Result<(), TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_3))
        .map_err(setup)?;
    builder
        .set_max_proto_version(Some(SslVersion::TLS1_3))
        .map_err(setup)?;
    let identity = Identity::read(certificate)?;
    builder.set_certificate(&identity.leaf).map_err(setup)?;
    for intermediate in identity.intermediates {
        builder.add_extra_chain_cert(intermediate).map_err(setup)?;
    }
    builder.set_private_key(&identity.key).map_err(setup)?;
    Ok(())
}

fn authorities(pem: &str) -> Result<Vec<X509>, TlsError> {
    let read = X509::stack_from_pem(pem.as_bytes())
        .map_err(|error| TlsError::Authorities(error.to_string()))?;
    if read.is_empty() {
        return Err(TlsError::Authorities("no certificate in them".to_owned()));
    }
    Ok(read)
}
