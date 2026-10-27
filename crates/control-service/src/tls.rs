//! The stream's TLS: 1.3 alone, each end checked against the stream's own authorities.

use boring::error::ErrorStack;
use boring::ssl::{
    AlpnError, SslAcceptor, SslConnector, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion,
    select_next_proto,
};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use edgerush_certificates::{CertificateError, Identity};
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
}

/// HTTP/2, as gRPC is spoken, in ALPN's form.
const H2: &[u8] = b"\x02h2";

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

/// What both ends are set up with: TLS 1.3 alone, their own certificate, and the stream's
/// authorities, alone: the machine's own store is not consulted.
fn common(builder: &mut SslContextBuilder, credentials: &Credentials) -> Result<(), TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_3))
        .map_err(setup)?;
    builder
        .set_max_proto_version(Some(SslVersion::TLS1_3))
        .map_err(setup)?;
    let identity = Identity::read(&credentials.certificate)?;
    builder.set_certificate(&identity.leaf).map_err(setup)?;
    for intermediate in identity.intermediates {
        builder.add_extra_chain_cert(intermediate).map_err(setup)?;
    }
    builder.set_private_key(&identity.key).map_err(setup)?;
    let mut trusted = X509StoreBuilder::new().map_err(setup)?;
    for authority in authorities(&credentials.authorities)? {
        trusted.add_cert(authority).map_err(setup)?;
    }
    builder
        .set_verify_cert_store(trusted.build())
        .map_err(setup)?;
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
