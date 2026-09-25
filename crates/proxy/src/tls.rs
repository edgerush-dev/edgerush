//! TLS on the client hop ([03 §3](../../docs/03-data-plane.md)): BoringSSL, through
//! `boring` for the setup and `tokio-boring` for the connection.
//!
//! A listener's certificates are made into one acceptor when a config arrives, and a
//! config that has the same certificates keeps it: the keys its session tickets are
//! sealed with are the acceptor's, so a reload that does not touch them does not cost the
//! clients their resumption. Which certificate a client is given is decided by the name it
//! asks for (SNI) against the names the certificates themselves carry.
//!
//! Sessions are resumed by ticket only. BoringSSL's session cache is one table for all
//! the workers behind a lock, which every full handshake would write to.

use boring::error::ErrorStack;
use boring::pkey::PKey;
use boring::ssl::{
    AlpnError, NameType, SslAcceptor, SslAcceptorBuilder, SslContext, SslMethod,
    SslSessionCacheMode, SslVerifyMode, select_next_proto,
};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use edgerush_config::Certificate;
use std::collections::HashMap;
use std::sync::Arc;

/// What is offered to a client that asks for protocols (ALPN), in the order preferred:
/// HTTP/2, then HTTP/1.1, in the wire form (RFC 7301 §3.1). One that asks for neither, or
/// for nothing, is spoken to in HTTP/1.1 (RFC 9113 §3.2).
const PROTOCOLS: &[u8] = b"\x02h2\x08http/1.1";

/// The protocol a client that is to speak HTTP/2 was given.
pub(crate) const H2: &[u8] = b"h2";

/// The key exchanges, in the order preferred: the post-quantum hybrid first, then what
/// BoringSSL offers by default. BoringSSL's defaults leave the hybrid out.
const GROUPS: &str = "X25519MLKEM768:X25519:P-256:P-384";

/// A listener's TLS: what a connection is accepted with.
pub(crate) struct Tls {
    acceptor: SslAcceptor,
    /// What it was made from, which a later config's is compared with.
    source: edgerush_config::Tls,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("certificates", &self.source.certificates.len())
            .finish_non_exhaustive()
    }
}

/// A certificate that cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TlsError {
    /// The chain is not certificates in PEM.
    #[error("certificate {index}: the chain is not PEM certificates: {reason}")]
    Chain {
        /// Its position among the listener's certificates.
        index: usize,
        /// What BoringSSL said.
        reason: String,
    },
    /// The chain has no certificate in it.
    #[error("certificate {index}: the chain is empty")]
    Empty {
        /// Its position among the listener's certificates.
        index: usize,
    },
    /// The key is not a private key in PEM.
    #[error("certificate {index}: the key is not a PEM private key: {reason}")]
    Key {
        /// Its position among the listener's certificates.
        index: usize,
        /// What BoringSSL said.
        reason: String,
    },
    /// The key is not the one the certificate was issued for.
    #[error("certificate {index}: the key is not the certificate's")]
    Mismatch {
        /// Its position among the listener's certificates.
        index: usize,
    },
    /// An authority trusted to vouch for clients is not a certificate in PEM.
    #[error("client validation, authority {index}: not a PEM certificate: {reason}")]
    Authority {
        /// Its position among the authorities.
        index: usize,
        /// What was wrong with it.
        reason: String,
    },
    /// BoringSSL would not set up what every certificate is served with. Not known to
    /// happen: the settings are fixed.
    #[error("TLS cannot be set up: {0}")]
    Setup(String),
}

impl Tls {
    /// What `source`'s certificates are served with.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for the first certificate that cannot be used.
    pub(crate) fn new(source: &edgerush_config::Tls) -> Result<Self, TlsError> {
        let authorities = source
            .client_validation
            .as_ref()
            .map(|validation| authorities(&validation.authorities))
            .transpose()?;
        let mut names = Names::default();
        let mut others = Vec::with_capacity(source.certificates.len().saturating_sub(1));
        let mut first = None;
        for (index, certificate) in source.certificates.iter().enumerate() {
            let (mut builder, leaf) = context(certificate, index)?;
            // On every certificate's context: a client that asked for another name is moved
            // to that one's, and must be held to the same.
            if let Some(authorities) = &authorities {
                validate_clients(&mut builder, authorities)?;
            }
            names.add(&leaf, index);
            if first.is_none() {
                first = Some(builder);
            } else {
                others.push(builder.build().into_context());
            }
        }
        let Some(mut first) = first else {
            // The config model refuses an `https` listener with no certificate.
            return Err(TlsError::Empty { index: 0 });
        };
        let others: Arc<[SslContext]> = others.into();
        // The first certificate is the context every connection starts in; one that asked
        // for a name another certificate carries is moved to that one's.
        first.set_servername_callback(move |ssl, _alert| {
            let asked = ssl.servername(NameType::HOST_NAME);
            if let Some(other) = asked
                .map(|name| names.choose(name))
                .and_then(|chosen| chosen.checked_sub(1))
                .and_then(|other| others.get(other))
            {
                // Only fails for a context with no certificate, which none of these is.
                let _moved = ssl.set_ssl_context(other);
            }
            Ok(())
        });
        Ok(Self {
            acceptor: first.build(),
            source: source.clone(),
        })
    }

    /// What connections are accepted with.
    pub(crate) fn acceptor(&self) -> &SslAcceptor {
        &self.acceptor
    }

    /// Whether this was made from `source`, and can serve a config that has it.
    pub(crate) fn is_for(&self, source: &edgerush_config::Tls) -> bool {
        self.source == *source
    }
}

/// The certificates of the authorities trusted to vouch for clients, read.
fn authorities(pems: &[String]) -> Result<Vec<X509>, TlsError> {
    let mut read = Vec::new();
    for (index, pem) in pems.iter().enumerate() {
        let certificates =
            X509::stack_from_pem(pem.as_bytes()).map_err(|error| TlsError::Authority {
                index,
                reason: error.to_string(),
            })?;
        if certificates.is_empty() {
            return Err(TlsError::Authority {
                index,
                reason: "no certificate in it".to_owned(),
            });
        }
        read.extend(certificates);
    }
    Ok(read)
}

/// Has a context refuse a client that does not show a certificate `authorities` vouch
/// for, and name them to the client, as nginx and HAProxy do, so one with several
/// certificates can tell which to show.
fn validate_clients(
    builder: &mut SslAcceptorBuilder,
    authorities: &[X509],
) -> Result<(), TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut trusted = X509StoreBuilder::new().map_err(setup)?;
    for authority in authorities {
        trusted.add_cert(authority.clone()).map_err(setup)?;
        builder.add_client_ca(authority).map_err(setup)?;
    }
    // Only these: the machine's own store is not consulted.
    builder
        .set_verify_cert_store(trusted.build())
        .map_err(setup)?;
    builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    Ok(())
}

/// A context for one certificate, and the certificate itself, whose names are for the
/// caller to file.
fn context(
    certificate: &Certificate,
    index: usize,
) -> Result<(SslAcceptorBuilder, X509), TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    // TLS 1.2 and 1.3, and for 1.2 only forward-secret AEAD suites.
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).map_err(setup)?;
    builder.set_curves_list(GROUPS).map_err(setup)?;
    builder.set_session_cache_mode(SslSessionCacheMode::OFF);
    builder.set_alpn_select_callback(|_ssl, offered| {
        select_next_proto(PROTOCOLS, offered).ok_or(AlpnError::NOACK)
    });

    let chain =
        X509::stack_from_pem(certificate.chain.as_bytes()).map_err(|error| TlsError::Chain {
            index,
            reason: error.to_string(),
        })?;
    let mut chain = chain.into_iter();
    let leaf = chain.next().ok_or(TlsError::Empty { index })?;
    builder.set_certificate(&leaf).map_err(setup)?;
    for intermediate in chain {
        builder.add_extra_chain_cert(intermediate).map_err(setup)?;
    }
    let key =
        PKey::private_key_from_pem(certificate.key.as_bytes()).map_err(|error| TlsError::Key {
            index,
            reason: error.to_string(),
        })?;
    // Compared here, where it can be said which: BoringSSL refuses the pair as well, but
    // only as a failure to set the key.
    let matches = leaf.public_key().is_ok_and(|public| public.public_eq(&key));
    if !matches {
        return Err(TlsError::Mismatch { index });
    }
    builder.set_private_key(&key).map_err(setup)?;
    Ok((builder, leaf))
}

/// Which certificate answers for which name: by the DNS names in each one's subject
/// alternative names, exact names before wildcards, and among certificates that carry the
/// same name, the first. A name no certificate carries is answered by the first.
#[derive(Debug, Default)]
struct Names {
    /// By the name, in lower case.
    exact: HashMap<String, usize>,
    /// By what follows the `*.`, in lower case: a wildcard stands for one label, whole
    /// (RFC 6125 §6.4.3).
    wildcard: HashMap<String, usize>,
}

impl Names {
    fn add(&mut self, certificate: &X509, index: usize) {
        let Some(alternatives) = certificate.subject_alt_names() else {
            return;
        };
        for name in alternatives.iter().filter_map(|name| name.dnsname()) {
            self.add_name(name, index);
        }
    }

    fn add_name(&mut self, name: &str, index: usize) {
        let name = name.to_ascii_lowercase();
        match name.strip_prefix("*.") {
            Some(parent) => self.wildcard.entry(parent.to_owned()).or_insert(index),
            None => self.exact.entry(name).or_insert(index),
        };
    }

    /// The certificate for a client that asked for `name`.
    fn choose(&self, name: &str) -> usize {
        // Once for a handshake, which does far more than this.
        let name = name.to_ascii_lowercase();
        if let Some(&index) = self.exact.get(&name) {
            return index;
        }
        name.split_once('.')
            .filter(|(label, _)| !label.is_empty())
            .and_then(|(_, parent)| self.wildcard.get(parent))
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! Certificates made on the spot, for tests that need a server to present one.

    use boring::asn1::Asn1Time;
    use boring::bn::BigNum;
    use boring::ec::{EcGroup, EcKey};
    use boring::hash::MessageDigest;
    use boring::nid::Nid;
    use boring::pkey::PKey;
    use boring::x509::extension::SubjectAlternativeName;
    use boring::x509::{X509, X509NameBuilder};
    use edgerush_config::Certificate;

    /// A self-signed certificate for `names`, with its key.
    pub(crate) fn certificate(names: &[&str]) -> Certificate {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut subject = X509NameBuilder::new().unwrap();
        subject
            .append_entry_by_nid(Nid::COMMONNAME, names.first().copied().unwrap_or("none"))
            .unwrap();
        let subject = subject.build();
        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        let serial = BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();
        builder.set_serial_number(&serial).unwrap();
        builder.set_subject_name(&subject).unwrap();
        builder.set_issuer_name(&subject).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(30).unwrap())
            .unwrap();
        if !names.is_empty() {
            let mut alternatives = SubjectAlternativeName::new();
            for name in names {
                alternatives.dns(name);
            }
            let alternatives = alternatives
                .build(&builder.x509v3_context(None, None))
                .unwrap();
            builder.append_extension(&alternatives).unwrap();
        }
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        let chain = builder.build().to_pem().unwrap();
        Certificate {
            chain: String::from_utf8(chain).unwrap(),
            key: String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::certificate;
    use super::*;
    use proptest::prelude::*;

    fn names(certificates: &[&[&str]]) -> Names {
        let mut names = Names::default();
        for (index, carried) in certificates.iter().enumerate() {
            for name in *carried {
                names.add_name(name, index);
            }
        }
        names
    }

    #[test]
    fn a_name_is_answered_by_the_certificate_that_carries_it() {
        let names = names(&[&["default.test"], &["a.test", "*.b.test"], &["c.test"]]);
        assert_eq!(names.choose("a.test"), 1);
        assert_eq!(names.choose("c.test"), 2);
        assert_eq!(names.choose("default.test"), 0);
        // Names are not told apart by case.
        assert_eq!(names.choose("A.Test"), 1);
        // A wildcard stands for one whole label: not none, not two.
        assert_eq!(names.choose("x.b.test"), 1);
        assert_eq!(names.choose("b.test"), 0);
        assert_eq!(names.choose("y.x.b.test"), 0);
        assert_eq!(names.choose(".b.test"), 0);
        // Nobody's name: the first.
        assert_eq!(names.choose("elsewhere.test"), 0);
        assert_eq!(names.choose(""), 0);
    }

    #[test]
    fn an_exact_name_comes_before_a_wildcard_and_the_first_before_the_rest() {
        let names = names(&[&["*.x.test"], &["a.x.test"], &["a.x.test", "*.x.test"]]);
        assert_eq!(names.choose("a.x.test"), 1);
        assert_eq!(names.choose("b.x.test"), 0);
    }

    proptest! {
        /// Against a naive reading of the rules: every certificate's names in order, an
        /// exact match anywhere first, then the first wildcard that covers the name.
        #[test]
        fn choosing_agrees_with_a_naive_reading(
            certificates in prop::collection::vec(
                prop::collection::vec("(\\*\\.)?[ab]{1,2}(\\.[ab]{1,2}){0,2}", 0..4),
                1..5,
            ),
            asked in "[ab.]{0,8}",
        ) {
            let carried: Vec<Vec<&str>> = certificates
                .iter()
                .map(|names| names.iter().map(String::as_str).collect())
                .collect();
            let borrowed: Vec<&[&str]> = carried.iter().map(Vec::as_slice).collect();
            let chosen = names(&borrowed).choose(&asked);

            let exact = carried.iter().position(|names| names.contains(&asked.as_str()));
            let wildcard = carried.iter().position(|names| {
                names.iter().any(|name| {
                    name.strip_prefix("*.").is_some_and(|parent| {
                        asked
                            .split_once('.')
                            .is_some_and(|(label, rest)| !label.is_empty() && rest == parent)
                    })
                })
            });
            prop_assert_eq!(chosen, exact.or(wildcard).unwrap_or(0));
        }
    }

    #[test]
    fn a_listener_with_usable_certificates_is_set_up() {
        let source = edgerush_config::Tls {
            certificates: vec![certificate(&["a.test"]), certificate(&["b.test"])],
            client_validation: None,
        };
        let tls = Tls::new(&source).unwrap();
        assert!(tls.is_for(&source));
        let other = edgerush_config::Tls {
            certificates: vec![certificate(&["a.test"])],
            client_validation: None,
        };
        assert!(!tls.is_for(&other));
    }

    #[test]
    fn a_certificate_that_cannot_be_used_is_refused_and_said_which() {
        let good = certificate(&["a.test"]);
        let tls = |certificates: Vec<Certificate>| {
            Tls::new(&edgerush_config::Tls {
                certificates,
                client_validation: None,
            })
            .map(|_| ())
        };
        // Text with no PEM in it is no certificates; a PEM block that is not one is an error.
        let nothing = Certificate {
            chain: "not a certificate".to_owned(),
            ..good.clone()
        };
        assert_eq!(
            tls(vec![good.clone(), nothing]),
            Err(TlsError::Empty { index: 1 })
        );
        let broken = Certificate {
            chain: "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n"
                .to_owned(),
            ..good.clone()
        };
        assert!(matches!(
            tls(vec![good.clone(), broken]),
            Err(TlsError::Chain { index: 1, .. })
        ));
        let key = Certificate {
            key: "not a key".to_owned(),
            ..good.clone()
        };
        assert!(matches!(
            tls(vec![key]),
            Err(TlsError::Key { index: 0, .. })
        ));
        let mismatched = Certificate {
            key: certificate(&["b.test"]).key,
            ..good.clone()
        };
        assert_eq!(
            tls(vec![good, mismatched]),
            Err(TlsError::Mismatch { index: 1 })
        );
        let validated = |authorities: Vec<String>| {
            Tls::new(&edgerush_config::Tls {
                certificates: vec![certificate(&["a.test"])],
                client_validation: Some(edgerush_config::ClientValidation { authorities }),
            })
            .map(|_| ())
        };
        assert_eq!(validated(vec![certificate(&["ca"]).chain]), Ok(()));
        assert!(matches!(
            validated(vec![certificate(&["ca"]).chain, "no PEM".to_owned()]),
            Err(TlsError::Authority { index: 1, .. })
        ));
    }
}
