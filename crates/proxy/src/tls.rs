//! TLS on the client hop ([03 §3](../../docs/03-data-plane.md)): BoringSSL, through
//! `boring` for the setup and `tokio-boring` for the connection.
//!
//! Every connection to a listener starts in one context of the listener's that holds no
//! certificate — its front — and is moved to the context of the certificate for the name
//! it asks for (SNI), against the names the certificates themselves carry. Session
//! tickets are sealed with the front's keys, which BoringSSL rotates every two days, and
//! the front is kept by every config that validates clients as it does: new certificates
//! are new contexts behind the same front, so rotating them does not cost the clients
//! their resumption. The keys never leave the pod.
//!
//! Sessions are resumed by ticket only. BoringSSL's session cache is one table for all
//! the workers behind a lock, which every full handshake would write to.

use arc_swap::ArcSwap;
use boring::error::ErrorStack;
use boring::pkey::PKey;
use boring::ssl::{
    AlpnError, NameType, SslAcceptor, SslAcceptorBuilder, SslContext, SslMethod,
    SslSessionCacheMode, SslVerifyMode, select_next_proto,
};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use edgerush_config::{Certificate, ClientValidation};
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
    front: Arc<Front>,
    /// Its certificates, which the front serves once they are installed.
    certificates: Arc<Certificates>,
    /// What it was made from, which a later config's is compared with.
    source: edgerush_config::Tls,
}

/// Where every connection to a listener starts, and whose keys seal its session tickets.
struct Front {
    acceptor: SslAcceptor,
    /// The clients it was made to validate, which a later config must too to keep it.
    validation: Option<ClientValidation>,
    /// The certificates it moves connections to.
    serving: Arc<ArcSwap<Certificates>>,
}

/// A listener's certificates, each in a context of its own, and which answers for which
/// name.
struct Certificates {
    names: Names,
    contexts: Vec<SslContext>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("certificates", &self.certificates.contexts.len())
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
    /// What `source`'s certificates are served with, behind a front of its own.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for the first certificate or authority that cannot be used.
    pub(crate) fn new(source: &edgerush_config::Tls) -> Result<Self, TlsError> {
        let authorities = authorities_of(source)?;
        let certificates = Arc::new(Certificates::new(source, authorities.as_deref())?);
        let serving = Arc::new(ArcSwap::new(Arc::clone(&certificates)));
        let front = Front::acceptor(authorities.as_deref(), &serving)?;
        Ok(Self {
            front: Arc::new(Front {
                acceptor: front,
                validation: source.client_validation.clone(),
                serving,
            }),
            certificates,
            source: source.clone(),
        })
    }

    /// What `source` is served with where `previous` was: behind `previous`'s front, and
    /// so with its ticket keys, if both validate clients alike; otherwise as new. The
    /// certificates are served once [`Tls::install`] is called.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for the first certificate or authority that cannot be used.
    pub(crate) fn after(previous: &Self, source: &edgerush_config::Tls) -> Result<Self, TlsError> {
        if previous.front.validation != source.client_validation {
            return Self::new(source);
        }
        let authorities = authorities_of(source)?;
        Ok(Self {
            front: Arc::clone(&previous.front),
            certificates: Arc::new(Certificates::new(source, authorities.as_deref())?),
            source: source.clone(),
        })
    }

    /// Serves these certificates from now on, in place of whatever the front served.
    pub(crate) fn install(&self) {
        self.front.serving.store(Arc::clone(&self.certificates));
    }

    /// What connections are accepted with.
    pub(crate) fn acceptor(&self) -> &SslAcceptor {
        &self.front.acceptor
    }

    /// Whether this was made from `source`, and can serve a config that has it.
    pub(crate) fn is_for(&self, source: &edgerush_config::Tls) -> bool {
        self.source == *source
    }

    /// Whether this and `other` seal session tickets with the same keys.
    #[cfg(test)]
    pub(crate) fn shares_keys_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.front, &other.front)
    }
}

impl Front {
    /// Its context: one with no certificate, which moves every connection to the certificate it
    /// asked for among those `serving` holds at the time.
    fn acceptor(
        authorities: Option<&[X509]>,
        serving: &Arc<ArcSwap<Certificates>>,
    ) -> Result<SslAcceptor, TlsError> {
        let mut builder = protocol()?;
        // What a connection is held to is set when it starts, here: the certificate's
        // context it is moved to does not change it.
        if let Some(authorities) = authorities {
            validate_clients(&mut builder, authorities)?;
        }
        let serving = Arc::clone(serving);
        // Called for every handshake, a name asked for or not.
        builder.set_servername_callback(move |ssl, _alert| {
            let certificates = serving.load();
            let asked = ssl.servername(NameType::HOST_NAME).unwrap_or("");
            let chosen = certificates.names.choose(asked);
            if let Some(context) = certificates.contexts.get(chosen) {
                // Only fails for a context with no certificate, which none of these is.
                let _moved = ssl.set_ssl_context(context);
            }
            Ok(())
        });
        Ok(builder.build())
    }
}

impl Certificates {
    fn new(source: &edgerush_config::Tls, authorities: Option<&[X509]>) -> Result<Self, TlsError> {
        let mut names = Names::default();
        let mut contexts = Vec::with_capacity(source.certificates.len());
        for (index, certificate) in source.certificates.iter().enumerate() {
            let (mut builder, leaf) = context(certificate, index)?;
            // As the front: whatever this context's own settings, it is the front's that
            // hold, and these are kept the same so that nothing rests on which.
            if let Some(authorities) = authorities {
                validate_clients(&mut builder, authorities)?;
            }
            names.add(&leaf, index);
            contexts.push(builder.build().into_context());
        }
        if contexts.is_empty() {
            // The config model refuses an `https` listener with no certificate.
            return Err(TlsError::Empty { index: 0 });
        }
        Ok(Self { names, contexts })
    }
}

/// The authorities `source` trusts to vouch for clients, read, if it validates them.
fn authorities_of(source: &edgerush_config::Tls) -> Result<Option<Vec<X509>>, TlsError> {
    source
        .client_validation
        .as_ref()
        .map(|validation| authorities(&validation.authorities))
        .transpose()
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

/// What every context of a listener's is set up with: TLS 1.2 and 1.3, and for 1.2 only
/// forward-secret AEAD suites; the key exchanges; tickets and no cache; the protocols.
fn protocol() -> Result<SslAcceptorBuilder, TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).map_err(setup)?;
    builder.set_curves_list(GROUPS).map_err(setup)?;
    builder.set_session_cache_mode(SslSessionCacheMode::OFF);
    builder.set_alpn_select_callback(|_ssl, offered| {
        select_next_proto(PROTOCOLS, offered).ok_or(AlpnError::NOACK)
    });
    Ok(builder)
}

/// A context for one certificate, and the certificate itself, whose names are for the
/// caller to file.
fn context(
    certificate: &Certificate,
    index: usize,
) -> Result<(SslAcceptorBuilder, X509), TlsError> {
    let setup = |error: ErrorStack| TlsError::Setup(error.to_string());
    let mut builder = protocol()?;
    let identity = Identity::read(certificate, index)?;
    builder.set_certificate(&identity.leaf).map_err(setup)?;
    for intermediate in identity.intermediates {
        builder.add_extra_chain_cert(intermediate).map_err(setup)?;
    }
    builder.set_private_key(&identity.key).map_err(setup)?;
    Ok((builder, identity.leaf))
}

/// A certificate, the intermediates that lead from it, and its key, read and checked to
/// belong together.
pub(crate) struct Identity {
    pub(crate) leaf: X509,
    pub(crate) intermediates: Vec<X509>,
    pub(crate) key: PKey<boring::pkey::Private>,
}

impl Identity {
    /// Reads `certificate`, which is the `index`th of its kind, for errors to say so.
    ///
    /// # Errors
    ///
    /// A [`TlsError`] for a chain or key that cannot be read, or a key not the
    /// certificate's.
    pub(crate) fn read(certificate: &Certificate, index: usize) -> Result<Self, TlsError> {
        let chain = X509::stack_from_pem(certificate.chain.as_bytes()).map_err(|error| {
            TlsError::Chain {
                index,
                reason: error.to_string(),
            }
        })?;
        let mut chain = chain.into_iter();
        let leaf = chain.next().ok_or(TlsError::Empty { index })?;
        let key = PKey::private_key_from_pem(certificate.key.as_bytes()).map_err(|error| {
            TlsError::Key {
                index,
                reason: error.to_string(),
            }
        })?;
        // Compared here, where it can be said which: BoringSSL refuses the pair as well,
        // but only as a failure to set the key.
        let matches = leaf.public_key().is_ok_and(|public| public.public_eq(&key));
        if !matches {
            return Err(TlsError::Mismatch { index });
        }
        Ok(Self {
            leaf,
            intermediates: chain.collect(),
            key,
        })
    }
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
