//! A certificate and its key, read and checked to belong together
//! ([03 §3](../../../docs/03-data-plane.md)): by BoringSSL, as the data plane presents them.
//!
//! The data plane reads every certificate a listener or an upstream names when a config
//! comes, and refuses the config over one it cannot use. Translation reads a Secret's with
//! the same code before it puts it in a config, so that what it admits is exactly what the
//! data plane can present, and one bad Secret never costs a listener every other
//! certificate.
//!
//! Reading does no I/O and asks no clock: an expired certificate is read as any other.

use boring::pkey::{PKey, Private};
use boring::x509::{X509, X509Ref};
use edgerush_config::Certificate;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// A certificate, the intermediates that lead from it, and its key, read and checked to
/// belong together.
pub struct Identity {
    /// The certificate.
    pub leaf: X509,
    /// The intermediates, in the order the chain gave them.
    pub intermediates: Vec<X509>,
    /// Its private key.
    pub key: PKey<Private>,
}

/// Why a certificate cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CertificateError {
    /// The chain is not certificates in PEM.
    #[error("the chain is not PEM certificates: {0}")]
    Chain(String),
    /// The chain has no certificate in it.
    #[error("the chain is empty")]
    Empty,
    /// The key is not a private key in PEM.
    #[error("the key is not a PEM private key: {0}")]
    Key(String),
    /// The key is not the one the certificate was issued for.
    #[error("the key is not the certificate's")]
    Mismatch,
}

impl Identity {
    /// Reads `certificate`.
    ///
    /// # Errors
    ///
    /// A [`CertificateError`] for a chain or key that cannot be read, or a key not the
    /// certificate's.
    pub fn read(certificate: &Certificate) -> Result<Self, CertificateError> {
        let chain = X509::stack_from_pem(certificate.chain.as_bytes())
            .map_err(|error| CertificateError::Chain(error.to_string()))?;
        let mut chain = chain.into_iter();
        let leaf = chain.next().ok_or(CertificateError::Empty)?;
        let key = PKey::private_key_from_pem(certificate.key.as_bytes())
            .map_err(|error| CertificateError::Key(error.to_string()))?;
        // Compared here, where it can be said what is wrong: BoringSSL refuses the pair as
        // well, but only as a failure to set the key.
        let matches = leaf.public_key().is_ok_and(|public| public.public_eq(&key));
        if !matches {
            return Err(CertificateError::Mismatch);
        }
        Ok(Self {
            leaf,
            intermediates: chain.collect(),
            key,
        })
    }

    /// Whether the certificate carries `host`, as a client checking it would find it: among
    /// the DNS names of its subject alternative names, in any letter case, a wildcard
    /// standing for one whole leftmost label (RFC 6125 §6.4.3), as the data plane matches a
    /// name. A wildcard `host` is carried only by the same wildcard. The subject's common
    /// name is not looked at: clients have long stopped reading it (RFC 6125 §6.4.4).
    #[must_use]
    pub fn covers(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        let Some(alternatives) = self.leaf.subject_alt_names() else {
            return false;
        };
        alternatives
            .iter()
            .filter_map(|name| name.dnsname())
            .map(str::to_ascii_lowercase)
            .any(|name| {
                name == host
                    || name.strip_prefix("*.").is_some_and(|parent| {
                        host.split_once('.').is_some_and(|(label, rest)| {
                            !label.is_empty() && label != "*" && rest == parent
                        })
                    })
            })
    }
}

/// The SPIFFE ID `certificate` names its workload by: its one URI subject alternative name,
/// if that is a SPIFFE ID (`spiffe://` and a trust domain). None for a certificate with no
/// URI, or with more than one, as a SPIFFE certificate carries exactly one (the X.509-SVID
/// standard, §2): a workload is known by one ID or by none.
#[must_use]
pub fn spiffe_id(certificate: &X509Ref) -> Option<String> {
    let alternatives = certificate.subject_alt_names()?;
    let mut uris = alternatives.iter().filter_map(|name| name.uri());
    let id = uris.next()?;
    if uris.next().is_some() {
        return None;
    }
    let domain = id.strip_prefix("spiffe://")?;
    (!domain.is_empty() && !domain.starts_with('/')).then(|| id.to_owned())
}

#[cfg(test)]
mod tests {
    use super::testing::certificate;
    use super::*;

    #[test]
    fn a_certificate_and_its_key_are_read() {
        let identity = Identity::read(&certificate(&["a.test"])).unwrap();
        assert!(identity.intermediates.is_empty());
        let names: Vec<String> = identity
            .leaf
            .subject_alt_names()
            .unwrap()
            .iter()
            .filter_map(|name| name.dnsname().map(str::to_owned))
            .collect();
        assert_eq!(names, ["a.test"]);
    }

    #[test]
    fn a_workload_is_known_by_its_one_spiffe_id() {
        let id = |names: &[&str]| spiffe_id(&Identity::read(&certificate(names)).unwrap().leaf);
        assert_eq!(
            id(&["rl-0", "spiffe://cluster.local/ns/edge/sa/rl"]),
            Some("spiffe://cluster.local/ns/edge/sa/rl".to_owned())
        );
        assert_eq!(id(&["rl-0"]), None);
        assert_eq!(id(&["https://cluster.local/ns/edge/sa/rl"]), None);
        assert_eq!(id(&["spiffe:///ns/edge/sa/rl"]), None);
        assert_eq!(
            id(&[
                "spiffe://cluster.local/ns/edge/sa/rl",
                "spiffe://cluster.local/ns/edge/sa/plane",
            ]),
            None
        );
        assert_eq!(
            id(&[
                "spiffe://cluster.local/ns/edge/sa/rl",
                "https://elsewhere.test/",
            ]),
            None
        );
    }

    #[test]
    fn a_certificate_covers_the_names_it_carries_a_wildcard_one_label() {
        let identity =
            Identity::read(&certificate(&["shop.example.com", "*.Blog.example.com"])).unwrap();
        for covered in [
            "shop.example.com",
            "SHOP.example.com",
            "a.blog.example.com",
            "*.blog.example.com",
        ] {
            assert!(identity.covers(covered), "{covered}");
        }
        for uncovered in [
            "example.com",
            "other.example.com",
            "blog.example.com",
            "a.b.blog.example.com",
            ".blog.example.com",
            "*.example.com",
            "*.shop.example.com",
        ] {
            assert!(!identity.covers(uncovered), "{uncovered}");
        }
        // The common name alone carries nothing.
        let unnamed = Identity::read(&certificate(&[])).unwrap();
        assert!(!unnamed.covers("none"));
    }

    #[test]
    fn a_certificate_that_cannot_be_used_is_said_why() {
        let good = certificate(&["a.test"]);
        let error = |certificate: Certificate| Identity::read(&certificate).err();
        // Text with no PEM in it is no certificates; a PEM block that is not one is an error.
        let nothing = Certificate {
            chain: "not a certificate".to_owned(),
            ..good.clone()
        };
        assert_eq!(error(nothing), Some(CertificateError::Empty));
        let broken = Certificate {
            chain: "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n"
                .to_owned(),
            ..good.clone()
        };
        assert!(matches!(error(broken), Some(CertificateError::Chain(_))));
        let key = Certificate {
            key: "not a key".to_owned(),
            ..good.clone()
        };
        assert!(matches!(error(key), Some(CertificateError::Key(_))));
        let mismatched = Certificate {
            key: certificate(&["b.test"]).key,
            ..good.clone()
        };
        assert_eq!(error(mismatched), Some(CertificateError::Mismatch));
        assert!(error(good).is_none());
    }
}
