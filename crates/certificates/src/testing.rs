//! Certificates made on the spot, for tests that need one: a server to present, a Secret
//! to translate, an authority that issues both ends' of a stream.

#![allow(
    clippy::unwrap_used,
    reason = "test set-up: a certificate that cannot be made is a broken test"
)]

use boring::asn1::Asn1Time;
use boring::bn::BigNum;
use boring::ec::{EcGroup, EcKey};
use boring::hash::MessageDigest;
use boring::nid::Nid;
use boring::pkey::{PKey, Private};
use boring::rsa::Rsa;
use boring::x509::extension::{BasicConstraints, KeyUsage, SubjectAlternativeName};
use boring::x509::{X509, X509Builder, X509Name, X509NameBuilder, X509NameRef};
use edgerush_config::Certificate;
use std::sync::atomic::{AtomicU32, Ordering};

/// A self-signed certificate for `names`, with its key: each an IP address, a URI such as
/// a SPIFFE ID, or else a DNS name.
#[must_use]
pub fn certificate(names: &[&str]) -> Certificate {
    let key = key();
    let subject = subject(names.first().copied().unwrap_or("none"));
    let mut builder = builder(&subject, &subject, &key);
    alternatives(&mut builder, names, None);
    builder.sign(&key, MessageDigest::sha256()).unwrap();
    written(&builder.build(), &key)
}

/// The same, of an RSA key of 2048 bits in place of an ECDSA key on P-256: for tests of a
/// listener that holds both kinds for one name.
#[must_use]
pub fn rsa_certificate(names: &[&str]) -> Certificate {
    let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let subject = subject(names.first().copied().unwrap_or("none"));
    let mut builder = builder(&subject, &subject, &key);
    alternatives(&mut builder, names, None);
    builder.sign(&key, MessageDigest::sha256()).unwrap();
    written(&builder.build(), &key)
}

/// A certificate authority made on the spot, for tests of TLS that check both ends: it
/// issues certificates, and its own is what a peer trusts.
pub struct Authority {
    certificate: X509,
    key: PKey<Private>,
}

impl Authority {
    /// An authority named `name`.
    #[must_use]
    pub fn new(name: &str) -> Self {
        let key = key();
        let subject = subject(name);
        let mut builder = builder(&subject, &subject, &key);
        let constraints = BasicConstraints::new().critical().ca().build().unwrap();
        builder.append_extension(&constraints).unwrap();
        let usage = KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()
            .unwrap();
        builder.append_extension(&usage).unwrap();
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        Self {
            certificate: builder.build(),
            key,
        }
    }

    /// Its certificate, in PEM.
    #[must_use]
    pub fn pem(&self) -> String {
        String::from_utf8(self.certificate.to_pem().unwrap()).unwrap()
    }

    /// A certificate for `names` that it signs, with its key: each an IP address, a URI such
    /// as a SPIFFE ID, or else a DNS name.
    #[must_use]
    pub fn issue(&self, names: &[&str]) -> Certificate {
        let key = key();
        let subject = subject(names.first().copied().unwrap_or("none"));
        let mut builder = builder(&subject, self.certificate.subject_name(), &key);
        alternatives(&mut builder, names, Some(&self.certificate));
        builder.sign(&self.key, MessageDigest::sha256()).unwrap();
        written(&builder.build(), &key)
    }
}

fn key() -> PKey<Private> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
    PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap()
}

fn subject(name: &str) -> X509Name {
    let mut subject = X509NameBuilder::new().unwrap();
    subject.append_entry_by_nid(Nid::COMMONNAME, name).unwrap();
    subject.build()
}

/// A certificate of `key`'s for `subject`, by `issuer`, good for 30 days, not yet signed.
fn builder(subject: &X509NameRef, issuer: &X509NameRef, key: &PKey<Private>) -> X509Builder {
    // No two certificates an issuer signs in one test run share a serial number.
    static SERIAL: AtomicU32 = AtomicU32::new(1);
    let mut builder = X509::builder().unwrap();
    builder.set_version(2).unwrap();
    let serial = BigNum::from_u32(SERIAL.fetch_add(1, Ordering::Relaxed))
        .unwrap()
        .to_asn1_integer()
        .unwrap();
    builder.set_serial_number(&serial).unwrap();
    builder.set_subject_name(subject).unwrap();
    builder.set_issuer_name(issuer).unwrap();
    builder.set_pubkey(key).unwrap();
    builder
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&Asn1Time::days_from_now(30).unwrap())
        .unwrap();
    builder
}

fn alternatives(builder: &mut X509Builder, names: &[&str], issuer: Option<&X509>) {
    if names.is_empty() {
        return;
    }
    let mut alternatives = SubjectAlternativeName::new();
    for name in names {
        if name.parse::<std::net::IpAddr>().is_ok() {
            alternatives.ip(name);
        } else if name.contains("://") {
            alternatives.uri(name);
        } else {
            alternatives.dns(name);
        }
    }
    let alternatives = alternatives
        .build(&builder.x509v3_context(issuer.map(|issuer| &**issuer), None))
        .unwrap();
    builder.append_extension(&alternatives).unwrap();
}

fn written(certificate: &X509, key: &PKey<Private>) -> Certificate {
    Certificate {
        chain: String::from_utf8(certificate.to_pem().unwrap()).unwrap(),
        key: String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap(),
    }
}
