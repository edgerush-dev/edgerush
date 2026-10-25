//! Certificates made on the spot, for tests that need one: a server to present, a Secret
//! to translate.

#![allow(
    clippy::unwrap_used,
    reason = "test set-up: a certificate that cannot be made is a broken test"
)]

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
#[must_use]
pub fn certificate(names: &[&str]) -> Certificate {
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
