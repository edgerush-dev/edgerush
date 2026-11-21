//! What `X-Forwarded-Client-Cert` tells an upstream of the certificate a client showed
//! (03 §3 in the docs), on a listener that forwards it: Envoy's text form, with the fields
//! Istio's gateways send, `Hash`, `Cert`, `Subject`, `URI` and `DNS`, in Envoy's order.
//!
//! Written once for a connection, at its handshake, and only for a certificate that
//! verified against the listener's client validation: under `AllowInsecureFallback` a
//! client is let through whatever its certificate is, and an upstream that reads the header
//! must be able to take it as vouched for.

use boring::hash::MessageDigest;
use boring::x509::X509Ref;
use http::header::{HeaderName, HeaderValue};
use std::fmt::Write as _;

/// `X-Forwarded-Client-Cert`.
pub(crate) const HEADER: HeaderName = HeaderName::from_static("x-forwarded-client-cert");

/// BoringSSL's `XN_FLAG_RFC2253`, which `boring` does not export: `ASN1_STRFLGS_RFC2253`
/// (0x317: RFC 2253's escapes, control characters and every byte above 127 escaped too,
/// after conversion to UTF-8; unknown types dumped as DER), RDNs separated by `,` and
/// attributes by `+` (1 << 16), most significant last (1 << 20), short names, unknown
/// attribute types in hex (1 << 24). What Envoy prints a subject with; ASCII alone.
const RFC2253: i32 = 0x0111_0317;

/// The header's value for `certificate`, a client's that verified; `None` for one whose
/// names cannot go in a header, which no upstream is then told of.
pub(crate) fn value(certificate: &X509Ref) -> Option<HeaderValue> {
    let mut value = String::with_capacity(2048);
    let digest = certificate.digest(MessageDigest::sha256()).ok()?;
    value.push_str("Hash=");
    for byte in digest.iter() {
        // Writing to a String cannot fail.
        let _ = write!(value, "{byte:02x}");
    }
    value.push_str(";Cert=\"");
    let pem = certificate.to_pem().ok()?;
    percent_encode(&pem, &mut value);
    value.push_str("\";Subject=\"");
    value.push_str(
        &certificate
            .subject_name()
            .print_ex(RFC2253)
            .unwrap_or_default(),
    );
    value.push('"');
    let names = certificate.subject_alt_names();
    let names = || names.iter().flat_map(|names| names.iter());
    // As Envoy has it, the key is there even with no URI to give.
    let mut uris = 0;
    for uri in names().filter_map(|name| name.uri()) {
        value.push_str(";URI=");
        push_field(uri, &mut value);
        uris += 1;
    }
    if uris == 0 {
        value.push_str(";URI=");
    }
    for dns in names().filter_map(|name| name.dnsname()) {
        value.push_str(";DNS=");
        push_field(dns, &mut value);
    }
    HeaderValue::try_from(value).ok()
}

/// `field`, a name the certificate carries, written to `out` so that it stays one value of
/// the header: as it is, or, where it holds a character that would end it or start another
/// (`,`, `;`, `=`) or a quote, in double quotes with `"` and `\` escaped by `\`, as Envoy's
/// XFCC format says such a value is written. Envoy itself writes them as they are; a CA
/// that signs a name of a client's choosing could then have its client add fields.
fn push_field(field: &str, out: &mut String) {
    if !field.contains([',', ';', '=', '"', '\\']) {
        out.push_str(field);
        return;
    }
    out.push('"');
    for c in field.chars() {
        if matches!(c, '"' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}

/// `bytes` written to `out` as Envoy's `urlEncode` has them: every byte but letters,
/// digits and `*-._` as `%` and two upper-case hex digits.
fn percent_encode(bytes: &[u8], out: &mut String) {
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'-' | b'.' | b'_') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boring::asn1::Asn1Time;
    use boring::bn::BigNum;
    use boring::ec::{EcGroup, EcKey};
    use boring::nid::Nid;
    use boring::pkey::PKey;
    use boring::x509::extension::SubjectAlternativeName;
    use boring::x509::{X509, X509NameBuilder};

    /// A self-signed certificate of this subject and these alternative names.
    fn certificate(subject: &[(&str, &str)], names: &[&str]) -> X509 {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        for (field, text) in subject {
            name.append_entry_by_text(field, text).unwrap();
        }
        let name = name.build();
        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        builder
            .set_serial_number(&BigNum::from_u32(7).unwrap().to_asn1_integer().unwrap())
            .unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        if !names.is_empty() {
            let mut alternative = SubjectAlternativeName::new();
            for name in names {
                match name.strip_prefix("uri:") {
                    Some(uri) => alternative.uri(uri),
                    None => alternative.dns(name),
                };
            }
            let extension = alternative
                .build(&builder.x509v3_context(None, None))
                .unwrap();
            builder.append_extension(&extension).unwrap();
        }
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        builder.build()
    }

    /// The value's fields, `Cert` read back to its PEM.
    fn fields(value: &HeaderValue) -> Vec<String> {
        value
            .to_str()
            .unwrap()
            .split(';')
            .map(str::to_owned)
            .collect()
    }

    fn hash(certificate: &X509) -> String {
        certificate
            .digest(MessageDigest::sha256())
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// Envoy's fields, in its order: the hash of the DER, the PEM URL-encoded, the subject
    /// as RFC 2253 has it, then each URI and each DNS name.
    #[test]
    fn the_value_is_envoys_text_form() {
        let shown = certificate(
            &[("C", "GB"), ("O", "Shop, Ltd"), ("CN", "client")],
            &[
                "uri:spiffe://shop/ns/web/sa/client",
                "client.shop.test",
                "api.shop.test",
            ],
        );
        let value = value(&shown).unwrap();
        let fields = fields(&value);
        assert_eq!(fields[0], format!("Hash={}", hash(&shown)));
        let pem = String::from_utf8(shown.to_pem().unwrap()).unwrap();
        let cert = fields[1]
            .strip_prefix("Cert=\"")
            .and_then(|rest| rest.strip_suffix('"'))
            .unwrap();
        assert!(!cert.contains(['\n', '+', '/', '=', ' ']), "{cert}");
        assert_eq!(decoded(cert), pem);
        assert_eq!(
            &fields[2..],
            [
                r#"Subject="CN=client,O=Shop\, Ltd,C=GB""#,
                "URI=spiffe://shop/ns/web/sa/client",
                "DNS=client.shop.test",
                "DNS=api.shop.test",
            ]
        );
    }

    /// With no URI, its key is there all the same, as Envoy writes it; with no DNS name,
    /// there is none.
    #[test]
    fn a_certificate_without_names_still_says_uri() {
        let shown = certificate(&[("CN", "bare")], &[]);
        let value = value(&shown).unwrap();
        assert_eq!(&fields(&value)[2..], [r#"Subject="CN=bare""#, "URI="]);
    }

    /// A subject outside ASCII is escaped, so that the value stays a header's.
    #[test]
    fn a_subject_outside_ascii_is_escaped() {
        let shown = certificate(&[("CN", "Zoë")], &[]);
        let value = value(&shown).unwrap();
        assert_eq!(fields(&value)[2], r#"Subject="CN=Zo\C3\AB""#);
    }

    /// A name that would end its field, or start another, is quoted, so that a client
    /// whose CA signed whatever names it asked for cannot add fields of its own.
    #[test]
    fn a_name_that_would_add_a_field_is_quoted() {
        let shown = certificate(
            &[("CN", "sly")],
            &[
                "uri:spiffe://a;Hash=00",
                "uri:spiffe://b\"c\\d",
                "uri:spiffe://plain",
            ],
        );
        let value = value(&shown).unwrap();
        assert_eq!(
            &fields(&value)[3..],
            [
                r#"URI="spiffe://a"#,
                r#"Hash=00""#,
                r#"URI="spiffe://b\"c\\d""#,
                "URI=spiffe://plain",
            ]
        );
        let mut out = String::new();
        push_field("a,b=c", &mut out);
        assert_eq!(out, r#""a,b=c""#);
    }

    /// Envoy's set: letters, digits and `*-._` as they are, all else as `%XX`.
    #[test]
    fn percent_encoding_keeps_only_envoys_unreserved() {
        let mut out = String::new();
        percent_encode(b"aZ09*-._ \n+/=%~", &mut out);
        assert_eq!(out, "aZ09*-._%20%0A%2B%2F%3D%25%7E");
    }

    fn decoded(encoded: &str) -> String {
        let bytes = encoded.as_bytes();
        let mut out = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            if bytes[at] == b'%' {
                let hex = std::str::from_utf8(&bytes[at + 1..at + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                at += 3;
            } else {
                out.push(bytes[at]);
                at += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }
}
