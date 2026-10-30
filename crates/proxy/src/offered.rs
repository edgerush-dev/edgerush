//! What a client's ClientHello offers that decides which of a name's certificates it is
//! shown: whether it can verify a signature by an RSA key, or by an ECDSA key on each
//! curve ([03 §3](../../../docs/03-data-plane.md)). Read from the bodies of its
//! `signature_algorithms`, `supported_groups` and `supported_versions` extensions and its
//! list of cipher suites, as BoringSSL hands them to the early callback, without keeping
//! any of them: a handshake allocates nothing here.
//!
//! In TLS 1.3 a signature scheme names its key and, for ECDSA, its curve (RFC 8446
//! §4.2.3), and the cipher suites say nothing of the key. Below it, a scheme names the key
//! and the hash alone, the curve is one of the client's groups (RFC 8422 §5.1), the suite
//! names the key too, and a client that sends no schemes takes SHA-1 with the suite's key
//! (RFC 5246 §7.4.1.4.1). A body that cannot be read offers nothing, so that the
//! certificate chosen is the first, as it is for a client that can use none of them.

// Public for the fuzz targets alone.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

/// The kind of key a certificate's leaf holds, as far as choosing among certificates goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    /// RSA.
    Rsa,
    /// ECDSA, on one of the curves TLS names.
    Ecdsa(Curve),
    /// Any other (Ed25519, an ECDSA curve TLS does not name): not told apart, so taken as
    /// usable, BoringSSL deciding.
    Other,
}

/// An ECDSA curve TLS names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Curve {
    /// secp256r1.
    P256,
    /// secp384r1.
    P384,
    /// secp521r1.
    P521,
}

/// Which keys a client can verify a signature by.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Offered {
    rsa: bool,
    p256: bool,
    p384: bool,
    p521: bool,
}

/// The extension bodies and suites a client sent, as BoringSSL hands them over; an
/// extension it did not send is none.
#[derive(Debug, Clone, Copy, Default)]
pub struct Hello<'a> {
    /// `signature_algorithms` (13): a list of two-byte schemes, its length in two bytes.
    pub schemes: Option<&'a [u8]>,
    /// `supported_groups` (10): a list of two-byte groups, its length in two bytes.
    pub groups: Option<&'a [u8]>,
    /// `supported_versions` (43): a list of two-byte versions, its length in one byte.
    pub versions: Option<&'a [u8]>,
    /// The cipher suites, two bytes each, with no length before them.
    pub suites: &'a [u8],
}

const TLS_1_3: u16 = 0x0304;

const RSA_PKCS1_SHA1: u16 = 0x0201;
const ECDSA_SHA1: u16 = 0x0203;
const RSA_PKCS1_SHA256: u16 = 0x0401;
const ECDSA_SECP256R1_SHA256: u16 = 0x0403;
const RSA_PKCS1_SHA384: u16 = 0x0501;
const ECDSA_SECP384R1_SHA384: u16 = 0x0503;
const RSA_PKCS1_SHA512: u16 = 0x0601;
const ECDSA_SECP521R1_SHA512: u16 = 0x0603;
const RSA_PSS_RSAE: [u16; 3] = [0x0804, 0x0805, 0x0806];

const SECP256R1: u16 = 0x0017;
const SECP384R1: u16 = 0x0018;
const SECP521R1: u16 = 0x0019;

/// TLS 1.2's suites that sign with an ECDSA key: ECDHE-ECDSA, any cipher (RFC 8422 §6, and
/// the AEAD ones of RFC 5289 and RFC 7905).
fn signs_with_ecdsa(suite: u16) -> bool {
    matches!(
        suite,
        0xC006..=0xC00A | 0xC023 | 0xC024 | 0xC02B | 0xC02C | 0xC072 | 0xC073 | 0xCCA9
    )
}

/// TLS 1.2's suites that sign with an RSA key, or encrypt to one: ECDHE-RSA, DHE-RSA, and
/// plain RSA.
fn uses_rsa(suite: u16) -> bool {
    matches!(
        suite,
        0x0004 | 0x0005 | 0x000A | 0x0016 | 0x002F | 0x0033 | 0x0035 | 0x0039 | 0x003C | 0x003D
            | 0x0067 | 0x006B | 0x009C..=0x009F | 0xC010..=0xC014 | 0xC027 | 0xC028 | 0xC02F
            | 0xC030 | 0xCCA8 | 0xCCAA
    )
}

/// The two-byte codes of `list`, a trailing odd byte left out.
fn codes(list: &[u8]) -> impl Iterator<Item = u16> + '_ {
    list.as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u16::from_be_bytes(*pair))
}

/// The list in `body` after its length, in two bytes, where that length is the rest of it.
fn after_two(body: &[u8]) -> Option<&[u8]> {
    let (length, list) = body.split_at_checked(2)?;
    (usize::from(u16::from_be_bytes([length[0], length[1]])) == list.len()).then_some(list)
}

/// The list in `body` after its length, in one byte, where that length is the rest of it.
fn after_one(body: &[u8]) -> Option<&[u8]> {
    let (length, list) = body.split_first()?;
    (usize::from(*length) == list.len()).then_some(list)
}

impl Offered {
    /// What `hello` offers.
    #[must_use]
    pub fn of(hello: &Hello<'_>) -> Self {
        let tls13 = hello
            .versions
            .and_then(after_one)
            .is_some_and(|versions| codes(versions).any(|version| version == TLS_1_3));
        // Sent but unreadable is as good as nothing offered; not sent is a default below
        // TLS 1.3.
        let schemes = hello
            .schemes
            .map(|body| after_two(body).unwrap_or_default());
        let groups = hello.groups.map(|body| after_two(body).unwrap_or_default());
        let scheme = |wanted: &dyn Fn(u16) -> bool| {
            schemes.is_some_and(|schemes| codes(schemes).any(wanted))
        };
        if tls13 {
            return Self {
                rsa: scheme(&|code| RSA_PSS_RSAE.contains(&code)),
                p256: scheme(&|code| code == ECDSA_SECP256R1_SHA256),
                p384: scheme(&|code| code == ECDSA_SECP384R1_SHA384),
                p521: scheme(&|code| code == ECDSA_SECP521R1_SHA512),
            };
        }
        let suites = || codes(hello.suites);
        let rsa_schemes = schemes.is_none()
            || scheme(&|code| {
                matches!(
                    code,
                    RSA_PKCS1_SHA1 | RSA_PKCS1_SHA256 | RSA_PKCS1_SHA384 | RSA_PKCS1_SHA512
                ) || RSA_PSS_RSAE.contains(&code)
            });
        let ecdsa_schemes = schemes.is_none()
            || scheme(&|code| {
                matches!(
                    code,
                    ECDSA_SHA1
                        | ECDSA_SECP256R1_SHA256
                        | ECDSA_SECP384R1_SHA384
                        | ECDSA_SECP521R1_SHA512
                )
            });
        let ecdsa = ecdsa_schemes && suites().any(signs_with_ecdsa);
        // A client that names no groups takes any (RFC 8422 §4).
        let group = |wanted: u16| groups.is_none_or(|groups| codes(groups).any(|g| g == wanted));
        Self {
            rsa: rsa_schemes && suites().any(uses_rsa),
            p256: ecdsa && group(SECP256R1),
            p384: ecdsa && group(SECP384R1),
            p521: ecdsa && group(SECP521R1),
        }
    }

    /// Whether the client can verify a signature by `key`.
    #[must_use]
    pub fn can_use(&self, key: Key) -> bool {
        match key {
            Key::Rsa => self.rsa,
            Key::Ecdsa(Curve::P256) => self.p256,
            Key::Ecdsa(Curve::P384) => self.p384,
            Key::Ecdsa(Curve::P521) => self.p521,
            Key::Other => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Curve, Hello, Key, Offered};

    /// A list of `codes`, its length before it in `width` bytes.
    fn list(width: usize, codes: &[u16]) -> Vec<u8> {
        let body: Vec<u8> = codes.iter().flat_map(|code| code.to_be_bytes()).collect();
        let mut written = if width == 1 {
            vec![u8::try_from(body.len()).unwrap()]
        } else {
            u16::try_from(body.len()).unwrap().to_be_bytes().to_vec()
        };
        written.extend(body);
        written
    }

    fn suites(codes: &[u16]) -> Vec<u8> {
        codes.iter().flat_map(|code| code.to_be_bytes()).collect()
    }

    const P256: Key = Key::Ecdsa(Curve::P256);
    const P384: Key = Key::Ecdsa(Curve::P384);

    #[test]
    fn in_tls_1_3_the_schemes_alone_decide() {
        let versions = list(1, &[0x0304, 0x0303]);
        let suites = suites(&[0x1301]);
        let offered = |schemes: &[u16]| {
            let schemes = list(2, schemes);
            Offered::of(&Hello {
                schemes: Some(&schemes),
                groups: None,
                versions: Some(&versions),
                suites: &suites,
            })
        };
        let ecdsa = offered(&[0x0403]);
        assert!(ecdsa.can_use(P256) && !ecdsa.can_use(P384) && !ecdsa.can_use(Key::Rsa));
        let rsa = offered(&[0x0804]);
        assert!(rsa.can_use(Key::Rsa) && !rsa.can_use(P256));
        // PKCS #1 signs no TLS 1.3 handshake.
        assert!(!offered(&[0x0401]).can_use(Key::Rsa));
        let both = offered(&[0x0403, 0x0503, 0x0804]);
        assert!(both.can_use(Key::Rsa) && both.can_use(P256) && both.can_use(P384));
        // Another kind of key is BoringSSL's to judge.
        assert!(offered(&[]).can_use(Key::Other));
    }

    #[test]
    fn below_tls_1_3_the_suite_and_the_groups_decide_too() {
        let offered = |schemes: Option<&[u16]>, groups: Option<&[u16]>, suites_: &[u16]| {
            let schemes = schemes.map(|schemes| list(2, schemes));
            let groups = groups.map(|groups| list(2, groups));
            let suites = suites(suites_);
            Offered::of(&Hello {
                schemes: schemes.as_deref(),
                groups: groups.as_deref(),
                versions: None,
                suites: &suites,
            })
        };
        // An ECDHE-ECDSA suite and a P-256 group: P-256 alone.
        let ecdsa = offered(Some(&[0x0403]), Some(&[0x0017]), &[0xC02B]);
        assert!(ecdsa.can_use(P256) && !ecdsa.can_use(P384) && !ecdsa.can_use(Key::Rsa));
        // A scheme's hash is not its curve below TLS 1.3; a curve not among the groups is
        // not offered.
        let p384 = offered(Some(&[0x0403]), Some(&[0x0018]), &[0xC02B]);
        assert!(p384.can_use(P384) && !p384.can_use(P256));
        // No ECDSA suite, no ECDSA key, whatever the schemes.
        assert!(!offered(Some(&[0x0403, 0x0401]), None, &[0xC02F]).can_use(P256));
        assert!(offered(Some(&[0x0403, 0x0401]), None, &[0xC02F]).can_use(Key::Rsa));
        // No schemes sent: SHA-1 with the suite's key; no groups sent: any.
        let defaults = offered(None, None, &[0xC02B, 0x002F]);
        assert!(defaults.can_use(P256) && defaults.can_use(P384) && defaults.can_use(Key::Rsa));
        // Schemes sent, none of them RSA's: no RSA key.
        assert!(!offered(Some(&[0x0403]), None, &[0xC02F]).can_use(Key::Rsa));
    }

    #[test]
    fn what_cannot_be_read_offers_nothing() {
        let suites = suites(&[0xC02B, 0xC02F]);
        let offered = |schemes: &[u8]| {
            Offered::of(&Hello {
                schemes: Some(schemes),
                groups: None,
                versions: None,
                suites: &suites,
            })
        };
        // A length that is not the rest, or none at all.
        for schemes in [&[0x00, 0x04, 0x04, 0x03][..], &[0x00][..], &[][..]] {
            let offered = offered(schemes);
            assert!(
                !offered.can_use(P256) && !offered.can_use(Key::Rsa),
                "{schemes:?}"
            );
        }
        // Versions that cannot be read are not TLS 1.3's.
        let schemes = list(2, &[0x0401, 0x0403]);
        let below = Offered::of(&Hello {
            schemes: Some(&schemes),
            groups: None,
            versions: Some(&[0x05, 0x03, 0x04]),
            suites: &suites,
        });
        assert!(below.can_use(Key::Rsa));
        // A trailing odd byte of the suites is no suite.
        let odd = Offered::of(&Hello {
            schemes: None,
            groups: None,
            versions: None,
            suites: &[0xC0, 0x2B, 0xC0],
        });
        assert!(odd.can_use(P256) && !odd.can_use(Key::Rsa));
    }
}
