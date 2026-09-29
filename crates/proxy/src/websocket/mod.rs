//! WebSocket's opening handshake, as far as the gateway reads and writes it
//! ([19 §2](../../../docs/19-websocket.md)).
//!
//! The gateway switches a connection to a tunnel only for a WebSocket, and only on the
//! word of a backend that proves it read the handshake the gateway sent: a 101 carrying the
//! `Sec-WebSocket-Accept` of a key the gateway made itself. A client picks its own key and
//! can work out the Accept of it, so a backend that hands back a response it fetched for
//! the client (a reflected 101) must not be able to pass with the client's key; it cannot
//! with one the client never saw.
//!
//! Nothing here does I/O: the key's randomness is passed in. [`frames`] follows a stream
//! once it is switched.

#[cfg(feature = "fuzzing")]
pub mod frames;
#[cfg(not(feature = "fuzzing"))]
pub(crate) mod frames;

use edgerush_router::Fields;
use http::header::{CONNECTION, CONTENT_LENGTH, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY};
use http::header::{TRANSFER_ENCODING, UPGRADE};
use http::{HeaderValue, Method, Version};

/// The protocol, as `Upgrade` names it.
pub(crate) const WEBSOCKET: HeaderValue = HeaderValue::from_static("websocket");

/// The connection option that says `Upgrade` is this hop's.
pub(crate) const UPGRADE_OPTION: HeaderValue = HeaderValue::from_static("upgrade");

/// What RFC 6455 §4.2.2 has a server append to the key before it hashes it.
const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// A `Sec-WebSocket-Key`: 16 bytes in base64, 24 characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Key([u8; 24]);

impl Key {
    /// A key of `bytes`, which should be random and unguessable (RFC 6455 §4.1).
    pub(crate) fn of(bytes: [u8; 16]) -> Self {
        let mut key = [0; 24];
        encode(&bytes, &mut key);
        Self(key)
    }

    /// A key as a client sent it: 24 characters that are the base64 of 16 bytes, and
    /// nothing else. None otherwise.
    pub(crate) fn read(value: &[u8]) -> Option<Self> {
        let value = value.trim_ascii();
        let key: [u8; 24] = value.try_into().ok()?;
        // Decoded and written again, it must come out the same: that rules out every
        // character outside the alphabet, the wrong padding, and bits set in the last
        // character that 16 bytes cannot have.
        let bytes = decode_16(&key)?;
        (Self::of(bytes).0 == key).then_some(Self(key))
    }

    /// The key as a field's value.
    pub(crate) fn value(&self) -> HeaderValue {
        // Base64 is printable ASCII throughout, so the empty value is never given: it is
        // there only because the conversion's type allows a failure, and a backend would
        // refuse a handshake with it.
        HeaderValue::from_bytes(&self.0).unwrap_or_else(|_| HeaderValue::from_static(""))
    }

    /// The `Sec-WebSocket-Accept` a server that read this key answers with: the base64 of
    /// the SHA-1 of the key and RFC 6455's GUID.
    pub(crate) fn accept(&self) -> [u8; 28] {
        let mut hashed = [0_u8; 24 + GUID.len()];
        hashed[..24].copy_from_slice(&self.0);
        hashed[24..].copy_from_slice(GUID);
        let digest = boring::sha::sha1(&hashed);
        let mut accept = [0; 28];
        encode(&digest, &mut accept);
        accept
    }

    /// The same, as a field's value.
    pub(crate) fn accept_value(&self) -> HeaderValue {
        // As for [`Key::value`]: never empty, and a client would refuse the switch if it were.
        HeaderValue::from_bytes(&self.accept()).unwrap_or_else(|_| HeaderValue::from_static(""))
    }
}

/// Whether an HTTP/1 request is a WebSocket handshake the gateway carries, and the key the
/// client sent if it is (19 §2): HTTP/1.1, `GET`, a `Connection` that names `upgrade`, one
/// `Upgrade` that is `websocket` and nothing else, no body, and one `Sec-WebSocket-Key`
/// that is 16 bytes in base64. Anything short of that is not one, and its `Upgrade` is
/// taken off with the other hop-by-hop fields: RFC 9110 §7.8 lets a server ignore an
/// upgrade, and has it ignore one in HTTP/1.0.
///
/// `Sec-WebSocket-Version` is the backend's to judge: one it does not speak it answers with
/// 426 and a list of its own, which reaches the client as it came.
pub(crate) fn handshake<F: Fields + ?Sized>(
    version: Version,
    method: &Method,
    fields: &F,
) -> Option<Key> {
    if version != Version::HTTP_11 || method != Method::GET {
        return None;
    }
    let upgrading = fields
        .values(&CONNECTION)
        .flat_map(crate::hop_by_hop::options_of)
        .any(|option| option.eq_ignore_ascii_case(b"upgrade"));
    if !upgrading || !only(fields.values(&UPGRADE), is_websocket) {
        return None;
    }
    // A body would come before the switch (RFC 9110 §7.8), and a handshake has none.
    let bodiless = fields.values(&TRANSFER_ENCODING).next().is_none()
        && fields
            .values(&CONTENT_LENGTH)
            .all(|length| !length.is_empty() && length.iter().all(|&digit| digit == b'0'));
    if !bodiless {
        return None;
    }
    let mut keys = fields.values(&SEC_WEBSOCKET_KEY);
    let key = Key::read(keys.next()?)?;
    keys.next().is_none().then_some(key)
}

/// Whether a backend's 101 switched to the WebSocket the gateway asked for with `key`
/// (19 §2): an `Upgrade` that is `websocket`, a `Connection` that names `upgrade`, and the
/// `Sec-WebSocket-Accept` of `key`, once.
pub(crate) fn switched<F: Fields + ?Sized>(fields: &F, key: &Key) -> bool {
    let upgrading = fields
        .values(&CONNECTION)
        .flat_map(crate::hop_by_hop::options_of)
        .any(|option| option.eq_ignore_ascii_case(b"upgrade"));
    let accept = key.accept();
    upgrading
        && only(fields.values(&UPGRADE), is_websocket)
        && only(fields.values(&SEC_WEBSOCKET_ACCEPT), |value| {
            value.trim_ascii() == accept
        })
}

/// Whether an answer's `Upgrade` offers WebSocket among what it names: a 426 that does
/// keeps `Upgrade: websocket` on its way to an HTTP/1.1 client, the one protocol the
/// gateway can switch to (19 §2).
pub(crate) fn offered<F: Fields + ?Sized>(fields: &F) -> bool {
    fields
        .values(&UPGRADE)
        .flat_map(crate::hop_by_hop::options_of)
        .any(|protocol| protocol.eq_ignore_ascii_case(b"websocket"))
}

/// Whether there is exactly one field value, and it passes `test`.
fn only<'a>(mut values: impl Iterator<Item = &'a [u8]>, test: impl Fn(&[u8]) -> bool) -> bool {
    values.next().is_some_and(test) && values.next().is_none()
}

fn is_websocket(value: &[u8]) -> bool {
    value.trim_ascii().eq_ignore_ascii_case(b"websocket")
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Writes `bytes` in base64, padded, into `out`, which is exactly as long as that takes.
fn encode(bytes: &[u8], out: &mut [u8]) {
    for (group, written) in bytes.chunks(3).zip(out.chunks_mut(4)) {
        let at = |index: usize| u32::from(group.get(index).copied().unwrap_or(0));
        let joined = at(0) << 16 | at(1) << 8 | at(2);
        for (place, slot) in written.iter_mut().enumerate() {
            *slot = if place <= group.len() {
                let six = (joined >> (18 - 6 * place)) & 0x3f;
                ALPHABET[six as usize]
            } else {
                b'='
            };
        }
    }
}

/// Reads the 16 bytes 24 characters of base64 hold, the last two of them padding.
fn decode_16(key: &[u8; 24]) -> Option<[u8; 16]> {
    if &key[22..] != b"==" {
        return None;
    }
    let six = |character: u8| {
        ALPHABET
            .iter()
            .position(|&letter| letter == character)
            .and_then(|at| u32::try_from(at).ok())
    };
    let mut bytes = [0_u8; 18];
    for (group, out) in key[..24].chunks(4).zip(bytes.chunks_mut(3)) {
        let mut joined = 0_u32;
        for (place, &character) in group.iter().enumerate() {
            let value = if character == b'=' {
                0
            } else {
                six(character)?
            };
            joined |= value << (18 - 6 * place);
        }
        for (place, slot) in out.iter_mut().enumerate() {
            *slot = (joined >> (16 - 8 * place)).to_le_bytes()[0];
        }
    }
    bytes[..16].try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use proptest::prelude::*;

    /// RFC 6455 §1.3's own example.
    const SAMPLE: &[u8] = b"dGhlIHNhbXBsZSBub25jZQ==";

    fn fields(lines: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in lines {
            map.append(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    fn handshake_fields() -> Vec<(&'static str, &'static str)> {
        vec![
            ("host", "example.com"),
            ("connection", "Upgrade"),
            ("upgrade", "websocket"),
            ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ("sec-websocket-version", "13"),
        ]
    }

    #[test]
    fn the_rfcs_example_key_is_accepted_as_it_says() {
        let key = Key::read(SAMPLE).unwrap();
        assert_eq!(&key.accept(), b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
        assert_eq!(key.0, *<&[u8; 24]>::try_from(SAMPLE).unwrap());
    }

    #[test]
    fn a_key_made_of_bytes_reads_back_as_them() {
        let bytes = *b"the sample nonce";
        let key = Key::of(bytes);
        assert_eq!(&key.0, SAMPLE);
        assert_eq!(decode_16(&key.0), Some(bytes));
        assert_eq!(
            key.value(),
            HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ==")
        );
    }

    #[test]
    fn a_handshake_is_recognised_with_its_key() {
        let map = fields(&handshake_fields());
        let key = handshake(Version::HTTP_11, &Method::GET, &map).unwrap();
        assert_eq!(&key.0, SAMPLE);
        // Names and the protocol in any case, `Connection` a list, a `Content-Length` of
        // nothing.
        let map = fields(&[
            ("connection", "keep-alive, UPGRADE"),
            ("upgrade", " WebSocket "),
            ("sec-websocket-key", SAMPLE_STR),
            ("content-length", "0"),
        ]);
        assert!(handshake(Version::HTTP_11, &Method::GET, &map).is_some());
    }

    const SAMPLE_STR: &str = "dGhlIHNhbXBsZSBub25jZQ==";

    #[test]
    fn anything_short_of_a_handshake_is_not_one() {
        let base = handshake_fields();
        let without = |name: &str| -> Vec<(&'static str, &'static str)> {
            base.iter()
                .copied()
                .filter(|(field, _)| *field != name)
                .collect()
        };
        let with = |extra: (&'static str, &'static str)| {
            let mut lines = base.clone();
            lines.push(extra);
            lines
        };
        let replaced = |name: &str, value: &'static str| -> Vec<(&'static str, &'static str)> {
            base.iter()
                .map(|&(field, old)| (field, if field == name { value } else { old }))
                .collect()
        };
        type Case = (
            &'static str,
            Version,
            Method,
            Vec<(&'static str, &'static str)>,
        );
        let refused: Vec<Case> = vec![
            ("HTTP/1.0", Version::HTTP_10, Method::GET, base.clone()),
            ("HTTP/2", Version::HTTP_2, Method::GET, base.clone()),
            ("POST", Version::HTTP_11, Method::POST, base.clone()),
            (
                "no Connection",
                Version::HTTP_11,
                Method::GET,
                without("connection"),
            ),
            (
                "Connection not naming upgrade",
                Version::HTTP_11,
                Method::GET,
                replaced("connection", "keep-alive"),
            ),
            (
                "no Upgrade",
                Version::HTTP_11,
                Method::GET,
                without("upgrade"),
            ),
            (
                "h2c",
                Version::HTTP_11,
                Method::GET,
                replaced("upgrade", "h2c"),
            ),
            (
                "a list",
                Version::HTTP_11,
                Method::GET,
                replaced("upgrade", "websocket, h2c"),
            ),
            (
                "a version",
                Version::HTTP_11,
                Method::GET,
                replaced("upgrade", "websocket/13"),
            ),
            (
                "two Upgrades",
                Version::HTTP_11,
                Method::GET,
                with(("upgrade", "websocket")),
            ),
            (
                "no key",
                Version::HTTP_11,
                Method::GET,
                without("sec-websocket-key"),
            ),
            (
                "two keys",
                Version::HTTP_11,
                Method::GET,
                with(("sec-websocket-key", SAMPLE_STR)),
            ),
            (
                "a key of 15 bytes",
                Version::HTTP_11,
                Method::GET,
                replaced("sec-websocket-key", "dGhlIHNhbXBsZSBub25j"),
            ),
            (
                "a key with bits past its 16 bytes",
                Version::HTTP_11,
                Method::GET,
                replaced("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZR=="),
            ),
            (
                "a key outside the alphabet",
                Version::HTTP_11,
                Method::GET,
                replaced("sec-websocket-key", "dGhlIHNhbXBsZSBub25j_Q=="),
            ),
            (
                "a counted body",
                Version::HTTP_11,
                Method::GET,
                with(("content-length", "5")),
            ),
            (
                "a chunked body",
                Version::HTTP_11,
                Method::GET,
                with(("transfer-encoding", "chunked")),
            ),
        ];
        for (why, version, method, lines) in refused {
            assert!(
                handshake(version, &method, &fields(&lines)).is_none(),
                "{why}"
            );
        }
    }

    #[test]
    fn a_101_switches_only_with_the_accept_of_the_key_sent() {
        let key = Key::read(SAMPLE).unwrap();
        let good = [
            ("upgrade", "websocket"),
            ("connection", "Upgrade"),
            ("sec-websocket-accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="),
        ];
        assert!(switched(&fields(&good), &key));
        let other = Key::of(*b"some other nonce");
        assert!(!switched(&fields(&good), &other), "another key's Accept");
        for (why, name) in [
            ("no Upgrade", "upgrade"),
            ("no Connection", "connection"),
            ("no Accept", "sec-websocket-accept"),
        ] {
            let lines: Vec<_> = good
                .iter()
                .copied()
                .filter(|(field, _)| *field != name)
                .collect();
            assert!(!switched(&fields(&lines), &key), "{why}");
        }
        let mut twice = good.to_vec();
        twice.push(("sec-websocket-accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        assert!(!switched(&fields(&twice), &key), "two Accepts");
        let mut other_protocol = good.to_vec();
        other_protocol[0] = ("upgrade", "h2c");
        assert!(
            !switched(&fields(&other_protocol), &key),
            "another protocol"
        );
    }

    #[test]
    fn a_426_offers_websocket_when_it_names_it() {
        assert!(offered(&fields(&[("upgrade", "h2c, WebSocket")])));
        assert!(!offered(&fields(&[("upgrade", "h2c")])));
        assert!(!offered(&fields(&[])));
    }

    /// The same as [`encode`], written the long way.
    fn naive_encode(bytes: &[u8]) -> Vec<u8> {
        let mut bits = Vec::new();
        for byte in bytes {
            for shift in (0..8).rev() {
                bits.push((byte >> shift) & 1);
            }
        }
        while bits.len() % 6 != 0 {
            bits.push(0);
        }
        let mut out: Vec<u8> = bits
            .chunks(6)
            .map(|six| {
                ALPHABET[six
                    .iter()
                    .fold(0_usize, |value, bit| value << 1 | usize::from(*bit))]
            })
            .collect();
        while !out.len().is_multiple_of(4) {
            out.push(b'=');
        }
        out
    }

    proptest! {
        #[test]
        fn keys_are_written_as_base64_is(bytes in any::<[u8; 16]>()) {
            let key = Key::of(bytes);
            prop_assert_eq!(key.0.to_vec(), naive_encode(&bytes));
            prop_assert_eq!(Key::read(&key.0), Some(key));
        }

        #[test]
        fn accepts_are_written_as_base64_is(digest in any::<[u8; 20]>()) {
            let mut out = [0; 28];
            encode(&digest, &mut out);
            prop_assert_eq!(out.to_vec(), naive_encode(&digest));
        }

        #[test]
        fn a_key_is_read_only_if_it_is_written_back_the_same(key in proptest::collection::vec(any::<u8>(), 0..30)) {
            if let Some(read) = Key::read(&key) {
                prop_assert_eq!(read.0.to_vec(), key.trim_ascii().to_vec());
            }
        }
    }
}
