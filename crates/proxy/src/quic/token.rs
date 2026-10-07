//! Retry tokens ([16 §4](../../../../docs/16-http3.md), RFC 9000 §8.1.2): what a worker asks
//! a client to echo before it spends a handshake on it, when too many are already under
//! way.
//!
//! A token holds the destination ID the client first used, which the connection must be
//! accepted with (quiche refuses the handshake otherwise), and the second it was made, and
//! is bound to the client's address by an HMAC under a key of the process's: only the client
//! that received it, at that address, can use it, and only for [`LIFETIME`] seconds. Its
//! first byte says which kind of token it is, so a token of another kind — NEW_TOKEN's, one
//! day — is not mistaken for it.

use std::net::SocketAddr;

/// How long a token is good for, in seconds: a round trip, generously. HAProxy's figure.
pub const LIFETIME: u64 = 10;

/// What a Retry token starts with.
const RETRY: u8 = 0x52;

/// The MAC's length in a token: HMAC-SHA256, truncated.
const MAC: usize = 16;

/// The longest destination ID a client may use (RFC 9000 §17.2).
const MAX_ID: usize = 20;

/// A token for `peer`, whose Initial named `odcid`, made at `now` (seconds since the Unix
/// epoch).
pub fn mint(key: &[u8; 32], now: u64, peer: SocketAddr, odcid: &[u8]) -> Option<Vec<u8>> {
    if odcid.len() > MAX_ID {
        return None;
    }
    let mut token = Vec::with_capacity(1 + 8 + 1 + odcid.len() + MAC);
    token.push(RETRY);
    token.extend(now.to_be_bytes());
    token.push(odcid.len() as u8);
    token.extend(odcid);
    let mac = mac(key, &token, peer)?;
    token.extend(mac);
    Some(token)
}

/// The destination ID the client first used, if `token` is one of ours for `peer` and was
/// made no more than [`LIFETIME`] seconds before `now`.
pub fn validate<'a>(
    key: &[u8; 32],
    now: u64,
    peer: SocketAddr,
    token: &'a [u8],
) -> Option<&'a [u8]> {
    let (&kind, rest) = token.split_first()?;
    if kind != RETRY {
        return None;
    }
    let (made, rest) = rest.split_first_chunk::<8>()?;
    let (&len, rest) = rest.split_first()?;
    let len = usize::from(len);
    if len > MAX_ID || rest.len() != len + MAC {
        return None;
    }
    let (odcid, given) = rest.split_at(len);
    let signed = &token[..token.len() - MAC];
    let expected = mac(key, signed, peer)?;
    if !boring::memcmp::eq(&expected, given) {
        return None;
    }
    let made = u64::from_be_bytes(*made);
    // Made in the future is made with another clock: refused as well.
    (made <= now && now - made <= LIFETIME).then_some(odcid)
}

/// Whether `token` says it is a Retry token, whether or not it is good.
pub fn is_retry(token: &[u8]) -> bool {
    token.first() == Some(&RETRY)
}

/// The MAC over `signed` and `peer`'s address and port.
fn mac(key: &[u8; 32], signed: &[u8], peer: SocketAddr) -> Option<[u8; MAC]> {
    let mut input = Vec::with_capacity(signed.len() + 18);
    input.extend(signed);
    match peer.ip() {
        std::net::IpAddr::V4(ip) => input.extend(ip.to_ipv6_mapped().octets()),
        std::net::IpAddr::V6(ip) => input.extend(ip.octets()),
    }
    input.extend(peer.port().to_be_bytes());
    let full = boring::hash::hmac_sha256(key, &input).ok()?;
    let mut mac = [0; MAC];
    mac.copy_from_slice(&full[..MAC]);
    Some(mac)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const KEY: [u8; 32] = [0x7b; 32];

    fn peer() -> SocketAddr {
        "192.0.2.1:4433".parse().unwrap()
    }

    #[test]
    fn a_token_gives_back_the_id_for_its_client_within_its_lifetime() {
        let token = mint(&KEY, 1_000, peer(), b"first-id").unwrap();
        assert_eq!(
            validate(&KEY, 1_000, peer(), &token),
            Some(&b"first-id"[..])
        );
        assert_eq!(
            validate(&KEY, 1_000 + LIFETIME, peer(), &token),
            Some(&b"first-id"[..])
        );
        assert_eq!(
            validate(&KEY, 1_001 + LIFETIME, peer(), &token),
            None,
            "expired"
        );
        assert_eq!(validate(&KEY, 999, peer(), &token), None, "from the future");
    }

    /// A token says it is a Retry token by its first byte, good or not; one of another kind,
    /// or none, does not.
    #[test]
    fn a_retry_token_says_so_whether_or_not_it_holds() {
        let token = mint(&KEY, 1_000, peer(), b"first-id").unwrap();
        assert!(is_retry(&token));
        let moved: SocketAddr = "192.0.2.1:4434".parse().unwrap();
        assert_eq!(validate(&KEY, 1_000, moved, &token), None);
        assert!(is_retry(&token), "failing, still a Retry token");
        let mut other = token.clone();
        other[0] = 0x4e;
        assert!(!is_retry(&other));
        assert!(!is_retry(&[]));
    }

    #[test]
    fn a_token_is_refused_anywhere_but_where_it_was_given() {
        let token = mint(&KEY, 1_000, peer(), b"first-id").unwrap();
        let moved: SocketAddr = "192.0.2.1:4434".parse().unwrap();
        let other: SocketAddr = "192.0.2.2:4433".parse().unwrap();
        assert_eq!(validate(&KEY, 1_000, moved, &token), None);
        assert_eq!(validate(&KEY, 1_000, other, &token), None);
        assert_eq!(validate(&[0x7c; 32], 1_000, peer(), &token), None);
        // The same address as IPv4 and as an IPv4-mapped IPv6 address is the same client.
        let mapped: SocketAddr = "[::ffff:192.0.2.1]:4433".parse().unwrap();
        assert_eq!(
            validate(&KEY, 1_000, mapped, &token),
            Some(&b"first-id"[..])
        );
    }

    #[test]
    fn an_id_longer_than_quic_allows_is_not_minted() {
        assert!(mint(&KEY, 0, peer(), &[1; 20]).is_some());
        assert!(mint(&KEY, 0, peer(), &[1; 21]).is_none());
    }

    proptest! {
        /// Change any byte of a token and it is refused.
        #[test]
        fn a_token_changed_anywhere_is_refused(
            odcid in prop::collection::vec(any::<u8>(), 0..=20),
            at in any::<prop::sample::Index>(),
            flip in 1..=255_u8,
        ) {
            let mut token = mint(&KEY, 1_000, peer(), &odcid).unwrap();
            let at = at.index(token.len());
            token[at] ^= flip;
            prop_assert_eq!(validate(&KEY, 1_000, peer(), &token), None);
        }

        /// Whatever the bytes, validating them never fails otherwise than by refusing.
        #[test]
        fn any_bytes_are_refused_or_read(token in prop::collection::vec(any::<u8>(), 0..64)) {
            let _ = validate(&KEY, 1_000, peer(), &token);
        }
    }
}
