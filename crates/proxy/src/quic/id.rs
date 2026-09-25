//! The connection IDs a worker issues as a server, in QUIC-LB's format, and their
//! stateless-reset tokens ([16 §3](../../../../docs/16-http3.md)).
//!
//! An ID is 17 bytes: a first octet, then one AES-128 block holding the worker's number and
//! a nonce ([QUIC-LB] §5.4.1, the single-pass case). Any worker can read which worker an ID
//! names, so a packet that reaches the wrong one is handed to its owner; a client cannot,
//! and no two IDs look related (RFC 9000 §5.1).
//!
//! [QUIC-LB]: https://www.ietf.org/archive/id/draft-ietf-quic-load-balancers-21.txt

use boring::error::ErrorStack;
use boring::symm::{Cipher, Crypter, Mode};

/// The length of every connection ID EdgeRush issues as a server.
pub const LEN: usize = 17;

/// The first octet of every ID: QUIC-LB's config codepoint 0 in the top three bits, and the
/// length of the rest in the low five (QUIC-LB §3.4), so that the length is known without
/// state, as RFC 9000 §10.3.2 asks of IDs that reset tokens are derived from.
const FIRST_OCTET: u8 = (LEN - 1) as u8;

/// An AES block: what follows the first octet.
const BLOCK: usize = 16;

/// The worker's number takes the block's first two bytes; the nonce the rest.
const NONCE_LEN: usize = BLOCK - 2;

/// The part of an ID that makes it unique among its worker's.
pub type Nonce = [u8; NONCE_LEN];

/// The keys every worker of the process issues and reads IDs with: made once at start, from
/// a secure generator, and never shared with another process (16 §3).
#[derive(Clone)]
pub struct Keys {
    /// AES-128, for the block.
    id: [u8; 16],
    /// HMAC-SHA256, for the reset tokens.
    reset: [u8; 32],
}

impl Keys {
    /// The keys, as the driver drew them.
    pub fn new(id: [u8; 16], reset: [u8; 32]) -> Keys {
        Keys { id, reset }
    }
}

/// Why an ID could not be made or read. BoringSSL refuses nothing a well-formed key and
/// block could cause, so either is a fault in the library or the process, not in a packet.
#[derive(Debug, thiserror::Error)]
pub enum IdError {
    /// BoringSSL failed an AES or HMAC operation.
    #[error("BoringSSL failed on a connection ID: {0}")]
    Crypto(#[from] ErrorStack),
    /// The cipher gave back other than one block for one block.
    #[error("the cipher gave {0} bytes for a 16-byte block")]
    Block(usize),
}

/// One worker's codec: it holds its cipher contexts, so it is made once per worker and kept.
pub struct Codec {
    encrypt: Crypter,
    decrypt: Crypter,
    reset: [u8; 32],
}

impl Codec {
    /// A codec under `keys`.
    pub fn new(keys: &Keys) -> Result<Codec, IdError> {
        let cipher = |mode| -> Result<Crypter, IdError> {
            let mut crypter = Crypter::new(Cipher::aes_128_ecb(), mode, &keys.id, None)?;
            // One block in, one block out: nothing to pad and nothing held back.
            crypter.pad(false);
            Ok(crypter)
        };
        Ok(Codec {
            encrypt: cipher(Mode::Encrypt)?,
            decrypt: cipher(Mode::Decrypt)?,
            reset: keys.reset,
        })
    }

    /// The ID that names `worker`, made unique by `nonce`.
    pub fn encode(&mut self, worker: u16, nonce: &Nonce) -> Result<[u8; LEN], IdError> {
        let mut plain = [0; BLOCK];
        plain[..2].copy_from_slice(&worker.to_be_bytes());
        plain[2..].copy_from_slice(nonce);
        let mut id = [0; LEN];
        id[0] = FIRST_OCTET;
        id[1..].copy_from_slice(&one_block(&mut self.encrypt, &plain)?);
        Ok(id)
    }

    /// The worker `dcid` names, if it has the form of an ID of ours. Any 16 bytes after our
    /// first octet decrypt to some worker: whether that worker exists is the caller's to
    /// check, and a made-up ID is not refused here (16 §3: routing is not authentication).
    pub fn decode(&mut self, dcid: &[u8]) -> Result<Option<u16>, IdError> {
        let Some((&FIRST_OCTET, block)) = dcid.split_first() else {
            return Ok(None);
        };
        let Ok(block) = <&[u8; BLOCK]>::try_from(block) else {
            return Ok(None);
        };
        let plain = one_block(&mut self.decrypt, block)?;
        Ok(Some(u16::from_be_bytes([plain[0], plain[1]])))
    }

    /// The stateless-reset token for `id`: HMAC-SHA256 under the reset key, its first 16
    /// bytes (RFC 9000 §10.3.2), as the `u128` quiche takes and writes big-endian.
    pub fn reset_token(&self, id: &[u8]) -> Result<u128, IdError> {
        let mac = boring::hash::hmac_sha256(&self.reset, id)?;
        let mut token = [0; 16];
        token.copy_from_slice(&mac[..16]);
        Ok(u128::from_be_bytes(token))
    }
}

/// `block` through `crypter`, which is in ECB mode without padding.
fn one_block(crypter: &mut Crypter, block: &[u8; BLOCK]) -> Result<[u8; BLOCK], IdError> {
    // BoringSSL's update asks for a block's room beyond the input, padding or not.
    let mut out = [0; 2 * BLOCK];
    let written = crypter.update(block, &mut out)?;
    if written != BLOCK {
        return Err(IdError::Block(written));
    }
    let mut result = [0; BLOCK];
    result.copy_from_slice(&out[..BLOCK]);
    Ok(result)
}

/// The nonces one worker puts in its IDs: a counter over 112 bits from a random start, so
/// that none repeats under a key (QUIC-LB §9.6). At a billion IDs a second it would take
/// 10^17 years to come round to its start.
pub struct Nonces {
    next: u128,
}

/// How many nonces there are.
const NONCES: u128 = 1 << (8 * NONCE_LEN);

impl Nonces {
    /// A counter from `start`, of which only the low 112 bits count.
    pub fn starting_at(start: u128) -> Nonces {
        Nonces {
            next: start % NONCES,
        }
    }

    /// The next nonce.
    pub fn draw(&mut self) -> Nonce {
        let mut nonce = [0; NONCE_LEN];
        nonce.copy_from_slice(&self.next.to_be_bytes()[size_of::<u128>() - NONCE_LEN..]);
        // Below 2^112, so adding one cannot overflow.
        self.next = (self.next + 1) % NONCES;
        nonce
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    }

    fn codec(id: [u8; 16]) -> Codec {
        Codec::new(&Keys::new(id, [0x5e; 32])).unwrap()
    }

    fn nonce(bytes: &[u8]) -> Nonce {
        bytes.try_into().unwrap()
    }

    fn differing_bits(a: &[u8], b: &[u8]) -> u32 {
        a.iter().zip(b).map(|(a, b)| (a ^ b).count_ones()).sum()
    }

    /// QUIC-LB's single-pass vector (Appendix B.2, config 2): server ID `ed793a51d49b8f5f`
    /// and nonce `ee080dbf48c0d1e5` under its key. A single pass encrypts the 16 bytes as
    /// one block however they are split, so they read as our worker and nonce; only the
    /// first octet differs, since ours is config codepoint 0 where the vector's is 2.
    #[test]
    fn quic_lb_single_pass_test_vector() {
        let key: [u8; 16] = hex("8f95f09245765f80256934e50c66207f").try_into().unwrap();
        let mut codec = codec(key);

        let id = codec
            .encode(0xed79, &nonce(&hex("3a51d49b8f5fee080dbf48c0d1e5")))
            .unwrap();

        assert_eq!(id[0], 0b000_10000);
        assert_eq!(id[1..], hex("4dd2d05a7b0de9b2b9907afb5ecf8cc3"));
        assert_eq!(codec.decode(&id).unwrap(), Some(0xed79));
    }

    proptest! {
        /// Every worker, under every key and nonce, is read back from its ID.
        #[test]
        fn every_worker_is_read_back(
            key in any::<[u8; 16]>(),
            worker in any::<u16>(),
            nonce in any::<[u8; NONCE_LEN]>(),
        ) {
            let mut codec = codec(key);
            let id = codec.encode(worker, &nonce).unwrap();
            prop_assert_eq!(id.len(), LEN);
            prop_assert_eq!(codec.decode(&id).unwrap(), Some(worker));
        }

        /// Anything not 17 bytes long, or not starting with our first octet, is not an ID of
        /// ours: the client's own Initial DCID, another pod's, garbage.
        #[test]
        fn only_our_form_is_read(dcid in proptest::collection::vec(any::<u8>(), 0..=20)) {
            let ours = dcid.len() == LEN && dcid[0] == FIRST_OCTET;
            let read = codec([1; 16]).decode(&dcid).unwrap();
            prop_assert_eq!(read.is_some(), ours);
        }
    }

    /// IDs a worker issues one after another share nothing a client could see: consecutive
    /// nonces and the same worker still differ in about half their bits, and so do two
    /// workers' IDs with the same nonce. Left unencrypted, they would differ in a few.
    #[test]
    fn ids_look_unrelated() {
        let mut codec = codec([9; 16]);
        let mut nonces = Nonces::starting_at(0x1234);
        let ids: Vec<[u8; LEN]> = (0..1_000)
            .map(|_| codec.encode(3, &nonces.draw()).unwrap())
            .collect();
        for pair in ids.windows(2) {
            let bits = differing_bits(&pair[0][1..], &pair[1][1..]);
            assert!((32..=96).contains(&bits), "{bits} bits apart");
        }
        let same_nonce = [0x42; NONCE_LEN];
        let one = codec.encode(0, &same_nonce).unwrap();
        let other = codec.encode(1, &same_nonce).unwrap();
        assert!((32..=96).contains(&differing_bits(&one[1..], &other[1..])));
    }

    /// RFC 4231's second HMAC-SHA256 case (key "Jefe", zero-padded, which HMAC does anyway):
    /// the token is the MAC's first 16 bytes, in the order quiche writes them.
    #[test]
    fn the_reset_token_is_the_mac_s_first_sixteen_bytes() {
        let mut reset = [0; 32];
        reset[..4].copy_from_slice(b"Jefe");
        let codec = Codec::new(&Keys::new([0; 16], reset)).unwrap();

        let token = codec.reset_token(b"what do ya want for nothing?").unwrap();

        assert_eq!(
            token.to_be_bytes()[..],
            hex("5bdcc146bf60754e6a042426089575c7")
        );
    }

    #[test]
    fn tokens_differ_by_id_and_by_key() {
        let first = Codec::new(&Keys::new([0; 16], [1; 32])).unwrap();
        let second = Codec::new(&Keys::new([0; 16], [2; 32])).unwrap();
        let id = [FIRST_OCTET; LEN];
        let mut other = id;
        other[LEN - 1] ^= 1;

        assert_eq!(
            first.reset_token(&id).unwrap(),
            first.reset_token(&id).unwrap()
        );
        assert_ne!(
            first.reset_token(&id).unwrap(),
            first.reset_token(&other).unwrap()
        );
        assert_ne!(
            first.reset_token(&id).unwrap(),
            second.reset_token(&id).unwrap()
        );
    }

    #[test]
    fn nonces_count_from_their_start_and_wrap_within_112_bits() {
        let mut nonces = Nonces::starting_at(0xabcd);
        assert_eq!(u128_of(nonces.draw()), 0xabcd);
        assert_eq!(u128_of(nonces.draw()), 0xabce);

        let mut nonces = Nonces::starting_at(u128::MAX);
        assert_eq!(u128_of(nonces.draw()), NONCES - 1);
        assert_eq!(u128_of(nonces.draw()), 0);
    }

    fn u128_of(nonce: Nonce) -> u128 {
        let mut bytes = [0; 16];
        bytes[16 - NONCE_LEN..].copy_from_slice(&nonce);
        u128::from_be_bytes(bytes)
    }
}
