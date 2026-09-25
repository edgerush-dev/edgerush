//! Fuzzes the connection-ID codec: an ID made from any key, worker and nonce is read back as
//! that worker, and any other bytes offered as an ID are read without failing, as ours only
//! when they have our form.
//!
//! The input is a 16-byte key, a 2-byte worker and a 14-byte nonce, then the bytes offered
//! as a destination ID: `cargo fuzz run quic_id corpus/quic_id seeds/quic_id`.

#![no_main]

use edgerush_proxy::quic::id::{Codec, Keys, LEN};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let Some((key, rest)) = bytes.split_first_chunk::<16>() else {
        return;
    };
    let Some((worker, rest)) = rest.split_first_chunk::<2>() else {
        return;
    };
    let Some((nonce, dcid)) = rest.split_first_chunk::<14>() else {
        return;
    };
    let mut codec = Codec::new(&Keys::new(*key, [0x5e; 32])).expect("any 16 bytes are a key");
    let worker = u16::from_be_bytes(*worker);

    let id = codec.encode(worker, nonce).expect("a block encrypts");
    assert_eq!(codec.decode(&id).expect("a block decrypts"), Some(worker));

    let read = codec.decode(dcid).expect("a block decrypts");
    assert_eq!(read.is_some(), dcid.len() == LEN && dcid[0] == id[0]);
    assert_eq!(
        codec.reset_token(dcid).expect("any bytes are a message"),
        codec.reset_token(dcid).expect("any bytes are a message"),
    );
});
