//! Fuzzes the cluster store's wire: any bytes offered as a message on either stream between
//! pods are decoded and converted without failing, whatever converts comes back the same
//! through an encoding of its own, and any bytes offered as a run of pieces are put together
//! without failing or holding more than the bound:
//! `cargo fuzz run cluster_wire corpus/cluster_wire`.

#![no_main]

use bytes::Bytes;
use edgerush_cluster_synchronous::Value;
use edgerush_ratelimit_service::cluster::wire::{
    self, Joining, Outgoing, Piece, Synchronous, ToCopy, ToPeer,
};
use libfuzzer_sys::fuzz_target;
use prost::Message;

/// The most a run of pieces is let hold here.
const MOST: usize = 4_096;

fuzz_target!(|bytes: &[u8]| {
    let again = |outgoing: Outgoing<Synchronous>| match outgoing {
        Outgoing::Peer(sent) => wire::from_peer::<Value>(
            ToPeer::decode(sent.encode_to_vec().as_slice()).expect("our own encoding decodes"),
        ),
        Outgoing::Copy(sent) => wire::from_copy::<Value>(
            ToCopy::decode(sent.encode_to_vec().as_slice()).expect("our own encoding decodes"),
        ),
    };
    if let Ok(sent) = ToPeer::<Synchronous>::decode(bytes)
        && let Ok((from, message)) = wire::from_peer::<Value>(sent)
    {
        assert_eq!(
            again(wire::wire(from, message.clone())),
            Ok((from, message))
        );
    }
    if let Ok(sent) = ToCopy::<Synchronous>::decode(bytes)
        && let Ok((from, message)) = wire::from_copy::<Value>(sent)
    {
        assert_eq!(
            again(wire::wire(from, message.clone())),
            Ok((from, message))
        );
    }
    // The bytes as pieces, each a length byte and its part, the length's top bit for more.
    let mut joining = Joining::new(MOST);
    let mut rest = bytes;
    while let Some((&head, tail)) = rest.split_first() {
        let length = usize::from(head & 0x7f).min(tail.len());
        let (part, after) = tail.split_at(length);
        let piece = Piece {
            part: Bytes::copy_from_slice(part),
            more: head & 0x80 != 0,
        };
        if let Ok(Some(whole)) = joining.take(&piece) {
            assert!(whole.len() <= MOST);
        }
        rest = after;
    }
});
