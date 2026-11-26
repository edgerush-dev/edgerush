//! Fuzzes the asynchronous mode's wire and the pod of both modes': any bytes offered as a data
//! plane's batch up its stream, or as a message on any stream between pods, the owners'
//! copies among them, are decoded and converted without failing, and whatever converts comes
//! back the same through an encoding of its own: `cargo fuzz run ratelimit_wire
//! corpus/ratelimit_wire`.

#![no_main]

use edgerush_cluster_asynchronous::Value as AsyncValue;
use edgerush_ratelimit_service::cluster::wire::asynchronous::{
    self, Asynchronous, Batch, batch_from, batch_to_wire,
};
use edgerush_ratelimit_service::cluster::wire::both::{self, Both};
use edgerush_ratelimit_service::cluster::wire::{Outgoing, ToCopy, ToPeer};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(sent) = Batch::decode(bytes)
        && let Ok(batch) = batch_from(sent)
    {
        let again = Batch::decode(batch_to_wire(batch.clone()).encode_to_vec().as_slice())
            .expect("our own encoding decodes");
        assert_eq!(batch_from(again), Ok(batch));
    }
    // The asynchronous mode's pods.
    let again = |outgoing: Outgoing<Asynchronous>| match outgoing {
        Outgoing::Coordinate(sent) | Outgoing::Peer(sent) => {
            asynchronous::mode_from_peer::<AsyncValue>(
                ToPeer::decode(sent.encode_to_vec().as_slice()).expect("our own encoding decodes"),
            )
        }
        Outgoing::Copy(sent) => asynchronous::mode_from_copy::<AsyncValue>(
            ToCopy::decode(sent.encode_to_vec().as_slice()).expect("our own encoding decodes"),
        ),
    };
    if let Ok(sent) = ToPeer::<Asynchronous>::decode(bytes)
        && let Ok((from, message)) = asynchronous::mode_from_peer::<AsyncValue>(sent)
        && let Some(outgoing) = asynchronous::mode_wire(from, message.clone())
    {
        assert_eq!(again(outgoing), Ok((from, message)));
    }
    if let Ok(sent) = ToCopy::<Asynchronous>::decode(bytes)
        && let Ok((from, message)) = asynchronous::mode_from_copy::<AsyncValue>(sent)
        && let Some(outgoing) = asynchronous::mode_wire(from, message.clone())
    {
        assert_eq!(again(outgoing), Ok((from, message)));
    }
    // The pods of both modes.
    let again = |outgoing: Outgoing<Both>| match outgoing {
        Outgoing::Coordinate(sent) | Outgoing::Peer(sent) => both::mode_from_peer(
            ToPeer::decode(sent.encode_to_vec().as_slice()).expect("our own encoding decodes"),
        ),
        Outgoing::Copy(sent) => both::mode_from_copy(
            ToCopy::decode(sent.encode_to_vec().as_slice()).expect("our own encoding decodes"),
        ),
    };
    if let Ok(sent) = ToPeer::<Both>::decode(bytes)
        && let Ok((from, message)) = both::mode_from_peer(sent)
        && let Some(outgoing) = both::mode_wire(from, message.clone())
    {
        assert_eq!(again(outgoing), Ok((from, message)));
    }
    if let Ok(sent) = ToCopy::<Both>::decode(bytes)
        && let Ok((from, message)) = both::mode_from_copy(sent)
        && let Some(outgoing) = both::mode_wire(from, message.clone())
    {
        assert_eq!(again(outgoing), Ok((from, message)));
    }
});
