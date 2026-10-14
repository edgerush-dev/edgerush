//! Fuzzes the rate-limiting service's wire: any bytes offered as a data plane's batch or an
//! owner's copies are decoded and converted without failing, and whatever converts comes
//! back the same through an encoding of its own:
//! `cargo fuzz run ratelimit_wire corpus/ratelimit_wire`.

#![no_main]

use edgerush_ratelimit::service;
use edgerush_ratelimit_service::wire::{Batch, Replicas};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(batch) = Batch::decode(bytes)
        && let Ok(batch) = service::Batch::try_from(batch)
    {
        let again = Batch::decode(Batch::from(batch.clone()).encode_to_vec().as_slice())
            .expect("our own encoding decodes");
        assert_eq!(service::Batch::try_from(again), Ok(batch));
    }
    if let Ok(replicas) = Replicas::decode(bytes)
        && let Ok(replicas) = service::Replicas::try_from(replicas)
    {
        let again = Replicas::decode(Replicas::from(replicas.clone()).encode_to_vec().as_slice())
            .expect("our own encoding decodes");
        assert_eq!(service::Replicas::try_from(again), Ok(replicas));
    }
});
