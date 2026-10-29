//! Fuzzes the reading of a nameserver's answer ([03 §6](../../../docs/03-data-plane.md)):
//! any bytes at all, as whoever can send to the resolver's socket can send any.
//!
//! Reading must not fail, and an error must be told without failing. An answer of
//! addresses holds at least one, each once and in order.
//!
//! `cargo fuzz run dns_answer corpus/dns_answer seeds/dns_answer`.

#![no_main]

use edgerush_dns::{Answer, Kind, read};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    for kind in [Kind::A, Kind::Aaaa] {
        match read(data, 0x4d2a, "web.shop.svc.cluster.local", kind) {
            Ok(Answer::Addresses { addresses, .. }) => {
                assert!(!addresses.is_empty());
                assert!(addresses.windows(2).all(|pair| pair[0] < pair[1]));
            }
            Ok(_) => {}
            Err(error) => {
                let _told = error.to_string();
            }
        }
    }
});
