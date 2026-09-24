//! Fuzzes the HTTP/1 server composed: the real connection driver over a socket that is
//! nothing but memory, on a stopped clock, in front of a scripted core
//! ([14 §9](../../docs/14-downstream-server.md), step 5).
//!
//! One input becomes a client's script and a core's ([`composed::decode`]); the run is held
//! against both oracles. The lifecycle oracle says whether the connection's life was one
//! it may have had — what reached the core, in what order, which answers went back, and
//! when it had to close; the reference reader says whether what the core was handed is
//! what the client sent. Storage left held, or a run that no deadline of the driver's
//! ended, is a finding too.

#![no_main]

use edgerush_proxy::downstream::h1::composed::{self, judge};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let (client, core, pipe) = composed::decode(bytes);
    let run = composed::run_with(&client, &core, pipe);
    if let Err(finding) = judge(&run) {
        panic!(
            "{finding:?}\npipe: {pipe}\nclient: {client:?}\ncore: {core:?}\nevents: {:?}\nsent: {:?}\nreceived: {:?}",
            run.events,
            String::from_utf8_lossy(&run.sent),
            String::from_utf8_lossy(&run.received),
        );
    }
});
