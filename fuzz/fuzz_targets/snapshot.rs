//! Fuzzes the reading of a controller replica's snapshot, the leader's record that the
//! others translate from ([04 §2](../../../docs/04-control-plane.md)): any bytes at all, as
//! a peer's are read before anything of them is trusted.
//!
//! Reading must not fail, and an error must be told without failing. A snapshot read is
//! written again and read back as it was.
//!
//! `cargo fuzz run snapshot corpus/snapshot seeds/snapshot`.

#![no_main]

use edgerush_control_stream::{CONFIG_VERSION, ConfigVersion};
use edgerush_controller::snapshot::Snapshot;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let snapshot = match Snapshot::decode(data) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let _told = error.to_string();
            return;
        }
    };
    let version = ConfigVersion::served(CONFIG_VERSION).expect("this build reads its own version");
    let written = snapshot
        .encode(version)
        .expect("a snapshot read is written");
    assert_eq!(
        Snapshot::decode(&written).expect("a snapshot written is read"),
        snapshot
    );
});
