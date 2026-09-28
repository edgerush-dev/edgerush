//! Fuzzes the walk over `X-Forwarded-For` that finds a trusted proxy's client against its
//! reference, which splits every entry out and reads them from the right: the one pass from
//! the left must come to the same address, however the fields are written or broken.
//!
//! Input: one field value per line.

#![no_main]

use edgerush_filters::forwarding::{TrustedProxies, client_address, reference};
use libfuzzer_sys::fuzz_target;
use std::net::IpAddr;

fuzz_target!(|input: &[u8]| {
    let values: Vec<&[u8]> = input.split(|&byte| byte == b'\n').collect();
    let trusted = TrustedProxies::new(["10.0.0.0/8", "192.0.2.0/24", "2001:db8::/32"])
        .unwrap_or_else(|error| panic!("{error}"));
    for peer in ["10.1.0.5", "::ffff:10.1.0.5", "2001:db8::1"] {
        let peer: IpAddr = peer.parse().unwrap_or_else(|error| panic!("{error}"));
        let found = client_address(peer, values.iter().copied(), &trusted);
        assert_eq!(
            found,
            reference::client_address(peer, values.iter().copied(), &trusted),
            "{values:?} from {peer}"
        );
        // Always in its one form.
        assert_eq!(found, found.to_canonical(), "{values:?} from {peer}");
    }
});
