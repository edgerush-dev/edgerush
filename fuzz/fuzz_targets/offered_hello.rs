//! Fuzzes the reader of what a client's ClientHello offers, which picks among a name's
//! certificates (`offered.rs`): any bytes at all, as the bodies BoringSSL hands the early
//! callback. It must never panic, must judge a key it does not tell apart as usable, and
//! must judge alike bytes that cannot be read and bytes never sent where TLS 1.3 needs
//! them.
//!
//! Input: four parts, each its length in one byte and then that many bytes (what is left
//! where a part runs short): the signature schemes, the groups and the versions, each
//! none where its length byte is 255, then the cipher suites.
//!
//! `cargo fuzz run offered_hello`.

#![no_main]

use edgerush_proxy::offered::{Curve, Hello, Key, Offered};
use libfuzzer_sys::fuzz_target;

/// The next part of `data`, and what follows it: none where its length byte says 255.
fn part(data: &[u8]) -> (Option<&[u8]>, &[u8]) {
    let Some((&length, rest)) = data.split_first() else {
        return (None, data);
    };
    if length == 255 {
        return (None, rest);
    }
    let (body, rest) = rest.split_at(usize::from(length).min(rest.len()));
    (Some(body), rest)
}

fuzz_target!(|data: &[u8]| {
    let (schemes, data) = part(data);
    let (groups, data) = part(data);
    let (versions, suites) = part(data);
    let offered = Offered::of(&Hello {
        schemes,
        groups,
        versions,
        suites,
    });
    assert!(offered.can_use(Key::Other));
    let keys = [
        Key::Rsa,
        Key::Ecdsa(Curve::P256),
        Key::Ecdsa(Curve::P384),
        Key::Ecdsa(Curve::P521),
    ];
    // Read again, the same: nothing is kept between reads.
    let again = Offered::of(&Hello {
        schemes,
        groups,
        versions,
        suites,
    });
    for key in keys {
        assert_eq!(offered.can_use(key), again.can_use(key));
    }
    // In TLS 1.3 the schemes alone decide: no suite or group changes what is offered.
    let tls13 = versions.is_some_and(|versions| {
        versions.first().is_some_and(|&length| usize::from(length) + 1 == versions.len())
            && versions[1..]
                .chunks_exact(2)
                .any(|version| version == [0x03, 0x04])
    });
    if tls13 {
        let bare = Offered::of(&Hello {
            schemes,
            groups: None,
            versions,
            suites: &[],
        });
        for key in keys {
            assert_eq!(offered.can_use(key), bare.can_use(key), "{key:?}");
        }
    }
});
