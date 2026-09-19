//! Fuzzes the reading of a request's host: whatever text arrives as a `Host` field or a
//! target's authority, reading it must not panic, and what it accepts the `http` crate must
//! read in the same way — the host that is routed on is then the host a request built from
//! the same text is sent to. The bare host is the start of the text, is never empty, and
//! read again is itself.

#![no_main]

use edgerush_proxy::host::bare_host;
use http::uri::Authority;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let Ok(host) = bare_host(text) else {
        return;
    };
    assert!(!host.is_empty(), "{text:?}");
    assert!(text.starts_with(host), "{text:?} -> {host:?}");
    if !host.ends_with('.') {
        assert_eq!(bare_host(host), Ok(host), "{text:?}");
    }

    let authority = Authority::try_from(text).unwrap_or_else(|error| panic!("{text:?}: {error}"));
    let theirs = authority.host();
    assert_eq!(host, theirs.strip_suffix('.').unwrap_or(theirs), "{text:?}");
});
