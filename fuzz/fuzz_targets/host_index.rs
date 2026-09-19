//! Fuzzes the host stage against its reference: every pattern must agree with the
//! label-by-label matcher, and the index with the scan of every claim — which claims are
//! candidates for a host, and in what order.
//!
//! Input: the request host on the first line, then one claim per line. The first character
//! of a claim line gives its flags (bit 0: falls through; bit 1: the wildcard stands for
//! exactly one label), the rest is the pattern; no pattern claims every host. Lines that
//! are not valid patterns are skipped.

#![no_main]

use edgerush_router::reference::{self, HostClaimSpec};
use edgerush_router::{HostClaim, HostIndex, HostPattern, WildcardLabels};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    let mut lines = input.split('\n');
    let Some(host) = lines.next() else {
        return;
    };

    let mut specs: Vec<HostClaimSpec> = Vec::new();
    let mut claims = Vec::new();
    for line in lines {
        let mut characters = line.chars();
        let Some(flags) = characters.next().map(u32::from) else {
            continue;
        };
        let text = characters.as_str();
        let falls_through = flags & 1 != 0;
        let wildcard = if flags & 2 != 0 {
            WildcardLabels::One
        } else {
            WildcardLabels::OneOrMore
        };
        let pattern = if text.is_empty() {
            None
        } else if let Ok(pattern) = HostPattern::parse(text, wildcard) {
            assert_eq!(
                pattern.matches(host),
                reference::host_matches(text, wildcard, host),
                "{text:?} ({wildcard:?}) on {host:?}"
            );
            Some(pattern)
        } else {
            continue;
        };
        specs.push((
            pattern.as_ref().map(|_| (text.to_owned(), wildcard)),
            falls_through,
        ));
        claims.push(HostClaim {
            pattern,
            falls_through,
            value: claims.len(),
        });
    }

    let index = HostIndex::new(claims);
    assert_eq!(
        index.lookup(host),
        reference::host_candidates(&specs, host),
        "{specs:?} on {host:?}"
    );
});
