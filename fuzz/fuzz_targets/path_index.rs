//! Fuzzes the path stage against its reference: exact and prefix patterns must agree with
//! the segment-by-segment matcher, gRPC methods with the reference's, and the index with
//! the scan of every entry — which
//! entries are candidates for a path, and in what order. (There is no second regex engine
//! to compare regex patterns with; for those only the index's use of them is checked.)
//!
//! Input: the request path on the first line, then one entry per line. The first character
//! of an entry line gives its kind (`e` exact, `p` prefix, `r` regex, `g` gRPC method; any
//! other character counts as one of the first three), the rest is the pattern. Lines that
//! are not valid patterns are skipped.

#![no_main]

use edgerush_router::reference::{self, PathKind, PathSpec};
use edgerush_router::{PathIndex, PathPattern, normalise_path};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    let mut lines = input.split('\n');
    let Some(raw_path) = lines.next() else {
        return;
    };

    let mut specs: Vec<PathSpec> = Vec::new();
    let mut patterns = Vec::new();
    for line in lines {
        let mut characters = line.chars();
        let Some(kind) = characters.next() else {
            continue;
        };
        let text = characters.as_str();
        let (kind, pattern) = match kind {
            'g' => (PathKind::GrpcMethod, PathPattern::grpc_method(text)),
            _ => match u32::from(kind) % 3 {
                0 => (PathKind::Regex, PathPattern::regex(text)),
                1 => (PathKind::Prefix, PathPattern::prefix(text)),
                _ => (PathKind::Exact, PathPattern::exact(text)),
            },
        };
        let Ok(pattern) = pattern else {
            continue;
        };
        // The reference works on the canonical text of exact and prefix patterns and of
        // gRPC methods, and it must find every text canonical that the real code accepted.
        let rejected = || panic!("accepted {text:?}, which the reference rejects");
        let text = match kind {
            PathKind::Regex => text.to_owned(),
            PathKind::Exact | PathKind::Prefix => {
                reference::normalise_path(text).unwrap_or_else(rejected)
            }
            PathKind::GrpcMethod => reference::normalise_path(&format!("/{text}"))
                .and_then(|path| path.strip_prefix('/').map(str::to_owned))
                .unwrap_or_else(rejected),
        };
        specs.push((text, kind));
        patterns.push(pattern);
    }

    // What the router is given is the normalised path; the index must agree with its
    // patterns on any text all the same.
    let normal = normalise_path(raw_path).ok();
    let index = PathIndex::new(patterns.iter().cloned().zip(0..));
    for path in [Some(raw_path), normal.as_deref()].into_iter().flatten() {
        let matches = |n: usize| match &specs[n] {
            (_, PathKind::Regex) => patterns[n].matches(path),
            (text, PathKind::GrpcMethod) => {
                let expected = reference::grpc_method_matches(text, path);
                assert_eq!(patterns[n].matches(path), expected, "{text:?} on {path:?}");
                expected
            }
            (text, kind) => {
                let expected = reference::path_matches(text, *kind == PathKind::Prefix, path);
                assert_eq!(patterns[n].matches(path), expected, "{text:?} on {path:?}");
                expected
            }
        };
        let candidates: Vec<usize> = index.lookup(path).copied().collect();
        assert_eq!(
            candidates,
            reference::path_candidates(&specs, matches),
            "{specs:?} on {path:?}"
        );
    }
});
