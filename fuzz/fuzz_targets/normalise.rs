//! Fuzzes path normalisation: whatever text arrives as a request path, normalising must
//! not panic and must agree with the reference implementation, on the result and on what
//! is rejected. What it accepts must be in normal form, stay the same when normalised
//! again, and be matched by the patterns made from the same text. The same holds where an
//! encoded slash is kept, which changes nothing for a path without one.

#![no_main]

use edgerush_router::{
    EncodedSlash, NormaliseError, PathPattern, normalise_path, normalise_path_with, reference,
};
use libfuzzer_sys::fuzz_target;
use std::borrow::Cow;

fuzz_target!(|path: &str| {
    // Paths that do not start with a slash are rejected at once; spend the effort behind it.
    let absolute = format!("/{path}");
    for path in [path, &absolute] {
        check(path);
        check_kept(path);
    }
});

/// What [`check`] holds a path to, where an encoded slash inside a segment is kept.
fn check_kept(path: &str) {
    let keep = EncodedSlash::Keep;
    let kept = normalise_path_with(path, keep);
    assert_eq!(
        kept.as_deref().ok(),
        reference::normalise_path_with(path, keep).as_deref(),
        "{path:?}"
    );
    // Only a path refused for an encoded slash alone is normalised otherwise.
    match normalise_path(path) {
        Err(NormaliseError::EncodedSlash) => {}
        refused_or_not => assert_eq!(kept, refused_or_not, "{path:?}"),
    }
    let Ok(kept) = kept else {
        return;
    };
    assert!(!kept.contains("%2f"), "{path:?} -> {kept:?}");
    for segment in kept.split('/').skip(1) {
        let pieces: Vec<&str> = segment.split("%2F").collect();
        if pieces.len() > 1 {
            for piece in pieces {
                let name = piece.split(';').next().unwrap_or(piece);
                let name = name.split("%3B").next().unwrap_or(name);
                assert!(
                    !piece.is_empty() && name != "." && name != "..",
                    "{path:?} -> {kept:?}"
                );
            }
        }
    }
    assert_eq!(
        normalise_path_with(&kept, keep),
        Ok(Cow::Borrowed(&*kept)),
        "{path:?}"
    );
    if let Ok(exact) = PathPattern::exact_with(path, keep) {
        assert!(exact.matches(&kept), "exact {path:?} -> {kept:?}");
    }
    if let Ok(prefix) = PathPattern::prefix_with(path, keep) {
        assert!(prefix.matches(&kept), "prefix {path:?} -> {kept:?}");
    }
}

fn check(path: &str) {
    let normal = normalise_path(path).ok();
    assert_eq!(
        normal.as_deref(),
        reference::normalise_path(path).as_deref(),
        "{path:?}"
    );
    let Some(normal) = normal else {
        return;
    };

    // The definition of normal form, as far as it can be said without repeating the code.
    assert!(normal.starts_with('/'), "{path:?} -> {normal:?}");
    assert!(normal.is_ascii(), "{path:?} -> {normal:?}");
    assert!(!normal.contains('\\'), "{path:?} -> {normal:?}");
    assert!(
        !normal.bytes().any(|byte| byte.is_ascii_control()),
        "{path:?} -> {normal:?}"
    );
    let mut segments = normal.split('/').skip(1).peekable();
    while let Some(segment) = segments.next() {
        let last = segments.peek().is_none();
        assert!(last || !segment.is_empty(), "{path:?} -> {normal:?}");
        // Up to the first `;`, however it is written: no server may find a dot segment.
        let name = segment.split(';').next().unwrap_or(segment);
        let name = name.split("%3B").next().unwrap_or(name);
        assert!(name != "." && name != "..", "{path:?} -> {normal:?}");
    }
    for encoded in ["%2F", "%2f", "%5C", "%5c", "%2E", "%2e", "%41", "%61"] {
        assert!(!normal.contains(encoded), "{path:?} -> {normal:?}");
    }

    // Normalising again changes nothing.
    assert_eq!(
        normalise_path(&normal),
        Ok(Cow::Borrowed(&*normal)),
        "{path:?}"
    );

    // However a pattern is spelt, it matches the normal form of the same spelling.
    if let Ok(exact) = PathPattern::exact(path) {
        assert!(exact.matches(&normal), "exact {path:?} -> {normal:?}");
    }
    if let Ok(prefix) = PathPattern::prefix(path) {
        assert!(prefix.matches(&normal), "prefix {path:?} -> {normal:?}");
    }
}
