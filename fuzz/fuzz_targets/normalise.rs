//! Fuzzes path normalisation: whatever text arrives as a request path, normalising must
//! not panic and must agree with the reference implementation, on the result and on what
//! is rejected. What it accepts must be in normal form, stay the same when normalised
//! again, and be matched by the patterns made from the same text.

#![no_main]

use edgerush_router::{PathPattern, normalise_path, reference};
use libfuzzer_sys::fuzz_target;
use std::borrow::Cow;

fuzz_target!(|path: &str| {
    // Paths that do not start with a slash are rejected at once; spend the effort behind it.
    let absolute = format!("/{path}");
    for path in [path, &absolute] {
        check(path);
    }
});

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
        let name = segment.split(';').next().unwrap_or(segment);
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
