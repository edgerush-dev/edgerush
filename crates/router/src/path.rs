//! Path patterns: exact paths and segment-wise prefixes, as Gateway API and Ingress both
//! define them.
//!
//! Patterns are compared byte for byte, case-sensitively, with the *normalised* request
//! path ([`normalise_path`]). Normalising the request path is not done here: the caller
//! does it once per request. Patterns get the same canonical percent-encoding when they
//! are built, so `/café`, `/caf%c3%a9` and `/caf%C3%A9` are one pattern; but a pattern
//! with `//` or dot segments, which says something other than what it would match, is
//! rejected rather than quietly rewritten.

use crate::{NormaliseError, normalise_path};

/// A validated path pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathPattern {
    pub(crate) kind: Kind,
    /// For a prefix, without its trailing slash (the root prefix is the empty string), so
    /// that `/shop` and `/shop/` are one pattern and the boundary check is uniform.
    pub(crate) path: Box<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Exact,
    Prefix,
}

impl PathPattern {
    /// A pattern that matches `path` and nothing else; `/shop` and `/shop/` differ.
    ///
    /// # Errors
    ///
    /// Returns a [`PathPatternError`] if `path` is not absolute, has empty or dot segments, or is
    /// a path that requests are rejected for.
    pub fn exact(path: &str) -> Result<Self, PathPatternError> {
        Ok(Self {
            kind: Kind::Exact,
            path: canonical(path)?.into(),
        })
    }

    /// A pattern that matches `prefix` and everything below it, segment by segment:
    /// `/shop` covers `/shop`, `/shop/` and `/shop/cart` but not `/shopping`. A trailing
    /// slash on the prefix means nothing, and `/` covers every path.
    ///
    /// # Errors
    ///
    /// Returns a [`PathPatternError`] if `prefix` is not absolute, has empty or dot segments, or
    /// is a path that requests are rejected for.
    pub fn prefix(prefix: &str) -> Result<Self, PathPatternError> {
        let prefix = canonical(prefix)?;
        Ok(Self {
            kind: Kind::Prefix,
            path: prefix.strip_suffix('/').unwrap_or(&prefix).into(),
        })
    }

    /// Whether the request path falls under this pattern.
    ///
    /// `path` is the normalised path alone, without the query string. Anything that does
    /// not start with `/` (such as the `*` of `OPTIONS *`) matches nothing. Never
    /// allocates.
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        match self.kind {
            Kind::Exact => path == &*self.path,
            // What follows the prefix must be nothing or start a new segment. The check for
            // a leading slash only matters to the root prefix, stored as the empty string.
            Kind::Prefix => {
                path.starts_with('/')
                    && path
                        .strip_prefix(&*self.path)
                        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
            }
        }
    }
}

/// The pattern text in the normal form request paths have: structure checked, not changed;
/// percent-encoding made canonical.
fn canonical(path: &str) -> Result<std::borrow::Cow<'_, str>, PathPatternError> {
    validate(path)?;
    Ok(normalise_path(path)?)
}

/// Checks that `path` is absolute and has the structure the normaliser produces.
fn validate(path: &str) -> Result<(), PathPatternError> {
    let segments = path
        .strip_prefix('/')
        .ok_or(PathPatternError::NotAbsolute)?;
    let mut segments = segments.split('/').peekable();
    while let Some(segment) = segments.next() {
        let last = segments.peek().is_none();
        match segment {
            // An empty last segment is just a trailing slash.
            "" if !last => return Err(PathPatternError::EmptySegment),
            "." | ".." => return Err(PathPatternError::DotSegment),
            _ => {}
        }
    }
    Ok(())
}

/// Why a path pattern was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PathPatternError {
    /// The path does not start with `/` (or is empty).
    #[error("path does not start with `/`")]
    NotAbsolute,
    /// The path contains `//`, which normalised request paths never do.
    #[error("path contains an empty segment (`//`)")]
    EmptySegment,
    /// The path contains a `.` or `..` segment, which normalised request paths never do.
    #[error("path contains a `.` or `..` segment")]
    DotSegment,
    /// The path is one that a request would be rejected for, so nothing could match it.
    #[error(transparent)]
    Ambiguous(#[from] NormaliseError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::{nasty_path, path_near, path_pattern_text};
    use proptest::prelude::*;

    fn exact(path: &str) -> PathPattern {
        PathPattern::exact(path).unwrap()
    }

    fn prefix(path: &str) -> PathPattern {
        PathPattern::prefix(path).unwrap()
    }

    #[test]
    fn exact_pattern_matches_only_the_same_bytes() {
        let shop = exact("/shop");
        assert!(shop.matches("/shop"));
        assert!(!shop.matches("/shop/"));
        assert!(!shop.matches("/shop/cart"));
        assert!(!shop.matches("/Shop"));
        assert!(!shop.matches("/sho"));
        assert!(!shop.matches(""));
        assert!(exact("/shop/").matches("/shop/"));
        assert!(!exact("/shop/").matches("/shop"));
        assert!(exact("/").matches("/"));
        assert!(!exact("/").matches("/shop"));
    }

    #[test]
    fn prefix_pattern_matches_whole_segments() {
        let shop = prefix("/shop");
        assert!(shop.matches("/shop"));
        assert!(shop.matches("/shop/"));
        assert!(shop.matches("/shop/cart"));
        assert!(shop.matches("/shop/cart/"));
        assert!(!shop.matches("/shopping"));
        assert!(!shop.matches("/shop.html"));
        assert!(!shop.matches("/Shop"));
        assert!(!shop.matches("/sho"));
        assert!(!shop.matches("/"));
        assert!(!shop.matches("/a/shop"));
    }

    #[test]
    fn trailing_slash_on_a_prefix_means_nothing() {
        assert_eq!(prefix("/shop/"), prefix("/shop"));
        assert!(prefix("/shop/").matches("/shop"));
        assert!(prefix("/a/b/").matches("/a/b/c"));
        assert!(!prefix("/a/b/").matches("/a/bc"));
    }

    #[test]
    fn root_prefix_matches_every_absolute_path() {
        let root = prefix("/");
        assert!(root.matches("/"));
        assert!(root.matches("/shop"));
        assert!(root.matches("/shop/cart/"));
        assert!(!root.matches(""));
        assert!(!root.matches("*"));
        assert!(!root.matches("shop"));
    }

    #[test]
    fn exact_and_prefix_patterns_on_one_path_differ() {
        assert_ne!(exact("/shop"), prefix("/shop"));
    }

    #[test]
    fn paths_that_are_not_ascii_are_compared_without_panicking() {
        assert!(prefix("/caf").matches("/caf/é"));
        assert!(!prefix("/caf").matches("/café"));
        assert!(!exact("/café").matches("/cafe"));
    }

    #[test]
    fn every_spelling_of_a_pattern_is_the_same_pattern() {
        for spelling in ["/café", "/caf%c3%a9", "/caf%C3%A9", "/c%61f%C3%A9"] {
            assert_eq!(exact(spelling), exact("/caf%C3%A9"), "{spelling:?}");
            assert_eq!(prefix(spelling), prefix("/caf%C3%A9"), "{spelling:?}");
        }
        assert_eq!(prefix("/%61dmin/"), prefix("/admin"));
    }

    #[test]
    fn patterns_match_the_normal_form_of_request_paths() {
        // The request path arrives normalised; however the pattern was spelt, it matches.
        assert!(exact("/café").matches("/caf%C3%A9"));
        assert!(prefix("/my files").matches("/my%20files/report.pdf"));
        assert!(prefix("/%61dmin").matches("/admin/users"));
        // A request path that skipped normalisation is not what patterns are written for.
        assert!(!exact("/café").matches("/café"));
    }

    #[test]
    fn patterns_no_request_path_could_be_normalised_to_are_rejected() {
        use crate::NormaliseError;
        let cases = [
            ("/a%2Fb", NormaliseError::EncodedSeparator),
            ("/a\\b", NormaliseError::Backslash),
            ("/a%zz", NormaliseError::InvalidPercentEncoding),
            ("/a%00", NormaliseError::ControlCharacter),
            ("/a/%2e%2e/b", NormaliseError::AmbiguousDotSegment),
            ("/a/..;x/b", NormaliseError::AmbiguousDotSegment),
        ];
        for (path, reason) in cases {
            let reason = Err(PathPatternError::Ambiguous(reason));
            assert_eq!(PathPattern::exact(path), reason, "exact {path:?}");
            assert_eq!(PathPattern::prefix(path), reason, "prefix {path:?}");
        }
    }

    #[test]
    fn patterns_that_no_normalised_path_could_match_are_rejected() {
        let cases = [
            ("", PathPatternError::NotAbsolute),
            ("shop", PathPatternError::NotAbsolute),
            ("*", PathPatternError::NotAbsolute),
            ("//", PathPatternError::EmptySegment),
            ("//shop", PathPatternError::EmptySegment),
            ("/shop//cart", PathPatternError::EmptySegment),
            ("/shop//", PathPatternError::EmptySegment),
            ("/.", PathPatternError::DotSegment),
            ("/..", PathPatternError::DotSegment),
            ("/shop/./cart", PathPatternError::DotSegment),
            ("/shop/../cart", PathPatternError::DotSegment),
            ("/shop/..", PathPatternError::DotSegment),
            ("/shop/../", PathPatternError::DotSegment),
        ];
        for (path, reason) in cases {
            assert_eq!(PathPattern::exact(path), Err(reason), "exact {path:?}");
            assert_eq!(PathPattern::prefix(path), Err(reason), "prefix {path:?}");
        }
    }

    #[test]
    fn dots_inside_a_segment_are_ordinary_characters() {
        assert!(exact("/.well-known/acme").matches("/.well-known/acme"));
        assert!(prefix("/v1.2/...").matches("/v1.2/.../x"));
    }

    #[test]
    fn errors_describe_themselves() {
        assert_eq!(
            PathPatternError::NotAbsolute.to_string(),
            "path does not start with `/`"
        );
    }

    /// The obvious way to match a prefix: split both into segments and compare them one by
    /// one, as the Gateway API text describes it.
    fn reference_matches(pattern: &str, is_prefix: bool, path: &str) -> bool {
        if !is_prefix {
            return pattern == path;
        }
        let mut wanted: Vec<&str> = pattern.split('/').collect();
        if wanted.last() == Some(&"") {
            wanted.pop();
        }
        let given: Vec<&str> = path.split('/').collect();
        path.starts_with('/')
            && given.len() >= wanted.len()
            && wanted.iter().zip(&given).all(|(a, b)| a == b)
    }

    fn case() -> impl Strategy<Value = (String, bool, String)> {
        (path_pattern_text(), any::<bool>()).prop_flat_map(|(pattern, is_prefix)| {
            let path = path_near(&pattern);
            (Just(pattern), Just(is_prefix), path)
        })
    }

    proptest! {
        #[test]
        fn pattern_made_from_any_path_matches_the_normal_form_of_that_path(
            path in nasty_path()
        ) {
            if let (Ok(normal), Ok(exact), Ok(prefix)) = (
                normalise_path(&path),
                PathPattern::exact(&path),
                PathPattern::prefix(&path),
            ) {
                prop_assert!(exact.matches(&normal), "exact {normal:?}");
                prop_assert!(prefix.matches(&normal), "prefix {normal:?}");
            }
        }

        #[test]
        fn matching_agrees_with_the_segment_by_segment_reference(
            (text, is_prefix, path) in case()
        ) {
            let pattern = if is_prefix { prefix(&text) } else { exact(&text) };
            prop_assert_eq!(
                pattern.matches(&path),
                reference_matches(&text, is_prefix, &path)
            );
        }
    }
}
