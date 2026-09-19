//! Path patterns: exact paths, segment-wise prefixes and regular expressions, the three
//! kinds of Gateway API (Ingress has the first two).
//!
//! Patterns are compared case-sensitively with the *normalised* request path
//! ([`normalise_path`]). Normalising the request path is not done here: the caller does it
//! once per request. Exact and prefix patterns get the same canonical percent-encoding when
//! they are built, so `/café`, `/caf%c3%a9` and `/caf%C3%A9` are one pattern; but a pattern
//! with `//` or dot segments, which says something other than what it would match, is
//! rejected rather than quietly rewritten.
//!
//! Regular expressions are RE2-style and matched in linear time, whatever the pattern and
//! the path. That is the contract; the engine behind it is a detail of this module and
//! appears nowhere in the crate's interface.

use crate::{NormaliseError, normalise_path};
use regex::bytes::{Regex, RegexBuilder};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;

/// The most memory one compiled regex may take. Far more than any sane path pattern needs;
/// a pattern that repeats large groups a large number of times is refused.
const REGEX_SIZE_LIMIT: usize = 256 * 1024;
/// The most memory the engine may use to speed up matching one regex, per thread that
/// matches it. Going without only makes matching slower, never wrong.
const REGEX_CACHE_LIMIT: usize = 512 * 1024;

/// A validated path pattern.
#[derive(Debug, Clone)]
pub struct PathPattern {
    pub(crate) kind: Kind,
    /// The pattern text. For a prefix, without its trailing slash (the root prefix is the
    /// empty string), so that `/shop` and `/shop/` are one pattern and the boundary check
    /// is uniform. For a regex, as the user wrote it.
    pub(crate) path: Box<str>,
}

#[derive(Debug, Clone)]
pub(crate) enum Kind {
    Exact,
    Prefix,
    /// Compiled to match the whole path.
    Regex(Regex),
}

/// Patterns are what their kind and text say; a compiled regex adds nothing to that.
impl PartialEq for PathPattern {
    fn eq(&self, other: &Self) -> bool {
        discriminant(&self.kind) == discriminant(&other.kind) && self.path == other.path
    }
}

impl Eq for PathPattern {}

impl Hash for PathPattern {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(&self.kind).hash(state);
        self.path.hash(state);
    }
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

    /// A pattern that matches the paths a regular expression describes: the **whole** path,
    /// so `/users/\d+` matches `/users/42` but not `/users/42/edit`; whoever wants less
    /// writes `.*`. Case-sensitive unless the pattern says otherwise (`(?i)`).
    ///
    /// The syntax is RE2-style: no look-around and no backreferences, which is what makes
    /// linear-time matching possible. The regex sees the normalised path, in which anything
    /// but ASCII is percent-encoded, so the pattern has to be ASCII too.
    ///
    /// # Errors
    ///
    /// Returns a [`PathPatternError`] if `pattern` is not ASCII, not valid syntax, or
    /// compiles to something unreasonably large.
    pub fn regex(pattern: &str) -> Result<Self, PathPatternError> {
        if !pattern.is_ascii() {
            return Err(PathPatternError::RegexNotAscii);
        }
        // On its own first: a pattern such as `a)|(b` must not be able to close the group
        // that holds it to the whole path, and syntax errors should quote the user's text.
        compile(pattern)?;
        Ok(Self {
            kind: Kind::Regex(compile(&format!("^(?:{pattern})$"))?),
            path: pattern.into(),
        })
    }

    /// Whether the request path falls under this pattern.
    ///
    /// `path` is the normalised path alone, without the query string. Anything that does
    /// not start with `/` (such as the `*` of `OPTIONS *`) matches nothing. Never
    /// allocates.
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        match &self.kind {
            Kind::Exact => path == &*self.path,
            // What follows the prefix must be nothing or start a new segment. The check for
            // a leading slash only matters to the root prefix, stored as the empty string.
            Kind::Prefix => {
                path.starts_with('/')
                    && path
                        .strip_prefix(&*self.path)
                        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
            }
            Kind::Regex(regex) => path.starts_with('/') && regex.is_match(path.as_bytes()),
        }
    }
}

/// Compiles with the limits and the dialect of the contract. Unicode support is off, as in
/// RE2's defaults: `\d` and `\w` are the ASCII classes, and normalised paths are ASCII.
fn compile(pattern: &str) -> Result<Regex, PathPatternError> {
    RegexBuilder::new(pattern)
        .unicode(false)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(REGEX_CACHE_LIMIT)
        .build()
        .map_err(|error| match error {
            regex::Error::CompiledTooBig(_) => PathPatternError::RegexTooLarge,
            other => PathPatternError::RegexSyntax(other.to_string()),
        })
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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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
    /// The regular expression is not valid RE2-style syntax; the text says what is wrong.
    #[error("invalid regular expression: {0}")]
    RegexSyntax(String),
    /// The regular expression contains something other than ASCII, which no normalised
    /// path does.
    #[error("regular expression is not ASCII; write other characters percent-encoded")]
    RegexNotAscii,
    /// The regular expression compiles to more than the size allowed for one pattern.
    #[error("regular expression is too large once compiled")]
    RegexTooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference;
    use crate::strategies::{nasty_path, path_near, path_pattern_text};
    use proptest::prelude::*;

    fn exact(path: &str) -> PathPattern {
        PathPattern::exact(path).unwrap()
    }

    fn prefix(path: &str) -> PathPattern {
        PathPattern::prefix(path).unwrap()
    }

    fn regex(pattern: &str) -> PathPattern {
        PathPattern::regex(pattern).unwrap()
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
            assert_eq!(
                PathPattern::exact(path),
                Err(reason.clone()),
                "exact {path:?}"
            );
            assert_eq!(PathPattern::prefix(path), Err(reason), "prefix {path:?}");
        }
    }

    #[test]
    fn dots_inside_a_segment_are_ordinary_characters() {
        assert!(exact("/.well-known/acme").matches("/.well-known/acme"));
        assert!(prefix("/v1.2/...").matches("/v1.2/.../x"));
    }

    #[test]
    fn regex_pattern_must_match_the_whole_path() {
        let user = regex(r"/users/\d+");
        assert!(user.matches("/users/42"));
        assert!(!user.matches("/users/42/edit"));
        assert!(!user.matches("/x/users/42"));
        assert!(!user.matches("/users/"));
        // Whoever wants less says so.
        assert!(regex(r"/users/\d+(/.*)?").matches("/users/42/edit"));
        assert!(regex(r".*/users/\d+").matches("/x/users/42"));
    }

    #[test]
    fn anchors_the_user_writes_change_nothing() {
        let user = regex(r"^/users/\d+$");
        assert!(user.matches("/users/42"));
        assert!(!user.matches("/users/42/edit"));
        assert!(!user.matches("/x/users/42"));
    }

    #[test]
    fn every_branch_of_an_alternation_is_held_to_the_whole_path() {
        let either = regex("/a|/ab");
        assert!(either.matches("/a"));
        assert!(either.matches("/ab"));
        assert!(!either.matches("/abc"));
        assert!(!either.matches("/x/a"));
    }

    #[test]
    fn regex_pattern_is_case_sensitive_unless_it_says_otherwise() {
        assert!(regex("/shop").matches("/shop"));
        assert!(!regex("/shop").matches("/Shop"));
        assert!(regex("(?i)/shop").matches("/SHOP"));
    }

    #[test]
    fn regex_pattern_sees_the_normalised_path() {
        assert!(regex(r"/caf%C3%A9/\d+").matches("/caf%C3%A9/7"));
        assert!(regex("/[^/]+/menu").matches("/caf%C3%A9/menu"));
        assert!(!regex("/[^/]+/menu").matches("/a/b/menu"));
    }

    #[test]
    fn regex_pattern_matches_nothing_that_is_not_an_absolute_path() {
        let anything = regex(".*");
        assert!(anything.matches("/"));
        assert!(anything.matches("/shop"));
        assert!(!anything.matches(""));
        assert!(!anything.matches("*"));
        assert!(!anything.matches("shop"));
    }

    #[test]
    fn regex_patterns_are_compared_by_their_text() {
        assert_eq!(regex("/a+"), regex("/a+"));
        assert_ne!(regex("/a+"), regex("/a*"));
        assert_ne!(regex("/a"), exact("/a"));
        assert_ne!(regex("/a"), prefix("/a"));
    }

    #[test]
    fn regex_patterns_outside_the_contract_are_rejected() {
        // Broken syntax, an attempt to break out of the anchoring, and the two features
        // that cannot be matched in linear time: look-around and backreferences.
        for pattern in ["/a(", "/a)|(/b", "/a(?=b)", r"/(a)\1"] {
            assert!(
                matches!(
                    PathPattern::regex(pattern),
                    Err(PathPatternError::RegexSyntax(_))
                ),
                "{pattern:?}"
            );
        }
        assert_eq!(
            PathPattern::regex("/café"),
            Err(PathPatternError::RegexNotAscii)
        );
        assert_eq!(
            PathPattern::regex("(?:/[a-z]{1,500}){1,500}"),
            Err(PathPatternError::RegexTooLarge)
        );
    }

    #[test]
    fn hostile_pattern_and_path_are_matched_in_linear_time() {
        // Catastrophic for a backtracking engine; a test that finishes is the assertion.
        let pattern = regex("/(a+)+b");
        let path = format!("/{}", "a".repeat(100_000));
        assert!(!pattern.matches(&path));
    }

    #[test]
    fn errors_describe_themselves() {
        assert_eq!(
            PathPatternError::NotAbsolute.to_string(),
            "path does not start with `/`"
        );
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
        fn regex_of_a_literal_path_is_the_exact_pattern_and_with_a_tail_the_prefix(
            (text, _, path) in case()
        ) {
            // Whole-path matching, checked against the two kinds that define it by hand.
            let literal = ::regex::escape(text.strip_suffix('/').unwrap_or(&text));
            let as_exact = regex(&::regex::escape(&text));
            let as_prefix = regex(&format!("{literal}(?:/.*)?"));
            prop_assert_eq!(as_exact.matches(&path), exact(&text).matches(&path));
            prop_assert_eq!(as_prefix.matches(&path), prefix(&text).matches(&path));
        }

        #[test]
        fn matching_agrees_with_the_segment_by_segment_reference(
            (text, is_prefix, path) in case()
        ) {
            let pattern = if is_prefix { prefix(&text) } else { exact(&text) };
            prop_assert_eq!(
                pattern.matches(&path),
                reference::path_matches(&text, is_prefix, &path)
            );
        }
    }
}
