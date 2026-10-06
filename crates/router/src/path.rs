//! Path patterns: exact paths, segment-wise prefixes and regular expressions, the three
//! kinds of Gateway API (Ingress has the first two); and a gRPC method in any service,
//! which GRPCRoute ranks where no path pattern of those three kinds would.
//!
//! Patterns are compared case-sensitively with the *normalised* request path
//! ([`normalise_path`]). Normalising the request path is not done here: the caller does it
//! once per request. Exact and prefix patterns get the same canonical percent-encoding when
//! they are built, so `/café`, `/caf%c3%a9` and `/caf%C3%A9` are one pattern; but a pattern
//! with `//` or dot segments, which says something other than what it would match, is
//! rejected rather than quietly rewritten.
//!
//! Regular expressions are RE2-style, held to the whole path and matched in linear time,
//! whatever the pattern and the path ([`RegexError`] says what is refused).

use crate::whole_regex::WholeRegex;
use crate::{NormaliseError, RegexError, normalise_path};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;

/// A validated path pattern.
#[derive(Debug, Clone)]
pub struct PathPattern {
    pub(crate) kind: Kind,
    /// The pattern text. For a prefix, without its trailing slash (the root prefix is the
    /// empty string), so that `/shop` and `/shop/` are one pattern and the boundary check
    /// is uniform. For a regex, as the user wrote it. For a gRPC method, its name.
    pub(crate) path: Box<str>,
}

#[derive(Debug, Clone)]
pub(crate) enum Kind {
    Exact,
    Prefix,
    /// Compiled to match the whole path.
    Regex(WholeRegex),
    /// A path of two segments, the second this name.
    GrpcMethod,
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

    /// The root prefix, which covers every path: what a match on hostnames alone has, as a
    /// `tls` listener's route is for `explain`.
    #[must_use]
    pub fn every() -> Self {
        Self {
            kind: Kind::Prefix,
            path: "".into(),
        }
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
        Ok(Self {
            kind: Kind::Regex(WholeRegex::new(pattern)?),
            path: pattern.into(),
        })
    }

    /// A pattern that matches a call of the gRPC method `name` in any service: a path of
    /// two segments, `/{service}/{name}`, whatever the first. It ranks after every prefix
    /// but the root (03 §4), as GRPCRoute puts a method alone after a service.
    ///
    /// # Errors
    ///
    /// Returns a [`PathPatternError`] if `name` is not one path segment, or is a segment
    /// that requests are rejected for.
    pub fn grpc_method(name: &str) -> Result<Self, PathPatternError> {
        if name.is_empty() || name.contains('/') {
            return Err(PathPatternError::NotOneSegment);
        }
        let path = format!("/{name}");
        let path = canonical(&path)?;
        Ok(Self {
            kind: Kind::GrpcMethod,
            path: path.strip_prefix('/').unwrap_or(&path).into(),
        })
    }

    /// The pattern in its canonical form: an exact path as it is matched; a prefix
    /// without its trailing slash, so that the root prefix is the empty string; a regular
    /// expression as it was written; a gRPC method's name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.path
    }

    /// Whether this is a prefix pattern.
    #[must_use]
    pub fn is_prefix(&self) -> bool {
        matches!(self.kind, Kind::Prefix)
    }

    /// Whether this is a regular expression.
    #[must_use]
    pub fn is_regex(&self) -> bool {
        matches!(self.kind, Kind::Regex(_))
    }

    /// Whether this is a gRPC method in any service.
    #[must_use]
    pub fn is_grpc_method(&self) -> bool {
        matches!(self.kind, Kind::GrpcMethod)
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
            Kind::GrpcMethod => grpc_method_of(path) == Some(&*self.path),
        }
    }
}

/// The method a path calls, if it has the shape of a gRPC call: two segments, the first
/// not empty. The second may hold anything but a slash; only a pattern's name is compared
/// with it.
pub(crate) fn grpc_method_of(path: &str) -> Option<&str> {
    let (service, method) = path.strip_prefix('/')?.split_once('/')?;
    (!service.is_empty() && !method.contains('/')).then_some(method)
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
    /// A gRPC method's name is empty or holds a `/`.
    #[error("a gRPC method is not one path segment")]
    NotOneSegment,
    /// The path is one that a request would be rejected for, so nothing could match it.
    #[error(transparent)]
    Ambiguous(#[from] NormaliseError),
    /// The regular expression is outside the contract.
    #[error(transparent)]
    Regex(#[from] RegexError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference;
    use crate::strategies::{host_label, nasty_path, path_near, path_pattern_text, valid_label};
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

    fn grpc_method(name: &str) -> PathPattern {
        PathPattern::grpc_method(name).unwrap()
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
    fn every_path_is_the_root_prefix() {
        assert_eq!(PathPattern::every(), prefix("/"));
        assert!(PathPattern::every().matches("/"));
        assert!(PathPattern::every().matches("/a/b"));
        assert!(!PathPattern::every().matches("*"));
    }

    #[test]
    fn each_kind_says_what_it_is() {
        let kinds = |pattern: &PathPattern| {
            (
                pattern.is_prefix(),
                pattern.is_regex(),
                pattern.is_grpc_method(),
            )
        };
        assert_eq!(kinds(&prefix("/shop")), (true, false, false));
        assert_eq!(kinds(&regex("/shop")), (false, true, false));
        assert_eq!(kinds(&grpc_method("shop")), (false, false, true));
        assert_eq!(kinds(&exact("/shop")), (false, false, false));
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
    fn regex_pattern_sees_the_normalised_path() {
        assert!(regex(r"/caf%C3%A9/\d+").matches("/caf%C3%A9/7"));
        assert!(regex("/[^/]+/menu").matches("/caf%C3%A9/menu"));
        assert!(!regex("/[^/]+/menu").matches("/a/b/menu"));
    }

    #[test]
    fn grpc_method_pattern_matches_that_method_in_any_service() {
        let method = grpc_method("Do");
        assert!(method.matches("/pkg.Svc/Do"));
        assert!(method.matches("/other.Svc/Do"));
        assert!(method.matches("/x/Do"));
        assert!(!method.matches("/pkg.Svc/Done"));
        assert!(!method.matches("/pkg.Svc/do"));
        assert!(!method.matches("/pkg.Svc/Do/"));
        assert!(!method.matches("/pkg.Svc/Do/x"));
        assert!(!method.matches("/a/b/Do"));
        assert!(!method.matches("/Do"));
        assert!(!method.matches("//Do"));
        assert!(!method.matches("Do"));
        assert!(!method.matches(""));
    }

    #[test]
    fn grpc_method_pattern_is_one_segment_in_canonical_form() {
        assert_eq!(grpc_method("caf%c3%a9"), grpc_method("café"));
        assert!(grpc_method("café").matches("/s/caf%C3%A9"));
        assert_eq!(grpc_method("Do").as_str(), "Do");
        assert_ne!(grpc_method("Do"), exact("/Do"));
        assert_ne!(grpc_method("Do"), prefix("/Do"));
        for (name, reason) in [
            ("", PathPatternError::NotOneSegment),
            ("a/b", PathPatternError::NotOneSegment),
            ("Do/", PathPatternError::NotOneSegment),
            ("/Do", PathPatternError::NotOneSegment),
            (".", PathPatternError::DotSegment),
            ("..", PathPatternError::DotSegment),
            (
                "a%2Fb",
                PathPatternError::Ambiguous(crate::NormaliseError::EncodedSeparator),
            ),
        ] {
            assert_eq!(PathPattern::grpc_method(name), Err(reason), "{name:?}");
        }
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
    fn regex_patterns_outside_the_contract_are_rejected_with_the_reason() {
        assert_eq!(
            PathPattern::regex("/café"),
            Err(PathPatternError::Regex(RegexError::NotAscii))
        );
        assert!(matches!(
            PathPattern::regex("/a)|(/b"),
            Err(PathPatternError::Regex(RegexError::Syntax(_)))
        ));
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

    /// A method name and a path near a call of it, in a service that may be empty.
    fn grpc_case() -> impl Strategy<Value = (String, String)> {
        (valid_label(), host_label()).prop_flat_map(|(name, service)| {
            let path = path_near(&format!("/{service}/{name}"));
            (Just(name), path)
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
        fn grpc_method_matches_as_its_regex_and_the_reference_do(
            (name, path) in grpc_case()
        ) {
            let pattern = grpc_method(&name);
            let as_regex = regex(&format!("/[^/]+/{}", ::regex::escape(pattern.as_str())));
            prop_assert_eq!(pattern.matches(&path), as_regex.matches(&path));
            prop_assert_eq!(
                pattern.matches(&path),
                reference::grpc_method_matches(pattern.as_str(), &path)
            );
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
