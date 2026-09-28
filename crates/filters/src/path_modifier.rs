//! Path changes: Gateway API's `ReplaceFullPath` and `ReplacePrefixMatch`, for redirects
//! and rewrites alike.
//!
//! A change works on the normalised path a request was routed on, and what it makes is in
//! normal form too, without being checked per request: a replacement is held to what a
//! path pattern is held to when it is built, and a prefix is replaced by whole segments.
//! Byte counting, as Envoy's `prefix_rewrite` and NGINX's `proxy_pass` do it, is what
//! makes `//x` and `/xyzbar`.

use edgerush_router::{PathPattern, PathPatternError};

/// The longest replacement, in bytes, as Gateway API bounds it.
pub const MOST_BYTES: usize = 1024;

/// A change to a path, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathModifier {
    /// The whole path becomes this.
    Full(Box<str>),
    /// The matched prefix is replaced by whole segments.
    Prefix {
        /// The prefix the rule matches, as the router holds it: canonical, without its
        /// trailing slash, and empty for the root.
        matched: Box<str>,
        /// What takes its place, canonical, without its trailing slash; empty for none.
        replacement: Box<str>,
    },
}

impl PathModifier {
    /// A change of the whole path to `replacement`.
    ///
    /// # Errors
    ///
    /// Returns a [`PathModifierError`] for a replacement that is not a path a request could
    /// have in normal form, holds a `?` or `#`, or is longer than [`MOST_BYTES`].
    pub fn full(replacement: &str) -> Result<Self, PathModifierError> {
        Ok(Self::Full(canonical(replacement)?.into()))
    }

    /// A change of the prefix `matched` — the rule's one match — to `replacement`, which
    /// may be empty.
    ///
    /// # Errors
    ///
    /// Returns a [`PathModifierError`] if `matched` is not a prefix pattern, or for a
    /// replacement as [`PathModifier::full`] does, the empty one aside.
    pub fn prefix(matched: &PathPattern, replacement: &str) -> Result<Self, PathModifierError> {
        if !matched.is_prefix() {
            return Err(PathModifierError::NotPrefix);
        }
        let replacement = if replacement.is_empty() {
            String::new()
        } else {
            canonical(replacement)?
        };
        let replacement = replacement.strip_suffix('/').unwrap_or(&replacement);
        Ok(Self::Prefix {
            matched: matched.as_str().into(),
            replacement: replacement.into(),
        })
    }

    /// Writes the changed `path` onto the end of `out`. `path` is the normalised path of a
    /// request the rule matched, so it lies under a prefix change's prefix.
    pub fn write(&self, path: &str, out: &mut String) {
        match self {
            Self::Full(replacement) => out.push_str(replacement),
            Self::Prefix {
                matched,
                replacement,
            } => {
                // What comes after a whole-segment prefix is nothing or starts with `/`.
                let rest = path.get(matched.len()..).unwrap_or_default();
                // The root has no form without its slash: under the root prefix, `/` is
                // the prefix itself, as `/foo` is under `/foo` (Gateway API #3592).
                let rest = if matched.is_empty() && rest == "/" {
                    ""
                } else {
                    rest
                };
                let start = out.len();
                out.push_str(replacement);
                out.push_str(rest);
                if out.len() == start {
                    out.push('/');
                }
            }
        }
    }
}

/// A replacement in normal form, or why it cannot be one.
fn canonical(replacement: &str) -> Result<String, PathModifierError> {
    if replacement.len() > MOST_BYTES {
        return Err(PathModifierError::TooLong);
    }
    // The normaliser would encode them, and a path that says `?` means something else.
    if replacement.contains(['?', '#']) {
        return Err(PathModifierError::QueryOrFragment);
    }
    Ok(PathPattern::exact(replacement)?.as_str().to_owned())
}

/// Why a path change was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathModifierError {
    /// The replacement is not a path in normal form could be.
    #[error("replacement: {0}")]
    Path(#[from] PathPatternError),
    /// The replacement holds a query or a fragment.
    #[error("replacement holds a `?` or `#`: a path cannot")]
    QueryOrFragment,
    /// The replacement is longer than Gateway API allows.
    #[error("replacement is longer than 1024 bytes")]
    TooLong,
    /// A prefix change for a match that is not a prefix.
    #[error("a prefix is replaced only under a prefix match")]
    NotPrefix,
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_router::normalise_path;
    use proptest::prelude::*;

    fn written(modifier: &PathModifier, path: &str) -> String {
        let mut out = String::new();
        modifier.write(path, &mut out);
        out
    }

    fn prefix(matched: &str, replacement: &str) -> PathModifier {
        PathModifier::prefix(&PathPattern::prefix(matched).unwrap(), replacement).unwrap()
    }

    /// Gateway API's table for `ReplacePrefixMatch`, row by row (`httproute_types.go`).
    #[test]
    fn a_prefix_is_replaced_as_gateway_apis_table_says() {
        let table = [
            ("/foo/bar", "/foo", "/xyz", "/xyz/bar"),
            ("/foo/bar", "/foo", "/xyz/", "/xyz/bar"),
            ("/foo/bar", "/foo/", "/xyz", "/xyz/bar"),
            ("/foo/bar", "/foo/", "/xyz/", "/xyz/bar"),
            ("/foo", "/foo", "/xyz", "/xyz"),
            ("/foo/", "/foo", "/xyz", "/xyz/"),
            ("/foo/bar", "/foo", "", "/bar"),
            ("/foo/", "/foo", "", "/"),
            ("/foo", "/foo", "", "/"),
            ("/foo/", "/foo", "/", "/"),
            ("/foo", "/foo", "/", "/"),
        ];
        for (path, matched, replacement, expected) in table {
            let got = written(&prefix(matched, replacement), path);
            assert_eq!(got, expected, "{path} under {matched} to {replacement:?}");
        }
    }

    /// Under the root prefix: Gateway API issue #3592's proposal, which 18 §4 adopts.
    #[test]
    fn under_the_root_the_root_path_is_the_prefix_itself() {
        let table = [
            ("/bar", "/xyz", "/xyz/bar"),
            ("/bar", "/xyz/", "/xyz/bar"),
            ("/bar", "", "/bar"),
            ("/bar/", "/xyz", "/xyz/bar/"),
            ("/", "/xyz", "/xyz"),
            ("/", "/xyz/", "/xyz"),
            ("/", "", "/"),
            ("/", "/", "/"),
        ];
        for (path, replacement, expected) in table {
            let got = written(&prefix("/", replacement), path);
            assert_eq!(got, expected, "{path} under / to {replacement:?}");
        }
    }

    /// Gateway API's conformance cases (`httproute-rewrite-path`, `httproute-redirect-path`).
    #[test]
    fn gateway_apis_conformance_cases_come_out_as_expected() {
        let one = prefix("/prefix/one", "/one");
        assert_eq!(written(&one, "/prefix/one/two"), "/one/two");
        let strip = prefix("/strip-prefix", "/");
        assert_eq!(written(&strip, "/strip-prefix/three"), "/three");
        assert_eq!(written(&strip, "/strip-prefix"), "/");
        let redirect = prefix("/original-prefix", "/replacement-prefix");
        assert_eq!(
            written(&redirect, "/original-prefix/lemon"),
            "/replacement-prefix/lemon"
        );
        let full = PathModifier::full("/full-path-replacement").unwrap();
        assert_eq!(
            written(&full, "/full/path/original"),
            "/full-path-replacement"
        );
    }

    #[test]
    fn what_is_written_goes_after_what_was_there() {
        let mut out = String::from("https://example.org");
        prefix("/a", "/b").write("/a/c", &mut out);
        assert_eq!(out, "https://example.org/b/c");
        let mut out = String::from("https://example.org");
        prefix("/a", "").write("/a", &mut out);
        assert_eq!(out, "https://example.org/");
    }

    #[test]
    fn a_replacement_is_made_canonical_as_a_pattern_is() {
        assert_eq!(
            PathModifier::full("/caf%c3%a9").unwrap(),
            PathModifier::Full("/caf%C3%A9".into())
        );
        assert_eq!(
            PathModifier::full("/%61dmin").unwrap(),
            PathModifier::Full("/admin".into())
        );
    }

    #[test]
    fn a_replacement_that_is_no_normal_path_is_refused() {
        use PathModifierError::{Path, QueryOrFragment, TooLong};
        let refused = |replacement: &str| PathModifier::full(replacement).unwrap_err();
        assert_eq!(refused(""), Path(PathPatternError::NotAbsolute));
        assert_eq!(refused("new"), Path(PathPatternError::NotAbsolute));
        assert_eq!(
            refused("//evil.example/x"),
            Path(PathPatternError::EmptySegment)
        );
        assert_eq!(refused("/a/../b"), Path(PathPatternError::DotSegment));
        assert!(matches!(
            refused("/a%2Fb"),
            Path(PathPatternError::Ambiguous(_))
        ));
        assert!(matches!(
            refused("/a\\b"),
            Path(PathPatternError::Ambiguous(_))
        ));
        assert_eq!(refused("/a?b=1"), QueryOrFragment);
        assert_eq!(refused("/a#top"), QueryOrFragment);
        assert!(PathModifier::full(&format!("/{}", "a".repeat(MOST_BYTES - 1))).is_ok());
        assert_eq!(refused(&format!("/{}", "a".repeat(MOST_BYTES))), TooLong);

        let root = PathPattern::prefix("/").unwrap();
        assert!(PathModifier::prefix(&root, "").is_ok());
        assert_eq!(
            PathModifier::prefix(&root, "x").unwrap_err(),
            Path(PathPatternError::NotAbsolute)
        );
        let exact = PathPattern::exact("/a").unwrap();
        assert_eq!(
            PathModifier::prefix(&exact, "/b").unwrap_err(),
            PathModifierError::NotPrefix
        );
    }

    fn segment() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop_oneof![
                Just("a".to_owned()),
                Just("b".to_owned()),
                Just("%C3%A9".to_owned()),
                Just("~".to_owned()),
                Just(":".to_owned()),
            ],
            1..4,
        )
        .prop_map(|parts| parts.concat())
    }

    /// A path in normal form: segments, perhaps a trailing slash.
    fn normal_path() -> impl Strategy<Value = String> {
        (prop::collection::vec(segment(), 0..4), any::<bool>()).prop_map(|(segments, slash)| {
            let mut path = format!("/{}", segments.join("/"));
            if slash && !segments.is_empty() {
                path.push('/');
            }
            path
        })
    }

    proptest! {
        /// Whatever a change makes of a normal path under its prefix is itself in normal
        /// form: nothing needs checking per request.
        #[test]
        fn a_change_of_a_normal_path_is_normal(
            matched in normal_path(),
            rest in normal_path(),
            replacement in prop_oneof![Just(String::new()), normal_path()],
            root_rest in any::<bool>(),
        ) {
            let pattern = PathPattern::prefix(&matched).unwrap();
            let under = pattern.as_str();
            // A path the prefix covers: the prefix itself, or it and more segments.
            let path = if root_rest || rest == "/" {
                if under.is_empty() { "/".to_owned() } else { under.to_owned() }
            } else {
                format!("{under}{rest}")
            };
            prop_assume!(pattern.matches(&path));
            let modifier = PathModifier::prefix(&pattern, &replacement).unwrap();
            let changed = written(&modifier, &path);
            let normal = normalise_path(&changed);
            prop_assert_eq!(normal.as_deref(), Ok(changed.as_str()));
            // The rest of the path survives, segment for segment.
            let kept = path.strip_prefix(under).unwrap_or(&path);
            if kept.len() > 1 {
                prop_assert!(changed.ends_with(kept), "{} lost {}", changed, kept);
            }
        }
    }
}
