//! Hostname patterns: the hostnames a listener or route claims, and whether a request's
//! host falls under them.
//!
//! Gateway API and Ingress write wildcards the same way (`*.example.com`) but mean
//! different things: a Gateway API wildcard covers any number of leading labels, an
//! Ingress wildcard exactly one. Both are explicit kinds here ([`WildcardLabels`]), chosen
//! by whoever translates the source object.

/// Longest hostname DNS allows, in bytes.
pub(crate) const MAX_NAME_LEN: usize = 253;
/// Longest single label DNS allows, in bytes.
const MAX_LABEL_LEN: usize = 63;

/// How many leading labels the `*` of a wildcard pattern stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WildcardLabels {
    /// Exactly one label, as in Ingress: `*.example.com` covers `a.example.com` but not
    /// `a.b.example.com`.
    One,
    /// One or more labels, as in Gateway API: `*.example.com` covers both
    /// `a.example.com` and `a.b.example.com`.
    OneOrMore,
}

/// A validated hostname pattern: an exact name, or a wildcard over leading labels.
///
/// Patterns are normalised to lower case, so two patterns that differ only in case are
/// equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostPattern {
    pub(crate) kind: Kind,
    /// Lower case. For a wildcard this is the part after the `*`, leading dot included
    /// (`.example.com`), so a suffix comparison also checks the label boundary.
    pub(crate) name: Box<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Exact,
    Wildcard(WildcardLabels),
}

impl HostPattern {
    /// Parses and validates a pattern.
    ///
    /// `wildcard` says what a leading `*` label means if the pattern has one; it is
    /// ignored for exact names. Names must be DNS names as Kubernetes accepts them: labels
    /// of 1–63 letters, digits or hyphens, not starting or ending with a hyphen, at most
    /// 253 bytes in all. A `*` is only allowed as the whole first label, followed by at
    /// least one more label.
    ///
    /// # Errors
    ///
    /// Returns a [`HostPatternError`] describing the first rule the pattern breaks.
    pub fn parse(pattern: &str, wildcard: WildcardLabels) -> Result<Self, HostPatternError> {
        if pattern.is_empty() {
            return Err(HostPatternError::Empty);
        }
        if pattern.len() > MAX_NAME_LEN {
            return Err(HostPatternError::TooLong);
        }
        let (kind, name) = match pattern.strip_prefix('*') {
            Some(suffix) => {
                let rest = suffix
                    .strip_prefix('.')
                    .ok_or(HostPatternError::MisplacedWildcard)?;
                validate_name(rest)?;
                (Kind::Wildcard(wildcard), suffix)
            }
            None => {
                validate_name(pattern)?;
                (Kind::Exact, pattern)
            }
        };
        Ok(Self {
            kind,
            name: name.to_ascii_lowercase().into_boxed_str(),
        })
    }

    /// Whether `host` falls under this pattern.
    ///
    /// `host` is the bare hostname of the request: no port and no trailing dot — the
    /// caller strips those once per request. The comparison is ASCII case-insensitive and
    /// purely textual: `host` is not validated, and anything that is not a hostname simply
    /// matches by the same byte rules — except that a host longer than any DNS name (253
    /// bytes) matches nothing. Never allocates.
    #[must_use]
    pub fn matches(&self, host: &str) -> bool {
        if host.len() > MAX_NAME_LEN {
            return false;
        }
        let host = host.as_bytes();
        let name = self.name.as_bytes();
        match self.kind {
            Kind::Exact => host.eq_ignore_ascii_case(name),
            Kind::Wildcard(labels) => {
                // The `*` stands for at least one byte, so the host must be strictly
                // longer than the suffix.
                let Some(split) = host.len().checked_sub(name.len()).filter(|&n| n > 0) else {
                    return false;
                };
                let (leading, suffix) = host.split_at(split);
                suffix.eq_ignore_ascii_case(name)
                    && match labels {
                        WildcardLabels::One => !leading.contains(&b'.'),
                        WildcardLabels::OneOrMore => true,
                    }
            }
        }
    }
}

/// Checks that `name` is a dot-separated sequence of valid DNS labels.
fn validate_name(name: &str) -> Result<(), HostPatternError> {
    for label in name.split('.') {
        if label.is_empty() {
            return Err(HostPatternError::EmptyLabel);
        }
        if label.len() > MAX_LABEL_LEN {
            return Err(HostPatternError::LabelTooLong);
        }
        for character in label.chars() {
            match character {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' => {}
                '*' => return Err(HostPatternError::MisplacedWildcard),
                other => return Err(HostPatternError::InvalidCharacter(other)),
            }
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err(HostPatternError::HyphenAtLabelEdge);
        }
    }
    Ok(())
}

/// Why a hostname pattern was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostPatternError {
    /// The pattern is the empty string.
    #[error("hostname is empty")]
    Empty,
    /// The pattern is longer than 253 bytes.
    #[error("hostname is longer than {} bytes", MAX_NAME_LEN)]
    TooLong,
    /// A label is empty: a leading, trailing or doubled dot.
    #[error("hostname has an empty label")]
    EmptyLabel,
    /// A label is longer than 63 bytes.
    #[error("hostname label is longer than {} bytes", MAX_LABEL_LEN)]
    LabelTooLong,
    /// A character other than a letter, digit or hyphen.
    #[error("hostname contains invalid character {0:?}")]
    InvalidCharacter(char),
    /// A label starts or ends with a hyphen.
    #[error("hostname label starts or ends with a hyphen")]
    HyphenAtLabelEdge,
    /// A `*` that is not the whole first label.
    #[error("wildcard `*` is only allowed as the whole first label")]
    MisplacedWildcard,
}

#[cfg(test)]
mod tests {
    use super::WildcardLabels::{One, OneOrMore};
    use super::*;
    use crate::reference;
    use crate::strategies::{host_near, pattern_text, wildcard_labels};
    use proptest::prelude::*;

    fn pattern(text: &str, wildcard: WildcardLabels) -> HostPattern {
        HostPattern::parse(text, wildcard).unwrap()
    }

    #[test]
    fn exact_name_matches_only_itself_ignoring_case() {
        let exact = pattern("example.com", OneOrMore);
        assert!(exact.matches("example.com"));
        assert!(exact.matches("EXAMPLE.Com"));
        assert!(!exact.matches("www.example.com"));
        assert!(!exact.matches("com"));
        assert!(!exact.matches("example.comm"));
        assert!(!exact.matches(""));
    }

    #[test]
    fn patterns_are_normalised_to_lower_case() {
        assert_eq!(pattern("EXAMPLE.com", One), pattern("example.COM", One));
        assert_eq!(pattern("*.EXAMPLE.com", One), pattern("*.example.com", One));
        assert!(pattern("*.EXAMPLE.com", One).matches("a.example.com"));
    }

    #[test]
    fn wildcard_kind_is_ignored_for_exact_names() {
        assert_eq!(
            pattern("example.com", One),
            pattern("example.com", OneOrMore)
        );
    }

    #[test]
    fn gateway_api_wildcard_covers_one_or_more_labels() {
        let wildcard = pattern("*.example.com", OneOrMore);
        assert!(wildcard.matches("a.example.com"));
        assert!(wildcard.matches("a.b.example.com"));
        assert!(wildcard.matches("A.B.Example.COM"));
        assert!(!wildcard.matches("example.com"));
        assert!(!wildcard.matches(".example.com"));
        assert!(!wildcard.matches("a.example.org"));
    }

    #[test]
    fn ingress_wildcard_covers_exactly_one_label() {
        let wildcard = pattern("*.example.com", One);
        assert!(wildcard.matches("a.example.com"));
        assert!(!wildcard.matches("a.b.example.com"));
        assert!(!wildcard.matches("example.com"));
        assert!(!wildcard.matches(".example.com"));
    }

    #[test]
    fn wildcard_respects_the_label_boundary() {
        for kind in [One, OneOrMore] {
            let wildcard = pattern("*.example.com", kind);
            assert!(!wildcard.matches("badexample.com"));
            assert!(!wildcard.matches("a.badexample.com"));
        }
    }

    #[test]
    fn hosts_that_are_not_ascii_are_compared_without_panicking() {
        assert!(!pattern("example.com", One).matches("exämple.com"));
        assert!(!pattern("*.example.com", One).matches("ä.example.org"));
        // Textual matching: the wildcard part is not validated.
        assert!(pattern("*.example.com", One).matches("ä.example.com"));
    }

    #[test]
    fn hosts_longer_than_any_dns_name_match_nothing() {
        let wildcard = pattern("*.example.com", OneOrMore);
        let longest = format!("{}.example.com", "a".repeat(253 - ".example.com".len()));
        assert_eq!(longest.len(), 253);
        assert!(wildcard.matches(&longest));
        assert!(!wildcard.matches(&format!("a{longest}")));
    }

    #[test]
    fn longest_valid_names_are_accepted() {
        let label = "a".repeat(63);
        let name = format!("{label}.{label}.{label}.{}", "a".repeat(61));
        assert_eq!(name.len(), 253);
        assert!(pattern(&name, One).matches(&name));
    }

    #[test]
    fn invalid_patterns_are_rejected_with_the_reason() {
        let long_label = format!("{}.com", "a".repeat(64));
        let label = "a".repeat(63);
        let long_name = format!("{label}.{label}.{label}.{}", "a".repeat(62));
        let cases = [
            ("", HostPatternError::Empty),
            (long_name.as_str(), HostPatternError::TooLong),
            ("a..com", HostPatternError::EmptyLabel),
            (".example.com", HostPatternError::EmptyLabel),
            ("example.com.", HostPatternError::EmptyLabel),
            ("*.", HostPatternError::EmptyLabel),
            (long_label.as_str(), HostPatternError::LabelTooLong),
            ("a_b.com", HostPatternError::InvalidCharacter('_')),
            ("exa mple.com", HostPatternError::InvalidCharacter(' ')),
            ("example.com:80", HostPatternError::InvalidCharacter(':')),
            ("exämple.com", HostPatternError::InvalidCharacter('ä')),
            ("-a.com", HostPatternError::HyphenAtLabelEdge),
            ("a-.com", HostPatternError::HyphenAtLabelEdge),
            ("*", HostPatternError::MisplacedWildcard),
            ("*example.com", HostPatternError::MisplacedWildcard),
            ("a.*.com", HostPatternError::MisplacedWildcard),
            ("*.*.com", HostPatternError::MisplacedWildcard),
            ("a*.com", HostPatternError::MisplacedWildcard),
        ];
        for (text, reason) in cases {
            for kind in [One, OneOrMore] {
                assert_eq!(HostPattern::parse(text, kind), Err(reason), "{text:?}");
            }
        }
    }

    #[test]
    fn errors_describe_themselves() {
        assert_eq!(
            HostPatternError::InvalidCharacter('_').to_string(),
            "hostname contains invalid character '_'"
        );
        assert_eq!(
            HostPatternError::TooLong.to_string(),
            "hostname is longer than 253 bytes"
        );
    }

    fn case() -> impl Strategy<Value = (String, WildcardLabels, String)> {
        (pattern_text(), wildcard_labels()).prop_flat_map(|(pattern, wildcard)| {
            let host = host_near(&pattern);
            (Just(pattern), Just(wildcard), host)
        })
    }

    proptest! {
        #[test]
        fn matching_agrees_with_the_label_by_label_reference(
            (text, wildcard, host) in case()
        ) {
            let compiled = pattern(&text, wildcard);
            prop_assert_eq!(
                compiled.matches(&host),
                reference::host_matches(&text, wildcard, &host)
            );
        }
    }
}
