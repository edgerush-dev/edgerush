//! Header predicates: the `headers` of a Gateway API route match.
//!
//! A rule's header predicates must all hold. Names are case-insensitive, values are not.
//! When a rule names a header twice, only the first entry counts, as the spec requires. A
//! header that the request repeats is read as the RFC says such headers are to be read —
//! as one value, the fields joined by commas — so `x: a` plus `x: b` is `a,b`, which an
//! exact match on `a` does not accept.

use crate::RegexError;
use crate::whole_regex::WholeRegex;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// A validated condition on one request header.
#[derive(Debug, Clone)]
pub struct HeaderPredicate {
    name: HeaderName,
    value: ValueMatch,
}

#[derive(Debug, Clone)]
enum ValueMatch {
    Exact(HeaderValue),
    Regex(WholeRegex),
}

impl HeaderPredicate {
    /// The header must be present with exactly this value, byte for byte.
    ///
    /// # Errors
    ///
    /// Returns a [`HeaderPredicateError`] if `name` is not a header name, or `value` is
    /// not something a header could have as its value: empty, with white space at either
    /// end (which is not part of a value), or with control characters.
    pub fn exact(name: &str, value: &str) -> Result<Self, HeaderPredicateError> {
        let trimmed = value.trim_matches([' ', '\t']);
        let value = HeaderValue::from_str(value)
            .ok()
            .filter(|_| !value.is_empty() && trimmed.len() == value.len())
            .ok_or(HeaderPredicateError::InvalidValue)?;
        Ok(Self {
            name: parse_name(name)?,
            value: ValueMatch::Exact(value),
        })
    }

    /// The header must be present with a value that, as a whole, matches the regular
    /// expression: RE2-style syntax, linear-time matching.
    ///
    /// # Errors
    ///
    /// Returns a [`HeaderPredicateError`] if `name` is not a header name or `pattern` is
    /// outside the contract for regular expressions.
    pub fn regex(name: &str, pattern: &str) -> Result<Self, HeaderPredicateError> {
        Ok(Self {
            name: parse_name(name)?,
            value: ValueMatch::Regex(WholeRegex::new(pattern)?),
        })
    }

    /// Whether the request's headers satisfy this predicate. Allocates only to join the
    /// values of a repeated header for a regex.
    #[must_use]
    pub fn matches(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(&self.name).iter();
        let Some(first) = values.next() else {
            return false;
        };
        let mut rest = values.peekable();
        match &self.value {
            ValueMatch::Exact(expected) if rest.peek().is_none() => first == expected,
            ValueMatch::Regex(regex) if rest.peek().is_none() => regex.is_match(first.as_bytes()),
            // A repeated header. The expected value is compared piece by piece with what
            // joining would give, so the usual kind of match needs no copy.
            ValueMatch::Exact(expected) => {
                let mut expected = expected.as_bytes().strip_prefix(first.as_bytes());
                for value in rest {
                    expected = expected
                        .and_then(|expected| expected.strip_prefix(b","))
                        .and_then(|expected| expected.strip_prefix(value.as_bytes()));
                }
                expected.is_some_and(<[u8]>::is_empty)
            }
            ValueMatch::Regex(regex) => {
                let mut joined = first.as_bytes().to_vec();
                for value in rest {
                    joined.push(b',');
                    joined.extend_from_slice(value.as_bytes());
                }
                regex.is_match(&joined)
            }
        }
    }
}

fn parse_name(name: &str) -> Result<HeaderName, HeaderPredicateError> {
    HeaderName::from_bytes(name.as_bytes()).map_err(|_| HeaderPredicateError::InvalidName)
}

/// The header predicates of one rule: all of them must hold.
#[derive(Debug, Clone, Default)]
pub struct HeaderPredicates(Box<[HeaderPredicate]>);

impl HeaderPredicates {
    /// Keeps the first predicate for every header name and drops later ones, which the
    /// spec says must be ignored.
    pub fn new(predicates: impl IntoIterator<Item = HeaderPredicate>) -> Self {
        let mut kept: Vec<HeaderPredicate> = Vec::new();
        for predicate in predicates {
            if kept.iter().all(|earlier| earlier.name != predicate.name) {
                kept.push(predicate);
            }
        }
        Self(kept.into_boxed_slice())
    }

    /// How many predicates count. A rule with more of them takes precedence.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there is nothing to check, so every request passes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the request's headers satisfy every predicate.
    #[must_use]
    pub fn matches(&self, headers: &HeaderMap) -> bool {
        self.0.iter().all(|predicate| predicate.matches(headers))
    }
}

/// Why a header predicate was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HeaderPredicateError {
    /// Not a valid header name.
    #[error("invalid header name")]
    InvalidName,
    /// Not something a header could have as its value.
    #[error("invalid header value")]
    InvalidValue,
    /// The regular expression is outside the contract.
    #[error(transparent)]
    Regex(#[from] RegexError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference;
    use proptest::prelude::*;

    fn headers(fields: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    fn exact(name: &str, value: &str) -> HeaderPredicate {
        HeaderPredicate::exact(name, value).unwrap()
    }

    fn regex(name: &str, pattern: &str) -> HeaderPredicate {
        HeaderPredicate::regex(name, pattern).unwrap()
    }

    #[test]
    fn exact_predicate_wants_the_header_with_that_very_value() {
        let version = exact("x-version", "v2");
        assert!(version.matches(&headers(&[("x-version", "v2")])));
        assert!(version.matches(&headers(&[("accept", "*/*"), ("x-version", "v2")])));
        assert!(!version.matches(&headers(&[("x-version", "V2")])));
        assert!(!version.matches(&headers(&[("x-version", "v2.1")])));
        assert!(!version.matches(&headers(&[("x-other", "v2")])));
        assert!(!version.matches(&headers(&[])));
    }

    #[test]
    fn header_names_are_case_insensitive() {
        assert!(exact("X-Version", "v2").matches(&headers(&[("x-version", "v2")])));
        assert!(regex("X-VERSION", "v[0-9]").matches(&headers(&[("x-version", "v2")])));
    }

    #[test]
    fn regex_predicate_is_held_to_the_whole_value() {
        let version = regex("x-version", r"v\d+");
        assert!(version.matches(&headers(&[("x-version", "v12")])));
        assert!(!version.matches(&headers(&[("x-version", "v12-beta")])));
        assert!(!version.matches(&headers(&[("x-version", "xv12")])));
        assert!(!version.matches(&headers(&[])));
    }

    #[test]
    fn repeated_header_is_read_as_one_comma_joined_value() {
        let twice = headers(&[("x-tag", "a"), ("accept", "*/*"), ("x-tag", "b")]);
        assert!(!exact("x-tag", "a").matches(&twice));
        assert!(!exact("x-tag", "b").matches(&twice));
        assert!(exact("x-tag", "a,b").matches(&twice));
        assert!(!exact("x-tag", "a,b,c").matches(&twice));
        assert!(!exact("x-tag", "a,").matches(&twice));
        assert!(!exact("x-tag", "ab").matches(&twice));
        assert!(regex("x-tag", "a,b").matches(&twice));
        assert!(regex("x-tag", "(a|b)(,(a|b))*").matches(&twice));
        assert!(!regex("x-tag", "a").matches(&twice));
    }

    #[test]
    fn header_sent_once_with_commas_equals_the_same_header_repeated() {
        let once = headers(&[("x-tag", "a,b")]);
        assert!(exact("x-tag", "a,b").matches(&once));
        assert!(!exact("x-tag", "a").matches(&once));
    }

    #[test]
    fn values_that_are_not_text_are_compared_as_bytes() {
        let mut odd = HeaderMap::new();
        odd.insert("x-odd", HeaderValue::from_bytes(b"caf\xe9").unwrap());
        assert!(!exact("x-odd", "cafe").matches(&odd));
        assert!(regex("x-odd", "caf.").matches(&odd));
    }

    #[test]
    fn all_predicates_of_a_rule_must_hold() {
        let both = HeaderPredicates::new([exact("x-a", "1"), regex("x-b", "[0-9]+")]);
        assert_eq!(both.len(), 2);
        assert!(both.matches(&headers(&[("x-a", "1"), ("x-b", "22")])));
        assert!(!both.matches(&headers(&[("x-a", "1")])));
        assert!(!both.matches(&headers(&[("x-a", "2"), ("x-b", "22")])));
    }

    #[test]
    fn no_predicates_means_every_request_passes() {
        let none = HeaderPredicates::default();
        assert!(none.is_empty());
        assert!(none.matches(&headers(&[])));
        assert!(none.matches(&headers(&[("x-a", "1")])));
    }

    #[test]
    fn only_the_first_predicate_for_a_header_name_counts() {
        let predicates = HeaderPredicates::new([
            exact("x-a", "1"),
            exact("X-A", "2"),
            exact("x-b", "3"),
            regex("x-a", "never"),
        ]);
        assert_eq!(predicates.len(), 2);
        assert!(predicates.matches(&headers(&[("x-a", "1"), ("x-b", "3")])));
        assert!(!predicates.matches(&headers(&[("x-a", "2"), ("x-b", "3")])));
    }

    #[test]
    fn predicates_that_could_never_hold_are_rejected() {
        use HeaderPredicateError::{InvalidName, InvalidValue};
        for name in ["", "x a", "x:a", "x-ä", "x\n"] {
            assert_eq!(
                HeaderPredicate::exact(name, "1").err(),
                Some(InvalidName),
                "{name:?}"
            );
            assert_eq!(
                HeaderPredicate::regex(name, "1").err(),
                Some(InvalidName),
                "{name:?}"
            );
        }
        for value in ["", " v2", "v2 ", "\tv2", "v2\r\nx: y", "v\0"] {
            assert_eq!(
                HeaderPredicate::exact("x-a", value).err(),
                Some(InvalidValue),
                "{value:?}"
            );
        }
        assert!(exact("x-a", "two words").matches(&headers(&[("x-a", "two words")])));
        assert!(matches!(
            HeaderPredicate::regex("x-a", "("),
            Err(HeaderPredicateError::Regex(RegexError::Syntax(_)))
        ));
    }

    /// Few names, in both cases, and values that are each other's pieces and joins, so that
    /// repeated headers and near misses come up all the time.
    fn field() -> impl Strategy<Value = (String, String)> {
        (
            prop::sample::select(vec!["x-a", "X-A", "x-b", "X-b", "x-c"]),
            prop::sample::select(vec!["1", "2", "1,2", "1,1", "12", ",", "1,"]),
        )
            .prop_map(|(name, value)| (name.to_owned(), value.to_owned()))
    }

    proptest! {
        #[test]
        fn exact_predicates_agree_with_the_join_everything_reference(
            request in prop::collection::vec(field(), 0..6),
            rule in prop::collection::vec(field(), 0..4),
        ) {
            let fields: Vec<(&str, &str)> =
                request.iter().map(|(name, value)| (&**name, &**value)).collect();
            let predicates =
                HeaderPredicates::new(rule.iter().map(|(name, value)| exact(name, value)));
            prop_assert_eq!(
                predicates.matches(&headers(&fields)),
                reference::exact_headers_match(&rule, &request)
            );
        }
    }
}
