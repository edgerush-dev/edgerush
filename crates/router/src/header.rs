//! Header predicates: the `headers` of a Gateway API route match.
//!
//! A rule's header predicates must all hold. Names are case-insensitive, values are not.
//! When a rule names a header twice, only the first entry counts, as the spec requires. A
//! header that the request repeats is read as the RFC says such headers are to be read —
//! as one value, the fields joined by commas — so `x: a` plus `x: b` is `a,b`, which an
//! exact match on `a` does not accept. `Cookie` is the exception the RFCs make: HTTP/2 lets
//! a client split its cookie string into fields, and what puts it together again is `"; "`
//! (RFC 9113 §8.2.3) — a comma would make a different cookie string of it.

use crate::RegexError;
use crate::explain::{Seen, Wanted};
use crate::whole_regex::WholeRegex;
use http::header::{COOKIE, HeaderMap, HeaderName, HeaderValue};

/// Read access to a request's header fields, whatever holds them: a map, or the lines of
/// the head as they arrived.
pub trait Fields {
    /// The values of the field lines called `name`, in the order they arrived. Names are
    /// compared case-insensitively; `name`, like every [`HeaderName`], is lower case.
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]>;
}

impl Fields for HeaderMap {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        self.get_all(name).iter().map(HeaderValue::as_bytes)
    }
}

impl<F: Fields + ?Sized> Fields for &F {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        (**self).values(name)
    }
}

/// A validated condition on one request header.
#[derive(Debug, Clone)]
pub struct HeaderPredicate {
    name: HeaderName,
    value: ValueMatch,
    /// What joins the fields of this header when the request repeats it.
    between: &'static [u8],
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
        Self::new(name, ValueMatch::Exact(value))
    }

    /// The header must be present with a value that, as a whole, matches the regular
    /// expression: RE2-style syntax, linear-time matching.
    ///
    /// # Errors
    ///
    /// Returns a [`HeaderPredicateError`] if `name` is not a header name or `pattern` is
    /// outside the contract for regular expressions.
    pub fn regex(name: &str, pattern: &str) -> Result<Self, HeaderPredicateError> {
        Self::new(name, ValueMatch::Regex(WholeRegex::new(pattern)?))
    }

    fn new(name: &str, value: ValueMatch) -> Result<Self, HeaderPredicateError> {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| HeaderPredicateError::InvalidName)?;
        let between: &[u8] = if name == COOKIE { b"; " } else { b"," };
        Ok(Self {
            name,
            value,
            between,
        })
    }

    /// The header it is about, in lower case.
    #[must_use]
    pub fn name(&self) -> &HeaderName {
        &self.name
    }

    /// What the header's value must be.
    #[must_use]
    pub fn wanted(&self) -> Wanted<'_> {
        match &self.value {
            ValueMatch::Exact(value) => Wanted::Exact(value.as_bytes()),
            ValueMatch::Regex(regex) => Wanted::Regex(regex.as_str()),
        }
    }

    /// The value this predicate judges the request by: its fields of the header, joined as
    /// for matching.
    pub(crate) fn seen<F: Fields + ?Sized>(&self, headers: &F) -> Seen {
        let mut values = headers.values(&self.name);
        let Some(first) = values.next() else {
            return Seen::Absent;
        };
        let mut joined = first.to_vec();
        for value in values {
            joined.extend_from_slice(self.between);
            joined.extend_from_slice(value);
        }
        Seen::Value(joined)
    }

    /// Whether the request's headers satisfy this predicate. Allocates only to join the
    /// values of a repeated header for a regex.
    #[must_use]
    pub fn matches<F: Fields + ?Sized>(&self, headers: &F) -> bool {
        let mut values = headers.values(&self.name);
        let Some(first) = values.next() else {
            return false;
        };
        let mut rest = values.peekable();
        match &self.value {
            ValueMatch::Exact(expected) if rest.peek().is_none() => first == expected.as_bytes(),
            ValueMatch::Regex(regex) if rest.peek().is_none() => regex.is_match(first),
            // A repeated header. The expected value is compared piece by piece with what
            // joining would give, so the usual kind of match needs no copy.
            ValueMatch::Exact(expected) => {
                let mut expected = expected.as_bytes().strip_prefix(first);
                for value in rest {
                    expected = expected
                        .and_then(|expected| expected.strip_prefix(self.between))
                        .and_then(|expected| expected.strip_prefix(value));
                }
                expected.is_some_and(<[u8]>::is_empty)
            }
            ValueMatch::Regex(regex) => {
                let mut joined = first.to_vec();
                for value in rest {
                    joined.extend_from_slice(self.between);
                    joined.extend_from_slice(value);
                }
                regex.is_match(&joined)
            }
        }
    }
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
    pub fn matches<F: Fields + ?Sized>(&self, headers: &F) -> bool {
        self.0.iter().all(|predicate| predicate.matches(headers))
    }

    /// The predicates that count, in the order they are checked.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &HeaderPredicate> {
        self.0.iter()
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
    fn repeated_cookie_is_read_as_one_cookie_string() {
        // HTTP/2 may split the cookie string into fields; "; " puts it together again
        // (RFC 9113 §8.2.3), and a comma would make another cookie string of it.
        let split = headers(&[("cookie", "a=1"), ("accept", "*/*"), ("Cookie", "b=2")]);
        assert!(exact("cookie", "a=1; b=2").matches(&split));
        assert!(exact("Cookie", "a=1; b=2").matches(&split));
        assert!(!exact("cookie", "a=1,b=2").matches(&split));
        assert!(!exact("cookie", "a=1").matches(&split));
        assert!(!exact("cookie", "a=1; b=2; c=3").matches(&split));
        assert!(regex("cookie", "(.*; )?b=2(; .*)?").matches(&split));
        assert!(!regex("cookie", "a=1,b=2").matches(&split));

        let whole = headers(&[("cookie", "a=1; b=2")]);
        assert!(exact("cookie", "a=1; b=2").matches(&whole));
        assert!(regex("cookie", "(.*; )?b=2(; .*)?").matches(&whole));
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
    /// repeated headers and near misses come up all the time — for cookies too, which are
    /// joined in a way of their own.
    fn field() -> impl Strategy<Value = (String, String)> {
        (
            prop::sample::select(vec!["x-a", "X-A", "x-b", "X-b", "x-c", "cookie", "Cookie"]),
            prop::sample::select(vec![
                "1", "2", "1,2", "1,1", "12", ",", "1,", "1; 2", "1;2", "1; 1", ";",
            ]),
        )
            .prop_map(|(name, value)| (name.to_owned(), value.to_owned()))
    }

    /// Field lines as they arrived, found by comparing names case-insensitively: the
    /// simplest other thing that holds a head, standing in for the raw one.
    struct Lines(Vec<(String, String)>);

    impl Fields for Lines {
        fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
            self.0
                .iter()
                .filter(move |(line, _)| line.eq_ignore_ascii_case(name.as_str()))
                .map(|(_, value)| value.as_bytes())
        }
    }

    fn rule_predicate() -> impl Strategy<Value = HeaderPredicate> {
        (
            field(),
            any::<bool>(),
            prop::sample::select(vec!["1.*", "1(,1)*", "(.*; )?2", ".*,.*"]),
        )
            .prop_map(|((name, value), is_regex, pattern)| {
                if is_regex {
                    regex(&name, pattern)
                } else {
                    exact(&name, &value)
                }
            })
    }

    proptest! {
        #[test]
        fn predicates_see_the_same_through_a_map_and_through_lines(
            request in prop::collection::vec(field(), 0..6),
            rule in prop::collection::vec(rule_predicate(), 0..4),
        ) {
            let fields: Vec<(&str, &str)> =
                request.iter().map(|(name, value)| (&**name, &**value)).collect();
            let predicates = HeaderPredicates::new(rule);
            prop_assert_eq!(
                predicates.matches(&headers(&fields)),
                predicates.matches(&Lines(request.clone()))
            );
        }

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
