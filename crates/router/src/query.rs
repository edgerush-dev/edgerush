//! Query parameter predicates: the `queryParams` of a Gateway API route match.
//!
//! Names and values are compared **decoded**, byte for byte and case-sensitively. The spec
//! does not say whether to decode, but nearly every backend does, and a rule on
//! `admin=true` that lets `?%61dmin=true` pass would be a way around whatever the rule is
//! there to enforce. Decoding is the usual one for queries: split on `&`, split each pair
//! at its first `=`, read `+` as a space, decode percent-encoding once.
//!
//! The rest is the spec or its recommendation: all of a rule's predicates must hold; when
//! a rule names a parameter twice only the first entry counts; a parameter the request
//! repeats is matched by its first occurrence. `?flag` and `?a=` are present with an empty
//! value, and a pair whose percent-encoding is malformed satisfies no predicate.
//!
//! The query is only read here, never rewritten: it is forwarded as it came.

use crate::RegexError;
use crate::explain::{Seen, Wanted};
use crate::normalise::hex_value;
use crate::whole_regex::WholeRegex;
use std::borrow::Cow;

/// A validated condition on one query parameter.
#[derive(Debug, Clone)]
pub struct QueryPredicate {
    /// As the decoded name in a request must read.
    name: Box<[u8]>,
    value: ValueMatch,
}

#[derive(Debug, Clone)]
enum ValueMatch {
    Exact(Box<[u8]>),
    Regex(WholeRegex),
}

impl QueryPredicate {
    /// The parameter must be present with exactly this value once decoded. `name` and
    /// `value` are taken as they are, not decoded: they say what the decoded text must be.
    ///
    /// # Errors
    ///
    /// Returns a [`QueryPredicateError`] if `name` is empty.
    pub fn exact(name: &str, value: &str) -> Result<Self, QueryPredicateError> {
        Ok(Self {
            name: parse_name(name)?,
            value: ValueMatch::Exact(value.as_bytes().into()),
        })
    }

    /// The parameter must be present with a decoded value that, as a whole, matches the
    /// regular expression: RE2-style syntax, linear-time matching.
    ///
    /// # Errors
    ///
    /// Returns a [`QueryPredicateError`] if `name` is empty or `pattern` is outside the
    /// contract for regular expressions.
    pub fn regex(name: &str, pattern: &str) -> Result<Self, QueryPredicateError> {
        Ok(Self {
            name: parse_name(name)?,
            value: ValueMatch::Regex(WholeRegex::new(pattern)?),
        })
    }

    /// The parameter it is about, as its decoded name must read.
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// What the parameter's decoded value must be.
    #[must_use]
    pub fn wanted(&self) -> Wanted<'_> {
        match &self.value {
            ValueMatch::Exact(value) => Wanted::Exact(value),
            ValueMatch::Regex(regex) => Wanted::Regex(regex.as_str()),
        }
    }

    /// The value this predicate judges the query by: its first occurrence's, decoded.
    pub(crate) fn seen(&self, query: &str) -> Seen {
        // Found as `matches` finds it. Not shared with it: as a function of its own, called
        // from `matches`, the search cost a match 12–15% more instructions.
        let first = pairs(query).find(|(name, _)| decoded_equals(name, &self.name) == Some(true));
        match first {
            None => Seen::Absent,
            Some((_, value)) => match decoded(value).collect() {
                Some(decoded) => Seen::Value(decoded),
                None => Seen::Undecodable(value.to_vec()),
            },
        }
    }

    /// Whether the query string (without its `?`) satisfies this predicate. Allocates only
    /// to decode a value that has encoding in it for a regex.
    #[must_use]
    pub fn matches(&self, query: &str) -> bool {
        // The first occurrence of the parameter decides, whatever comes after it.
        let first = pairs(query).find(|(name, _)| decoded_equals(name, &self.name) == Some(true));
        let Some((_, value)) = first else {
            return false;
        };
        match &self.value {
            ValueMatch::Exact(expected) => decoded_equals(value, expected) == Some(true),
            ValueMatch::Regex(regex) => {
                let plain = !value.iter().any(|byte| matches!(byte, b'%' | b'+'));
                let value = if plain {
                    Some(Cow::Borrowed(value))
                } else {
                    decoded(value).collect::<Option<Vec<u8>>>().map(Cow::Owned)
                };
                value.is_some_and(|value| regex.is_match(&value))
            }
        }
    }
}

fn parse_name(name: &str) -> Result<Box<[u8]>, QueryPredicateError> {
    if name.is_empty() {
        return Err(QueryPredicateError::EmptyName);
    }
    Ok(name.as_bytes().into())
}

/// The raw (name, value) pairs of a query string. A pair without `=` has an empty value;
/// empty pieces between `&`s are not pairs.
fn pairs(query: &str) -> impl Iterator<Item = (&[u8], &[u8])> {
    query
        .as_bytes()
        .split(|&byte| byte == b'&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, |&byte| byte == b'=');
            (
                parts.next().unwrap_or_default(),
                parts.next().unwrap_or_default(),
            )
        })
}

/// The bytes that `raw` decodes to; `None` in place of a malformed percent-encoding.
fn decoded(raw: &[u8]) -> impl Iterator<Item = Option<u8>> {
    let mut bytes = raw.iter().copied();
    std::iter::from_fn(move || {
        Some(match bytes.next()? {
            b'+' => Some(b' '),
            b'%' => {
                let mut digit = || bytes.next().and_then(hex_value);
                match (digit(), digit()) {
                    (Some(high), Some(low)) => Some((high << 4) | low),
                    _ => None,
                }
            }
            byte => Some(byte),
        })
    })
}

/// Whether `raw` decodes to exactly `expected`, without decoding it anywhere; `None` if it
/// cannot be decoded at all, which is looked for to the end even after a difference.
fn decoded_equals(raw: &[u8], expected: &[u8]) -> Option<bool> {
    // Nearly all names and values have no encoding in them and are what they say.
    if !raw.iter().any(|byte| matches!(byte, b'%' | b'+')) {
        return Some(raw == expected);
    }
    let mut expected = expected.iter();
    let mut equal = true;
    for byte in decoded(raw) {
        equal &= expected.next() == Some(&byte?);
    }
    Some(equal && expected.next().is_none())
}

/// The query parameter predicates of one rule: all of them must hold.
#[derive(Debug, Clone, Default)]
pub struct QueryPredicates(Box<[QueryPredicate]>);

impl QueryPredicates {
    /// Keeps the first predicate for every parameter name and drops later ones, which the
    /// spec says must be ignored. Names are case-sensitive.
    pub fn new(predicates: impl IntoIterator<Item = QueryPredicate>) -> Self {
        let mut kept: Vec<QueryPredicate> = Vec::new();
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

    /// Whether there is nothing to check, so every request passes and its query is never
    /// looked at.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the query string (without its `?`; empty if the request has none) satisfies
    /// every predicate.
    #[must_use]
    pub fn matches(&self, query: &str) -> bool {
        self.0.iter().all(|predicate| predicate.matches(query))
    }

    /// The predicates that count, in the order they are checked.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &QueryPredicate> {
        self.0.iter()
    }
}

/// Why a query parameter predicate was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryPredicateError {
    /// The parameter name is empty.
    #[error("query parameter name is empty")]
    EmptyName,
    /// The regular expression is outside the contract.
    #[error(transparent)]
    Regex(#[from] RegexError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference;
    use proptest::prelude::*;

    fn exact(name: &str, value: &str) -> QueryPredicate {
        QueryPredicate::exact(name, value).unwrap()
    }

    fn regex(name: &str, pattern: &str) -> QueryPredicate {
        QueryPredicate::regex(name, pattern).unwrap()
    }

    #[test]
    fn exact_predicate_wants_the_parameter_with_that_very_value() {
        let whale = exact("animal", "whale");
        assert!(whale.matches("animal=whale"));
        assert!(whale.matches("colour=blue&animal=whale&x=1"));
        assert!(!whale.matches("animal=whaledolphin"));
        assert!(!whale.matches("animal=Whale"));
        assert!(!whale.matches("ANIMAL=whale"));
        assert!(!whale.matches("animals=whale"));
        assert!(!whale.matches("colour=blue"));
        assert!(!whale.matches(""));
    }

    #[test]
    fn names_and_values_are_compared_decoded() {
        let admin = exact("admin", "true");
        assert!(admin.matches("%61dmin=true"));
        assert!(admin.matches("admin=%74%72%75%65"));
        assert!(admin.matches("%61%64%6D%69%6e=tru%65"));
        let spaced = exact("full name", "Ada Lovelace");
        assert!(spaced.matches("full+name=Ada+Lovelace"));
        assert!(spaced.matches("full%20name=Ada%20Lovelace"));
        assert!(!spaced.matches("full name=Ada"));
        // What the rule says is the decoded text, so encoding in the rule is just text.
        assert!(exact("a%20b", "1").matches("a%2520b=1"));
        assert!(!exact("a%20b", "1").matches("a%20b=1"));
    }

    #[test]
    fn plus_is_a_space_and_an_encoded_plus_is_a_plus() {
        assert!(exact("q", "a b").matches("q=a+b"));
        assert!(exact("q", "a+b").matches("q=a%2Bb"));
        assert!(!exact("q", "a+b").matches("q=a+b"));
    }

    #[test]
    fn repeated_parameter_is_matched_by_its_first_occurrence() {
        assert!(exact("admin", "false").matches("admin=false&admin=true"));
        assert!(!exact("admin", "true").matches("admin=false&admin=true"));
        assert!(!exact("admin", "true").matches("%61dmin=false&admin=true"));
    }

    #[test]
    fn parameter_without_a_value_is_present_and_empty() {
        for query in ["flag", "flag=", "x=1&flag&y=2", "flag=&flag=on"] {
            assert!(exact("flag", "").matches(query), "{query:?}");
            assert!(regex("flag", "").matches(query), "{query:?}");
            assert!(regex("flag", ".*").matches(query), "{query:?}");
            assert!(!exact("flag", "on").matches(query), "{query:?}");
        }
        assert!(!exact("flag", "").matches("flags"));
    }

    #[test]
    fn only_the_first_equals_sign_splits_a_pair() {
        assert!(exact("expr", "a=b=c").matches("expr=a=b=c"));
        assert!(exact("expr", "=").matches("expr=="));
    }

    #[test]
    fn semicolon_is_not_a_separator_and_empty_pieces_are_not_pairs() {
        assert!(!exact("b", "2").matches("a=1;b=2"));
        assert!(exact("a", "1;b=2").matches("a=1;b=2"));
        assert!(exact("b", "2").matches("&&a=1&&&b=2&"));
    }

    #[test]
    fn malformed_encoding_satisfies_no_predicate() {
        for query in ["a=%zz", "a=%4", "a=%", "a=1%"] {
            assert!(!exact("a", "1").matches(query), "{query:?}");
            assert!(!regex("a", ".*").matches(query), "{query:?}");
        }
        // The first occurrence decides even when it cannot be read.
        assert!(!exact("a", "1").matches("a=%zz&a=1"));
        // A pair whose name cannot be read is nobody's parameter.
        assert!(exact("a", "1").matches("%zz=9&a=1"));
        assert!(exact("a", "1").matches("a%=9&a=1"));
    }

    #[test]
    fn values_that_are_not_text_are_compared_as_bytes() {
        assert!(exact("q", "café").matches("q=caf%C3%A9"));
        assert!(!exact("q", "cafe").matches("q=caf%E9"));
        assert!(regex("q", "caf.").matches("q=caf%E9"));
        assert!(regex("q", "caf.+").matches("q=café"));
    }

    #[test]
    fn regex_predicate_is_held_to_the_whole_decoded_value() {
        let version = regex("v", r"\d+\.\d+");
        assert!(version.matches("v=1.20"));
        assert!(version.matches("v=1%2E20"));
        assert!(!version.matches("v=1.20-beta"));
        assert!(!version.matches("v=x1.20"));
        assert!(!version.matches("w=1.20"));
    }

    #[test]
    fn all_predicates_of_a_rule_must_hold_and_none_means_every_request_passes() {
        let both = QueryPredicates::new([exact("animal", "dolphin"), exact("colour", "blue")]);
        assert_eq!(both.len(), 2);
        assert!(both.matches("animal=dolphin&colour=blue"));
        assert!(both.matches("colour=blue&x=1&animal=dolphin"));
        assert!(!both.matches("animal=dolphin&colour=yellow"));
        assert!(!both.matches("colour=blue"));

        let none = QueryPredicates::default();
        assert!(none.is_empty());
        assert!(none.matches(""));
        assert!(none.matches("a=%zz"));
    }

    #[test]
    fn only_the_first_predicate_for_a_parameter_name_counts() {
        let predicates = QueryPredicates::new([
            exact("a", "1"),
            exact("a", "2"),
            exact("A", "3"),
            regex("a", "never"),
        ]);
        assert_eq!(predicates.len(), 2);
        assert!(predicates.matches("a=1&A=3"));
        assert!(!predicates.matches("a=2&A=3"));
    }

    #[test]
    fn predicates_that_make_no_sense_are_rejected() {
        assert_eq!(
            QueryPredicate::exact("", "1").err(),
            Some(QueryPredicateError::EmptyName)
        );
        assert_eq!(
            QueryPredicate::regex("", "1").err(),
            Some(QueryPredicateError::EmptyName)
        );
        assert!(matches!(
            QueryPredicate::regex("a", "("),
            Err(QueryPredicateError::Regex(RegexError::Syntax(_)))
        ));
    }

    /// Queries over the pieces every rule is about: separators, the two ways to write a
    /// space, good and bad encodings, and names and values that are each other's pieces.
    fn query() -> impl Strategy<Value = String> {
        let piece = prop_oneof![
            4 => Just("&"),
            4 => Just("="),
            3 => Just("a"),
            3 => Just("b"),
            2 => Just("1"),
            3 => Just("+"),
            2 => Just("%20"),
            1 => Just("%61"),
            1 => Just("%3D"),
            1 => Just("%26"),
            1 => Just("%2B"),
            1 => Just("%"),
            1 => Just("%4"),
            1 => Just("%zz"),
            1 => Just(";"),
            1 => Just("é"),
        ];
        prop::collection::vec(piece, 0..12).prop_map(|pieces| pieces.concat())
    }

    fn entry() -> impl Strategy<Value = (String, String)> {
        (
            prop::sample::select(vec!["a", "b", "ab", "a b", "a+b", " ", "a=", "A"]),
            prop::sample::select(vec![
                "", "1", "a", "a b", " ", "+", "a=1", "a&b", "a+b", "b",
            ]),
        )
            .prop_map(|(name, value)| (name.to_owned(), value.to_owned()))
    }

    /// One of the many ways to write `text` in a query: a space as `+` or `%20`, any other
    /// byte plain or percent-encoded, and what would split the query always encoded.
    fn spelt(text: &str) -> impl Strategy<Value = String> + use<> {
        let text = text.to_owned();
        prop::collection::vec(any::<bool>(), text.len()).prop_map(move |encode| {
            text.bytes()
                .zip(encode)
                .map(|(byte, encode)| match byte {
                    b' ' if !encode => "+".to_owned(),
                    b' ' | b'&' | b'=' | b'+' | b'%' => format!("%{byte:02X}"),
                    _ if encode => format!("%{byte:02x}"),
                    _ => char::from(byte).to_string(),
                })
                .collect()
        })
    }

    /// A rule and a query written for it: the rule's entries in some spelling, each as it
    /// is or spoilt by a trailing character, with arbitrary pairs before or after them —
    /// before, they may be the first occurrence of a name and decide instead.
    fn rule_and_query() -> impl Strategy<Value = (Vec<(String, String)>, String)> {
        prop::collection::vec(entry(), 0..4).prop_flat_map(|rule| {
            let pairs: Vec<_> = rule
                .iter()
                .map(|(name, value)| {
                    (
                        spelt(name),
                        spelt(value),
                        prop::sample::select(vec!["", "", "x"]),
                    )
                })
                .collect();
            let written =
                (pairs, query(), any::<bool>()).prop_map(|(pairs, noise, noise_first)| {
                    let mut parts: Vec<String> = pairs
                        .into_iter()
                        .map(|(name, value, spoilt)| format!("{name}={value}{spoilt}"))
                        .collect();
                    parts.insert(if noise_first { 0 } else { parts.len() }, noise);
                    parts.join("&")
                });
            (Just(rule), written)
        })
    }

    proptest! {
        #[test]
        fn exact_predicates_agree_with_the_reference_on_queries_written_for_the_rule(
            (rule, query) in rule_and_query()
        ) {
            let predicates =
                QueryPredicates::new(rule.iter().map(|(name, value)| exact(name, value)));
            prop_assert_eq!(
                predicates.matches(&query),
                reference::exact_query_matches(&rule, &query)
            );
        }

        #[test]
        fn exact_predicates_agree_with_the_decode_everything_reference(
            query in query(),
            rule in prop::collection::vec(entry(), 0..4),
        ) {
            let predicates =
                QueryPredicates::new(rule.iter().map(|(name, value)| exact(name, value)));
            prop_assert_eq!(
                predicates.matches(&query),
                reference::exact_query_matches(&rule, &query)
            );
        }

        #[test]
        fn match_anything_regex_holds_exactly_when_the_first_occurrence_can_be_read(
            query in query(),
        ) {
            let readable = reference::query_parameters(&query)
                .into_iter()
                .find(|(name, _)| name == b"a")
                .is_some_and(|(_, value)| value.is_some());
            prop_assert_eq!(regex("a", "(?s).*").matches(&query), readable);
        }
    }
}
