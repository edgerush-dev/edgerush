//! Fuzzes query parameter predicates against their reference, which decodes the whole query
//! into a list first: the predicates must agree with it on every query, however it is
//! encoded, repeated or broken.
//!
//! Input: the query string on the first line, then one exact match per line as
//! `name=value` (split at the first `=`; lines without a name are skipped).

#![no_main]

use edgerush_router::{QueryPredicate, QueryPredicates, reference};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    let mut lines = input.split('\n');
    let Some(query) = lines.next() else {
        return;
    };
    let rule: Vec<(String, String)> = lines
        .map(|line| line.split_once('=').unwrap_or((line, "")))
        .filter(|(name, _)| !name.is_empty())
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();

    let predicates = QueryPredicates::new(rule.iter().map(|(name, value)| {
        QueryPredicate::exact(name, value).unwrap_or_else(|error| panic!("{name:?}: {error}"))
    }));
    assert_eq!(
        predicates.matches(query),
        reference::exact_query_matches(&rule, query),
        "{rule:?} on {query:?}"
    );

    // A regex that accepts anything holds exactly when the first occurrence can be read.
    for (name, _) in &rule {
        let readable = reference::query_parameters(query)
            .into_iter()
            .find(|(parameter, _)| parameter == name.as_bytes())
            .is_some_and(|(_, value)| value.is_some());
        let anything = QueryPredicate::regex(name, "(?s).*")
            .unwrap_or_else(|error| panic!("{name:?}: {error}"));
        assert_eq!(anything.matches(query), readable, "{name:?} on {query:?}");
    }
});
