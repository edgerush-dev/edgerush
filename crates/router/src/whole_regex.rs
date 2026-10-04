//! The regular expressions of the routing contract: RE2-style syntax, matched in linear
//! time against the **whole** value — a path, a header value, a query parameter value.
//!
//! The engine behind the contract is a detail of this module and appears nowhere in the
//! crate's interface, so it can be replaced by anything that honours the contract.

use regex::bytes::{Regex, RegexBuilder};

/// The most memory one compiled regex may take. Far more than any sane pattern needs; a
/// pattern that repeats large groups a large number of times is refused.
const SIZE_LIMIT: usize = 256 * 1024;
/// The most memory the engine may use to speed up matching one regex, per thread that
/// matches it. Going without only makes matching slower, never wrong.
const CACHE_LIMIT: usize = 512 * 1024;

/// A compiled regular expression that matches whole values only: `/users/\d+` matches
/// `/users/42` but not `/users/42/edit`; whoever wants less writes `.*`. Case-sensitive
/// unless the pattern says otherwise (`(?i)`).
#[derive(Debug, Clone)]
pub(crate) struct WholeRegex(Regex);

impl WholeRegex {
    /// Compiles `pattern`. No look-around and no backreferences, which is what makes
    /// linear-time matching possible; Unicode support is off, as in RE2's defaults, so
    /// `\d` and `\w` are the ASCII classes. The pattern itself has to be ASCII: the values
    /// it is matched against are (normalised paths) or should be (header values).
    pub(crate) fn new(pattern: &str) -> Result<Self, RegexError> {
        if !pattern.is_ascii() {
            return Err(RegexError::NotAscii);
        }
        // On its own first: a pattern such as `a)|(b` must not be able to close the group
        // that holds it to the whole value, and syntax errors should quote the user's text.
        compile(pattern)?;
        compile(&format!("^(?:{pattern})$")).map(Self)
    }

    /// Whether the whole of `value` matches. Never allocates once the engine has warmed
    /// up for this regex on this thread.
    pub(crate) fn is_match(&self, value: &[u8]) -> bool {
        self.0.is_match(value)
    }

    /// The pattern as it was written. Read back from the compiled form rather than kept
    /// beside it: only `explain` asks.
    pub(crate) fn as_str(&self) -> &str {
        let compiled = self.0.as_str();
        compiled
            .strip_prefix("^(?:")
            .and_then(|pattern| pattern.strip_suffix(")$"))
            .unwrap_or(compiled)
    }
}

fn compile(pattern: &str) -> Result<Regex, RegexError> {
    RegexBuilder::new(pattern)
        .unicode(false)
        .size_limit(SIZE_LIMIT)
        .dfa_size_limit(CACHE_LIMIT)
        .build()
        .map_err(|error| match error {
            regex::Error::CompiledTooBig(_) => RegexError::TooLarge,
            other => RegexError::Syntax(other.to_string()),
        })
}

/// Why a regular expression was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegexError {
    /// Not valid RE2-style syntax; the text says what is wrong.
    #[error("invalid regular expression: {0}")]
    Syntax(String),
    /// Contains something other than ASCII.
    #[error("regular expression is not ASCII; write other characters percent-encoded")]
    NotAscii,
    /// Compiles to more than the size allowed for one pattern.
    #[error("regular expression is too large once compiled")]
    TooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regex(pattern: &str) -> WholeRegex {
        WholeRegex::new(pattern).unwrap()
    }

    #[test]
    fn the_whole_value_must_match() {
        let user = regex(r"/users/\d+");
        assert!(user.is_match(b"/users/42"));
        assert!(!user.is_match(b"/users/42/edit"));
        assert!(!user.is_match(b"/x/users/42"));
        assert!(regex(r"/users/\d+(/.*)?").is_match(b"/users/42/edit"));
        assert!(regex(r".*/users/\d+").is_match(b"/x/users/42"));
    }

    #[test]
    fn anchors_the_user_writes_change_nothing() {
        let user = regex(r"^/users/\d+$");
        assert!(user.is_match(b"/users/42"));
        assert!(!user.is_match(b"/users/42/edit"));
    }

    #[test]
    fn every_branch_of_an_alternation_is_held_to_the_whole_value() {
        let either = regex("/a|/ab");
        assert!(either.is_match(b"/a"));
        assert!(either.is_match(b"/ab"));
        assert!(!either.is_match(b"/abc"));
        assert!(!either.is_match(b"/x/a"));
    }

    #[test]
    fn matching_is_case_sensitive_unless_the_pattern_says_otherwise() {
        assert!(!regex("/shop").is_match(b"/Shop"));
        assert!(regex("(?i)/shop").is_match(b"/SHOP"));
    }

    #[test]
    fn the_pattern_reads_back_as_it_was_written() {
        for pattern in [r"/users/\d+", "/a|/ab", r"^/x$", r"\)$", "(?i)a.*", ""] {
            assert_eq!(regex(pattern).as_str(), pattern);
        }
    }

    #[test]
    fn values_that_are_not_text_are_matched_as_bytes() {
        assert!(regex("a.b").is_match(b"a\xffb"));
        assert!(!regex(r"a\wb").is_match(b"a\xffb"));
    }

    #[test]
    fn patterns_outside_the_contract_are_rejected() {
        // Broken syntax, an attempt to break out of the anchoring, and the two features
        // that cannot be matched in linear time: look-around and backreferences.
        for pattern in ["/a(", "/a)|(/b", "/a(?=b)", r"/(a)\1"] {
            assert!(
                matches!(WholeRegex::new(pattern), Err(RegexError::Syntax(_))),
                "{pattern:?}"
            );
        }
        assert_eq!(WholeRegex::new("/café").err(), Some(RegexError::NotAscii));
        assert_eq!(
            WholeRegex::new("(?:/[a-z]{1,500}){1,500}").err(),
            Some(RegexError::TooLarge)
        );
    }

    #[test]
    fn hostile_pattern_and_value_are_matched_in_linear_time() {
        // Catastrophic for a backtracking engine; a test that finishes is the assertion.
        let pattern = regex("/(a+)+b");
        let value = format!("/{}", "a".repeat(100_000));
        assert!(!pattern.is_match(value.as_bytes()));
    }
}
