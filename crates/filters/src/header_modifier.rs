//! Header modification: Gateway API's `RequestHeaderModifier` and `ResponseHeaderModifier`.
//!
//! `set` gives a header one value, whatever it had; `add` appends a value to whatever it
//! had; `remove` takes the header away. Names are case-insensitive. The spec does not say
//! what a header named in more than one of the three, or twice in one, should come to — so
//! that is not accepted, and the order in which the three are carried out can never matter.
//!
//! Some headers are the gateway's own and no modifier may name them ([`RESERVED`]). `Host`
//! says where a request was routed; a rule that wants another host for the upstream needs a
//! rewrite of the host, which changes the target with it. The others describe the connection
//! and the framing of the message on it, which the gateway makes anew on either side: a
//! `Transfer-Encoding` or `Content-Length` out of step with the body that is really sent is
//! how requests are smuggled. The spec is silent on all of this.
//!
//! Nor may a modifier name `X-Request-ID` ([`request_id::HEADER`](crate::request_id::HEADER)),
//! whether or not its listener makes the IDs: a request is to be known by one ID to the
//! upstream, the client and the gateway, and a rule that changed it for one of them would
//! take that away. Only a rule's renaming of the header, which keeps its value, is to be
//! allowed, and there is none yet.

use crate::request_id;
use http::header::{self, HeaderMap, HeaderName, HeaderValue};

/// The headers no modifier may set, add or remove.
pub const RESERVED: [HeaderName; 8] = [
    header::HOST,
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::TE,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
    header::CONTENT_LENGTH,
];

/// What a modifier's changes are made to: a header map, or anything else that holds a
/// message's fields and can take them out, give one a single value, and add to one.
pub trait Edit {
    /// Takes away every value of `name`.
    fn remove(&mut self, name: &HeaderName);
    /// Gives `name` this one value, in place of every one it had.
    fn set(&mut self, name: &HeaderName, value: &HeaderValue);
    /// Adds a value to `name`, after every one it has.
    fn append(&mut self, name: &HeaderName, value: &HeaderValue);
}

impl Edit for HeaderMap {
    fn remove(&mut self, name: &HeaderName) {
        HeaderMap::remove(self, name);
    }

    fn set(&mut self, name: &HeaderName, value: &HeaderValue) {
        self.insert(name.clone(), value.clone());
    }

    fn append(&mut self, name: &HeaderName, value: &HeaderValue) {
        HeaderMap::append(self, name.clone(), value.clone());
    }
}

/// The most entries each of `set`, `add` and `remove` may have: Gateway API's own bound
/// (`HTTPHeaderFilter`, `MaxItems=16` on each). With a rule's modifier the only one a request
/// meets, what a request's head may have added to it is bounded, which a head kept as the
/// bytes it arrived in, with its edits beside it, relies on.
pub const MOST_PER_LIST: usize = 16;

/// A validated set of changes to a header map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderModifier {
    set: Box<[(HeaderName, HeaderValue)]>,
    add: Box<[(HeaderName, HeaderValue)]>,
    remove: Box<[HeaderName]>,
}

impl HeaderModifier {
    /// Builds a modifier from (name, value) pairs to set and to add, and names to remove.
    ///
    /// # Errors
    ///
    /// Returns a [`HeaderModifierError`] for a name that is not a header name or is
    /// [`RESERVED`], a value that no header could have (empty, white space at either end,
    /// control characters), a header that is named more than once, or more than
    /// [`MOST_PER_LIST`] entries in one of the three.
    pub fn new<'a>(
        set: impl IntoIterator<Item = (&'a str, &'a str)>,
        add: impl IntoIterator<Item = (&'a str, &'a str)>,
        remove: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, HeaderModifierError> {
        let mut named = Named(Vec::new());
        let set = named.pairs(set)?;
        let add = named.pairs(add)?;
        let remove: Box<[HeaderName]> = remove
            .into_iter()
            .map(|name| named.once(name))
            .collect::<Result<_, _>>()?;
        for (list, count) in [
            ("set", set.len()),
            ("add", add.len()),
            ("remove", remove.len()),
        ] {
            if count > MOST_PER_LIST {
                return Err(HeaderModifierError::TooMany(list));
            }
        }
        Ok(Self { set, add, remove })
    }

    /// Whether there is nothing to do, so the modifier need not be kept at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.add.is_empty() && self.remove.is_empty()
    }

    /// Whether any of the three names `name`.
    #[must_use]
    pub fn names(&self, name: &HeaderName) -> bool {
        self.remove.contains(name)
            || self.set.iter().any(|(named, _)| named == name)
            || self.add.iter().any(|(named, _)| named == name)
    }

    /// Carries out the changes. Allocates only as what is edited does to hold what is
    /// added; names and values are shared with the modifier, not copied.
    pub fn apply<E: Edit + ?Sized>(&self, headers: &mut E) {
        for name in &self.remove {
            headers.remove(name);
        }
        for (name, value) in &self.set {
            headers.set(name, value);
        }
        for (name, value) in &self.add {
            headers.append(name, value);
        }
    }
}

/// The headers named so far.
struct Named(Vec<HeaderName>);

impl Named {
    /// The header name, if it is one and has not been named before.
    fn once(&mut self, name: &str) -> Result<HeaderName, HeaderModifierError> {
        let parsed = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| HeaderModifierError::InvalidName(name.to_owned()))?;
        if RESERVED.contains(&parsed) {
            return Err(HeaderModifierError::Reserved(parsed.to_string()));
        }
        if parsed == request_id::HEADER {
            return Err(HeaderModifierError::RequestId);
        }
        if self.0.contains(&parsed) {
            return Err(HeaderModifierError::NamedTwice(parsed.to_string()));
        }
        self.0.push(parsed.clone());
        Ok(parsed)
    }

    fn pairs<'a>(
        &mut self,
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Box<[(HeaderName, HeaderValue)]>, HeaderModifierError> {
        pairs
            .into_iter()
            .map(|(name, value)| Ok((self.once(name)?, parse_value(name, value)?)))
            .collect()
    }
}

fn parse_value(name: &str, value: &str) -> Result<HeaderValue, HeaderModifierError> {
    let trimmed = value.trim_matches([' ', '\t']);
    HeaderValue::from_str(value)
        .ok()
        .filter(|_| !value.is_empty() && trimmed.len() == value.len())
        .ok_or_else(|| HeaderModifierError::InvalidValue(name.to_owned()))
}

/// Why a header modifier was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HeaderModifierError {
    /// Not a valid header name.
    #[error("`{0}` is not a header name")]
    InvalidName(String),
    /// One of the gateway's own headers ([`RESERVED`]).
    #[error("header `{0}` is the gateway's own and cannot be modified")]
    Reserved(String),
    /// `X-Request-ID`, which carries the request's ID.
    #[error("header `x-request-id` carries the request's ID, which a rule cannot change")]
    RequestId,
    /// Not something a header could have as its value.
    #[error("the value for header `{0}` is not a header value")]
    InvalidValue(String),
    /// The header is named more than once, within `set`, `add` and `remove` or across them.
    #[error("header `{0}` is named more than once")]
    NamedTwice(String),
    /// More than [`MOST_PER_LIST`] entries in `set`, `add` or `remove`.
    #[error("more than {MOST_PER_LIST} headers in `{0}`")]
    TooMany(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// The fields of a header map, names in lower case, sorted by name with each name's
    /// values in their order.
    fn fields(headers: &HeaderMap) -> Vec<(String, String)> {
        let mut fields: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect();
        fields.sort_by(|(one, _), (other, _)| one.cmp(other));
        fields
    }

    fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn borrowed(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
        pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }

    fn modified(modifier: &HeaderModifier, before: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut headers = headers(before);
        modifier.apply(&mut headers);
        fields(&headers)
    }

    #[test]
    fn set_gives_the_header_one_value_whatever_it_had() {
        let modifier = HeaderModifier::new([("My-Header", "bar")], [], []).unwrap();
        assert_eq!(
            modified(&modifier, &[("my-header", "foo")]),
            pairs(&[("my-header", "bar")])
        );
        assert_eq!(
            modified(
                &modifier,
                &[("my-header", "foo"), ("my-header", "baz"), ("x", "1")]
            ),
            pairs(&[("my-header", "bar"), ("x", "1")])
        );
        assert_eq!(modified(&modifier, &[]), pairs(&[("my-header", "bar")]));
    }

    #[test]
    fn add_appends_to_whatever_the_header_had() {
        let modifier = HeaderModifier::new([], [("my-header", "bar,baz")], []).unwrap();
        assert_eq!(
            modified(&modifier, &[("My-Header", "foo")]),
            pairs(&[("my-header", "foo"), ("my-header", "bar,baz")])
        );
        assert_eq!(modified(&modifier, &[]), pairs(&[("my-header", "bar,baz")]));
    }

    #[test]
    fn remove_takes_the_header_away_with_all_its_values() {
        let modifier = HeaderModifier::new([], [], ["My-Header1", "my-header3"]).unwrap();
        let before = [
            ("my-header1", "foo"),
            ("my-header2", "bar"),
            ("my-header3", "baz"),
            ("my-header1", "again"),
        ];
        assert_eq!(
            modified(&modifier, &before),
            pairs(&[("my-header2", "bar")])
        );
        assert_eq!(modified(&modifier, &[]), pairs(&[]));
    }

    #[test]
    fn all_three_at_once_leave_other_headers_alone() {
        let modifier =
            HeaderModifier::new([("x-set", "s")], [("x-add", "a")], ["x-remove"]).unwrap();
        let before = [
            ("x-set", "old"),
            ("x-add", "old"),
            ("x-remove", "old"),
            ("x-keep", "k"),
        ];
        assert_eq!(
            modified(&modifier, &before),
            pairs(&[
                ("x-add", "old"),
                ("x-add", "a"),
                ("x-keep", "k"),
                ("x-set", "s")
            ])
        );
    }

    #[test]
    fn a_modifier_says_which_headers_it_names_whatever_their_case() {
        let modifier =
            HeaderModifier::new([("X-Set", "1")], [("x-add", "2")], ["X-Remove"]).unwrap();
        for named in ["x-set", "x-add", "x-remove"] {
            assert!(modifier.names(&HeaderName::from_static(named)), "{named}");
        }
        assert!(!modifier.names(&header::LOCATION));
        assert!(!HeaderModifier::default().names(&header::LOCATION));
    }

    #[test]
    fn a_modifier_with_nothing_to_do_says_so() {
        let nothing = HeaderModifier::new([], [], []).unwrap();
        assert!(nothing.is_empty());
        assert_eq!(nothing, HeaderModifier::default());
        assert_eq!(modified(&nothing, &[("x", "1")]), pairs(&[("x", "1")]));
        assert!(!HeaderModifier::new([], [], ["x"]).unwrap().is_empty());
    }

    #[test]
    fn a_header_named_more_than_once_is_rejected() {
        use HeaderModifierError::NamedTwice;
        let twice = Err(NamedTwice("x-a".to_owned()));
        assert_eq!(
            HeaderModifier::new([("x-a", "1"), ("X-A", "2")], [], []),
            twice
        );
        assert_eq!(
            HeaderModifier::new([], [("x-a", "1"), ("x-a", "1")], []),
            twice
        );
        assert_eq!(HeaderModifier::new([], [], ["x-a", "X-a"]), twice);
        assert_eq!(
            HeaderModifier::new([("x-a", "1")], [("X-A", "2")], []),
            twice
        );
        assert_eq!(HeaderModifier::new([("x-a", "1")], [], ["x-a"]), twice);
        assert_eq!(HeaderModifier::new([], [("x-a", "1")], ["x-a"]), twice);
    }

    /// Gateway API holds each of `set`, `add` and `remove` to 16 entries, and so does this:
    /// 16 goes, 17 does not, in any of the three.
    #[test]
    fn more_than_sixteen_in_a_list_is_rejected() {
        let names: Vec<String> = (0..=MOST_PER_LIST).map(|n| format!("x-{n}")).collect();
        let pairs = |count: usize| names[..count].iter().map(|name| (name.as_str(), "v"));
        let removed = |count: usize| names[..count].iter().map(String::as_str);
        assert!(HeaderModifier::new(pairs(MOST_PER_LIST), [], []).is_ok());
        assert!(HeaderModifier::new([], pairs(MOST_PER_LIST), []).is_ok());
        assert!(HeaderModifier::new([], [], removed(MOST_PER_LIST)).is_ok());
        let too_many = |list| Err(HeaderModifierError::TooMany(list));
        assert_eq!(
            HeaderModifier::new(pairs(MOST_PER_LIST + 1), [], []),
            too_many("set")
        );
        assert_eq!(
            HeaderModifier::new([], pairs(MOST_PER_LIST + 1), []),
            too_many("add")
        );
        assert_eq!(
            HeaderModifier::new([], [], removed(MOST_PER_LIST + 1)),
            too_many("remove")
        );
    }

    #[test]
    fn headers_that_are_the_gateways_own_are_rejected() {
        for name in [
            "host",
            "Host",
            "connection",
            "keep-alive",
            "proxy-connection",
            "te",
            "Transfer-Encoding",
            "upgrade",
            "content-length",
        ] {
            let reserved = Err(HeaderModifierError::Reserved(name.to_ascii_lowercase()));
            assert_eq!(
                HeaderModifier::new([(name, "1")], [], []),
                reserved,
                "{name}"
            );
            assert_eq!(
                HeaderModifier::new([], [(name, "1")], []),
                reserved,
                "{name}"
            );
            assert_eq!(HeaderModifier::new([], [], [name]), reserved, "{name}");
        }
        // HTTP/2's pseudo-headers are no header names at all, to remove as little as to set.
        for name in [":authority", ":path"] {
            let invalid = Err(HeaderModifierError::InvalidName(name.to_owned()));
            assert_eq!(HeaderModifier::new([], [], [name]), invalid, "{name}");
        }
    }

    #[test]
    fn the_request_id_is_changed_by_no_modifier() {
        for name in ["x-request-id", "X-Request-ID"] {
            let refused = Err(HeaderModifierError::RequestId);
            assert_eq!(
                HeaderModifier::new([(name, "1")], [], []),
                refused,
                "{name}"
            );
            assert_eq!(
                HeaderModifier::new([], [(name, "1")], []),
                refused,
                "{name}"
            );
            assert_eq!(HeaderModifier::new([], [], [name]), refused, "{name}");
        }
        // Names that only look like it are anyone's.
        for name in ["x-request-ids", "request-id", "x-correlation-id"] {
            assert!(HeaderModifier::new([(name, "1")], [], []).is_ok(), "{name}");
        }
    }

    #[test]
    fn headers_about_forwarding_and_content_are_not_reserved() {
        let names = [
            "x-forwarded-for",
            "x-forwarded-host",
            "content-type",
            "trailer",
            "authorization",
        ];
        for name in names {
            assert!(HeaderModifier::new([(name, "1")], [], []).is_ok(), "{name}");
            assert!(HeaderModifier::new([], [], [name]).is_ok(), "{name}");
        }
    }

    #[test]
    fn names_and_values_no_header_could_have_are_rejected() {
        use HeaderModifierError::{InvalidName, InvalidValue};
        for name in ["", "x a", "x:a", "x-ä", "x\n"] {
            let invalid = Err(InvalidName(name.to_owned()));
            assert_eq!(
                HeaderModifier::new([(name, "1")], [], []),
                invalid,
                "{name:?}"
            );
            assert_eq!(
                HeaderModifier::new([], [(name, "1")], []),
                invalid,
                "{name:?}"
            );
            assert_eq!(HeaderModifier::new([], [], [name]), invalid, "{name:?}");
        }
        for value in ["", " v", "v ", "\tv", "v\r\nx-injected: 1", "v\0"] {
            let invalid = Err(InvalidValue("x-a".to_owned()));
            assert_eq!(
                HeaderModifier::new([("x-a", value)], [], []),
                invalid,
                "{value:?}"
            );
            assert_eq!(
                HeaderModifier::new([], [("x-a", value)], []),
                invalid,
                "{value:?}"
            );
        }
        assert!(HeaderModifier::new([("x-a", "two words")], [], []).is_ok());
    }

    /// The specification on a plain list of fields: remove, then set, then add — in any
    /// order it would come to the same, since no header is named twice.
    fn reference(
        before: &[(String, String)],
        set: &[(String, String)],
        add: &[(String, String)],
        remove: &[String],
    ) -> Vec<(String, String)> {
        let lower = |name: &String| name.to_ascii_lowercase();
        let gone: Vec<String> = remove
            .iter()
            .chain(set.iter().map(|(name, _)| name))
            .map(lower)
            .collect();
        let mut after: Vec<(String, String)> = before
            .iter()
            .map(|(name, value)| (lower(name), value.clone()))
            .filter(|(name, _)| !gone.contains(name))
            .collect();
        after.extend(
            set.iter()
                .chain(add)
                .map(|(name, value)| (lower(name), value.clone())),
        );
        after.sort_by(|(one, _), (other, _)| one.cmp(other));
        after
    }

    /// Distinct names for set, add and remove out of a handful, and fields over the same
    /// handful in both cases, so that every change meets headers it applies to.
    fn case() -> impl Strategy<Value = (Vec<(String, String)>, [Vec<String>; 3])> {
        let field = (
            prop::sample::select(vec!["x-a", "X-A", "x-b", "X-B", "x-c", "x-d", "x-e"]),
            prop::sample::select(vec!["1", "2", "3"]),
        )
            .prop_map(|(name, value)| (name.to_owned(), value.to_owned()));
        let names = Just(vec!["x-a", "x-b", "x-c", "x-d", "x-e"])
            .prop_shuffle()
            .prop_flat_map(|names| (Just(names), 0..=2_usize, 0..=2_usize, 0..=1_usize))
            .prop_map(|(names, set, add, remove)| {
                let names: Vec<String> = names.into_iter().map(str::to_owned).collect();
                let (set, rest) = names.split_at(set);
                let (add, rest) = rest.split_at(add);
                [set.to_vec(), add.to_vec(), rest[..remove].to_vec()]
            });
        (prop::collection::vec(field, 0..8), names)
    }

    proptest! {
        #[test]
        fn applying_agrees_with_the_reference_on_a_plain_list((before, [set, add, remove]) in case()) {
            let valued = |names: &[String], value: &str| -> Vec<(String, String)> {
                names.iter().map(|name| (name.clone(), value.to_owned())).collect()
            };
            let (set, add) = (valued(&set, "set"), valued(&add, "add"));
            let modifier = HeaderModifier::new(
                borrowed(&set),
                borrowed(&add),
                remove.iter().map(String::as_str),
            )
            .unwrap();

            let before_fields = borrowed(&before);
            prop_assert_eq!(
                modified(&modifier, &before_fields),
                reference(&before, &set, &add, &remove)
            );
        }
    }
}
