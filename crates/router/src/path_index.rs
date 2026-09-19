//! The path stage of routing: from a normalised request path to the candidates whose path
//! pattern it satisfies, in precedence order — exact matches first, then regular
//! expressions, then prefixes from the longest to the shortest.
//!
//! Gateway API fixes the order of exact and prefix matches and leaves regular expressions
//! to the implementation. They come before prefixes because a regex is usually the more
//! specific statement, and a catch-all `/` prefix would otherwise shadow every regex beside
//! it. They have no specificity among themselves and are tried in the order given.

use crate::hash::Map;
use crate::path::{Kind, PathPattern};

/// An immutable index from request path to candidates, built once per config snapshot.
#[derive(Debug)]
pub struct PathIndex<T> {
    exact: Map<Box<[T]>>,
    /// Tried one after the other, and only as far as the caller asks for candidates. An
    /// index without regexes pays nothing for them.
    regexes: Vec<(PathPattern, T)>,
    /// A trie over path segments. The root, always present at position 0, is the prefix
    /// `/`. Children are created after their parents, so they sit at higher positions.
    prefixes: Vec<PrefixNode<T>>,
}

#[derive(Debug)]
struct PrefixNode<T> {
    /// Towards the root, which is how candidates are handed out: longest prefix first.
    parent: Option<usize>,
    children: Map<usize>,
    /// The values of the prefix pattern that ends at this node, if there is one.
    values: Vec<T>,
}

impl<T> PrefixNode<T> {
    fn new(parent: Option<usize>) -> Self {
        Self {
            parent,
            children: Map::default(),
            values: Vec::new(),
        }
    }
}

impl<T> PathIndex<T> {
    /// Builds the index. Values with the same pattern keep the order they were given in.
    pub fn new(entries: impl IntoIterator<Item = (PathPattern, T)>) -> Self {
        let mut exact: Map<Vec<T>> = Map::default();
        let mut regexes = Vec::new();
        let mut prefixes = vec![PrefixNode::new(None)];
        for (pattern, value) in entries {
            match &pattern.kind {
                Kind::Regex(_) => regexes.push((pattern, value)),
                Kind::Exact => exact
                    .entry(pattern.path.as_bytes().into())
                    .or_default()
                    .push(value),
                Kind::Prefix => {
                    let mut at = 0;
                    for segment in segments(&pattern.path) {
                        let new = prefixes.len();
                        let child = prefixes.get_mut(at).map(|node| {
                            *node
                                .children
                                .entry(segment.as_bytes().into())
                                .or_insert(new)
                        });
                        // `at` is always a position handed out below, so the node exists.
                        let Some(child) = child else { break };
                        if child == new {
                            prefixes.push(PrefixNode::new(Some(at)));
                        }
                        at = child;
                    }
                    if let Some(node) = prefixes.get_mut(at) {
                        node.values.push(value);
                    }
                }
            }
        }
        Self {
            exact: exact
                .into_iter()
                .map(|(path, values)| (path, values.into_boxed_slice()))
                .collect(),
            regexes,
            prefixes,
        }
    }

    /// The candidates for a request path, best match first: those with exactly this path,
    /// then those with a regex that matches it, then those with a prefix of it, longest
    /// prefix first.
    ///
    /// `path` is the normalised path alone, as for [`PathPattern::matches`], and the two
    /// always agree on what matches. Never allocates. Regexes are only run as the
    /// candidates before them are used up.
    #[must_use]
    pub fn lookup<'a>(&'a self, path: &'a str) -> PathCandidates<'a, T> {
        let exact = self
            .exact
            .get(path.as_bytes())
            .map(|values| &**values)
            .unwrap_or_default();
        // Only absolute paths have prefixes. Walk down as far as the segments lead; the
        // node reached and its ancestors are the matching prefixes.
        let deepest = path.starts_with('/').then(|| {
            let mut at = 0;
            for segment in segments(path) {
                let child = self
                    .prefixes
                    .get(at)
                    .and_then(|node| node.children.get(segment.as_bytes()));
                match child {
                    Some(&child) => at = child,
                    None => break,
                }
            }
            at
        });
        PathCandidates {
            path,
            regexes: self.regexes.iter(),
            prefixes: &self.prefixes,
            current: exact.iter(),
            next: deepest,
        }
    }
}

/// The segments of an absolute path, or of a stored prefix (which has no trailing slash, so
/// the root prefix has none). A trailing slash on a request path gives a last empty
/// segment; no pattern has one, so the walk down the trie ends there.
fn segments(path: &str) -> impl Iterator<Item = &str> {
    path.strip_prefix('/')
        .into_iter()
        .flat_map(|rest| rest.split('/'))
}

/// Candidates for one request path, best match first; see [`PathIndex::lookup`].
#[derive(Debug)]
pub struct PathCandidates<'a, T> {
    path: &'a str,
    /// The regexes not tried yet; their turn comes when the exact matches in `current`
    /// run dry, before the first trie node is handed out.
    regexes: std::slice::Iter<'a, (PathPattern, T)>,
    prefixes: &'a [PrefixNode<T>],
    current: std::slice::Iter<'a, T>,
    /// The trie node to hand out once `current` runs dry.
    next: Option<usize>,
}

impl<'a, T> Iterator for PathCandidates<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        loop {
            if let Some(value) = self.current.next() {
                return Some(value);
            }
            let path = self.path;
            if let Some((_, value)) = self.regexes.find(|(regex, _)| regex.matches(path)) {
                return Some(value);
            }
            let node = self.prefixes.get(self.next.take()?)?;
            self.current = node.values.iter();
            self.next = node.parent;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::{self, PathKind as SpecKind};
    use crate::strategies::{path_near, path_pattern_text};
    use proptest::prelude::*;

    /// An entry as the tests write it; its value is its position in the list.
    type Spec = reference::PathSpec;

    fn exact(path: &str) -> Spec {
        (path.to_owned(), SpecKind::Exact)
    }

    fn regex(pattern: &str) -> Spec {
        (pattern.to_owned(), SpecKind::Regex)
    }

    fn prefix(path: &str) -> Spec {
        (path.to_owned(), SpecKind::Prefix)
    }

    fn compile((text, kind): &Spec) -> PathPattern {
        match kind {
            SpecKind::Exact => PathPattern::exact(text),
            SpecKind::Regex => PathPattern::regex(text),
            SpecKind::Prefix => PathPattern::prefix(text),
        }
        .unwrap()
    }

    fn index(specs: &[Spec]) -> PathIndex<usize> {
        PathIndex::new(specs.iter().map(compile).zip(0..))
    }

    fn lookup(index: &PathIndex<usize>, path: &str) -> Vec<usize> {
        index.lookup(path).copied().collect()
    }

    #[test]
    fn nothing_matching_means_no_candidates() {
        assert!(lookup(&index(&[]), "/").is_empty());
        let index = index(&[exact("/shop"), prefix("/api")]);
        assert!(lookup(&index, "/").is_empty());
        assert!(lookup(&index, "/shopping").is_empty());
        assert!(lookup(&index, "/apis").is_empty());
        assert!(lookup(&index, "").is_empty());
    }

    #[test]
    fn exact_match_comes_before_any_prefix() {
        let index = index(&[prefix("/"), prefix("/shop"), exact("/shop")]);
        assert_eq!(lookup(&index, "/shop"), [2, 1, 0]);
        assert_eq!(lookup(&index, "/shop/"), [1, 0]);
    }

    #[test]
    fn regex_match_comes_after_exact_and_before_any_prefix() {
        let index = index(&[
            prefix("/"),
            prefix("/users/42"),
            regex(r"/users/\d+"),
            exact("/users/42"),
        ]);
        assert_eq!(lookup(&index, "/users/42"), [3, 2, 1, 0]);
        assert_eq!(lookup(&index, "/users/7"), [2, 0]);
        assert_eq!(lookup(&index, "/users/42/edit"), [1, 0]);
    }

    #[test]
    fn regexes_that_match_keep_the_order_they_were_given_in() {
        let index = index(&[
            regex("/a.*"),
            regex("/b.*"),
            regex("/ab?c"),
            regex(".*c"),
            regex("/a.*"),
        ]);
        assert_eq!(lookup(&index, "/abc"), [0, 2, 3, 4]);
        assert_eq!(lookup(&index, "/bc"), [1, 3]);
        assert!(lookup(&index, "/x").is_empty());
    }

    #[test]
    fn longer_prefix_comes_before_shorter() {
        let index = index(&[
            prefix("/"),
            prefix("/a/b/c"),
            prefix("/a"),
            prefix("/a/b"),
            prefix("/x"),
        ]);
        assert_eq!(lookup(&index, "/a/b/c/d"), [1, 3, 2, 0]);
        assert_eq!(lookup(&index, "/a/b/c"), [1, 3, 2, 0]);
        assert_eq!(lookup(&index, "/a/b/"), [3, 2, 0]);
        assert_eq!(lookup(&index, "/a/bc"), [2, 0]);
        assert_eq!(lookup(&index, "/other"), [0]);
    }

    #[test]
    fn prefix_without_a_pattern_of_its_own_gives_nothing() {
        let index = index(&[prefix("/a/b/c")]);
        assert!(lookup(&index, "/a/b").is_empty());
        assert_eq!(lookup(&index, "/a/b/c"), [0]);
    }

    #[test]
    fn entries_with_the_same_pattern_keep_their_order() {
        let index = index(&[
            prefix("/shop"),
            exact("/shop"),
            prefix("/shop/"),
            exact("/shop"),
        ]);
        assert_eq!(lookup(&index, "/shop"), [1, 3, 0, 2]);
    }

    #[test]
    fn paths_that_are_not_absolute_have_no_candidates() {
        let index = index(&[prefix("/"), prefix("/a"), regex(".*")]);
        assert!(lookup(&index, "").is_empty());
        assert!(lookup(&index, "*").is_empty());
        assert!(lookup(&index, "a").is_empty());
    }

    #[test]
    fn paths_that_are_not_normalised_are_looked_up_as_they_are() {
        let index = index(&[prefix("/"), prefix("/a"), prefix("/a/b")]);
        assert_eq!(lookup(&index, "/a//b"), [1, 0]);
        assert_eq!(lookup(&index, "//a"), [0]);
    }

    /// Exact and prefix patterns, and regexes that say the same as one of those or match
    /// one segment of anything — so that all three kinds keep matching the same paths.
    fn spec() -> impl Strategy<Value = (Spec, String)> {
        (path_pattern_text(), 0..5).prop_map(|(path, shape)| {
            let literal = ::regex::escape(path.strip_suffix('/').unwrap_or(&path));
            let spec = match shape {
                0 => exact(&path),
                1 => prefix(&path),
                2 => regex(&::regex::escape(&path)),
                3 => regex(&format!("{literal}(?:/.*)?")),
                _ => regex(&format!("{literal}/[^/]+")),
            };
            (spec, path)
        })
    }

    /// A handful of entries and a path near one of them (or near nothing at all).
    fn case() -> impl Strategy<Value = (Vec<Spec>, String)> {
        (
            prop::collection::vec(spec(), 0..8),
            any::<prop::sample::Index>(),
        )
            .prop_flat_map(|(specs, pick)| {
                let near = if specs.is_empty() {
                    "/a/b".to_owned()
                } else {
                    pick.get(&specs).1.clone()
                };
                let path = path_near(&near);
                let specs = specs.into_iter().map(|(spec, _)| spec).collect();
                (Just(specs), path)
            })
    }

    proptest! {
        #[test]
        fn lookup_agrees_with_the_scan_everything_reference((specs, path) in case()) {
            let patterns: Vec<PathPattern> = specs.iter().map(compile).collect();
            let expected = reference::path_candidates(&specs, |n| patterns[n].matches(&path));
            prop_assert_eq!(lookup(&index(&specs), &path), expected);
        }
    }
}
