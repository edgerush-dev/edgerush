//! The path stage of routing: from a normalised request path to the candidates whose path
//! pattern it satisfies, in Gateway API precedence — exact matches first, then prefixes
//! from the longest to the shortest.

use crate::hash::Map;
use crate::path::{Kind, PathPattern};

/// An immutable index from request path to candidates, built once per config snapshot.
#[derive(Debug)]
pub struct PathIndex<T> {
    exact: Map<Box<[T]>>,
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
        let mut prefixes = vec![PrefixNode::new(None)];
        for (pattern, value) in entries {
            match pattern.kind {
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
            prefixes,
        }
    }

    /// The candidates for a request path, best match first: those with exactly this path,
    /// then those with a prefix of it, longest prefix first.
    ///
    /// `path` is the normalised path alone, as for [`PathPattern::matches`], and the two
    /// always agree on what matches. Never allocates.
    #[must_use]
    pub fn lookup(&self, path: &str) -> PathCandidates<'_, T> {
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
            let node = self.prefixes.get(self.next.take()?)?;
            self.current = node.values.iter();
            self.next = node.parent;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::{path_near, path_pattern_text};
    use proptest::prelude::*;
    use std::cmp::Reverse;

    /// An entry as the tests write it: pattern text and whether it is a prefix. Its value
    /// is its position in the list.
    type Spec = (String, bool);

    fn exact(path: &str) -> Spec {
        (path.to_owned(), false)
    }

    fn prefix(path: &str) -> Spec {
        (path.to_owned(), true)
    }

    fn compile((text, is_prefix): &Spec) -> PathPattern {
        if *is_prefix {
            PathPattern::prefix(text).unwrap()
        } else {
            PathPattern::exact(text).unwrap()
        }
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
        let index = index(&[prefix("/"), prefix("/a")]);
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

    /// The specification, written the slow and obvious way: scan every entry, keep those
    /// whose pattern matches, exact ones first and then the longest prefix first.
    fn reference(specs: &[Spec], path: &str) -> Vec<usize> {
        let mut candidates: Vec<(usize, &Spec)> = specs
            .iter()
            .enumerate()
            .filter(|(_, spec)| compile(spec).matches(path))
            .collect();
        candidates.sort_by_key(|(position, (text, is_prefix))| {
            let length = text.strip_suffix('/').unwrap_or(text).len();
            (*is_prefix, Reverse(length), *position)
        });
        candidates
            .into_iter()
            .map(|(position, _)| position)
            .collect()
    }

    /// A handful of entries and a path near one of them (or near nothing at all).
    fn case() -> impl Strategy<Value = (Vec<Spec>, String)> {
        let spec = (path_pattern_text(), any::<bool>());
        (
            prop::collection::vec(spec, 0..8),
            any::<prop::sample::Index>(),
        )
            .prop_flat_map(|(specs, pick)| {
                let near = if specs.is_empty() {
                    "/a/b"
                } else {
                    pick.get(&specs).0.as_str()
                };
                let path = path_near(near);
                (Just(specs), path)
            })
    }

    proptest! {
        #[test]
        fn lookup_agrees_with_the_scan_everything_reference((specs, path) in case()) {
            prop_assert_eq!(lookup(&index(&specs), &path), reference(&specs, &path));
        }
    }
}
