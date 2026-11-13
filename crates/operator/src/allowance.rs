//! What `PlatformDefaults` allows a ControlPlane to watch
//! ([05 §4](../../../docs/05-operator.md)). No I/O.
//!
//! An allowance picks the ControlPlanes of the namespaces its selector matches, and lets
//! them watch every namespace (`All`) or those a selector matches. A ControlPlane's own
//! namespace needs none. A `Selector` scope is judged by its construction, not by the
//! namespaces it happens to select: it is within a selector allowance only where it carries
//! every one of the allowance's requirements, so it can select no namespace beyond it,
//! however namespaces are labelled later. `matchLabels` count as `In` of one value.

use edgerush_planes::control_plane::{
    ControlPlaneWatchNamespaces, ControlPlaneWatchNamespacesSelector,
};
use edgerush_planes::platform_defaults::{
    PlatformDefaults, PlatformDefaultsWatchScopesControlPlanesNamespaceSelector,
    PlatformDefaultsWatchScopesNamespaces, PlatformDefaultsWatchScopesNamespacesSelector,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The one `PlatformDefaults` read: any other name is refused as it is applied.
pub const NAME: &str = "default";

/// A namespace's labels.
pub type Labels = BTreeMap<String, String>;

/// One requirement of a label selector.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Requirement {
    In(String, BTreeSet<String>),
    NotIn(String, BTreeSet<String>),
    Exists(String),
    DoesNotExist(String),
}

impl Requirement {
    fn matches(&self, labels: &Labels) -> bool {
        match self {
            Self::In(key, values) => labels.get(key).is_some_and(|value| values.contains(value)),
            Self::NotIn(key, values) => labels.get(key).is_none_or(|value| !values.contains(value)),
            Self::Exists(key) => labels.contains_key(key),
            Self::DoesNotExist(key) => !labels.contains_key(key),
        }
    }
}

impl fmt::Display for Requirement {
    /// As `kubectl` writes a selector: `team=payments`, `tier in (a,b)`, `!legacy`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = |values: &BTreeSet<String>| {
            values
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(",")
        };
        match self {
            Self::In(key, one) if one.len() == 1 => write!(f, "{key}={}", values(one)),
            Self::In(key, many) => write!(f, "{key} in ({})", values(many)),
            Self::NotIn(key, many) => write!(f, "{key} notin ({})", values(many)),
            Self::Exists(key) => write!(f, "{key}"),
            Self::DoesNotExist(key) => write!(f, "!{key}"),
        }
    }
}

/// A label selector, read: its requirements, or `None` where one names an operator
/// Kubernetes does not have, which selects nothing and is within nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector(Option<BTreeSet<Requirement>>);

impl Selector {
    /// A selector of `labels` and `expressions`, each `(key, operator, values)`.
    fn read<'a>(
        labels: Option<&'a BTreeMap<String, String>>,
        expressions: impl Iterator<Item = (&'a str, &'a str, Option<&'a Vec<String>>)>,
    ) -> Self {
        let mut requirements: BTreeSet<Requirement> = labels
            .into_iter()
            .flatten()
            .map(|(key, value)| Requirement::In(key.clone(), BTreeSet::from([value.clone()])))
            .collect();
        for (key, operator, values) in expressions {
            let (key, values) = (
                key.to_owned(),
                values.into_iter().flatten().cloned().collect(),
            );
            requirements.insert(match operator {
                "In" => Requirement::In(key, values),
                "NotIn" => Requirement::NotIn(key, values),
                "Exists" => Requirement::Exists(key),
                "DoesNotExist" => Requirement::DoesNotExist(key),
                _ => return Self(None),
            });
        }
        Self(Some(requirements))
    }

    /// Whether it selects a namespace of `labels`.
    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        self.0
            .as_ref()
            .is_some_and(|requirements| requirements.iter().all(|r| r.matches(labels)))
    }

    /// Whether it carries every requirement of `allowed`, and so selects nothing beyond it.
    fn within(&self, allowed: &Self) -> bool {
        match (&self.0, &allowed.0) {
            (Some(asked), Some(allowed)) => allowed.is_subset(asked),
            _ => false,
        }
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            None => f.write_str("an operator Kubernetes does not have"),
            Some(requirements) if requirements.is_empty() => f.write_str("every namespace"),
            Some(requirements) => {
                let each: Vec<String> = requirements.iter().map(ToString::to_string).collect();
                f.write_str(&each.join(","))
            }
        }
    }
}

impl From<&ControlPlaneWatchNamespacesSelector> for Selector {
    fn from(written: &ControlPlaneWatchNamespacesSelector) -> Self {
        Self::read(
            written.match_labels.as_ref(),
            written
                .match_expressions
                .iter()
                .flatten()
                .map(|e| (e.key.as_str(), e.operator.as_str(), e.values.as_ref())),
        )
    }
}

impl From<&PlatformDefaultsWatchScopesControlPlanesNamespaceSelector> for Selector {
    fn from(written: &PlatformDefaultsWatchScopesControlPlanesNamespaceSelector) -> Self {
        Self::read(
            written.match_labels.as_ref(),
            written
                .match_expressions
                .iter()
                .flatten()
                .map(|e| (e.key.as_str(), e.operator.as_str(), e.values.as_ref())),
        )
    }
}

impl From<&PlatformDefaultsWatchScopesNamespacesSelector> for Selector {
    fn from(written: &PlatformDefaultsWatchScopesNamespacesSelector) -> Self {
        Self::read(
            written.match_labels.as_ref(),
            written
                .match_expressions
                .iter()
                .flatten()
                .map(|e| (e.key.as_str(), e.operator.as_str(), e.values.as_ref())),
        )
    }
}

/// A watch scope, read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Its own namespace alone.
    Same,
    /// Every namespace.
    All,
    /// The namespaces a selector selects.
    Selector(Selector),
    /// A `from` this does not know, or `Selector` with no selector: within nothing.
    Unknown(String),
}

impl Scope {
    /// A ControlPlane's scope, as written.
    #[must_use]
    pub fn of(written: &ControlPlaneWatchNamespaces) -> Self {
        match (written.from.as_str(), written.selector.as_ref()) {
            ("Same", _) => Self::Same,
            ("All", _) => Self::All,
            ("Selector", Some(selector)) => Self::Selector(selector.into()),
            (other, _) => Self::Unknown(other.to_owned()),
        }
    }

    /// What an allowance lets the ControlPlanes it picks watch.
    fn allowed(written: &PlatformDefaultsWatchScopesNamespaces) -> Self {
        match (written.from.as_str(), written.selector.as_ref()) {
            ("All", _) => Self::All,
            ("Selector", Some(selector)) => Self::Selector(selector.into()),
            (other, _) => Self::Unknown(other.to_owned()),
        }
    }

    /// Whether this scope, asked for, is within `allowed`.
    fn within(&self, allowed: &Self) -> bool {
        match (self, allowed) {
            (Self::Same, _) | (_, Self::All) => true,
            (Self::Selector(asked), Self::Selector(allowed)) => asked.within(allowed),
            _ => false,
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Same => f.write_str("Same"),
            Self::All => f.write_str("All"),
            Self::Selector(selector) => write!(f, "Selector of {selector}"),
            Self::Unknown(from) => write!(f, "{from}"),
        }
    }
}

/// What a ControlPlane asks beyond what it is allowed, refused: the scope it asks for and
/// every one allowed in its namespace, besides its own namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Beyond {
    /// The scope it asks for.
    pub asked: Scope,
    /// The scopes the allowances that pick it allow, in the order written.
    pub allowed: Vec<Scope>,
}

/// Whether a ControlPlane of `namespace`, its labels `labels`, may watch `asked`, as
/// `platform` allows: `Err` says what it asks beyond.
///
/// # Errors
///
/// [`Beyond`], for a scope no allowance that picks it allows.
pub fn allows(
    platform: Option<&PlatformDefaults>,
    labels: &Labels,
    asked: &ControlPlaneWatchNamespaces,
) -> Result<(), Beyond> {
    let asked = Scope::of(asked);
    if asked == Scope::Same {
        return Ok(());
    }
    let allowed: Vec<Scope> = platform
        .filter(|platform| platform.metadata.name.as_deref() == Some(NAME))
        .and_then(|platform| platform.spec.watch_scopes.as_ref())
        .into_iter()
        .flatten()
        .filter(|scope| Selector::from(&scope.control_planes.namespace_selector).matches(labels))
        .map(|scope| Scope::allowed(&scope.namespaces))
        .collect();
    if allowed.iter().any(|allowed| asked.within(allowed)) {
        Ok(())
    } else {
        Err(Beyond { asked, allowed })
    }
}

/// The namespaces of `namespaces` a ControlPlane of scope `asked` selects, by name: empty
/// for any scope but `Selector`.
#[must_use]
pub fn selected(
    asked: &ControlPlaneWatchNamespaces,
    namespaces: &BTreeMap<String, Labels>,
) -> BTreeSet<String> {
    let Scope::Selector(selector) = Scope::of(asked) else {
        return BTreeSet::new();
    };
    namespaces
        .iter()
        .filter(|(_, labels)| selector.matches(labels))
        .map(|(name, _)| name.clone())
        .collect()
}

#[cfg(test)]
mod tests;
