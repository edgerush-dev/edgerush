//! Routes as the model states them. These types say what was written; whether it makes
//! sense is for [`compile_routes`](crate::compile_routes) to find out.

use serde::Deserialize;

/// A route: the hosts it serves and its rules.
///
/// Routes come as an ordered list, and the order matters: it is the last tie-breaker of
/// precedence. The control plane emits them oldest first, then by name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// Unique among the routes; what errors, status and metrics refer to.
    pub name: String,
    /// The hosts served. At least one: every host has to be asked for by the name `*`.
    pub hostnames: Vec<Hostname>,
    /// The rules, in order. At least one.
    pub rules: Vec<Rule>,
}

/// A claim on hosts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hostname {
    /// An exact name, a wildcard (`*.example.com`), or `*` for every host.
    pub name: String,
    /// What the `*` of a wildcard name stands for. Required for a wildcard name; without
    /// meaning, and ignored, for any other.
    #[serde(default)]
    pub wildcard: Option<Wildcard>,
    /// Whether this claim stays a candidate on hosts that a more specific claim also
    /// covers: Gateway API requires it, nginx-style Ingress does not want it.
    pub falls_through: bool,
}

/// What the `*` of a wildcard hostname stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Wildcard {
    /// Exactly one label, as in Ingress.
    OneLabel,
    /// One or more labels, as in Gateway API.
    AnyLabels,
}

/// A rule: the requests it is for. What it does with them (filters, backends) is to come.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// The rule is for a request that satisfies any one of these. At least one.
    pub matches: Vec<Match>,
}

/// One way for a request to belong to a rule: all of what is stated here must hold.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Match {
    /// The path; `{ prefix: / }` is how to say any path.
    pub path: PathMatch,
    /// The method, in upper case, if it matters.
    #[serde(default)]
    pub method: Option<String>,
    /// Conditions on headers; names are case-insensitive.
    #[serde(default)]
    pub headers: Vec<ValuePredicate>,
    /// Conditions on decoded query parameters; names are case-sensitive.
    #[serde(default)]
    pub query: Vec<ValuePredicate>,
}

/// How a path is matched.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathMatch {
    /// This path and no other.
    Exact(String),
    /// This path and everything below it, by whole segments.
    Prefix(String),
    /// The paths this regular expression describes as a whole.
    Regex(String),
}

/// A condition on a named value: a header or a query parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValuePredicate {
    /// The header or parameter name.
    pub name: String,
    /// What its value must be.
    pub value: ValueMatch,
}

/// How a value is matched.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueMatch {
    /// This value and no other.
    Exact(String),
    /// The values this regular expression describes as a whole.
    Regex(String),
}
