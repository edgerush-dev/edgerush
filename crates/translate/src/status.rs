//! What the status of each Gateway API object translated is to say (04 §6): the data
//! plane's Gateway's, each of its listeners', and each HTTPRoute's for each of its parents
//! that is that Gateway. Each condition without its times, which the controller keeps as it
//! writes them, and without `Programmed`, which the data plane's acks say: only whether
//! there is anything to program.

use crate::gateway::{GatewayOutcome, ListenerOutcome, ListenerProblem};
use crate::http_route::{NotAttached, ParentOutcome, RouteOutcome};
use crate::{Finding, Object, Problem, RefProblem, RefReason};
use std::fmt;

/// A condition, as status says it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    /// Its type: `Accepted`, `ResolvedRefs`.
    pub kind: &'static str,
    /// Whether it holds.
    pub status: bool,
    /// Why, in a word of the spec's.
    pub reason: &'static str,
    /// Why, for a reader.
    pub message: String,
}

impl Condition {
    fn new(kind: &'static str, status: bool, reason: &'static str, message: String) -> Self {
        Self {
            kind,
            status,
            reason,
            message,
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = if self.status { "True" } else { "False" };
        write!(f, "{}={status}/{}", self.kind, self.reason)?;
        if self.message.is_empty() {
            Ok(())
        } else {
            write!(f, " ({})", self.message)
        }
    }
}

/// What the data plane's Gateway's status is to say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayStatus {
    /// The generation it says of.
    pub generation: i64,
    /// Its conditions.
    pub conditions: Vec<Condition>,
    /// Whether any of its listeners is served: what its `Programmed` waits on.
    pub programmable: bool,
    /// Each of its listeners', in the order it lists them.
    pub listeners: Vec<ListenerStatus>,
}

/// What a listener's status is to say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerStatus {
    /// Its name.
    pub name: String,
    /// The route kinds it takes, of the Gateway API's group.
    pub supported_kinds: Vec<&'static str>,
    /// The routes accepted on it.
    pub attached_routes: i32,
    /// Its conditions.
    pub conditions: Vec<Condition>,
    /// Whether it is served, what its `Programmed` waits on; or why not
    /// (`Programmed` false, `Invalid`).
    pub programmable: Result<(), String>,
}

impl fmt::Display for GatewayStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Gateway, generation {}: ", self.generation)?;
        conditions(f, &self.conditions)?;
        if !self.programmable {
            f.write_str("; nothing to program")?;
        }
        for listener in &self.listeners {
            let kinds = if listener.supported_kinds.is_empty() {
                "no kind".to_owned()
            } else {
                listener.supported_kinds.join(", ")
            };
            write!(
                f,
                "\nlistener {}: {kinds}, attached {}: ",
                listener.name, listener.attached_routes
            )?;
            conditions(f, &listener.conditions)?;
            if let Err(why) = &listener.programmable {
                write!(f, "; Programmed=False/Invalid ({why})")?;
            }
        }
        Ok(())
    }
}

/// What an HTTPRoute's status is to say for each of its parents that is the data plane's
/// Gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteStatus {
    /// The HTTPRoute.
    pub route: Object,
    /// The generation it says of.
    pub generation: i64,
    /// For each such parent, in the order its `parentRefs` list them.
    pub parents: Vec<ParentStatus>,
}

/// What an HTTPRoute's status is to say of one of its parents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentStatus {
    /// The parent's namespace, as its `parentRef` writes it: none for the route's own.
    pub namespace: Option<String>,
    /// The parent's name.
    pub name: String,
    /// The listener its `parentRef` names, if it names one.
    pub section_name: Option<String>,
    /// The port its `parentRef` names, if it names one.
    pub port: Option<i32>,
    /// Its conditions.
    pub conditions: Vec<Condition>,
}

impl fmt::Display for RouteStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (n, parent) in self.parents.iter().enumerate() {
            if n > 0 {
                f.write_str("\n")?;
            }
            write!(
                f,
                "{}, generation {}, parent {}",
                self.route, self.generation, parent.name
            )?;
            if let Some(section) = &parent.section_name {
                write!(f, " section {section}")?;
            }
            if let Some(port) = parent.port {
                write!(f, " port {port}")?;
            }
            f.write_str(": ")?;
            conditions(f, &parent.conditions)?;
        }
        Ok(())
    }
}

/// `conditions`, said one after another.
fn conditions(f: &mut fmt::Formatter<'_>, conditions: &[Condition]) -> fmt::Result {
    for (n, condition) in conditions.iter().enumerate() {
        if n > 0 {
            f.write_str("; ")?;
        }
        write!(f, "{condition}")?;
    }
    Ok(())
}

/// What the Gateway's status is to say, its listeners' with the routes accepted on each:
/// those `routes` say are accepted, attached there as `outcomes` say.
pub(crate) fn gateway(
    gateway: &GatewayOutcome,
    routes: &[RouteStatus],
    outcomes: &[RouteOutcome],
) -> GatewayStatus {
    let refused: Vec<&str> = gateway
        .listeners
        .iter()
        .filter(|listener| !listener.accepted())
        .map(|listener| listener.name.as_str())
        .collect();
    let accepted = if refused.is_empty() {
        Condition::new("Accepted", true, "Accepted", String::new())
    } else if refused.len() < gateway.listeners.len() {
        Condition::new(
            "Accepted",
            true,
            "ListenersNotValid",
            format!("listeners not accepted: {}", refused.join(", ")),
        )
    } else {
        Condition::new(
            "Accepted",
            false,
            "ListenersNotValid",
            "no listener is accepted".to_owned(),
        )
    };
    let listeners: Vec<ListenerStatus> = gateway
        .listeners
        .iter()
        .map(|listener| ListenerStatus {
            name: listener.name.clone(),
            supported_kinds: if listener.accepted() {
                listener.allowed.kinds.clone()
            } else {
                Vec::new()
            },
            attached_routes: attached(&listener.name, routes, outcomes),
            conditions: listener_conditions(listener),
            programmable: match &listener.served {
                Ok(_) => Ok(()),
                Err(problem) => Err(problem.to_string()),
            },
        })
        .collect();
    GatewayStatus {
        generation: gateway.generation,
        conditions: vec![accepted],
        programmable: listeners.iter().any(|l| l.programmable.is_ok()),
        listeners,
    }
}

/// How many routes are accepted on the listener `name`: each once, however many of its
/// parents attach it there; only those accepted, as the spec has it.
fn attached(name: &str, routes: &[RouteStatus], outcomes: &[RouteOutcome]) -> i32 {
    let count = outcomes
        .iter()
        .zip(routes)
        .filter(|(outcome, status)| {
            outcome
                .parents
                .iter()
                .zip(&status.parents)
                .any(|(parent, said)| {
                    accepted(said)
                        && parent
                            .attached
                            .as_ref()
                            .is_ok_and(|on| on.iter().any(|a| a.listener == name))
                })
        })
        .count();
    i32::try_from(count).unwrap_or(i32::MAX)
}

/// Whether a parent's status says the route is accepted.
fn accepted(parent: &ParentStatus) -> bool {
    parent
        .conditions
        .iter()
        .any(|c| c.kind == "Accepted" && c.status)
}

/// A listener's conditions: `Accepted`, `Conflicted` where it is, and `ResolvedRefs`.
fn listener_conditions(listener: &ListenerOutcome) -> Vec<Condition> {
    let mut conditions = Vec::new();
    match &listener.served {
        Err(problem) if !problem.accepted() => {
            let reason = match problem {
                ListenerProblem::UnsupportedProtocol(_) | ListenerProblem::NotSupportedYet(_) => {
                    "UnsupportedProtocol"
                }
                ListenerProblem::HostnameConflict(_) => "HostnameConflict",
                ListenerProblem::PortUnbound(_) | ListenerProblem::PortOtherProtocol { .. } => {
                    "PortUnavailable"
                }
                ListenerProblem::TlsMode(_) | ListenerProblem::NoCertificate => "UnsupportedValue",
            };
            conditions.push(Condition::new(
                "Accepted",
                false,
                reason,
                problem.to_string(),
            ));
            if let ListenerProblem::HostnameConflict(_) = problem {
                conditions.push(Condition::new(
                    "Conflicted",
                    true,
                    "HostnameConflict",
                    problem.to_string(),
                ));
            }
        }
        _ => conditions.push(Condition::new("Accepted", true, "Accepted", String::new())),
    }
    let mut unresolved: Vec<String> = listener
        .certificate_problems
        .iter()
        .map(|problem| problem.why.clone())
        .collect();
    if !listener.allowed.invalid_kinds.is_empty() {
        unresolved.push(format!(
            "takes no route of kind {}",
            listener.allowed.invalid_kinds.join(", ")
        ));
    }
    // A grant missing is said as such, as the spec has it, before a certificate that is
    // not there and a route kind not taken.
    let reason = if listener
        .certificate_problems
        .iter()
        .any(|problem| problem.reason == RefReason::NotPermitted)
    {
        "RefNotPermitted"
    } else if !listener.certificate_problems.is_empty() {
        "InvalidCertificateRef"
    } else if !listener.allowed.invalid_kinds.is_empty() {
        "InvalidRouteKinds"
    } else {
        "ResolvedRefs"
    };
    conditions.push(Condition::new(
        "ResolvedRefs",
        unresolved.is_empty(),
        reason,
        unresolved.join("; "),
    ));
    conditions
}

/// What each route's status is to say for its parents that are the data plane's Gateway,
/// as `report` says of it.
pub(crate) fn routes(outcomes: &[RouteOutcome], report: &[Finding]) -> Vec<RouteStatus> {
    outcomes
        .iter()
        .map(|outcome| {
            let said: Vec<&Problem> = report
                .iter()
                .filter(|finding| finding.object == outcome.route)
                .map(|finding| &finding.problem)
                .collect();
            RouteStatus {
                route: outcome.route.clone(),
                generation: outcome.generation,
                parents: outcome
                    .parents
                    .iter()
                    .map(|parent| ParentStatus {
                        namespace: parent.namespace.clone(),
                        name: parent.name.clone(),
                        section_name: parent.section_name.clone(),
                        port: parent.port,
                        conditions: parent_conditions(parent, &said),
                    })
                    .collect(),
            }
        })
        .collect()
}

/// A route's conditions for one parent: `Accepted`, `ResolvedRefs` and, where some of its
/// rules are served as written and some not, `PartiallyInvalid`.
fn parent_conditions(parent: &ParentOutcome, said: &[&Problem]) -> Vec<Condition> {
    let no_rule = said.iter().any(|p| matches!(p, Problem::NoRuleServed));
    let accepted = match parent.attached {
        Err(why) => {
            let reason = match why {
                NotAttached::NoMatchingParent => "NoMatchingParent",
                NotAttached::NotAllowedByListeners => "NotAllowedByListeners",
                NotAttached::NoMatchingListenerHostname => "NoMatchingListenerHostname",
            };
            Condition::new("Accepted", false, reason, why.to_string())
        }
        Ok(_) if no_rule => Condition::new(
            "Accepted",
            false,
            "UnsupportedValue",
            Problem::NoRuleServed.to_string(),
        ),
        Ok(_) => Condition::new("Accepted", true, "Accepted", String::new()),
    };
    let unresolved: Vec<&RefProblem> = said
        .iter()
        .filter_map(|p| match p {
            Problem::BackendInvalid { why, .. } | Problem::MirrorLeftOff { why, .. } => Some(why),
            _ => None,
        })
        .collect();
    // The reason of the gravest, a grant missing first, as a listener says it; every one
    // in the message.
    let gravest = [
        (RefReason::NotPermitted, "RefNotPermitted"),
        (RefReason::InvalidKind, "InvalidKind"),
        (RefReason::NotFound, "BackendNotFound"),
        (RefReason::UnsupportedProtocol, "UnsupportedProtocol"),
        (RefReason::Unsupported, "UnsupportedValue"),
    ]
    .into_iter()
    .find(|(reason, _)| unresolved.iter().any(|p| p.reason == *reason));
    let resolved = match gravest {
        None => Condition::new("ResolvedRefs", true, "ResolvedRefs", String::new()),
        Some((_, reason)) => Condition::new(
            "ResolvedRefs",
            false,
            reason,
            unresolved
                .iter()
                .map(|p| p.why.as_str())
                .collect::<Vec<_>>()
                .join("; "),
        ),
    };
    let mut dropped: Vec<(usize, String)> = said
        .iter()
        .filter_map(|p| match p {
            Problem::RuleAnswered { rule, .. } | Problem::RuleLeftOut { rule, .. } => {
                Some((*rule, p.to_string()))
            }
            _ => None,
        })
        .collect();
    dropped.sort_by_key(|(rule, _)| *rule);
    let dropped: Vec<String> = dropped.into_iter().map(|(_, said)| said).collect();
    let partly = accepted.status && !dropped.is_empty();
    let mut conditions = vec![accepted, resolved];
    // Set only while true, as the spec has it; `Dropped Rule` the prefix it asks of rules
    // not served as written, here answered 500 or left out (04 §2).
    if partly {
        conditions.push(Condition::new(
            "PartiallyInvalid",
            true,
            "UnsupportedValue",
            format!("Dropped Rule: {}", dropped.join("; ")),
        ));
    }
    conditions
}
