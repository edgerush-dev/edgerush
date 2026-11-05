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
    /// How many ListenerSets are attached to it and accepted (`attachedListenerSets`).
    pub attached_listener_sets: i32,
    /// What the status of each ListenerSet naming it is to say, in precedence.
    pub listener_sets: Vec<ListenerSetStatus>,
}

/// What a ListenerSet's status is to say: of its own listeners alone, its `Programmed`
/// that of the Gateway's config where any of them is served (GEP-1713).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerSetStatus {
    /// The ListenerSet.
    pub set: Object,
    /// The generation it says of.
    pub generation: i64,
    /// Its conditions.
    pub conditions: Vec<Condition>,
    /// Whether any of its listeners is served, what its `Programmed` waits on; or its
    /// `Programmed` where none is.
    pub programmable: Result<(), Condition>,
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
    /// The data plane's listeners it is served on, whose pods' serving them its
    /// `Programmed` waits on; or why it is not served (`Programmed` false, `Invalid`).
    pub programmable: Result<Vec<String>, String>,
}

impl fmt::Display for GatewayStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Gateway, generation {}: ", self.generation)?;
        conditions(f, &self.conditions)?;
        if !self.programmable {
            f.write_str("; nothing to program")?;
        }
        for listener in &self.listeners {
            listener_line(f, listener)?;
        }
        // Said where ListenerSets name the Gateway, so that a Gateway of none reads as ever.
        if !self.listener_sets.is_empty() {
            write!(
                f,
                "\nattached ListenerSets: {}",
                self.attached_listener_sets
            )?;
        }
        for set in &self.listener_sets {
            write!(f, "\n{}, generation {}: ", set.set, set.generation)?;
            conditions(f, &set.conditions)?;
            if let Err(programmed) = &set.programmable {
                write!(f, "; {programmed}")?;
            }
            for listener in &set.listeners {
                listener_line(f, listener)?;
            }
        }
        Ok(())
    }
}

/// A listener's status, on a line of its own.
fn listener_line(f: &mut fmt::Formatter<'_>, listener: &ListenerStatus) -> fmt::Result {
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
    Ok(())
}

/// What a route's status, an HTTPRoute's or a GRPCRoute's, is to say for each of its parents
/// that is the data plane's Gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteStatus {
    /// The route, its kind among it.
    pub route: Object,
    /// The generation it says of.
    pub generation: i64,
    /// For each such parent, in the order its `parentRefs` list them.
    pub parents: Vec<ParentStatus>,
}

/// What a route's status is to say of one of its parents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentStatus {
    /// The parent's kind: `Gateway` or `ListenerSet`.
    pub kind: &'static str,
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
            write!(f, "{}, generation {}, parent ", self.route, self.generation)?;
            if parent.kind != "Gateway" {
                write!(f, "{} ", parent.kind)?;
            }
            f.write_str(&parent.name)?;
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
/// those `routes` say are accepted, attached there as `outcomes` say; and each ListenerSet
/// naming it, of its own listeners alone, as a Gateway's is of its (GEP-1713).
pub(crate) fn gateway(
    gateway: &GatewayOutcome,
    routes: &[RouteStatus],
    outcomes: &[RouteOutcome],
) -> GatewayStatus {
    let own: Vec<&ListenerOutcome> = gateway.own().collect();
    let (accepted, listeners) = of_listeners(&own, routes, outcomes);
    let listener_sets: Vec<ListenerSetStatus> = gateway
        .sets
        .iter()
        .map(|set| {
            if !set.attached {
                let why = format!(
                    "{} takes no ListenerSet of its namespace, as its allowedListeners say",
                    gateway.gateway
                );
                return ListenerSetStatus {
                    set: set.set.clone(),
                    generation: set.generation,
                    conditions: vec![Condition::new("Accepted", false, "NotAllowed", why.clone())],
                    programmable: Err(Condition::new("Programmed", false, "NotAllowed", why)),
                    listeners: Vec::new(),
                };
            }
            let theirs: Vec<&ListenerOutcome> = gateway
                .listeners
                .iter()
                .filter(|listener| listener.set.as_ref() == Some(&set.set))
                .collect();
            let (accepted, listeners) = of_listeners(&theirs, routes, outcomes);
            let programmable = if listeners.iter().any(|l| l.programmable.is_ok()) {
                Ok(())
            } else {
                Err(Condition::new(
                    "Programmed",
                    false,
                    "ListenersNotValid",
                    "no listener is served".to_owned(),
                ))
            };
            ListenerSetStatus {
                set: set.set.clone(),
                generation: set.generation,
                conditions: vec![accepted],
                programmable,
                listeners,
            }
        })
        .collect();
    let attached = listener_sets
        .iter()
        .filter(|set| {
            set.conditions
                .iter()
                .any(|c| c.kind == "Accepted" && c.status)
        })
        .count();
    GatewayStatus {
        generation: gateway.generation,
        conditions: vec![accepted],
        programmable: listeners.iter().any(|l| l.programmable.is_ok()),
        listeners,
        attached_listener_sets: i32::try_from(attached).unwrap_or(i32::MAX),
        listener_sets,
    }
}

/// The `Accepted` of a Gateway or a ListenerSet, of `listeners`, its own, and what each of
/// them is to say.
fn of_listeners(
    listeners: &[&ListenerOutcome],
    routes: &[RouteStatus],
    outcomes: &[RouteOutcome],
) -> (Condition, Vec<ListenerStatus>) {
    let refused: Vec<&str> = listeners
        .iter()
        .filter(|listener| !listener.accepted())
        .map(|listener| listener.name.as_str())
        .collect();
    let accepted = if refused.is_empty() {
        Condition::new("Accepted", true, "Accepted", String::new())
    } else if refused.len() < listeners.len() {
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
    let statuses = listeners
        .iter()
        .map(|listener| ListenerStatus {
            name: listener.name.clone(),
            supported_kinds: if listener.accepted() {
                listener.allowed.kinds.clone()
            } else {
                Vec::new()
            },
            attached_routes: attached(listener, routes, outcomes),
            conditions: listener_conditions(listener),
            programmable: match &listener.served {
                Ok(served) => Ok(served.clone()),
                Err(problem) => Err(problem.to_string()),
            },
        })
        .collect();
    (accepted, statuses)
}

/// How many routes are accepted on `listener`: each once, however many of its parents
/// attach it there; only those accepted, as the spec has it.
fn attached(listener: &ListenerOutcome, routes: &[RouteStatus], outcomes: &[RouteOutcome]) -> i32 {
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
                        && parent.attached.as_ref().is_ok_and(|on| {
                            on.iter()
                                .any(|a| a.listener == listener.name && a.set == listener.set)
                        })
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
                ListenerProblem::HostnameConflict { .. } => "HostnameConflict",
                ListenerProblem::ProtocolConflict { .. } => "ProtocolConflict",
                ListenerProblem::PortPastSlots { .. } => "PortUnavailable",
                ListenerProblem::TooManyListeners { .. } => "TooManyListeners",
                ListenerProblem::TlsMode(_)
                | ListenerProblem::PassthroughOnly(_)
                | ListenerProblem::NoCertificate => "UnsupportedValue",
            };
            conditions.push(Condition::new(
                "Accepted",
                false,
                reason,
                problem.to_string(),
            ));
            if matches!(
                problem,
                ListenerProblem::HostnameConflict { .. } | ListenerProblem::ProtocolConflict { .. }
            ) {
                conditions.push(Condition::new(
                    "Conflicted",
                    true,
                    reason,
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
                        kind: parent.kind,
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
                NotAttached::NoMatchingParent | NotAttached::SetNotAttached => "NoMatchingParent",
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
