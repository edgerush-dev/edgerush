//! What a translation says of the objects with no status to say it in, which the control
//! plane writes as Events on them ([04 §6](../../../docs/04-control-plane.md)): everything
//! it says of an Ingress, whose status holds addresses alone; a Service served unprotected,
//! whose status is not the control plane's; and a RateLimitPolicy none of whose targets is
//! there, which has no ancestor to be written under. An object with conditions of the
//! control plane's has them say it, and is told nothing here.

use crate::{Finding, Object, Problem, TlsProblem, Translation};
use std::collections::BTreeSet;
use std::fmt;

/// One thing a translation says of an object, as an Event on it says it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Told {
    /// The object.
    pub object: Object,
    /// Whether it is a fault, a `Warning`; else a note on what is served as the spec has
    /// it ([`Problem::is_note`]), `Normal`.
    pub warning: bool,
    /// What it is, in a word: `NotSupportedYet`, `Unresolved`, `TlsUnsecured`.
    pub reason: &'static str,
    /// What it says, without the object, which the Event names.
    pub note: String,
}

impl fmt::Display for Told {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.warning { "Warning" } else { "Normal" };
        write!(f, "{kind} {} {}: {}", self.object, self.reason, self.note)
    }
}

impl Translation {
    /// What it says of the objects with no status to say it in, each once, by object.
    #[must_use]
    pub fn told(&self) -> Vec<Told> {
        let mut told = BTreeSet::new();
        for finding in self.report.iter().filter(|finding| told_of(finding)) {
            told.insert(Told {
                object: finding.object.clone(),
                warning: !finding.problem.is_note(),
                reason: finding.problem.name(),
                note: finding.problem.to_string(),
            });
        }
        for unresolved in &self.unresolved {
            told.insert(fault(&unresolved.object, "Unresolved", unresolved));
        }
        for tls in &self.tls {
            told.insert(fault(&tls.object, tls.problem.name(), tls));
        }
        for shadowed in self
            .shadowed
            .iter()
            .filter(|shadowed| is_ingress(&shadowed.object))
        {
            told.insert(fault(&shadowed.object, "Shadowed", shadowed));
        }
        for taken in &self.taken {
            if let Some(object) = ingress_of(&taken.from) {
                told.insert(Told {
                    object,
                    warning: true,
                    reason: "HostTaken",
                    note: taken.to_string(),
                });
            }
        }
        told.into_iter().collect()
    }
}

/// Whether `finding` is told: of an Ingress or a Service, or a RateLimitPolicy's of none of
/// its targets there.
fn told_of(finding: &Finding) -> bool {
    matches!(finding.object.kind, "Ingress" | "Service")
        || matches!(finding.problem, Problem::LimitTargetsNoneThere)
}

fn is_ingress(object: &Object) -> bool {
    object.kind == "Ingress"
}

/// A fault of `object`, said as `said` says it, without the object it starts with.
fn fault(object: &Object, reason: &'static str, said: &dyn fmt::Display) -> Told {
    let said = said.to_string();
    let note = said
        .strip_prefix(&format!("{object}: "))
        .map_or_else(|| said.clone(), str::to_owned);
    Told {
        object: object.clone(),
        warning: true,
        reason,
        note,
    }
}

/// The Ingress a config route is of, by its name (`ingress/<namespace>/<name>/…`); none for
/// a route of the Gateway API.
fn ingress_of(route: &str) -> Option<Object> {
    let mut parts = route.splitn(4, '/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("ingress"), Some(namespace), Some(name)) => Some(Object {
            kind: "Ingress",
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }),
        _ => None,
    }
}

impl Problem {
    /// Its name, as an Event's `reason`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::NotSupportedYet { .. } => "NotSupportedYet",
            Self::Invalid { .. } => "Invalid",
            Self::NeverServed { .. } => "NeverServed",
            Self::NotBoolean { .. } => "NotBoolean",
            Self::Refused { .. } => "Refused",
            Self::HttpPortTaken { .. } => "HttpPortTaken",
            Self::ClassMissing => "ClassMissing",
            Self::ClassNotOurs { .. } => "ClassNotOurs",
            Self::GatewayMissing => "GatewayMissing",
            Self::ListenerSetNotAllowed { .. } => "ListenerSetNotAllowed",
            Self::GatewayClassMissing => "GatewayClassMissing",
            Self::GatewayClassNotOurs { .. } => "GatewayClassNotOurs",
            Self::GatewayClassNotMade => "GatewayClassNotMade",
            Self::Listener { .. } => "Listener",
            Self::NoListenerServed => "NoListenerServed",
            Self::NotAttached { .. } => "NotAttached",
            Self::RuleAnswered { .. } => "RuleAnswered",
            Self::RuleLeftOut { .. } => "RuleLeftOut",
            Self::NoRuleServed => "NoRuleServed",
            Self::BackendInvalid { .. } => "BackendInvalid",
            Self::RouteRefused { .. } => "RouteRefused",
            Self::ConnectionsClosed { .. } => "ConnectionsClosed",
            Self::DatagramsDropped { .. } => "DatagramsDropped",
            Self::PolicyConflicted { .. } => "PolicyConflicted",
            Self::PolicyTargetNotFound { .. } => "PolicyTargetNotFound",
            Self::PolicyFailing { .. } => "PolicyFailing",
            Self::Unprotected { .. } => "Unprotected",
            Self::LimitNameHeld { .. } => "LimitNameHeld",
            Self::LimitPolicyUnimplementable { .. } => "LimitPolicyUnimplementable",
            Self::LimitTargetNotFound { .. } => "LimitTargetNotFound",
            Self::LimitTargetsNoneThere => "LimitTargetsNoneThere",
            Self::LimitNotForwarding { .. } => "LimitNotForwarding",
            Self::LastAcceptedApplies { .. } => "LastAcceptedApplies",
            Self::LimitsTooMany { .. } => "LimitsTooMany",
            Self::LimitsMany { .. } => "LimitsMany",
            Self::HostnameTaken { .. } => "HostnameTaken",
            Self::ListenerTaken { .. } => "ListenerTaken",
            Self::MirrorLeftOff { .. } => "MirrorLeftOff",
            Self::RetryCapped { .. } => "RetryCapped",
            Self::ListenerCertificate { .. } => "ListenerCertificate",
            Self::ClientAuthority { .. } => "ClientAuthority",
            Self::ClientCertificate { .. } => "ClientCertificate",
            Self::HostOverTls { .. } => "HostOverTls",
            Self::Http3GivenWay { .. } => "Http3GivenWay",
            Self::ListenerKinds { .. } => "ListenerKinds",
            Self::ListenerNamespaces { .. } => "ListenerNamespaces",
        }
    }
}

impl TlsProblem {
    /// Its name, as an Event's `reason`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::WithoutHosts => "TlsWithoutHosts",
            Self::ListedAgain { .. } => "TlsListedAgain",
            Self::Unsecured { .. } => "TlsUnsecured",
            Self::PassedOver { .. } => "TlsPassedOver",
            Self::PassedThrough { .. } => "TlsPassedThrough",
            Self::ClientsValidated { .. } => "TlsClientsValidated",
            Self::Taken { .. } => "TlsTaken",
        }
    }
}
