//! What a `ControlPlane`'s or `DataPlane`'s conditions say
//! ([05 §1](../../../docs/05-operator.md)), as a Gateway's do: `Accepted`, the checks only
//! the cluster can make, which the control plane acts on; and `Programmed`, what was made.
//! Each is of the generation it was set on, and keeps the time it last changed from true to
//! false or back. No I/O.

use crate::judge::Refused;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};

/// The condition saying whether the operator accepted a resource.
pub const ACCEPTED: &str = "Accepted";
/// The condition saying whether what a resource asks for is made.
pub const PROGRAMMED: &str = "Programmed";

/// How far what a resource asks for is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Made {
    /// Nothing of this generation yet, as said.
    Pending(String),
    /// Every object applied.
    Applied,
    /// Objects of names it would use that are not the operator's, so left alone, each as
    /// `<Kind> <namespace>/<name>`; the rest applied.
    NotOurs(Vec<String>),
    /// An object the API server refused, as said; applied again.
    Refused(String),
}

/// The conditions of a resource at `generation`, judged `accepted`, `made` so far, whose
/// workload, Deployment `workload`, has a replica available or not; each keeping the time
/// of `existing`'s of its type where its status is the same, else `now`.
#[must_use]
pub fn conditions(
    generation: Option<i64>,
    accepted: Result<(), &Refused>,
    made: &Made,
    workload: &str,
    available: bool,
    existing: &[Condition],
    now: &Time,
) -> Vec<Condition> {
    let condition = |type_: &str, status: bool, reason: &str, message: String| {
        let status = if status { "True" } else { "False" };
        let since = existing
            .iter()
            .find(|condition| condition.type_ == type_ && condition.status == status)
            .map_or_else(
                || now.clone(),
                |condition| condition.last_transition_time.clone(),
            );
        Condition {
            type_: type_.to_owned(),
            status: status.to_owned(),
            reason: reason.to_owned(),
            message,
            observed_generation: generation,
            last_transition_time: since,
        }
    };
    let accepted_condition = match accepted {
        Ok(()) => condition(ACCEPTED, true, "Accepted", "Accepted.".to_owned()),
        Err(refused) => condition(ACCEPTED, false, refused.reason(), refused.message()),
    };
    let programmed = match (accepted, made) {
        (Err(_), _) => condition(
            PROGRAMMED,
            false,
            "Pending",
            "Nothing of this generation is made, as it is not accepted.".to_owned(),
        ),
        (Ok(()), Made::Pending(why)) => condition(PROGRAMMED, false, "Pending", why.clone()),
        (Ok(()), Made::NotOurs(names)) => condition(
            PROGRAMMED,
            false,
            "NotOurs",
            format!("Not the operator's, so left alone: {}.", names.join(", ")),
        ),
        (Ok(()), Made::Refused(why)) => condition(PROGRAMMED, false, "ApplyRefused", why.clone()),
        (Ok(()), Made::Applied) if available => condition(
            PROGRAMMED,
            true,
            "Programmed",
            format!("Made, and a replica of Deployment {workload} is available."),
        ),
        (Ok(()), Made::Applied) => condition(
            PROGRAMMED,
            false,
            "Pending",
            format!("Made; no replica of Deployment {workload} is available yet."),
        ),
    };
    vec![accepted_condition, programmed]
}

/// Whether `wanted` says anything `existing` does not: a condition of a type it lacks, or
/// one that differs but for its time.
#[must_use]
pub fn differs(existing: &[Condition], wanted: &[Condition]) -> bool {
    wanted.iter().any(|wanted| {
        !existing.iter().any(|existing| {
            existing.type_ == wanted.type_
                && existing.status == wanted.status
                && existing.reason == wanted.reason
                && existing.message == wanted.message
                && existing.observed_generation == wanted.observed_generation
        })
    })
}

#[cfg(test)]
mod tests;
