//! The GatewayClasses naming EdgeRush's controller that no ControlPlane made
//! ([05 §3](../../../docs/05-operator.md)), such as the conformance suite's own: refused,
//! `Accepted` false of reason `Unsupported`, Gateway API's for an implementation that takes
//! no user-defined class, of each one's generation, and again as it changes. A class is a
//! ControlPlane's by the labels the operator makes it with, of the ControlPlane's name and
//! namespace: one of a ControlPlane's name that it did not make, left alone and said on
//! the ControlPlane's status (05 §2), is refused too, and its control plane takes it no
//! more than the operator does. Classes of any other controller are not looked at. No I/O.

use crate::status::{ACCEPTED, differs};
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::objects::{GATEWAY_CONTROLLER, control_plane_labels};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};

/// The reason a class no ControlPlane made is not accepted for.
pub const UNSUPPORTED: &str = "Unsupported";

/// What a refused class's `Accepted` says.
const MESSAGE: &str = "No ControlPlane made this GatewayClass: EdgeRush serves the class each \
                       ControlPlane makes, of the ControlPlane's name, and no other.";

/// A class refused whose status does not say so yet.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal<'a> {
    /// The class's name.
    pub name: &'a str,
    /// The `Accepted` condition its status is to hold.
    pub accepted: Condition,
}

/// Whether `class` names EdgeRush's controller and carries the labels of no ControlPlane of
/// `control_planes`, as the operator makes each one's class.
#[must_use]
pub fn refused(class: &GatewayClass, control_planes: &[ControlPlane]) -> bool {
    let held = class.metadata.labels.as_ref();
    class.spec.controller_name == GATEWAY_CONTROLLER
        && !control_planes.iter().any(|control_plane| {
            let metadata = &control_plane.metadata;
            let (Some(name), Some(namespace)) = (&metadata.name, &metadata.namespace) else {
                return false;
            };
            control_plane_labels(namespace, name)
                .iter()
                .all(|(key, value)| held.and_then(|held| held.get(key)) == Some(value))
        })
}

/// The `Accepted` condition `class`, refused, is to hold: of its generation, keeping the time
/// of the one it holds where that is already false, else `now`.
#[must_use]
pub fn accepted(class: &GatewayClass, now: &Time) -> Condition {
    let since = class
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_deref())
        .unwrap_or_default()
        .iter()
        .find(|condition| condition.type_ == ACCEPTED && condition.status == "False")
        .map_or_else(
            || now.clone(),
            |condition| condition.last_transition_time.clone(),
        );
    Condition {
        type_: ACCEPTED.to_owned(),
        status: "False".to_owned(),
        reason: UNSUPPORTED.to_owned(),
        message: MESSAGE.to_owned(),
        observed_generation: class.metadata.generation,
        last_transition_time: since,
    }
}

/// Each of `classes` [`refused`] by `control_planes` whose status does not yet say what
/// [`accepted`] has it say at `now`.
#[must_use]
pub fn refusals<'a>(
    classes: &'a [GatewayClass],
    control_planes: &[ControlPlane],
    now: &Time,
) -> Vec<Refusal<'a>> {
    classes
        .iter()
        .filter(|class| refused(class, control_planes))
        .filter_map(|class| {
            let name = class.metadata.name.as_deref()?;
            let accepted = accepted(class, now);
            let held = class
                .status
                .as_ref()
                .and_then(|status| status.conditions.as_deref())
                .unwrap_or_default();
            differs(held, std::slice::from_ref(&accepted)).then_some(Refusal { name, accepted })
        })
        .collect()
}

#[cfg(test)]
mod tests;
