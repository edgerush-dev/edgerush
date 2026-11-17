//! Gateway-first ([05 §3, §4](../../../docs/05-operator.md)): which Gateways written alone
//! get a DataPlane generated, which are refused, and which DataPlanes are the operator's
//! generated ones. No I/O.
//!
//! A Gateway of an accepted ControlPlane's class, the class the operator made for it,
//! whose `PlatformDefaults` entry there is, in the ControlPlane's watch scope, and not one
//! the operator made from a DataPlane, gets the DataPlane `edgerush-planes` generates for
//! it. One that sets `infrastructure.parametersRef` gets none, and its status says
//! `InvalidParameters`; a DataPlane it already has is left as it is, neither applied again
//! nor deleted, and serves its last config until the field is taken away. A generated
//! DataPlane whose Gateway has moved to another class is another name, so it is deleted,
//! and the Gateway gets another; one whose Gateway is deleted goes with it, by its owner
//! reference.
//!
//! A generated DataPlane is told apart by its owner, its Gateway, and its name, which that
//! Gateway's name and class make: the shipped admission policy lets no one but the
//! operator write a DataPlane that names a Gateway or a ControlPlane of another namespace,
//! so none written otherwise carries them.

use crate::allowance::{self, Labels};
use crate::status::{ACCEPTED, PROGRAMMED, differs};
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::gateway_first::{self as planes, data_plane_name};
use edgerush_planes::objects::{MANAGED_BY, MANAGED_BY_LABEL, control_plane_labels};
use edgerush_planes::platform_defaults::PlatformDefaults;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta, Time};
use std::collections::BTreeMap;

/// The reason a Gateway with parameters is not accepted for, Gateway API's own.
pub const INVALID_PARAMETERS: &str = "InvalidParameters";

/// What such a Gateway's conditions say.
const PARAMETERS: &str = "spec.infrastructure.parametersRef names no kind EdgeRush reads: a \
                          generated data plane is its class's PlatformDefaults entry and \
                          nothing else, so this Gateway gets none; one it already has is left \
                          as it is, serving its last config, until the field is taken away.";

/// What a step does for Gateway-first.
#[derive(Debug, Default)]
pub struct Generation<'a> {
    /// Each DataPlane to apply, beside the Gateway it is generated for.
    pub data_planes: Vec<(&'a Gateway, DataPlane)>,
    /// Each Gateway refused whose status does not say so yet, with the conditions it is
    /// to hold.
    pub refused: Vec<(&'a Gateway, Vec<Condition>)>,
    /// Each Gateway whose status still holds the operator's refusal though it is no longer
    /// refused: the conditions are taken back, for its controller to write.
    pub released: Vec<&'a Gateway>,
    /// Each generated DataPlane whose Gateway has moved to another class.
    pub stale: Vec<&'a DataPlane>,
    /// What could not be generated, as said.
    pub problems: Vec<String>,
}

/// What the operator makes of `gateways` for Gateway-first: from `control_planes`, each
/// beside whether it is accepted, `classes`, the cluster's GatewayClasses, `platform`, its
/// entries, and `namespaces`, each namespace's labels, which a scope selects by; the
/// generated among `data_planes` judged against them; at `now`.
#[must_use]
pub fn generation<'a>(
    gateways: &'a [Gateway],
    data_planes: &'a [DataPlane],
    control_planes: &[(&ControlPlane, bool)],
    classes: &[GatewayClass],
    platform: Option<&PlatformDefaults>,
    namespaces: &BTreeMap<String, Labels>,
    now: &Time,
) -> Generation<'a> {
    let mut generation = Generation::default();
    let entries = platform
        .and_then(|platform| platform.spec.gateway_classes.as_deref())
        .unwrap_or_default();
    for gateway in gateways {
        if gateway.metadata.deletion_timestamp.is_some() || made_from_a_data_plane(gateway) {
            continue;
        }
        let refused = gateway
            .spec
            .infrastructure
            .as_ref()
            .is_some_and(|infrastructure| infrastructure.parameters_ref.is_some());
        let generates = class_of(gateway, control_planes, classes)
            .filter(|control_plane| watches(control_plane, &gateway.metadata, namespaces))
            .and_then(|control_plane| {
                let class = &gateway.spec.gateway_class_name;
                let entry = entries.iter().find(|entry| &entry.name == class)?;
                Some((control_plane, entry))
            });
        match generates {
            Some(_) if refused => {
                let conditions = invalid_parameters(gateway, now);
                if differs(held(gateway), &conditions) {
                    generation.refused.push((gateway, conditions));
                }
            }
            Some((control_plane, entry)) => {
                match planes::data_plane(gateway, &entry.data_plane, control_plane) {
                    Ok(data_plane) => generation.data_planes.push((gateway, data_plane)),
                    Err(error) => generation
                        .problems
                        .push(format!("{}: {error}", described(&gateway.metadata))),
                }
            }
            None => {}
        }
        if !refused && holds_refusal(gateway) {
            generation.released.push(gateway);
        }
    }
    generation.stale = data_planes
        .iter()
        .filter(|data_plane| data_plane.metadata.deletion_timestamp.is_none())
        .filter(|data_plane| {
            owner(data_plane, gateways).is_some_and(|gateway| {
                let wanted = data_plane_name(
                    gateway.metadata.name.as_deref().unwrap_or_default(),
                    &gateway.spec.gateway_class_name,
                );
                data_plane.metadata.name.as_deref() != Some(wanted.as_str())
            })
        })
        .collect();
    generation
}

/// Whether `data_plane` is the one the operator generated for a Gateway of `gateways` as
/// that Gateway now is: made by it, owned by the Gateway, naming it, of its name, for the
/// ControlPlane whose class the Gateway is of. What its ControlPlane's namespace is, and
/// whether that one holds the name, the judge says, as of any DataPlane.
#[must_use]
pub fn is_generated(data_plane: &DataPlane, gateways: &[Gateway]) -> bool {
    owner(data_plane, gateways).is_some_and(|gateway| {
        let name = gateway.metadata.name.as_deref().unwrap_or_default();
        let class = &gateway.spec.gateway_class_name;
        data_plane.spec.control_plane_ref.name == *class
            && data_plane.spec.control_plane_ref.namespace.is_some()
            && data_plane.metadata.name.as_deref() == Some(data_plane_name(name, class).as_str())
    })
}

/// The Gateway of `gateways` that `data_plane`, made by the operator and naming it, is
/// owned by: of its namespace and name, and of the UID its owner reference holds.
fn owner<'a>(data_plane: &DataPlane, gateways: &'a [Gateway]) -> Option<&'a Gateway> {
    let named = data_plane.spec.gateway_ref.as_ref()?;
    let made = data_plane
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(MANAGED_BY_LABEL))
        .is_some_and(|by| by == MANAGED_BY);
    if !made {
        return None;
    }
    let uid = data_plane
        .metadata
        .owner_references
        .iter()
        .flatten()
        .find(|owner| {
            owner.kind == "Gateway"
                && owner.api_version.starts_with("gateway.networking.k8s.io/")
                && owner.controller == Some(true)
                && owner.name == named.name
        })
        .map(|owner| owner.uid.as_str())?;
    gateways.iter().find(|gateway| {
        gateway.metadata.uid.as_deref() == Some(uid)
            && gateway.metadata.namespace == data_plane.metadata.namespace
            && gateway.metadata.name.as_deref() == Some(named.name.as_str())
    })
}

/// Whether the operator made `gateway`, from a DataPlane with `gateway` (05 §3): those it
/// keeps as their DataPlane states them, never given a DataPlane of their own.
fn made_from_a_data_plane(gateway: &Gateway) -> bool {
    gateway
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(MANAGED_BY_LABEL))
        .is_some_and(|by| by == MANAGED_BY)
}

/// The accepted ControlPlane of `control_planes` whose class `gateway` is of: of the
/// class's name, the class there and carrying the labels the operator made it with for
/// that one (05 §3).
fn class_of<'a>(
    gateway: &Gateway,
    control_planes: &[(&'a ControlPlane, bool)],
    classes: &[GatewayClass],
) -> Option<&'a ControlPlane> {
    let name = &gateway.spec.gateway_class_name;
    let class = classes
        .iter()
        .find(|class| class.metadata.name.as_ref() == Some(name))?;
    let held = class.metadata.labels.clone().unwrap_or_default();
    control_planes
        .iter()
        .filter(|(_, accepted)| *accepted)
        .map(|(control_plane, _)| *control_plane)
        .find(|control_plane| {
            let metadata = &control_plane.metadata;
            let (Some(cp), Some(namespace)) = (&metadata.name, &metadata.namespace) else {
                return false;
            };
            cp == name
                && control_plane_labels(namespace, cp)
                    .iter()
                    .all(|(key, value)| held.get(key) == Some(value))
        })
}

/// Whether `control_plane` watches the namespace of the object of `metadata`, its scope as
/// it is written, its selector's by `namespaces`' labels: a Gateway beyond it is one its
/// control plane cannot read (05 §4).
fn watches(
    control_plane: &ControlPlane,
    metadata: &ObjectMeta,
    namespaces: &BTreeMap<String, Labels>,
) -> bool {
    let Some(namespace) = metadata.namespace.as_ref() else {
        return false;
    };
    let scope = &control_plane.spec.watch.namespaces;
    match scope.from.as_str() {
        "All" => true,
        "Selector" => allowance::selected(scope, namespaces).contains(namespace),
        // `Same`, and anything a newer CRD may allow that this does not know: the narrowest.
        _ => control_plane.metadata.namespace.as_ref() == Some(namespace),
    }
}

/// `gateway`'s conditions, as its status holds them.
fn held(gateway: &Gateway) -> &[Condition] {
    gateway
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_deref())
        .unwrap_or_default()
}

/// Whether `gateway`'s status holds the operator's refusal of its parameters.
fn holds_refusal(gateway: &Gateway) -> bool {
    held(gateway)
        .iter()
        .any(|condition| condition.reason == INVALID_PARAMETERS && condition.message == PARAMETERS)
}

/// The conditions `gateway`, of parameters EdgeRush does not read, is to hold: `Accepted`
/// and `Programmed` false, of its generation, each keeping the time of the one it holds
/// where that is already false, else `now`.
fn invalid_parameters(gateway: &Gateway, now: &Time) -> Vec<Condition> {
    [ACCEPTED, PROGRAMMED]
        .into_iter()
        .map(|type_| {
            let since = held(gateway)
                .iter()
                .find(|condition| condition.type_ == type_ && condition.status == "False")
                .map_or_else(
                    || now.clone(),
                    |condition| condition.last_transition_time.clone(),
                );
            Condition {
                type_: type_.to_owned(),
                status: "False".to_owned(),
                reason: INVALID_PARAMETERS.to_owned(),
                message: PARAMETERS.to_owned(),
                observed_generation: gateway.metadata.generation,
                last_transition_time: since,
            }
        })
        .collect()
}

/// An object as `<namespace>/<name>`, as said: `Gateway shop/web`.
fn described(metadata: &ObjectMeta) -> String {
    format!(
        "Gateway {}/{}",
        metadata.namespace.as_deref().unwrap_or_default(),
        metadata.name.as_deref().unwrap_or_default()
    )
}

#[cfg(test)]
mod tests;
