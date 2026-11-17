//! Which `ControlPlane`s and `DataPlane`s the operator accepts: the checks only the
//! cluster can make ([05 §1](../../../docs/05-operator.md)), what their `Accepted`
//! condition says, and what the control plane acts on. No I/O.
//!
//! A ControlPlane's name is unique in the cluster: the oldest of a name holds it, by its
//! creation time, then its UID. It may watch its own namespace, and beyond it what an
//! allowance of `PlatformDefaults` for its namespace allows ([`allowance`], 05 §4). A
//! DataPlane names a ControlPlane of its own namespace, which must hold that name; a
//! ControlPlane serves every DataPlane that does (05 §3). One that names a Gateway, or a
//! ControlPlane of another namespace, is accepted only as the DataPlane the operator
//! generates for a Gateway of that ControlPlane's class ([`gateway_first`]), of the
//! namespace it names.
//!
//! A ControlPlane refused once accepted, for an edit beyond the checks or an allowance
//! narrowed, refuses none of its DataPlanes: what was made for it is kept, but for the
//! rights beyond its own namespace, as the control plane keeps the last accepted, and it
//! would stop serving a DataPlane refused.

use crate::allowance::{self, Beyond, Labels};
use crate::gateway_first;
use edgerush_gateway_api::gateway::Gateway;
use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::platform_defaults::PlatformDefaults;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

/// Why a resource is not accepted: its `Accepted` condition's reason and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A ControlPlane whose name an older one, in another namespace, holds.
    NameTaken {
        /// That one, as `<namespace>/<name>`.
        holder: String,
    },
    /// A ControlPlane watching beyond its own namespace further than any allowance lets it.
    ScopeNotAllowed(Beyond),
    /// A DataPlane naming a ControlPlane that none of its namespace holds the name of.
    NoControlPlane {
        /// The name it names.
        name: String,
    },
    /// A DataPlane naming a Gateway, or a ControlPlane of another namespace, that is not
    /// the one the operator generates for a Gateway of that ControlPlane's class.
    NotGenerated {
        /// The ControlPlane it names, as `<namespace>/<name>`.
        control_plane: String,
    },
}

impl Refused {
    /// The condition's reason.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NameTaken { .. } => "NameTaken",
            Self::ScopeNotAllowed(_) => "ScopeNotAllowed",
            // Gateway API's reason, as for any DataPlane its ControlPlane does not serve.
            Self::NoControlPlane { .. } | Self::NotGenerated { .. } => "NoControlPlane",
        }
    }

    /// The condition's message.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::NameTaken { holder } => format!(
                "The name is held by ControlPlane {holder}, which is older: a ControlPlane's \
                 name is unique in the cluster."
            ),
            Self::ScopeNotAllowed(Beyond { asked, allowed }) if allowed.is_empty() => format!(
                "A watch scope of {asked} is beyond what is allowed: no allowance of \
                 PlatformDefaults {} is for this namespace, so a ControlPlane here may watch \
                 its own namespace alone (Same).",
                allowance::NAME
            ),
            Self::ScopeNotAllowed(Beyond { asked, allowed }) => {
                let allowed: Vec<String> = allowed.iter().map(ToString::to_string).collect();
                format!(
                    "A watch scope of {asked} is beyond what is allowed: PlatformDefaults {} \
                     lets a ControlPlane here watch its own namespace (Same), or {}; a Selector \
                     scope is within a Selector allowed where its selector carries every \
                     requirement of that one's.",
                    allowance::NAME,
                    allowed.join(", or ")
                )
            }
            Self::NoControlPlane { name } => {
                format!("No ControlPlane {name} of this namespace holds that name.")
            }
            Self::NotGenerated { control_plane } => format!(
                "Only the DataPlane the operator generates for a Gateway of the class of \
                 ControlPlane {control_plane} may name a Gateway, or that ControlPlane from \
                 another namespace, and this is not it: the operator alone writes one."
            ),
        }
    }
}

/// What the operator makes of the cluster's ControlPlanes and DataPlanes, each beside its
/// verdict, in the order given.
#[derive(Debug)]
pub struct Judged<'a> {
    /// Each ControlPlane: accepted, or why not.
    pub control_planes: Vec<(&'a ControlPlane, Result<(), Refused>)>,
    /// Each DataPlane: accepted, with the ControlPlane it is of, or why not.
    pub data_planes: Vec<(&'a DataPlane, Result<&'a ControlPlane, Refused>)>,
}

/// Judges every ControlPlane and DataPlane of the cluster, as `platform` allows, each
/// ControlPlane's namespace's labels found by its name in `namespaces`, the generated
/// DataPlanes against `gateways`, every Gateway of the cluster.
#[must_use]
pub fn judge<'a>(
    control_planes: &'a [ControlPlane],
    data_planes: &'a [DataPlane],
    platform: Option<&PlatformDefaults>,
    namespaces: &BTreeMap<String, Labels>,
    gateways: &[Gateway],
) -> Judged<'a> {
    // The holder of each name: the oldest.
    let mut holders: BTreeMap<&str, &ControlPlane> = BTreeMap::new();
    for control_plane in control_planes {
        let Some(name) = control_plane.metadata.name.as_deref() else {
            continue;
        };
        match holders.entry(name) {
            Entry::Vacant(vacant) => {
                vacant.insert(control_plane);
            }
            Entry::Occupied(mut held) => {
                if age(control_plane) < age(held.get()) {
                    held.insert(control_plane);
                }
            }
        }
    }
    let judged_control_planes = control_planes
        .iter()
        .map(|control_plane| {
            let holder = control_plane
                .metadata
                .name
                .as_deref()
                .and_then(|name| holders.get(name));
            let verdict = match holder {
                Some(holder) if !std::ptr::eq(*holder, control_plane) => Err(Refused::NameTaken {
                    holder: named(&holder.metadata),
                }),
                _ => {
                    let labels = control_plane
                        .metadata
                        .namespace
                        .as_ref()
                        .and_then(|namespace| namespaces.get(namespace));
                    allowance::allows(
                        platform,
                        labels.unwrap_or(&Labels::new()),
                        &control_plane.spec.watch.namespaces,
                    )
                    .map_err(Refused::ScopeNotAllowed)
                }
            };
            (control_plane, verdict)
        })
        .collect();

    // Each DataPlane's ControlPlane, where one of the namespace it names, its own unless it
    // names another, holds the name it names: every one that does is served. One naming a
    // Gateway or another namespace only where it is the generated one.
    let of = |data_plane: &DataPlane| -> Result<&'a ControlPlane, Refused> {
        let reference = &data_plane.spec.control_plane_ref;
        let own = data_plane.metadata.namespace.as_ref();
        let namespace = reference.namespace.as_ref().or(own);
        if (data_plane.spec.gateway_ref.is_some() || namespace != own)
            && !gateway_first::is_generated(data_plane, gateways)
        {
            return Err(Refused::NotGenerated {
                control_plane: format!(
                    "{}/{}",
                    namespace.map(String::as_str).unwrap_or_default(),
                    reference.name
                ),
            });
        }
        holders
            .get(reference.name.as_str())
            .copied()
            .filter(|holder| holder.metadata.namespace.as_ref() == namespace)
            .ok_or_else(|| Refused::NoControlPlane {
                name: reference.name.clone(),
            })
    };
    let judged_data_planes = data_planes
        .iter()
        .map(|data_plane| (data_plane, of(data_plane)))
        .collect();
    Judged {
        control_planes: judged_control_planes,
        data_planes: judged_data_planes,
    }
}

/// What orders ControlPlanes of one name: the oldest first, then by UID.
fn age(control_plane: &ControlPlane) -> (Option<&Time>, Option<&str>) {
    (
        control_plane.metadata.creation_timestamp.as_ref(),
        control_plane.metadata.uid.as_deref(),
    )
}

/// An object as `<namespace>/<name>`.
fn named(metadata: &ObjectMeta) -> String {
    format!(
        "{}/{}",
        metadata.namespace.as_deref().unwrap_or_default(),
        metadata.name.as_deref().unwrap_or_default()
    )
}

#[cfg(test)]
mod tests;
