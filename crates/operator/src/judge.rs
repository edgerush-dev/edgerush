//! Which `ControlPlane`s and `DataPlane`s the operator accepts: the checks only the
//! cluster can make ([05 §1](../../../docs/05-operator.md)), what their `Accepted`
//! condition says, and what the control plane acts on. No I/O.
//!
//! A ControlPlane's name is unique in the cluster: the oldest of a name holds it, by its
//! creation time, then its UID. Until the allowance of `PlatformDefaults` (05 §4), it may
//! watch its own namespace alone. A DataPlane names a ControlPlane of its own namespace,
//! which must hold that name; and of one ControlPlane's DataPlanes the oldest alone is
//! served, by its creation time, then its name, as the control plane chooses (05 §3).
//!
//! A ControlPlane refused for an edit beyond the checks refuses none of its DataPlanes:
//! what was made for it is kept, as the control plane keeps the last accepted, and it
//! would stop serving a DataPlane refused.

use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::data_plane::DataPlane;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

/// The one watch scope accepted while no allowance says more: the ControlPlane's own
/// namespace.
const SAME: &str = "Same";

/// Why a resource is not accepted: its `Accepted` condition's reason and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A ControlPlane whose name an older one, in another namespace, holds.
    NameTaken {
        /// That one, as `<namespace>/<name>`.
        holder: String,
    },
    /// A ControlPlane watching beyond its own namespace, which no allowance lets it.
    ScopeNotAllowed {
        /// The scope it asks for.
        from: String,
    },
    /// A DataPlane of a ControlPlane an older DataPlane is served by.
    OneDataPlaneOnly {
        /// That DataPlane's name.
        holder: String,
    },
    /// A DataPlane naming a ControlPlane that none of its namespace holds the name of.
    NoControlPlane {
        /// The name it names.
        name: String,
    },
}

impl Refused {
    /// The condition's reason.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NameTaken { .. } => "NameTaken",
            Self::ScopeNotAllowed { .. } => "ScopeNotAllowed",
            Self::OneDataPlaneOnly { .. } => "OneDataPlaneOnly",
            Self::NoControlPlane { .. } => "NoControlPlane",
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
            Self::ScopeNotAllowed { from } => format!(
                "A watch scope of {from} is beyond what is allowed: a ControlPlane may watch \
                 its own namespace alone (Same)."
            ),
            Self::OneDataPlaneOnly { holder } => format!(
                "DataPlane {holder}, which is older, is its ControlPlane's: a ControlPlane \
                 serves one DataPlane."
            ),
            Self::NoControlPlane { name } => {
                format!("No ControlPlane {name} of this namespace holds that name.")
            }
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

/// Judges every ControlPlane and DataPlane of the cluster.
#[must_use]
pub fn judge<'a>(control_planes: &'a [ControlPlane], data_planes: &'a [DataPlane]) -> Judged<'a> {
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
                _ if control_plane.spec.watch.namespaces.from != SAME => {
                    Err(Refused::ScopeNotAllowed {
                        from: control_plane.spec.watch.namespaces.from.clone(),
                    })
                }
                _ => Ok(()),
            };
            (control_plane, verdict)
        })
        .collect();

    // Each DataPlane's ControlPlane, where one of its namespace holds the name it names.
    let of = |data_plane: &DataPlane| -> Result<&'a ControlPlane, Refused> {
        let name = &data_plane.spec.control_plane_ref.name;
        holders
            .get(name.as_str())
            .copied()
            .filter(|holder| holder.metadata.namespace == data_plane.metadata.namespace)
            .ok_or_else(|| Refused::NoControlPlane { name: name.clone() })
    };
    // The DataPlane each ControlPlane serves, by the name it holds: its oldest.
    let mut served: BTreeMap<&str, &DataPlane> = BTreeMap::new();
    for data_plane in data_planes {
        if of(data_plane).is_ok() {
            match served.entry(data_plane.spec.control_plane_ref.name.as_str()) {
                Entry::Vacant(vacant) => {
                    vacant.insert(data_plane);
                }
                Entry::Occupied(mut held) => {
                    if data_plane_age(data_plane) < data_plane_age(held.get()) {
                        held.insert(data_plane);
                    }
                }
            }
        }
    }
    let judged_data_planes = data_planes
        .iter()
        .map(|data_plane| {
            let verdict = of(data_plane).and_then(|control_plane| {
                match served.get(data_plane.spec.control_plane_ref.name.as_str()) {
                    Some(holder) if !std::ptr::eq(*holder, data_plane) => {
                        Err(Refused::OneDataPlaneOnly {
                            holder: holder.metadata.name.clone().unwrap_or_default(),
                        })
                    }
                    _ => Ok(control_plane),
                }
            });
            (data_plane, verdict)
        })
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

/// What orders the DataPlanes of one ControlPlane: the oldest first, then by name, as the
/// control plane orders them.
fn data_plane_age(data_plane: &DataPlane) -> (Option<&Time>, Option<&str>) {
    (
        data_plane.metadata.creation_timestamp.as_ref(),
        data_plane.metadata.name.as_deref(),
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
