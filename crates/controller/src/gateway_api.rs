//! Which of the Gateway API's kinds the cluster serves at the version EdgeRush reads. Once
//! the data plane names a Gateway, each kind's CRD is followed by its name, and the kind
//! itself only while its CRD is there and serves `v1`: one an older install lacks, or
//! serves at another version alone, is taken as having no objects, and said, never waited
//! on ([04 §1](../../../docs/04-control-plane.md)).

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use std::fmt;

/// The version of each Gateway API kind that EdgeRush reads.
pub const VERSION: &str = "v1";

/// The annotation on a Gateway API CRD that names the release it is from.
pub const RELEASE: &str = "gateway.networking.k8s.io/bundle-version";

/// The Gateway API minor releases supported: the latest two (05 §6).
pub const SUPPORTED: [&str; 2] = ["v1.6", "v1.7"];

/// A Gateway API kind the control plane follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GatewayKind {
    /// `GatewayClass`.
    GatewayClass,
    /// `Gateway`.
    Gateway,
    /// `HTTPRoute`.
    HTTPRoute,
    /// `ReferenceGrant`.
    ReferenceGrant,
}

impl GatewayKind {
    /// Every kind followed, in the order said.
    pub const ALL: [Self; 4] = [
        Self::GatewayClass,
        Self::Gateway,
        Self::HTTPRoute,
        Self::ReferenceGrant,
    ];

    /// The name of its CRD.
    #[must_use]
    pub fn crd(self) -> &'static str {
        match self {
            Self::GatewayClass => "gatewayclasses.gateway.networking.k8s.io",
            Self::Gateway => "gateways.gateway.networking.k8s.io",
            Self::HTTPRoute => "httproutes.gateway.networking.k8s.io",
            Self::ReferenceGrant => "referencegrants.gateway.networking.k8s.io",
        }
    }
}

/// What the cluster has of a kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Served {
    /// Its CRD serves [`VERSION`]: from the release it names, if it names one.
    Served {
        /// The release, `v1.7.0`.
        release: Option<String>,
    },
    /// Its CRD is there, but does not serve [`VERSION`]: an older release's.
    OtherVersions {
        /// The versions it does serve.
        versions: Vec<String>,
        /// The release, if it names one.
        release: Option<String>,
    },
    /// It has no CRD.
    Missing,
}

impl Served {
    /// What the CRDs listed of the kind's name, none or one, say.
    #[must_use]
    pub fn of(crds: &[CustomResourceDefinition]) -> Self {
        let Some(crd) = crds.first() else {
            return Self::Missing;
        };
        let release = crd
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(RELEASE))
            .cloned();
        let versions: Vec<String> = crd
            .spec
            .versions
            .iter()
            .filter(|version| version.served)
            .map(|version| version.name.clone())
            .collect();
        if versions.iter().any(|version| version == VERSION) {
            Self::Served { release }
        } else {
            Self::OtherVersions { versions, release }
        }
    }

    /// Whether the kind is followed.
    #[must_use]
    pub fn is_served(&self) -> bool {
        matches!(self, Self::Served { .. })
    }
}

impl GatewayKind {
    /// Its name, as a manifest's `kind` says it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::GatewayClass => "GatewayClass",
            Self::Gateway => "Gateway",
            Self::HTTPRoute => "HTTPRoute",
            Self::ReferenceGrant => "ReferenceGrant",
        }
    }
}

/// Whether the release of every kind `served` is one supported, its CRD there and serving
/// [`VERSION`], as a GatewayClass's `SupportedVersion` says (04 §6); or what of each is
/// not.
///
/// # Errors
///
/// What of each kind is not supported, and the releases that are.
pub fn supported(served: &[(GatewayKind, Served)]) -> Result<(), String> {
    let unsupported: Vec<String> = served
        .iter()
        .filter(|(_, served)| match served {
            Served::Served {
                release: Some(release),
            } => !SUPPORTED.iter().any(|minor| minor_of(release) == *minor),
            _ => true,
        })
        .map(|(kind, served)| format!("{} {served}", kind.name()))
        .collect();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{}; supported: Gateway API {}",
            unsupported.join("; "),
            SUPPORTED.join(", ")
        ))
    }
}

/// A release's minor: `v1.7` of `v1.7.0`.
fn minor_of(release: &str) -> &str {
    match release.match_indices('.').nth(1) {
        Some((at, _)) => release.get(..at).unwrap_or(release),
        None => release,
    }
}

impl fmt::Display for Served {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let release = |release: &Option<String>| match release {
            Some(release) => format!("Gateway API {release}"),
            None => "no release named".to_owned(),
        };
        match self {
            Self::Served { release: r } => write!(f, "served at {VERSION} ({})", release(r)),
            Self::OtherVersions {
                versions,
                release: r,
            } => write!(
                f,
                "not served at {VERSION} (only {}; {}), so taken as having no objects",
                versions.join(", "),
                release(r)
            ),
            Self::Missing => write!(f, "not installed, so taken as having no objects"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GatewayKind, Served};
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

    fn crd(release: Option<&str>, versions: &[(&str, bool)]) -> CustomResourceDefinition {
        let annotations = release
            .map(|release| {
                format!("annotations: {{ gateway.networking.k8s.io/bundle-version: {release} }}")
            })
            .unwrap_or_default();
        let versions: Vec<String> = versions
            .iter()
            .map(|(name, served)| format!("{{ name: {name}, served: {served}, storage: false }}"))
            .collect();
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: referencegrants.gateway.networking.k8s.io, {annotations} }}\n\
             spec: {{ group: gateway.networking.k8s.io, scope: Namespaced, \
             names: {{ kind: ReferenceGrant, plural: referencegrants }}, versions: [{}] }}",
            versions.join(", ")
        ))
        .unwrap()
    }

    #[test]
    fn a_kind_is_served_where_its_crd_serves_v1() {
        let served = Served::of(&[crd(Some("v1.7.0"), &[("v1", true), ("v1beta1", true)])]);
        assert_eq!(
            served,
            Served::Served {
                release: Some("v1.7.0".to_owned())
            }
        );
        assert!(served.is_served());
        assert_eq!(served.to_string(), "served at v1 (Gateway API v1.7.0)");
    }

    #[test]
    fn an_older_install_without_v1_is_taken_as_none() {
        // ReferenceGrant before Gateway API v1.5.
        let served = Served::of(&[crd(Some("v1.4.0"), &[("v1beta1", true)])]);
        assert!(!served.is_served());
        assert_eq!(
            served.to_string(),
            "not served at v1 (only v1beta1; Gateway API v1.4.0), so taken as having no objects"
        );
        // A version there but no longer served is not served.
        let served = Served::of(&[crd(None, &[("v1", false), ("v1beta1", true)])]);
        assert_eq!(
            served,
            Served::OtherVersions {
                versions: vec!["v1beta1".to_owned()],
                release: None
            }
        );
    }

    #[test]
    fn a_kind_without_its_crd_is_missing() {
        assert_eq!(Served::of(&[]), Served::Missing);
        assert_eq!(
            Served::Missing.to_string(),
            "not installed, so taken as having no objects"
        );
    }

    #[test]
    fn each_kind_names_its_crd() {
        let crds: Vec<_> = GatewayKind::ALL.iter().map(|kind| kind.crd()).collect();
        assert_eq!(
            crds,
            [
                "gatewayclasses.gateway.networking.k8s.io",
                "gateways.gateway.networking.k8s.io",
                "httproutes.gateway.networking.k8s.io",
                "referencegrants.gateway.networking.k8s.io",
            ]
        );
    }
}
