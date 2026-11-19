//! `RateLimitPolicy`, `gateway.edgerush.dev/v1alpha1`. Written by `edgerush-crdgen` from
//! `install/gateway.edgerush.dev_ratelimitpolicies.yaml`: not to be edited (the crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta};
use serde::{Deserialize, Serialize};

/// RateLimitPolicy adds rate limits to what its targets serve, and may remove less
/// specific ones by name. A request is under the limits of its DataPlane's
/// `rateLimiting.global` and of every policy on its Gateway, its listener, its
/// ListenerSet and that one's listener, its route or Ingress, and its rule, charged
/// to all of them or to none, less those a more specific policy ignores. A route's
/// and a rule's policies follow the objects: a request is never under a sibling
/// rule's, whatever its paths.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RateLimitPolicy {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec is where the policy applies, the limits it adds and those it removes.
    #[serde(default)]
    pub spec: RateLimitPolicySpec,
    /// Status is what each controller reports of the policy, by ancestor, as Gateway
    /// API's PolicyStatus has it (GEP-713): for EdgeRush, each DataPlane's Gateway its
    /// targets reach, and an Ingress it targets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<RateLimitPolicyStatus>,
}

/// Spec is where the policy applies, the limits it adds and those it removes.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicySpec {
    /// Ignore is the names of limits less specific than its targets that their
    /// requests are not under: the DataPlane's, and those of policies on a less
    /// specific level (a Gateway is less specific than its listeners, a route than
    /// its rules). ["*"], alone, ignores them all, those added later included. A
    /// name not there is no fault; a limit of the same level or a more specific one
    /// is never ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore: Option<Vec<String>>,
    /// Limits is the limits the policy adds, each written as a DataPlane's
    /// `rateLimiting.global` limit is, and each counted once across all of its
    /// targets. A limit's name is unique across the data plane: a change that
    /// brings a name another policy holds is refused whole, and the policy goes on
    /// as it was last accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<Vec<RateLimitPolicyLimits>>,
    /// TargetRefs is what the policy applies to, in its own namespace: Gateways,
    /// ListenerSets, HTTPRoutes and GRPCRoutes, or Ingresses, each whole or, with a
    /// sectionName, one listener of a Gateway or ListenerSet, or one rule of a
    /// route. They are all of one level, so that what the policy ignores is one
    /// thing: Gateways, ListenerSets, routes or Ingresses, each with a sectionName
    /// or each without. A target or section not there is no fault: the policy
    /// applies to it once it is.
    #[serde(rename = "targetRefs")]
    #[serde(default)]
    pub target_refs: Vec<RateLimitPolicyTargetRefs>,
}

/// Limit is one limit, whose requests it counts together and how many it allows.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyLimits {
    /// DryRun counts and reports, never refusing: a request it has no room
    /// for goes on, and is not charged.
    #[serde(rename = "dryRun")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
    /// Key is whose requests are counted together:
    ///
    /// * Client: the client's address, as the port's forwarding works it out, an IPv6 client by its /64.
    ///
    /// The schema's values: `Client`. Another is read as it is.
    #[serde(default)]
    pub key: String,
    /// Match is the requests it applies to; every request when left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#match: Option<RateLimitPolicyLimitsMatch>,
    /// Model is how it counts requests, one of its fields given.
    #[serde(default)]
    pub model: RateLimitPolicyLimitsModel,
    /// Name is the limit's, unique among them: what its counts are kept
    /// under, so that they go on through a change that leaves the limit as
    /// it was, and what the RateLimit header of a request it refuses calls
    /// it. Printable ASCII.
    #[serde(default)]
    pub name: String,
}

/// Match is the requests it applies to; every request when left out.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyLimitsMatch {
    /// Methods is the methods it applies to, in capitals; any when left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    /// PathPrefix is the start of the paths it applies to.
    #[serde(rename = "pathPrefix")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
}

/// Model is how it counts requests, one of its fields given.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyLimitsModel {
    /// Bucket is a token bucket: requests at Rate on average, at most Burst
    /// at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<RateLimitPolicyLimitsModelBucket>,
    /// FixedWindow is at most Limit requests in each Window, the count
    /// starting again as each ends: a whole window's worth may come at once.
    #[serde(rename = "fixedWindow")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_window: Option<RateLimitPolicyLimitsModelFixedWindow>,
    /// RollingWindow is at most Limit requests in any span of Window, each
    /// counted in one of Slots slots a window: room comes back up to a slot
    /// early, never late.
    #[serde(rename = "rollingWindow")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling_window: Option<RateLimitPolicyLimitsModelRollingWindow>,
}

/// Bucket is a token bucket: requests at Rate on average, at most Burst
/// at once.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyLimitsModelBucket {
    /// Burst is how many requests a client can send at once, from idle.
    #[serde(default)]
    pub burst: i64,
    /// Rate is requests a second, minute, hour or day, on average, such as 100/s, 1000/min, 10/h or 500/d.
    #[serde(default)]
    pub rate: String,
}

/// FixedWindow is at most Limit requests in each Window, the count
/// starting again as each ends: a whole window's worth may come at once.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyLimitsModelFixedWindow {
    /// Limit is requests a window.
    #[serde(default)]
    pub limit: i64,
    /// Window is a whole number and ms, s, m, h or d, such as 500ms, 1m or 1d.
    #[serde(default)]
    pub window: String,
}

/// RollingWindow is at most Limit requests in any span of Window, each
/// counted in one of Slots slots a window: room comes back up to a slot
/// early, never late.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyLimitsModelRollingWindow {
    /// Limit is requests in any window.
    #[serde(default)]
    pub limit: i64,
    /// Slots is the slots a window is counted in, 1,000 unless said:
    /// fewer bring room back sooner, and each key holds a count for
    /// every slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<i64>,
    /// Window is a whole number and ms, s, m, h or d, such as 500ms, 1m or 1d.
    #[serde(default)]
    pub window: String,
}

/// TargetRef is one object the policy applies to, or one section of it, as
/// Gateway API's LocalPolicyTargetReferenceWithSectionName has it (GEP-713).
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyTargetRefs {
    /// Group is the target's API group, gateway.networking.k8s.io, or networking.k8s.io for an Ingress.
    #[serde(default)]
    pub group: String,
    /// Kind is the target's kind, Gateway, ListenerSet, HTTPRoute, GRPCRoute or Ingress.
    #[serde(default)]
    pub kind: String,
    /// Name is the target's name.
    #[serde(default)]
    pub name: String,
    /// SectionName is one listener's name, of a Gateway or ListenerSet, or one
    /// rule's, of an HTTPRoute or GRPCRoute; left out, the whole object. An
    /// Ingress has none.
    #[serde(rename = "sectionName")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_name: Option<String>,
}

/// Status is what each controller reports of the policy, by ancestor, as Gateway
/// API's PolicyStatus has it (GEP-713): for EdgeRush, each DataPlane's Gateway its
/// targets reach, and an Ingress it targets.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyStatus {
    /// Ancestors is a list of ancestor resources (usually Gateways) that are
    /// associated with the policy, and the status of the policy with respect to
    /// each ancestor. When this policy attaches to a parent, the controller that
    /// manages the parent and the ancestors MUST add an entry to this list when
    /// the controller first sees the policy and SHOULD update the entry as
    /// appropriate when the relevant ancestor is modified.
    ///
    /// Note that choosing the relevant ancestor is left to the Policy designers;
    /// an important part of Policy design is designing the right object level at
    /// which to namespace this status.
    ///
    /// Note also that implementations MUST ONLY populate ancestor status for
    /// the Ancestor resources they are responsible for. Implementations MUST
    /// use the ControllerName field to uniquely identify the entries in this list
    /// that they are responsible for.
    ///
    /// Note that to achieve this, the list of PolicyAncestorStatus structs
    /// MUST be treated as a map with a composite key, made up of the AncestorRef
    /// and ControllerName fields combined.
    ///
    /// A maximum of 16 ancestors will be represented in this list. An empty list
    /// means the Policy is not relevant for any ancestors.
    ///
    /// If this slice is full, implementations MUST NOT add further entries.
    /// Instead they MUST consider the policy unimplementable and signal that
    /// on any related resources such as the ancestor that would be referenced
    /// here. For example, if this list was full on BackendTLSPolicy, no
    /// additional Gateways would be able to reference the Service targeted by
    /// the BackendTLSPolicy.
    #[serde(default)]
    pub ancestors: Vec<RateLimitPolicyStatusAncestors>,
}

/// PolicyAncestorStatus describes the status of a route with respect to an
/// associated Ancestor.
///
/// Ancestors refer to objects that are either the Target of a policy or above it
/// in terms of object hierarchy. For example, if a policy targets a Service, the
/// Policy's Ancestors are, in order, the Service, the HTTPRoute, the Gateway, and
/// the GatewayClass. Almost always, in this hierarchy, the Gateway will be the most
/// useful object to place Policy status on, so we recommend that implementations
/// SHOULD use Gateway as the PolicyAncestorStatus object unless the designers
/// have a _very_ good reason otherwise.
///
/// In the context of policy attachment, the Ancestor is used to distinguish which
/// resource results in a distinct application of this policy. For example, if a policy
/// targets a Service, it may have a distinct result per attached Gateway.
///
/// Policies targeting the same resource may have different effects depending on the
/// ancestors of those resources. For example, different Gateways targeting the same
/// Service may have different capabilities, especially if they have different underlying
/// implementations.
///
/// For example, in BackendTLSPolicy, the Policy attaches to a Service that is
/// used as a backend in a HTTPRoute that is itself attached to a Gateway.
/// In this case, the relevant object for status is the Gateway, and that is the
/// ancestor object referred to in this status.
///
/// Note that a parent is also an ancestor, so for objects where the parent is the
/// relevant object for status, this struct SHOULD still be used.
///
/// This struct is intended to be used in a slice that's effectively a map,
/// with a composite key made up of the AncestorRef and the ControllerName.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyStatusAncestors {
    /// AncestorRef corresponds with a ParentRef in the spec that this
    /// PolicyAncestorStatus struct describes the status of.
    #[serde(rename = "ancestorRef")]
    #[serde(default)]
    pub ancestor_ref: RateLimitPolicyStatusAncestorsAncestorRef,
    /// Conditions describes the status of the Policy with respect to the given Ancestor.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// ControllerName is a domain/path string that indicates the name of the
    /// controller that wrote this status. This corresponds with the
    /// controllerName field on GatewayClass.
    ///
    /// Example: "example.net/gateway-controller".
    ///
    /// The format of this field is DOMAIN "/" PATH, where DOMAIN and PATH are
    /// valid Kubernetes names
    /// (<https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names>).
    ///
    /// Controllers MUST populate this field when writing status. Controllers should ensure that
    /// entries to status populated with their ControllerName are cleaned up when they are no
    /// longer necessary.
    #[serde(rename = "controllerName")]
    #[serde(default)]
    pub controller_name: String,
}

/// AncestorRef corresponds with a ParentRef in the spec that this
/// PolicyAncestorStatus struct describes the status of.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RateLimitPolicyStatusAncestorsAncestorRef {
    /// Group is the group of the referent.
    /// When unspecified, "gateway.networking.k8s.io" is inferred.
    /// To set the core API group (such as for a "Service" kind referent),
    /// Group must be explicitly set to "" (empty string).
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Kind is kind of the referent.
    ///
    /// There are two kinds of parent resources with "Core" support:
    ///
    /// * Gateway (Gateway conformance profile)
    /// * Service (Mesh conformance profile, ClusterIP Services only)
    ///
    /// Support for other resources is Implementation-Specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Name is the name of the referent.
    ///
    /// Support: Core
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of the referent. When unspecified, this refers
    /// to the local namespace of the Route.
    ///
    /// Note that there are specific rules for ParentRefs which cross namespace
    /// boundaries. Cross-namespace references are only valid if they are explicitly
    /// allowed by something in the namespace they are referring to. For example:
    /// Gateway has the AllowedRoutes field, and ReferenceGrant provides a
    /// generic way to enable any other kind of cross-namespace reference.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Port is the network port this Route targets. It can be interpreted
    /// differently based on the type of parent resource.
    ///
    /// When the parent resource is a Gateway, this targets all listeners
    /// listening on the specified port that also support this kind of Route(and
    /// select this Route). It's not recommended to set `Port` unless the
    /// networking behaviors specified in a Route must apply to a specific port
    /// as opposed to a listener(s) whose port(s) may be changed. When both Port
    /// and SectionName are specified, the name and port of the selected listener
    /// must match both specified values.
    ///
    /// Implementations MAY choose to support other parent resources.
    /// Implementations supporting other types of parent resources MUST clearly
    /// document how/if Port is interpreted.
    ///
    /// For the purpose of status, an attachment is considered successful as
    /// long as the parent resource accepts it partially. For example, Gateway
    /// listeners can restrict which Routes can attach to them by Route kind,
    /// namespace, or hostname. If 1 of 2 Gateway listeners accept attachment
    /// from the referencing Route, the Route MUST be considered successfully
    /// attached. If no Gateway listeners accept attachment from this Route,
    /// the Route MUST be considered detached from the Gateway.
    ///
    /// Support: Extended
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    /// SectionName is the name of a section within the target resource. In the
    /// following resources, SectionName is interpreted as the following:
    ///
    /// * Gateway: Listener name. When both Port (experimental) and SectionName
    /// are specified, the name and port of the selected listener must match
    /// both specified values.
    /// * Service: Port name. When both Port (experimental) and SectionName
    /// are specified, the name and port of the selected listener must match
    /// both specified values.
    ///
    /// Implementations MAY choose to support attaching Routes to other resources.
    /// If that is the case, they MUST clearly document how SectionName is
    /// interpreted.
    ///
    /// When unspecified (empty string), this will reference the entire resource.
    /// For the purpose of status, an attachment is considered successful if at
    /// least one section in the parent resource accepts it. For example, Gateway
    /// listeners can restrict which Routes can attach to them by Route kind,
    /// namespace, or hostname. If 1 of 2 Gateway listeners accept attachment from
    /// the referencing Route, the Route MUST be considered successfully
    /// attached. If no Gateway listeners accept attachment from this Route, the
    /// Route MUST be considered detached from the Gateway.
    ///
    /// Support: Core
    #[serde(rename = "sectionName")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_name: Option<String>,
}

impl k8s_openapi::Resource for RateLimitPolicy {
    const API_VERSION: &'static str = "gateway.edgerush.dev/v1alpha1";
    const GROUP: &'static str = "gateway.edgerush.dev";
    const KIND: &'static str = "RateLimitPolicy";
    const VERSION: &'static str = "v1alpha1";
    const URL_PATH_SEGMENT: &'static str = "ratelimitpolicies";
    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::ListableResource for RateLimitPolicy {
    const LIST_KIND: &'static str = "RateLimitPolicyList";
}

impl k8s_openapi::Metadata for RateLimitPolicy {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
